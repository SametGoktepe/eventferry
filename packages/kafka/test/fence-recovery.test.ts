import { describe, expect, it, vi } from "vitest";
import type {
  PublishableMessage,
  PublishResult,
} from "@eventferry/core";
import { KafkaPublisher } from "../src/publisher.js";
import type { KafkaDriver } from "../src/driver.js";

function msg(over: Partial<PublishableMessage> = {}): PublishableMessage {
  return {
    topic: "orders",
    key: "agg-1",
    value: Buffer.from("{}"),
    headers: {},
    recordId: "1",
    messageId: "m1",
    ...over,
  };
}

/**
 * Programmable fake driver — each call to sendBatch consumes the next
 * scripted response. Lets us model first-attempt-fenced, second-attempt-ok
 * (and a few other shapes) without a real broker.
 */
class ScriptedDriver implements KafkaDriver {
  readonly transactional = true;
  connects = 0;
  disconnects = 0;
  sends = 0;
  private readonly script: Array<
    "ok" | "fenced" | "throw" | "fenced-again"
  >;
  constructor(...script: Array<"ok" | "fenced" | "throw" | "fenced-again">) {
    this.script = [...script];
  }
  async connect(): Promise<void> {
    this.connects++;
  }
  async disconnect(): Promise<void> {
    this.disconnects++;
  }
  async sendBatch(messages: PublishableMessage[]): Promise<PublishResult[]> {
    this.sends++;
    const next = this.script.shift() ?? "ok";
    if (next === "throw") throw new Error("driver crashed");
    if (next === "fenced" || next === "fenced-again") {
      return messages.map((m) => ({
        recordId: m.recordId,
        ok: false,
        error: new Error("PRODUCER_FENCED"),
        errorKind: "fenced",
      }));
    }
    return messages.map((m) => ({ recordId: m.recordId, ok: true }));
  }
  async admin() {
    throw new Error("not used");
  }
}

describe("KafkaPublisher — fence recovery", () => {
  it("autoRecoverFromFence: false (default) surfaces fenced results unchanged", async () => {
    const driver = new ScriptedDriver("fenced");
    const pub = new KafkaPublisher({
      brokers: ["b:9092"],
      customDriver: driver,
    });
    const [r] = await pub.publish([msg()]);
    expect(r?.ok).toBe(false);
    expect(r?.errorKind).toBe("fenced");
    expect(driver.disconnects).toBe(0);
    expect(driver.connects).toBe(0);
  });

  it("autoRecoverFromFence: true reconnects ONCE and retries the same batch", async () => {
    const driver = new ScriptedDriver("fenced", "ok");
    const pub = new KafkaPublisher({
      brokers: ["b:9092"],
      customDriver: driver,
      autoRecoverFromFence: true,
    });
    const [r] = await pub.publish([msg()]);
    expect(r?.ok).toBe(true);
    expect(driver.disconnects).toBe(1);
    expect(driver.connects).toBe(1);
    expect(driver.sends).toBe(2);
  });

  it("second attempt still fenced → publisher gives up, surfaces the SECOND result", async () => {
    const driver = new ScriptedDriver("fenced", "fenced-again");
    const pub = new KafkaPublisher({
      brokers: ["b:9092"],
      customDriver: driver,
      autoRecoverFromFence: true,
    });
    const [r] = await pub.publish([msg()]);
    expect(r?.ok).toBe(false);
    expect(r?.errorKind).toBe("fenced");
    // Only ONE reconnect — the publisher doesn't loop indefinitely.
    expect(driver.disconnects).toBe(1);
    expect(driver.connects).toBe(1);
    expect(driver.sends).toBe(2);
  });

  it("reconnect itself throws → original fence result is surfaced", async () => {
    class BrokenReconnect extends ScriptedDriver {
      override async connect(): Promise<void> {
        if (this.connects > 0) throw new Error("reconnect failed");
        this.connects++;
      }
    }
    const driver = new BrokenReconnect("fenced");
    await driver.connect(); // simulate initial publisher.connect()
    expect(driver.connects).toBe(1);
    const onError = vi.fn();
    const pub = new KafkaPublisher({
      brokers: ["b:9092"],
      customDriver: driver,
      autoRecoverFromFence: true,
      hooks: { onError },
    });
    const [r] = await pub.publish([msg()]);
    expect(r?.ok).toBe(false);
    expect(r?.errorKind).toBe("fenced");
    // The reconnect failure should surface through onError so operators
    // know the recovery itself died.
    expect(onError).toHaveBeenCalled();
    expect((onError.mock.calls[0]?.[0] as Error).message).toMatch(
      /reconnect failed/,
    );
  });

  it("onProducerFenced hook fires even when autoRecoverFromFence is OFF", async () => {
    const driver = new ScriptedDriver("fenced");
    const onProducerFenced = vi.fn();
    const pub = new KafkaPublisher({
      brokers: ["b:9092"],
      customDriver: driver,
      hooks: { onProducerFenced },
    });
    await pub.publish([msg()]);
    expect(onProducerFenced).toHaveBeenCalledTimes(1);
    expect((onProducerFenced.mock.calls[0]?.[0] as Error).message).toMatch(
      /PRODUCER_FENCED/,
    );
  });

  it("onProducerFenced fires BEFORE the recovery attempt (operators get the warning)", async () => {
    const order: string[] = [];
    class TracingDriver extends ScriptedDriver {
      override async disconnect(): Promise<void> {
        order.push("disconnect");
        await super.disconnect();
      }
    }
    const driver = new TracingDriver("fenced", "ok");
    const pub = new KafkaPublisher({
      brokers: ["b:9092"],
      customDriver: driver,
      autoRecoverFromFence: true,
      hooks: {
        onProducerFenced: () => {
          order.push("hook");
        },
      },
    });
    await pub.publish([msg()]);
    expect(order).toEqual(["hook", "disconnect"]);
  });

  it("subsequent fence after a recovery attempts another recovery (per-incident, not per-process)", async () => {
    const driver = new ScriptedDriver("fenced", "ok", "fenced", "ok");
    const pub = new KafkaPublisher({
      brokers: ["b:9092"],
      customDriver: driver,
      autoRecoverFromFence: true,
    });
    await pub.publish([msg()]);
    await pub.publish([msg({ recordId: "2" })]);
    // Two separate fence events → two separate recoveries.
    expect(driver.disconnects).toBe(2);
    expect(driver.connects).toBe(2);
    expect(driver.sends).toBe(4);
  });

  it("non-fenced failure does NOT trigger recovery", async () => {
    class RetriableDriver implements KafkaDriver {
      readonly transactional = true;
      sends = 0;
      disconnects = 0;
      connects = 0;
      async connect() {
        this.connects++;
      }
      async disconnect() {
        this.disconnects++;
      }
      async sendBatch(messages: PublishableMessage[]): Promise<PublishResult[]> {
        this.sends++;
        return messages.map((m) => ({
          recordId: m.recordId,
          ok: false,
          error: new Error("network glitch"),
          errorKind: "retriable",
        }));
      }
    }
    const driver = new RetriableDriver();
    const pub = new KafkaPublisher({
      brokers: ["b:9092"],
      customDriver: driver,
      autoRecoverFromFence: true,
    });
    await pub.publish([msg()]);
    expect(driver.disconnects).toBe(0);
    expect(driver.connects).toBe(0);
    expect(driver.sends).toBe(1);
  });

  it("concurrent fenced publishes share a SINGLE in-flight recovery", async () => {
    // Two concurrent publish() calls that both hit a fence must not
    // race to disconnect twice — that would tear the producer down
    // while the other is mid-restart.
    class SlowReconnectDriver extends ScriptedDriver {
      override async connect(): Promise<void> {
        // Yield once so both publish() calls land in recoverAndRetry.
        await new Promise((r) => setTimeout(r, 5));
        this.connects++;
      }
    }
    const driver = new SlowReconnectDriver("fenced", "fenced", "ok", "ok");
    const pub = new KafkaPublisher({
      brokers: ["b:9092"],
      customDriver: driver,
      autoRecoverFromFence: true,
    });
    const [a, b] = await Promise.all([
      pub.publish([msg({ recordId: "a" })]),
      pub.publish([msg({ recordId: "b" })]),
    ]);
    expect(a?.[0]?.ok).toBe(true);
    expect(b?.[0]?.ok).toBe(true);
    // Single shared reconnect for the two concurrent fence reports.
    expect(driver.disconnects).toBe(1);
    expect(driver.connects).toBe(1);
  });
});
