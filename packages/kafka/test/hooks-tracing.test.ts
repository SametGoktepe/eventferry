import { describe, expect, it, vi } from "vitest";
import type {
  PublishableMessage,
  PublishResult,
  Logger,
} from "@eventferry/core";
import { KafkaPublisher } from "../src/publisher.js";
import type {
  KafkaDriver,
  KafkaPublisherHooks,
  KafkaTracer,
  SpanLike,
} from "../src/index.js";

function msg(over: Partial<PublishableMessage> = {}): PublishableMessage {
  return {
    topic: "orders.created",
    key: "agg-1",
    value: Buffer.from("{}"),
    headers: {},
    recordId: "r1",
    messageId: "m1",
    ...over,
  };
}

class FakeDriver implements KafkaDriver {
  readonly transactional = false;
  connects = 0;
  disconnects = 0;
  sentBatches: PublishableMessage[][] = [];
  failedIds = new Set<string>();
  throwOnSend: Error | null = null;
  async connect(): Promise<void> {
    this.connects++;
  }
  async disconnect(): Promise<void> {
    this.disconnects++;
  }
  async sendBatch(messages: PublishableMessage[]): Promise<PublishResult[]> {
    if (this.throwOnSend) throw this.throwOnSend;
    this.sentBatches.push([...messages]);
    return messages.map((m) =>
      this.failedIds.has(m.recordId)
        ? { recordId: m.recordId, ok: false, error: new Error("boom") }
        : { recordId: m.recordId, ok: true },
    );
  }
}

class CapturingSpan implements SpanLike {
  attrs: Record<string, string | number | boolean> = {};
  status: { code: "ok" | "error"; message?: string } | null = null;
  exceptions: Error[] = [];
  ended = false;
  setAttribute(k: string, v: string | number | boolean): void {
    this.attrs[k] = v;
  }
  setAttributes(a: Record<string, string | number | boolean>): void {
    Object.assign(this.attrs, a);
  }
  setStatus(s: { code: "ok" | "error"; message?: string }): void {
    this.status = s;
  }
  recordException(e: Error): void {
    this.exceptions.push(e);
  }
  end(): void {
    this.ended = true;
  }
}

class CapturingTracer implements KafkaTracer {
  spans: CapturingSpan[] = [];
  lastName = "";
  startPublishSpan(
    name: string,
    attributes: Record<string, string | number | boolean>,
  ): SpanLike {
    this.lastName = name;
    const span = new CapturingSpan();
    span.setAttributes(attributes);
    this.spans.push(span);
    return span;
  }
}

function noopLogger(): Logger {
  return {
    debug: vi.fn(),
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
  };
}

describe("KafkaPublisher — hooks", () => {
  it("fires onConnect after the driver connects", async () => {
    const driver = new FakeDriver();
    const onConnect = vi.fn();
    const pub = new KafkaPublisher({
      brokers: ["b:9092"],
      customDriver: driver,
      hooks: { onConnect },
    });
    await pub.connect();
    expect(driver.connects).toBe(1);
    expect(onConnect).toHaveBeenCalledTimes(1);
  });

  it("fires onDisconnect after the driver disconnects", async () => {
    const driver = new FakeDriver();
    const onDisconnect = vi.fn();
    const pub = new KafkaPublisher({
      brokers: ["b:9092"],
      customDriver: driver,
      hooks: { onDisconnect },
    });
    await pub.disconnect();
    expect(driver.disconnects).toBe(1);
    expect(onDisconnect).toHaveBeenCalledTimes(1);
  });

  it("fires onPublish for every record (success or failure)", async () => {
    const driver = new FakeDriver();
    driver.failedIds.add("b");
    const onPublish = vi.fn();
    const pub = new KafkaPublisher({
      brokers: ["b:9092"],
      customDriver: driver,
      hooks: { onPublish },
    });
    await pub.publish([msg({ recordId: "a" }), msg({ recordId: "b" })]);
    expect(onPublish).toHaveBeenCalledTimes(2);
    const oks = onPublish.mock.calls.map((c) => c[0].ok);
    expect(oks).toEqual([true, false]);
  });

  it("fires onError for each failed record", async () => {
    const driver = new FakeDriver();
    driver.failedIds.add("a");
    driver.failedIds.add("b");
    const onError = vi.fn();
    const pub = new KafkaPublisher({
      brokers: ["b:9092"],
      customDriver: driver,
      hooks: { onError },
    });
    await pub.publish([msg({ recordId: "a" }), msg({ recordId: "b" })]);
    expect(onError).toHaveBeenCalledTimes(2);
    // The error wrapped on the result is forwarded to the hook.
    expect(onError.mock.calls[0]?.[0]).toBeInstanceOf(Error);
    expect(onError.mock.calls[0]?.[1]).toEqual(
      expect.objectContaining({ recordId: "a" }),
    );
  });

  it("fires onError once with no record when the driver throws", async () => {
    const driver = new FakeDriver();
    driver.throwOnSend = new Error("connection lost");
    const onError = vi.fn();
    const pub = new KafkaPublisher({
      brokers: ["b:9092"],
      customDriver: driver,
      hooks: { onError },
    });
    await expect(pub.publish([msg()])).rejects.toThrow(/connection lost/);
    expect(onError).toHaveBeenCalledTimes(1);
    expect(onError.mock.calls[0]?.[1]).toBeUndefined();
  });

  it("never throws back into publish when a hook throws — logs instead", async () => {
    const driver = new FakeDriver();
    const logger = noopLogger();
    const pub = new KafkaPublisher({
      brokers: ["b:9092"],
      customDriver: driver,
      logger,
      hooks: {
        onPublish() {
          throw new Error("hook bug");
        },
      },
    });
    await expect(pub.publish([msg()])).resolves.not.toThrow();
    expect(logger.warn).toHaveBeenCalled();
    const meta = (logger.warn as ReturnType<typeof vi.fn>).mock.calls[0]?.[1];
    expect((meta as { error: string })?.error).toBe("hook bug");
  });
});

describe("KafkaPublisher — OpenTelemetry span", () => {
  it("starts one span per publish() call with the messaging semconv attrs", async () => {
    const driver = new FakeDriver();
    const tracer = new CapturingTracer();
    const pub = new KafkaPublisher({
      brokers: ["b:9092"],
      customDriver: driver,
      tracer,
    });
    await pub.publish([msg({ recordId: "a" }), msg({ recordId: "b" })]);
    expect(tracer.spans).toHaveLength(1);
    expect(tracer.lastName).toBe("orders.created publish");
    const attrs = tracer.spans[0]!.attrs;
    expect(attrs["messaging.system"]).toBe("kafka");
    expect(attrs["messaging.operation.type"]).toBe("publish");
    expect(attrs["messaging.destination.name"]).toBe("orders.created");
    expect(attrs["messaging.batch.message_count"]).toBe(2);
  });

  it("sets OK status when every record succeeds", async () => {
    const driver = new FakeDriver();
    const tracer = new CapturingTracer();
    const pub = new KafkaPublisher({
      brokers: ["b:9092"],
      customDriver: driver,
      tracer,
    });
    await pub.publish([msg()]);
    expect(tracer.spans[0]?.status?.code).toBe("ok");
    expect(tracer.spans[0]?.ended).toBe(true);
  });

  it("sets ERROR status when any record fails", async () => {
    const driver = new FakeDriver();
    driver.failedIds.add("a");
    const tracer = new CapturingTracer();
    const pub = new KafkaPublisher({
      brokers: ["b:9092"],
      customDriver: driver,
      tracer,
    });
    await pub.publish([msg({ recordId: "a" })]);
    expect(tracer.spans[0]?.status?.code).toBe("error");
  });

  it("records the exception on the span and ends it when the driver throws", async () => {
    const driver = new FakeDriver();
    driver.throwOnSend = new Error("kaboom");
    const tracer = new CapturingTracer();
    const pub = new KafkaPublisher({
      brokers: ["b:9092"],
      customDriver: driver,
      tracer,
    });
    await expect(pub.publish([msg()])).rejects.toThrow(/kaboom/);
    const span = tracer.spans[0]!;
    expect(span.exceptions).toHaveLength(1);
    expect(span.exceptions[0]?.message).toBe("kaboom");
    expect(span.status?.code).toBe("error");
    expect(span.ended).toBe(true);
  });

  it("does not start any span for an empty publish() call", async () => {
    const driver = new FakeDriver();
    const tracer = new CapturingTracer();
    const pub = new KafkaPublisher({
      brokers: ["b:9092"],
      customDriver: driver,
      tracer,
    });
    const res = await pub.publish([]);
    expect(res).toEqual([]);
    expect(tracer.spans).toHaveLength(0);
  });
});

describe("KafkaPublisher — tracer.inject", () => {
  it("invokes inject() once per message with a fresh headers map", async () => {
    const driver = new FakeDriver();
    const calls: { spanRef: SpanLike; headers: Record<string, string> }[] = [];
    const tracer: KafkaTracer = {
      startPublishSpan: () => new CapturingSpan(),
      inject(span, headers) {
        headers["traceparent"] =
          "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        calls.push({ spanRef: span, headers });
      },
    };
    const pub = new KafkaPublisher({
      brokers: ["b:9092"],
      customDriver: driver,
      tracer,
    });

    const original = msg({ recordId: "r1", headers: { "x-tenant": "t1" } });
    await pub.publish([original, msg({ recordId: "r2" })]);

    expect(calls).toHaveLength(2);
    // Caller's PublishableMessage must NOT be mutated — the relay reuses
    // the same reference on retry; a mutation here would compound.
    expect(original.headers).toEqual({ "x-tenant": "t1" });

    // Driver received the enriched headers.
    const [sentBatch] = driver.sentBatches;
    expect(sentBatch![0]!.headers).toEqual({
      "x-tenant": "t1",
      traceparent:
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    });
    expect(sentBatch![1]!.headers).toEqual({
      traceparent:
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    });
  });

  it("publish works untouched when tracer.inject is omitted (no shallow copy cost)", async () => {
    const driver = new FakeDriver();
    const tracer: KafkaTracer = {
      startPublishSpan: () => new CapturingSpan(),
    };
    const pub = new KafkaPublisher({
      brokers: ["b:9092"],
      customDriver: driver,
      tracer,
    });
    const original = msg({ headers: { "x-tenant": "t1" } });
    await pub.publish([original]);
    // When inject is absent the publisher hands the caller's array straight
    // to the driver — verify the reference identity to lock that in.
    expect(driver.sentBatches[0]![0]).toBe(original);
  });
});

describe("KafkaPublisher — no tracer / no hooks", () => {
  it("publish() works with neither tracer nor hooks (pure backward compat)", async () => {
    const driver = new FakeDriver();
    const pub = new KafkaPublisher({
      brokers: ["b:9092"],
      customDriver: driver,
    });
    await pub.connect();
    const r = await pub.publish([msg()]);
    expect(r).toEqual([{ recordId: "r1", ok: true }]);
    await pub.disconnect();
  });
});
