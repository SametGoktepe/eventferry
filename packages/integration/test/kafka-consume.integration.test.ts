import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { PublishableMessage } from "@eventferry/core";
import {
  KafkaPublisher,
  type KafkaTracer,
  type SpanLike,
} from "@eventferry/kafka";
import { decode, extractTraceContext } from "@eventferry/kafka/consume";
import { brokers, collectMessages, createTopic, uniqueName } from "./helpers.js";

function msg(
  topic: string,
  over: Partial<PublishableMessage> = {},
): PublishableMessage {
  return {
    topic,
    key: "agg-1",
    value: Buffer.from(JSON.stringify({ orderId: "agg-1", total: 99 }), "utf8"),
    headers: { "x-tenant": "t1" },
    recordId: "1",
    messageId: "m1",
    ...over,
  };
}

const NOOP_SPAN: SpanLike = {
  setAttribute() {},
  setAttributes() {},
  setStatus() {},
  recordException() {},
  end() {},
};

/**
 * Verifies the publish→consume W3C trace context propagation end-to-end:
 * publisher's tracer.inject writes the headers, the broker delivers them
 * untouched, and extractTraceContext on the consumer side parses them.
 */
describe("consume helpers + tracer.inject against real Redpanda", () => {
  let publisher: KafkaPublisher;
  const TRACEPARENT =
    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

  beforeAll(async () => {
    const tracer: KafkaTracer = {
      startPublishSpan: () => NOOP_SPAN,
      inject(_span, headers) {
        headers["traceparent"] = TRACEPARENT;
        headers["tracestate"] = "vendor=value";
      },
    };
    publisher = new KafkaPublisher({ brokers: brokers(), tracer });
    await publisher.connect();
  });
  afterAll(async () => {
    await publisher.disconnect();
  });

  it("tracer.inject writes traceparent the consumer can extract via extractTraceContext", async () => {
    const topic = uniqueName("trace");
    await createTopic(topic);
    await publisher.publish([msg(topic)]);

    const [got] = await collectMessages(topic, 1);
    // The header arrived intact on the wire.
    expect(got?.headers["traceparent"]).toBe(TRACEPARENT);

    const ctx = extractTraceContext(got!.headers);
    expect(ctx).not.toBeNull();
    expect(ctx?.traceId).toBe("0af7651916cd43dd8448eb211c80319c");
    expect(ctx?.spanId).toBe("b7ad6b7169203331");
    expect(ctx?.sampled).toBe(true);
    expect(ctx?.tracestate).toBe("vendor=value");
  });

  it("decode normalizes the consumed payload + key + headers", async () => {
    const topic = uniqueName("decode");
    await createTopic(topic);
    await publisher.publish([msg(topic)]);

    const [got] = await collectMessages(topic, 1);
    const decoded = decode<{ orderId: string; total: number }>({
      key: got!.key,
      value: got!.value,
      headers: got!.headers,
    });
    expect(decoded.value).toEqual({ orderId: "agg-1", total: 99 });
    expect(decoded.key).toBe("agg-1");
    expect(decoded.headers["x-tenant"]).toBe("t1");
    // tracer.inject ran on this batch too — make sure decode preserves the header.
    expect(decoded.headers["traceparent"]).toBe(TRACEPARENT);
  });

  it("user-supplied PublishableMessage is NOT mutated by inject (regression: caller-owned reference)", async () => {
    const topic = uniqueName("immutability");
    await createTopic(topic);
    const original = msg(topic, {
      recordId: "imm-1",
      headers: { "x-tenant": "t1" },
    });
    await publisher.publish([original]);

    // The relay reuses the same PublishableMessage reference on retry.
    // If the publisher mutated headers in place, the second publish would
    // accumulate trace headers and corrupt subsequent retries. Lock this in.
    expect(original.headers).toEqual({ "x-tenant": "t1" });
  });
});
