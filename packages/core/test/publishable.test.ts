import { describe, expect, it } from "vitest";
import { buildPublishable } from "../src/publishable.js";
import { JsonSerializer } from "../src/serializer.js";
import type { OutboxRecord } from "../src/types.js";

function makeRecord(over: Partial<OutboxRecord> = {}): OutboxRecord {
  return {
    id: "42",
    messageId: "m42",
    topic: "orders.created",
    aggregateType: "order",
    aggregateId: "agg-7",
    key: null,
    payload: { orderId: "agg-7", total: 5 },
    headers: { source: "svc" },
    traceId: null,
    status: "pending",
    attempts: 0,
    nextRetryAt: null,
    createdAt: new Date(),
    processedAt: null,
    ...over,
  };
}

describe("buildPublishable", () => {
  it("serializes the payload and sets standard headers", async () => {
    const msg = await buildPublishable(makeRecord(), new JsonSerializer());

    expect(msg.topic).toBe("orders.created");
    expect(msg.recordId).toBe("42");
    expect(msg.messageId).toBe("m42");
    expect(JSON.parse(msg.value.toString("utf8"))).toEqual({
      orderId: "agg-7",
      total: 5,
    });
    expect(msg.headers).toMatchObject({
      source: "svc",
      "content-type": "application/json",
      "message-id": "m42",
      "aggregate-type": "order",
      "aggregate-id": "agg-7",
    });
  });

  it("falls back to aggregateId as the partition key", async () => {
    const msg = await buildPublishable(makeRecord({ key: null }), new JsonSerializer());
    expect(msg.key).toBe("agg-7");
  });

  it("uses an explicit key when present", async () => {
    const msg = await buildPublishable(
      makeRecord({ key: "explicit" }),
      new JsonSerializer(),
    );
    expect(msg.key).toBe("explicit");
  });

  it("includes trace-id only when present", async () => {
    const withTrace = await buildPublishable(
      makeRecord({ traceId: "t-1" }),
      new JsonSerializer(),
    );
    expect(withTrace.headers["trace-id"]).toBe("t-1");

    const without = await buildPublishable(makeRecord({ traceId: null }), new JsonSerializer());
    expect(without.headers["trace-id"]).toBeUndefined();
  });
});
