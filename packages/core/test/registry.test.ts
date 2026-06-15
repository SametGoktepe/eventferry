import { describe, expect, it } from "vitest";
import { defineOutbox } from "../src/registry.js";
import { OutboxValidationError } from "../src/errors.js";
import type { EnqueueableStore } from "../src/registry.js";
import type { StandardSchemaV1 } from "../src/standard-schema.js";

/**
 * Build a minimal Standard Schema by hand — no validator dependency. Proves the
 * registry is validator-agnostic (the same shape Zod/Valibot/ArkType expose).
 */
function schema<Output, Input = Output>(
  validate: (
    value: unknown,
  ) =>
    | StandardSchemaV1.Result<Output>
    | Promise<StandardSchemaV1.Result<Output>>,
): StandardSchemaV1<Input, Output> {
  return { "~standard": { version: 1, vendor: "test", validate } };
}

const orderCreated = schema<{ orderId: string; total: number }>((v) => {
  if (
    typeof v === "object" &&
    v !== null &&
    typeof (v as any).orderId === "string" &&
    typeof (v as any).total === "number"
  ) {
    return { value: v as { orderId: string; total: number } };
  }
  return { issues: [{ message: "expected { orderId: string, total: number }" }] };
});

// Output differs from input: coerces { total: string } -> { total: number }.
const coercedTotal = schema<{ total: number }, { total: string }>((v) => {
  const total = Number((v as any)?.total);
  if (Number.isNaN(total)) return { issues: [{ message: "total not numeric" }] };
  return { value: { total } };
});

// Async validate path.
const asyncSchema = schema<{ ok: boolean }>(async (v) => {
  if ((v as any)?.ok === true) return { value: { ok: true } };
  return { issues: [{ message: "ok must be true" }] };
});

const registry = {
  "orders.created": { aggregateType: "order", schema: orderCreated },
  "orders.coerced": { aggregateType: "order", schema: coercedTotal },
  "things.async": { aggregateType: "thing", schema: asyncSchema },
};

class FakeStore implements EnqueueableStore<string> {
  calls: { tx: string; msg: Record<string, unknown> }[] = [];
  async enqueue(tx: string, msg: Record<string, unknown>): Promise<string> {
    this.calls.push({ tx, msg });
    return "generated-id";
  }
}

describe("defineOutbox.enqueue", () => {
  it("validates, forwards the right fields, and returns the message id", async () => {
    const store = new FakeStore();
    const outbox = defineOutbox(registry, { store });

    const id = await outbox.enqueue("tx-1", "orders.created", {
      aggregateId: "o-1",
      payload: { orderId: "o-1", total: 99 },
      headers: { source: "svc" },
    });

    expect(id).toBe("generated-id");
    expect(store.calls).toHaveLength(1);
    expect(store.calls[0]?.tx).toBe("tx-1");
    expect(store.calls[0]?.msg).toMatchObject({
      topic: "orders.created",
      aggregateType: "order",
      aggregateId: "o-1",
      payload: { orderId: "o-1", total: 99 },
      headers: { source: "svc" },
    });
  });

  it("rejects an invalid payload WITHOUT touching the store", async () => {
    const store = new FakeStore();
    const outbox = defineOutbox(registry, { store });

    await expect(
      outbox.enqueue("tx-1", "orders.created", {
        aggregateId: "o-1",
        payload: { orderId: "o-1", total: "not-a-number" } as never,
      }),
    ).rejects.toBeInstanceOf(OutboxValidationError);

    expect(store.calls).toHaveLength(0);
  });

  it("stores the schema's transformed output, not the raw input", async () => {
    const store = new FakeStore();
    const outbox = defineOutbox(registry, { store });

    await outbox.enqueue("tx-1", "orders.coerced", {
      aggregateId: "o-2",
      payload: { total: "150" },
    });

    expect(store.calls[0]?.msg.payload).toEqual({ total: 150 });
  });

  it("supports async schemas", async () => {
    const store = new FakeStore();
    const outbox = defineOutbox(registry, { store });

    await outbox.enqueue("tx-1", "things.async", {
      aggregateId: "t-1",
      payload: { ok: true },
    });
    expect(store.calls[0]?.msg.payload).toEqual({ ok: true });

    await expect(
      outbox.enqueue("tx-1", "things.async", {
        aggregateId: "t-1",
        payload: { ok: false } as never,
      }),
    ).rejects.toBeInstanceOf(OutboxValidationError);
  });
});

describe("defineOutbox.validate", () => {
  it("returns the validated value", async () => {
    const outbox = defineOutbox(registry);
    await expect(
      outbox.validate("orders.created", { orderId: "x", total: 1 }),
    ).resolves.toEqual({ orderId: "x", total: 1 });
  });

  it("throws OutboxValidationError carrying topic + issues", async () => {
    const outbox = defineOutbox(registry);
    try {
      await outbox.validate("orders.created", { orderId: 1 });
      throw new Error("should have thrown");
    } catch (err) {
      expect(err).toBeInstanceOf(OutboxValidationError);
      const e = err as OutboxValidationError;
      expect(e.topic).toBe("orders.created");
      expect(e.issues.length).toBeGreaterThan(0);
    }
  });
});

describe("defineOutbox.decode", () => {
  it("decodes a Buffer and validates it", async () => {
    const outbox = defineOutbox(registry);
    const bytes = Buffer.from(JSON.stringify({ orderId: "o-9", total: 5 }), "utf8");
    await expect(outbox.decode("orders.created", bytes)).resolves.toEqual({
      orderId: "o-9",
      total: 5,
    });
  });

  it("decodes a string too", async () => {
    const outbox = defineOutbox(registry);
    await expect(
      outbox.decode("orders.created", '{"orderId":"o-9","total":5}'),
    ).resolves.toEqual({ orderId: "o-9", total: 5 });
  });

  it("throws OutboxValidationError on a schema-invalid message", async () => {
    const outbox = defineOutbox(registry);
    await expect(
      outbox.decode("orders.created", '{"orderId":"o-9"}'),
    ).rejects.toBeInstanceOf(OutboxValidationError);
  });

  it("throws OutboxValidationError on malformed JSON", async () => {
    const outbox = defineOutbox(registry);
    await expect(
      outbox.decode("orders.created", "not json"),
    ).rejects.toBeInstanceOf(OutboxValidationError);
  });
});
