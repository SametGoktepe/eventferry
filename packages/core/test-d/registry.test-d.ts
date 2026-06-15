// Type-level tests. Not executed — verified by `tsc --noEmit` via
// tsconfig.test-d.json. Each `@ts-expect-error` asserts that the line below it
// is a compile error; if the typing regressed, tsc would flag the unused
// directive and fail.
import { defineOutbox } from "../src/registry.js";
import type { StandardSchemaV1 } from "../src/standard-schema.js";

function schema<Output, Input = Output>(): StandardSchemaV1<Input, Output> {
  return {
    "~standard": {
      version: 1,
      vendor: "test",
      validate: (v) => ({ value: v as Output }),
    },
  };
}

const registry = {
  "orders.created": {
    aggregateType: "order",
    schema: schema<{ orderId: string; total: number }>(),
  },
};

const fakeStore = {
  async enqueue(_tx: { client: string }, _msg: unknown): Promise<string> {
    return "id";
  },
};

export async function producerTypes() {
  const outbox = defineOutbox(registry, { store: fakeStore });

  // ✓ correct payload and tx type
  await outbox.enqueue({ client: "tx" }, "orders.created", {
    aggregateId: "o-1",
    payload: { orderId: "o-1", total: 1 },
  });

  await outbox.enqueue({ client: "tx" }, "orders.created", {
    aggregateId: "o-1",
    payload: {
      orderId: "o-1",
      // @ts-expect-error wrong payload shape: total must be a number
      total: "nope",
    },
  });

  // @ts-expect-error unknown topic
  await outbox.enqueue({ client: "tx" }, "orders.unknown", {
    aggregateId: "o-1",
    payload: {},
  });

  // @ts-expect-error wrong tx type
  await outbox.enqueue({ wrong: true }, "orders.created", {
    aggregateId: "o-1",
    payload: { orderId: "o-1", total: 1 },
  });

  // decode infers the payload type
  const evt: { orderId: string; total: number } = await outbox.decode(
    "orders.created",
    "",
  );
  return evt;
}

export async function consumerTypes() {
  const consumer = defineOutbox(registry);
  // @ts-expect-error consumer-only facade has no enqueue
  consumer.enqueue;
  return consumer.decode("orders.created", "");
}
