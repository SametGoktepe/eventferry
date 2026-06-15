import { OutboxValidationError } from "./errors.js";
import type { StandardSchemaV1 } from "./standard-schema.js";
import type { OutboxMessageInput } from "./types.js";

/** One topic's contract: the aggregate it belongs to and its payload schema. */
export interface TopicDefinition {
  /** Aggregate type stamped on every event of this topic (e.g. "order"). */
  readonly aggregateType: string;
  /** Standard Schema for the payload (Zod 3.24+/Valibot/ArkType/…). */
  readonly schema: StandardSchemaV1;
}

/** A map of topic name -> its definition. The single source of truth. */
export type OutboxRegistry = Record<string, TopicDefinition>;

/**
 * Minimal store surface the producer facade needs. `PostgresStore` satisfies
 * this structurally, so the facade stays DB-agnostic and `tx` flows from the
 * concrete store (e.g. a pg client).
 */
export interface EnqueueableStore<Tx = unknown> {
  enqueue(tx: Tx, msg: OutboxMessageInput & { traceId?: string }): Promise<string>;
}

type TxOf<S> = S extends EnqueueableStore<infer Tx> ? Tx : never;

type PayloadInput<R extends OutboxRegistry, K extends keyof R> =
  StandardSchemaV1.InferInput<R[K]["schema"]>;
type PayloadOutput<R extends OutboxRegistry, K extends keyof R> =
  StandardSchemaV1.InferOutput<R[K]["schema"]>;

/** The write-side input for a typed enqueue (payload is the schema's input type). */
export interface EnqueueInput<R extends OutboxRegistry, K extends keyof R> {
  aggregateId: string;
  payload: PayloadInput<R, K>;
  key?: string;
  headers?: Record<string, string>;
  messageId?: string;
  traceId?: string;
}

/** Consumer-side facade: validate/decode without a store. */
export interface OutboxConsumer<R extends OutboxRegistry> {
  /** Validate an already-parsed value against the topic's schema. */
  validate<K extends keyof R & string>(
    topic: K,
    value: unknown,
  ): Promise<PayloadOutput<R, K>>;
  /** JSON-parse `bytes`, validate against the topic's schema, return the payload. */
  decode<K extends keyof R & string>(
    topic: K,
    bytes: Buffer | Uint8Array | string,
  ): Promise<PayloadOutput<R, K>>;
}

/** Producer-side facade: adds typed, validated enqueue. */
export interface OutboxProducer<R extends OutboxRegistry, Tx>
  extends OutboxConsumer<R> {
  /** Validate `payload`, then enqueue inside the caller's transaction `tx`. */
  enqueue<K extends keyof R & string>(
    tx: Tx,
    topic: K,
    msg: EnqueueInput<R, K>,
  ): Promise<string>;
}

// Loose shape used inside the implementation; the public types come from overloads.
interface LooseEnqueueInput {
  aggregateId: string;
  payload: unknown;
  key?: string;
  headers?: Record<string, string>;
  messageId?: string;
  traceId?: string;
}

export function defineOutbox<R extends OutboxRegistry>(
  registry: R,
): OutboxConsumer<R>;
export function defineOutbox<R extends OutboxRegistry, S extends EnqueueableStore>(
  registry: R,
  opts: { store: S },
): OutboxProducer<R, TxOf<S>>;
export function defineOutbox<R extends OutboxRegistry>(
  registry: R,
  opts?: { store: EnqueueableStore },
): OutboxConsumer<R> | OutboxProducer<R, unknown> {
  const validate = async (topic: string, value: unknown): Promise<unknown> => {
    const def = registry[topic];
    if (!def) {
      throw new OutboxValidationError(topic, [
        { message: `unknown topic "${topic}"` },
      ]);
    }
    const result = await def.schema["~standard"].validate(value);
    if (result.issues) throw new OutboxValidationError(topic, result.issues);
    return result.value;
  };

  const decode = async (
    topic: string,
    bytes: Buffer | Uint8Array | string,
  ): Promise<unknown> => {
    const text =
      typeof bytes === "string" ? bytes : Buffer.from(bytes).toString("utf8");
    let parsed: unknown;
    try {
      parsed = JSON.parse(text);
    } catch (err) {
      throw new OutboxValidationError(
        topic,
        [{ message: `invalid JSON: ${(err as Error).message}` }],
        { cause: err },
      );
    }
    return validate(topic, parsed);
  };

  const consumer = { validate, decode } as unknown as OutboxConsumer<R>;
  if (!opts?.store) return consumer;

  const store = opts.store;
  const enqueue = async (
    tx: unknown,
    topic: string,
    msg: LooseEnqueueInput,
  ): Promise<string> => {
    const def = registry[topic];
    if (!def) {
      throw new OutboxValidationError(topic, [
        { message: `unknown topic "${topic}"` },
      ]);
    }
    const payload = await validate(topic, msg.payload);
    return store.enqueue(tx, {
      topic,
      aggregateType: def.aggregateType,
      aggregateId: msg.aggregateId,
      payload,
      key: msg.key,
      headers: msg.headers,
      messageId: msg.messageId,
      traceId: msg.traceId,
    });
  };

  return { validate, decode, enqueue } as unknown as OutboxProducer<R, unknown>;
}
