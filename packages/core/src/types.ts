/**
 * Status lifecycle of an outbox record.
 *
 * pending     -> freshly enqueued, awaiting first publish
 * processing  -> claimed by a relay instance, publish in flight
 * done        -> successfully published to the broker
 * failed      -> publish failed, awaiting retry (next_retry_at)
 * dead        -> exhausted retries, routed to DLQ (or parked)
 */
export type OutboxStatus = "pending" | "processing" | "done" | "failed" | "dead";

export const OUTBOX_STATUS_CODE: Record<OutboxStatus, number> = {
  pending: 0,
  processing: 1,
  done: 2,
  failed: 3,
  dead: 4,
};

export const OUTBOX_STATUS_FROM_CODE: Record<number, OutboxStatus> = {
  0: "pending",
  1: "processing",
  2: "done",
  3: "failed",
  4: "dead",
};

/**
 * A message as enqueued by the application inside its own DB transaction.
 * This is the write-side input — no infra concerns leak in here.
 */
export interface OutboxMessageInput {
  /** Logical topic the message will be published to. */
  topic: string;
  /** Type of the aggregate that produced this event (e.g. "order"). */
  aggregateType: string;
  /**
   * Identifier of the aggregate instance (e.g. order id).
   * Used as the default partition key to preserve per-aggregate ordering.
   */
  aggregateId: string;
  /** Event payload. Serialized by the configured serializer. */
  payload: unknown;
  /** Optional explicit partition key. Falls back to aggregateId. */
  key?: string;
  /** Optional message headers. */
  headers?: Record<string, string>;
  /**
   * Optional client-supplied message id for idempotency / dedup.
   * If omitted, the store generates one.
   */
  messageId?: string;
}

/**
 * A persisted outbox record as read back by the relay.
 */
export interface OutboxRecord {
  id: string;
  messageId: string;
  topic: string;
  aggregateType: string;
  aggregateId: string;
  key: string | null;
  payload: unknown;
  headers: Record<string, string>;
  traceId: string | null;
  status: OutboxStatus;
  attempts: number;
  nextRetryAt: Date | null;
  createdAt: Date;
  processedAt: Date | null;
}

/**
 * The message handed to a Publisher after serialization.
 */
export interface PublishableMessage {
  topic: string;
  key: string | null;
  /** Serialized payload bytes. */
  value: Buffer;
  headers: Record<string, string>;
  /** Original record id, for correlation in publish results. */
  recordId: string;
  messageId: string;
  /**
   * Explicit partition override. When set, the publisher MUST route the
   * record to this exact partition, bypassing the configured partitioner.
   *
   * Use cases:
   *   - Compacted topics with application-managed sharding.
   *   - Tenant-affinity routing where you compute the partition yourself.
   *   - Geo-pinning records to a specific broker.
   *
   * When omitted (the default), the underlying client's partitioner
   * decides — usually a hash of `key`, falling back to sticky round-robin
   * when `key` is null.
   */
  partition?: number;
}

/**
 * Why a publish failed, in terms the relay can act on. Drivers classify their
 * native errors into one of these buckets; the relay reads `errorKind` to
 * decide whether to retry, short-circuit to the DLQ, or pause polling. The
 * field is optional for backward compatibility — when absent, the relay
 * treats the error as `"retriable"`.
 *
 * - `retriable` — transient (broker unreachable, leader election, request
 *   timeout); retry per the configured backoff policy. The default for any
 *   unclassified error.
 * - `fatal` — the producer or the credentials are broken (fenced epoch,
 *   authentication failed, ACL denied). Retrying cannot help; the relay
 *   short-circuits straight to the DLQ + `dead` status.
 * - `poison` — the message itself is rejectable by every broker
 *   (oversized record, corrupt payload, schema-registry refused encoding).
 *   Same handling as `fatal`: DLQ + dead, no retries.
 * - `backpressure` — the *producer's own* outbound buffer is full
 *   (librdkafka `__QUEUE_FULL`). The right response is to slow the relay
 *   down, not to burn retries. v2.1 treats this as `retriable` for
 *   compatibility; smarter handling (pause polling) is planned.
 * - `quota` — the broker is throttling us (`THROTTLING_QUOTA_EXCEEDED`).
 *   Back off with longer delays. v2.1 treats as `retriable`; smarter
 *   handling (longer backoff) is planned.
 */
export type PublishErrorKind =
  | "retriable"
  | "fatal"
  | "poison"
  | "backpressure"
  | "quota";

/**
 * Result of attempting to publish a single message.
 */
export interface PublishResult {
  recordId: string;
  ok: boolean;
  error?: Error;
  /**
   * Optional classification of `error` for relay-level decision-making. Set
   * by publisher implementations that know how to inspect their native error
   * shapes. Absent value is treated as `"retriable"` by the relay (the safe
   * default — at worst we retry an error we should have skipped).
   */
  errorKind?: PublishErrorKind;
}

/**
 * Pluggable serializer. Default is JSON; users can swap in
 * Avro / Protobuf / Schema-Registry-backed serializers.
 */
export interface Serializer {
  serialize(message: OutboxRecord): Buffer | Promise<Buffer>;
  /** Content-type header value advertised for this serializer. */
  readonly contentType: string;
}

/**
 * Storage abstraction. Implemented per-database (postgres, mysql, ...).
 * The relay only talks to the store through this interface.
 */
export interface OutboxStore {
  /**
   * Atomically claim up to `batchSize` due messages and mark them
   * as `processing`. Implementations MUST be safe under concurrent
   * relay instances (e.g. SELECT ... FOR UPDATE SKIP LOCKED).
   */
  claimBatch(batchSize: number): Promise<OutboxRecord[]>;

  /** Mark records as successfully published. */
  markDone(recordIds: string[]): Promise<void>;

  /**
   * Mark a record as failed and schedule its next retry.
   * `nextRetryAt` of null + status "dead" means terminal.
   * Implementations MUST increment `attempts` so the retry budget is honored.
   */
  markFailed(
    recordId: string,
    nextRetryAt: Date | null,
    status: "failed" | "dead",
  ): Promise<void>;

  /**
   * Re-queue a record back to `failed` with the given `retryAt` **WITHOUT
   * incrementing attempts**. Used by the relay for backpressure handling —
   * a client-side queue-full failure is a "slow down" signal, not a
   * record-specific failure, and counting it as a retry would burn the
   * attempt budget unfairly.
   *
   * Optional: stores that don't implement this fall back to `markFailed`
   * (which increments attempts, with the caveat documented above).
   */
  requeue?(recordId: string, retryAt: Date): Promise<void>;

  /** Best-effort lifecycle hooks; no-op allowed. */
  init?(): Promise<void>;
  close?(): Promise<void>;
}

/**
 * Broker abstraction. Implemented per-driver (kafkajs, confluent, ...).
 */
export interface Publisher {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  /**
   * Publish a batch. Returns a per-message result so the relay can
   * mark partial success. Implementations may use a transactional
   * producer to make the batch atomic.
   */
  publish(messages: PublishableMessage[]): Promise<PublishResult[]>;
  /** Route a permanently-failed message to a dead-letter destination. */
  publishToDlq?(message: PublishableMessage, error: Error): Promise<void>;
}

/**
 * Backoff strategy for retrying failed publishes.
 */
export type BackoffStrategy = "fixed" | "linear" | "exponential";

export interface RetryConfig {
  maxAttempts: number;
  strategy: BackoffStrategy;
  baseMs: number;
  maxMs: number;
  /** Add random jitter (0..baseMs) to avoid thundering herd. Default true. */
  jitter?: boolean;
  /**
   * Delay (ms) before re-queueing a record whose publish was rejected with
   * `errorKind: "backpressure"` (client-side producer buffer full).
   *
   * Backpressure failures do NOT count as a failed attempt — the buffer
   * being full is a "slow down" signal, not a record-specific failure.
   * The record is requeued at the next interval, not promoted to dead.
   *
   * Default 1000 ms. Requires the {@link OutboxStore} to implement
   * `requeue` — stores without it fall back to {@link OutboxStore.markFailed}
   * which DOES increment attempts.
   */
  backpressureDelayMs?: number;
  /**
   * Multiplier applied to the computed backoff for records rejected with
   * `errorKind: "quota"` (broker is throttling the producer). Default 5 —
   * a quota signal asks for a longer breath than a generic transient error.
   * Quota failures DO count as attempts (unlike backpressure).
   */
  quotaMultiplier?: number;
}

export interface DlqConfig {
  /** Topic to route dead messages to. If absent, dead messages are parked. */
  topic?: string;
  /**
   * Include a truncated stack trace as the `dlq-error-stack` header when
   * routing a record to the DLQ. Default false — keep DLQ messages small
   * by default; opt in if your triage workflow needs the stack.
   */
  includeStackTraces?: boolean;
  /**
   * Maximum bytes of the truncated stack trace included when
   * `includeStackTraces` is on. Default 4096.
   */
  maxStackBytes?: number;
}

/**
 * Optional trace-context propagator. `inject` writes the active trace context
 * into the carrier as W3C `traceparent`/`tracestate` (the shape of an
 * OpenTelemetry TextMapPropagator), so it is persisted with the outbox row and
 * carried to the published message. The library depends on no tracing package;
 * provide a thin adapter over yours (OpenTelemetry, Datadog, …).
 */
export interface Tracing {
  inject(carrier: Record<string, string>): void;
}

/**
 * Minimal structured logger. Console-backed default provided.
 */
export interface Logger {
  debug(msg: string, meta?: Record<string, unknown>): void;
  info(msg: string, meta?: Record<string, unknown>): void;
  warn(msg: string, meta?: Record<string, unknown>): void;
  error(msg: string, meta?: Record<string, unknown>): void;
}

/**
 * A low-latency wake source for the relay. Instead of only waking on the poll
 * interval, the relay claims immediately whenever `onWake` fires. The signal is
 * advisory: it may fire spuriously or be missed entirely, and the relay's
 * polling remains the safety net, so implementations need not deduplicate or
 * guarantee delivery. (e.g. a Postgres LISTEN/NOTIFY waker.)
 */
export interface Waker {
  start(onWake: () => void): Promise<void>;
  stop(): Promise<void>;
}

/**
 * Lifecycle / observability hooks emitted by the relay.
 */
export interface RelayHooks {
  onBatchClaimed?(count: number): void;
  onPublished?(result: PublishResult): void;
  onFailed?(record: OutboxRecord, error: Error, willRetry: boolean): void;
  onDead?(record: OutboxRecord, error: Error): void;
  onError?(error: Error): void;
}
