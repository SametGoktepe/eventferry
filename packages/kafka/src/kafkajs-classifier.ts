import type { PublishErrorKind } from "@eventferry/core";

/**
 * Classify a kafkajs producer error into a {@link PublishErrorKind} so the
 * core relay can decide whether to retry, short-circuit to the DLQ, or pause
 * polling.
 *
 * Mapping verified against `kafkajs/src/errors.js` (v2.x). Protocol error
 * codes match the Kafka Protocol error-code registry. Library-specific
 * subclasses (`KafkaJSRequestTimeoutError`, `KafkaJSConnectionError`,
 * `KafkaJSNonRetriableError`) are matched by the `name` property kafkajs
 * sets on its Error subclasses.
 *
 * Unknown errors fall back to `"retriable"` — the safe bias. At worst we
 * retry an error that should have been skipped; in practice we'd rather
 * over-retry than mis-classify a transient blip as terminal.
 */
export function classifyKafkajsError(err: unknown): PublishErrorKind {
  if (!err || typeof err !== "object") return "retriable";
  const e = err as { name?: string; type?: string; code?: number };

  // Class-based first — these don't carry a protocol error code.
  if (e.name === "KafkaJSConnectionError") return "retriable";
  if (e.name === "KafkaJSRequestTimeoutError") return "retriable";
  if (e.name === "KafkaJSNonRetriableError") return "fatal";

  // Protocol error type (string) — kafkajs's KafkaJSProtocolError exposes
  // both `type` (uppercase string) and `code` (number). Use `type` first
  // for readability and fall back to `code` for codes that lack a stable
  // string label.
  const type = typeof e.type === "string" ? e.type : undefined;
  if (type) {
    if (RETRIABLE_TYPES.has(type)) return "retriable";
    if (POISON_TYPES.has(type)) return "poison";
    if (FATAL_TYPES.has(type)) return "fatal";
  }

  if (typeof e.code === "number") {
    const k = CODE_TO_KIND.get(e.code);
    if (k) return k;
  }

  return "retriable";
}

const RETRIABLE_TYPES = new Set<string>([
  "NOT_LEADER_FOR_PARTITION",
  "LEADER_NOT_AVAILABLE",
  "UNKNOWN_TOPIC_OR_PARTITION",
  "NETWORK_EXCEPTION",
  "REQUEST_TIMED_OUT",
  "REPLICA_NOT_AVAILABLE",
  "NOT_ENOUGH_REPLICAS",
  "NOT_ENOUGH_REPLICAS_AFTER_APPEND",
  "FENCED_LEADER_EPOCH",
  "UNKNOWN_LEADER_EPOCH",
  "BROKER_NOT_AVAILABLE",
  "COORDINATOR_LOAD_IN_PROGRESS",
  "COORDINATOR_NOT_AVAILABLE",
]);

const POISON_TYPES = new Set<string>([
  "CORRUPT_MESSAGE",
  "MESSAGE_TOO_LARGE",
  "INVALID_RECORD",
  "UNSUPPORTED_COMPRESSION_TYPE",
  "INVALID_REQUIRED_ACKS",
  "INVALID_PARTITIONS",
]);

const FATAL_TYPES = new Set<string>([
  "INVALID_PRODUCER_EPOCH",
  "PRODUCER_FENCED",
  "TOPIC_AUTHORIZATION_FAILED",
  "CLUSTER_AUTHORIZATION_FAILED",
  "TRANSACTIONAL_ID_AUTHORIZATION_FAILED",
  "SASL_AUTHENTICATION_FAILED",
  "INVALID_TRANSACTION_STATE",
  "UNSUPPORTED_VERSION",
]);

/** Numeric fallback for clusters that only return the wire code. */
const CODE_TO_KIND: ReadonlyMap<number, PublishErrorKind> = new Map([
  [2, "poison"], // CORRUPT_MESSAGE
  [3, "retriable"], // UNKNOWN_TOPIC_OR_PARTITION
  [5, "retriable"], // LEADER_NOT_AVAILABLE
  [6, "retriable"], // NOT_LEADER_FOR_PARTITION
  [7, "retriable"], // REQUEST_TIMED_OUT
  [9, "retriable"], // REPLICA_NOT_AVAILABLE
  [10, "poison"], // MESSAGE_TOO_LARGE
  [13, "retriable"], // NETWORK_EXCEPTION
  [19, "retriable"], // NOT_ENOUGH_REPLICAS
  [29, "fatal"], // TOPIC_AUTHORIZATION_FAILED
  [31, "fatal"], // CLUSTER_AUTHORIZATION_FAILED
  [47, "fatal"], // INVALID_PRODUCER_EPOCH
  [58, "fatal"], // SASL_AUTHENTICATION_FAILED
  [74, "retriable"], // FENCED_LEADER_EPOCH
  [76, "poison"], // UNSUPPORTED_COMPRESSION_TYPE
  [87, "poison"], // INVALID_RECORD
]);
