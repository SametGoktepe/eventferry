import type { PublishErrorKind } from "@eventferry/core";

/**
 * Classify a `@confluentinc/kafka-javascript` (librdkafka) producer error
 * into a {@link PublishErrorKind} so the core relay can decide whether to
 * retry, short-circuit to the DLQ, or pause polling.
 *
 * librdkafka exposes errors as numeric `RD_KAFKA_RESP_ERR_*` codes — negative
 * codes are library-internal (transport, queue-full, ssl), non-negative
 * codes are wire-protocol errors that match the Kafka protocol's error-code
 * registry. The confluent driver surfaces these on `err.code` (alongside
 * an `err.name` for the symbolic form).
 *
 * Unknown errors fall back to `"retriable"` — the safe bias.
 */
export function classifyConfluentError(err: unknown): PublishErrorKind {
  if (!err || typeof err !== "object") return "retriable";
  const e = err as { code?: number; name?: string };

  if (typeof e.code === "number") {
    const k = CODE_TO_KIND.get(e.code);
    if (k) return k;
  }

  if (typeof e.name === "string") {
    const k = NAME_TO_KIND.get(e.name);
    if (k) return k;
  }

  return "retriable";
}

/**
 * Authoritative mapping for the most-common librdkafka producer error codes.
 * Sources: `librdkafka/src/rdkafka.h` (`RD_KAFKA_RESP_ERR_*` enum) and the
 * Kafka Protocol error-code registry. Adding a code here is a one-line
 * change — start narrow, broaden as production exposes new codes.
 */
const CODE_TO_KIND: ReadonlyMap<number, PublishErrorKind> = new Map([
  // Library-internal (negative codes)
  [-184, "backpressure"], // ERR__QUEUE_FULL — our outbound buffer is full
  [-185, "retriable"], // ERR__TIMED_OUT
  [-187, "retriable"], // ERR__ALL_BROKERS_DOWN
  [-188, "poison"], // ERR__UNKNOWN_TOPIC — topic doesn't exist on broker
  [-190, "poison"], // ERR__UNKNOWN_PARTITION
  [-192, "retriable"], // ERR__MSG_TIMED_OUT
  [-195, "retriable"], // ERR__TRANSPORT
  [-198, "poison"], // ERR__BAD_COMPRESSION
  [-144, "fatal"], // ERR__FENCED — producer fenced by another with same txn id
  [-150, "fatal"], // ERR__FATAL — unrecoverable librdkafka error
  [-169, "fatal"], // ERR__AUTHENTICATION
  [-181, "fatal"], // ERR__SSL
  [-196, "retriable"], // ERR__FAIL — catch-all, safe-default to retriable

  // Wire-protocol (non-negative codes — Kafka error-code registry)
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
  [89, "quota"], // THROTTLING_QUOTA_EXCEEDED
]);

/** Symbolic name fallback for clients that surface `err.name` only. */
const NAME_TO_KIND: ReadonlyMap<string, PublishErrorKind> = new Map([
  ["ERR__QUEUE_FULL", "backpressure"],
  ["ERR__FENCED", "fatal"],
  ["ERR__FATAL", "fatal"],
  ["ERR__AUTHENTICATION", "fatal"],
  ["ERR__SSL", "fatal"],
  ["ERR__UNKNOWN_TOPIC", "poison"],
  ["ERR__UNKNOWN_PARTITION", "poison"],
  ["ERR__BAD_COMPRESSION", "poison"],
  ["ERR_TOPIC_AUTHORIZATION_FAILED", "fatal"],
  ["ERR_CLUSTER_AUTHORIZATION_FAILED", "fatal"],
  ["ERR_INVALID_PRODUCER_EPOCH", "fatal"],
  ["ERR_SASL_AUTHENTICATION_FAILED", "fatal"],
  ["ERR_CORRUPT_MESSAGE", "poison"],
  ["ERR_MSG_SIZE_TOO_LARGE", "poison"],
  ["ERR_INVALID_RECORD", "poison"],
  ["ERR_UNSUPPORTED_COMPRESSION_TYPE", "poison"],
  ["ERR_THROTTLING_QUOTA_EXCEEDED", "quota"],
]);
