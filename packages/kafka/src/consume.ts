/**
 * Consumer-side helpers — paired with the publisher's outbound surface.
 *
 * eventferry is a publisher-only library, but the messages it produces are
 * consumed somewhere downstream. This module normalizes the message shape
 * that both `kafkajs` and `@confluentinc/kafka-javascript` deliver to
 * consumer callbacks, decodes the payload, and extracts the W3C trace
 * context the publisher injected (see {@link KafkaTracer.inject}).
 *
 * Imported via subpath so consumer code paths don't pull in the producer:
 *
 *   import { decode, extractTraceContext } from "@eventferry/kafka/consume";
 *
 * There is intentionally NO Kafka client here — bring your own consumer
 * (kafkajs's `Consumer`, librdkafka's, whatever) and call `decode()` /
 * `extractTraceContext()` on the message you receive.
 */

/**
 * Raw incoming Kafka message — structural subset both kafkajs and confluent
 * (via the kafkaJS-compat layer) deliver. Fields are optional because
 * different consumer APIs surface different subsets.
 */
export interface IncomingKafkaMessage {
  key?: Buffer | string | null;
  value?: Buffer | string | null;
  headers?: IncomingHeaders;
  /** ISO ms string or numeric epoch ms — depends on the client. */
  timestamp?: string | number;
  /** Numeric or string per client. */
  offset?: string | number;
  partition?: number;
}

/** Headers as the underlying clients deliver them: bytes, strings, or undefined. */
export type IncomingHeaders = Record<string, Buffer | string | undefined>;

/** Headers normalized to UTF-8 strings (the form most application code wants). */
export type DecodedHeaders = Record<string, string>;

/** Payload decoder. Buffer in, decoded value out. */
export type Decoder<T> = (bytes: Buffer) => T;

/** Decoded message wrapper — value plus normalized headers, key, metadata. */
export interface DecodedMessage<V = unknown> {
  key: string | null;
  value: V | null;
  headers: DecodedHeaders;
  /** Epoch ms when the broker stamped the record. */
  timestamp?: number;
  /** Stringified offset (Kafka offsets exceed 2^53 — strings stay safe). */
  offset?: string;
  partition?: number;
}

export interface DecodeOptions<V> {
  /**
   * Decoder for the payload bytes. Built-ins:
   *
   * - `"json"` (default) — `JSON.parse(value.toString("utf8"))`. Empty
   *   value returns `null` (matches Kafka tombstones on compacted topics).
   * - `"utf8"` — raw text. Returns the string as-is.
   * - `"none"` — returns the raw `Buffer` unchanged.
   *
   * Or pass your own `(bytes: Buffer) => V` for Avro / Protobuf / MessagePack.
   */
  decoder?: "json" | "utf8" | "none" | Decoder<V>;
}

/**
 * Normalize headers to a plain string→string map. Buffers are read as UTF-8;
 * `undefined` entries are dropped (consumers occasionally surface absent
 * headers as `undefined` values).
 */
export function decodeHeaders(raw?: IncomingHeaders): DecodedHeaders {
  if (!raw) return {};
  const out: DecodedHeaders = {};
  for (const [k, v] of Object.entries(raw)) {
    if (v === undefined || v === null) continue;
    out[k] = Buffer.isBuffer(v) ? v.toString("utf8") : v;
  }
  return out;
}

/**
 * Decode a Kafka message: normalize the key + headers, decode the value
 * with the chosen decoder, and surface the broker metadata.
 *
 * Tombstones (null/empty value) come back with `value: null` regardless of
 * the decoder — compaction-friendly.
 *
 * @throws when `decoder: "json"` (the default) and the payload is non-empty
 *   but not valid JSON. Catch the error and decide whether to DLQ the
 *   record or skip it — eventferry does not assume.
 */
export function decode<V = unknown>(
  msg: IncomingKafkaMessage,
  opts: DecodeOptions<V> = {},
): DecodedMessage<V> {
  const headers = decodeHeaders(msg.headers);
  const key = normalizeKey(msg.key);
  const value = decodeValue<V>(msg.value, opts.decoder ?? "json");
  const timestamp =
    msg.timestamp !== undefined ? Number(msg.timestamp) : undefined;
  const offset = msg.offset !== undefined ? String(msg.offset) : undefined;
  return {
    key,
    value,
    headers,
    timestamp,
    offset,
    partition: msg.partition,
  };
}

function normalizeKey(key?: Buffer | string | null): string | null {
  if (key === null || key === undefined) return null;
  return Buffer.isBuffer(key) ? key.toString("utf8") : key;
}

function decodeValue<V>(
  value: Buffer | string | null | undefined,
  decoder: DecodeOptions<V>["decoder"],
): V | null {
  if (value === null || value === undefined) return null;
  const buf = Buffer.isBuffer(value) ? value : Buffer.from(value);
  // Tombstones: an empty buffer is a kafka "delete me" on compacted
  // topics. Surface as null for every decoder — applications usually
  // want the same null-handling for both.
  if (buf.length === 0) return null;
  if (typeof decoder === "function") return decoder(buf);
  switch (decoder) {
    case "utf8":
      return buf.toString("utf8") as unknown as V;
    case "none":
      return buf as unknown as V;
    case "json":
    case undefined:
    default: {
      const text = buf.toString("utf8");
      try {
        return JSON.parse(text) as V;
      } catch (err) {
        throw new Error(
          `decode: JSON.parse failed on message value: ${(err as Error).message}`,
        );
      }
    }
  }
}

/**
 * W3C Trace Context extracted from message headers.
 *
 * - `traceparent`: full header value, format `version-traceId-spanId-flags`.
 * - `tracestate`: optional vendor-specific state (W3C `tracestate` header).
 * - `traceId`: 32 hex chars, parsed from `traceparent`.
 * - `spanId`: 16 hex chars (the PARENT span id from the producer).
 * - `sampled`: parsed from the `traceparent` flags (bit 0 = sampled).
 *
 * Returns `null` when no `traceparent` header is present or the value
 * fails W3C validation.
 *
 * Spec: https://www.w3.org/TR/trace-context/
 */
export interface TraceContext {
  traceparent: string;
  tracestate?: string;
  traceId: string;
  spanId: string;
  sampled: boolean;
}

const TRACEPARENT_RE =
  /^([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$/;
const INVALID_TRACE_ID = "0".repeat(32);
const INVALID_SPAN_ID = "0".repeat(16);

/**
 * Extract the W3C trace context the publisher injected into headers.
 * Headers may be raw (Buffer values) or already-decoded (string values) —
 * both shapes work, so you can call this before OR after `decode()`.
 *
 * Validation follows the W3C spec strictly: invalid all-zero trace/span
 * IDs are rejected, version `ff` is rejected, malformed hex is rejected.
 * On any of these, the function returns `null` rather than throwing —
 * consumer code should fall back to starting a fresh trace.
 */
export function extractTraceContext(
  headers: IncomingHeaders | DecodedHeaders | undefined,
): TraceContext | null {
  if (!headers) return null;
  const tp = readHeader(headers, "traceparent");
  if (!tp) return null;
  const match = TRACEPARENT_RE.exec(tp);
  if (!match) return null;
  const [, version, traceId, spanId, flags] = match as unknown as [
    string,
    string,
    string,
    string,
    string,
  ];
  // Spec §3.2.2.5: version "ff" is forbidden (reserved sentinel).
  if (version === "ff") return null;
  if (traceId === INVALID_TRACE_ID || spanId === INVALID_SPAN_ID) return null;
  const sampled = (parseInt(flags, 16) & 0x01) === 1;
  const ts = readHeader(headers, "tracestate");
  return {
    traceparent: tp,
    tracestate: ts && ts.length > 0 ? ts : undefined,
    traceId,
    spanId,
    sampled,
  };
}

function readHeader(
  headers: IncomingHeaders | DecodedHeaders,
  name: string,
): string | undefined {
  const v = (headers as Record<string, Buffer | string | undefined>)[name];
  if (v === undefined || v === null) return undefined;
  if (typeof v === "string") return v;
  if (Buffer.isBuffer(v)) return v.toString("utf8");
  return undefined;
}
