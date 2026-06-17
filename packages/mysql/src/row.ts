import type { OutboxRecord, OutboxStatus } from "@eventferry/core";
import { OUTBOX_STATUS_FROM_CODE } from "@eventferry/core";

/** Raw outbox row shape as read from MySQL (snake_case columns). */
export interface OutboxRow {
  id: number | string | bigint;
  message_id: string;
  aggregate_type: string;
  aggregate_id: string;
  topic: string;
  key: string | null;
  payload: unknown;
  headers: Record<string, string> | null;
  trace_id: string | null;
  status: number;
  attempts: number;
  next_retry_at: Date | null;
  created_at: Date;
  processed_at: Date | null;
}

/**
 * Map a raw DB row to the broker-agnostic core record. `id` is stringified
 * because mysql2 may return BIGINT either as a JS number, string, or bigint
 * depending on driver options — the core contract is `string`.
 *
 * `payload` / `headers` are JSON columns, but driver behavior splits:
 *   - MySQL 8 has a native JSON type, and the `mysql2` driver auto-parses
 *     it to a JS object / array on read.
 *   - MariaDB exposes JSON as a `LONGTEXT` alias with a CHECK constraint —
 *     no native type → the driver returns the raw string.
 * To stay engine-agnostic we re-parse strings here; objects pass through
 * untouched. Belt and suspenders.
 */
export function rowToRecord(row: OutboxRow): OutboxRecord {
  const status: OutboxStatus = OUTBOX_STATUS_FROM_CODE[row.status] ?? "pending";
  return {
    id: String(row.id),
    messageId: row.message_id,
    topic: row.topic,
    aggregateType: row.aggregate_type,
    aggregateId: row.aggregate_id,
    key: row.key,
    payload: parseJsonField<unknown>(row.payload),
    headers: parseJsonField<Record<string, string> | null>(row.headers) ?? {},
    traceId: row.trace_id,
    status,
    attempts: row.attempts,
    nextRetryAt: row.next_retry_at,
    createdAt: row.created_at,
    processedAt: row.processed_at,
  };
}

/**
 * Defensive JSON parser for fields the driver may or may not have parsed
 * (MySQL 8 yes, MariaDB no). Strings get JSON.parse'd; everything else
 * passes through. Throws if the string is malformed — a malformed JSON
 * payload in the outbox would be a write-side bug, surface it loudly.
 */
function parseJsonField<T>(value: unknown): T {
  if (value === null || value === undefined) return value as T;
  if (typeof value === "string") return JSON.parse(value) as T;
  return value as T;
}
