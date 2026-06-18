import type { OutboxRecord, OutboxStatus } from "@eventferry/core";
import { OUTBOX_STATUS_FROM_CODE } from "@eventferry/core";

/**
 * Raw outbox row shape as read from SQL Server via `mssql`/`tedious`
 * (snake_case columns).
 *
 * Type notes — cross-engine parity story:
 *   - `id` is `string`: tedious returns BIGINT as a JS string by default
 *     (see `lib/value-parser.js`: `value.toString()`). The outbox id can
 *     exceed `2^53`, so it MUST be kept as a string end-to-end. NEVER
 *     `Number(row.id)`.
 *   - `payload` / `headers` come back as `string`: tedious never
 *     auto-parses NVARCHAR(MAX) JSON the way `node-postgres` does for
 *     `jsonb` or `mysql2` does for the native MySQL `JSON` type. Even
 *     when a future opt-in `useNativeJson` migration upgrades the
 *     columns to SQL Server 2025 `json`, the driver still hands back a
 *     string — we always parse on read.
 *   - `status` is `number` (TINYINT 0..4) and is mapped to the
 *     {@link OutboxStatus} string via {@link OUTBOX_STATUS_FROM_CODE}.
 *   - DATETIME2(3) columns surface as `Date | null`. Server-side they
 *     are stamped via `SYSUTCDATETIME()` and the integration suite
 *     verifies UTC round-trip across DST boundaries.
 */
export interface OutboxRow {
  id: string;
  message_id: string;
  aggregate_type: string;
  aggregate_id: string;
  topic: string;
  key: string | null;
  payload: string;
  headers: string | null;
  trace_id: string | null;
  status: number;
  attempts: number;
  next_retry_at: Date | null;
  created_at: Date;
  processed_at: Date | null;
}

/**
 * Map a raw DB row to the broker-agnostic core record.
 *
 * Parity highlights vs. the Postgres / MySQL adapters:
 *   - `id` is passed through as a string. tedious returns BIGINT as a
 *     JS string and the core contract is `string`. NEVER `Number(row.id)`
 *     — outbox ids can exceed `2^53` after enough throughput, and a
 *     silent precision loss there would corrupt `markDone` / `markFailed`
 *     lookups.
 *   - `payload` / `headers` are always re-parsed. SQL Server's
 *     NVARCHAR(MAX) JSON storage (and even the native `json` type when
 *     the `useNativeJson` migration option is set) come back as a
 *     string from tedious — there is no driver-side auto-parse. The
 *     defensive {@link parseJsonField} helper also tolerates a future
 *     driver/world where the value is already an object.
 *   - `status` (TINYINT 0..4) is mapped to the string lifecycle via
 *     {@link OUTBOX_STATUS_FROM_CODE}, falling back to `"pending"` for
 *     any unknown code (forward compatibility).
 */
export function rowToRecord(row: OutboxRow): OutboxRecord {
  const status: OutboxStatus = OUTBOX_STATUS_FROM_CODE[row.status] ?? "pending";
  return {
    id: row.id,
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
 * Defensive JSON parser for fields the driver hands back as a string.
 *
 * Today, `mssql`/`tedious` never auto-parses JSON columns — NVARCHAR(MAX)
 * and the native SQL Server 2025 `json` type both surface as strings on
 * the JS side. Strings get `JSON.parse`'d; objects pass through (covers
 * a future driver release that learns to auto-parse, plus the parity
 * shape used by the MySQL adapter where MySQL 8 returns objects but
 * MariaDB returns strings). `null` / `undefined` pass through.
 *
 * Throws on malformed JSON — a malformed payload in the outbox is a
 * write-side bug and must surface loudly rather than silently degrade
 * a `processing` row.
 */
function parseJsonField<T>(value: unknown): T {
  if (value === null || value === undefined) return value as T;
  if (typeof value === "string") return JSON.parse(value) as T;
  return value as T;
}
