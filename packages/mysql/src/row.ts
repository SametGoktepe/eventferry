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
    payload: row.payload,
    headers: row.headers ?? {},
    traceId: row.trace_id,
    status,
    attempts: row.attempts,
    nextRetryAt: row.next_retry_at,
    createdAt: row.created_at,
    processedAt: row.processed_at,
  };
}
