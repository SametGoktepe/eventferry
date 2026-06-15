import type { OutboxRecord, OutboxStatus } from "@eventferry/core";
import { OUTBOX_STATUS_FROM_CODE } from "@eventferry/core";

/** Raw outbox row shape as read from Postgres (snake_case columns). */
export interface OutboxRow {
  id: string;
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

/** Map a raw DB row to the broker-agnostic core record. */
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
