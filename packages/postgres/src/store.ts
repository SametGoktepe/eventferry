import type {
  OutboxMessageInput,
  OutboxRecord,
  OutboxStore,
  Tracing,
} from "@eventferry/core";
import { OUTBOX_STATUS_CODE } from "@eventferry/core";
import { assertIdent } from "./ident.js";
import { rowToRecord, type OutboxRow } from "./row.js";

/**
 * Minimal query interface satisfied by both `pg.Pool` and `pg.PoolClient`,
 * so `enqueue` can run inside a caller-supplied transaction.
 */
export interface Queryable {
  query(
    queryText: string,
    values?: unknown[],
  ): Promise<{ rows: Record<string, unknown>[] }>;
}

export interface PostgresStoreOptions {
  /** A connected pg Pool used by the relay for claim/ack queries. */
  pool: Queryable;
  /** Outbox table name. Default "outbox". */
  table?: string;
  /**
   * Visibility timeout (ms) after which a row stuck in `processing` is
   * reclaimable by any relay. Guards against permanently-orphaned rows when
   * a relay crashes between claiming and acking. MUST be comfortably larger
   * than the worst-case publish latency, otherwise a slow-but-alive relay's
   * in-flight rows get reclaimed and re-published (a duplicate). Default 60s.
   */
  claimTimeoutMs?: number;
  /**
   * When true, `claimBatch` never claims `pending`(0) rows — only `failed`(3)
   * (retry due) and timed-out `processing`(1). Used by the streaming relay,
   * where pending rows are owned by the WAL stream and the claim loop only
   * drains failures. Default false (claims pending too).
   */
  claimFailedOnly?: boolean;
  /**
   * Optional trace-context propagator. When set, `enqueue` captures the active
   * W3C trace context into the row's headers, so it rides along to the published
   * message and the consumer can continue the trace. See {@link Tracing}.
   */
  tracing?: Tracing;
}

export interface PurgeDoneOptions {
  /** Delete `done` rows whose processed_at is older than this (milliseconds). */
  olderThanMs: number;
  /** Rows deleted per batch. Default 1000. */
  batchSize?: number;
  /** Optional cap on the total rows deleted in this call. */
  maxRows?: number;
}

const DEFAULT_CLAIM_TIMEOUT_MS = 60_000;

export class PostgresStore implements OutboxStore {
  private readonly pool: Queryable;
  private readonly table: string;
  private readonly claimTimeoutMs: number;
  private readonly claimFailedOnly: boolean;
  private readonly tracing: Tracing | null;

  constructor(opts: PostgresStoreOptions) {
    this.pool = opts.pool;
    this.table = assertIdent(opts.table ?? "outbox");
    this.claimTimeoutMs = opts.claimTimeoutMs ?? DEFAULT_CLAIM_TIMEOUT_MS;
    this.claimFailedOnly = opts.claimFailedOnly ?? false;
    this.tracing = opts.tracing ?? null;
  }

  /**
   * Insert a message into the outbox. MUST be called with the same
   * transaction (`tx`) that persists the business state, so the event
   * and the state commit atomically.
   *
   * @returns the generated message id.
   */
  async enqueue(
    tx: Queryable,
    msg: OutboxMessageInput & { traceId?: string },
  ): Promise<string> {
    // Copy (never mutate the caller's object) and let tracing capture the
    // active W3C context into the headers, so it rides along to the broker.
    const headers = { ...(msg.headers ?? {}) };
    this.tracing?.inject(headers);

    const text = `
      INSERT INTO ${this.table}
        (message_id, aggregate_type, aggregate_id, topic, "key", payload, headers, trace_id, status)
      VALUES
        (COALESCE($1, gen_random_uuid()), $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8, 0)
      RETURNING message_id
    `;
    const res = await tx.query(text, [
      msg.messageId ?? null,
      msg.aggregateType,
      msg.aggregateId,
      msg.topic,
      msg.key ?? null,
      JSON.stringify(msg.payload),
      JSON.stringify(headers),
      msg.traceId ?? null,
    ]);
    return res.rows[0]?.message_id as string;
  }

  /**
   * Claim up to `batchSize` due rows using FOR UPDATE SKIP LOCKED so that
   * concurrent relay instances never contend for the same rows. Claimed
   * rows are flipped to status=processing(1) and stamped with claimed_at,
   * atomically in the same CTE.
   *
   * Strict per-aggregate ordering is enforced by only claiming a row when it
   * is the *head* of its aggregate — i.e. no earlier row (lower id) for the
   * same aggregate_id is still unfinished (pending/processing/failed). This
   * guarantees:
   *   - at most one in-flight row per aggregate at any time (across relays),
   *   - a failed row blocks its successors until it is done or dead,
   *   - within a single batch every aggregate appears at most once, so the
   *     broker can never observe two same-key messages out of order.
   * `done`(2) and `dead`(4) are terminal and stop blocking successors, so a
   * poison message routed to the DLQ does not stall its aggregate forever.
   *
   * A row stuck in `processing` longer than claimTimeoutMs is treated as due
   * again (its owning relay is presumed dead), which is what keeps a crash
   * between claim and ack from orphaning messages.
   *
   * $1 = batchSize, $2 = claimTimeoutMs.
   */
  async claimBatch(batchSize: number): Promise<OutboxRecord[]> {
    const processing = OUTBOX_STATUS_CODE.processing;
    // Streaming mode (claimFailedOnly) leaves pending(0) rows to the WAL stream.
    const pendingClause = this.claimFailedOnly ? "" : "o.status = 0\n           OR ";
    const text = `
      WITH due AS (
        SELECT o.id
        FROM ${this.table} o
        WHERE (
              ${pendingClause}(o.status = 3 AND (o.next_retry_at IS NULL OR o.next_retry_at <= now()))
           OR (o.status = 1 AND o.claimed_at IS NOT NULL
                 AND o.claimed_at <= now() - ($2 * interval '1 millisecond'))
        )
        AND NOT EXISTS (
              SELECT 1
              FROM ${this.table} earlier
              WHERE earlier.aggregate_id = o.aggregate_id
                AND earlier.id < o.id
                AND earlier.status IN (0, 1, 3)
        )
        ORDER BY o.id
        LIMIT $1
        FOR UPDATE SKIP LOCKED
      )
      UPDATE ${this.table} AS o
      SET status = ${processing}, claimed_at = now()
      FROM due
      WHERE o.id = due.id
      RETURNING o.id, o.message_id, o.aggregate_type, o.aggregate_id,
                o.topic, o."key", o.payload, o.headers, o.trace_id,
                o.status, o.attempts, o.next_retry_at, o.created_at, o.processed_at
    `;
    const res = await this.pool.query(text, [batchSize, this.claimTimeoutMs]);
    return (res.rows as unknown as OutboxRow[]).map(rowToRecord);
  }

  async markDone(recordIds: string[]): Promise<void> {
    if (recordIds.length === 0) return;
    const done = OUTBOX_STATUS_CODE.done;
    await this.pool.query(
      `UPDATE ${this.table}
         SET status = ${done}, processed_at = now()
       WHERE id = ANY($1::bigint[])`,
      [recordIds],
    );
  }

  async markFailed(
    recordId: string,
    nextRetryAt: Date | null,
    status: "failed" | "dead",
  ): Promise<void> {
    const code = OUTBOX_STATUS_CODE[status];
    await this.pool.query(
      `UPDATE ${this.table}
         SET status = $2,
             attempts = attempts + 1,
             next_retry_at = $3
       WHERE id = $1`,
      [recordId, code, nextRetryAt],
    );
  }

  /**
   * Re-queue a record to `failed` with the given `retryAt` **without
   * bumping attempts** — used by the relay for backpressure handling.
   * Also clears `claimed_at` so the reaper does not race the row.
   */
  async requeue(recordId: string, retryAt: Date): Promise<void> {
    const failed = OUTBOX_STATUS_CODE.failed;
    await this.pool.query(
      `UPDATE ${this.table}
         SET status = ${failed},
             claimed_at = NULL,
             next_retry_at = $2
       WHERE id = $1`,
      [recordId, retryAt],
    );
  }

  /**
   * Delete `done` rows whose `processed_at` is older than `olderThanMs`, in
   * batches (to avoid long locks / table bloat). Returns the total deleted.
   * Run it periodically (e.g. from a cron) — there is no built-in scheduler.
   * Note: only `done`(2) rows are purged; `dead` rows are left in place.
   */
  async purgeDone(opts: PurgeDoneOptions): Promise<number> {
    const done = OUTBOX_STATUS_CODE.done;
    const batchSize = opts.batchSize ?? 1000;
    const text = `
      DELETE FROM ${this.table}
      WHERE id IN (
        SELECT id FROM ${this.table}
        WHERE status = ${done}
          AND processed_at IS NOT NULL
          AND processed_at < now() - ($1 * interval '1 millisecond')
        ORDER BY id
        LIMIT $2
      )
      RETURNING id
    `;
    let total = 0;
    for (;;) {
      const res = await this.pool.query(text, [opts.olderThanMs, batchSize]);
      total += res.rows.length;
      if (res.rows.length < batchSize) break;
      if (opts.maxRows !== undefined && total >= opts.maxRows) break;
    }
    return total;
  }
}
