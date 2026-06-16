import { randomUUID } from "node:crypto";
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
 * Minimal mysql2/promise query surface — satisfied by both `Pool` and
 * `PoolConnection`, so `enqueue` can run inside a caller-supplied transaction.
 * `query` returns the canonical `[rows, fields]` tuple shape of the driver.
 */
export interface MysqlQueryable {
  query(sql: string, values?: unknown[]): Promise<[unknown, unknown]>;
}

/**
 * Minimal mysql2/promise PoolConnection surface used by {@link MysqlStore.claimBatch}
 * for its internal `BEGIN ... SELECT FOR UPDATE SKIP LOCKED ... UPDATE ... COMMIT`
 * dance — MySQL has no `RETURNING`, so we cannot fold the claim into a single
 * statement the way the Postgres adapter does.
 */
export interface MysqlConnection extends MysqlQueryable {
  beginTransaction(): Promise<void>;
  commit(): Promise<void>;
  rollback(): Promise<void>;
  release(): void;
}

/**
 * Minimal mysql2/promise Pool surface — `query` for stateless ops and
 * `getConnection` for the transactional claim path.
 */
export interface MysqlPool extends MysqlQueryable {
  getConnection(): Promise<MysqlConnection>;
}

export interface MysqlStoreOptions {
  /** A connected mysql2/promise Pool used by the relay for claim/ack queries. */
  pool: MysqlPool;
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
   * (retry due) and timed-out `processing`(1). Reserved for future streaming
   * relay modes that own the pending path. Default false.
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
const PROCESSING = OUTBOX_STATUS_CODE.processing;
const DONE = OUTBOX_STATUS_CODE.done;

export class MysqlStore implements OutboxStore {
  private readonly pool: MysqlPool;
  private readonly table: string;
  private readonly claimTimeoutMs: number;
  private readonly claimFailedOnly: boolean;
  private readonly tracing: Tracing | null;

  constructor(opts: MysqlStoreOptions) {
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
   * @returns the generated (or caller-supplied) message id.
   */
  async enqueue(
    tx: MysqlQueryable,
    msg: OutboxMessageInput & { traceId?: string },
  ): Promise<string> {
    // Copy (never mutate the caller's object) and let tracing capture the
    // active W3C context into the headers, so it rides along to the broker.
    const headers = { ...(msg.headers ?? {}) };
    this.tracing?.inject(headers);

    // MySQL has no UUID-generating default; mint client-side. crypto.randomUUID
    // is RFC 4122 v4 and available in Node 18+ (our minimum engine).
    const messageId = msg.messageId ?? randomUUID();

    const sql = `
      INSERT INTO \`${this.table}\`
        (message_id, aggregate_type, aggregate_id, topic, \`key\`, payload, headers, trace_id, status)
      VALUES
        (?, ?, ?, ?, ?, ?, ?, ?, 0)
    `;
    await tx.query(sql, [
      messageId,
      msg.aggregateType,
      msg.aggregateId,
      msg.topic,
      msg.key ?? null,
      JSON.stringify(msg.payload),
      JSON.stringify(headers),
      msg.traceId ?? null,
    ]);
    return messageId;
  }

  /**
   * Claim up to `batchSize` due rows using FOR UPDATE SKIP LOCKED so that
   * concurrent relay instances never contend for the same rows. Claimed
   * rows are flipped to status=processing(1) and stamped with claimed_at.
   *
   * MySQL has no `RETURNING`, so the claim is a three-step transaction:
   *
   *   1. SELECT due ids FOR UPDATE SKIP LOCKED — the locks are held until COMMIT.
   *   2. UPDATE ... WHERE id IN (...) — atomically flip status + claimed_at.
   *   3. SELECT * WHERE id IN (...) — read the (now updated) rows back.
   *
   * Strict per-aggregate ordering is enforced by only claiming a row when it
   * is the *head* of its aggregate — i.e. no earlier row (lower id) for the
   * same aggregate_id is still unfinished (pending/processing/failed). This
   * guarantees:
   *   - at most one in-flight row per aggregate at any time (across relays),
   *   - a failed row blocks its successors until it is done or dead,
   *   - within a single batch every aggregate appears at most once, so the
   *     broker can never observe two same-key messages out of order.
   *
   * A row stuck in `processing` longer than claimTimeoutMs is treated as due
   * again (its owning relay is presumed dead), which keeps a crash between
   * claim and ack from orphaning messages.
   */
  async claimBatch(batchSize: number): Promise<OutboxRecord[]> {
    const conn = await this.pool.getConnection();
    try {
      await conn.beginTransaction();

      // The reaper window cutoff: any `processing` row claimed before this
      // instant is presumed orphaned and eligible to re-claim.
      const reaperCutoff = new Date(Date.now() - this.claimTimeoutMs);
      const pendingClause = this.claimFailedOnly ? "" : "o.status = 0 OR ";

      const selectDue = `
        SELECT o.id
        FROM \`${this.table}\` o
        WHERE (
              ${pendingClause}(o.status = 3 AND (o.next_retry_at IS NULL OR o.next_retry_at <= NOW(3)))
           OR (o.status = 1 AND o.claimed_at IS NOT NULL AND o.claimed_at <= ?)
        )
        AND NOT EXISTS (
              SELECT 1
              FROM \`${this.table}\` earlier
              WHERE earlier.aggregate_id = o.aggregate_id
                AND earlier.id < o.id
                AND earlier.status IN (0, 1, 3)
        )
        ORDER BY o.id
        LIMIT ?
        FOR UPDATE SKIP LOCKED
      `;
      const [dueRows] = await conn.query(selectDue, [reaperCutoff, batchSize]);
      const ids = (dueRows as Array<{ id: number | string | bigint }>).map(
        (r) => r.id,
      );

      if (ids.length === 0) {
        await conn.commit();
        return [];
      }

      await conn.query(
        `UPDATE \`${this.table}\` SET status = ${PROCESSING}, claimed_at = NOW(3) WHERE id IN (?)`,
        [ids],
      );

      const [rows] = await conn.query(
        `SELECT id, message_id, aggregate_type, aggregate_id, topic, \`key\`,
                payload, headers, trace_id, status, attempts, next_retry_at,
                created_at, processed_at
           FROM \`${this.table}\`
          WHERE id IN (?)
          ORDER BY id`,
        [ids],
      );

      await conn.commit();
      return (rows as unknown as OutboxRow[]).map(rowToRecord);
    } catch (err) {
      try {
        await conn.rollback();
      } catch {
        // Connection may already be dead; let the original error surface.
      }
      throw err;
    } finally {
      conn.release();
    }
  }

  async markDone(recordIds: string[]): Promise<void> {
    if (recordIds.length === 0) return;
    await this.pool.query(
      `UPDATE \`${this.table}\` SET status = ${DONE}, processed_at = NOW(3) WHERE id IN (?)`,
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
      `UPDATE \`${this.table}\`
          SET status = ?,
              attempts = attempts + 1,
              next_retry_at = ?
        WHERE id = ?`,
      [code, nextRetryAt, recordId],
    );
  }

  /**
   * Delete `done` rows whose `processed_at` is older than `olderThanMs`, in
   * batches (to avoid long locks / table bloat). Returns the total deleted.
   * Run periodically (e.g. from a cron) — there is no built-in scheduler.
   * Note: only `done`(2) rows are purged; `dead` rows are left in place for
   * post-mortem.
   */
  async purgeDone(opts: PurgeDoneOptions): Promise<number> {
    const batchSize = opts.batchSize ?? 1000;
    let total = 0;
    for (;;) {
      const cutoff = new Date(Date.now() - opts.olderThanMs);
      const [result] = await this.pool.query(
        `DELETE FROM \`${this.table}\`
          WHERE status = ${DONE}
            AND processed_at IS NOT NULL
            AND processed_at < ?
          ORDER BY id
          LIMIT ?`,
        [cutoff, batchSize],
      );
      const deleted = (result as { affectedRows?: number }).affectedRows ?? 0;
      total += deleted;
      if (deleted < batchSize) break;
      if (opts.maxRows !== undefined && total >= opts.maxRows) break;
    }
    return total;
  }
}
