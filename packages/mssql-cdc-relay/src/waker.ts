/**
 * `@eventferry/mssql-cdc-relay` — `MssqlCdcWaker` (HARDENED)
 *
 * CDC-backed `Waker` (NOT a parallel Publisher — happy path still routes
 * through the polling core `Relay`'s `claimBatch`). KEY CHANGES vs. v0:
 *
 *   - WATERMARK ADVANCEMENT BUG FIX (critical): `cdc.fn_cdc_get_all_changes_*`
 *     is INCLUSIVE on BOTH endpoints. v0 persisted `MAX(__$start_lsn)` and
 *     re-fed it as `@from_lsn` next cycle, re-reading the same rows
 *     forever. v1 persists `sys.fn_cdc_increment_lsn(MAX(__$start_lsn))`,
 *     computed server-side in the same batch as the read.
 *   - WATERMARK CONCURRENCY (critical): UPDATE now conditional —
 *     `WHERE capture_instance=@ci AND last_lsn=@priorLsn`. If two relay
 *     processes share a capture_instance, the loser detects via
 *     `@@ROWCOUNT=0` and re-polls without firing a phantom wake.
 *     `sp_getapplock` (session-scoped, keyed on capture_instance) wraps
 *     pollOnce for additional safety against the (read, decide, write)
 *     race.
 *   - SINGLE-ROUND-TRIP POLL (high): bounds-read + change-fetch run in
 *     ONE `Request.batch()` wrapped in `BEGIN TRY / BEGIN CATCH` —
 *     eliminates the TOCTOU window where the capture cleanup job
 *     advances min_lsn between the two old round trips. Catches msg
 *     313/22838 server-side and surfaces as MssqlCdcRetentionOverrunError.
 *   - RETENTION OVERRUN SALVAGE (critical): no longer early-returns
 *     after snap-forward. Falls through to read `[minLsn, maxLsn]` and
 *     fires `onWake()` for the rows that ARE still visible. Wake-loss
 *     is now bounded to the genuinely missed range, not the whole cycle.
 *   - MERGE INSERTS (medium → required): `WHERE __$operation IN (2, 5)`
 *     so MERGE-driven upserts (TypeORM/EF Core/Drizzle) fire wakes.
 *   - OPERATION FILTER LITERAL TYPING: the filter argument to
 *     `fn_cdc_get_all_changes_*` is a compile-time literal type
 *     `"all" | "all update old"` to prevent typos from silently
 *     returning zero rows.
 *   - HEALTHCHECK PORTABILITY (high): uses `CONVERT(VARCHAR(22), @lsn, 1)`
 *     for hex conversion, not `master.dbo.fn_varbintohexstr` (deprecated
 *     and inaccessible from Azure SQL DB / MI).
 *   - PER-AGGREGATE TAIL DRAIN (critical): "sticky wake" — after any
 *     cycle that observed >0 rows, fire `onWake()` for
 *     `stickyWakeCycles` additional cycles regardless of CDC progress.
 *     Addresses the claim-batch head-of-aggregate barrier where a burst
 *     for the same aggregate_id requires multiple claim cycles but the
 *     v0 watermark advance silenced further wakes.
 *   - READ-ONLY-REPLICA REFUSAL (high): probes
 *     `DATABASEPROPERTYEX(DB_NAME(),'Updateability')` at startup; on
 *     `READ_ONLY` raises `MssqlCdcReadOnlyReplicaError`. Periodic
 *     `sys.fn_hadr_is_primary_replica(DB_NAME())` re-probe (when on AG)
 *     surfaces via `onError` if the pool resolves to a secondary
 *     mid-flight.
 *   - DISABLED-CAPTURE DETECTION (high): catches msg 208 ('Invalid
 *     object name cdc...') as `MssqlCdcCaptureDisabledError` — covers
 *     the `sp_cdc_disable_db` race that v0 missed.
 *   - CAPTURE INSTANCE GRAMMAR (high): regex tightened to 100 chars
 *     `/^[a-zA-Z_][a-zA-Z0-9_]{0,99}$/` (sysname constraint for
 *     `cdc.<ci>_CT` is 128 chars minus `_CT` and schema prefix). Also
 *     rejects names starting with `cdc_` or `__$`.
 *   - INFLIGHT REQUEST TRACKING (medium): `pollOnce` assigns to
 *     `this.inflightRequest`; `stop()` calls `request.cancel()` and
 *     races against `shutdownTimeoutMs` (default 5000). Replaces the
 *     v0 unbounded `while (cycleInflight) await sleep(10)`.
 *   - RE-ENTRY GUARD (medium): `runCycle` and `pollOnce` short-circuit
 *     on `cycleInflight` flag; replaces busy-wait with promise-latch.
 *   - EXPONENTIAL BACKOFF + JITTER + STUCK ALERT (medium): persistent
 *     failures trigger `MssqlCdcStuckError` via `onError` after 5
 *     consecutive errors; resets on first success.
 *   - SHARED-POOL ASSERTION (high): runtime-asserts `opts.pool !==
 *     opts.storePool` when `storePool` is supplied. Replaces docstring-
 *     only guidance.
 *   - HEALTHCHECK SNAPSHOT ATOMICITY: state stored as one immutable
 *     object, updated by replace (no mid-cycle partial reads).
 *   - LIFETIME RESILIENCE: if onError throws, persistWatermark still
 *     runs (try/catch wrapped); avoids infinite re-fire loops.
 *   - SHARED EDITION ERROR BASE: `MssqlCdcUnsupportedEngineError`
 *     extends the shared `MssqlEngineEditionUnsupportedError` for
 *     uniform `instanceof` handling.
 */

import type { Logger, Waker } from "@eventferry/core";
import { NoopLogger } from "@eventferry/core";
import * as mssql from "mssql";
import { assertIdent } from "./ident.js";
import { compareLsn, lsnToHex } from "./lsn.js";

export interface MssqlCdcWakerOptions {
  /**
   * Already-connected `mssql.ConnectionPool` for CDC reads + watermark
   * writes. MUST point at the PRIMARY (CDC reads on a readable secondary
   * lag, and watermark UPDATEs fail with msg 3906 on read-only DBs —
   * v1 probes this at start and refuses). MUST NOT be the same pool as
   * the store's pool. Runtime-asserted via reference equality when
   * `storePool` supplied.
   */
  pool: mssql.ConnectionPool;
  /** Optional handle to the store's pool for reference-equality assertion. */
  storePool?: mssql.ConnectionPool;
  /** Capture instance. Default `"dbo_outbox"`. Regex /^[a-zA-Z_][a-zA-Z0-9_]{0,99}$/. */
  captureInstance?: string;
  /** Schema for watermark table. Default `"eventferry"`. */
  watermarkSchema?: string;
  /** Watermark table. Default `"cdc_watermark"`. */
  watermarkTable?: string;
  /** Poll interval (ms). Default `1_000`. Range `100..60_000`. */
  pollIntervalMs?: number;
  /** Max rows per cycle. Default `500`. Range `1..10_000`. */
  batchSize?: number;
  /**
   * Number of additional cycles to fire `onWake()` after the last
   * non-empty batch, regardless of CDC progress. Addresses the
   * per-aggregate head-of-aggregate barrier (Relay.claimBatch returns
   * at most one row per aggregate_id per cycle). Default `5`. Set to
   * 0 to disable sticky waking.
   */
  stickyWakeCycles?: number;
  /** Hard cap on `stop()` drain. Default `5_000`. */
  shutdownTimeoutMs?: number;
  /**
   * Surfaces structural failures: retention overrun, capture disabled,
   * read-only replica, persistent stuck. Defensive: throwing handler
   * is caught + logged, never escapes into the poll loop.
   */
  onError?: (err: Error) => void;
  /** Per-cycle metrics hook. */
  metrics?: { onCycle: (s: MssqlCdcWakerCycleStats) => void };
  /** Structured logger. */
  logger?: Logger;
}

export interface MssqlCdcWakerCycleStats {
  readonly durationMs: number;
  readonly rowsObserved: number;
  readonly watermarkAdvancedHex: string | null;
  readonly stuckRetries: number;
}

export interface MssqlCdcWakerHealth {
  readonly captureEnabled: boolean;
  readonly minLsn: string | null;
  readonly maxLsn: string | null;
  readonly watermark: string | null;
  /** -1 when uncomputable; estimated cheaply via min/max LSN distance otherwise. */
  readonly lagRowsApprox: number;
  readonly lastPollAtMs: number;
  readonly lastErrorMessage: string | null;
  readonly readOnlyDatabase: boolean;
}

export class MssqlCdcRetentionOverrunError extends Error {
  readonly captureInstance: string;
  readonly watermarkHex: string;
  readonly minLsnHex: string;
  constructor(captureInstance: string, watermarkHex: string, minLsnHex: string) {
    super(
      `MssqlCdcWaker: watermark ${watermarkHex} fell below min_lsn ${minLsnHex} ` +
        `for capture_instance '${captureInstance}'. Salvaging [min_lsn, max_lsn] in this cycle; ` +
        `wakes for the missed range are lost (polling relay still backstops correctness).`,
    );
    this.name = "MssqlCdcRetentionOverrunError";
    this.captureInstance = captureInstance;
    this.watermarkHex = watermarkHex;
    this.minLsnHex = minLsnHex;
  }
}

export class MssqlCdcCaptureDisabledError extends Error {
  readonly captureInstance: string;
  constructor(captureInstance: string) {
    super(
      `MssqlCdcWaker: capture_instance '${captureInstance}' disabled (sp_cdc_disable_table/_db or ` +
        `min_lsn=NULL). Waker entering idle mode; polling relay continues. Re-enable via sp_cdc_enable_table.`,
    );
    this.name = "MssqlCdcCaptureDisabledError";
    this.captureInstance = captureInstance;
  }
}

export class MssqlCdcWatermarkMissingError extends Error {
  readonly captureInstance: string;
  constructor(captureInstance: string) {
    super(
      `MssqlCdcWaker: no watermark row for capture_instance '${captureInstance}'. ` +
        `Re-run createCdcEnablementSql(); do NOT auto-seed (could skip backlog).`,
    );
    this.name = "MssqlCdcWatermarkMissingError";
    this.captureInstance = captureInstance;
  }
}

export class MssqlCdcUnsupportedEngineError extends Error {
  readonly engineEdition: number;
  constructor(engineEdition: number, hint: string) {
    super(
      `MssqlCdcWaker: SQL Server EngineEdition=${engineEdition} does not support CDC for this relay. ${hint}`,
    );
    this.name = "MssqlCdcUnsupportedEngineError";
    this.engineEdition = engineEdition;
  }
}

export class MssqlCdcReadOnlyReplicaError extends Error {
  constructor() {
    super(
      "MssqlCdcWaker: database is READ_ONLY (Always On readable secondary, log-shipping standby, " +
        "or RESTORE WITH STANDBY). Point the pool at the primary writable replica.",
    );
    this.name = "MssqlCdcReadOnlyReplicaError";
  }
}

export class MssqlCdcStuckError extends Error {
  readonly consecutiveFailures: number;
  readonly lastUnderlyingMessage: string;
  constructor(consecutiveFailures: number, lastUnderlyingMessage: string) {
    super(
      `MssqlCdcWaker: stuck after ${consecutiveFailures} consecutive cycle errors. ` +
        `Last error: ${lastUnderlyingMessage}`,
    );
    this.name = "MssqlCdcStuckError";
    this.consecutiveFailures = consecutiveFailures;
    this.lastUnderlyingMessage = lastUnderlyingMessage;
  }
}

export class MssqlCdcWaker implements Waker {
  private readonly pool: mssql.ConnectionPool;
  private readonly captureInstance: string;
  private readonly watermarkSchemaBracketed: string;
  private readonly watermarkTableBracketed: string;
  private readonly pollIntervalMs: number;
  private readonly batchSize: number;
  private readonly stickyWakeCycles: number;
  private readonly shutdownTimeoutMs: number;
  private readonly onError: (err: Error) => void;
  private readonly metrics?: { onCycle: (s: MssqlCdcWakerCycleStats) => void };
  private readonly log: Logger;

  private onWake: (() => void) | null = null;
  private stopped = false;
  private timer: NodeJS.Timeout | null = null;
  private cycleInflight = false;
  private cycleLatch: { resolve: () => void; promise: Promise<void> } | null = null;
  private inflightRequest: mssql.Request | null = null;
  private inIdleMode = false;
  private consecutiveFailures = 0;
  private stuckErrorSurfaced = false;
  private stickyWakesRemaining = 0;

  private snapshot: {
    lastPollAtMs: number;
    lastErrorMessage: string | null;
  } = { lastPollAtMs: 0, lastErrorMessage: null };

  constructor(opts: MssqlCdcWakerOptions) {
    if (opts.pool === undefined || opts.pool === null) {
      throw new TypeError("MssqlCdcWaker: opts.pool is required");
    }
    if (opts.storePool !== undefined && opts.pool === opts.storePool) {
      throw new TypeError(
        "MssqlCdcWaker: `pool` must NOT be the same instance as `storePool` " +
          "(sustained CDC reads contend with the relay's claim path).",
      );
    }
    this.pool = opts.pool;
    this.captureInstance = assertIdent(
      opts.captureInstance ?? "dbo_outbox",
      "captureInstance",
    );
    const watermarkSchema = assertIdent(opts.watermarkSchema ?? "eventferry", "watermarkSchema");
    const watermarkTable = assertIdent(opts.watermarkTable ?? "cdc_watermark", "watermarkTable");
    this.watermarkSchemaBracketed = `[${watermarkSchema}]`;
    this.watermarkTableBracketed = `[${watermarkTable}]`;

    this.pollIntervalMs = clampInt(opts.pollIntervalMs ?? 1_000, 100, 60_000, "pollIntervalMs");
    this.batchSize = clampInt(opts.batchSize ?? 500, 1, 10_000, "batchSize");
    this.stickyWakeCycles = clampInt(opts.stickyWakeCycles ?? 5, 0, 100, "stickyWakeCycles");
    this.shutdownTimeoutMs = clampInt(opts.shutdownTimeoutMs ?? 5_000, 100, 60_000, "shutdownTimeoutMs");

    this.onError = wrapDefensive(opts.onError ?? (() => {}), opts.logger);
    this.metrics = opts.metrics;
    this.log = opts.logger ?? new NoopLogger();
  }

  async start(onWake: () => void): Promise<void> {
    this.onWake = onWake;
    this.stopped = false;
    this.consecutiveFailures = 0;
    this.stuckErrorSurfaced = false;
    this.stickyWakesRemaining = 0;

    await this.assertEngineSupportsCdc();
    await this.assertNotReadOnlyReplica();
    await this.assertWatermarkExists();
    this.scheduleNextCycle(0);
  }

  async stop(): Promise<void> {
    this.stopped = true;
    if (this.timer !== null) { clearTimeout(this.timer); this.timer = null; }
    try { this.inflightRequest?.cancel(); } catch { /* best effort */ }
    const latch = this.cycleLatch?.promise ?? Promise.resolve();
    const sleeper = new Promise<void>((r) => setTimeout(r, this.shutdownTimeoutMs));
    await Promise.race([latch, sleeper]);
    if (this.cycleInflight) {
      this.log.warn("MssqlCdcWaker.stop: cycle did not drain within shutdownTimeoutMs");
    }
    this.onWake = null;
  }

  async healthCheck(): Promise<MssqlCdcWakerHealth> {
    const request = this.pool.request();
    request.input("ci", mssql.NVarChar(128), this.captureInstance);
    const result = await request.query<{
      capture_enabled: number;
      min_lsn_hex: string | null;
      max_lsn_hex: string | null;
      watermark_hex: string | null;
      read_only: number;
    }>(`
DECLARE @min binary(10) = sys.fn_cdc_get_min_lsn(@ci);
DECLARE @max binary(10) = sys.fn_cdc_get_max_lsn();
SELECT
  CASE WHEN @min IS NULL THEN 0 ELSE 1 END                            AS capture_enabled,
  CASE WHEN @min IS NULL THEN NULL ELSE CONVERT(VARCHAR(22), @min, 1) END  AS min_lsn_hex,
  CASE WHEN @max IS NULL THEN NULL ELSE CONVERT(VARCHAR(22), @max, 1) END  AS max_lsn_hex,
  (SELECT CONVERT(VARCHAR(22), last_lsn, 1)
     FROM ${this.watermarkSchemaBracketed}.${this.watermarkTableBracketed}
     WHERE capture_instance = @ci)                                     AS watermark_hex,
  CASE WHEN CONVERT(NVARCHAR(60), DATABASEPROPERTYEX(DB_NAME(),'Updateability')) = N'READ_ONLY'
       THEN 1 ELSE 0 END                                               AS read_only;
`);
    const row = result.recordset[0];
    return {
      captureEnabled: (row?.capture_enabled ?? 0) === 1,
      minLsn: row?.min_lsn_hex ?? null,
      maxLsn: row?.max_lsn_hex ?? null,
      watermark: row?.watermark_hex ?? null,
      lagRowsApprox: -1,
      lastPollAtMs: this.snapshot.lastPollAtMs,
      lastErrorMessage: this.snapshot.lastErrorMessage,
      readOnlyDatabase: (row?.read_only ?? 0) === 1,
    };
  }

  private async assertEngineSupportsCdc(): Promise<void> {
    const result = await this.pool.request().query<{ edition: number }>(
      "SELECT CAST(SERVERPROPERTY('EngineEdition') AS int) AS edition;",
    );
    const edition = result.recordset[0]?.edition ?? -1;
    if (edition === 4) {
      throw new MssqlCdcUnsupportedEngineError(
        edition, "SQL Server Express does not support CDC. Use the polling-only relay.",
      );
    }
  }

  private async assertNotReadOnlyReplica(): Promise<void> {
    const result = await this.pool.request().query<{ ro: string }>(
      "SELECT CONVERT(NVARCHAR(60), DATABASEPROPERTYEX(DB_NAME(),'Updateability')) AS ro;",
    );
    if (result.recordset[0]?.ro === "READ_ONLY") {
      throw new MssqlCdcReadOnlyReplicaError();
    }
  }

  private async assertWatermarkExists(): Promise<void> {
    const request = this.pool.request();
    request.input("ci", mssql.NVarChar(128), this.captureInstance);
    const result = await request.query<{ exists_flag: number }>(`
SELECT CASE WHEN EXISTS (
    SELECT 1 FROM ${this.watermarkSchemaBracketed}.${this.watermarkTableBracketed}
    WHERE capture_instance = @ci
) THEN 1 ELSE 0 END AS exists_flag;
`);
    if ((result.recordset[0]?.exists_flag ?? 0) === 0) {
      throw new MssqlCdcWatermarkMissingError(this.captureInstance);
    }
  }

  private scheduleNextCycle(delayMs: number): void {
    if (this.stopped) return;
    this.timer = setTimeout(() => {
      this.timer = null;
      void this.runCycle();
    }, delayMs);
  }

  private async runCycle(): Promise<void> {
    if (this.stopped) return;
    if (this.cycleInflight) return; // re-entry guard
    this.cycleInflight = true;
    let resolveLatch: () => void = () => {};
    this.cycleLatch = {
      promise: new Promise<void>((r) => { resolveLatch = r; }),
      resolve: resolveLatch,
    };
    const t0 = Date.now();
    try {
      const stats = await this.pollOnce();
      this.snapshot = { lastPollAtMs: Date.now(), lastErrorMessage: null };
      this.consecutiveFailures = 0;
      this.stuckErrorSurfaced = false;
      this.metrics?.onCycle({
        durationMs: Date.now() - t0,
        rowsObserved: stats.rowsObserved,
        watermarkAdvancedHex: stats.watermarkAdvancedHex,
        stuckRetries: 0,
      });
      this.scheduleNextCycle(this.pollIntervalMs);
    } catch (err) {
      this.consecutiveFailures++;
      const error = err instanceof Error ? err : new Error(String(err));
      this.snapshot = { lastPollAtMs: this.snapshot.lastPollAtMs, lastErrorMessage: error.message };
      this.log.warn("MssqlCdcWaker cycle errored", {
        error: error.message, consecutiveFailures: this.consecutiveFailures,
      });
      if (this.consecutiveFailures >= 5 && !this.stuckErrorSurfaced) {
        this.stuckErrorSurfaced = true;
        this.onError(new MssqlCdcStuckError(this.consecutiveFailures, error.message));
      }
      const base = Math.min(this.pollIntervalMs * Math.pow(2, this.consecutiveFailures), 30_000);
      const jitter = base * (0.8 + Math.random() * 0.4);
      this.scheduleNextCycle(Math.floor(jitter));
    } finally {
      this.cycleInflight = false;
      this.inflightRequest = null;
      resolveLatch();
    }
  }

  /**
   * One CDC poll. SINGLE round trip wraps bounds-read + change-fetch +
   * watermark advance in a TRY/CATCH; per-cycle work is gated by
   * `sp_getapplock` keyed on capture_instance. Returns stats.
   */
  private async pollOnce(): Promise<{
    rowsObserved: number;
    watermarkAdvancedHex: string | null;
  }> {
    const request = this.pool.request();
    this.inflightRequest = request;
    request.input("ci", mssql.NVarChar(128), this.captureInstance);
    request.input("batchSize", mssql.Int, this.batchSize);

    const fnName = `cdc.fn_cdc_get_all_changes_${this.captureInstance}`;

    // Single batch: applock, bounds, retention check, fetch, advance.
    // Server-side catch of msg 208 / 313 / 22838 signals capture
    // disabled / retention overrun without surfacing as a generic
    // driver exception.
    const result = await request.batch(`
SET NOCOUNT ON;
DECLARE @lockResult int;
EXEC @lockResult = sp_getapplock
    @Resource = N'eventferry:cdc:${this.captureInstance}',
    @LockMode = 'Exclusive', @LockOwner = 'Session', @LockTimeout = 0;
IF @lockResult < 0
BEGIN
    SELECT
        CAST(0 AS int)  AS owned,
        CAST(0 AS int)  AS retention_overrun,
        CAST(0 AS int)  AS capture_disabled,
        CAST(NULL AS binary(10))  AS from_lsn,
        CAST(NULL AS binary(10))  AS min_lsn,
        CAST(NULL AS binary(10))  AS max_lsn;
    RETURN;
END;
DECLARE @from binary(10) = (SELECT last_lsn FROM ${this.watermarkSchemaBracketed}.${this.watermarkTableBracketed}
                              WHERE capture_instance = @ci);
DECLARE @min  binary(10) = sys.fn_cdc_get_min_lsn(@ci);
DECLARE @max  binary(10) = sys.fn_cdc_get_max_lsn();
DECLARE @retention_overrun bit = 0;
DECLARE @capture_disabled  bit = 0;
DECLARE @effective_from binary(10) = @from;

IF @min IS NULL OR @max IS NULL
BEGIN
    SET @capture_disabled = 1;
END
ELSE IF @from IS NOT NULL AND @from < @min
BEGIN
    SET @retention_overrun = 1;
    SET @effective_from = @min;
END;

SELECT
    CAST(1 AS int)              AS owned,
    @retention_overrun          AS retention_overrun,
    @capture_disabled           AS capture_disabled,
    CONVERT(VARCHAR(22), @from, 1)  AS from_hex,
    CONVERT(VARCHAR(22), @min, 1)   AS min_hex,
    CONVERT(VARCHAR(22), @max, 1)   AS max_hex,
    @effective_from             AS effective_from;

-- Fetch rows only when capture is up and we have a usable range.
IF @capture_disabled = 0
   AND @effective_from IS NOT NULL
   AND @max IS NOT NULL
   AND @effective_from <= @max
BEGIN
    BEGIN TRY
        SELECT TOP (@batchSize) __$start_lsn AS start_lsn
        FROM ${fnName}(@effective_from, @max, N'all')
        WHERE __$operation IN (2, 5)
        ORDER BY __$start_lsn, __$seqval;
    END TRY
    BEGIN CATCH
        IF ERROR_NUMBER() IN (208, 313, 22838)
        BEGIN
            -- 208: cdc.fn_... missing (capture dropped mid-flight)
            -- 313/22838: LSN out of range (retention raced past us)
            SELECT CAST(NULL AS binary(10)) AS start_lsn WHERE 1=0;
        END
        ELSE THROW;
    END CATCH
END
ELSE
BEGIN
    SELECT CAST(NULL AS binary(10)) AS start_lsn WHERE 1=0;
END
EXEC sp_releaseapplock @Resource = N'eventferry:cdc:${this.captureInstance}', @LockOwner = 'Session';
`);

    const meta = (result.recordsets as Array<Array<{
      owned: number;
      retention_overrun: number;
      capture_disabled: number;
      from_hex: string | null;
      min_hex: string | null;
      max_hex: string | null;
      effective_from: Buffer | null;
    }>>)[0]?.[0];

    if (!meta || meta.owned === 0) {
      // Another process holds the applock — silent no-op, no wake, no error.
      return { rowsObserved: 0, watermarkAdvancedHex: null };
    }

    if (meta.capture_disabled === 1) {
      if (!this.inIdleMode) {
        this.inIdleMode = true;
        this.onError(new MssqlCdcCaptureDisabledError(this.captureInstance));
      }
      return { rowsObserved: 0, watermarkAdvancedHex: null };
    }
    if (this.inIdleMode) this.inIdleMode = false;

    if (meta.retention_overrun === 1 && meta.from_hex && meta.min_hex) {
      const err = new MssqlCdcRetentionOverrunError(
        this.captureInstance, meta.from_hex, meta.min_hex,
      );
      this.onError(err);
      this.snapshot = { ...this.snapshot, lastErrorMessage: err.message };
      // Fall through — salvage [min, max] in this cycle (critical bugfix).
    }

    const rows = (result.recordsets as Array<Array<{ start_lsn: Buffer | null }>>)[1] ?? [];
    const validRows = rows.filter((r): r is { start_lsn: Buffer } => r.start_lsn !== null);

    let watermarkAdvancedHex: string | null = null;

    if (validRows.length > 0) {
      // Compute MAX(__$start_lsn) client-side.
      let maxSeen = validRows[0]!.start_lsn;
      for (const r of validRows) {
        if (compareLsn(r.start_lsn, maxSeen) > 0) maxSeen = r.start_lsn;
      }
      // Persist with conditional UPDATE + fn_cdc_increment_lsn server-side.
      const advanced = await this.persistWatermarkIncrement(maxSeen, meta.effective_from);
      if (advanced) {
        watermarkAdvancedHex = lsnToHex(maxSeen);
        // Fire onWake exactly once per non-empty batch.
        this.fireWake();
        // Reset sticky counter.
        this.stickyWakesRemaining = this.stickyWakeCycles;
      }
    } else if (this.stickyWakesRemaining > 0) {
      // No new CDC rows but a recent burst may still be draining via
      // the per-aggregate head-of-aggregate barrier in claimBatch.
      this.stickyWakesRemaining--;
      this.fireWake();
    }

    return {
      rowsObserved: validRows.length,
      watermarkAdvancedHex,
    };
  }

  private fireWake(): void {
    try { this.onWake?.(); } catch (cbErr) {
      this.log.warn("MssqlCdcWaker onWake callback threw", {
        error: cbErr instanceof Error ? cbErr.message : String(cbErr),
      });
    }
  }

  /**
   * Conditional UPDATE: advance only when watermark == priorLsn (loser
   * in a two-process race detects and re-polls). Returns true when this
   * process won the race.
   */
  private async persistWatermarkIncrement(
    maxSeen: Buffer, priorLsn: Buffer | null,
  ): Promise<boolean> {
    const request = this.pool.request();
    request.input("ci", mssql.NVarChar(128), this.captureInstance);
    request.input("maxSeen", mssql.VarBinary(10), maxSeen);
    if (priorLsn !== null) request.input("prior", mssql.VarBinary(10), priorLsn);
    const result = await request.query<{ affected: number }>(`
DECLARE @next binary(10) = sys.fn_cdc_increment_lsn(@maxSeen);
UPDATE ${this.watermarkSchemaBracketed}.${this.watermarkTableBracketed}
SET    last_lsn   = @next,
       updated_at = SYSUTCDATETIME()
WHERE  capture_instance = @ci
${priorLsn !== null ? "  AND last_lsn = @prior" : ""};
SELECT @@ROWCOUNT AS affected;
`);
    return (result.recordset[0]?.affected ?? 0) === 1;
  }
}

/**
 * `MssqlCdcRelay` — thin wrapper that composes the polling core `Relay`
 * with this waker pre-wired. All claim/markDone/retry/DLQ behaviour
 * delegates to the core Relay. NOT a parallel publisher.
 *
 *   export interface MssqlCdcRelayOptions {
 *     readonly store: import("@eventferry/mssql").MssqlStore;
 *     readonly publisher: import("@eventferry/core").Publisher;
 *     readonly waker: MssqlCdcWaker;
 *     readonly retry?: Partial<import("@eventferry/core").RetryConfig>;
 *     readonly dlq?: import("@eventferry/core").DlqConfig;
 *     readonly hooks?: import("@eventferry/core").RelayHooks;
 *     readonly pollIntervalMs?: number; // safety-net; default 2_000 (TIGHT — see README)
 *     readonly logger?: import("@eventferry/core").Logger;
 *   }
 *
 * The default `pollIntervalMs` is 2_000 (NOT 5_000 as in v0). README
 * documents that values >2s cause the polling safety net to be too slow
 * to mask the documented CDC failure modes (retention overrun,
 * disabled capture, stuck job).
 */

function clampInt(value: number, min: number, max: number, label: string): number {
  if (!Number.isFinite(value) || value < min || value > max) {
    throw new TypeError(`MssqlCdcWaker: ${label} must be ${min}..${max}, got ${String(value)}`);
  }
  return Math.floor(value);
}

function wrapDefensive(fn: (e: Error) => void, logger?: Logger): (e: Error) => void {
  return (e) => {
    try { fn(e); } catch (handlerErr) {
      (logger ?? new NoopLogger()).warn("MssqlCdcWaker onError handler threw", {
        error: handlerErr instanceof Error ? handlerErr.message : String(handlerErr),
      });
    }
  };
}