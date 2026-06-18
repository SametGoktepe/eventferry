/**
 * `@eventferry/mssql-cdc-relay` — error taxonomy.
 *
 * The CDC waker observes four distinct structural failure modes that the
 * polling-only relay either cannot surface or surfaces only generically.
 * Each is modelled as a dedicated `CdcRelayError` subclass so operators
 * can branch on `instanceof` (preferred) or, when crossing a process
 * boundary, on the typed `name` literal.
 *
 * Mapping intentionally tracks the ORIGINAL SQL Server diagnostic
 * surface so a failing waker can be cross-referenced against
 * `sys.messages`, the CDC system views, and SQL Server Agent state
 * without grepping driver internals:
 *
 *   - `CdcNotEnabledError`         → `sys.databases.is_cdc_enabled = 0`,
 *                                    `sys.tables.is_tracked_by_cdc = 0`,
 *                                    or `sys.fn_cdc_get_min_lsn(@ci) IS NULL`
 *                                    (covers `sp_cdc_disable_db` /
 *                                    `sp_cdc_disable_table` races and
 *                                    error 208 — "Invalid object name
 *                                    cdc.fn_cdc_get_all_changes_*").
 *   - `CdcRetentionExceededError`  → errors 313 / 22838 returned by
 *                                    `cdc.fn_cdc_get_all_changes_*` when
 *                                    the requested `@from_lsn` is below
 *                                    `sys.fn_cdc_get_min_lsn` (the CDC
 *                                    cleanup job ran past us).
 *   - `WatermarkBelowMinLsnError`  → the relay's own watermark row
 *                                    `eventferry.cdc_watermark.last_lsn`
 *                                    compared against
 *                                    `sys.fn_cdc_get_min_lsn(@ci)` —
 *                                    same root cause as 313/22838 but
 *                                    detected proactively in the
 *                                    bounds-check before any change-fetch.
 *   - `CdcCaptureJobStoppedError`  → `sys.dm_cdc_log_scan_sessions`
 *                                    reports no recent session AND
 *                                    `msdb.dbo.cdc_jobs` shows the
 *                                    `cdc.<db>_capture` SQL Agent job
 *                                    disabled or stopped (capture
 *                                    process not draining the
 *                                    transaction log into the change
 *                                    tables).
 *
 * All subclasses are plain `Error`s (no driver dependency) so they can
 * be re-thrown, serialised through structured loggers, and matched in
 * tests without importing `mssql`.
 */

/**
 * Base class for every structural error surfaced by the CDC waker.
 *
 * Distinguishes "the waker cannot make progress" from generic
 * `mssql.RequestError` / `TypeError` / runtime exceptions. Catch this
 * in operator code paths (alerting, autohealing) when you don't care
 * which specific CDC pathology triggered.
 *
 * The `cause` field follows the standard `ErrorOptions` shape (Node 16.9+)
 * so underlying `mssql.RequestError` instances can be chained without
 * losing the SQL Server error number / state / line.
 */
export class CdcRelayError extends Error {
  override readonly name: string = "CdcRelayError";

  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    // Restore prototype chain for `instanceof` across transpilation
    // targets that down-level `Error` (ES5/ES2015 emit). No-op on
    // modern targets but cheap and idempotent.
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

/**
 * CDC is not enabled for the requested capture_instance (or has been
 * disabled mid-flight).
 *
 * SQL diagnostic surface:
 *   - `sys.databases.is_cdc_enabled` — 0 when `sp_cdc_disable_db` has
 *     been run.
 *   - `sys.tables.is_tracked_by_cdc` — 0 when `sp_cdc_disable_table`
 *     has been run for the source table backing this capture instance.
 *   - `sys.fn_cdc_get_min_lsn(@ci)` returning `NULL` (the function the
 *     waker calls first every cycle — cheap detection without raising
 *     208).
 *   - Server error 208 ("Invalid object name 'cdc.fn_cdc_get_all_changes_<ci>'")
 *     when the `cdc.*` functions have been dropped while the waker
 *     held a reference.
 *
 * Recovery: the waker enters idle mode and stops firing `onWake()`;
 * the polling core relay continues. Re-enable via `sp_cdc_enable_table`
 * (and `sp_cdc_enable_db` if necessary) and the waker resumes on the
 * next cycle without restart.
 */
export class CdcNotEnabledError extends CdcRelayError {
  override readonly name = "CdcNotEnabledError" as const;
  readonly captureInstance: string;

  constructor(captureInstance: string, options?: { cause?: unknown }) {
    super(
      `CDC is not enabled for capture_instance '${captureInstance}'. ` +
        `sys.fn_cdc_get_min_lsn(@ci) returned NULL (or error 208 was raised). ` +
        `Likely sp_cdc_disable_db / sp_cdc_disable_table ran, or the cdc.* ` +
        `functions were dropped. Re-enable via sp_cdc_enable_table; the polling ` +
        `relay continues to drain in the meantime.`,
      options,
    );
    this.captureInstance = captureInstance;
  }
}

/**
 * The relay's persisted watermark is below the current `min_lsn` AND
 * the change-fetch call already failed with the corresponding SQL
 * Server error.
 *
 * SQL diagnostic surface:
 *   - Error 313 ("An insufficient number of arguments were supplied
 *     for the procedure or function cdc.fn_cdc_get_all_changes_...")
 *     — the CDC functions raise this when `@from_lsn` is below
 *     `min_lsn`.
 *   - Error 22838 ("The Log Sequence Number (LSN) ... is not available
 *     in the change table.") — explicit retention overrun message on
 *     newer SQL Server builds.
 *
 * Difference from `WatermarkBelowMinLsnError`: this one was raised
 * SERVER-SIDE because we already issued the change-fetch before
 * comparing bounds. Use it to track how often we lose the race against
 * the CDC cleanup job.
 *
 * Recovery: the waker snaps `last_lsn` forward to `min_lsn`, salvages
 * `[min_lsn, max_lsn]` in the same cycle, and surfaces this error via
 * `onError`. Rows in the genuinely missed range `(observed_lsn, min_lsn)`
 * are NOT re-fed; the polling relay backstops correctness.
 */
export class CdcRetentionExceededError extends CdcRelayError {
  override readonly name = "CdcRetentionExceededError" as const;
  readonly captureInstance: string;
  readonly observedLsn: string;
  readonly minLsn: string;
  readonly sqlErrorNumber: number | null;

  constructor(
    captureInstance: string,
    observedLsn: string,
    minLsn: string,
    sqlErrorNumber: number | null = null,
    options?: { cause?: unknown },
  ) {
    super(
      `CDC retention exceeded for capture_instance '${captureInstance}': ` +
        `requested @from_lsn ${observedLsn} is below min_lsn ${minLsn} ` +
        `(SQL error ${sqlErrorNumber ?? "313/22838"}). The CDC cleanup job ` +
        `advanced past the relay's watermark. Snapping forward to min_lsn; ` +
        `rows in the missed range are lost from CDC but backstopped by the ` +
        `polling relay. Consider increasing @retention on cdc.change_tables ` +
        `or lowering pollIntervalMs.`,
      options,
    );
    this.captureInstance = captureInstance;
    this.observedLsn = observedLsn;
    this.minLsn = minLsn;
    this.sqlErrorNumber = sqlErrorNumber;
  }
}

/**
 * The relay's persisted watermark is below `min_lsn`, detected
 * PROACTIVELY in the bounds-check before any change-fetch is issued.
 *
 * SQL diagnostic surface:
 *   - `sys.fn_cdc_get_min_lsn(@ci)` compared against the watermark
 *     stored in `eventferry.cdc_watermark.last_lsn` for this
 *     capture_instance. Detected in the single-round-trip poll batch
 *     where `@from < @min` raises the `retention_overrun` flag
 *     server-side without ever invoking `cdc.fn_cdc_get_all_changes_*`.
 *   - Same underlying pathology as `CdcRetentionExceededError` but the
 *     cleanup job did NOT actually race the change-fetch — we noticed
 *     first.
 *
 * Use this to distinguish "we saw the gap and salvaged cleanly" from
 * "the server told us we missed the gap" in metrics; both indicate the
 * same operational problem (retention too short or poll interval too
 * long), but only the latter implies the wake-loss window may be
 * larger than `[observedLsn, minLsn]`.
 */
export class WatermarkBelowMinLsnError extends CdcRelayError {
  override readonly name = "WatermarkBelowMinLsnError" as const;
  readonly captureInstance: string;
  readonly observedLsn: string;
  readonly minLsn: string;

  constructor(
    captureInstance: string,
    observedLsn: string,
    minLsn: string,
    options?: { cause?: unknown },
  ) {
    super(
      `Watermark below min_lsn for capture_instance '${captureInstance}': ` +
        `persisted last_lsn ${observedLsn} < sys.fn_cdc_get_min_lsn(@ci) = ${minLsn}. ` +
        `Detected proactively in the bounds-check (no change-fetch issued). ` +
        `Salvaging [min_lsn, max_lsn] in this cycle; wakes for the genuinely ` +
        `missed range are lost. Increase CDC retention (sys.sp_cdc_change_job ` +
        `@job_type = N'cleanup', @retention = ...) or shorten pollIntervalMs.`,
      options,
    );
    this.captureInstance = captureInstance;
    this.observedLsn = observedLsn;
    this.minLsn = minLsn;
  }
}

/**
 * The SQL Server Agent CDC capture job is not running — change tables
 * are not being populated from the transaction log, so `max_lsn` is
 * frozen even when application traffic continues. The waker would
 * silently observe zero rows forever without this error.
 *
 * SQL diagnostic surface:
 *   - `sys.dm_cdc_log_scan_sessions` — no recent row, or
 *     `empty_scan_count` climbing while application writes continue
 *     (capture process polling but never finding work because it isn't
 *     attached to the log).
 *   - `msdb.dbo.cdc_jobs` — the `cdc.<db>_capture` row's
 *     `job_type = N'capture'` exists but the underlying SQL Agent job
 *     `cdc.<db>_capture` is `enabled = 0` or in `failed` state.
 *   - `sys.dm_server_services` — `SQLServerAgent` itself stopped
 *     (Express and some managed editions don't run Agent — see also
 *     `CdcNotEnabledError` for the edition-mismatch case).
 *
 * Recovery: restart via `sys.sp_cdc_start_job @job_type = N'capture'`
 * or fix the SQL Agent job. The waker continues polling but will not
 * fire `onWake()` until `max_lsn` advances; the polling relay continues
 * to drain anything already written to the change tables.
 */
export class CdcCaptureJobStoppedError extends CdcRelayError {
  override readonly name = "CdcCaptureJobStoppedError" as const;
  readonly captureInstance: string;
  readonly observedMaxLsn: string | null;
  readonly lastScanSessionEndTime: Date | null;

  constructor(
    captureInstance: string,
    observedMaxLsn: string | null,
    lastScanSessionEndTime: Date | null = null,
    options?: { cause?: unknown },
  ) {
    super(
      `CDC capture job appears stopped for capture_instance '${captureInstance}': ` +
        `sys.fn_cdc_get_max_lsn() ${
          observedMaxLsn === null ? "is NULL" : `= ${observedMaxLsn}`
        } and sys.dm_dm_cdc_log_scan_sessions ${
          lastScanSessionEndTime === null
            ? "reports no recent session"
            : `last ran at ${lastScanSessionEndTime.toISOString()}`
        }. Restart via sys.sp_cdc_start_job @job_type = N'capture' and verify ` +
        `the SQL Server Agent service is running (Express/some managed editions ` +
        `do not provide Agent — use the polling-only relay there).`,
      options,
    );
    this.captureInstance = captureInstance;
    this.observedMaxLsn = observedMaxLsn;
    this.lastScanSessionEndTime = lastScanSessionEndTime;
  }
}
