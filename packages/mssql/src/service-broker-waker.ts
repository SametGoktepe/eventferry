/**
 * `@eventferry/mssql` — `MssqlServiceBrokerWaker` (HARDENED)
 *
 * In-engine low-latency wake source for the polling core `Relay` running on
 * top of `MssqlStore`. Pluggable via the `Waker` contract documented at
 * `packages/core/src/types.ts:299-309` — `start(onWake): Promise<void>` /
 * `stop(): Promise<void>`, with `onWake` permitted to fire spuriously or be
 * missed entirely (polling is the safety net).
 *
 * ── DESIGN NOTES (cite when changing) ────────────────────────────────────
 *
 *   - WAITFOR(RECEIVE...), TIMEOUT N returns an EMPTY rowset on timeout
 *     (MS Learn: "WAITFOR ... Returns the empty result set when the timeout
 *     occurs"). The loop interprets empty receives as "no work" and silently
 *     re-issues the next slot — no `onWake()` fires on timeout. This is the
 *     foundation of the chained-short-WAITFOR pattern.
 *
 *   - END CONVERSATION is performed INSIDE the receive transaction by this
 *     waker (TARGET side, single hop). Dialog cleanup on the INITIATOR side
 *     — the side that ran `BEGIN DIALOG` in the AFTER INSERT trigger — is
 *     the responsibility of the activation procedure attached to the
 *     initiator queue (see `createServiceBrokerSetupSql` BATCH[8/9]).
 *     The waker MUST NOT take responsibility for initiator-side cleanup;
 *     that path is decoupled by design.
 *
 *   - DEDICATED CONNECTION (RATIONALE): a long-lived WAITFOR holds the TDS
 *     connection in a busy state. If the waker shared the store's pool, it
 *     could starve `claimBatch`/`markPublished` of every available
 *     connection, deadlocking the relay. The waker therefore owns a
 *     `min=1, max=1` pool — either supplied by the caller (asserted at
 *     start) or constructed internally from `opts.config`.
 *
 *   - AZURE SQL DATABASE EXCLUSION: Service Broker is supported on Azure
 *     SQL Managed Instance (EngineEdition=8) and on every on-prem edition
 *     (1..4, 6, 7) but is UNSUPPORTED on Azure SQL Database
 *     (EngineEdition=5). `start()` probes `SERVERPROPERTY('EngineEdition')`
 *     and throws `MssqlServiceBrokerUnsupportedError` rather than entering
 *     an infinite reconnect loop. Operators should drop the waker option
 *     (Relay falls back to its built-in poll loop) or migrate to MI.
 *
 *   - CHAINED SHORT WAITFORs (default 1s, capped by `idleHintMs`) replace
 *     a single long WAITFOR. Rationale: `Request.cancel()` semantics are
 *     driver-version-sensitive across the `tedious`/`mssql` matrix, and
 *     `stop()` must drain promptly. With slot chaining, `stop()` drains
 *     in O(slotMs) without depending on cancel.
 *
 *   - AT-LEAST-ONCE WAKE DELIVERY: RECEIVE + END CONVERSATION are wrapped
 *     in a single explicit transaction. `onWake()` is fired BEFORE
 *     `commit()` so an `onWake()` throw triggers rollback, returning the
 *     message to the queue for redelivery. Lost wakes (e.g. SIGKILL
 *     between `commit()` and `onWake()`) are absorbed by the relay's poll
 *     loop — by design.
 *
 *   - SPURIOUS-WAKE SUPPRESSION: the projection includes
 *     `message_type_name`; only rows whose type equals `messageTypeName`
 *     count toward firing `onWake()`. System dialog messages
 *     (`.../EndDialog`, `.../Error`) are ENDed but never fire wakes.
 */

import type { Logger, Waker } from "@eventferry/core";
import { NoopLogger } from "@eventferry/core";
import * as mssql from "mssql";
import { assertIdent } from "./ident.js";

/** Base for all edition-refusal errors across this package's wakers. */
export class MssqlEngineEditionUnsupportedError extends Error {
  readonly engineEdition: number;
  constructor(message: string, engineEdition: number) {
    super(message);
    this.name = "MssqlEngineEditionUnsupportedError";
    this.engineEdition = engineEdition;
  }
}

/**
 * Thrown by `start()` when the connected database reports
 * `SERVERPROPERTY('EngineEdition') = 5` (Azure SQL Database), which does
 * not support Service Broker. Drop the waker option (the polling relay
 * remains a complete, correct fallback) or migrate to Azure SQL Managed
 * Instance.
 */
export class MssqlServiceBrokerUnsupportedError
  extends MssqlEngineEditionUnsupportedError {
  constructor(engineEdition: number) {
    super(
      `MssqlServiceBrokerWaker: Service Broker is not supported on Azure SQL Database ` +
        `(SERVERPROPERTY('EngineEdition') = ${engineEdition}). Use the polling-only ` +
        `MssqlStore relay (omit the waker), or migrate to Azure SQL Managed Instance.`,
      engineEdition,
    );
    this.name = "MssqlServiceBrokerUnsupportedError";
  }
}

/**
 * Thrown / surfaced when a sticky, operator-fixable structural condition
 * is observed: broker disabled (`is_broker_enabled=0`), target queue
 * missing or with `is_receive_enabled=0`, or repeated reconnect failure.
 */
export class MssqlServiceBrokerStructuralError extends Error {
  readonly code:
    | "BROKER_DISABLED"
    | "QUEUE_MISSING"
    | "QUEUE_DISABLED"
    | "PERSISTENT_RECONNECT";
  constructor(
    code: MssqlServiceBrokerStructuralError["code"],
    detail: string,
  ) {
    super(`MssqlServiceBrokerWaker: ${code} — ${detail}`);
    this.name = "MssqlServiceBrokerStructuralError";
    this.code = code;
  }
}

/** Per-cycle telemetry shape consumed by the optional `metrics` hook. */
export interface MssqlServiceBrokerWakerCycleStats {
  readonly durationMs: number;
  readonly wakeMessages: number;
  readonly systemMessages: number;
  readonly endConversationHandles: number;
}

export interface MssqlServiceBrokerWakerOptions {
  /**
   * REQUIRED (one of `pool` / `config`). Already-connected DEDICATED
   * `mssql.ConnectionPool` sized `min=1, max=1`. RUNTIME-ASSERTED at
   * `start()`. MUST NOT be shared with the store pool — see "dedicated
   * connection rationale" in the file-level JSDoc. Caller MUST attach
   * `pool.on('error', ...)` BEFORE handing it over (unhandled `'error'`
   * on `mssql.ConnectionPool` crashes the Node process); also
   * RUNTIME-ASSERTED via `pool.listenerCount('error')`.
   *
   * Mutually exclusive with `config`.
   */
  pool?: mssql.ConnectionPool;
  /**
   * Alternative to `pool` — waker constructs and owns a pool from this
   * `mssql.config`, forcing `pool.min=1, pool.max=1` and attaching its
   * own `'error'` listener. Mirrors `PostgresNotifyWaker`'s connect
   * factory shape (caller supplies the configuration, the waker owns
   * the resource).
   */
  config?: mssql.config;
  /** Owning schema for the target queue. Default `"dbo"`. */
  schema?: string;
  /** Outbox table (informational, kept for symmetry with the store). Default `"outbox"`. */
  table?: string;
  /** Service Broker target queue name. Default `"OutboxWakerQueue"`. */
  queueName?: string;
  /** Service Broker target service name. Default `"//eventferry/outbox/WakerTargetService"`. */
  serviceName?: string;
  /** Service Broker contract. Default `"//eventferry/outbox/WakeupContract"`. */
  contractName?: string;
  /** Wakeup message type. Default `"//eventferry/outbox/Wakeup"`. */
  messageTypeName?: string;
  /**
   * Slot length for each chained WAITFOR (ms). Default `1000`. Range
   * `200..5_000`. `stop()` drains in O(slotMs) — choose larger slots for
   * lower idle CPU, smaller slots for faster shutdown.
   */
  slotMs?: number;
  /**
   * Soft cap on how long the waker stays in chained WAITFORs before
   * recycling the statement (for plan-cache freshness and connection
   * liveness checks). Default `30_000`. The waker internally chunks
   * this into `slotMs` iterations.
   */
  idleHintMs?: number;
  /** Rows per RECEIVE batch. Default `32`. Range `1..1_000`. */
  receiveBatchSize?: number;
  /** Initial reconnect delay (ms). Default `1_000`. Exponential up to `maxReconnectDelayMs`. */
  reconnectDelayMs?: number;
  /** Max reconnect delay (ms). Default `60_000`. */
  maxReconnectDelayMs?: number;
  /**
   * Interval (ms) for the periodic structural re-probe of
   * `SERVERPROPERTY('EngineEdition')` and `is_broker_enabled`. Default
   * `300_000` (5 min). Detects post-restore migrations and out-of-band
   * `ALTER DATABASE ... SET DISABLE_BROKER`.
   */
  structuralProbeIntervalMs?: number;
  /** Hard cap on `stop()` drain. Default `5_000`. */
  shutdownTimeoutMs?: number;
  /**
   * Structural-failure callback. Fired ONCE on transition into a sticky
   * failure (broker disabled, queue missing, edition flipped to 5,
   * >=5 consecutive reconnect failures). Internally wrapped — a throwing
   * handler is logged and cannot kill the loop.
   */
  onError?: (err: Error) => void;
  /** Optional per-cycle metrics hook (OpenTelemetry-shaped consumption). */
  metrics?: { onCycle: (stats: MssqlServiceBrokerWakerCycleStats) => void };
  /** Structured logger. Defaults to `NoopLogger`. */
  logger?: Logger;
}

/**
 * SQL Server Service Broker–backed {@link Waker}. See file-level JSDoc
 * for the design constraints (chained WAITFOR, dedicated connection,
 * Azure SQL Database exclusion, initiator-side cleanup boundary,
 * at-least-once via transaction wrapping).
 */
export class MssqlServiceBrokerWaker implements Waker {
  private pool: mssql.ConnectionPool | null;
  private readonly ownsPool: boolean;
  private readonly poolConfig: mssql.config | null;

  private readonly schemaBracketed: string;
  /**
   * Outbox table name — informational only, held so that
   * {@link describe} can report it for diagnostic dashboards. Not
   * referenced by the WAITFOR loop (which targets the queue, not the
   * table).
   */
  readonly tableName: string;
  private readonly queueBracketed: string;
  /**
   * Configured target service name — informational only, exposed for
   * {@link describe}. The waker never SENDs (only RECEIVEs); the
   * service is used by the AFTER INSERT trigger emitted by
   * `createServiceBrokerSetupSql`.
   */
  readonly serviceName: string;
  /**
   * Configured contract name — informational only, exposed for
   * {@link describe}. See {@link serviceName}.
   */
  readonly contractName: string;
  private readonly messageTypeName: string;
  private readonly slotMs: number;
  private readonly idleHintMs: number;
  private readonly receiveBatchSize: number;
  private readonly reconnectDelayMs: number;
  private readonly maxReconnectDelayMs: number;
  private readonly structuralProbeIntervalMs: number;
  private readonly shutdownTimeoutMs: number;
  private readonly onError: (err: Error) => void;
  private readonly metrics?: {
    onCycle: (s: MssqlServiceBrokerWakerCycleStats) => void;
  };
  private readonly log: Logger;

  private onWake: (() => void) | null = null;
  private stopped = false;
  private loopDone: Promise<void> = Promise.resolve();
  private consecutiveFailures = 0;
  private lastStructuralProbeAt = 0;
  private structuralErrorSurfaced:
    | MssqlServiceBrokerStructuralError["code"]
    | null = null;

  constructor(opts: MssqlServiceBrokerWakerOptions) {
    if (opts.pool === undefined && opts.config === undefined) {
      throw new TypeError(
        "MssqlServiceBrokerWaker: provide either `pool` (dedicated, min=1/max=1) or `config`.",
      );
    }
    if (opts.pool !== undefined && opts.config !== undefined) {
      throw new TypeError(
        "MssqlServiceBrokerWaker: `pool` and `config` are mutually exclusive.",
      );
    }
    this.pool = opts.pool ?? null;
    this.ownsPool = opts.pool === undefined;
    this.poolConfig = opts.config ?? null;

    const schema = assertIdent(opts.schema ?? "dbo", "schema");
    this.schemaBracketed = `[${schema}]`;
    this.tableName = assertIdent(opts.table ?? "outbox", "table");
    const queue = assertIdent(opts.queueName ?? "OutboxWakerQueue", "queueName");
    this.queueBracketed = `[${queue}]`;
    this.serviceName = assertUrn(
      opts.serviceName ?? "//eventferry/outbox/WakerTargetService",
      "serviceName",
    );
    this.contractName = assertUrn(
      opts.contractName ?? "//eventferry/outbox/WakeupContract",
      "contractName",
    );
    this.messageTypeName = assertUrn(
      opts.messageTypeName ?? "//eventferry/outbox/Wakeup",
      "messageTypeName",
    );

    this.slotMs = clampInt(opts.slotMs ?? 1_000, 200, 5_000, "slotMs");
    this.idleHintMs = clampInt(
      opts.idleHintMs ?? 30_000,
      1_000,
      600_000,
      "idleHintMs",
    );
    this.receiveBatchSize = clampInt(
      opts.receiveBatchSize ?? 32,
      1,
      1_000,
      "receiveBatchSize",
    );
    this.reconnectDelayMs = clampInt(
      opts.reconnectDelayMs ?? 1_000,
      100,
      60_000,
      "reconnectDelayMs",
    );
    this.maxReconnectDelayMs = clampInt(
      opts.maxReconnectDelayMs ?? 60_000,
      1_000,
      600_000,
      "maxReconnectDelayMs",
    );
    this.structuralProbeIntervalMs = clampInt(
      opts.structuralProbeIntervalMs ?? 300_000,
      10_000,
      3_600_000,
      "structuralProbeIntervalMs",
    );
    this.shutdownTimeoutMs = clampInt(
      opts.shutdownTimeoutMs ?? 5_000,
      100,
      60_000,
      "shutdownTimeoutMs",
    );

    this.onError = wrapDefensive(opts.onError ?? (() => {}), opts.logger);
    this.metrics = opts.metrics;
    this.log = opts.logger ?? new NoopLogger();
  }

  /**
   * Connect (if `ownsPool`), validate the supplied pool's sizing and
   * error-listener contract, run the structural startup probe (refuses
   * EngineEdition=5, refuses `is_broker_enabled=0`, refuses missing
   * queue), and launch the chained-WAITFOR loop. Returns once the loop
   * is scheduled; the loop runs until `stop()`.
   */
  async start(onWake: () => void): Promise<void> {
    this.onWake = onWake;
    this.stopped = false;
    this.consecutiveFailures = 0;
    this.structuralErrorSurfaced = null;

    try {
      if (this.pool === null && this.poolConfig !== null) {
        const config: mssql.config = {
          ...this.poolConfig,
          pool: { ...(this.poolConfig.pool ?? {}), min: 1, max: 1 },
        };
        const pool = new mssql.ConnectionPool(config);
        pool.on("error", (err: Error) => {
          this.log.warn("broker waker pool error", { error: err.message });
        });
        await pool.connect();
        this.pool = pool;
      }

      // Runtime assertion on caller-supplied pool sizing & error listener.
      if (!this.ownsPool && this.pool !== null) {
        const max = (
          this.pool as unknown as { config?: { pool?: { max?: number } } }
        ).config?.pool?.max;
        if (max !== 1) {
          throw new TypeError(
            `MssqlServiceBrokerWaker: caller-supplied pool must be max=1 (got ${String(max)}).`,
          );
        }
        if (this.pool.listenerCount("error") === 0) {
          throw new TypeError(
            "MssqlServiceBrokerWaker: caller-supplied pool must have an 'error' listener attached.",
          );
        }
      }

      await this.runStructuralProbe(/*isStartup*/ true);
      this.loopDone = this.runLoop();
    } catch (e) {
      this.stopped = true;
      throw e;
    }
  }

  /**
   * Set the stop flag and await loop drain bounded by
   * `shutdownTimeoutMs`. Closes the owned pool. Best-effort: never
   * throws; logs on timeout.
   */
  async stop(): Promise<void> {
    this.stopped = true;
    const deadline = Date.now() + this.shutdownTimeoutMs;
    const sleeper = new Promise<void>((r) =>
      setTimeout(r, this.shutdownTimeoutMs),
    );
    await Promise.race([this.loopDone.catch(() => undefined), sleeper]);
    if (Date.now() >= deadline) {
      this.log.warn(
        "MssqlServiceBrokerWaker.stop: loop did not drain within shutdownTimeoutMs",
      );
    }
    if (this.ownsPool && this.pool !== null) {
      try {
        await this.pool.close();
      } catch {
        /* best effort */
      }
    }
    this.pool = null;
    this.onWake = null;
  }

  /**
   * Operator probe — broker readiness snapshot. Reports engine edition,
   * database-level broker enablement, and queue enqueue/receive/activation
   * status. Does NOT throw on degraded state; returns the booleans for the
   * caller to surface via their own health-check endpoint.
   */
  async healthCheck(): Promise<{
    readonly brokerEnabled: boolean;
    readonly queueEnqueueEnabled: boolean;
    readonly queueReceiveEnabled: boolean;
    readonly queueActivationEnabled: boolean;
    readonly engineEdition: number;
  }> {
    if (this.pool === null) {
      throw new Error("MssqlServiceBrokerWaker.healthCheck: not started");
    }
    const result = await this.pool
      .request()
      .input(
        "queueName",
        mssql.NVarChar(128),
        this.queueBracketed.slice(1, -1),
      )
      .query<{
        edition: number;
        broker_enabled: number;
        is_enqueue: number | null;
        is_receive: number | null;
        is_activation: number | null;
      }>(`
SELECT
  CAST(SERVERPROPERTY('EngineEdition') AS int)                       AS edition,
  CAST(d.is_broker_enabled AS int)                                   AS broker_enabled,
  (SELECT TOP 1 CAST(sq.is_enqueue_enabled AS int)
     FROM sys.service_queues sq WHERE sq.name = @queueName)          AS is_enqueue,
  (SELECT TOP 1 CAST(sq.is_receive_enabled AS int)
     FROM sys.service_queues sq WHERE sq.name = @queueName)          AS is_receive,
  (SELECT TOP 1 CAST(sq.is_activation_enabled AS int)
     FROM sys.service_queues sq WHERE sq.name = @queueName)          AS is_activation
FROM sys.databases d
WHERE d.name = DB_NAME();
`);
    const row = result.recordset[0];
    return {
      brokerEnabled: (row?.broker_enabled ?? 0) === 1,
      queueEnqueueEnabled: (row?.is_enqueue ?? 0) === 1,
      queueReceiveEnabled: (row?.is_receive ?? 0) === 1,
      queueActivationEnabled: (row?.is_activation ?? 0) === 1,
      engineEdition: row?.edition ?? -1,
    };
  }

  /**
   * Structural probe: edition + `is_broker_enabled` + queue presence /
   * receive-enabled. On startup, throws on any failure (caller can
   * surface a startup error). On periodic re-probe, surfaces via
   * `onError` ONCE per sticky failure transition.
   */
  private async runStructuralProbe(isStartup: boolean): Promise<void> {
    this.lastStructuralProbeAt = Date.now();
    if (this.pool === null) return;
    const probe = await this.pool
      .request()
      .input(
        "queueName",
        mssql.NVarChar(128),
        this.queueBracketed.slice(1, -1),
      )
      .query<{
        edition: number;
        broker_enabled: number;
        queue_present: number;
        queue_receive: number;
      }>(`
SELECT
  CAST(SERVERPROPERTY('EngineEdition') AS int)         AS edition,
  CAST(d.is_broker_enabled AS int)                     AS broker_enabled,
  CASE WHEN EXISTS (SELECT 1 FROM sys.service_queues WHERE name = @queueName) THEN 1 ELSE 0 END AS queue_present,
  ISNULL((SELECT TOP 1 CAST(is_receive_enabled AS int) FROM sys.service_queues WHERE name = @queueName), 0) AS queue_receive
FROM sys.databases d WHERE d.name = DB_NAME();
`);
    const row = probe.recordset[0];
    const edition = row?.edition ?? -1;
    if (edition === 5) {
      throw new MssqlServiceBrokerUnsupportedError(edition);
    }
    if (isStartup) {
      if ((row?.broker_enabled ?? 0) === 0) {
        throw new MssqlServiceBrokerStructuralError(
          "BROKER_DISABLED",
          "is_broker_enabled=0",
        );
      }
      if ((row?.queue_present ?? 0) === 0) {
        throw new MssqlServiceBrokerStructuralError(
          "QUEUE_MISSING",
          `${this.queueBracketed}`,
        );
      }
    } else {
      if ((row?.broker_enabled ?? 0) === 0) {
        this.maybeSurface("BROKER_DISABLED", "is_broker_enabled=0");
      } else if ((row?.queue_present ?? 0) === 0) {
        this.maybeSurface("QUEUE_MISSING", `${this.queueBracketed}`);
      } else if ((row?.queue_receive ?? 0) === 0) {
        this.maybeSurface("QUEUE_DISABLED", "is_receive_enabled=0");
      } else {
        this.structuralErrorSurfaced = null;
      }
    }
  }

  private maybeSurface(
    code: MssqlServiceBrokerStructuralError["code"],
    detail: string,
  ): void {
    if (this.structuralErrorSurfaced === code) return;
    this.structuralErrorSurfaced = code;
    this.onError(new MssqlServiceBrokerStructuralError(code, detail));
  }

  /**
   * Outer loop: periodic structural re-probe + chained-WAITFOR window.
   * On error, increments `consecutiveFailures` and sleeps with
   * exponential backoff + jitter. Resets on first success.
   */
  private async runLoop(): Promise<void> {
    while (!this.stopped) {
      try {
        if (
          Date.now() - this.lastStructuralProbeAt >
          this.structuralProbeIntervalMs
        ) {
          await this.runStructuralProbe(/*isStartup*/ false);
        }
        await this.runIdleHintWindow();
        this.consecutiveFailures = 0;
      } catch (err) {
        if (this.stopped) return;
        this.consecutiveFailures++;
        const error = err instanceof Error ? err : new Error(String(err));
        this.log.warn("broker waker cycle errored", {
          error: error.message,
          consecutiveFailures: this.consecutiveFailures,
        });
        if (this.consecutiveFailures === 5) {
          this.maybeSurface("PERSISTENT_RECONNECT", error.message);
        }
        await this.sleepBackoff();
      }
    }
  }

  /**
   * One idle-hint window: chain `slotMs` WAITFORs until `idleHintMs`
   * elapses. Recycles the statement on each window boundary for
   * plan-cache freshness and TDS liveness.
   */
  private async runIdleHintWindow(): Promise<void> {
    const windowDeadline = Date.now() + this.idleHintMs;
    while (!this.stopped && Date.now() < windowDeadline) {
      await this.waitSlot();
    }
  }

  /**
   * One WAITFOR slot. RECEIVE + END CONVERSATION batched in a single
   * transaction; END CONVERSATIONs collapsed into one server round trip
   * via a server-side cursor over the received `@msgs` table. Filters
   * wake-firing on `message_type_name` so system messages
   * (`.../EndDialog`, `.../Error`) do not cause spurious `onWake()`.
   *
   * At-least-once: `onWake()` fires BEFORE `commit()`. If the operator
   * callback throws, the transaction rolls back and the wake message
   * returns to the queue for redelivery on the next slot.
   *
   * NOTE: WAITFOR(RECEIVE...), TIMEOUT N returns an EMPTY rowset on
   * timeout (MS Learn). The transaction still commits cleanly with zero
   * wakes/system messages/ended handles in that case.
   */
  private async waitSlot(): Promise<void> {
    if (this.pool === null) return;
    const t0 = Date.now();
    const tx = new mssql.Transaction(this.pool);
    await tx.begin(mssql.ISOLATION_LEVEL.READ_COMMITTED);
    let committed = false;
    try {
      const request = new mssql.Request(tx);
      request.input("slotMs", mssql.Int, this.slotMs);
      request.input("batchSize", mssql.Int, this.receiveBatchSize);
      request.input("wakeType", mssql.NVarChar(256), this.messageTypeName);

      const result = await request.batch(`
SET NOCOUNT ON;
DECLARE @msgs TABLE (
    conversation_handle UNIQUEIDENTIFIER,
    message_type_name   SYSNAME
);
WAITFOR (
    RECEIVE TOP (@batchSize)
        conversation_handle,
        message_type_name
    INTO @msgs
    FROM ${this.schemaBracketed}.${this.queueBracketed}
), TIMEOUT @slotMs;

SELECT
  (SELECT COUNT(*) FROM @msgs WHERE message_type_name = @wakeType)        AS wake_count,
  (SELECT COUNT(*) FROM @msgs WHERE message_type_name <> @wakeType)       AS system_count;

DECLARE @handle UNIQUEIDENTIFIER;
DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
    SELECT DISTINCT conversation_handle FROM @msgs WHERE conversation_handle IS NOT NULL;
OPEN cur; FETCH NEXT FROM cur INTO @handle;
DECLARE @ended int = 0;
WHILE @@FETCH_STATUS = 0
BEGIN
    BEGIN TRY END CONVERSATION @handle; SET @ended = @ended + 1; END TRY
    BEGIN CATCH /* swallow 8429 — dialog already auto-ended */ END CATCH
    FETCH NEXT FROM cur INTO @handle;
END
CLOSE cur; DEALLOCATE cur;
SELECT @ended AS ended_count;
`);

      const counts = result.recordsets as unknown as Array<
        Array<{
          wake_count?: number;
          system_count?: number;
          ended_count?: number;
        }>
      >;
      const wake = counts[0]?.[0]?.wake_count ?? 0;
      const system = counts[0]?.[0]?.system_count ?? 0;
      const ended = counts[1]?.[0]?.ended_count ?? 0;

      // Fire onWake BEFORE commit so the wake is durable: if onWake throws,
      // we rollback and the message returns to the queue for redelivery.
      if (wake > 0) {
        try {
          this.onWake?.();
        } catch (cbErr) {
          this.log.warn("onWake callback threw", {
            batchSize: wake,
            error: cbErr instanceof Error ? cbErr.message : String(cbErr),
          });
          throw cbErr; // force rollback — at-least-once delivery
        }
      }

      await tx.commit();
      committed = true;

      this.metrics?.onCycle({
        durationMs: Date.now() - t0,
        wakeMessages: wake,
        systemMessages: system,
        endConversationHandles: ended,
      });
    } finally {
      if (!committed) {
        try {
          await tx.rollback();
        } catch {
          /* best effort */
        }
      }
    }
  }

  /** Exponential backoff with ±20% jitter, capped at `maxReconnectDelayMs`. */
  private async sleepBackoff(): Promise<void> {
    const base = Math.min(
      this.reconnectDelayMs *
        Math.pow(2, Math.max(0, this.consecutiveFailures - 1)),
      this.maxReconnectDelayMs,
    );
    const jitter = base * (0.8 + Math.random() * 0.4);
    await new Promise((r) => setTimeout(r, Math.floor(jitter)));
  }
}

function clampInt(
  value: number,
  min: number,
  max: number,
  label: string,
): number {
  if (!Number.isFinite(value) || value < min || value > max) {
    throw new TypeError(
      `MssqlServiceBrokerWaker: ${label} must be ${min}..${max}, got ${String(value)}`,
    );
  }
  return Math.floor(value);
}

function wrapDefensive(
  fn: (e: Error) => void,
  logger?: Logger,
): (e: Error) => void {
  const log = logger ?? new NoopLogger();
  return (e) => {
    try {
      fn(e);
    } catch (handlerErr) {
      log.warn("onError handler threw", {
        error:
          handlerErr instanceof Error
            ? handlerErr.message
            : String(handlerErr),
      });
    }
  };
}

/**
 * URN validator for Service Broker object names that the T-SQL grammar
 * forces us to literal-interpolate (e.g. `CREATE SERVICE [name]`). The
 * waker only USES these names at runtime — DDL is emitted by
 * `createServiceBrokerSetupSql` — but we still validate to fail fast on
 * obviously malformed input.
 *
 * Allowed: printable ASCII (0x20..0x7e) MINUS the SQL grammar
 * meta-characters `'`, `;`, `[`, `]`. Rejects the SQL comment
 * sequences `--` and `/*`. Length capped at 256 (Service Broker object
 * name limit, `sysname`).
 *
 * Inlined here (rather than imported from `./urn.js`) so this file is
 * self-contained; the sibling `urn.ts` can be added later for cross-
 * module reuse without changing the waker's behaviour.
 */
function assertUrn(value: string, label: string): string {
  if (typeof value !== "string" || value.length === 0 || value.length > 256) {
    throw new TypeError(
      `MssqlServiceBrokerWaker: invalid URN for ${label}: length must be 1..256`,
    );
  }
  if (value.includes("--") || value.includes("/*")) {
    throw new TypeError(
      `MssqlServiceBrokerWaker: invalid URN for ${label}: contains SQL comment sequence`,
    );
  }
  for (let i = 0; i < value.length; i++) {
    const c = value.charCodeAt(i);
    if (c < 0x20 || c > 0x7e) {
      throw new TypeError(
        `MssqlServiceBrokerWaker: invalid URN for ${label}: non-printable-ASCII at offset ${i}`,
      );
    }
    if (c === 0x27 /* ' */ || c === 0x3b /* ; */ || c === 0x5b /* [ */ || c === 0x5d /* ] */) {
      throw new TypeError(
        `MssqlServiceBrokerWaker: invalid URN for ${label}: contains forbidden character at offset ${i}`,
      );
    }
  }
  return value;
}
