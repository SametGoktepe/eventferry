/**
 * `@eventferry/mssql-cdc-relay` — `MssqlCdcRelay`
 *
 * Thin convenience wrapper that constructs the core `Relay` AND an internal
 * `MssqlCdcWaker`, then drives both through a single `start()`/`stop()` pair.
 *
 * RATIONALE — why this file exists separately from `waker.ts`:
 *
 *   The core engine already accepts a `waker?: Waker` on `RelayOptions`
 *   (`packages/core/src/relay.ts:35`), and `Relay.start()` internally calls
 *   `waker.start(onWake)` before entering its claim loop. So a power user can
 *   wire CDC by hand:
 *
 *       const waker = new MssqlCdcWaker({ pool, captureInstance });
 *       const relay = new Relay({ store, publisher, waker, pollIntervalMs: 2_000 });
 *       await relay.start();
 *
 *   `MssqlCdcRelay` exists as the discoverable, one-import "happy path"
 *   surface that bakes in two non-obvious defaults the README documents as
 *   contractual:
 *
 *     1. `pollIntervalMs` defaults to `2_000` (NOT the engine's `200`). The
 *        polling claim path remains the correctness backstop, but tightening
 *        the wake-driven path's safety net to ~2s is part of the CDC relay's
 *        published latency budget (the design doc calls this out at
 *        `cdc_package_layout[relay.ts].purpose`).
 *     2. The waker's `onError` and `onWake` are fanned out to a single pair
 *        of user-supplied hooks (`onWake`, `onError`), so operator
 *        instrumentation lives in ONE place rather than threaded through
 *        both objects.
 *
 *   The wrapper does NOT shadow `RelayHooks` (the engine's hook surface is
 *   already comprehensive — `onPublished` / `onFailed` / `onDead` /
 *   `onError`); it only adds the waker-specific `onWake` callback that has
 *   no engine equivalent.
 *
 * LIFECYCLE — start/stop ordering is delegated to the core `Relay`:
 *
 *   - `start()`  → `Relay.start()` which: (a) connects publisher, (b) calls
 *                  `waker.start(signal)`, (c) enters the claim loop. We do
 *                  NOT call `waker.start()` ourselves — doing so would
 *                  double-start and is a known footgun on the v0 design
 *                  (see waker comment about re-entry guards).
 *   - `stop()`   → `Relay.stop()` which: (a) interrupts the claim loop,
 *                  (b) awaits the in-flight batch, (c) calls `waker.stop()`,
 *                  (d) disconnects publisher. Reverse order is therefore
 *                  guaranteed by the engine, not duplicated here.
 *
 *   This matches `PostgresStreamingRelay`'s pattern at the structural
 *   level (wrapper owns lifecycle of an internal core Relay + an internal
 *   wake source) while staying simpler — the Postgres streaming relay
 *   has to manually drive a WAL stream that is NOT a `Waker`, whereas
 *   `MssqlCdcWaker` already implements the `Waker` contract and slots into
 *   the engine's existing waker hook directly.
 *
 * RUNTIME OWNERSHIP:
 *
 *   - The CDC pool, watermark pool, capture instance, etc. are owned by the
 *     embedded `MssqlCdcWaker`. The wrapper never touches `mssql` directly.
 *   - The `store` and `publisher` are user-supplied; the wrapper passes
 *     them through unchanged.
 *   - `store.init?.()` and `publisher.connect()` are still owned by
 *     `Relay.start()` (we do not re-run them).
 *
 * @see packages/postgres/src/streaming-relay.ts — equivalent wrapper for
 *      the WAL-streaming path (more complex because WAL is not a `Waker`).
 */

import {
  Relay,
  type DlqConfig,
  type Logger,
  type OutboxStore,
  type Publisher,
  type RelayHooks,
  type RetryConfig,
  type Serializer,
} from "@eventferry/core";
import { MssqlCdcWaker, type MssqlCdcWakerOptions } from "./waker.js";

/**
 * Default poll interval (ms) when the wrapper constructs its internal Relay.
 *
 * Explicit and exported so integration tests can assert the contract value
 * without re-reading the constructor body. The CDC waker fires within
 * `pollIntervalMs` (the waker's own field, default 1s) of an INSERT, so the
 * engine's idle wait is the floor on lost-wake recovery, not on happy-path
 * latency. 2s keeps the worst-case wake-loss publish delay under the README's
 * stated p99 budget while leaving the engine's hot path firmly wake-driven.
 */
export const DEFAULT_CDC_RELAY_POLL_INTERVAL_MS = 2_000;

/**
 * Construction options for `MssqlCdcRelay`.
 *
 * The shape is intentionally a UNION rather than a SUPERSET of
 * `RelayOptions`: we omit the engine's `waker` field (we build it) and
 * inline the CDC waker's options under a single `cdc` key so callers can't
 * accidentally pass two conflicting wake sources.
 */
export interface MssqlCdcRelayOptions {
  /** Outbox store. Pass through to the core `Relay`. */
  store: OutboxStore;
  /** Publisher. Pass through to the core `Relay`. */
  publisher: Publisher;
  /**
   * CDC waker options. Everything `new MssqlCdcWaker(opts)` accepts —
   * `pool`, `captureInstance`, `pollIntervalMs` (the waker's OWN scan
   * interval, distinct from the engine's `pollIntervalMs`), `batchSize`,
   * `stickyWakeCycles`, etc.
   */
  cdc: MssqlCdcWakerOptions;
  /** Messages claimed per Relay iteration. Default 100 (engine default). */
  batchSize?: number;
  /**
   * Idle wait (ms) for the engine's polling safety net. Default
   * `DEFAULT_CDC_RELAY_POLL_INTERVAL_MS` (2_000), NOT the engine's 200ms
   * default — see the constant's JSDoc.
   */
  pollIntervalMs?: number;
  retry?: Partial<RetryConfig>;
  dlq?: DlqConfig;
  serializer?: Serializer;
  logger?: Logger;
  /**
   * Relay-level lifecycle hooks (`onPublished`, `onFailed`, `onDead`,
   * `onError`). The engine fires these for publish-path events. The
   * wrapper FANS OUT `onError` to both the engine's `RelayHooks.onError`
   * AND the waker's structural-failure `onError`, so a single handler sees
   * every reportable error regardless of which subsystem emitted it.
   */
  hooks?: RelayHooks;
  /**
   * Fired every time the CDC waker observes new outbox rows (or its
   * sticky-wake counter elects to fire). Has NO engine equivalent — this
   * is the only way to instrument the wake side without subclassing.
   * Defensive: a throwing handler is caught and logged; it cannot kill
   * the waker loop.
   */
  onWake?: () => void;
}

/**
 * One-import driver for "polling MssqlStore + CDC waker" with sensible
 * defaults. Equivalent to constructing both manually and threading them
 * through `new Relay({ ..., waker })`, except:
 *
 *   - `pollIntervalMs` defaults to `DEFAULT_CDC_RELAY_POLL_INTERVAL_MS`
 *     (2_000) instead of the engine's 200 — the waker handles low-latency
 *     wakes; polling is the safety net, not the hot path.
 *   - `onWake` and `onError` fan out to both the waker and the engine,
 *     so operator code touches ONE callback surface.
 *
 * Example:
 *
 *     const relay = new MssqlCdcRelay({
 *       store,
 *       publisher,
 *       cdc: { pool: cdcPool, captureInstance: "dbo_outbox" },
 *       onWake: () => metrics.increment("cdc.wake"),
 *       hooks: { onPublished: r => metrics.increment("publish.ok") },
 *     });
 *     await relay.start();
 *     // ... application runs ...
 *     await relay.stop();
 */
export class MssqlCdcRelay {
  private readonly relay: Relay;
  private readonly waker: MssqlCdcWaker;

  constructor(opts: MssqlCdcRelayOptions) {
    if (opts.store === undefined) {
      throw new TypeError("MssqlCdcRelay: `store` is required");
    }
    if (opts.publisher === undefined) {
      throw new TypeError("MssqlCdcRelay: `publisher` is required");
    }
    if (opts.cdc === undefined) {
      throw new TypeError(
        "MssqlCdcRelay: `cdc` (MssqlCdcWakerOptions) is required",
      );
    }

    // Fan out onError to BOTH the engine's RelayHooks AND the waker's
    // structural-failure callback. Immutable hook composition so the
    // user-supplied `hooks` object is never mutated.
    const userHooks: RelayHooks = opts.hooks ?? {};
    const userOnError = userHooks.onError;
    const composedHooks: RelayHooks = {
      ...userHooks,
      onError: (err: Error) => {
        // Defensive: user handler runs in its own try/catch so a throw
        // doesn't propagate back into the engine loop.
        try {
          userOnError?.(err);
        } catch (handlerErr) {
          (opts.logger ?? null)?.warn?.("MssqlCdcRelay.hooks.onError threw", {
            error:
              handlerErr instanceof Error
                ? handlerErr.message
                : String(handlerErr),
          });
        }
      },
    };

    // Fan out: waker's own `onError` (structural failures: retention
    // overrun, capture disabled, read-only replica, stuck) routes into
    // the SAME composed `onError` the engine uses for publish errors.
    // Immutable: never mutates `opts.cdc`.
    const userWakerOnError = opts.cdc.onError;
    const composedWakerOptions: MssqlCdcWakerOptions = {
      ...opts.cdc,
      onError: (err: Error) => {
        try {
          userWakerOnError?.(err);
        } catch (handlerErr) {
          (opts.logger ?? null)?.warn?.(
            "MssqlCdcRelay.cdc.onError threw",
            {
              error:
                handlerErr instanceof Error
                  ? handlerErr.message
                  : String(handlerErr),
            },
          );
        }
        // Always surface to the engine-level error sink too.
        composedHooks.onError?.(err);
      },
    };

    this.waker = new MssqlCdcWaker(composedWakerOptions);

    // The engine's Relay drives the waker lifecycle for us — see the
    // file-level JSDoc. We just hand it the constructed waker and let
    // `Relay.start/stop` orchestrate.
    this.relay = new Relay({
      store: opts.store,
      publisher: opts.publisher,
      batchSize: opts.batchSize,
      pollIntervalMs:
        opts.pollIntervalMs ?? DEFAULT_CDC_RELAY_POLL_INTERVAL_MS,
      retry: opts.retry,
      dlq: opts.dlq,
      serializer: opts.serializer,
      logger: opts.logger,
      hooks: composedHooks,
      waker: this.wrapWakerForOnWake(opts.onWake),
    });
  }

  /**
   * Start the core Relay (which in turn starts the waker, connects the
   * publisher, and enters the claim loop).
   *
   * NOT idempotent at the wrapper level — guarding is the engine's job
   * (`Relay.start()` is itself idempotent).
   */
  async start(): Promise<void> {
    await this.relay.start();
  }

  /**
   * Stop the core Relay (which in turn stops the waker after the in-flight
   * batch drains, then disconnects the publisher).
   *
   * Bounded by the waker's `shutdownTimeoutMs` (default 5_000ms) AND the
   * core Relay's own drain semantics; see each for tunables.
   */
  async stop(): Promise<void> {
    await this.relay.stop();
  }

  /**
   * Wrap the internal `MssqlCdcWaker` in a tiny `Waker` adapter that
   * intercepts the `onWake` callback so the wrapper can ALSO fire the
   * user-supplied `onWake` hook (which the bare `Waker` interface does
   * not expose). The wrapped object is what we hand to the core Relay.
   *
   * Why an adapter instead of patching the waker: the waker's `onWake`
   * is supplied per-start (`waker.start(onWake)`), not constructor-time,
   * so the only seam to layer behavior on is at the `start` boundary.
   * Wrapping here keeps `MssqlCdcWaker` itself free of wrapper concerns.
   */
  private wrapWakerForOnWake(
    userOnWake: (() => void) | undefined,
  ): MssqlCdcWaker {
    if (userOnWake === undefined) return this.waker;
    const innerWaker = this.waker;
    // Return an object that satisfies the `Waker` interface AND keeps
    // the concrete `MssqlCdcWaker` type so downstream code (tests,
    // healthChecks) can still see it. We use Object.create on the
    // prototype so instance state stays on `innerWaker`.
    const adapter = Object.create(
      Object.getPrototypeOf(innerWaker) as object,
    ) as MssqlCdcWaker;
    Object.assign(adapter, innerWaker);
    adapter.start = async (onWake: () => void): Promise<void> => {
      await innerWaker.start(() => {
        // Engine signal FIRST (don't gate the claim loop on user code).
        try {
          onWake();
        } finally {
          // Defensive: user handler isolated.
          try {
            userOnWake();
          } catch {
            // Swallowed by design — onWake is advisory, not lifecycle.
            // The engine's claim loop has already been signalled.
          }
        }
      });
    };
    adapter.stop = async (): Promise<void> => innerWaker.stop();
    return adapter;
  }
}
