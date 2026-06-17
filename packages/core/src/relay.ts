import { computeBackoff } from "./backoff.js";
import { buildPublishable } from "./publishable.js";
import { ConsoleLogger, JsonSerializer } from "./serializer.js";
import type {
  DlqConfig,
  Logger,
  OutboxRecord,
  OutboxStore,
  PublishableMessage,
  PublishErrorKind,
  Publisher,
  RelayHooks,
  RetryConfig,
  Serializer,
  Waker,
} from "./types.js";

export interface RelayOptions {
  store: OutboxStore;
  publisher: Publisher;
  /** Messages claimed per poll iteration. Default 100. */
  batchSize?: number;
  /** Idle wait (ms) when a poll returns no work. Default 200. */
  pollIntervalMs?: number;
  retry?: Partial<RetryConfig>;
  dlq?: DlqConfig;
  serializer?: Serializer;
  logger?: Logger;
  hooks?: RelayHooks;
  /**
   * Optional low-latency wake source. When provided, the relay claims as soon as
   * the waker signals new work, instead of waiting out `pollIntervalMs`. Polling
   * stays on as a safety net, so set `pollIntervalMs` longer when using a waker.
   */
  waker?: Waker;
}

const DEFAULT_RETRY: RetryConfig = {
  maxAttempts: 5,
  strategy: "exponential",
  baseMs: 200,
  maxMs: 30_000,
  jitter: true,
};

/**
 * The Relay drains the outbox store and publishes messages to the broker.
 *
 * It is safe to run multiple Relay instances concurrently against the same
 * store as long as the store's claimBatch uses a lock-free claim strategy
 * (e.g. SELECT ... FOR UPDATE SKIP LOCKED).
 */
export class Relay {
  private readonly store: OutboxStore;
  private readonly publisher: Publisher;
  private readonly batchSize: number;
  private readonly pollIntervalMs: number;
  private readonly retry: RetryConfig;
  private readonly dlq: DlqConfig;
  private readonly serializer: Serializer;
  private readonly log: Logger;
  private readonly hooks: RelayHooks;
  private readonly waker: Waker | null;

  private running = false;
  private stopping = false;
  private loopPromise: Promise<void> | null = null;

  // Interruptible idle wait: `signal()` wakes a pending wait (or marks one
  // pending if none is in flight, so a wake can't be lost between cycles).
  private wakePending = false;
  private wakeResolver: (() => void) | null = null;

  constructor(opts: RelayOptions) {
    this.store = opts.store;
    this.publisher = opts.publisher;
    this.batchSize = opts.batchSize ?? 100;
    this.pollIntervalMs = opts.pollIntervalMs ?? 200;
    this.retry = { ...DEFAULT_RETRY, ...opts.retry };
    this.dlq = opts.dlq ?? {};
    this.serializer = opts.serializer ?? new JsonSerializer();
    this.log = opts.logger ?? new ConsoleLogger();
    this.hooks = opts.hooks ?? {};
    this.waker = opts.waker ?? null;
  }

  async start(): Promise<void> {
    if (this.running) return;
    this.running = true;
    this.stopping = false;

    await this.store.init?.();
    await this.publisher.connect();
    await this.waker?.start(() => this.signal());
    this.log.info("relay started", {
      batchSize: this.batchSize,
      pollIntervalMs: this.pollIntervalMs,
      waker: this.waker !== null,
    });

    this.loopPromise = this.loop();
  }

  /**
   * Stop accepting new work, finish the in-flight batch, then disconnect.
   */
  async stop(): Promise<void> {
    if (!this.running) return;
    this.stopping = true;
    this.signal(); // break any in-flight idle wait so shutdown is immediate
    this.log.info("relay stopping, draining in-flight batch");
    await this.loopPromise;
    await this.waker?.stop();
    await this.publisher.disconnect();
    await this.store.close?.();
    this.running = false;
    this.log.info("relay stopped");
  }

  private async loop(): Promise<void> {
    while (!this.stopping) {
      try {
        const processed = await this.tick();
        if (processed === 0 && !this.stopping) {
          await this.waitForWork(this.pollIntervalMs);
        }
      } catch (err) {
        const error = err instanceof Error ? err : new Error(String(err));
        this.log.error("relay loop error", { error: error.message });
        this.hooks.onError?.(error);
        await this.waitForWork(this.pollIntervalMs);
      }
    }
  }

  /**
   * Run a single claim+publish cycle. Returns number of records processed.
   * Exposed for tests / manual single-shot draining.
   */
  async tick(): Promise<number> {
    const batch = await this.store.claimBatch(this.batchSize);
    if (batch.length === 0) return 0;

    this.hooks.onBatchClaimed?.(batch.length);
    this.log.debug("batch claimed", { count: batch.length });

    const messages = await this.toPublishable(batch);
    const recordsById = new Map(batch.map((r) => [r.id, r]));

    const results = await this.publisher.publish(messages);

    const succeeded: string[] = [];
    for (const result of results) {
      const record = recordsById.get(result.recordId);
      if (!record) continue;

      if (result.ok) {
        succeeded.push(record.id);
        this.hooks.onPublished?.(result);
      } else {
        await this.handleFailure(
          record,
          result.error ?? new Error("unknown publish error"),
          result.errorKind,
        );
      }
    }

    if (succeeded.length > 0) {
      await this.store.markDone(succeeded);
    }

    return batch.length;
  }

  private async handleFailure(
    record: OutboxRecord,
    error: Error,
    errorKind?: PublishErrorKind,
  ): Promise<void> {
    // Backpressure: client-side producer queue is full. Re-queue the record
    // with a brief delay; do NOT increment attempts (this isn't the record's
    // fault, the producer is just full). Stores without `requeue` fall back
    // to markFailed, accepting the attempt-counter hit.
    if (errorKind === "backpressure") {
      const delay = this.retry.backpressureDelayMs ?? 1000;
      const retryAt = new Date(Date.now() + delay);
      this.hooks.onFailed?.(record, error, true);
      this.log.warn("publish backpressure — requeueing without bumping attempts", {
        recordId: record.id,
        retryAt: retryAt.toISOString(),
        errorKind,
        error: error.message,
      });
      if (this.store.requeue) {
        await this.store.requeue(record.id, retryAt);
      } else {
        // Fallback: markFailed (will increment attempts). Documented in the
        // OutboxStore.requeue JSDoc.
        await this.store.markFailed(record.id, retryAt, "failed");
      }
      return;
    }

    const attempts = record.attempts + 1;
    // fatal/poison short-circuit: retrying cannot help (auth denied, fenced
    // epoch, oversized record, schema rejected). Skip the backoff schedule
    // entirely and go straight to DLQ + dead.
    const isTerminalKind = errorKind === "fatal" || errorKind === "poison";
    const retryAt = isTerminalKind
      ? null
      : this.nextRetryAtForKind(attempts, errorKind);
    const willRetry = retryAt !== null;

    this.hooks.onFailed?.(record, error, willRetry);
    this.log.warn("publish failed", {
      recordId: record.id,
      attempts,
      willRetry,
      errorKind: errorKind ?? "retriable",
      error: error.message,
    });

    if (willRetry) {
      await this.store.markFailed(record.id, retryAt, "failed");
      return;
    }

    // Terminal: route to DLQ if configured, then mark dead.
    if (this.dlq.topic && this.publisher.publishToDlq) {
      try {
        const msg = (await this.toPublishable([record]))[0];
        if (msg) {
          await this.publisher.publishToDlq(
            {
              ...msg,
              topic: this.dlq.topic,
              // Per-record enrichment so the DLQ consumer has everything
              // needed for triage: original destination, aggregate / message
              // identity, the attempts count, and (opt-in) a truncated stack.
              headers: this.buildDlqHeaders(record, error, msg.headers),
            },
            error,
          );
        }
      } catch (dlqErr) {
        const e = dlqErr instanceof Error ? dlqErr : new Error(String(dlqErr));
        this.log.error("DLQ publish failed", {
          recordId: record.id,
          error: e.message,
        });
      }
    }

    await this.store.markFailed(record.id, null, "dead");
    this.hooks.onDead?.(record, error);
  }

  /**
   * Compute the next retry instant, applying the quota multiplier when the
   * driver classified the failure as a server throttle. Quota failures DO
   * count as attempts (unlike backpressure) — the multiplier only stretches
   * the delay, not the budget.
   */
  private nextRetryAtForKind(
    attempts: number,
    errorKind: PublishErrorKind | undefined,
  ): Date | null {
    if (attempts >= this.retry.maxAttempts) return null;
    const baseDelay = computeBackoff(this.retry, attempts);
    const multiplier =
      errorKind === "quota" ? this.retry.quotaMultiplier ?? 5 : 1;
    const delay = Math.min(baseDelay * multiplier, this.retry.maxMs * 10);
    return new Date(Date.now() + delay);
  }

  /**
   * Build the per-record DLQ header bag. Operators triaging the DLQ get the
   * original destination, the aggregate / message identity, attempts count,
   * and optionally a truncated error stack — everything you need to decide
   * whether to re-enqueue, escalate, or drop.
   */
  private buildDlqHeaders(
    record: OutboxRecord,
    error: Error,
    existing: Record<string, string>,
  ): Record<string, string> {
    const out: Record<string, string> = {
      ...existing,
      "original-topic": record.topic,
      "dlq-attempts": String(record.attempts + 1),
      "dlq-original-aggregate-id": record.aggregateId,
      "dlq-original-message-id": record.messageId,
    };
    if (this.dlq.includeStackTraces && error.stack) {
      const maxBytes = this.dlq.maxStackBytes ?? 4096;
      out["dlq-error-stack"] = truncateUtf8(error.stack, maxBytes);
    }
    return out;
  }

  private async toPublishable(
    records: OutboxRecord[],
  ): Promise<PublishableMessage[]> {
    const out: PublishableMessage[] = [];
    for (const record of records) {
      out.push(await buildPublishable(record, this.serializer));
    }
    return out;
  }

  /** Wake a pending idle wait, or mark one pending so the next wait is skipped. */
  private signal(): void {
    this.wakePending = true;
    this.wakeResolver?.();
  }

  /**
   * Idle wait that resolves after `ms`, or early when `signal()` fires. A signal
   * raised while no wait is in flight is remembered (wakePending), so a wake that
   * races a claim cycle is never lost.
   */
  private waitForWork(ms: number): Promise<void> {
    if (this.wakePending) {
      this.wakePending = false;
      return Promise.resolve();
    }
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        this.wakeResolver = null;
        resolve();
      }, ms);
      this.wakeResolver = () => {
        clearTimeout(timer);
        this.wakeResolver = null;
        this.wakePending = false;
        resolve();
      };
    });
  }
}

/**
 * Truncate a string to at most `maxBytes` of its UTF-8 encoding, appending a
 * "…[truncated]" marker when bytes are removed. Used to keep the DLQ stack
 * header bounded — headers in Kafka are byte-sized, not character-sized, so
 * a naive `slice` on multibyte text can blow the limit. Always returns a
 * valid (no-broken-codepoint) UTF-8 string.
 */
function truncateUtf8(input: string, maxBytes: number): string {
  const marker = "…[truncated]";
  const markerBytes = Buffer.byteLength(marker, "utf8");
  const buf = Buffer.from(input, "utf8");
  if (buf.byteLength <= maxBytes) return input;
  const budget = Math.max(0, maxBytes - markerBytes);
  // Slice and decode; if the slice lands mid-codepoint, drop trailing bytes
  // until the decode is clean.
  let end = budget;
  while (end > 0) {
    const slice = buf.subarray(0, end).toString("utf8");
    if (!slice.endsWith("�")) {
      return slice + marker;
    }
    end--;
  }
  return marker;
}
