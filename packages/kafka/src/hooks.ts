import type { PublishableMessage, PublishResult, Logger } from "@eventferry/core";

/**
 * Lifecycle hooks fired by `KafkaPublisher`. Every hook is optional. The
 * publisher wraps each invocation in a try/catch and logs (via the
 * configured logger) on failure — a misbehaving hook will NEVER break
 * publishing.
 *
 * Typical wiring:
 *   - Custom observability stacks (Datadog APM, New Relic) → `onPublish`,
 *     `onError`, `onTransactionAbort`.
 *   - Connection-aware readiness probes → `onConnect` / `onDisconnect`.
 *   - Audit logs of every published record → `onPublish`.
 */
export interface KafkaPublisherHooks {
  /** Fires after the underlying client successfully connects. */
  onConnect?(): void | Promise<void>;
  /** Fires after the underlying client disconnects (clean shutdown). */
  onDisconnect?(): void | Promise<void>;
  /**
   * Fires once per record after a publish attempt — both successes and
   * failures. The `result.ok` flag distinguishes them.
   */
  onPublish?(
    result: PublishResult,
    message: PublishableMessage,
  ): void | Promise<void>;
  /**
   * Fires for any error surfaced from the publish path — driver-thrown
   * errors, transaction abort errors, etc. `message` is set when the error
   * is per-record; absent for batch-level errors (e.g. connect failure).
   */
  onError?(
    error: Error,
    message?: PublishableMessage,
  ): void | Promise<void>;
  /**
   * Fires when a transactional sendBatch's inner abort path is taken.
   * Useful for observability dashboards that track EOS failure rates.
   */
  onTransactionAbort?(error: Error): void | Promise<void>;
}

/**
 * Invoke a hook safely. Never throws back into the caller — logs the hook's
 * failure via the configured logger (or no-op when logger is absent).
 */
export async function safeHook(
  logger: Logger | undefined,
  hookName: keyof KafkaPublisherHooks,
  invoke: () => void | Promise<void> | undefined,
): Promise<void> {
  try {
    const r = invoke();
    if (r && typeof (r as Promise<void>).then === "function") {
      await r;
    }
  } catch (err) {
    const error = err instanceof Error ? err : new Error(String(err));
    logger?.warn(`[@eventferry/kafka] hook ${hookName} threw; ignored`, {
      error: error.message,
    });
  }
}
