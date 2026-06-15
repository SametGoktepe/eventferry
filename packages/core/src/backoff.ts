import type { RetryConfig } from "./types.js";

/**
 * Compute the delay (ms) before the next retry attempt.
 *
 * @param attempt 1-based attempt number that just failed.
 */
export function computeBackoff(config: RetryConfig, attempt: number): number {
  const { strategy, baseMs, maxMs } = config;
  const jitter = config.jitter ?? true;

  let delay: number;
  switch (strategy) {
    case "fixed":
      delay = baseMs;
      break;
    case "linear":
      delay = baseMs * attempt;
      break;
    case "exponential":
      // base * 2^(attempt-1), capped to avoid overflow before clamping
      delay = baseMs * 2 ** Math.min(attempt - 1, 30);
      break;
  }

  delay = Math.min(delay, maxMs);

  if (jitter) {
    // Full jitter: random in [0, delay]. Decorrelates concurrent relays.
    delay = Math.random() * delay;
  }

  return Math.floor(delay);
}

/**
 * Resolve when (Date) the next retry should occur, or null if the
 * record has exhausted its attempts and should go dead.
 */
export function nextRetryAt(
  config: RetryConfig,
  attempts: number,
  now: Date = new Date(),
): Date | null {
  if (attempts >= config.maxAttempts) return null;
  const delay = computeBackoff(config, attempts);
  return new Date(now.getTime() + delay);
}
