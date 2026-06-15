import { describe, expect, it } from "vitest";
import { computeBackoff, nextRetryAt } from "../src/backoff.js";
import type { RetryConfig } from "../src/types.js";

const base: RetryConfig = {
  maxAttempts: 5,
  strategy: "exponential",
  baseMs: 100,
  maxMs: 10_000,
  jitter: false,
};

describe("computeBackoff", () => {
  it("fixed returns baseMs", () => {
    const c = { ...base, strategy: "fixed" as const };
    expect(computeBackoff(c, 1)).toBe(100);
    expect(computeBackoff(c, 4)).toBe(100);
  });

  it("linear scales with attempt", () => {
    const c = { ...base, strategy: "linear" as const };
    expect(computeBackoff(c, 1)).toBe(100);
    expect(computeBackoff(c, 3)).toBe(300);
  });

  it("exponential doubles each attempt", () => {
    expect(computeBackoff(base, 1)).toBe(100);
    expect(computeBackoff(base, 2)).toBe(200);
    expect(computeBackoff(base, 3)).toBe(400);
  });

  it("clamps to maxMs", () => {
    const c = { ...base, maxMs: 500 };
    expect(computeBackoff(c, 10)).toBe(500);
  });

  it("jitter keeps result within [0, delay]", () => {
    const c = { ...base, jitter: true };
    for (let i = 0; i < 100; i++) {
      const v = computeBackoff(c, 3); // base delay 400
      expect(v).toBeGreaterThanOrEqual(0);
      expect(v).toBeLessThanOrEqual(400);
    }
  });
});

describe("nextRetryAt", () => {
  it("returns null once attempts reach maxAttempts", () => {
    expect(nextRetryAt(base, 5)).toBeNull();
    expect(nextRetryAt(base, 6)).toBeNull();
  });

  it("returns a future date while retries remain", () => {
    const now = new Date("2025-01-01T00:00:00Z");
    const at = nextRetryAt(base, 1, now);
    expect(at).not.toBeNull();
    expect(at!.getTime()).toBeGreaterThan(now.getTime());
  });
});
