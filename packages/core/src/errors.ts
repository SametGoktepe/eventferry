import type { StandardSchemaV1 } from "./standard-schema.js";

/**
 * Thrown when an outbox payload fails its topic's schema — at `enqueue`
 * (before the DB insert) or at `decode` (malformed bytes / schema mismatch).
 * Carries the topic and the validator's structured issues for diagnostics.
 */
export class OutboxValidationError extends Error {
  readonly topic: string;
  readonly issues: ReadonlyArray<StandardSchemaV1.Issue>;

  constructor(
    topic: string,
    issues: ReadonlyArray<StandardSchemaV1.Issue>,
    options?: { cause?: unknown },
  ) {
    const first = issues[0]?.message ?? "unknown validation error";
    super(`Outbox payload for "${topic}" failed validation: ${first}`, options);
    this.name = "OutboxValidationError";
    this.topic = topic;
    this.issues = issues;
  }
}
