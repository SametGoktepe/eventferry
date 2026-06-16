---
"@eventferry/core": minor
"@eventferry/kafka": minor
---

**feat: error classification for smarter retry, DLQ, and pause behavior**

Publisher implementations can now tag each failed `PublishResult` with an `errorKind` so the relay knows whether the error is worth retrying.

**New in `@eventferry/core`:**

- `PublishErrorKind = "retriable" | "fatal" | "poison" | "backpressure" | "quota"` — opt-in classification surface on `PublishResult.errorKind`.
- The `Relay` now reads `errorKind`:
  - `"fatal"` (auth denied, fenced epoch, transactional id rejected) and `"poison"` (oversized record, corrupt payload, schema rejected) **short-circuit retries** straight to the DLQ + `dead` status. No more burning the retry budget on errors that cannot succeed.
  - `"retriable"`, `"backpressure"`, `"quota"`, and absent (`undefined`) continue to use the existing backoff schedule, preserving backward compatibility. Smarter `backpressure` / `quota` handling (pause polling, longer backoff) is planned for a follow-up release.

**New in `@eventferry/kafka`:**

- `classifyKafkajsError(err): PublishErrorKind` — maps the most-common `KafkaJSProtocolError` types/codes and the `KafkaJSConnectionError` / `KafkaJSRequestTimeoutError` / `KafkaJSNonRetriableError` subclasses to a category. Verified against `kafkajs/src/errors.js`.
- `classifyConfluentError(err): PublishErrorKind` — maps the librdkafka `RD_KAFKA_RESP_ERR_*` codes (both negative internal codes and Kafka wire-protocol codes) to a category. Verified against `librdkafka/src/rdkafka.h`. Includes the dedicated `"backpressure"` mapping for `ERR__QUEUE_FULL` (-184) and `"quota"` for `ERR_THROTTLING_QUOTA_EXCEEDED` (89).
- Both drivers (`KafkaJsDriver`, `ConfluentDriver`) now call their respective classifier in the catch path and emit the `errorKind` on every failed `PublishResult`.

**Backward compatibility:** `errorKind` is optional everywhere. Existing publisher implementations that don't set it continue to work unchanged — the relay treats absent `errorKind` as `"retriable"`, which is what the relay did before this change.

**Migration:** none required.
