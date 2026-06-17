# @eventferry/core

## 3.1.0

### Minor Changes

- da39b08: **feat: producer tuning passthrough + per-message partition override + kafkajs partitioner choice**

  ### Producer tuning

  `KafkaPublisher` now accepts the full set of producer tuning knobs every serious Kafka deployment eventually needs:

  ```ts
  new KafkaPublisher({
    driver: "confluent",
    brokers,
    lingerMs: 25, // ⚠ confluent only
    batchSize: 131_072, // ⚠ confluent only
    maxInFlightRequests: 5,
    requestTimeoutMs: 30_000,
    deliveryTimeoutMs: 120_000, // ⚠ confluent only
    maxRequestSize: 2_000_000, // ⚠ confluent only
    transactionTimeoutMs: 90_000,
  });
  ```

  **Driver asymmetry:** `kafkajs` has no producer-level config for `lingerMs`, `batchSize`, `deliveryTimeoutMs`, or `maxRequestSize` — its batching is sticky-partitioner + hardcoded internals. The typed API stays uniform; on the kafkajs driver, those four knobs log a **one-time** warning (deduped process-wide) and are otherwise ignored. For fine-grained tuning, switch to the confluent driver.

  ### Per-message partition override

  `PublishableMessage` gains an optional `partition?: number` field. When set, the publisher routes that record to the exact partition, bypassing the configured partitioner. Use cases: compacted topics with application-managed sharding, tenant-affinity routing, geo-pinning. Both drivers honor it.

  ### kafkajs partitioner choice

  Silences the noisy `KafkaJSPartitionerNotSpecified` warning kafkajs v2 emits on every producer instance, by letting you pick a partitioner explicitly:

  ```ts
  new KafkaPublisher({
    driver: "kafkajs",
    brokers,
    partitioner: "java-compatible", // (default) | "legacy" | "default"
  });
  ```

  - `"java-compatible"` is the new greenfield default (matches the Java client's murmur2).
  - `"legacy"` preserves pre-v2 hash continuity for existing topics.
  - `"default"` follows kafkajs's current default.

  ### Backward compatibility

  Pure-additive. Existing call sites continue to work unchanged; the partitioner-choice default (`"java-compatible"`) is what kafkajs v2's migration guide recommends for new producers.

## 3.0.0

### Minor Changes

- f0c7483: **feat: error classification for smarter retry, DLQ, and pause behavior**

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

## 2.0.0

## 1.0.4

### Patch Changes

- 64d115d: docs / metadata: expand `keywords` on all packages for better npm and LLM discoverability (outbox-pattern, dual-write, cdc, event-driven, microservices, etc.). No code changes.

## 1.0.3

### Patch Changes

- aaca9a2: docs: use a non-expiring `2026-present` copyright year in LICENSE and a static MIT license badge in the README

## 1.0.2

### Patch Changes

- 89f1867: Declare `engines.node` (>=18) so npm shows the supported Node version and tooling can warn on unsupported runtimes.

## 1.0.1

### Patch Changes

- docs: polish per-package READMEs (npm page content). No code changes.

## 1.0.0

### Minor Changes

- b06f8ec: Add a low-latency notify-driven relay (Postgres `LISTEN`/`NOTIFY`).

  - **core:** new `Waker` interface and an optional `Relay({ waker })`. The relay's
    idle wait is now interruptible — when the waker signals, it claims immediately
    instead of sleeping out `pollIntervalMs`. With no waker, behavior is unchanged.
  - **postgres:** `PostgresNotifyWaker` holds a dedicated `LISTEN` connection and
    wakes the relay on each notification, reconnecting with backoff if it drops.
    `createNotifyTriggerSql(table, channel)` emits an `AFTER INSERT FOR EACH STATEMENT`
    trigger that `pg_notify`s on commit (empty payload — the relay re-claims).
  - Polling remains the safety net: a missed notification is caught by the next poll,
    so no event is lost. All ordering/retry/DLQ/crash-recovery guarantees are unchanged.
  - No new dependencies (`LISTEN`/`NOTIFY` is native to `pg`).

- b06f8ec: Add a streaming relay that publishes straight from the Postgres WAL (logical replication).

  - **postgres:** `PostgresStreamingRelay` consumes INSERTs on the outbox table via
    `pg-logical-replication` + `pgoutput` (built-in, no DB extension) and publishes them
    with no claim query on the happy path — lower DB load than the notify waker. A failed
    publish is demoted to `failed`; an internal claim-based retry loop drains it with the
    existing backoff / DLQ / dead handling. `pg-logical-replication` is a new **optional**
    peer dependency, loaded only in streaming mode.
  - **postgres:** `PostgresStore` gains `claimFailedOnly` (claims only `failed`/timed-out
    `processing` rows, never `pending`) so the stream owns pending rows with no duplication.
    `createPublicationSql(table, publication)` emits an idempotent insert-only publication.
  - **core:** the record→message builder is extracted as `buildPublishable(record,
serializer)` and shared by `Relay` and the streaming relay (no behavior change).
  - **At-least-once:** the slot's LSN is acknowledged only after a batch's side effects
    commit; a crash re-streams and re-publishes (idempotent consumers absorb the duplicate).
  - **Ordering:** streaming is best-effort per-aggregate (a retried failure may land after
    later same-aggregate rows). Use the polling relay for the strict head-of-aggregate
    guarantee. Requires `wal_level = logical`.

- b06f8ec: Strict per-aggregate ordering, crash recovery, and driver/packaging fixes.

  - **postgres:** the claim query now enforces strict per-aggregate ordering by
    only taking the _head_ of each aggregate (no earlier unfinished row for the
    same `aggregateId`). At most one in-flight message per aggregate; failed
    messages block their successors until resolved.
  - **postgres:** added a `claimed_at` column and a visibility-timeout reaper
    (`claimTimeoutMs`, default 60s) so rows orphaned by a crashed relay are
    reclaimed instead of stuck in `processing` forever. Migration is upgrade-safe
    (`ADD COLUMN IF NOT EXISTS`); the partial indexes were retuned for the new
    ordered, reaper-aware claim.
  - **core:** dead-lettered messages now carry the real `original-topic` header
    (previously always empty); `ConsoleLogger` routes warn/error to the matching
    `console` methods.
  - **kafka:** the confluent driver now honors `acks` and `compression` (it
    silently ignored them before), matching the kafkajs driver.
  - **packaging:** the `@eventferry/postgres/migrations` subpath export now
    advertises its types; `pnpm-workspace.yaml` dropped an invalid placeholder
    block.

  Note: `claimTimeoutMs` should exceed your worst-case publish latency. This is
  an at-least-once system — pair it with idempotent producers/consumers.

- b06f8ec: Add a type-safe event registry: `defineOutbox`.

  Declare each topic once (`{ aggregateType, schema }`) and get a typed, runtime-
  validated `enqueue` plus a `decode` helper consumers can reuse from the same
  registry. Payloads are validated before the row is inserted, so a malformed event
  rolls back with the rest of your transaction instead of reaching the outbox.

  - **Validator-agnostic:** any [Standard Schema](https://standardschema.dev) works
    (Zod 3.24+, Valibot, ArkType, …). The spec interface is inlined, so `@eventferry/core`
    gains no runtime dependency.
  - **Producer + consumer:** `defineOutbox(registry, { store })` exposes typed
    `enqueue`; `defineOutbox(registry)` (no store) exposes `decode`/`validate` for
    consuming services.
  - New `OutboxValidationError` carries the failing topic and the validator's issues.
  - Purely additive — `PostgresStore`, `Relay`, and untyped `store.enqueue` are unchanged.

- b06f8ec: Add W3C trace propagation (OpenTelemetry-compatible), dependency-free.

  - **core:** new `Tracing` interface (`inject(carrier)`), the shape of an OpenTelemetry
    `TextMapPropagator` — the library depends on no tracing package.
  - **postgres:** `PostgresStore({ tracing })` captures the active W3C
    `traceparent`/`tracestate` into the row's headers at `enqueue`, so it rides along to
    the published message (on every path — polling, notify, streaming — since headers
    already pass through) and the consumer can continue the trace.
  - The caller's `headers` object is never mutated. With no `tracing` configured,
    behavior is unchanged. The existing `trace-id` header stays for simple correlation.
  - OpenTelemetry/Datadog/custom integrate via a ~5-line adapter (documented, not bundled).
