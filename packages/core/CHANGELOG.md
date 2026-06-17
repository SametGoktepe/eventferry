# @eventferry/core

## 3.3.1

### Patch Changes

- 3c33f71: **chore: ship `CHANGELOG.md` inside the npm tarball**

  Previously, each package's `files` allowlist contained only `"dist"` (and `"sql"` for `@eventferry/postgres`), so the auto-generated `CHANGELOG.md` was never published. Users browsing the package on npmjs.com or unpacking the tarball couldn't see release notes — they had to navigate to the GitHub repo.

  This release adds `"CHANGELOG.md"` to the `files` array of every publishable package. Starting with this version, the per-version release notes are accessible:

  - Directly in `node_modules/@eventferry/<pkg>/CHANGELOG.md` after `npm install`
  - In the file listing on npmjs.com (under the "Code" / "Files" tab, depending on the npm UI)
  - Inside the tarball downloaded from `https://registry.npmjs.org/...`

  No code or API surface changes.

## 3.3.0

### Minor Changes

- cdc20cf: **feat: DLQ enrichment + backpressure runtime + quota multiplier — Tier 1 of the reliability gap closed**

  ### DLQ enrichment

  Records routed to the dead-letter queue now carry the full context an operator needs to triage:

  | Header                      | Set by    | Note                                                                                             |
  | --------------------------- | --------- | ------------------------------------------------------------------------------------------------ |
  | `original-topic`            | relay     | already existed                                                                                  |
  | `dlq-reason`                | publisher | already existed (`error.message`)                                                                |
  | `dlq-failed-at`             | publisher | already existed (ISO timestamp)                                                                  |
  | `dlq-error-class`           | publisher | **new** — `error.name` / constructor name                                                        |
  | `dlq-attempts`              | relay     | **new** — string-encoded `attempts` count                                                        |
  | `dlq-original-aggregate-id` | relay     | **new** — for joining with business state                                                        |
  | `dlq-original-message-id`   | relay     | **new** — for dedup / idempotency lookups                                                        |
  | `dlq-error-stack`           | relay     | **new** — opt-in via `DlqConfig.includeStackTraces`, truncated to `maxStackBytes` (default 4 KB) |

  ```ts
  new Relay({
    store,
    publisher,
    dlq: { topic: "orders.dlq", includeStackTraces: true, maxStackBytes: 4096 },
  });
  ```

  ### Backpressure runtime behavior

  When the driver classifies a failure as `errorKind: "backpressure"` (client-side producer queue full), the relay no longer treats it like a regular retriable failure. Instead:

  - The record is re-queued via the new `OutboxStore.requeue(id, retryAt)` method,
  - `attempts` is **not incremented** — the buffer being full is a "slow down" signal, not the record's fault,
  - The retry is scheduled `RetryConfig.backpressureDelayMs` ms ahead (default 1000 ms).

  Stores that don't implement `requeue` fall back to `markFailed` (with attempts++); both `@eventferry/postgres` and `@eventferry/mysql` ship a real implementation.

  ### Quota multiplier

  When the driver classifies a failure as `errorKind: "quota"` (broker `THROTTLING_QUOTA_EXCEEDED`), the scheduled retry delay is multiplied by `RetryConfig.quotaMultiplier` (default 5) so the producer gives the broker breathing room. Quota failures DO count as attempts — after the budget is exhausted the record routes to DLQ + `dead`.

  ### New / changed types

  - `RetryConfig` gains `backpressureDelayMs?` and `quotaMultiplier?`.
  - `DlqConfig` gains `includeStackTraces?` and `maxStackBytes?`.
  - `OutboxStore.requeue?(recordId, retryAt)` is a new **optional** method. Stores without it fall through to `markFailed`.

  ### Backward compatibility

  Pure-additive everywhere. Default behavior matches the prior release:

  - A `RetryConfig` without `backpressureDelayMs` uses 1000 ms (sensible default).
  - A `DlqConfig` without `includeStackTraces` keeps DLQ messages small (default off).
  - An `OutboxStore` without `requeue` falls back to `markFailed` — same as before, just with a documented quirk.

  This closes the last three Tier 1 items in `docs/kafka-gap-analysis/reliability.md`. Phase A reliability surface is now ~100% complete.

## 3.2.1

### Patch Changes

- 9beb3e2: **chore: migrate to independent versioning (Astro pattern)**

  Fixes the major-version inflation that produced four consecutive surprise majors (`1.0.4 → 2.0.0`, `2.0.0 → 3.0.0`, `3.0.0 → 4.0.0 corrected to 3.1.0`, `3.1.0 → 4.0.0 corrected to 3.2.0`) from changesets whose frontmatter only asked for `minor`.

  **Root cause** (cited in [changesets/changesets#1759](https://github.com/changesets/changesets/issues/1759) and [docs/decisions.md](https://github.com/changesets/changesets/blob/main/docs/decisions.md)): the adapters listed `@eventferry/core` as a `peerDependency` with `workspace:*`. Changesets' documented rule is that an internal bump of a peer forces a major bump on the dependent — and the `fixed: [["@eventferry/*"]]` group reconciler then propagated that major across every package in the group.

  **Fix** (exactly the [Astro config](https://github.com/withastro/astro/blob/main/.changeset/config.json)):

  1. `.changeset/config.json` — drop `fixed`, set `linked: []`, enable
     `___experimentalUnsafeOptions_WILL_CHANGE_IN_PATCH.onlyUpdatePeerDependentsWhenOutOfRange: true`.
  2. Move `@eventferry/core` from `peerDependencies` to `dependencies` in
     `@eventferry/postgres`, `@eventferry/mysql`, `@eventferry/kafka`, and
     `@eventferry/schema-registry`. External user-facing peers (`pg`,
     `mysql2`, `kafkajs`, `@confluentinc/kafka-javascript`,
     `@kafkajs/confluent-schema-registry`) stay unchanged.

  **Effect on releases.** Packages now evolve at independent semver tempos: a `core: minor` changeset produces `core@3.3.0` alongside `postgres@3.2.1` (patch, from "Updated dependencies"). No more major surprises. No more manual force-push corrections.

  **Effect on consumers.** Pure-additive at the install boundary: `npm i @eventferry/kafka` now resolves `@eventferry/core` automatically (it's a regular dep). Previously consumers had to install it themselves as a peer; the typical flow already did this. No source-code changes required.

## 3.2.0

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
