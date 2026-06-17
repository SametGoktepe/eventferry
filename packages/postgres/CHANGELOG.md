# @eventferry/postgres

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

- Updated dependencies [9beb3e2]
  - @eventferry/core@3.2.1

## 3.2.0

### Patch Changes

- @eventferry/core@3.2.0

## 3.1.0

### Patch Changes

- Updated dependencies [da39b08]
  - @eventferry/core@3.1.0

## 3.0.0

### Patch Changes

- Updated dependencies [f0c7483]
  - @eventferry/core@3.0.0

## 2.0.0

### Patch Changes

- @eventferry/core@2.0.0

## 1.0.4

### Patch Changes

- Updated dependencies [64d115d]
  - @eventferry/core@1.0.4

## 1.0.3

### Patch Changes

- Updated dependencies [aaca9a2]
  - @eventferry/core@1.0.3

## 1.0.2

### Patch Changes

- 89f1867: Declare `engines.node` (>=18) so npm shows the supported Node version and tooling can warn on unsupported runtimes.
- Updated dependencies [89f1867]
  - @eventferry/core@1.0.2

## 1.0.1

### Patch Changes

- docs: polish per-package READMEs (npm page content). No code changes.
- Updated dependencies
  - @eventferry/core@1.0.1

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

- b06f8ec: Add a retention helper: `PostgresStore.purgeDone({ olderThanMs, batchSize?, maxRows? })`.

  Batch-deletes `done` rows whose `processed_at` is older than the cutoff and returns the
  total removed, keeping the outbox table from growing unbounded. Run it periodically
  (your own scheduler). Only `done` rows are purged.

  Also adds `createRetentionIndexSql(table)` — an optional partial index over done rows
  (`WHERE status = 2`) that speeds up the purge scan on high-volume tables; the default
  indexes intentionally exclude done rows, so add this only if retention scans get slow.

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

### Patch Changes

- b06f8ec: The streaming relay now creates its persistent logical replication slot on start if
  it does not already exist (via `pg_create_logical_replication_slot`). Previously it
  assumed the slot was pre-created and `subscribe` would fail otherwise. (Caught by the
  new integration suite against real Postgres.)
- Updated dependencies [b06f8ec]
- Updated dependencies [b06f8ec]
- Updated dependencies [b06f8ec]
- Updated dependencies [b06f8ec]
- Updated dependencies [b06f8ec]
  - @eventferry/core@1.0.0
