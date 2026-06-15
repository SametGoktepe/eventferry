# @eventferry/core

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
