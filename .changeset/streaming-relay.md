---
"@eventferry/core": minor
"@eventferry/postgres": minor
---

Add a streaming relay that publishes straight from the Postgres WAL (logical replication).

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
