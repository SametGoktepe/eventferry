# @eventferry/mssql

[![npm](https://img.shields.io/npm/v/@eventferry/mssql.svg)](https://www.npmjs.com/package/@eventferry/mssql)

SQL Server (Azure SQL DB / Managed Instance / on-prem) outbox store for [@eventferry](https://github.com/SametGoktepe/eventferry).

Provides:

- `MssqlStore` — transaction-joining `enqueue` and a lock-free `claimBatch`
  using `READPAST + UPDLOCK + ROWLOCK + READCOMMITTEDLOCK`, with **strict
  per-aggregate ordering** and a **crash-recovery reaper** (visibility timeout).
- `createMigrationSql` — idempotent, per-object schema-qualified DDL generator.
- `createRetentionIndexSql` — separate retention-index emitter for parity with
  the other adapters.
- `purgeDone` — batched retention of `done` rows, with `DATETIME2` cutoff
  binding to dodge the 32-bit ms overflow on long retention windows.

## Requirements

- **SQL Server 2016 SP1+** (compatibility level 130) — needed for `OPENJSON`
  GA and filtered-index `WHERE` clauses. Also works on Azure SQL Database and
  Azure SQL Managed Instance.
- **Node.js 18+**
- **`mssql` ^10 || ^11 || ^12** as a peer dep (the driver).

## Install

```bash
npm i @eventferry/mssql @eventferry/core mssql
```

## Quick start

```ts
import * as sql from "mssql";
import { MssqlStore, createMigrationSql } from "@eventferry/mssql";

// 1) Build and connect the pool.
const pool = await new sql.ConnectionPool({
  server: "localhost",
  user: "sa",
  password: "...",
  database: "shop",
  options: { encrypt: true, trustServerCertificate: false },
  // requestTimeout MUST exceed your expected claim-batch latency. The mssql
  // default is 15s; if you run large batches under load, raise it to 30-60s.
  requestTimeout: 30_000,
}).connect();

// 2) CRITICAL: attach the pool error listener BEFORE constructing the store.
// `mssql` emits `error` for connection-level failures (TDS resets, transient
// Azure SQL drops, etc.); without a listener, Node crashes the whole process.
// The store does not attach this listener — the pool lifecycle belongs to you.
pool.on("error", (err) => {
  // wire to your logger / metrics
  console.error("[mssql pool]", err);
});

// 3) Apply the migration (idempotent, per-object).
await pool.request().batch(createMigrationSql("outbox"));

// 4) Construct the store.
const store = new MssqlStore({ pool });

// 5) Enqueue inside YOUR business transaction.
const tx = new sql.Transaction(pool);
await tx.begin();
try {
  await new sql.Request(tx)
    .input("id", sql.NVarChar(64), order.id)
    .query("INSERT INTO orders (id, total) VALUES (@id, @total)");

  await store.enqueue(tx, {
    topic: "orders.created",
    aggregateType: "order",
    aggregateId: order.id,
    payload: { orderId: order.id, total: order.total },
  });

  await tx.commit();
} catch (err) {
  await tx.rollback();
  throw err;
}
```

Hand the store to a `Relay` from `@eventferry/core` together with a publisher
from `@eventferry/kafka`:

```ts
import { Relay } from "@eventferry/core";
import { KafkaPublisher } from "@eventferry/kafka";

const publisher = new KafkaPublisher({
  driver: "kafkajs",
  brokers: ["localhost:19092"],
  idempotent: true,
});

const relay = new Relay({ store, publisher, dlq: { topic: "orders.dlq" } });
await relay.start();
process.on("SIGTERM", () => relay.stop());
```

## Migrations

```ts
createMigrationSql(
  table = "outbox",
  opts?: { schema?: string; useNativeJson?: boolean },
): string;
```

Emits a single batch with three guarded blocks:

1. `IF OBJECT_ID(N'[schema].[table]', N'U') IS NULL` → `CREATE TABLE …`
2. `IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_<table>_agg_id_id' …)` → `CREATE NONCLUSTERED INDEX …`
3. Same guarded `CREATE` for `IX_<table>_claim_ready` and `IX_<table>_done_processed_at`.

**Idempotency story.** Each object is guarded independently, so a *partial*
deployment (e.g. table created by a DBA script, indexes missed) is **repaired**
by re-running the migration — no `2714 object already exists` errors, no
dropped data, no manual reconciliation.

**`schema` option (default `"dbo"`).** Both the `OBJECT_ID` guard and the
`CREATE TABLE`/`CREATE INDEX` statements are fully `[schema].[table]`-qualified.
Without schema qualification, a non-`dbo` default schema (Azure AD logins,
contained DB users, AG per-app schemas) would cause the guard to inspect
`dbo.outbox` while `CREATE` lands in your default schema — duplicate tables
across schemas, or `2714` on the second run. Matches Postgres's `public`
default semantically.

Both `schema` and `table` are validated by `assertIdent`
(`/^[a-zA-Z_][a-zA-Z0-9_]{0,99}$/`) **before** interpolation into brackets.
The 100-char length cap exists so embedded constraint names like
`CK_<table>_payload_json` fit within SQL Server's 128-char object name limit.

```ts
// Default — table "outbox" in schema "dbo".
const ddl = createMigrationSql();

// Non-default schema, NVARCHAR(MAX) + ISJSON CHECK columns (the default).
const ddl2 = createMigrationSql("outbox", { schema: "messaging" });

// Same, but with the SQL 2025+ native json column type opted in.
const ddl3 = createMigrationSql("outbox", {
  schema: "messaging",
  useNativeJson: true,
});
```

## `useNativeJson` opt-in (default: `false`)

| Flag                 | Payload / headers column type                  | CHECK constraint                  | Engine support                                  |
| -------------------- | ---------------------------------------------- | --------------------------------- | ----------------------------------------------- |
| `false` (default)    | `NVARCHAR(MAX)`                                | `CHECK (ISJSON(col) = 1)`         | SQL Server **2016 SP1+**, Azure SQL DB, MI      |
| `true`               | `json`                                         | **omitted** (rejected on `json`)  | SQL Server **2025+**, current Azure SQL DB only |

**Why `false` is the default.** `NVARCHAR(MAX) + ISJSON` covers every engine
in our support matrix and the validation is enforced by the engine. The wire
behaviour is identical to native `json`: TDS still serialises as `NVARCHAR(MAX)`
on read, so the JS code path is `JSON.stringify` in, `JSON.parse` out either
way. Native `json` is a storage-format and engine-internal-parser optimisation,
not a wire-format change.

Enable `useNativeJson: true` only when **all** of your environments
(dev, CI, staging, prod, DR) are SQL Server 2025+ or Azure SQL DB. Migrating
the table later requires a column-type change with downtime.

## Concurrency model

The claim CTE uses table hint `WITH (READCOMMITTEDLOCK, READPAST, UPDLOCK, ROWLOCK)`
on the outer scan, and `WITH (READCOMMITTEDLOCK)` on the inner `NOT EXISTS`
head-of-aggregate probe.

| Hint                  | Reason                                                                                                                                                                                                                            |
| --------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `READCOMMITTEDLOCK`   | On a **RCSI** database (`READ_COMMITTED_SNAPSHOT ON` — the Azure SQL DB default, and widely enabled on on-prem), the default `READ COMMITTED` becomes snapshot-based. `READPAST` and `UPDLOCK` are *silently no-ops under RCSI* unless `READCOMMITTEDLOCK` re-asserts locking semantics for this statement. Without it, concurrent relays double-claim. |
| `READPAST`            | Skip rows another claimer currently holds an X/U lock on. Required so concurrent relays don't serialize.                                                                                                                          |
| `UPDLOCK`             | Take a U-lock at scan time, converted to X-lock at update time. Prevents the lost-update race between the CTE's `SELECT` phase and the `UPDATE` phase.                                                                              |
| `ROWLOCK`             | Disable lock escalation. **Mandatory** with `READPAST` — without it, SQL Server escalates to page locks under contention, `READPAST` silently degrades, and concurrent relays serialize.                                          |

The inner `NOT EXISTS` reference deliberately **does not** carry `READPAST`.
If it did, sibling claimers would skip each other's locked earlier rows,
`NOT EXISTS` would return `TRUE` incorrectly, and a later row of the same
aggregate would get claimed while an earlier row was still in flight —
breaking the head-of-aggregate invariant the design promises. Plain
`READCOMMITTEDLOCK` makes the inner probe briefly block on a competitor's
`UPDLOCK` and see the committed status afterwards. This is the right
behaviour.

### Do NOT use clustered columnstore on the outbox table

`ROWLOCK` is **not compatible** with clustered columnstore indexes — CCI is
delta-store + row-group based and ignores row-granularity hints. If you put
a clustered columnstore on the outbox table, the claim CTE's locking degrades
to row-group level, `READPAST` no longer skips correctly, and concurrent
relays serialize or double-claim. The default `PRIMARY KEY CLUSTERED (id)`
B-tree the migration emits is exactly what this design requires; do not
change it.

## BIGINT id returned as string

`tedious` (the underlying driver) returns SQL `BIGINT` columns as **JavaScript
strings** — see `lib/value-parser.js`: `value.toString()`. This is intentional:
JS `number` is IEEE-754 double, with safe-integer ceiling `2^53 - 1`. A long-
running outbox in production will exceed `2^53` after a few quadrillion rows,
and `Number(row.id)` would silently corrupt ids past that boundary.

`MssqlStore` keeps `OutboxRecord.id` as `string` end-to-end. Never `Number()`
the id; never bind it as `sql.Int`. If you need it as a number for a UI,
parse it at the very edge and accept the truncation risk explicitly.

The `message_id` column is `NVARCHAR(64)`, so it is read as a normal JS string
with no special handling. Only `BIGINT` columns get the string treatment.

## `claimTimeoutMs` (default 60s, 24h ceiling)

Visibility timeout for claimed rows. After this many milliseconds without an
ack, the reaper considers the row "stuck" (the relay crashed or the connection
died mid-publish) and reclaims it.

- **Default**: `60_000` ms.
- **Ceiling**: `86_400_000` ms (24h). The constructor throws if you exceed it.

Two reasons for the 24h cap:

1. Any value higher than that is almost always misconfiguration. A real
   25-hour-long publish doesn't exist; what does exist is a copy-pasted
   `claimTimeoutMs: 30 * 24 * 60 * 60 * 1000`.
2. The parameter is bound as `sql.Int`, which is signed-32-bit (max
   `2^31 - 1` ≈ 2_147_483_647 ms ≈ **24.85 days**). The 24h cap leaves a
   safe buffer before overflow.

## `markFailed(id, null, "failed")` is forbidden — runtime `TypeError`

The store throws `TypeError` on `markFailed(id, null, "failed")`. This combination
would set `status = 3` (failed) with `next_retry_at = NULL`, which the claim
predicate treats as "due **now**" — producing an instant-redrive hot loop that
hammers the broker until the row reaches `maxAttempts`.

The cross-adapter contract (Postgres/MySQL) currently *allows* this combination
but it's still a footgun there too — see the open question in
[`final-design.json`](./final-design.json) under the `open_questions` array.
The mssql adapter enforces it at runtime to prevent the hot loop outright.

`markFailed(id, null, "dead")` is allowed — `dead(4)` rows are not picked up
by the claim predicate, so a `NULL` `next_retry_at` has no instant-redrive
hazard.

## `requestTimeout` must exceed expected claim latency

The pool's `requestTimeout` defaults to **15 seconds** in `mssql`. The claim
batch runs a multi-statement `BEGIN TRY / BEGIN TRAN / … / COMMIT` block with
a `TABLE` variable, the head-of-aggregate `NOT EXISTS` probe, and the
`OUTPUT INTO @claimed` write — under contention this can be slower than 15s
on large outboxes.

If `requestTimeout` fires mid-claim:

- The server's `TRY/CATCH` rolls back the transaction (safe, no torn state).
- The client receives an abort error.
- The relay must re-claim the batch, taking real on-call latency.

**Rule of thumb**: set `requestTimeout` to at least 2× your worst observed
claim p99. For the default `claimBatchSize`, 30–60s is a good starting point.

## Do not change session isolation

**Do not** set `SET TRANSACTION ISOLATION LEVEL SERIALIZABLE` or
`REPEATABLE READ` at the session level for connections the store uses, and do
not use a setup hook that does. The claim path's `READCOMMITTEDLOCK` table
hint is precisely tuned to give the right locking semantics on **both**
locking-RC and RCSI databases. Stronger session isolation layers key-range
locks on top, which serialize the claim path and defeat `READPAST`.

The default RC / RCSI behaviour is what you want, and the table hint handles
the rest.

## Waker: polling only in v1

The polling claim loop has no native low-latency waker in v1. The two viable
SQL Server primitives both have hard problems:

- **Service Broker `WAITFOR (RECEIVE …)`** ties up a pool connection
  *indefinitely* waiting for a message. Pool sizing becomes mandatory and the
  maintained Node bindings for Service Broker are abandoned. This is the
  intended v2 path, but it needs a dedicated connection pool concept that
  doesn't exist in `mssql` today.
- **CDC streaming** is the right log-based source but requires SQL Agent
  (so not available on Azure SQL Database — see next section).

For v1, tune `pollIntervalMs` down (e.g. 100ms) if you need sub-second
latency, or use a different adapter (`@eventferry/postgres` ships `LISTEN/NOTIFY`).

## CDC streaming relay: deferred to a separate package

A SQL Server CDC streaming relay (the rough equivalent of `MysqlBinlogRelay`
or `PostgresStreamingRelay`) is **deferred to a separate
`@eventferry/mssql-cdc-relay` package**. Two reasons it's not bundled:

1. **CDC is unavailable on Azure SQL Database** — it requires SQL Server
   Agent, which Azure SQL Database does not provide. Bundling it would force
   every consumer of `@eventferry/mssql` to install a dep they cannot use on
   a major target deployment.
2. CDC capture needs a separate `cdc.fn_cdc_get_all_changes_<capture_instance>`
   path, change-table polling, LSN bookkeeping, and a position-persistence
   contract that's substantively different from the polling claim loop.
   It's its own package.

If you need log-based streaming on SQL Server today, your options are:
on-prem / SQL MI → wait for `@eventferry/mssql-cdc-relay`, or tune the
polling interval down on `@eventferry/mssql`.

## Retention

The outbox table grows; `purgeDone` batch-deletes old `done` rows (run from
your own cron / scheduled job):

```ts
// 7 days
await store.purgeDone({ olderThanMs: 7 * 24 * 60 * 60 * 1000 });

// 30 days, capped at 100k rows per invocation
await store.purgeDone({
  olderThanMs: 30 * 24 * 60 * 60 * 1000,
  batchSize: 1000,
  maxRows: 100_000,
});
```

Internally the cutoff is computed in TypeScript as
`new Date(Date.now() - opts.olderThanMs)` and bound as `sql.DateTime2(3)`.
This **deliberately** sidesteps `sql.Int` + `DATEADD(MILLISECOND, …)`, which
is signed-32-bit and overflows at ~24.85 days — exactly the 30/60/90-day
retention configurations every team writes first.

`maxRows` is a **soft cap**: the loop terminates after the iteration that
crosses it, so actual deletion may exceed `maxRows` by up to `batchSize - 1`
(parity with the Postgres and MySQL adapters).

📖 **Full documentation:** [github.com/SametGoktepe/eventferry](https://github.com/SametGoktepe/eventferry#readme)

## License

MIT
