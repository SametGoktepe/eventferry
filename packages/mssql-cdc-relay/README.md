# @eventferry/mssql-cdc-relay

[![npm](https://img.shields.io/npm/v/@eventferry/mssql-cdc-relay.svg)](https://www.npmjs.com/package/@eventferry/mssql-cdc-relay)

SQL Server **Change Data Capture** waker for [`@eventferry/mssql`](https://www.npmjs.com/package/@eventferry/mssql) — sub-second outbox latency without polling.

Provides:

- `MssqlCdcWaker` — a `Waker` implementation that streams committed outbox
  inserts off the `cdc.dbo_<outbox>_CT` change table and fires the relay's
  claim loop on each new LSN, **without** a busy-polling `pollIntervalMs`.
- `createCdcEnablementSql` — idempotent DDL to enable CDC on the database,
  enable CDC on the outbox table, create the watermark table, and grant the
  runtime role `db_datareader` on the change table.
- `MssqlCdcRelay` — thin sugar over `new Relay({ store, publisher, waker })`
  that wires the CDC waker for you.

## WHY: CDC vs polling

The default `@eventferry/mssql` relay is a polling loop — it scans the outbox
on a `pollIntervalMs` interval (default 250ms) looking for new claimable rows.
That floor sets your **publish latency**: `p50 ≈ pollIntervalMs / 2`,
`p99 ≈ pollIntervalMs`. Driving it below 100ms means hammering the outbox
table with empty claims under low write load — a real DBA conversation.

This package replaces the poll loop with a **log-tail of the CDC change table**.
SQL Server's CDC capture job tails the transaction log asynchronously (~5s
default capture cadence; tunable to 1s) and writes one row to
`cdc.dbo_<outbox>_CT` per committed outbox insert. `MssqlCdcWaker` polls
that change table (not the outbox) on a tight interval, calls
`waker.onChange()` the moment new LSNs appear, and the relay's claim loop
fires immediately.

End-to-end latency drops to **sub-second** (typically 100–500ms, dominated by
the CDC capture cadence) without the polling-cost / latency trade-off.

## Requirements

- **SQL Server 2008+** on-prem, **SQL Server on Linux 2017+** in containers,
  or **Azure SQL Managed Instance**. (Azure SQL Database is **not** supported
  — see [NOT FOR](#not-for) below.)
- **SQL Server Agent must be running** — CDC is built on the capture job
  (`cdc.<db>_capture`) and the cleanup job (`cdc.<db>_cleanup`), both
  scheduled by SQL Agent. If Agent is stopped, the change table stops
  receiving rows, `MssqlCdcWaker` stops firing, and you'll trip
  `CdcCaptureJobStoppedError` (see [Failure modes](#failure-modes)).
- **Permissions**:
  - **Setup** (one-time, via `createCdcEnablementSql`): `db_owner` —
    `sys.sp_cdc_enable_db` and `sys.sp_cdc_enable_table` both require it.
  - **Runtime** (the role/login your app connects with): `db_datareader` on
    `cdc.dbo_<outbox>_CT` and `SELECT` on `cdc.lsn_time_mapping`. The setup
    SQL grants these to a role you specify; do **not** run the runtime
    connection as `db_owner`.
- **Node.js 18+**
- **`mssql` ^10 || ^11 || ^12** as a peer dep (same driver `@eventferry/mssql` uses).
- **`@eventferry/mssql`** for the underlying store and migration.

## NOT FOR

**Azure SQL Database** (the single-DB serverless / vCore PaaS offering — not
Managed Instance) does not run SQL Server Agent. CDC depends on the capture
job, which depends on Agent, so **CDC is unavailable on Azure SQL Database**.
`sys.sp_cdc_enable_db` returns an error there.

On Azure SQL Database you have two options:

- Use the default **polling relay** from `@eventferry/mssql` — tune
  `pollIntervalMs` down (100–250ms is fine).
- Wait for the planned **`MssqlServiceBrokerWaker`** (Service Broker is
  available on Azure SQL DB and gives a notification-driven waker without CDC).

This package will throw `CdcNotEnabledError` at runtime if you point it at
Azure SQL Database, but the SKU check happens lazily on the first capture
probe — fail fast in your bootstrap by calling
`createCdcEnablementSql` against a non-SQL-DB environment first.

## Install

```bash
npm i @eventferry/mssql-cdc-relay @eventferry/mssql @eventferry/core mssql
```

## Quick start

```ts
import * as sql from "mssql";
import { MssqlStore, createMigrationSql } from "@eventferry/mssql";
import {
  MssqlCdcWaker,
  createCdcEnablementSql,
} from "@eventferry/mssql-cdc-relay";
import { Relay } from "@eventferry/core";
import { KafkaPublisher } from "@eventferry/kafka";

// 1) Build and connect the pool (same as @eventferry/mssql).
const pool = await new sql.ConnectionPool({
  server: "localhost",
  user: "sa",
  password: "...",
  database: "shop",
  options: { encrypt: true, trustServerCertificate: false },
  requestTimeout: 30_000,
}).connect();

pool.on("error", (err) => console.error("[mssql pool]", err));

// 2) Apply the outbox migration (idempotent).
await pool.request().batch(createMigrationSql("outbox"));

// 3) Apply the CDC enablement migration (idempotent; requires db_owner ONCE).
//    - Enables CDC on the database.
//    - Enables CDC on dbo.outbox with capture_instance = 'dbo_outbox'.
//    - Creates dbo.outbox_cdc_watermark.
//    - Grants db_datareader on cdc.dbo_outbox_CT to the runtime role.
await pool.request().batch(
  createCdcEnablementSql({
    table: "outbox",
    schema: "dbo",
    runtimeRole: "outbox_app",
  }),
);

// 4) Construct the store + waker.
const store = new MssqlStore({ pool });
const waker = new MssqlCdcWaker({
  pool,
  table: "outbox",
  schema: "dbo",
  // Optional: how often to probe cdc.fn_cdc_get_all_changes (default 250ms).
  // The CDC capture job itself runs every ~5s by default; you can tune the
  // capture cadence to 1s via sys.sp_cdc_change_job for tighter latency.
  pollIntervalMs: 250,
});

// 5) Hand both to the Relay.
const publisher = new KafkaPublisher({
  driver: "kafkajs",
  brokers: ["localhost:19092"],
  idempotent: true,
});

const relay = new Relay({
  store,
  publisher,
  waker, // <-- this is the only difference from the polling setup
  dlq: { topic: "orders.dlq" },
});

await relay.start();
process.on("SIGTERM", async () => {
  await relay.stop();
  await waker.stop();
});
```

The `enqueue` path is unchanged from `@eventferry/mssql` — you still call
`store.enqueue(tx, msg)` inside your business transaction, and the row commits
to `dbo.outbox` exactly as before. CDC picks it up asynchronously and the
waker fires the relay.

## Watermark table

`createCdcEnablementSql` creates `dbo.outbox_cdc_watermark` (one row, one
column: `last_processed_lsn BINARY(10)`). The waker reads this on startup,
calls `cdc.fn_cdc_get_all_changes_dbo_outbox(@from_lsn, @to_lsn, 'all')` for
the half-open window `(last_processed_lsn, sys.fn_cdc_get_max_lsn()]`, and
on success **writes the new high-water LSN back** in the same connection.

**Why a watermark table and not in-memory state?** Relays restart, deploy,
crash. If the waker only tracked the last LSN in memory, every restart would
either replay every CDC row since the retention floor (idempotent at the
publisher layer, but expensive) or skip rows committed between
`sys.fn_cdc_get_max_lsn()` at startup and the next outbox commit. The
watermark gives you exactly-once **wake-up** semantics across restarts.

**When to truncate the watermark.** Almost never. The legitimate cases:

- You've manually purged old outbox rows and reset the CDC capture instance
  (rare — usually you just let CDC retention handle it).
- You're migrating capture instances (e.g. schema change) and want the new
  one to start from "now".
- You've nuked and recreated the outbox table during a destructive migration.

**What happens if you delete (or truncate) the watermark.** On the next
waker tick, the row is missing, so `last_processed_lsn` is read as `NULL`
and the waker treats that as "start from `sys.fn_cdc_get_max_lsn()`". This
means **any outbox rows committed between the deletion and the next waker
tick will not trigger a waker fire** — they sit on the outbox until the
backstop poll (the relay still has a slow `pollIntervalMs` floor for exactly
this reason; the waker is a latency optimization, not a correctness one).
Once the polling claim picks them up, normal processing resumes. So the
window of degraded latency is bounded by the backstop poll interval, not
unbounded.

**Why `BINARY(10)`?** SQL Server LSNs are 10-byte values (`0x0000xxxxxx…`).
Storing as `BINARY(10)` matches the `cdc.fn_cdc_*` parameter types exactly
and lets the engine do range comparisons without conversion.

## Failure modes

The waker surfaces four typed errors. None of them are recoverable by
retrying inside the waker — they all require an operator decision.

### `CdcNotEnabledError`

Thrown on startup when `sys.databases.is_cdc_enabled = 0`, or when
`sys.tables.is_tracked_by_cdc = 0` for the outbox table, or when the
capture instance `dbo_<table>` doesn't exist in `cdc.change_tables`.

**What it means.** CDC was never enabled, or someone ran
`sys.sp_cdc_disable_table` / `sys.sp_cdc_disable_db`.

**What to do.** Re-run `createCdcEnablementSql` as a principal with `db_owner`.
If you're on Azure SQL Database, this error is permanent — switch to the
polling relay or wait for `MssqlServiceBrokerWaker`.

### `CdcRetentionExceededError`

Thrown when the watermark's `last_processed_lsn` is **older than**
`sys.fn_cdc_get_min_lsn('dbo_outbox')`. CDC has a retention window (default
72h) and the cleanup job hard-deletes rows older than that from the change
table. If the relay was down longer than retention, the watermark points at
an LSN that no longer exists in the change table.

**What it means.** The waker cannot replay the gap because CDC has already
forgotten about it. **The outbox rows themselves are not lost** — they're
still in `dbo.outbox`, claimable by the polling backstop. But the waker
needs to know whether to skip the gap (and rely on the polling backstop to
clean it up) or fail loudly.

**What to do.** This error is **fail-loud by default** to surface the
operational problem. To recover, the operator should:

1. Set `last_processed_lsn = sys.fn_cdc_get_max_lsn()` in the watermark
   table (skip the gap; polling backstop handles missed rows).
2. Restart the relay.
3. Investigate why the relay was down longer than CDC retention — usually
   either retention is too short for your deployment cadence
   (`sys.sp_cdc_change_job @job_type = 'cleanup', @retention = 4320` for
   72h) or your deploy/incident took too long.

### `WatermarkBelowMinLsnError`

Variant of the above raised when the watermark is non-`NULL` but **below**
`min_lsn` by a margin too large to be a clock skew or capture-job lag
explanation — i.e. clearly a stale state. Separated so monitoring can
distinguish "we were down for 80h" (`CdcRetentionExceededError`) from "the
watermark looks corrupted" (`WatermarkBelowMinLsnError`).

**What to do.** Same recovery as `CdcRetentionExceededError`, plus
investigate how the watermark got into that state — usually a manual edit
or a bad rollback.

### `CdcCaptureJobStoppedError`

Thrown when the waker observes `sys.fn_cdc_get_max_lsn()` stuck at the same
value for longer than `captureStalenessThresholdMs` (default 60s) while
the outbox table is receiving commits. This is the symptom of a stopped
SQL Agent or a disabled capture job.

**What it means.** New outbox rows are landing in `dbo.outbox` (the
business transactions are committing fine), but CDC isn't tailing the log
into the change table, so the waker is blind to them. The polling backstop
in the underlying relay will still drain the outbox — latency degrades from
sub-second to `pollIntervalMs`, but nothing is lost.

**What to do.** Check SQL Agent (`SELECT * FROM msdb.dbo.sysjobs WHERE name
LIKE 'cdc.%_capture'`), check `sys.dm_cdc_log_scan_sessions` for capture
errors, and restart the capture job
(`EXEC sys.sp_cdc_start_job @job_type = 'capture'`). The waker recovers
automatically once `max_lsn` advances again.

## Running outside Azure

### On-prem SQL Server (2008+)

- **SQL Agent must be running and set to start automatically.** Default-
  installed Express editions ship with Agent disabled — CDC needs Standard
  or higher anyway, but on Developer / Standard installs verify Agent is
  enabled in `services.msc`.
- **Capture cadence is tunable.** Default is `pollingInterval = 5` (seconds),
  which is the CDC capture job's loop sleep, not anything to do with our
  waker. Tune it down with:

  ```sql
  EXEC sys.sp_cdc_change_job
    @job_type     = 'capture',
    @pollinginterval = 1;        -- seconds; 0..86400
  ```

  Sub-second waker latency is **floor-limited by this value** — the change
  table doesn't get rows until the capture job runs.
- **Retention.** Default is 72h (`@retention = 4320` minutes). If you do
  rolling deploys with multi-hour blast doors, raise it.

### Azure SQL Managed Instance

- **CDC is enabled by default at the instance level** — but you still need
  to enable it per-database (`sys.sp_cdc_enable_db`) and per-table
  (`sys.sp_cdc_enable_table`). `createCdcEnablementSql` does both.
- **SQL Agent is built in and always running** on MI — you don't need to
  start it, but you do need to verify the capture job exists after enablement
  (`SELECT * FROM msdb.dbo.sysjobs WHERE name LIKE 'cdc.%_capture'`).
- **Permissions are stricter on MI.** The runtime login should be a contained
  database user mapped to an Azure AD principal where possible; the
  `runtimeRole` grant in `createCdcEnablementSql` works the same way as
  on-prem.

### SQL Server on Linux containers (2017+)

- **CDC is fully supported on Linux from SQL Server 2017 onwards.** Earlier
  releases (`mssql-server-linux:2016`) do **not** support CDC — the engine
  was missing the change-table machinery.
- **SQL Agent is enabled differently on Linux.** It's a separate `mssql-conf`
  setting (`sudo /opt/mssql/bin/mssql-conf set sqlagent.enabled true`,
  then `systemctl restart mssql-server`). In Docker, set the env var
  `MSSQL_AGENT_ENABLED=true` on the container.
- **The official `mcr.microsoft.com/mssql/server:2022-latest` image ships
  Agent disabled by default** — every team trips on this once. Verify with
  `SELECT @@SERVICENAME, SERVERPROPERTY('IsHadrEnabled')` and check Agent
  status via `EXEC msdb.dbo.sp_help_job`.

## License

MIT
