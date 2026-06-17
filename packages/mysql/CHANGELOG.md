# @eventferry/mysql

## 3.3.1

### Patch Changes

- 3c33f71: **chore: ship `CHANGELOG.md` inside the npm tarball**

  Previously, each package's `files` allowlist contained only `"dist"` (and `"sql"` for `@eventferry/postgres`), so the auto-generated `CHANGELOG.md` was never published. Users browsing the package on npmjs.com or unpacking the tarball couldn't see release notes — they had to navigate to the GitHub repo.

  This release adds `"CHANGELOG.md"` to the `files` array of every publishable package. Starting with this version, the per-version release notes are accessible:

  - Directly in `node_modules/@eventferry/<pkg>/CHANGELOG.md` after `npm install`
  - In the file listing on npmjs.com (under the "Code" / "Files" tab, depending on the npm UI)
  - Inside the tarball downloaded from `https://registry.npmjs.org/...`

  No code or API surface changes.

- Updated dependencies [3c33f71]
  - @eventferry/core@3.3.1

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

### Patch Changes

- Updated dependencies [cdc20cf]
  - @eventferry/core@3.3.0

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

### Minor Changes

- 0085bb1: **feat: MySQL & MariaDB support — `@eventferry/mysql`**

  eventferry now ships with first-class MySQL support, in lockstep parity with the Postgres adapter. If you write to MySQL and publish to Kafka/Redpanda, you can stop hand-rolling the dual-write fix.

  **What you get**

  - **`MysqlStore`** — the same `OutboxStore` contract as `@eventferry/postgres`, ported to MySQL **8.0.1+** and MariaDB **10.6+**. Lock-free claim via `SELECT … FOR UPDATE SKIP LOCKED`, **strict per-aggregate ordering** under concurrent relays (same NOT-EXISTS head guard as Postgres), and a **crash-recovery reaper** so a relay crash between claim and ack never orphans rows.
  - **`createMigrationSql`** — idempotent DDL for the outbox table (InnoDB + utf8mb4 + `DATETIME(3)` + native `JSON` columns). One call, you're set.
  - **`MysqlBinlogRelay`** — a CDC streaming relay that tails the MySQL **binary log** (row-based) via the optional `@vlasky/zongji` peer dep. This is the MySQL analogue of `PostgresStreamingRelay` over WAL, and the same mechanism Debezium uses — but as a Node.js library, not a JVM cluster. Drops latency from "one poll interval" to a few milliseconds and lets you go after high-throughput workloads.
  - **`purgeDone`** — batched retention of published rows, same shape as the Postgres adapter (`DELETE … ORDER BY id LIMIT`).

  **Quick start**

  ```bash
  npm i @eventferry/mysql @eventferry/core mysql2
  ```

  ```ts
  import mysql from "mysql2/promise";
  import { MysqlStore, createMigrationSql } from "@eventferry/mysql";

  const pool = mysql.createPool({ host, user, password, database });
  await pool.query(createMigrationSql("outbox"));

  const store = new MysqlStore({ pool });

  // Inside your business transaction:
  await store.enqueue(conn, {
    topic: "orders.created",
    aggregateType: "order",
    aggregateId: order.id,
    payload: { orderId: order.id, total: order.total },
  });
  ```

  Hand the store to the core `Relay` and you have at-least-once event publishing with retries, DLQ routing, and strict per-aggregate ordering. For CDC mode (binlog), see the binlog section in [`@eventferry/mysql` README](./packages/mysql/README.md).

  **Caveats (be honest)**

  - **No native low-latency waker** — MySQL has no `LISTEN/NOTIFY` analogue. Either use the binlog relay, or tune the poll interval down (e.g. 100ms).
  - **`@vlasky/zongji` is an optional peer** — only required if you actually use `MysqlBinlogRelay`. You'll get a clear runtime error if you forget it.
  - **Older MySQL versions are not supported** — `FOR UPDATE SKIP LOCKED` is hard-required; pre-8.0.1 / pre-MariaDB-10.6 would serialize on every claim.
  - **Binlog server config** is the user's responsibility — `binlog_format=ROW`, `binlog_row_image=FULL`, and a user with `REPLICATION SLAVE` + `REPLICATION CLIENT` grants. README has the full snippet.

  **Roadmap status**

  This closes the **MySQL / MariaDB** row on [ROADMAP.md](./ROADMAP.md) (Phase 1). Next up: SQL Server, then MongoDB.

### Patch Changes

- @eventferry/core@2.0.0
