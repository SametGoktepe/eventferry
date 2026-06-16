---
"@eventferry/mysql": minor
---

**feat: MySQL & MariaDB support — `@eventferry/mysql`**

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
