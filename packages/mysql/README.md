# @eventferry/mysql

[![npm](https://img.shields.io/npm/v/@eventferry/mysql.svg)](https://www.npmjs.com/package/@eventferry/mysql)

The **MySQL / MariaDB store** for [eventferry](https://github.com/SametGoktepe/eventferry) —
a transactional outbox toolkit for relational databases + Kafka/Redpanda.

Provides:

- `MysqlStore` — transaction-joining `enqueue` and a lock-free `claimBatch`
  using `SELECT ... FOR UPDATE SKIP LOCKED`, with **strict per-aggregate
  ordering** and a **crash-recovery reaper** (visibility timeout).
- `createMigrationSql` — idempotent DDL generator for the outbox table
  (InnoDB + utf8mb4 + DATETIME(3) + JSON).
- `purgeDone` — batched retention of published rows.

## Requirements

- **MySQL 8.0.1+** or **MariaDB 10.6+** (needs `FOR UPDATE SKIP LOCKED`)
- **InnoDB** storage engine (default)
- Node.js **18+**

> Older MySQL versions don't support `SKIP LOCKED` — concurrent relays would
> serialize on the same rows. There is no fallback in this MVP.

## Install

```bash
npm i @eventferry/mysql @eventferry/core mysql2
```

`mysql2` is an optional peer (the driver).

## Usage

```ts
import mysql from "mysql2/promise";
import { MysqlStore, createMigrationSql } from "@eventferry/mysql";

const pool = mysql.createPool({
  host: "localhost",
  user: "app",
  password: "...",
  database: "shop",
  // Recommended for the outbox: stable date handling and bigint-safe ids.
  dateStrings: false,
  supportBigNumbers: true,
});

await pool.query(createMigrationSql("outbox")); // idempotent DDL

const store = new MysqlStore({ pool });

// Inside your business transaction:
const conn = await pool.getConnection();
try {
  await conn.beginTransaction();
  await conn.query("INSERT INTO orders ...");
  await store.enqueue(conn, {
    topic: "orders.created",
    aggregateType: "order",
    aggregateId: order.id,
    payload: { orderId: order.id, total: order.total },
  });
  await conn.commit();
} catch (err) {
  await conn.rollback();
  throw err;
} finally {
  conn.release();
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

## Binlog streaming (CDC) mode

For high-throughput workloads, use the `MysqlBinlogRelay` to tail the **MySQL
binary log** directly — the same mechanism Debezium uses — and bypass the
polling claim loop entirely. Latency drops from "one poll interval" to a few
milliseconds.

**MySQL server requirements:**

```ini
# my.cnf
[mysqld]
binlog_format    = ROW
binlog_row_image = FULL
server_id        = 1            # any value, must be unique in the cluster
log_bin          = mysql-bin
gtid_mode        = ON           # recommended for safer resumption
enforce_gtid_consistency = ON
```

**Grants the reader user needs:**

```sql
CREATE USER 'outbox_reader'@'%' IDENTIFIED BY 'strong-password';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'outbox_reader'@'%';
GRANT SELECT ON shop.outbox TO 'outbox_reader'@'%';
```

**Install the optional peer dep:**

```bash
npm i @vlasky/zongji
```

**Usage:**

```ts
import { MysqlStore, MysqlBinlogRelay } from "@eventferry/mysql";
import { KafkaPublisher } from "@eventferry/kafka";

// IMPORTANT: claimFailedOnly=true so the internal retry loop only drains
// failures — pending rows are owned by the binlog stream.
const store = new MysqlStore({ pool, claimFailedOnly: true });
const publisher = new KafkaPublisher({ driver: "kafkajs", brokers, idempotent: true });

const relay = new MysqlBinlogRelay({
  store,
  publisher,
  binlog: {
    host: "localhost",
    user: "outbox_reader",
    password: "strong-password",
    database: "shop",
    table: "outbox",
    // serverId: 1_000_001,        // override if you have multiple readers
    // startPosition: { filename: "mysql-bin.000042", position: 12345 },
  },
});
await relay.start();
process.on("SIGTERM", () => relay.stop());
```

**Position persistence (at-least-once across restarts):**

MySQL has no server-side ack like Postgres logical replication, so binlog
position tracking lives outside the server. Hook `onCommit` to persist the
position to your own KV store, then pass it back via `startPosition` on the
next start — without it the relay starts at the **end** of the binlog (tail
mode) and won't replay rows written before it connected.

## What's still not in this package

- **No native low-latency waker** for the polling relay. MySQL has no
  `LISTEN/NOTIFY`; if you don't want binlog mode either, tune the relay's
  polling interval down (e.g. 100ms).
- **Built-in position persistence** for the binlog relay — for now, you persist
  the position yourself via the `onCommit` hook.

## Retention

The outbox table grows; `purgeDone` batch-deletes old `done` rows (run from
your own cron):

```ts
await store.purgeDone({ olderThanMs: 7 * 24 * 60 * 60 * 1000 }); // 7 days
```

📖 **Full documentation:** [github.com/SametGoktepe/eventferry](https://github.com/SametGoktepe/eventferry#readme)

## License

MIT
