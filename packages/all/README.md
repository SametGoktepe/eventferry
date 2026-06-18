# @eventferry/all

[![npm](https://img.shields.io/npm/v/@eventferry/all.svg)](https://www.npmjs.com/package/@eventferry/all)

The **meta-package** for [eventferry](https://github.com/SametGoktepe/eventferry) —
a transactional outbox toolkit for PostgreSQL, MySQL, or SQL Server + Kafka /
Redpanda / MSK. Installs and re-exports **the entire eventferry surface** from a
single entry point.

## What's inside

`@eventferry/all` installs **every** `@eventferry/*` package — eight in total:

| Package | One-liner |
| --- | --- |
| [`@eventferry/core`](https://www.npmjs.com/package/@eventferry/core) | Engine, `Relay`, `defineOutbox`, retry/poison/DLQ primitives, store + publisher interfaces. |
| [`@eventferry/postgres`](https://www.npmjs.com/package/@eventferry/postgres) | `PostgresStore` + `LISTEN/NOTIFY` waker over `pg`. |
| [`@eventferry/mysql`](https://www.npmjs.com/package/@eventferry/mysql) | `MysqlStore` + polling waker over `mysql2`, with `FOR UPDATE SKIP LOCKED` lease semantics. |
| [`@eventferry/mssql`](https://www.npmjs.com/package/@eventferry/mssql) | `MssqlStore` + `MssqlServiceBrokerWaker` (Service Broker push) over `mssql` / `tedious`. |
| [`@eventferry/mssql-cdc-relay`](https://www.npmjs.com/package/@eventferry/mssql-cdc-relay) | `MssqlCdcWaker` — CDC-driven relay that reads `cdc.*_CT` change tables instead of polling the outbox. |
| [`@eventferry/kafka`](https://www.npmjs.com/package/@eventferry/kafka) | `KafkaPublisher` with driver parity for `kafkajs` and `@confluentinc/kafka-javascript`. |
| [`@eventferry/kafka-iam`](https://www.npmjs.com/package/@eventferry/kafka-iam) | `createMskIamSasl` — AWS MSK IAM SASL/OAUTHBEARER helper for `kafkajs`. |
| [`@eventferry/schema-registry`](https://www.npmjs.com/package/@eventferry/schema-registry) | `SchemaRegistrySerializer` for Confluent Schema Registry (Avro / Protobuf / JSON Schema) with typed basic + bearer auth. |

## Install

```bash
# Postgres + Kafka (the original combo):
npm i @eventferry/all pg kafkajs

# MySQL + Kafka:
npm i @eventferry/all mysql2 kafkajs

# SQL Server + Kafka:
npm i @eventferry/all mssql kafkajs

# SQL Server (CDC mode) + AWS MSK with IAM:
npm i @eventferry/all mssql kafkajs aws-msk-iam-sasl-signer-js

# With Confluent Schema Registry:
npm i @eventferry/all pg @confluentinc/kafka-javascript @kafkajs/confluent-schema-registry
```

### Optional native peers

All native drivers are **optional peers** — install only what your engines and
brokers actually need:

- `pg` — for `@eventferry/postgres`
- `mysql2` — for `@eventferry/mysql`
- `mssql` — for `@eventferry/mssql` and `@eventferry/mssql-cdc-relay`
- `kafkajs` **or** `@confluentinc/kafka-javascript` — pick one Kafka client
- `@kafkajs/confluent-schema-registry` — for `@eventferry/schema-registry`
- `aws-msk-iam-sasl-signer-js` — for `@eventferry/kafka-iam` (MSK IAM auth)

## Naming convention

Because Postgres support shipped first, its exports are **flat** (`PostgresStore`,
`createListenNotifyWaker`, …). Later adapters are **prefixed** to avoid name
collisions when you import them side-by-side from `@eventferry/all`:

| Adapter | Exports look like |
| --- | --- |
| Postgres | `PostgresStore`, `createListenNotifyWaker` (flat — no prefix) |
| MySQL | `MysqlStore`, `MysqlPollingWaker`, … (`Mysql*` prefix) |
| MSSQL | `MssqlStore`, `MssqlServiceBrokerWaker`, `MssqlCdcWaker`, … (`Mssql*` prefix) |

If you want the **unprefixed** names (e.g. `Store` or `Waker` inside an
MSSQL-only service), import directly from the individual `@eventferry/<adapter>`
package instead of `@eventferry/all`.

## Quick start

One import line covers everything — Postgres, MySQL, MSSQL, CDC relay, Kafka,
MSK IAM, and Schema Registry:

```ts
import {
  Relay,
  PostgresStore,
  MysqlStore,
  MssqlStore,
  MssqlServiceBrokerWaker,
  MssqlCdcWaker,
  KafkaPublisher,
  createMskIamSasl,
  SchemaRegistrySerializer,
  defineOutbox,
} from "@eventferry/all";
```

## When NOT to use `@eventferry/all`

Production services that target **a single adapter** should install the
individual packages instead, to minimize the dependency footprint and avoid
pulling transitive declarations for engines you don't use:

```bash
# A Postgres + Kafka service — leanest install:
npm i @eventferry/core @eventferry/postgres @eventferry/kafka pg kafkajs
```

Use `@eventferry/all` for **demos, examples, polyglot monorepos, and
exploration** — where the convenience of one import line outweighs the cost of
shipping every adapter.

📖 **Full documentation:** [github.com/SametGoktepe/eventferry](https://github.com/SametGoktepe/eventferry#readme)

## License

MIT
