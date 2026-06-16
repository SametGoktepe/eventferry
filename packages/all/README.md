# @eventferry/all

[![npm](https://img.shields.io/npm/v/@eventferry/all.svg)](https://www.npmjs.com/package/@eventferry/all)

The **meta-package** for [eventferry](https://github.com/SametGoktepe/eventferry) —
a transactional outbox toolkit for PostgreSQL or MySQL + Kafka/Redpanda. Installs
**all** of eventferry and re-exports everything from a single entry point.

## Install

```bash
# Postgres + Kafka (the original combo):
npm i @eventferry/all pg kafkajs

# MySQL + Kafka:
npm i @eventferry/all mysql2 kafkajs
```

This pulls in [`@eventferry/core`](https://www.npmjs.com/package/@eventferry/core),
[`@eventferry/postgres`](https://www.npmjs.com/package/@eventferry/postgres),
[`@eventferry/mysql`](https://www.npmjs.com/package/@eventferry/mysql),
[`@eventferry/kafka`](https://www.npmjs.com/package/@eventferry/kafka), and
[`@eventferry/schema-registry`](https://www.npmjs.com/package/@eventferry/schema-registry).
`pg` and `mysql2` are peers of their respective adapters (install whichever you
use); pick a Kafka client (`kafkajs` or `@confluentinc/kafka-javascript`).

## Usage

```ts
import {
  Relay,
  PostgresStore,  // or MysqlStore
  MysqlStore,
  KafkaPublisher,
  SchemaRegistrySerializer,
  defineOutbox,
} from "@eventferry/all";
```

> Prefer installing only the packages you use (e.g. `@eventferry/core` +
> `@eventferry/mysql` + `@eventferry/kafka`) for a smaller dependency tree.

📖 **Full documentation:** [github.com/SametGoktepe/eventferry](https://github.com/SametGoktepe/eventferry#readme)

## License

MIT
