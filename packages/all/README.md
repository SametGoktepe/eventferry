# @eventferry/all

[![npm](https://img.shields.io/npm/v/@eventferry/all.svg)](https://www.npmjs.com/package/@eventferry/all)

The **meta-package** for [eventferry](https://github.com/SametGoktepe/eventferry) —
a transactional outbox toolkit for PostgreSQL + Kafka/Redpanda. Installs **all** of
eventferry and re-exports everything from a single entry point.

## Install

```bash
npm i @eventferry/all pg kafkajs
```

This pulls in [`@eventferry/core`](https://www.npmjs.com/package/@eventferry/core),
[`@eventferry/postgres`](https://www.npmjs.com/package/@eventferry/postgres),
[`@eventferry/kafka`](https://www.npmjs.com/package/@eventferry/kafka), and
[`@eventferry/schema-registry`](https://www.npmjs.com/package/@eventferry/schema-registry).
`pg` is a peer of the Postgres adapter; pick a Kafka client (`kafkajs` or
`@confluentinc/kafka-javascript`).

## Usage

```ts
import {
  Relay,
  PostgresStore,
  KafkaPublisher,
  SchemaRegistrySerializer,
  defineOutbox,
} from "@eventferry/all";
```

> Prefer installing only the packages you use (e.g. `@eventferry/core` +
> `@eventferry/postgres` + `@eventferry/kafka`) for a smaller dependency tree.

📖 **Full documentation:** [github.com/SametGoktepe/eventferry](https://github.com/SametGoktepe/eventferry#readme)

## License

MIT
