# @eventferry/all

Meta-package for [eventferry](../../README.md). Installs **all** of eventferry and
re-exports everything from one entry point.

```bash
npm i @eventferry/all pg kafkajs
```

This pulls in `@eventferry/core`, `@eventferry/postgres`, `@eventferry/kafka`, and
`@eventferry/schema-registry`. (`pg` is a peer of the Postgres adapter; pick a Kafka
client — `kafkajs` or `@confluentinc/kafka-javascript`.)

```ts
import {
  Relay,
  PostgresStore,
  KafkaPublisher,
  SchemaRegistrySerializer,
  defineOutbox,
} from "@eventferry/all";
```

Prefer installing only the packages you use (e.g. just `@eventferry/core` +
`@eventferry/postgres` + `@eventferry/kafka`) for a smaller dependency tree. See the
[root README](../../README.md) for full documentation.
