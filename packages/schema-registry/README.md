# @eventferry/schema-registry

[![npm](https://img.shields.io/npm/v/@eventferry/schema-registry.svg)](https://www.npmjs.com/package/@eventferry/schema-registry)

Confluent **Schema Registry serializer** for [eventferry](https://github.com/SametGoktepe/eventferry) —
encode outbox payloads as **Avro / Protobuf / JSON Schema** in the Confluent wire
format instead of plain JSON. It's a drop-in `Serializer` for the relay.

## Install

```bash
npm i @eventferry/schema-registry @kafkajs/confluent-schema-registry
```

`@kafkajs/confluent-schema-registry` is an optional peer (the underlying registry client).

## Usage

```ts
import { Relay } from "@eventferry/core";
import { SchemaRegistrySerializer } from "@eventferry/schema-registry";

const serializer = new SchemaRegistrySerializer({
  host: "http://localhost:8081",
  schemas: { "orders.created": { type: "AVRO", schema: orderCreatedAvsc } },
});

new Relay({ store, publisher, serializer }); // also works with PostgresStreamingRelay
```

Topics without a configured schema use the subject's latest registered schema
(default subject: `${topic}-value`). On the consumer, decode with the same client:
`await registry.decode(message.value)`.

📖 **Full documentation:** [github.com/SametGoktepe/eventferry](https://github.com/SametGoktepe/eventferry#readme)

## License

MIT
