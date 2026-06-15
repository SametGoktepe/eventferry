# @eventferry/schema-registry

Confluent Schema Registry [`Serializer`](../core) for [eventferry](../../README.md):
encode outbox payloads as **Avro / Protobuf / JSON Schema** in the Confluent wire
format, instead of plain JSON.

```bash
npm i @eventferry/schema-registry @kafkajs/confluent-schema-registry
```

```ts
import { SchemaRegistrySerializer } from "@eventferry/schema-registry";
import { Relay } from "@eventferry/core";

const serializer = new SchemaRegistrySerializer({
  host: "http://localhost:8081",
  schemas: { "orders.created": { type: "AVRO", schema: orderCreatedAvsc } },
});

new Relay({ store, publisher, serializer }); // also works with PostgresStreamingRelay
```

Topics without a configured schema use the subject's latest registered schema
(default subject: `${topic}-value`). On the consumer, decode with the same client:
`await registry.decode(message.value)`.

See the [root README](../../README.md) for the full picture.
