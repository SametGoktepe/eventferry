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

## Authentication

The serializer accepts the two HTTP auth shapes Confluent Schema Registry installations use:

```ts
// HTTP Basic — Confluent Cloud + most commercial registries.
new SchemaRegistrySerializer({
  host,
  auth: { type: "basic", username: "<api-key>", password: "<api-secret>" },
});

// Bearer token — OIDC, custom SR proxies, etc.
new SchemaRegistrySerializer({
  host,
  auth: { type: "bearer", token: "eyJhbGc..." },
});

// Rotating bearer token (refresh per request — cache inside your provider).
new SchemaRegistrySerializer({
  host,
  auth: {
    type: "bearer",
    token: async () => await getCachedAccessToken(),
  },
});
```

Bearer tokens are injected via a small middleware on the underlying client — the provider is invoked on **every** request, so you control rotation. `auth` is ignored when you pass an already-constructed `registry` client (configure auth there yourself).

For **mTLS** to the registry, supply a custom `https.Agent` on a self-constructed client and pass it via the `registry` option. The serializer does not surface a separate `tls` block — registry TLS is independent of broker TLS and `https.Agent` is the standard Node entry point.

## Subject naming strategies

Pick one of Confluent's three built-in strategies (default `TopicNameStrategy`):

```ts
new SchemaRegistrySerializer({
  host,
  subjectStrategy: "RecordNameStrategy",     // subject = recordName
  // or: "TopicRecordNameStrategy"           // subject = `${topic}-${recordName}`
  recordName: (record) => `com.example.${record.aggregateType}.Created`,
});
```

`recordName` is required for the `RecordName` and `TopicRecordName` strategies — typical implementation reads `${namespace}.${name}` from your avsc.

Need full control? Skip the preset and pass an explicit `subject` function:

```ts
new SchemaRegistrySerializer({
  host,
  subject: (topic, isKey, record) => `acme.${topic}.${isKey ? "key" : "value"}`,
});
```

## Avro key serialization

Configure per-topic key schemas and call `serializeKey` from your publish path:

```ts
const serializer = new SchemaRegistrySerializer({
  host,
  schemas:    { "orders.created": { type: "AVRO", schema: valueAvsc } },
  keySchemas: { "orders.created": { type: "AVRO", schema: keyAvsc } },
});

// Inside your custom publish glue:
const encodedValue = await serializer.serialize(record);
const encodedKey   = await serializer.serializeKey(record); // null when record.key is null
```

The relay does NOT call `serializeKey` automatically — Avro keys are an application-level convention, and adding them silently would break consumers expecting UTF-8 string keys. Wire it in your publish path explicitly.

Key and value subjects cache their schema ids independently — registering one doesn't affect the other.

## Auto-register toggle

For production clusters where schemas are managed out-of-band (Confluent Cloud, regulated environments), turn auto-registration off:

```ts
new SchemaRegistrySerializer({
  host,
  schemas: { /* ignored when autoRegister is false */ },
  autoRegister: false,
});
```

With `autoRegister: false`, the serializer ALWAYS resolves by `getLatestSchemaId` on the computed subject — locally supplied `schemas` / `keySchemas` bytes are ignored. Matches the Confluent client's `auto.register.schemas=false`.

📖 **Full documentation:** [github.com/SametGoktepe/eventferry](https://github.com/SametGoktepe/eventferry#readme)

## License

MIT
