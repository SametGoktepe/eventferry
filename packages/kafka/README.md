# @eventferry/kafka

[![npm](https://img.shields.io/npm/v/@eventferry/kafka.svg)](https://www.npmjs.com/package/@eventferry/kafka)

The **Kafka / Redpanda publisher** for [eventferry](https://github.com/SametGoktepe/eventferry) —
a transactional outbox toolkit for PostgreSQL + Kafka/Redpanda.

One `KafkaPublisher` over two interchangeable drivers — `kafkajs` (pure JS) and
`@confluentinc/kafka-javascript` (librdkafka-backed) — with idempotent and optional
transactional (EOS) producers, plus dead-letter (DLQ) routing.

## Install

```bash
npm i @eventferry/kafka @eventferry/core

# pick ONE Kafka client (both are optional peers):
npm i kafkajs                          # pure JS, zero native deps
npm i @confluentinc/kafka-javascript   # librdkafka-backed, higher throughput
```

## Usage

```ts
import { KafkaPublisher } from "@eventferry/kafka";

const publisher = new KafkaPublisher({
  driver: "kafkajs", // or "confluent"
  brokers: ["localhost:19092"],
  idempotent: true,
});

// Hand it to a Relay from @eventferry/core.
```

You can also pass a `customDriver` implementing the `KafkaDriver` interface.

## Authentication & TLS

### One-way TLS

```ts
new KafkaPublisher({
  brokers: ["broker:9093"],
  ssl: true, // uses the driver's default trust store
});
```

### mTLS (mutual TLS)

```ts
import { readFileSync } from "node:fs";

new KafkaPublisher({
  brokers: ["broker:9093"],
  ssl: {
    ca: readFileSync("/etc/ssl/kafka-ca.pem"),
    cert: readFileSync("/etc/ssl/client.pem"),
    key: readFileSync("/etc/ssl/client-key.pem"),
    passphrase: "optional",
    // servername: "broker.example.com",   // SNI override if cert SAN differs
  },
});
```

> `rejectUnauthorized` is intentionally NOT a knob. TLS verification is
> non-negotiable. For dev clusters with self-signed certs, pass the cluster
> CA via `ca` so verification succeeds.

### SASL — username + password (PLAIN / SCRAM)

```ts
new KafkaPublisher({
  brokers: ["broker:9093"],
  ssl: true,
  sasl: {
    mechanism: "scram-sha-512", // or "plain" | "scram-sha-256"
    username: process.env.KAFKA_USER!,
    password: process.env.KAFKA_PASSWORD!,
  },
});
```

### SASL/OAUTHBEARER (Azure Event Hubs, OIDC, MSK IAM)

```ts
new KafkaPublisher({
  brokers: ["broker:9093"],
  ssl: true,
  sasl: {
    mechanism: "oauthbearer",
    oauthBearerProvider: async () => {
      const token = await myTokenIssuer();
      return {
        value: token.value,           // required for both drivers
        principal: token.principal,   // required for confluent driver
        lifetime: token.expiresInMs,  // required for confluent driver
        extensions: token.extensions, // optional
      };
    },
  },
});
```

> **Driver asymmetry:** `kafkajs` reads only `value`; `@confluentinc/kafka-javascript` requires `value` + `principal` + `lifetime` (in milliseconds) and accepts an optional `extensions` map. Cross-driver portable providers should populate all four fields.

📖 **Full documentation:** [github.com/SametGoktepe/eventferry](https://github.com/SametGoktepe/eventferry#readme)

## License

MIT
