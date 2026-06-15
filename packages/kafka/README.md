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

📖 **Full documentation:** [github.com/SametGoktepe/eventferry](https://github.com/SametGoktepe/eventferry#readme)

## License

MIT
