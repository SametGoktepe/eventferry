# @eventferry/core

[![npm](https://img.shields.io/npm/v/@eventferry/core.svg)](https://www.npmjs.com/package/@eventferry/core)

The DB- and broker-agnostic **engine** of [eventferry](https://github.com/SametGoktepe/eventferry) —
a transactional outbox toolkit for PostgreSQL + Kafka/Redpanda. Zero runtime dependencies.

Contains the `Relay` loop (claim → publish → ack, with retries, backoff, and DLQ
routing), the default JSON serializer, the typed event registry (`defineOutbox`), the
W3C tracing hook, and the `OutboxStore` / `Publisher` / `Serializer` / `Waker`
interfaces that the storage and broker adapters implement.

## Install

```bash
npm i @eventferry/core
```

You normally pair it with a storage adapter and a broker adapter, e.g.
[`@eventferry/postgres`](https://www.npmjs.com/package/@eventferry/postgres) and
[`@eventferry/kafka`](https://www.npmjs.com/package/@eventferry/kafka) — or grab everything at
once with [`@eventferry/all`](https://www.npmjs.com/package/@eventferry/all).

## Usage

```ts
import { Relay, defineOutbox } from "@eventferry/core";

const relay = new Relay({
  store,       // an OutboxStore (e.g. PostgresStore)
  publisher,   // a Publisher  (e.g. KafkaPublisher)
  retry: { maxAttempts: 5, strategy: "exponential", baseMs: 200, maxMs: 30_000 },
  dlq: { topic: "orders.dlq" },
});
await relay.start();
```

📖 **Full documentation:** [github.com/SametGoktepe/eventferry](https://github.com/SametGoktepe/eventferry#readme)

## License

MIT
