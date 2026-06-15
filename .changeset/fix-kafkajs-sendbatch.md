---
"@eventferry/kafka": patch
---

Fix the kafkajs driver using `producer.send` with a multi-topic `topicMessages`
payload, which kafkajs rejects with "Invalid topic" — the `topicMessages` form is
`producer.sendBatch`. Batches now publish correctly (caught by the new integration
suite against real Redpanda; unit tests used a fake producer that didn't validate).
