---
"@eventferry/kafka": minor
---

Add typed admin surface to `KafkaPublisher`: `publisher.admin()` borrows a connected admin client (caller closes it), `publisher.ensureTopics()` idempotently provisions topics with an optional `growPartitions` flag, and a new `validateTopicsOnConnect` option fails fast at startup when expected topics are missing. Implemented on both the kafkajs and confluent drivers; custom drivers that don't implement the optional `admin()` method get a clear error message instead of a silent surprise.
