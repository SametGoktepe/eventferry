---
"@eventferry/kafka": minor
---

librdkafka stats hook on the confluent driver. New `onStats: (stats) => void` callback receives the librdkafka periodic statistics JSON, already parsed to a plain object — pipe queue depth, broker latencies, txmsgs counters, per-topic/per-partition stats into your metrics stack without a second client. The wrapper swallows callback exceptions and JSON parse failures so a misbehaving observer cannot take down the producer's event loop. `statsIntervalMs` controls the polling interval; defaults to 30000 ms when `onStats` is set, stays OFF otherwise (librdkafka CPU-bills the JSON serialization every tick — we don't enable it silently). `rawProducerConfig` still wins on precedence. kafkajs driver warns once and ignores both options — kafkajs has no equivalent surface.
