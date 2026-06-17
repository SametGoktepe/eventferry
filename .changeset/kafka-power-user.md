---
"@eventferry/kafka": minor
---

Power-user escape hatches for both drivers. The high-level options cover ~95% of cases; these let you reach into the native client when you need a knob we don't expose typed.

- `compressionLevel`: per-codec level (confluent only, e.g. `zstd` level 1-22). Maps to librdkafka's `compression.level`. The kafkajs driver warns once and ignores it (kafkajs has no codec-level config).
- `rawProducerConfig`: raw librdkafka keys merged into the confluent producer config. Native keys **win** against eventferry's translated ones — use this to override defaults or to tune surface area (queue buffering, statistics interval, socket keepalive, …) we don't expose.
- `rawKafkaJsProducerConfig`: same idea for kafkajs — raw keys merged into `kafka.producer({...})` with last-write-wins precedence.
- `customPartitioner`: kafkajs partitioner factory (`() => (args) => number`). Overrides the `partitioner` preset entirely. Confluent ignores it — librdkafka's partitioner is a C-level extension point.

Native config takes precedence over eventferry's translated keys in every case — that's the contract of an escape hatch.
