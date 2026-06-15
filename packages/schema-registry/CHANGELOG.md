# @eventferry/schema-registry

## 1.0.0

### Minor Changes

- b06f8ec: New package: `@eventferry/schema-registry`.

  A core `Serializer` that encodes outbox payloads with a Confluent-compatible Schema
  Registry (Avro / Protobuf / JSON Schema) instead of plain JSON — drop it into the
  `serializer` option of `Relay` or `PostgresStreamingRelay`.

  - Wraps `@kafkajs/confluent-schema-registry` as an **optional** peer dependency
    (dynamically imported); inject your own client for tests or custom config.
  - Per-topic schema resolution: register a supplied schema, or use the subject's latest
    (default subject `${topic}-value`, configurable). Schema ids are resolved once and
    cached.
  - Consumers decode with the same registry client; no changes to `core`/`postgres`/`kafka`.

### Patch Changes

- Updated dependencies [b06f8ec]
- Updated dependencies [b06f8ec]
- Updated dependencies [b06f8ec]
- Updated dependencies [b06f8ec]
- Updated dependencies [b06f8ec]
  - @eventferry/core@1.0.0
