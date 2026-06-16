# @eventferry/schema-registry

## 1.0.4

### Patch Changes

- Updated dependencies [64d115d]
  - @eventferry/core@1.0.4

## 1.0.3

### Patch Changes

- Updated dependencies [aaca9a2]
  - @eventferry/core@1.0.3

## 1.0.2

### Patch Changes

- 89f1867: Declare `engines.node` (>=18) so npm shows the supported Node version and tooling can warn on unsupported runtimes.
- Updated dependencies [89f1867]
  - @eventferry/core@1.0.2

## 1.0.1

### Patch Changes

- docs: polish per-package READMEs (npm page content). No code changes.
- Updated dependencies
  - @eventferry/core@1.0.1

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
