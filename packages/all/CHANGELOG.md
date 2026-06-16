# @eventferry/all

## 1.0.4

### Patch Changes

- Updated dependencies [64d115d]
  - @eventferry/core@1.0.4
  - @eventferry/kafka@1.0.4
  - @eventferry/postgres@1.0.4
  - @eventferry/schema-registry@1.0.4

## 1.0.3

### Patch Changes

- Updated dependencies [aaca9a2]
  - @eventferry/core@1.0.3
  - @eventferry/kafka@1.0.3
  - @eventferry/postgres@1.0.3
  - @eventferry/schema-registry@1.0.3

## 1.0.2

### Patch Changes

- 89f1867: Declare `engines.node` (>=18) so npm shows the supported Node version and tooling can warn on unsupported runtimes.
- Updated dependencies [89f1867]
  - @eventferry/core@1.0.2
  - @eventferry/postgres@1.0.2
  - @eventferry/kafka@1.0.2
  - @eventferry/schema-registry@1.0.2

## 1.0.1

### Patch Changes

- docs: polish per-package READMEs (npm page content). No code changes.
- Updated dependencies
  - @eventferry/core@1.0.1
  - @eventferry/postgres@1.0.1
  - @eventferry/kafka@1.0.1
  - @eventferry/schema-registry@1.0.1

## 1.0.0

### Minor Changes

- 7168285: New meta-package `@eventferry/all`: installs and re-exports the whole toolkit
  (`@eventferry/core` + `@eventferry/postgres` + `@eventferry/kafka` +
  `@eventferry/schema-registry`) from a single entry point. `npm i @eventferry/all`
  and import everything (`Relay`, `PostgresStore`, `KafkaPublisher`,
  `SchemaRegistrySerializer`, `defineOutbox`, …) from `@eventferry/all`.

### Patch Changes

- Updated dependencies [b06f8ec]
- Updated dependencies [b06f8ec]
- Updated dependencies [b06f8ec]
- Updated dependencies [b06f8ec]
- Updated dependencies [b06f8ec]
- Updated dependencies [b06f8ec]
- Updated dependencies [b06f8ec]
- Updated dependencies [b06f8ec]
- Updated dependencies [b06f8ec]
  - @eventferry/kafka@1.0.0
  - @eventferry/postgres@1.0.0
  - @eventferry/core@1.0.0
  - @eventferry/schema-registry@1.0.0
