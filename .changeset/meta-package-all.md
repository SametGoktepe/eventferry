---
"@eventferry/all": minor
---

New meta-package `@eventferry/all`: installs and re-exports the whole toolkit
(`@eventferry/core` + `@eventferry/postgres` + `@eventferry/kafka` +
`@eventferry/schema-registry`) from a single entry point. `npm i @eventferry/all`
and import everything (`Relay`, `PostgresStore`, `KafkaPublisher`,
`SchemaRegistrySerializer`, `defineOutbox`, …) from `@eventferry/all`.
