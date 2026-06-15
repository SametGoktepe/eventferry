---
"@eventferry/core": minor
---

Add a type-safe event registry: `defineOutbox`.

Declare each topic once (`{ aggregateType, schema }`) and get a typed, runtime-
validated `enqueue` plus a `decode` helper consumers can reuse from the same
registry. Payloads are validated before the row is inserted, so a malformed event
rolls back with the rest of your transaction instead of reaching the outbox.

- **Validator-agnostic:** any [Standard Schema](https://standardschema.dev) works
  (Zod 3.24+, Valibot, ArkType, …). The spec interface is inlined, so `@eventferry/core`
  gains no runtime dependency.
- **Producer + consumer:** `defineOutbox(registry, { store })` exposes typed
  `enqueue`; `defineOutbox(registry)` (no store) exposes `decode`/`validate` for
  consuming services.
- New `OutboxValidationError` carries the failing topic and the validator's issues.
- Purely additive — `PostgresStore`, `Relay`, and untyped `store.enqueue` are unchanged.
