---
"@eventferry/core": patch
"@eventferry/kafka": patch
---

Consumer-side documentation. No API change. The root README gains:

- **`Consuming what eventferry produced`** — canonical loop showing `decode(message)` → `extractTraceContext(headers)` → `defineOutbox(registry).decode(topic, bytes)`. Same registry the producer used, in reverse, returns the typed validated payload.
- **`Consuming the DLQ`** — copy-paste handler that routes by `dlq-error-class` (cleaner than parsing `dlq-reason`), pulls `dlq-attempts` for retry-queue accounting, and shows the alert-vs-retry split.

The `@eventferry/kafka` README adds matching subsections under the existing `Consumer helpers` block: **`Typed payload via the producer-side registry`** and **`DLQ recipe`**.

`defineOutbox(registry).decode()` was already shipped — the round just makes the symmetric "same registry, both sides" pattern discoverable.
