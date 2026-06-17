---
"@eventferry/schema-registry": minor
---

Schema Registry serializer gains three production-grade controls without breaking the existing API:

- **Subject naming strategy** — `subjectStrategy: "TopicNameStrategy" | "RecordNameStrategy" | "TopicRecordNameStrategy"` mirrors Confluent's three built-ins. The two record-based strategies take a `recordName: (record, isKey) => string` resolver. Default stays `TopicNameStrategy`; the existing `subject` callable still wins when set (now optionally receiving `(topic, isKey, record)` — the single-arg legacy form keeps working).
- **Avro key serialization** — new `keySchemas` option + `serializeKey(record): Promise<Buffer | null>` method. Returns `null` for keyless records (matching the kafka "no key" semantics) and uses the `-key` subject by default. Key and value subject ids cache independently. Not wired into the relay automatically — call it from your publish glue when you want Avro-encoded keys instead of UTF-8 strings.
- **`autoRegister: false`** — never call `register()`, always resolve schemas via `getLatestSchemaId` on the computed subject. Locally supplied `schemas` / `keySchemas` bytes become docs-only in this mode. Matches `auto.register.schemas=false` on every Confluent client, for production clusters where schemas are managed out-of-band.
