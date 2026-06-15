---
"@eventferry/postgres": patch
---

The streaming relay now creates its persistent logical replication slot on start if
it does not already exist (via `pg_create_logical_replication_slot`). Previously it
assumed the slot was pre-created and `subscribe` would fail otherwise. (Caught by the
new integration suite against real Postgres.)
