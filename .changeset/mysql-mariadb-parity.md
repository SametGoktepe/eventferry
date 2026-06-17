---
"@eventferry/mysql": patch
---

MariaDB parity fix: `MysqlStore.rowToRecord` now defensively `JSON.parse`s `payload` and `headers` when the driver returns them as strings. MySQL 8 has a native JSON type and the `mysql2` driver auto-parses it; MariaDB exposes JSON as a `LONGTEXT` alias with a CHECK constraint, so the driver returns the raw string. Without the parse, consumers would see `payload: '{"x":1}'` (string) instead of `payload: { x: 1 }` (object). Belt and suspenders — works the same on both engines, and on any future engine that parses or doesn't.

Caught by parametrizing the `mysql-store` integration suite over both MySQL 8 and MariaDB 10.11 — three previously-passing tests failed on MariaDB until the fix landed. Suite now passes on both.

The package README gains a "Running on an older engine" section documenting the `UPDATE ... ORDER BY id LIMIT n` + claim-token fallback for shops stuck on MySQL 5.7 / MariaDB <10.6 (no `SKIP LOCKED`). Schema addition + full claim path with caveats. Not bundled because the throughput floor is lower than the SKIP LOCKED path; documented as an explicit workaround for legacy engines.
