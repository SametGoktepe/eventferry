# @eventferry/mssql-cdc-relay

## 2.0.0

### Minor Changes

- cca1747: Two previously-deferred SQL Server features land in PR #41:

  **`MssqlServiceBrokerWaker` + `createServiceBrokerSetupSql`** in `@eventferry/mssql` — Service Broker-driven sub-second wake for the polling Relay. Opt-in (the core polling path is unchanged). Setup SQL is idempotent and refuses to run on Azure SQL Database (`EngineEdition = 5`) where Service Broker is unsupported per Microsoft Learn — the waker constructor auto-probes the engine at start and throws a clear error pointing operators to the polling fallback. The waker uses a DEDICATED `mssql.ConnectionPool` so the main pool never blocks on `WAITFOR (RECEIVE ...)`. The setup helper provisions the initiator + target queues, the activation-procedure-driven cleanup that prevents `sys.conversation_endpoints` from growing unbounded (Rusanu's leak fix), and the AFTER INSERT trigger that drops a wakeup per statement (not per row).

  **New package `@eventferry/mssql-cdc-relay`** at 0.1.0 — Change Data Capture-driven `Waker`. Reads `sys.fn_cdc_get_max_lsn()` on a small poll loop and wakes the core Relay when the LSN advances past the persisted watermark. Watermark is stored in a tiny table (`<schema>.<watermarkTable>`) keyed by `capture_instance`, holding the last-processed `BINARY(10)` LSN. `createCdcEnablementSql({ table, schema, captureInstance, watermarkSchema, watermarkTable })` emits the idempotent `EXEC sys.sp_cdc_enable_db` + `EXEC sys.sp_cdc_enable_table` + watermark table DDL. Failure modes surface as typed errors (`CdcNotEnabledError`, `CdcRetentionExceededError`, `WatermarkBelowMinLsnError`, `CdcCaptureJobStoppedError`) — the waker NEVER throws into the relay's claim loop. **Azure SQL Database is unsupported** (CDC requires SQL Server Agent, unavailable there); Azure SQL Managed Instance and on-prem SQL Server 2008+ work.

  **`@eventferry/all`** meta-package now installs and re-exports **every** `@eventferry/*` package — adding `@eventferry/mssql-cdc-relay` and `@eventferry/kafka-iam` alongside the prior set (core + postgres + mysql + mssql + kafka + schema-registry). Cross-adapter name collisions resolve via the existing `Mysql*` / `Mssql*` prefix convention. The optional native peers (`pg`, `mysql2`, `mssql`, `kafkajs` or `@confluentinc/kafka-javascript`, `@kafkajs/confluent-schema-registry`, `aws-msk-iam-sasl-signer-js`) remain optional — install only the ones for the engines and brokers you use.

  The Phase A design (3 lenses: correctness, concurrency, edge-cases) caught **45 bugs across 17+12+16** issues that were folded into the final implementation. The integration suite still uses the original 17 MSSQL tests from PR #41 — broker waker + CDC integration testing requires special container configuration (Service Broker enabled, SQL Server Agent running) and is tracked as a follow-up.

### Patch Changes

- Updated dependencies [cca1747]
  - @eventferry/mssql@1.2.0
