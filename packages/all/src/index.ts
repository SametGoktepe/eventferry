// Meta-package: re-exports the entire eventferry surface so consumers can
// `npm i @eventferry/all` and import every adapter (Postgres / MySQL / MSSQL
// + the MSSQL CDC waker), the Kafka publisher + MSK IAM helper, and the
// Schema Registry serializer from one place.
//
// Naming convention for cross-adapter collisions:
//   - Postgres is re-exported FLAT for backwards compatibility (it shipped
//     first; `createMigrationSql`, `PurgeDoneOptions`, etc. without a prefix
//     refer to Postgres).
//   - MySQL + MSSQL collide on the same names (`createMigrationSql`,
//     `MssqlStore`/`MysqlStore`, `PurgeDoneOptions`, `DecodedInsert`),
//     so their exports are renamed with `Mysql*` / `Mssql*` prefixes here.
//   - Users wanting the unprefixed names should import directly from the
//     individual `@eventferry/<adapter>` package.

// ── Flat re-exports (Postgres, Kafka, Schema Registry) ───────────────────
export * from "@eventferry/core";
export * from "@eventferry/postgres";
export * from "@eventferry/kafka";
export * from "@eventferry/schema-registry";

// ── MySQL adapter (prefixed) ─────────────────────────────────────────────
export {
  MysqlStore,
  MysqlBinlogRelay,
  createMigrationSql as createMysqlMigrationSql,
  createRetentionIndexSql as createMysqlRetentionIndexSql,
  type MysqlStoreOptions,
  type MysqlPool,
  type MysqlConnection,
  type MysqlQueryable,
  type BinlogReplicationConfig,
  type BinlogPosition,
  type BinlogStream,
  type BinlogStreamHandlers,
  type MysqlBinlogRelayOptions,
  type DecodedInsert as MysqlDecodedInsert,
  type PurgeDoneOptions as MysqlPurgeDoneOptions,
} from "@eventferry/mysql";

// ── MSSQL adapter (prefixed) ─────────────────────────────────────────────
export {
  MssqlStore,
  MssqlServiceBrokerWaker,
  createMigrationSql as createMssqlMigrationSql,
  createRetentionIndexSql as createMssqlRetentionIndexSql,
  createServiceBrokerSetupSql,
  rowToRecord as mssqlRowToRecord,
  type MssqlStoreOptions,
  type MssqlServiceBrokerWakerOptions,
  type OutboxRow as MssqlOutboxRow,
  type PurgeDoneOptions as MssqlPurgeDoneOptions,
} from "@eventferry/mssql";

// ── MSSQL CDC relay (prefixed) ───────────────────────────────────────────
export {
  MssqlCdcWaker,
  MssqlCdcRelay,
  createCdcEnablementSql,
  compareLsn,
  lsnToHex,
  lsnFromHex,
  ZERO_LSN,
  type Lsn,
  type MssqlCdcWakerOptions,
  CdcRelayError,
  CdcNotEnabledError,
  CdcRetentionExceededError,
  WatermarkBelowMinLsnError,
  CdcCaptureJobStoppedError,
} from "@eventferry/mssql-cdc-relay";

// ── AWS MSK IAM helper ───────────────────────────────────────────────────
export {
  createMskIamSasl,
  type MskIamSaslOptions,
  type MskIamSigner,
} from "@eventferry/kafka-iam";
