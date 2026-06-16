// Meta-package: re-exports the entire eventferry surface so consumers can
// `npm i @eventferry/all` and `import { Relay, PostgresStore, MysqlStore,
// KafkaPublisher, SchemaRegistrySerializer, defineOutbox, ... } from "@eventferry/all"`.
//
// Postgres is re-exported flat for backwards compatibility (it shipped first).
// MySQL has structurally identical names for some helpers / types (e.g.
// `createMigrationSql`, `PurgeDoneOptions`, `DecodedInsert`), so its exports
// are renamed here with the `Mysql` prefix to avoid ambiguity. Users wanting
// the unprefixed MySQL names should import directly from `@eventferry/mysql`.
export * from "@eventferry/core";
export * from "@eventferry/postgres";
export * from "@eventferry/kafka";
export * from "@eventferry/schema-registry";

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
