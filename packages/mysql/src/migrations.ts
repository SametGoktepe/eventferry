import { assertIdent } from "./ident.js";

/**
 * Generate the DDL for the MySQL outbox table, parameterized by table name.
 * Kept as a string template (not a file read) so it works regardless of how
 * the package is bundled or where it's installed.
 *
 * Requirements:
 *  - **MySQL 8.0.1+** or **MariaDB 10.6+** (needs `SELECT ... FOR UPDATE SKIP LOCKED`).
 *  - **InnoDB** engine — required for transactions and row-level locking.
 *  - `DATETIME(3)` is used (not `TIMESTAMP`) so values are tz-stable and free of
 *    the 2038 problem; millisecond precision matches the reaper's claim window.
 *
 * MySQL has no partial indexes, so `idx_${t}_ready` covers all statuses (it
 * still helps because the planner picks the index for `WHERE status IN (...)`).
 * Pair with `createRetentionIndexSql` only if `purgeDone` scans become hot.
 */
export function createMigrationSql(tableName = "outbox"): string {
  const t = assertIdent(tableName);
  return `
CREATE TABLE IF NOT EXISTS \`${t}\` (
  id              BIGINT NOT NULL AUTO_INCREMENT,
  message_id      CHAR(36) NOT NULL,
  aggregate_type  VARCHAR(255) NOT NULL,
  aggregate_id    VARCHAR(255) NOT NULL,
  topic           VARCHAR(255) NOT NULL,
  \`key\`           TEXT,
  payload         JSON NOT NULL,
  headers         JSON NOT NULL,
  trace_id        VARCHAR(64),
  status          TINYINT NOT NULL DEFAULT 0,
  attempts        INT NOT NULL DEFAULT 0,
  next_retry_at   DATETIME(3),
  claimed_at      DATETIME(3),
  created_at      DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  processed_at    DATETIME(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_${t}_message_id (message_id),
  KEY idx_${t}_ready (status, id),
  KEY idx_${t}_agg_order (aggregate_id, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
`.trim();
}

/**
 * Optional index that speeds up `purgeDone` on high-volume tables. The default
 * indexes don't cover `processed_at`, so the retention scan otherwise filesorts
 * across all done rows; this index makes it index-driven. Skip unless retention
 * scans are slow — it adds write/space overhead on the bulk (done) segment.
 */
export function createRetentionIndexSql(tableName = "outbox"): string {
  const t = assertIdent(tableName);
  return `
CREATE INDEX idx_${t}_done_processed ON \`${t}\` (processed_at);
`.trim();
}
