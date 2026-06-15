import { assertIdent } from "./ident.js";

/**
 * Generate the DDL for the outbox table, parameterized by table name.
 * Kept as a string template (not a file read) so it works regardless of
 * how the package is bundled or where it's installed.
 */
export function createMigrationSql(tableName = "outbox"): string {
  const t = assertIdent(tableName);
  return `
CREATE TABLE IF NOT EXISTS ${t} (
  id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  message_id      UUID NOT NULL DEFAULT gen_random_uuid(),
  aggregate_type  VARCHAR(255) NOT NULL,
  aggregate_id    VARCHAR(255) NOT NULL,
  topic           VARCHAR(255) NOT NULL,
  "key"           TEXT,
  payload         JSONB NOT NULL,
  headers         JSONB NOT NULL DEFAULT '{}'::jsonb,
  trace_id        VARCHAR(64),
  status          SMALLINT NOT NULL DEFAULT 0,
  attempts        INT NOT NULL DEFAULT 0,
  next_retry_at   TIMESTAMPTZ,
  claimed_at      TIMESTAMPTZ,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  processed_at    TIMESTAMPTZ
);

-- Upgrade path for tables created before claimed_at / the reaper existed.
ALTER TABLE ${t} ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ;

-- The v0.1 index was keyed on created_at and excluded processing rows; the
-- ordered, reaper-aware claim needs id-ordered scans over unfinished rows.
DROP INDEX IF EXISTS idx_${t}_due;

-- Rides the claim's id-ordered scan over unfinished rows. processing(1) is
-- included so the reaper can find timed-out rows. done(2)/dead(4) are excluded
-- so the index stays tiny even with millions of completed rows.
CREATE INDEX IF NOT EXISTS idx_${t}_ready
  ON ${t} (id)
  WHERE status IN (0, 1, 3);

-- Supports the per-aggregate head check (NOT EXISTS earlier unfinished row).
CREATE INDEX IF NOT EXISTS idx_${t}_agg_order
  ON ${t} (aggregate_id, id)
  WHERE status IN (0, 1, 3);

-- Dedup / idempotency lookups by message_id.
CREATE UNIQUE INDEX IF NOT EXISTS uq_${t}_message_id
  ON ${t} (message_id);
`.trim();
}

/**
 * Generate the trigger that fires a Postgres NOTIFY whenever a row is inserted
 * into the outbox table, so a {@link PostgresNotifyWaker} can wake the relay the
 * instant a row commits. The payload is empty — the relay re-claims from the
 * table, so it needs only the "something committed" edge (and this sidesteps the
 * 8 KB NOTIFY limit). NOTIFY is transactional: it is delivered only on commit.
 *
 * @param tableName outbox table the trigger is attached to. Default "outbox".
 * @param channel   LISTEN/NOTIFY channel; must match the waker's. Default "outbox".
 */
export function createNotifyTriggerSql(
  tableName = "outbox",
  channel = "outbox",
): string {
  const t = assertIdent(tableName);
  const ch = assertIdent(channel);
  return `
CREATE OR REPLACE FUNCTION ${t}_notify() RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify('${ch}', '');
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS ${t}_notify_trg ON ${t};
CREATE TRIGGER ${t}_notify_trg
  AFTER INSERT ON ${t}
  FOR EACH STATEMENT EXECUTE FUNCTION ${t}_notify();
`.trim();
}

/**
 * Generate an idempotent publication on the outbox table for the streaming relay
 * (logical replication). Only INSERTs are published — the outbox is append-only
 * from the relay's perspective. Requires `wal_level = logical` on the server.
 *
 * @param tableName   outbox table to capture. Default "outbox".
 * @param publication publication name; must match the streaming relay's. Default "outbox_pub".
 */
export function createPublicationSql(
  tableName = "outbox",
  publication = "outbox_pub",
): string {
  const t = assertIdent(tableName);
  const p = assertIdent(publication);
  return `
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = '${p}') THEN
    CREATE PUBLICATION ${p} FOR TABLE ${t} WITH (publish = 'insert');
  END IF;
END
$$;
`.trim();
}

/**
 * Optional index that speeds up `purgeDone` on high-volume tables. The default
 * partial indexes exclude done(2) rows, so the retention scan is unindexed; this
 * covers exactly the rows it deletes. Skip it unless retention scans are slow —
 * it adds write/space overhead on the bulk (done) segment.
 */
export function createRetentionIndexSql(tableName = "outbox"): string {
  const t = assertIdent(tableName);
  return `
CREATE INDEX IF NOT EXISTS idx_${t}_done_processed
  ON ${t} (processed_at)
  WHERE status = 2;
`.trim();
}
