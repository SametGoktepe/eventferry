-- @eventferry/postgres : outbox table schema
-- Safe to run repeatedly (IF NOT EXISTS). Adjust the table name via
-- the {{TABLE}} placeholder if you use createMigrationSql(tableName).

CREATE TABLE IF NOT EXISTS {{TABLE}} (
  id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  message_id      UUID NOT NULL DEFAULT gen_random_uuid(),
  aggregate_type  VARCHAR(255) NOT NULL,
  aggregate_id    VARCHAR(255) NOT NULL,
  topic           VARCHAR(255) NOT NULL,
  "key"           TEXT,
  payload         JSONB NOT NULL,
  headers         JSONB NOT NULL DEFAULT '{}'::jsonb,
  trace_id        VARCHAR(64),
  status          SMALLINT NOT NULL DEFAULT 0,  -- 0 pending,1 processing,2 done,3 failed,4 dead
  attempts        INT NOT NULL DEFAULT 0,
  next_retry_at   TIMESTAMPTZ,
  claimed_at      TIMESTAMPTZ,                  -- when the current relay claimed it; drives the reaper
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  processed_at    TIMESTAMPTZ
);

-- Upgrade path for tables created before claimed_at / the reaper existed.
ALTER TABLE {{TABLE}} ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ;

-- The v0.1 index was keyed on created_at and excluded processing rows; the
-- ordered, reaper-aware claim needs id-ordered scans over unfinished rows.
DROP INDEX IF EXISTS idx_{{TABLE}}_due;

-- Partial index the relay's claim query rides on: only pending(0), processing(1)
-- and failed(3) rows are ever scanned. processing(1) is included so the reaper
-- can pick up rows orphaned by a crashed relay; done(2)/dead(4) are excluded so
-- the index stays tiny even when the table accumulates millions of done rows.
CREATE INDEX IF NOT EXISTS idx_{{TABLE}}_ready
  ON {{TABLE}} (id)
  WHERE status IN (0, 1, 3);

-- Supports the per-aggregate head check that enforces strict ordering: for each
-- candidate row, "is there an earlier (lower id) unfinished row for this
-- aggregate?". Without this the NOT EXISTS would seq-scan.
CREATE INDEX IF NOT EXISTS idx_{{TABLE}}_agg_order
  ON {{TABLE}} (aggregate_id, id)
  WHERE status IN (0, 1, 3);

-- Dedup / idempotency lookups by message_id.
CREATE UNIQUE INDEX IF NOT EXISTS uq_{{TABLE}}_message_id
  ON {{TABLE}} (message_id);
