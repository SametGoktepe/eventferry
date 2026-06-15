import {
  buildPublishable,
  ConsoleLogger,
  JsonSerializer,
  Relay,
} from "@eventferry/core";
import type {
  DlqConfig,
  Logger,
  OutboxStore,
  Publisher,
  RelayHooks,
  RetryConfig,
  Serializer,
} from "@eventferry/core";
import { rowToRecord, type OutboxRow } from "./row.js";

/** Connection + slot/publication settings for the WAL stream. */
export interface ReplicationConfig {
  /** A replication-capable connection (e.g. a connection string). */
  connectionString: string;
  /** Persistent logical replication slot name. Created if absent. */
  slot: string;
  /** Publication name (see createPublicationSql). */
  publication: string;
  /** Outbox table to capture. Default "outbox". */
  table?: string;
}

export interface PostgresStreamingRelayOptions {
  /** Outbox store. Construct with `{ claimFailedOnly: true }` so the internal
   *  retry loop only drains failures (pending rows are owned by the stream). */
  store: OutboxStore;
  publisher: Publisher;
  replication: ReplicationConfig;
  retry?: Partial<RetryConfig>;
  dlq?: DlqConfig;
  serializer?: Serializer;
  logger?: Logger;
  hooks?: RelayHooks;
  /** Poll interval (ms) for the internal failed-row retry loop. Default 5000. */
  failedPollIntervalMs?: number;
  /** Mark happy-path rows done (status=2) after publish. Default true. */
  markPublished?: boolean;
  /** Max rows published per chunk within a committed transaction. Default 100. */
  batchSize?: number;
}

/** A decoded INSERT on the outbox table, with the WAL position it occurred at. */
export interface DecodedInsert {
  readonly lsn: string;
  readonly row: OutboxRow;
}

export interface ReplicationStreamHandlers {
  onInsert: (insert: DecodedInsert) => void;
  onCommit: (lsn: string) => void | Promise<void>;
  onError: (err: Error) => void;
}

/** The WAL source the streaming relay consumes. Implemented over
 *  pg-logical-replication; swappable for tests. */
export interface ReplicationStream {
  start(handlers: ReplicationStreamHandlers): Promise<void>;
  acknowledge(lsn: string): Promise<void>;
  stop(): Promise<void>;
}

/**
 * Publishes outbox events straight from the Postgres WAL (logical replication),
 * with no claim query on the happy path. Failures are demoted to `failed` and
 * drained by an internal claim-based retry loop (the core `Relay` over a
 * `claimFailedOnly` store), reusing the existing backoff / DLQ / dead handling.
 *
 * At-least-once: a commit's LSN is acknowledged only after its batch's side
 * effects commit, so a crash re-streams and re-publishes (a duplicate idempotent
 * consumers absorb). Ordering is best-effort per aggregate — a retried failure
 * lands after later same-aggregate rows; use the polling relay for strict order.
 */
export class PostgresStreamingRelay {
  private readonly store: OutboxStore;
  private readonly publisher: Publisher;
  private readonly serializer: Serializer;
  private readonly log: Logger;
  private readonly hooks: RelayHooks;
  private readonly replication: ReplicationConfig;
  private readonly markPublished: boolean;
  private readonly batchSize: number;
  private readonly retryRelay: Relay;

  private stream: ReplicationStream | null = null;
  private buffer: DecodedInsert[] = [];
  private tail: Promise<void> = Promise.resolve();
  private running = false;

  constructor(opts: PostgresStreamingRelayOptions) {
    this.store = opts.store;
    this.publisher = opts.publisher;
    this.serializer = opts.serializer ?? new JsonSerializer();
    this.log = opts.logger ?? new ConsoleLogger();
    this.hooks = opts.hooks ?? {};
    this.replication = opts.replication;
    this.markPublished = opts.markPublished ?? true;
    this.batchSize = opts.batchSize ?? 100;

    // Internal failed-only retry loop: owns publisher connect/disconnect and
    // reuses the engine's retry/backoff/DLQ/dead/reaper for demoted rows.
    this.retryRelay = new Relay({
      store: opts.store,
      publisher: opts.publisher,
      retry: opts.retry,
      dlq: opts.dlq,
      serializer: this.serializer,
      logger: opts.logger,
      hooks: opts.hooks,
      pollIntervalMs: opts.failedPollIntervalMs ?? 5000,
    });
  }

  async start(): Promise<void> {
    if (this.running) return;
    this.running = true;
    await this.retryRelay.start(); // connects publisher + starts the failed-only loop
    this.stream = this.createReplicationStream();
    await this.stream.start({
      onInsert: (insert) => {
        this.buffer.push(insert);
      },
      onCommit: (lsn) => {
        const batch = this.buffer;
        this.buffer = [];
        this.tail = this.tail.then(() => this.processBatch(batch, lsn));
        return this.tail;
      },
      onError: (err) => {
        this.log.error("replication stream error", { error: err.message });
        this.hooks.onError?.(err);
      },
    });
    this.log.info("streaming relay started", { slot: this.replication.slot });
  }

  async stop(): Promise<void> {
    if (!this.running) return;
    this.running = false;
    await this.stream?.stop();
    await this.tail; // drain any in-flight batch
    await this.retryRelay.stop(); // stops the loop + disconnects publisher
    this.log.info("streaming relay stopped");
  }

  /** Build the WAL stream. Overridable as a test seam. */
  protected createReplicationStream(): ReplicationStream {
    return createPgLogicalStream(this.replication);
  }

  private async processBatch(
    batch: DecodedInsert[],
    lsn: string,
  ): Promise<void> {
    try {
      for (let i = 0; i < batch.length; i += this.batchSize) {
        await this.publishChunk(batch.slice(i, i + this.batchSize));
      }
      await this.stream?.acknowledge(lsn);
    } catch (err) {
      // A failure here (e.g. a DB write while demoting) must NOT advance the
      // LSN: the commit is re-streamed and re-published on reconnect.
      const error = err instanceof Error ? err : new Error(String(err));
      this.log.error("streaming batch failed; not acknowledging", {
        error: error.message,
      });
      this.hooks.onError?.(error);
    }
  }

  private async publishChunk(chunk: DecodedInsert[]): Promise<void> {
    const records = chunk.map((c) => rowToRecord(c.row));
    const messages = await Promise.all(
      records.map((r) => buildPublishable(r, this.serializer)),
    );
    const results = await this.publisher.publish(messages);
    const byId = new Map(records.map((r) => [r.id, r]));

    const succeeded: string[] = [];
    for (const result of results) {
      const record = byId.get(result.recordId);
      if (!record) continue;
      if (result.ok) {
        succeeded.push(record.id);
        this.hooks.onPublished?.(result);
      } else {
        // Demote to failed (due now); the internal retry loop owns backoff/DLQ.
        await this.store.markFailed(record.id, null, "failed");
        this.hooks.onFailed?.(
          record,
          result.error ?? new Error("publish failed"),
          true,
        );
      }
    }
    if (this.markPublished && succeeded.length > 0) {
      await this.store.markDone(succeeded);
    }
  }
}

/** Real WAL stream backed by pg-logical-replication + pgoutput. Covered by the
 *  integration suite (it needs a live Postgres); unit tests use a fake stream. */
function createPgLogicalStream(config: ReplicationConfig): ReplicationStream {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let service: any = null;
  let stopped = false;
  const table = config.table ?? "outbox";

  return {
    async start(handlers: ReplicationStreamHandlers): Promise<void> {
      await ensureSlot(config.connectionString, config.slot);
      const mod = await importPgLogical();
      service = new mod.LogicalReplicationService(
        { connectionString: config.connectionString },
        { acknowledge: { auto: false, timeoutSeconds: 0 } },
      );
      const plugin = new mod.PgoutputPlugin({
        protoVersion: 2,
        publicationNames: [config.publication],
      });
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      service.on("data", (lsn: string, msg: any) => {
        if (msg?.tag === "insert" && msg?.relation?.name === table) {
          handlers.onInsert({ lsn, row: normalizeRow(msg.new) });
        } else if (msg?.tag === "commit") {
          void handlers.onCommit(lsn);
        }
      });
      service.on("error", (err: Error) => {
        if (stopped) return;
        handlers.onError(err);
        setTimeout(() => {
          if (stopped) return;
          service
            .subscribe(plugin, config.slot)
            .catch((e: Error) => handlers.onError(e));
        }, 1000);
      });
      service
        .subscribe(plugin, config.slot)
        .catch((e: Error) => {
          if (!stopped) handlers.onError(e);
        });
    },
    async acknowledge(lsn: string): Promise<void> {
      if (service) await service.acknowledge(lsn);
    },
    async stop(): Promise<void> {
      stopped = true;
      if (service) await service.stop();
    },
  };
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function normalizeRow(raw: any): OutboxRow {
  const json = (v: unknown) =>
    typeof v === "string" ? JSON.parse(v) : v;
  return {
    id: String(raw.id),
    message_id: raw.message_id,
    aggregate_type: raw.aggregate_type,
    aggregate_id: raw.aggregate_id,
    topic: raw.topic,
    key: raw.key ?? null,
    payload: json(raw.payload),
    headers: (json(raw.headers) as Record<string, string>) ?? {},
    trace_id: raw.trace_id ?? null,
    status: Number(raw.status),
    attempts: Number(raw.attempts),
    next_retry_at: raw.next_retry_at ? new Date(raw.next_retry_at) : null,
    created_at: raw.created_at ? new Date(raw.created_at) : new Date(0),
    processed_at: raw.processed_at ? new Date(raw.processed_at) : null,
  };
}

/** Create the persistent pgoutput slot if it does not already exist. */
async function ensureSlot(
  connectionString: string,
  slot: string,
): Promise<void> {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const pg: any = await import("pg");
  const client = new pg.Client({ connectionString });
  await client.connect();
  try {
    const existing = await client.query(
      "SELECT 1 FROM pg_replication_slots WHERE slot_name = $1",
      [slot],
    );
    if (existing.rows.length === 0) {
      await client.query(
        "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
        [slot],
      );
    }
  } finally {
    await client.end();
  }
}

async function importPgLogical(): Promise<{
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  LogicalReplicationService: any;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  PgoutputPlugin: any;
}> {
  try {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return (await import("pg-logical-replication")) as any;
  } catch {
    throw new Error(
      'Streaming relay needs the "pg-logical-replication" package. Run: npm i pg-logical-replication',
    );
  }
}
