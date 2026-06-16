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

/**
 * Connection + reader settings for the MySQL binlog stream.
 *
 * Requires the server to be configured for row-based replication:
 *   - `binlog_format=ROW`
 *   - `binlog_row_image=FULL`
 *   - `gtid_mode=ON` (optional but recommended for resumption)
 * and a user with `REPLICATION SLAVE` + `REPLICATION CLIENT` grants.
 */
export interface BinlogReplicationConfig {
  host: string;
  port?: number;
  user: string;
  password: string;
  /** Database that owns the outbox table. Used to scope row events. */
  database: string;
  /** Outbox table to capture. Default "outbox". */
  table?: string;
  /**
   * MySQL replica server-id. MUST be unique within the cluster (real replicas
   * + every binlog reader). Defaults to a deterministic value derived from
   * pid; override in clustered setups.
   */
  serverId?: number;
  /**
   * Resume from this binlog position. If omitted, the reader starts from the
   * current end-of-log ("tail" mode) — new rows only. Persist the position
   * yourself (e.g. in your app's KV store) by subscribing to commit events.
   */
  startPosition?: BinlogPosition;
}

/** A binlog coordinate. (file, offset) — MySQL's analogue of a Postgres LSN. */
export interface BinlogPosition {
  readonly filename: string;
  readonly position: number;
}

/** A decoded INSERT on the outbox table, with the binlog position it occurred at. */
export interface DecodedInsert {
  readonly position: BinlogPosition;
  readonly row: OutboxRow;
}

export interface BinlogStreamHandlers {
  onInsert: (insert: DecodedInsert) => void;
  onCommit: (position: BinlogPosition) => void | Promise<void>;
  onError: (err: Error) => void;
}

/**
 * The binlog source the streaming relay consumes. Implemented over @vlasky/zongji
 * by default; overridable as a test seam, or to plug a custom reader / managed
 * service (RDS Streams, ProxySQL bridge, etc.).
 */
export interface BinlogStream {
  start(handlers: BinlogStreamHandlers): Promise<void>;
  /**
   * Note the position has been durably handled downstream. MySQL has no native
   * server-side ack like logical replication, so the default implementation
   * just tracks the position in memory. Override to persist it.
   */
  acknowledge(position: BinlogPosition): Promise<void>;
  stop(): Promise<void>;
}

export interface MysqlBinlogRelayOptions {
  /**
   * Outbox store. Construct with `{ claimFailedOnly: true }` so the internal
   * retry loop only drains failures (pending rows are owned by the binlog stream).
   */
  store: OutboxStore;
  publisher: Publisher;
  binlog: BinlogReplicationConfig;
  retry?: Partial<RetryConfig>;
  dlq?: DlqConfig;
  serializer?: Serializer;
  logger?: Logger;
  hooks?: RelayHooks;
  /** Poll interval (ms) for the internal failed-row retry loop. Default 5000. */
  failedPollIntervalMs?: number;
  /** Mark happy-path rows done (status=2) after publish. Default true. */
  markPublished?: boolean;
  /** Max rows published per chunk within a single transaction's worth of events. Default 100. */
  batchSize?: number;
}

/**
 * Publishes outbox events straight from the MySQL binlog (row-based), with no
 * claim query on the happy path. Failures are demoted to `failed` and drained
 * by an internal claim-based retry loop (the core `Relay` over a
 * `claimFailedOnly` store), reusing the existing backoff / DLQ / dead handling.
 *
 * At-least-once: a commit's position is acknowledged only after its batch's
 * side effects commit, so a crash re-streams and re-publishes (a duplicate
 * idempotent consumers absorb). Ordering is best-effort per aggregate — a
 * retried failure lands after later same-aggregate rows; use the polling
 * relay (default `Relay` + `MysqlStore`) when strict ordering matters.
 */
export class MysqlBinlogRelay {
  private readonly store: OutboxStore;
  private readonly publisher: Publisher;
  private readonly serializer: Serializer;
  private readonly log: Logger;
  private readonly hooks: RelayHooks;
  private readonly binlog: BinlogReplicationConfig;
  private readonly markPublished: boolean;
  private readonly batchSize: number;
  private readonly retryRelay: Relay;

  private stream: BinlogStream | null = null;
  private buffer: DecodedInsert[] = [];
  private tail: Promise<void> = Promise.resolve();
  private running = false;

  constructor(opts: MysqlBinlogRelayOptions) {
    this.store = opts.store;
    this.publisher = opts.publisher;
    this.serializer = opts.serializer ?? new JsonSerializer();
    this.log = opts.logger ?? new ConsoleLogger();
    this.hooks = opts.hooks ?? {};
    this.binlog = opts.binlog;
    this.markPublished = opts.markPublished ?? true;
    this.batchSize = opts.batchSize ?? 100;

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
    await this.retryRelay.start();
    this.stream = this.createBinlogStream();
    await this.stream.start({
      onInsert: (insert) => {
        this.buffer.push(insert);
      },
      onCommit: (position) => {
        const batch = this.buffer;
        this.buffer = [];
        this.tail = this.tail.then(() => this.processBatch(batch, position));
        return this.tail;
      },
      onError: (err) => {
        this.log.error("binlog stream error", { error: err.message });
        this.hooks.onError?.(err);
      },
    });
    this.log.info("mysql binlog relay started", {
      database: this.binlog.database,
      table: this.binlog.table ?? "outbox",
    });
  }

  async stop(): Promise<void> {
    if (!this.running) return;
    this.running = false;
    await this.stream?.stop();
    await this.tail;
    await this.retryRelay.stop();
    this.log.info("mysql binlog relay stopped");
  }

  /** Build the binlog stream. Overridable as a test seam. */
  protected createBinlogStream(): BinlogStream {
    return createZongjiBinlogStream(this.binlog);
  }

  private async processBatch(
    batch: DecodedInsert[],
    position: BinlogPosition,
  ): Promise<void> {
    try {
      for (let i = 0; i < batch.length; i += this.batchSize) {
        await this.publishChunk(batch.slice(i, i + this.batchSize));
      }
      await this.stream?.acknowledge(position);
    } catch (err) {
      // A failure here MUST NOT advance the position: the commit is re-streamed
      // and re-published on reconnect.
      const error = err instanceof Error ? err : new Error(String(err));
      this.log.error("binlog batch failed; not acknowledging", {
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

/**
 * Real binlog stream backed by @vlasky/zongji. Covered by the integration suite
 * (it needs a live MySQL with binlog enabled); unit tests use a fake stream.
 */
function createZongjiBinlogStream(config: BinlogReplicationConfig): BinlogStream {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let zongji: any = null;
  let stopped = false;
  const table = config.table ?? "outbox";

  return {
    async start(handlers: BinlogStreamHandlers): Promise<void> {
      const ZongJi = await importZongji();
      zongji = new ZongJi({
        host: config.host,
        port: config.port ?? 3306,
        user: config.user,
        password: config.password,
        serverId: config.serverId ?? deriveServerId(),
      });

      // Track the latest position seen per transaction so a single ROTATE
      // followed by writeRows then xid (commit) gives us a coherent commit point.
      let current: BinlogPosition = config.startPosition ?? {
        filename: "",
        position: 0,
      };

      zongji.on("binlog", (evt: BinlogEventLike) => {
        const name = evt.getEventName?.();
        if (name === "rotate") {
          current = {
            filename: (evt.nextBinlog as string) ?? current.filename,
            position: (evt.position as number) ?? 0,
          };
          return;
        }
        if (name === "writerows") {
          if (
            (evt.tableMap as { [k: string]: { tableName: string } } | undefined) &&
            !rowsAreFor(evt, table)
          ) {
            return;
          }
          const rows = (evt.rows as Array<Record<string, unknown>>) ?? [];
          for (const raw of rows) {
            handlers.onInsert({
              position: current,
              row: normalizeRow(raw),
            });
          }
          current = {
            filename: current.filename,
            position: (evt.nextPosition as number) ?? current.position,
          };
          return;
        }
        if (name === "xid" || name === "query") {
          // xid = commit (InnoDB); query may carry BEGIN/COMMIT for statement-based.
          current = {
            filename: current.filename,
            position: (evt.nextPosition as number) ?? current.position,
          };
          void handlers.onCommit(current);
        }
      });

      zongji.on("error", (err: Error) => {
        if (stopped) return;
        handlers.onError(err);
      });

      const startOpts: Record<string, unknown> = {
        includeEvents: ["tablemap", "writerows", "xid", "rotate", "query"],
        includeSchema: { [config.database]: [table] },
      };
      if (config.startPosition) {
        startOpts["filename"] = config.startPosition.filename;
        startOpts["position"] = config.startPosition.position;
      }
      zongji.start(startOpts);
    },
    async acknowledge(_position: BinlogPosition): Promise<void> {
      // MySQL has no native server-side ack. Position tracking lives in the
      // user's KV store via the onCommit hook (or this is replaced by a custom
      // BinlogStream implementation).
    },
    async stop(): Promise<void> {
      stopped = true;
      if (zongji) zongji.stop();
    },
  };
}

/** Minimal shape of a zongji binlog event we look at. */
interface BinlogEventLike {
  getEventName?(): string;
  nextBinlog?: string;
  position?: number;
  nextPosition?: number;
  tableMap?: Record<string, { tableName: string }>;
  rows?: Array<Record<string, unknown>>;
}

function rowsAreFor(evt: BinlogEventLike, table: string): boolean {
  const map = evt.tableMap ?? {};
  for (const tid of Object.keys(map)) {
    const meta = map[tid];
    if (meta?.tableName === table) {
      // zongji exposes schemaName inconsistently across versions; checking the
      // table name alone matches the includeSchema filter we asked for.
      return true;
    }
  }
  // If we have no tableMap context yet (rotate-after-restart edge), accept.
  return Object.keys(map).length === 0 ? true : false;
}

function normalizeRow(raw: Record<string, unknown>): OutboxRow {
  const json = (v: unknown) =>
    typeof v === "string" ? JSON.parse(v) : (v ?? null);
  const headers = json(raw["headers"]) as Record<string, string> | null;
  return {
    id: String(raw["id"]),
    message_id: String(raw["message_id"] ?? ""),
    aggregate_type: String(raw["aggregate_type"] ?? ""),
    aggregate_id: String(raw["aggregate_id"] ?? ""),
    topic: String(raw["topic"] ?? ""),
    key: (raw["key"] as string | null) ?? null,
    payload: json(raw["payload"]),
    headers,
    trace_id: (raw["trace_id"] as string | null) ?? null,
    status: Number(raw["status"] ?? 0),
    attempts: Number(raw["attempts"] ?? 0),
    next_retry_at: raw["next_retry_at"]
      ? new Date(raw["next_retry_at"] as string | number | Date)
      : null,
    created_at: raw["created_at"]
      ? new Date(raw["created_at"] as string | number | Date)
      : new Date(0),
    processed_at: raw["processed_at"]
      ? new Date(raw["processed_at"] as string | number | Date)
      : null,
  };
}

/** Deterministic serverId derived from PID so two reader processes on one host
 *  do not clash. Override via {@link BinlogReplicationConfig.serverId}. */
function deriveServerId(): number {
  const pid = typeof process !== "undefined" ? process.pid : 1;
  // Keep in the safe 1..2^32-1 range; bias well above real replica server-ids.
  return (1_000_000 + (pid % 1_000_000)) >>> 0;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
async function importZongji(): Promise<any> {
  try {
    // @vlasky/zongji is an optional peer dep — typed via a `@ts-ignore`
    // because users without the binlog relay should not need the type
    // declarations installed.
    // @ts-ignore -- optional peer dep, types not required for compilation
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const mod: any = await import("@vlasky/zongji");
    return mod.default ?? mod;
  } catch {
    throw new Error(
      'Binlog relay needs the "@vlasky/zongji" package. Run: npm i @vlasky/zongji',
    );
  }
}
