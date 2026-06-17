import type { OutboxRecord, Serializer } from "@eventferry/core";

export type SchemaType = "AVRO" | "PROTOBUF" | "JSON";

export interface SchemaSpec {
  type: SchemaType;
  /** Schema definition string (avsc JSON / .proto / JSON Schema). */
  schema: string;
}

/**
 * Subject naming strategy. Mirrors Confluent's three built-ins:
 *
 * - `"TopicNameStrategy"` (default) — `${topic}-value` / `${topic}-key`.
 *   The conventional default; one schema per (topic, isKey) tuple.
 * - `"RecordNameStrategy"` — `${recordName}`. Same record type can flow
 *   on multiple topics. Requires a `recordName` resolver.
 * - `"TopicRecordNameStrategy"` — `${topic}-${recordName}`. Multiple record
 *   types per topic. Requires a `recordName` resolver.
 *
 * Set `subject` (function form) to override entirely.
 */
export type SubjectNameStrategy =
  | "TopicNameStrategy"
  | "RecordNameStrategy"
  | "TopicRecordNameStrategy";

/**
 * Authentication for the Schema Registry HTTP API.
 *
 * - `"basic"` — HTTP Basic Auth. The shape Confluent Cloud and most
 *   commercial registries use; passed straight through to the underlying
 *   client's `auth` config.
 * - `"bearer"` — `Authorization: Bearer <token>` header. The `token`
 *   field accepts either a static string OR a callable that resolves a
 *   fresh token on every request (cache inside your callable to amortise
 *   cost; we don't memoize for you, so OAuth refresh-loop logic lives
 *   on your side).
 *
 * mTLS for the registry connection itself is handled by Node's `tls`
 * stack — supply a custom `https.Agent` via the upstream client's
 * `agent` option (use the `registry` injection here and configure it
 * yourself), separate from the broker TLS the publisher uses.
 */
export type SchemaRegistryAuth =
  | { type: "basic"; username: string; password: string }
  | {
      type: "bearer";
      token: string | (() => string | Promise<string>);
    };

/**
 * The subset of a Confluent Schema Registry client this serializer uses. The
 * `@kafkajs/confluent-schema-registry` `SchemaRegistry` satisfies it structurally.
 */
export interface SchemaRegistryClient {
  register(
    schema: { type: string; schema: string },
    opts?: { subject: string },
  ): Promise<{ id: number }>;
  getLatestSchemaId(subject: string): Promise<number>;
  encode(registryId: number, payload: unknown): Promise<Buffer>;
}

export interface SchemaRegistrySerializerOptions {
  /** Inject a ready client (tests, custom config). */
  registry?: SchemaRegistryClient;
  /** Or construct one from a host (requires @kafkajs/confluent-schema-registry). */
  host?: string;
  /**
   * Optional authentication for the Schema Registry HTTP API. See
   * {@link SchemaRegistryAuth} for the two supported shapes. Ignored
   * when `registry` is provided (configure auth on the injected client
   * yourself in that case).
   */
  auth?: SchemaRegistryAuth;
  /** Per-topic VALUE schema to register. Topics omitted here use the subject's latest. */
  schemas?: Record<string, SchemaSpec>;
  /**
   * Per-topic KEY schema. When set, `serializeKey(record)` Avro-encodes
   * the record key for the matching topic. Topics omitted here fall back
   * to the subject's latest (or, with `autoRegister: false`, ALWAYS the
   * subject's latest).
   */
  keySchemas?: Record<string, SchemaSpec>;
  /**
   * Subject naming strategy preset. Default `"TopicNameStrategy"`.
   * Setting `subject` (function) overrides this entirely.
   */
  subjectStrategy?: SubjectNameStrategy;
  /**
   * Resolve the schema's record name (used by `RecordNameStrategy` and
   * `TopicRecordNameStrategy`). REQUIRED when `subjectStrategy` is set to
   * one of those — throws on first serialize if absent.
   *
   * Typical implementation: read `${namespace}.${name}` from the avsc you
   * already supply via `schemas` / `keySchemas`.
   */
  recordName?: (record: OutboxRecord, isKey: boolean) => string;
  /**
   * Custom subject function — overrides BOTH `subjectStrategy` and
   * `recordName`. Receives `(topic, isKey, record)` for full flexibility.
   *
   * Backwards-compatible with the single-argument legacy form
   * `(topic) => string` — extra args are ignored by JavaScript.
   */
  subject?: (
    topic: string,
    isKey?: boolean,
    record?: OutboxRecord,
  ) => string;
  /** content-type header value. Default "application/vnd.confluent.avro". */
  contentType?: string;
  /**
   * Auto-register schemas when one is supplied via `schemas` / `keySchemas`.
   * Default `true` — matches Confluent client behavior.
   *
   * Set to `false` for production clusters where schemas are managed
   * out-of-band (Confluent Cloud, regulated environments). With
   * autoRegister off, the serializer ALWAYS resolves by `getLatestSchemaId`
   * on the computed subject — and the locally-supplied schema bytes are
   * ignored.
   */
  autoRegister?: boolean;
}

const DEFAULT_CONTENT_TYPE = "application/vnd.confluent.avro";

/**
 * A core {@link Serializer} that encodes payloads with a Confluent Schema Registry
 * (Avro / Protobuf / JSON Schema). Drop it into `Relay`/`PostgresStreamingRelay`'s
 * `serializer` option. The schema id per (topic, isKey) tuple is resolved once
 * and cached.
 *
 * Also exposes `serializeKey(record)` for users who want Avro-encoded message
 * keys — call it manually when building the publish path; the relay does NOT
 * call it automatically (key encoding is application-level by convention).
 */
export class SchemaRegistrySerializer implements Serializer {
  readonly contentType: string;
  private readonly schemas: Record<string, SchemaSpec>;
  private readonly keySchemas: Record<string, SchemaSpec>;
  private readonly subjectStrategy: SubjectNameStrategy;
  private readonly recordName:
    | ((record: OutboxRecord, isKey: boolean) => string)
    | null;
  private readonly subjectFn:
    | ((topic: string, isKey?: boolean, record?: OutboxRecord) => string)
    | null;
  private readonly host: string | null;
  private readonly auth: SchemaRegistryAuth | null;
  private readonly autoRegister: boolean;
  // Keyed by `${topic}:${isKey}` to keep value- and key-subject ids distinct.
  private readonly idCache = new Map<string, Promise<number>>();
  private registry: SchemaRegistryClient | null;

  constructor(opts: SchemaRegistrySerializerOptions) {
    if (!opts.registry && !opts.host) {
      throw new Error(
        "SchemaRegistrySerializer requires either a `registry` client or a `host`.",
      );
    }
    this.registry = opts.registry ?? null;
    this.host = opts.host ?? null;
    this.auth = opts.auth ?? null;
    this.schemas = opts.schemas ?? {};
    this.keySchemas = opts.keySchemas ?? {};
    this.subjectStrategy = opts.subjectStrategy ?? "TopicNameStrategy";
    this.recordName = opts.recordName ?? null;
    this.subjectFn = opts.subject ?? null;
    this.contentType = opts.contentType ?? DEFAULT_CONTENT_TYPE;
    this.autoRegister = opts.autoRegister ?? true;
  }

  async serialize(record: OutboxRecord): Promise<Buffer> {
    const registry = await this.getRegistry();
    const subject = this.resolveSubject(record, false);
    const id = await this.schemaId(registry, subject, this.schemas[record.topic], false, record.topic);
    return registry.encode(id, record.payload);
  }

  /**
   * Avro-encode the record's KEY using the registered key schema. Returns
   * `null` when the record has no key (kafkajs/confluent treat null keys
   * as the producer-side "no key" signal).
   *
   * Not part of the core `Serializer` interface — callers wire it into
   * their publish path manually when they want Avro keys instead of raw
   * UTF-8 strings.
   */
  async serializeKey(record: OutboxRecord): Promise<Buffer | null> {
    if (record.key === null || record.key === undefined) return null;
    const registry = await this.getRegistry();
    const subject = this.resolveSubject(record, true);
    const id = await this.schemaId(
      registry,
      subject,
      this.keySchemas[record.topic],
      true,
      record.topic,
    );
    return registry.encode(id, record.key);
  }

  /**
   * Resolve the subject for this (record, isKey) tuple. Order of
   * precedence: explicit `subject` function → `subjectStrategy` preset.
   */
  private resolveSubject(record: OutboxRecord, isKey: boolean): string {
    if (this.subjectFn) return this.subjectFn(record.topic, isKey, record);
    switch (this.subjectStrategy) {
      case "TopicNameStrategy":
        return `${record.topic}-${isKey ? "key" : "value"}`;
      case "RecordNameStrategy":
        return this.recordNameFor(record, isKey);
      case "TopicRecordNameStrategy":
        return `${record.topic}-${this.recordNameFor(record, isKey)}`;
    }
  }

  private recordNameFor(record: OutboxRecord, isKey: boolean): string {
    if (!this.recordName) {
      throw new Error(
        `SchemaRegistrySerializer: subjectStrategy "${this.subjectStrategy}" requires a \`recordName\` resolver.`,
      );
    }
    return this.recordName(record, isKey);
  }

  private schemaId(
    registry: SchemaRegistryClient,
    subject: string,
    spec: SchemaSpec | undefined,
    isKey: boolean,
    topic: string,
  ): Promise<number> {
    const cacheKey = `${topic}:${isKey ? "key" : "value"}`;
    const cached = this.idCache.get(cacheKey);
    if (cached) return cached;

    // autoRegister=false → ALWAYS resolve by latest; the local spec
    // (if any) is ignored. Matches Confluent's auto.register.schemas=false.
    const lookup =
      spec && this.autoRegister
        ? registry
            .register({ type: spec.type, schema: spec.schema }, { subject })
            .then((r) => r.id)
        : registry.getLatestSchemaId(subject);

    // Cache the in-flight promise so concurrent first calls don't double-register;
    // drop it on failure so a transient error can be retried.
    const guarded = lookup.catch((err) => {
      this.idCache.delete(cacheKey);
      throw err;
    });
    this.idCache.set(cacheKey, guarded);
    return guarded;
  }

  private async getRegistry(): Promise<SchemaRegistryClient> {
    if (this.registry) return this.registry;
    const mod = await importSchemaRegistry();
    const cfg: SchemaRegistryConstructorConfig = {
      host: this.host as string,
    };
    if (this.auth) {
      if (this.auth.type === "basic") {
        // Confluent SR client accepts `auth: { username, password }`
        // and the mappersmith basic-auth middleware builds the header.
        cfg.auth = {
          username: this.auth.username,
          password: this.auth.password,
        };
      } else {
        // Bearer: SR doesn't ship a built-in middleware. Inject our own
        // so every API call carries `Authorization: Bearer <token>`.
        cfg.middlewares = [bearerAuthMiddleware(this.auth.token)];
      }
    }
    this.registry = new mod.SchemaRegistry(cfg);
    return this.registry;
  }
}

/**
 * Mappersmith middleware shape — the structural subset the Confluent
 * SR client passes through. We don't depend on mappersmith types
 * directly so the package compiles without the optional peer installed.
 */
interface MmRequest {
  enhance(args: { headers?: Record<string, string> }): MmRequest;
}

/**
 * Build a mappersmith middleware that adds `Authorization: Bearer <token>`.
 *
 * Exported for testing only — not part of the public API surface. The
 * middleware resolves the token on EVERY request, so callable token
 * providers can rotate without re-constructing the serializer. Cache
 * inside your provider if rotation cost matters.
 */
export function bearerAuthMiddleware(
  token: string | (() => string | Promise<string>),
) {
  return () => ({
    async prepareRequest(next: () => Promise<MmRequest>): Promise<MmRequest> {
      const request = await next();
      const value = typeof token === "function" ? await token() : token;
      return request.enhance({ headers: { Authorization: `Bearer ${value}` } });
    },
  });
}

/**
 * Structural shape the SR client's constructor accepts. Mirrors
 * `SchemaRegistryAPIClientArgs` from the upstream package without
 * importing the type directly (optional peer).
 */
interface SchemaRegistryConstructorConfig {
  host: string;
  auth?: { username: string; password: string };
  middlewares?: Array<() => unknown>;
}

async function importSchemaRegistry(): Promise<{
  SchemaRegistry: new (
    cfg: SchemaRegistryConstructorConfig,
  ) => SchemaRegistryClient;
}> {
  try {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return (await import("@kafkajs/confluent-schema-registry")) as any;
  } catch {
    throw new Error(
      'SchemaRegistrySerializer with `host` needs the "@kafkajs/confluent-schema-registry" package. Run: npm i @kafkajs/confluent-schema-registry',
    );
  }
}
