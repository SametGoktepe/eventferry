import type { OutboxRecord, Serializer } from "@eventferry/core";

export type SchemaType = "AVRO" | "PROTOBUF" | "JSON";

export interface SchemaSpec {
  type: SchemaType;
  /** Schema definition string (avsc JSON / .proto / JSON Schema). */
  schema: string;
}

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
  /** Per-topic schema to register. Topics omitted here use the subject's latest. */
  schemas?: Record<string, SchemaSpec>;
  /** Subject naming. Default TopicNameStrategy: `${topic}-value`. */
  subject?: (topic: string) => string;
  /** content-type header value. Default "application/vnd.confluent.avro". */
  contentType?: string;
}

const DEFAULT_CONTENT_TYPE = "application/vnd.confluent.avro";

/**
 * A core {@link Serializer} that encodes payloads with a Confluent Schema Registry
 * (Avro / Protobuf / JSON Schema). Drop it into `Relay`/`PostgresStreamingRelay`'s
 * `serializer` option. The schema id per topic is resolved once and cached.
 */
export class SchemaRegistrySerializer implements Serializer {
  readonly contentType: string;
  private readonly schemas: Record<string, SchemaSpec>;
  private readonly subject: (topic: string) => string;
  private readonly host: string | null;
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
    this.schemas = opts.schemas ?? {};
    this.subject = opts.subject ?? ((topic) => `${topic}-value`);
    this.contentType = opts.contentType ?? DEFAULT_CONTENT_TYPE;
  }

  async serialize(record: OutboxRecord): Promise<Buffer> {
    const registry = await this.getRegistry();
    const id = await this.schemaId(registry, record.topic);
    return registry.encode(id, record.payload);
  }

  private schemaId(
    registry: SchemaRegistryClient,
    topic: string,
  ): Promise<number> {
    const cached = this.idCache.get(topic);
    if (cached) return cached;

    const subject = this.subject(topic);
    const spec = this.schemas[topic];
    const lookup = spec
      ? registry
          .register({ type: spec.type, schema: spec.schema }, { subject })
          .then((r) => r.id)
      : registry.getLatestSchemaId(subject);

    // Cache the in-flight promise so concurrent first calls don't double-register;
    // drop it on failure so a transient error can be retried.
    const guarded = lookup.catch((err) => {
      this.idCache.delete(topic);
      throw err;
    });
    this.idCache.set(topic, guarded);
    return guarded;
  }

  private async getRegistry(): Promise<SchemaRegistryClient> {
    if (this.registry) return this.registry;
    const mod = await importSchemaRegistry();
    this.registry = new mod.SchemaRegistry({ host: this.host as string });
    return this.registry;
  }
}

async function importSchemaRegistry(): Promise<{
  SchemaRegistry: new (cfg: { host: string }) => SchemaRegistryClient;
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
