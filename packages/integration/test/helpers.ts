import { Kafka } from "kafkajs";
import mysql from "mysql2/promise";
import { Pool } from "pg";

export function pgUrl(): string {
  const url = process.env.PG_URL;
  if (!url) throw new Error("PG_URL not set (global setup did not run?)");
  return url;
}

export function newPool(): Pool {
  return new Pool({ connectionString: pgUrl() });
}

export interface MysqlConnectionInfo {
  host: string;
  port: number;
  user: string;
  password: string;
  database: string;
}

export function mysqlInfo(): MysqlConnectionInfo {
  const host = process.env.MYSQL_HOST;
  const port = process.env.MYSQL_PORT;
  const user = process.env.MYSQL_USER;
  const password = process.env.MYSQL_PASSWORD;
  const database = process.env.MYSQL_DATABASE;
  if (!host || !port || !user || !password || !database) {
    throw new Error("MYSQL_* env vars not set (global setup did not run?)");
  }
  return { host, port: Number(port), user, password, database };
}

export function newMysqlPool(): mysql.Pool {
  return mysql.createPool({
    ...mysqlInfo(),
    // BIGINT ids returned as strings so id comparisons are stable.
    supportBigNumbers: true,
    bigNumberStrings: true,
    // Stable date handling for the reaper.
    dateStrings: false,
  });
}

export function brokers(): string[] {
  const b = process.env.KAFKA_BROKERS;
  if (!b) throw new Error("KAFKA_BROKERS not set");
  // kafkajs wants host:port without a scheme.
  return [b.includes("://") ? b.split("://")[1]! : b];
}

export function schemaRegistryUrl(): string {
  const url = process.env.SCHEMA_REGISTRY_URL;
  if (!url) throw new Error("SCHEMA_REGISTRY_URL not set");
  return url;
}

let seq = 0;
/** Unique identifier so shared-container tests never collide on table/topic/group. */
export function uniqueName(prefix: string): string {
  seq += 1;
  return `${prefix}_${Date.now().toString(36)}_${seq}`;
}

/** Pre-create a topic (Redpanda does not auto-create on produce here). */
export async function createTopic(topic: string, partitions = 1): Promise<void> {
  const kafka = new Kafka({ clientId: uniqueName("admin"), brokers: brokers() });
  const admin = kafka.admin();
  await admin.connect();
  try {
    await admin.createTopics({
      topics: [{ topic, numPartitions: partitions }],
      waitForLeaders: true,
    });
  } finally {
    await admin.disconnect();
  }
}

export interface ConsumedMessage {
  key: string | null;
  value: Buffer;
  headers: Record<string, string>;
}

/** Subscribe from the beginning and collect `count` messages (or time out). */
export async function collectMessages(
  topic: string,
  count: number,
  timeoutMs = 20_000,
): Promise<ConsumedMessage[]> {
  const kafka = new Kafka({ clientId: uniqueName("it"), brokers: brokers() });
  const consumer = kafka.consumer({ groupId: uniqueName("itg") });
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });

  const out: ConsumedMessage[] = [];
  try {
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new Error(`timed out waiting for ${count} msg(s) on ${topic}`)),
        timeoutMs,
      );
      void consumer.run({
        eachMessage: async ({ message }) => {
          const headers: Record<string, string> = {};
          for (const [k, v] of Object.entries(message.headers ?? {})) {
            headers[k] = v == null ? "" : v.toString();
          }
          out.push({
            key: message.key ? message.key.toString() : null,
            value: message.value ?? Buffer.alloc(0),
            headers,
          });
          if (out.length >= count) {
            clearTimeout(timer);
            resolve();
          }
        },
      });
    });
  } finally {
    await consumer.disconnect();
  }
  return out;
}
