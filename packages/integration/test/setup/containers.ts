import {
  PostgreSqlContainer,
  type StartedPostgreSqlContainer,
} from "@testcontainers/postgresql";
import {
  RedpandaContainer,
  type StartedRedpandaContainer,
} from "@testcontainers/redpanda";

let pg: StartedPostgreSqlContainer | undefined;
let redpanda: StartedRedpandaContainer | undefined;

export async function setup(): Promise<void> {
  pg = await new PostgreSqlContainer("postgres:16")
    // wal_level=logical is required for the streaming relay (logical replication).
    .withCommand(["postgres", "-c", "wal_level=logical"])
    // The default pg_hba only allows replication from localhost; the mapped
    // (host) connection appears non-local, so allow replication from anywhere.
    .withCopyContentToContainer([
      {
        content:
          "#!/bin/bash\necho 'host replication all all trust' >> \"$PGDATA/pg_hba.conf\"\n",
        target: "/docker-entrypoint-initdb.d/00-replication-hba.sh",
        mode: 0o755,
      },
    ])
    .start();

  redpanda = await new RedpandaContainer(
    "redpandadata/redpanda:latest",
  ).start();

  process.env.PG_URL = pg.getConnectionUri();
  process.env.KAFKA_BROKERS = redpanda.getBootstrapServers();
  process.env.SCHEMA_REGISTRY_URL = redpanda.getSchemaRegistryAddress();
}

export async function teardown(): Promise<void> {
  await redpanda?.stop();
  await pg?.stop();
}
