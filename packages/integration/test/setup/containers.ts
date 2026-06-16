import {
  MySqlContainer,
  type StartedMySqlContainer,
} from "@testcontainers/mysql";
import {
  PostgreSqlContainer,
  type StartedPostgreSqlContainer,
} from "@testcontainers/postgresql";
import {
  RedpandaContainer,
  type StartedRedpandaContainer,
} from "@testcontainers/redpanda";

let pg: StartedPostgreSqlContainer | undefined;
let mysql: StartedMySqlContainer | undefined;
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

  mysql = await new MySqlContainer("mysql:8.0")
    .withUsername("root")
    .withRootPassword("test")
    .withDatabase("eventferry")
    // Row-based binlog is required for MysqlBinlogRelay; the server-id and
    // log-bin flags turn the binlog on. GTID mode is recommended for safer
    // resumption when the relay reconnects.
    .withCommand([
      "--server-id=1",
      "--log-bin=mysql-bin",
      "--binlog-format=ROW",
      "--binlog-row-image=FULL",
      "--gtid-mode=ON",
      "--enforce-gtid-consistency=ON",
    ])
    .start();

  redpanda = await new RedpandaContainer(
    "redpandadata/redpanda:latest",
  ).start();

  process.env.PG_URL = pg.getConnectionUri();
  process.env.MYSQL_HOST = mysql.getHost();
  process.env.MYSQL_PORT = String(mysql.getPort());
  process.env.MYSQL_USER = "root";
  process.env.MYSQL_PASSWORD = "test";
  process.env.MYSQL_DATABASE = "eventferry";
  process.env.KAFKA_BROKERS = redpanda.getBootstrapServers();
  process.env.SCHEMA_REGISTRY_URL = redpanda.getSchemaRegistryAddress();
}

export async function teardown(): Promise<void> {
  await redpanda?.stop();
  await mysql?.stop();
  await pg?.stop();
}
