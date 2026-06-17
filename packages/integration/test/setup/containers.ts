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
import {
  GenericContainer,
  Wait,
  type StartedTestContainer,
} from "testcontainers";

let pg: StartedPostgreSqlContainer | undefined;
let mysql: StartedMySqlContainer | undefined;
let mariadb: StartedTestContainer | undefined;
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
    .withUsername("eventferry")
    .withUserPassword("test")
    .withRootPassword("test")
    .withDatabase("eventferry")
    // Row-based binlog is required for MysqlBinlogRelay. Drop the config into
    // /etc/mysql/conf.d (read by the official mysql:8.0 image's entrypoint)
    // instead of `.withCommand(...)`, which would clobber the image's default
    // mysqld arg-set and break first-run initialisation.
    .withCopyContentToContainer([
      {
        content: [
          "[mysqld]",
          "server-id=1",
          "log-bin=mysql-bin",
          "binlog-format=ROW",
          "binlog-row-image=FULL",
          "gtid-mode=ON",
          "enforce-gtid-consistency=ON",
          "",
        ].join("\n"),
        target: "/etc/mysql/conf.d/binlog.cnf",
        mode: 0o644,
      },
      // Init script: grant REPLICATION privileges to the app user so the same
      // user that the store uses can also drive the binlog relay. Runs once on
      // first-boot (the image's docker-entrypoint processes *.sql here).
      {
        content:
          "GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'eventferry'@'%';\nFLUSH PRIVILEGES;\n",
        target: "/docker-entrypoint-initdb.d/01-replication-grants.sql",
        mode: 0o644,
      },
    ])
    // Cold MySQL 8 + binlog init can take a while on a fresh runner.
    .withStartupTimeout(180_000)
    .start();

  // MariaDB 10.11 (LTS) — same `MysqlStore` code, different engine. The
  // mariadb image takes MARIADB_* env vars; auth/grant shape matches MySQL's
  // and the connection is reachable via the same `mysql2` driver.
  // We use GenericContainer (not @testcontainers/mysql) because that helper
  // hard-codes mysql-image env names and entrypoint expectations.
  mariadb = await new GenericContainer("mariadb:10.11")
    .withEnvironment({
      MARIADB_ROOT_PASSWORD: "test",
      MARIADB_USER: "eventferry",
      MARIADB_PASSWORD: "test",
      MARIADB_DATABASE: "eventferry",
    })
    .withExposedPorts(3306)
    .withWaitStrategy(
      Wait.forLogMessage(/ready for connections/, 2),
    )
    .withStartupTimeout(180_000)
    .start();

  redpanda = await new RedpandaContainer(
    "redpandadata/redpanda:latest",
  ).start();

  process.env.PG_URL = pg.getConnectionUri();
  process.env.MYSQL_HOST = mysql.getHost();
  process.env.MYSQL_PORT = String(mysql.getPort());
  process.env.MYSQL_USER = "eventferry";
  process.env.MYSQL_PASSWORD = "test";
  process.env.MYSQL_DATABASE = "eventferry";
  process.env.MARIADB_HOST = mariadb.getHost();
  process.env.MARIADB_PORT = String(mariadb.getMappedPort(3306));
  process.env.MARIADB_USER = "eventferry";
  process.env.MARIADB_PASSWORD = "test";
  process.env.MARIADB_DATABASE = "eventferry";
  process.env.KAFKA_BROKERS = redpanda.getBootstrapServers();
  process.env.SCHEMA_REGISTRY_URL = redpanda.getSchemaRegistryAddress();
}

export async function teardown(): Promise<void> {
  await redpanda?.stop();
  await mariadb?.stop();
  await mysql?.stop();
  await pg?.stop();
}
