package io.github.zerodowntime.core;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;
import javax.sql.DataSource;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/** One shared Postgres container per test JVM; each test gets its own schema. */
final class PostgresTestSupport {

    private static final GenericContainer<?> CONTAINER =
            new GenericContainer<>(DockerImageName.parse("postgres:16-alpine"))
                    .withEnv("POSTGRES_PASSWORD", "test")
                    .withEnv("POSTGRES_USER", "test")
                    .withEnv("POSTGRES_DB", "test")
                    .withExposedPorts(5432)
                    .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*\\n", 2));

    private PostgresTestSupport() {}

    /** A data source pointing at a brand new schema with the source tables already created. */
    static synchronized DataSource newSchema() {
        if (!CONTAINER.isRunning()) {
            CONTAINER.start();
        }
        String schema = "s_" + UUID.randomUUID().toString().replace("-", "");
        DataSource admin = dataSource(null);
        try (Connection c = admin.getConnection(); Statement s = c.createStatement()) {
            s.execute("CREATE SCHEMA " + schema);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
        DataSource ds = dataSource(schema);
        SourceSchema.create(ds);
        return ds;
    }

    private static DataSource dataSource(String schema) {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setServerNames(new String[] {CONTAINER.getHost()});
        ds.setPortNumbers(new int[] {CONTAINER.getMappedPort(5432)});
        ds.setDatabaseName("test");
        ds.setUser("test");
        ds.setPassword("test");
        if (schema != null) {
            ds.setCurrentSchema(schema);
        }
        return ds;
    }
}
