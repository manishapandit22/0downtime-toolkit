package io.github.zerodowntime.demo;

import io.github.zerodowntime.cli.Services;
import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;
import javax.sql.DataSource;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/** Postgres 16 and DynamoDB Local in Docker. Everything the demo measures runs on this machine. */
public final class Infra implements AutoCloseable {

    private final GenericContainer<?> postgres = new GenericContainer<>(DockerImageName.parse("postgres:16-alpine"))
            .withEnv("POSTGRES_PASSWORD", "demo")
            .withEnv("POSTGRES_USER", "demo")
            .withEnv("POSTGRES_DB", "demo")
            .withExposedPorts(5432)
            .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*\\n", 2));

    private final GenericContainer<?> dynamo = new GenericContainer<>(DockerImageName.parse("amazon/dynamodb-local:2.5.3"))
            .withExposedPorts(8000)
            .withCommand("-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb");

    public Infra() {
        postgres.start();
        dynamo.start();
    }

    /** Fresh schema and fresh tables, so repeated runs never see each other's data. */
    public Services newServices() {
        String id = UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        String schema = "demo_" + id;
        try (Connection c = pgDataSource(null).getConnection(); Statement st = c.createStatement()) {
            st.execute("CREATE SCHEMA " + schema);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
        DataSource pg = new io.github.zerodowntime.cli.PooledDataSource(pgDataSource(schema), 24);
        DynamoDbClient ddb = DynamoDbClient.builder()
                .endpointOverride(URI.create("http://" + dynamo.getHost() + ":" + dynamo.getMappedPort(8000)))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x")))
                .build();
        return Services.create(pg, ddb, "data_" + id, "control_" + id);
    }

    private DataSource pgDataSource(String schema) {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setServerNames(new String[] {postgres.getHost()});
        ds.setPortNumbers(new int[] {postgres.getMappedPort(5432)});
        ds.setDatabaseName("demo");
        ds.setUser("demo");
        ds.setPassword("demo");
        if (schema != null) {
            ds.setCurrentSchema(schema);
        }
        return ds;
    }

    @Override
    public void close() {
        dynamo.stop();
        postgres.stop();
    }
}
