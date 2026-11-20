package io.github.zerodowntime.cli;

import java.net.URI;
import org.postgresql.ds.PGSimpleDataSource;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;

/**
 * Entry point. Configuration comes from the environment so no secret ever lands in shell history:
 * PG_URL (jdbc:postgresql://host:5432/db), PG_USER, PG_PASSWORD, DATA_TABLE, CONTROL_TABLE,
 * AWS_REGION (default us-east-1), DDB_ENDPOINT (only for DynamoDB Local / LocalStack).
 * Real AWS credentials come from the standard AWS credential chain.
 */
public final class Main {

    private Main() {}

    public static void main(String[] args) {
        String pgUrl = require("PG_URL");
        PGSimpleDataSource pg = new PGSimpleDataSource();
        pg.setUrl(pgUrl);
        pg.setUser(System.getenv("PG_USER"));
        pg.setPassword(System.getenv("PG_PASSWORD"));

        DynamoDbClientBuilder ddb = DynamoDbClient.builder()
                .region(Region.of(System.getenv().getOrDefault("AWS_REGION", "us-east-1")));
        String endpoint = System.getenv("DDB_ENDPOINT");
        if (endpoint != null && !endpoint.isBlank()) {
            ddb.endpointOverride(URI.create(endpoint));
        }

        Services services = Services.create(new PooledDataSource(pg, 8), ddb.build(), require("DATA_TABLE"), require("CONTROL_TABLE"));
        System.exit(new Cli(services, System.out, System.err).run(args));
    }

    private static String require(String name) {
        String v = System.getenv(name);
        if (v == null || v.isBlank()) {
            System.err.println("missing environment variable " + name);
            System.exit(Cli.USAGE);
        }
        return v;
    }
}
