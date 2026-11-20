package io.github.zerodowntime.core;

import java.net.URI;
import java.util.UUID;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

/** Starts one shared DynamoDB Local container per test JVM. */
final class DynamoTestSupport {

    private static final GenericContainer<?> CONTAINER =
            new GenericContainer<>(DockerImageName.parse("amazon/dynamodb-local:2.5.3"))
                    .withExposedPorts(8000)
                    .withCommand("-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb");

    private static DynamoDbClient client;

    private DynamoTestSupport() {}

    static synchronized DynamoDbClient client() {
        if (client == null) {
            CONTAINER.start();
            client = DynamoDbClient.builder()
                    .endpointOverride(URI.create("http://" + CONTAINER.getHost() + ":" + CONTAINER.getMappedPort(8000)))
                    .region(Region.US_EAST_1)
                    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x")))
                    .build();
        }
        return client;
    }

    /** Creates a fresh uniquely named table so tests never share state. */
    static String newTable() {
        String name = "t_" + UUID.randomUUID().toString().replace("-", "");
        client().createTable(CreateTableRequest.builder()
                .tableName(name)
                .attributeDefinitions(AttributeDefinition.builder()
                        .attributeName(DynamoTargetWriter.PK)
                        .attributeType(ScalarAttributeType.S)
                        .build())
                .keySchema(KeySchemaElement.builder()
                        .attributeName(DynamoTargetWriter.PK)
                        .keyType(KeyType.HASH)
                        .build())
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .build());
        return name;
    }
}
