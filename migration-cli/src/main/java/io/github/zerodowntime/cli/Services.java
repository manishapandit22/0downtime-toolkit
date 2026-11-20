package io.github.zerodowntime.cli;

import io.github.zerodowntime.core.DynamoTargetWriter;
import io.github.zerodowntime.core.MigrationState;
import io.github.zerodowntime.core.OutboxStore;
import io.github.zerodowntime.core.SourceRepository;
import io.github.zerodowntime.core.TargetStore;
import javax.sql.DataSource;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/** The wired-up engine objects one CLI invocation works against. */
public record Services(
        DataSource postgres,
        DynamoDbClient dynamo,
        String dataTable,
        String controlTable,
        SourceRepository source,
        OutboxStore outbox,
        MigrationState state,
        DynamoTargetWriter dataWriter,
        TargetStore targetStore) {

    public static Services create(DataSource postgres, DynamoDbClient dynamo, String dataTable, String controlTable) {
        return new Services(
                postgres,
                dynamo,
                dataTable,
                controlTable,
                new SourceRepository(postgres),
                new OutboxStore(postgres),
                new MigrationState(postgres),
                new DynamoTargetWriter(dynamo, dataTable),
                new TargetStore(dynamo, dataTable, controlTable));
    }
}
