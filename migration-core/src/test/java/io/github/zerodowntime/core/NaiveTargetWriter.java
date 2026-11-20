package io.github.zerodowntime.core;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

/**
 * The BUGGY writer, kept in test code only. It does an unconditional put, so whoever writes
 * last wins, regardless of version. Exists to reproduce the backfill-vs-live-write race.
 */
final class NaiveTargetWriter implements TargetWriter {

    private final DynamoDbClient client;
    private final String table;
    private final DynamoTargetWriter reader;

    NaiveTargetWriter(DynamoDbClient client, String table) {
        this.client = client;
        this.table = table;
        this.reader = new DynamoTargetWriter(client, table);
    }

    @Override
    public WriteResult write(VersionedRecord record) {
        Map<String, AttributeValue> attrs = new HashMap<>();
        record.attributes().forEach((k, v) -> attrs.put(k, AttributeValue.fromS(v)));
        client.putItem(PutItemRequest.builder()
                .tableName(table)
                .item(Map.of(
                        DynamoTargetWriter.PK, AttributeValue.fromS(record.key()),
                        DynamoTargetWriter.VERSION, AttributeValue.fromN(Long.toString(record.version())),
                        DynamoTargetWriter.DELETED, AttributeValue.fromBool(record.deleted()),
                        DynamoTargetWriter.ATTRS, AttributeValue.fromM(attrs)))
                .build());
        return WriteResult.APPLIED;
    }

    @Override
    public Optional<VersionedRecord> read(String key) {
        return reader.read(key);
    }
}
