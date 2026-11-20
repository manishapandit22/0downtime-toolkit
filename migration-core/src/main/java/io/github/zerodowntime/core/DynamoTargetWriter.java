package io.github.zerodowntime.core;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

/**
 * DynamoDB target. Every write carries
 * {@code attribute_not_exists(pk) OR version < :v}, so the database itself rejects stale data.
 * No read-then-write, hence no race window between check and write.
 *
 * <p>Item layout: {@code pk} (S), {@code version} (N), {@code deleted} (BOOL), {@code attrs} (M of S).
 */
public final class DynamoTargetWriter implements TargetWriter, TargetScanner {

    static final String PK = "pk";
    static final String VERSION = "version";
    static final String DELETED = "deleted";
    static final String ATTRS = "attrs";

    private final DynamoDbClient client;
    private final String table;

    public DynamoTargetWriter(DynamoDbClient client, String table) {
        this.client = client;
        this.table = table;
    }

    @Override
    public WriteResult write(VersionedRecord record) {
        Map<String, AttributeValue> item = toItem(record);

        try {
            client.putItem(PutItemRequest.builder()
                    .tableName(table)
                    .item(item)
                    .conditionExpression("attribute_not_exists(#pk) OR #v < :v")
                    .expressionAttributeNames(Map.of("#pk", PK, "#v", VERSION))
                    .expressionAttributeValues(Map.of(":v", AttributeValue.fromN(Long.toString(record.version()))))
                    .build());
            return WriteResult.APPLIED;
        } catch (ConditionalCheckFailedException stale) {
            return WriteResult.SKIPPED_STALE;
        }
    }

    /** The stored shape of a record; shared with the authoritative-target write path. */
    static Map<String, AttributeValue> toItem(VersionedRecord record) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put(PK, AttributeValue.fromS(record.key()));
        item.put(VERSION, AttributeValue.fromN(Long.toString(record.version())));
        item.put(DELETED, AttributeValue.fromBool(record.deleted()));
        Map<String, AttributeValue> attrs = new HashMap<>();
        record.attributes().forEach((k, v) -> attrs.put(k, AttributeValue.fromS(v)));
        item.put(ATTRS, AttributeValue.fromM(attrs));
        return item;
    }

    /** Paginated scan projecting only the key, so it stays cheap on read capacity. */
    @Override
    public void forEachKey(java.util.function.Consumer<String> consumer) {
        client.scanPaginator(software.amazon.awssdk.services.dynamodb.model.ScanRequest.builder()
                        .tableName(table)
                        .projectionExpression("#pk")
                        .expressionAttributeNames(Map.of("#pk", PK))
                        .consistentRead(true)
                        .build())
                .items()
                .forEach(item -> consumer.accept(item.get(PK).s()));
    }

    @Override
    public Optional<VersionedRecord> read(String key) {
        var response = client.getItem(GetItemRequest.builder()
                .tableName(table)
                .key(Map.of(PK, AttributeValue.fromS(key)))
                .consistentRead(true)
                .build());
        if (!response.hasItem() || response.item().isEmpty()) {
            return Optional.empty();
        }
        Map<String, AttributeValue> item = response.item();
        Map<String, String> attrs = new HashMap<>();
        item.get(ATTRS).m().forEach((k, v) -> attrs.put(k, v.s()));
        return Optional.of(new VersionedRecord(
                item.get(PK).s(),
                Long.parseLong(item.get(VERSION).n()),
                item.get(DELETED).bool(),
                attrs));
    }
}
