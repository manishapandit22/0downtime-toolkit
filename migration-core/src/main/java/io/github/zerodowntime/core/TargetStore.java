package io.github.zerodowntime.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionCheck;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

/**
 * The target while it is AUTHORITATIVE (after cutover). This is the mirror image of the forward
 * path: the application writes here, and the reverse log is the outbox that lets rollback copy
 * those writes back to Postgres.
 *
 * <p>Each write is ONE DynamoDB transaction of three items:
 * <ol>
 *   <li>the data row, put with {@code attribute_not_exists OR version = :seen} (optimistic
 *       compare-and-set: the new version is {@code seen + 1}, assigned by the target now that the
 *       source no longer is);
 *   <li>a reverse-log entry for that exact (key, version);
 *   <li>a condition check that the control item says {@code authority = TARGET}. This is the fence:
 *       rollback flips it to DRAINING, after which no write can commit.
 * </ol>
 * Row, log entry and fence either all commit or none do, so there is no target change without a
 * log entry and no write after the fence closed.
 *
 * <p>The control item and the reverse log live in a second table so the data table stays a pure
 * mirror of the rows (the verifier scans it).
 */
public class TargetStore {

    public static final String SOURCE = "SOURCE";
    public static final String TARGET = "TARGET";
    public static final String DRAINING = "DRAINING";

    static final String CONTROL_PK = "__authority";
    static final String LOG_PREFIX = "r#";
    private static final String STATE = "state";
    private static final String RKEY = "rkey";
    private static final int MAX_CAS_ATTEMPTS = 50;

    private final DynamoDbClient client;
    private final String dataTable;
    private final String controlTable;
    private final DynamoTargetWriter reader;

    public TargetStore(DynamoDbClient client, String dataTable, String controlTable) {
        this.client = client;
        this.dataTable = dataTable;
        this.controlTable = controlTable;
        this.reader = new DynamoTargetWriter(client, dataTable);
    }

    // ---------------------------------------------------------------- fence / authority

    /** Creates the control item with authority SOURCE if it does not exist yet. */
    public void initAuthority() {
        try {
            client.putItem(PutItemRequest.builder()
                    .tableName(controlTable)
                    .item(Map.of("pk", AttributeValue.fromS(CONTROL_PK), STATE, AttributeValue.fromS(SOURCE)))
                    .conditionExpression("attribute_not_exists(pk)")
                    .build());
        } catch (ConditionalCheckFailedException exists) {
            // already initialised
        }
    }

    public String authority() {
        var r = client.getItem(GetItemRequest.builder()
                .tableName(controlTable)
                .key(Map.of("pk", AttributeValue.fromS(CONTROL_PK)))
                .consistentRead(true)
                .build());
        if (!r.hasItem() || r.item().isEmpty()) {
            throw new IllegalStateException("authority not initialised; call initAuthority()");
        }
        return r.item().get(STATE).s();
    }

    /** Atomically moves the authority flag {@code from} to {@code to}; false if it was not {@code from}. */
    public boolean casAuthority(String from, String to) {
        try {
            client.updateItem(UpdateItemRequest.builder()
                    .tableName(controlTable)
                    .key(Map.of("pk", AttributeValue.fromS(CONTROL_PK)))
                    .updateExpression("SET #s = :to")
                    .conditionExpression("#s = :from")
                    .expressionAttributeNames(Map.of("#s", STATE))
                    .expressionAttributeValues(
                            Map.of(":to", AttributeValue.fromS(to), ":from", AttributeValue.fromS(from)))
                    .build());
            return true;
        } catch (ConditionalCheckFailedException other) {
            return false;
        }
    }

    // ---------------------------------------------------------------- authoritative writes

    /** Creates or updates a row on the target. Returns the new version. */
    public long upsert(String key, Map<String, String> attributes) {
        return write(key, false, attributes).orElseThrow();
    }

    /** Tombstones a row. Empty if it does not exist or is already deleted. */
    public Optional<Long> delete(String key) {
        return write(key, true, Map.of());
    }

    public Optional<VersionedRecord> read(String key) {
        return reader.read(key);
    }

    private Optional<Long> write(String key, boolean delete, Map<String, String> attributes) {
        for (int attempt = 0; attempt < MAX_CAS_ATTEMPTS; attempt++) {
            Optional<VersionedRecord> current = reader.read(key);
            if (delete && (current.isEmpty() || current.get().deleted())) {
                return Optional.empty();
            }
            long seen = current.map(VersionedRecord::version).orElse(0L);
            VersionedRecord next = delete
                    ? VersionedRecord.tombstone(key, seen + 1)
                    : VersionedRecord.live(key, seen + 1, attributes);
            switch (tryCommit(next, current.isPresent() ? seen : null)) {
                case COMMITTED -> {
                    return Optional.of(next.version());
                }
                case CONFLICT -> {
                    // Another writer changed the row, or a concurrent transaction touched the shared
                    // fence item. Back off briefly (with jitter) and retry from a fresh read.
                    backoff(attempt);
                }
                case FENCED -> throw new WritesPausedException("target is not authoritative (authority != TARGET)");
            }
        }
        throw new IllegalStateException("gave up after " + MAX_CAS_ATTEMPTS + " conflicting attempts on " + key);
    }

    private static void backoff(int attempt) {
        try {
            Thread.sleep(1 + java.util.concurrent.ThreadLocalRandom.current().nextInt(Math.min(attempt + 2, 10)));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while retrying write", e);
        }
    }

    private enum Outcome { COMMITTED, CONFLICT, FENCED }

    private Outcome tryCommit(VersionedRecord next, Long seenVersion) {
        Put.Builder data = Put.builder().tableName(dataTable).item(DynamoTargetWriter.toItem(next));
        if (seenVersion == null) {
            data.conditionExpression("attribute_not_exists(#pk)")
                    .expressionAttributeNames(Map.of("#pk", DynamoTargetWriter.PK));
        } else {
            data.conditionExpression("#v = :seen")
                    .expressionAttributeNames(Map.of("#v", DynamoTargetWriter.VERSION))
                    .expressionAttributeValues(Map.of(":seen", AttributeValue.fromN(Long.toString(seenVersion))));
        }

        try {
            client.transactWriteItems(TransactWriteItemsRequest.builder()
                    .transactItems(
                            TransactWriteItem.builder().put(data.build()).build(),
                            TransactWriteItem.builder()
                                    .put(Put.builder()
                                            .tableName(controlTable)
                                            .item(logItem(next))
                                            .build())
                                    .build(),
                            TransactWriteItem.builder()
                                    .conditionCheck(ConditionCheck.builder()
                                            .tableName(controlTable)
                                            .key(Map.of("pk", AttributeValue.fromS(CONTROL_PK)))
                                            .conditionExpression("#s = :t")
                                            .expressionAttributeNames(Map.of("#s", STATE))
                                            .expressionAttributeValues(Map.of(":t", AttributeValue.fromS(TARGET)))
                                            .build())
                                    .build())
                    .build());
            return Outcome.COMMITTED;
        } catch (TransactionCanceledException cancelled) {
            var reasons = cancelled.cancellationReasons();
            if (reasons != null && reasons.size() == 3) {
                if ("ConditionalCheckFailed".equals(reasons.get(2).code())) {
                    return Outcome.FENCED; // check the fence first: it wins over a CAS conflict
                }
                if ("ConditionalCheckFailed".equals(reasons.get(0).code())) {
                    return Outcome.CONFLICT;
                }
            }
            // TransactionConflict (a concurrent transaction on the same item) and the like: retry
            boolean transientConflict = reasons != null
                    && reasons.stream().anyMatch(r -> "TransactionConflict".equals(r.code()));
            if (transientConflict) {
                return Outcome.CONFLICT;
            }
            throw cancelled;
        }
    }

    // ---------------------------------------------------------------- reverse log

    private static Map<String, AttributeValue> logItem(VersionedRecord r) {
        Map<String, AttributeValue> item = new HashMap<>(DynamoTargetWriter.toItem(r));
        item.put("pk", AttributeValue.fromS(logId(r)));
        item.put(RKEY, AttributeValue.fromS(r.key()));
        return item;
    }

    private static String logId(VersionedRecord r) {
        return LOG_PREFIX + r.key() + "#" + r.version();
    }

    /** Reverse-log entries not yet applied to the source. Order is irrelevant: apply is monotonic. */
    public List<VersionedRecord> pendingReverse() {
        List<VersionedRecord> out = new ArrayList<>();
        client.scanPaginator(ScanRequest.builder()
                        .tableName(controlTable)
                        .filterExpression("begins_with(#pk, :p)")
                        .expressionAttributeNames(Map.of("#pk", "pk"))
                        .expressionAttributeValues(Map.of(":p", AttributeValue.fromS(LOG_PREFIX)))
                        .consistentRead(true)
                        .build())
                .items()
                .forEach(item -> {
                    Map<String, String> attrs = new HashMap<>();
                    item.get(DynamoTargetWriter.ATTRS).m().forEach((k, v) -> attrs.put(k, v.s()));
                    out.add(new VersionedRecord(
                            item.get(RKEY).s(),
                            Long.parseLong(item.get(DynamoTargetWriter.VERSION).n()),
                            item.get(DynamoTargetWriter.DELETED).bool(),
                            attrs));
                });
        return out;
    }

    /** Removes an applied log entry. Idempotent. */
    public void ackReverse(VersionedRecord r) {
        client.deleteItem(DeleteItemRequest.builder()
                .tableName(controlTable)
                .key(Map.of("pk", AttributeValue.fromS(logId(r))))
                .build());
    }
}
