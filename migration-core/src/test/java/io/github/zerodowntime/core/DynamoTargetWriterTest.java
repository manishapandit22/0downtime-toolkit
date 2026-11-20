package io.github.zerodowntime.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DynamoTargetWriterTest {

    private TargetWriter writer;

    @BeforeEach
    void setUp() {
        writer = new DynamoTargetWriter(DynamoTestSupport.client(), DynamoTestSupport.newTable());
    }

    @Test
    void firstWriteIsApplied() {
        var r = VersionedRecord.live("u1", 1, Map.of("name", "Asha"));

        assertThat(writer.write(r)).isEqualTo(WriteResult.APPLIED);
        assertThat(writer.read("u1")).contains(r);
    }

    @Test
    void newerVersionReplacesOlder() {
        writer.write(VersionedRecord.live("u1", 1, Map.of("name", "Asha")));

        var result = writer.write(VersionedRecord.live("u1", 2, Map.of("name", "Asha K")));

        assertThat(result).isEqualTo(WriteResult.APPLIED);
        assertThat(writer.read("u1").orElseThrow().attributes()).containsEntry("name", "Asha K");
    }

    @Test
    void olderVersionIsRejected() {
        writer.write(VersionedRecord.live("u1", 5, Map.of("name", "new")));

        var result = writer.write(VersionedRecord.live("u1", 3, Map.of("name", "old")));

        assertThat(result).isEqualTo(WriteResult.SKIPPED_STALE);
        assertThat(writer.read("u1").orElseThrow().version()).isEqualTo(5);
    }

    @Test
    void sameVersionIsIdempotentNoOp() {
        var r = VersionedRecord.live("u1", 2, Map.of("name", "Asha"));
        writer.write(r);

        assertThat(writer.write(r)).isEqualTo(WriteResult.SKIPPED_STALE);
        assertThat(writer.read("u1")).contains(r);
    }

    @Test
    void tombstoneBlocksStaleResurrection() {
        writer.write(VersionedRecord.live("u1", 1, Map.of("name", "Asha")));
        writer.write(VersionedRecord.tombstone("u1", 2));

        var stale = writer.write(VersionedRecord.live("u1", 1, Map.of("name", "Asha")));

        assertThat(stale).isEqualTo(WriteResult.SKIPPED_STALE);
        assertThat(writer.read("u1").orElseThrow().deleted()).isTrue();
    }

    @Test
    void readMissingKeyIsEmpty() {
        assertThat(writer.read("nope")).isEmpty();
    }
}
