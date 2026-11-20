package io.github.zerodowntime.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

/**
 * Module 1b: reproduces the central race of a live migration and shows the fix.
 *
 * <p>Interleaving (forced deterministically, no sleeps):
 * <ol>
 *   <li>Backfill reads the row from the source (a snapshot at version 5).
 *   <li>A live update (version 6) is applied to source and target.
 *   <li>Backfill writes its stale snapshot to the target.
 * </ol>
 */
class BackfillRaceTest {

    private static final String KEY = "u1";

    private TargetWriter newWriter(Function<String, TargetWriter> factory) {
        return factory.apply(DynamoTestSupport.newTable());
    }

    private TargetWriter naive(String table) {
        return new NaiveTargetWriter(DynamoTestSupport.client(), table);
    }

    private TargetWriter fixed(String table) {
        return new DynamoTargetWriter(DynamoTestSupport.client(), table);
    }

    /** Runs the race and returns what the target holds at the end. */
    private VersionedRecord runUpdateRace(TargetWriter target) {
        var snapshotRead = VersionedRecord.live(KEY, 5, Map.of("name", "old"));   // 1. backfill reads
        var liveWrite = VersionedRecord.live(KEY, 6, Map.of("name", "new"));      // 2. live write lands
        target.write(liveWrite);
        target.write(snapshotRead);                                               // 3. stale backfill write
        return target.read(KEY).orElseThrow();
    }

    private VersionedRecord runDeleteRace(TargetWriter target) {
        var snapshotRead = VersionedRecord.live(KEY, 5, Map.of("name", "old"));
        var liveDelete = VersionedRecord.tombstone(KEY, 6);
        target.write(liveDelete);
        target.write(snapshotRead);
        return target.read(KEY).orElseThrow();
    }

    @Test
    void naiveWriterLosesTheLiveUpdate() {
        var result = runUpdateRace(newWriter(this::naive));

        // The bug, reproduced: the target went BACK to version 5.
        assertThat(result.version()).isEqualTo(5);
        assertThat(result.attributes()).containsEntry("name", "old");
    }

    @Test
    void naiveWriterResurrectsADeletedRow() {
        var result = runDeleteRace(newWriter(this::naive));

        // The bug, reproduced: a deleted row is alive again.
        assertThat(result.deleted()).isFalse();
    }

    @Test
    void fixedWriterKeepsTheLiveUpdate() {
        var result = runUpdateRace(newWriter(this::fixed));

        assertThat(result.version()).isEqualTo(6);
        assertThat(result.attributes()).containsEntry("name", "new");
    }

    @Test
    void fixedWriterKeepsTheDelete() {
        var result = runDeleteRace(newWriter(this::fixed));

        assertThat(result.version()).isEqualTo(6);
        assertThat(result.deleted()).isTrue();
    }
}
