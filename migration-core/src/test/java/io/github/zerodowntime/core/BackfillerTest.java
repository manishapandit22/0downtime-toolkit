package io.github.zerodowntime.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Module 3: chunked, checkpointed, resumable backfill. */
class BackfillerTest {

    private static final String JOB = "users-v1";
    private static final int ROWS = 25;
    private static final int CHUNK = 4;

    private DataSource pg;
    private SourceRepository source;
    private OutboxStore outbox;
    private CheckpointStore checkpoints;
    private TargetWriter target;

    @BeforeEach
    void setUp() {
        pg = PostgresTestSupport.newSchema();
        source = new SourceRepository(pg);
        outbox = new OutboxStore(pg);
        checkpoints = new CheckpointStore(pg);
        target = new DynamoTargetWriter(DynamoTestSupport.client(), DynamoTestSupport.newTable());
    }

    private static String key(int i) {
        return String.format("k%02d", i);
    }

    /** Pre-existing data: ROWS rows, every 5th one updated, every 7th one deleted. */
    private void seed() {
        for (int i = 1; i <= ROWS; i++) {
            source.upsert(key(i), Map.of("n", String.valueOf(i)));
            if (i % 5 == 0) {
                source.upsert(key(i), Map.of("n", i + "-updated"));
            }
            if (i % 7 == 0) {
                source.delete(key(i));
            }
        }
    }

    /** Every source row, including tombstones, must equal the target row. */
    private void assertTargetMatchesSource() {
        List<VersionedRecord> all = source.readChunk(null, 10_000);
        for (VersionedRecord r : all) {
            assertThat(target.read(r.key())).as(r.key()).contains(r);
        }
    }

    private Backfiller backfiller(TargetWriter writer, CheckpointStore store) {
        return new Backfiller(source, writer, store, JOB, CHUNK);
    }

    @Test
    void copiesEveryRowIncludingTombstones() {
        seed();

        var stats = backfiller(target, checkpoints).run();

        assertThat(stats.rows()).isEqualTo(ROWS);
        assertThat(stats.applied()).isEqualTo(ROWS);
        assertThat(stats.chunks()).isEqualTo(7); // ceil(25 / 4)
        assertTargetMatchesSource();
        assertThat(target.read(key(7)).orElseThrow().deleted()).isTrue();
    }

    @Test
    void killedMidRunResumesFromCheckpointWithoutRedoingFinishedChunks() {
        seed();
        var writes = new AtomicInteger();
        TargetWriter dies = wrap(target, r -> {
            if (writes.incrementAndGet() == 10) { // inside chunk 3 (rows 9-12)
                throw new IllegalStateException("simulated kill");
            }
        });

        assertThatThrownBy(() -> backfiller(dies, checkpoints).run()).hasMessageContaining("simulated kill");
        assertThat(checkpoints.load(JOB)).contains(key(8)); // chunks 1 and 2 are done

        var resumed = backfiller(target, checkpoints).run();

        assertThat(resumed.rows()).isEqualTo(ROWS - 8); // started at row 9, not row 1
        assertTargetMatchesSource();
    }

    @Test
    void crashBetweenTargetWriteAndCheckpointRewritesTheChunkHarmlessly() {
        seed();
        var crashingCheckpoints = new CheckpointStore(pg) {
            private boolean crashed;

            @Override
            public void save(String jobName, String lastKey) {
                if (!crashed) {
                    crashed = true;
                    throw new IllegalStateException("simulated crash before checkpoint");
                }
                super.save(jobName, lastKey);
            }
        };

        assertThatThrownBy(() -> backfiller(target, crashingCheckpoints).run()).hasMessageContaining("simulated crash");
        assertThat(checkpoints.load(JOB)).isEmpty(); // chunk 1 is on the target but not checkpointed

        var resumed = backfiller(target, crashingCheckpoints).run();

        assertThat(resumed.rows()).isEqualTo(ROWS);          // had to redo from the start
        assertThat(resumed.skippedStale()).isEqualTo(CHUNK); // chunk 1 again: all harmless repeats
        assertThat(resumed.applied()).isEqualTo(ROWS - CHUNK);
        assertTargetMatchesSource();
    }

    @Test
    void rerunAfterCompletionDoesNothing() {
        seed();
        backfiller(target, checkpoints).run();

        var again = backfiller(target, checkpoints).run();

        assertThat(again.rows()).isZero();
        assertTargetMatchesSource();
    }

    /**
     * The headline test. While the backfill runs, the "application" keeps updating, deleting and
     * inserting rows, and the relay delivers those changes to the target BEFORE the backfill
     * writes its older snapshot of the same row. The final state must still match the source.
     */
    @Test
    void backfillRacingLiveWritesStillConverges() {
        seed();
        var relay = new OutboxRelay(outbox, target, 50);
        relay.drain(); // clear the outbox events created by seed(); backfill covers that data
        var liveEvents = new AtomicInteger();
        var writes = new AtomicInteger();

        TargetWriter racing = wrap(target, r -> {
            int n = writes.incrementAndGet();
            if (n == 1) {
                // a brand new row that sorts BEFORE the cursor: only the relay can ever deliver it
                source.upsert("0_early", Map.of("n", "early"));
                relay.drain();
                liveEvents.incrementAndGet();
            }
            if (n % 4 == 0) {
                // live change to the very row the backfill is about to write (its snapshot is now stale)
                if (n % 8 == 0) {
                    source.delete(r.key());
                } else {
                    source.upsert(r.key(), Map.of("n", "live-" + n));
                }
                relay.drain();
                liveEvents.incrementAndGet();
            }
        });

        var stats = backfiller(racing, checkpoints).run();
        relay.drain();

        assertThat(liveEvents.get()).isGreaterThan(5);
        assertThat(stats.skippedStale()).as("stale backfill snapshots were rejected").isGreaterThan(5);
        assertThat(target.read("0_early")).isPresent();
        assertTargetMatchesSource();
        assertThat(outbox.pendingCount()).isZero();
    }

    /** Wraps a writer with a hook that runs just BEFORE each write is delegated. */
    private static TargetWriter wrap(TargetWriter delegate, Consumer<VersionedRecord> beforeWrite) {
        return new TargetWriter() {
            @Override
            public WriteResult write(VersionedRecord record) {
                beforeWrite.accept(record);
                return delegate.write(record);
            }

            @Override
            public Optional<VersionedRecord> read(String key) {
                return delegate.read(key);
            }
        };
    }
}
