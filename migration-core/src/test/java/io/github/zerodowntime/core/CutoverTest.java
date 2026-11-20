package io.github.zerodowntime.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

/** Module 5: gated cutover, authority switch, and a rollback that is actually exercised. */
class CutoverTest {

    private DataSource pg;
    private SourceRepository source;
    private OutboxStore outbox;
    private MigrationState state;
    private String dataTable;
    private DynamoTargetWriter dataWriter;
    private TargetStore targetStore;
    private OutboxRelay forwardRelay;
    private Verifier verifier;
    private MigratedStore router;
    private ShadowReader shadow;

    @BeforeEach
    void setUp() {
        pg = PostgresTestSupport.newSchema();
        source = new SourceRepository(pg);
        outbox = new OutboxStore(pg);
        state = new MigrationState(pg);
        dataTable = DynamoTestSupport.newTable();
        dataWriter = new DynamoTargetWriter(DynamoTestSupport.client(), dataTable);
        targetStore = new TargetStore(DynamoTestSupport.client(), dataTable, DynamoTestSupport.newTable());
        targetStore.initAuthority();
        forwardRelay = new OutboxRelay(outbox, dataWriter, 50);
        verifier = new Verifier(source, dataWriter, dataWriter, 10, 0, Duration.ZERO);
        router = new MigratedStore(source, targetStore, state);
        shadow = new ShadowReader(source, dataWriter, d -> {});
    }

    private CutoverController controller() {
        return controller(new ReverseRelay(targetStore, source));
    }

    private CutoverController controller(ReverseRelay reverse) {
        return new CutoverController(state, targetStore, forwardRelay, outbox, reverse, verifier, shadow);
    }

    /** Pre-existing data, backfilled, with live replication caught up. */
    private void migrateFiveRows() {
        for (int i = 1; i <= 5; i++) {
            router.upsert("k" + i, Map.of("n", String.valueOf(i)));
        }
        router.upsert("k2", Map.of("n", "2b"));
        router.delete("k5");
        new Backfiller(source, dataWriter, new CheckpointStore(pg), "job", 2).run();
        forwardRelay.drain();
    }

    // ------------------------------------------------------------------ the gate

    @Test
    void gateRejectsWhenTargetIsMissingData_andWritesResume() throws Exception {
        migrateFiveRows();
        // a row that bypassed the outbox and was never backfilled: the classic "forgot a table" bug
        try (Connection c = pg.getConnection();
                PreparedStatement ps = c.prepareStatement(
                        "INSERT INTO source_records (record_key, version, deleted, attrs) VALUES ('orphan', 1, false, '{}')")) {
            ps.executeUpdate();
        }

        assertThatThrownBy(() -> controller().cutover())
                .isInstanceOf(CutoverRejectedException.class)
                .satisfies(e -> {
                    var report = ((CutoverRejectedException) e).report();
                    assertThat(report.diffs()).extracting(Diff::key).containsExactly("orphan");
                    assertThat(report.diffs().get(0).type()).isEqualTo(DiffType.MISSING);
                });

        assertThat(state.current()).isEqualTo(Phase.DUAL_WRITE);
        assertThat(targetStore.authority()).isEqualTo(TargetStore.SOURCE);
        router.upsert("after", Map.of("n", "x")); // writes are accepted again
        assertThat(source.read("after")).isPresent();
    }

    @Test
    void gateRejectsOnShadowReadMismatch() {
        migrateFiveRows();
        VersionedRecord real = source.read("k1").orElseThrow();
        rawPut(new VersionedRecord("k1", real.version(), false, Map.of("n", "corrupt")));
        shadow.read("k1");                       // shadow reads see the corruption
        rawPut(real);                            // operator repairs the row before the gate runs
        assertThat(verifier.verify().isClean()).isTrue();

        assertThatThrownBy(() -> controller().cutover())
                .isInstanceOf(CutoverRejectedException.class)
                .hasMessageContaining("shadow-read mismatches");
        assertThat(state.current()).isEqualTo(Phase.DUAL_WRITE);
    }

    // ------------------------------------------------------------------ fences

    @Test
    void sourceWritesArePausedWhileCuttingOver_readsStillWork() {
        migrateFiveRows();
        state.transition(Phase.DUAL_WRITE, Phase.CUTTING_OVER);

        assertThatThrownBy(() -> router.upsert("k1", Map.of())).isInstanceOf(WritesPausedException.class);
        assertThatThrownBy(() -> router.delete("k1")).isInstanceOf(WritesPausedException.class);
        assertThat(router.read("k1")).isPresent();
    }

    @Test
    void phaseChangeWaitsForAnInFlightSourceWrite() throws Exception {
        migrateFiveRows();
        try (Connection inFlight = pg.getConnection()) {
            inFlight.setAutoCommit(false);
            state.writeGuard().check(inFlight); // a source write has started: holds the shared phase lock

            CompletableFuture<Boolean> flip =
                    CompletableFuture.supplyAsync(() -> state.transition(Phase.DUAL_WRITE, Phase.CUTTING_OVER));
            Thread.sleep(400);
            assertThat(flip).as("cutover must wait for the in-flight write").isNotDone();

            inFlight.commit();
            assertThat(flip.get(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void targetFenceRefusesWritesOnceClosed() {
        migrateFiveRows();
        controller().cutover();
        router.upsert("k1", Map.of("n", "while-open"));

        // a stale router (still believes phase = CUTOVER) calls the target after the fence closed
        assertThat(targetStore.casAuthority(TargetStore.TARGET, TargetStore.DRAINING)).isTrue();

        assertThatThrownBy(() -> targetStore.upsert("k1", Map.of("n", "too-late")))
                .isInstanceOf(WritesPausedException.class);
        assertThat(targetStore.read("k1").orElseThrow().attributes()).containsEntry("n", "while-open");
    }

    // ------------------------------------------------------------------ cutover

    @Test
    void cutoverMakesTargetAuthoritative() {
        migrateFiveRows();
        long before = source.read("k1").orElseThrow().version();

        controller().cutover();

        assertThat(state.current()).isEqualTo(Phase.CUTOVER);
        assertThat(targetStore.authority()).isEqualTo(TargetStore.TARGET);

        long v = router.upsert("k1", Map.of("n", "on-target"));
        assertThat(v).isEqualTo(before + 1);
        assertThat(router.read("k1").orElseThrow().attributes()).containsEntry("n", "on-target");
        assertThat(source.read("k1").orElseThrow().version()).as("source untouched until rollback").isEqualTo(before);
        assertThat(targetStore.pendingReverse()).extracting(VersionedRecord::key).containsExactly("k1");
        assertThat(outbox.pendingCount()).as("no forward events after cutover").isZero();
    }

    @Test
    void cutoverIsIdempotent() {
        migrateFiveRows();
        controller().cutover();
        controller().cutover();
        assertThat(state.current()).isEqualTo(Phase.CUTOVER);
    }

    @Test
    void interruptedCutoverResumes() {
        migrateFiveRows();
        state.transition(Phase.DUAL_WRITE, Phase.CUTTING_OVER); // crashed right after pausing writes

        controller().cutover();

        assertThat(state.current()).isEqualTo(Phase.CUTOVER);
    }

    // ------------------------------------------------------------------ rollback

    @Test
    void rollbackCopiesTargetWritesBackAndDualWriteResumes() {
        migrateFiveRows();
        controller().cutover();

        router.upsert("k1", Map.of("n", "t1"));
        router.upsert("k1", Map.of("n", "t2"));         // same key twice
        router.upsert("fresh", Map.of("n", "new"));     // brand new key, only ever on the target
        router.delete("k3");                            // tombstone made on the target
        router.upsert("k4", Map.of("n", "t4"));
        assertThat(targetStore.pendingReverse()).isNotEmpty();

        controller().rollback();

        assertThat(state.current()).isEqualTo(Phase.ROLLED_BACK);
        assertThat(targetStore.authority()).isEqualTo(TargetStore.SOURCE);
        assertThat(targetStore.pendingReverse()).isEmpty();
        for (String k : List.of("k1", "k2", "k3", "k4", "k5", "fresh")) {
            assertThat(source.read(k)).as(k).isEqualTo(targetStore.read(k));
        }
        assertThat(source.read("k1").orElseThrow().attributes()).containsEntry("n", "t2");
        assertThat(source.read("k3").orElseThrow().deleted()).isTrue();
        assertThat(source.read("fresh")).isPresent();
        assertThat(verifier.verify().isClean()).isTrue();

        // source is authoritative again and the forward path works, continuing the version line
        long v = router.upsert("k1", Map.of("n", "back-on-source"));
        assertThat(v).isEqualTo(targetStore.read("k1").orElseThrow().version() + 1);
        forwardRelay.drain();
        assertThat(verifier.verify().isClean()).isTrue();
    }

    @Test
    void rollbackInterruptedMidwayKeepsWritesPausedThenResumes() {
        migrateFiveRows();
        controller().cutover();
        router.upsert("k1", Map.of("n", "t1"));
        router.upsert("k2", Map.of("n", "t2"));

        var crashing = new ReverseRelay(targetStore, source) {
            @Override
            public Stats drain() {
                throw new IllegalStateException("simulated crash during reverse drain");
            }
        };
        assertThatThrownBy(() -> controller(crashing).rollback()).hasMessageContaining("simulated crash");

        assertThat(state.current()).isEqualTo(Phase.ROLLING_BACK);
        assertThatThrownBy(() -> router.upsert("k1", Map.of("n", "x"))).isInstanceOf(WritesPausedException.class);
        assertThat(router.read("k1").orElseThrow().attributes()).containsEntry("n", "t1"); // reads from target

        controller().rollback();

        assertThat(state.current()).isEqualTo(Phase.ROLLED_BACK);
        assertThat(source.read("k1").orElseThrow().attributes()).containsEntry("n", "t1");
        assertThat(source.read("k2").orElseThrow().attributes()).containsEntry("n", "t2");
        assertThat(verifier.verify().isClean()).isTrue();
    }

    @Test
    void migrationCanBeRetriedAfterRollback() {
        migrateFiveRows();
        controller().cutover();
        router.upsert("k1", Map.of("n", "t1"));
        controller().rollback();
        forwardRelay.drain();

        controller().cutover();

        assertThat(state.current()).isEqualTo(Phase.CUTOVER);
        assertThat(router.upsert("k1", Map.of("n", "second-round"))).isGreaterThan(1);
    }

    @Test
    void illegalTransitionsAreRefused() {
        assertThatThrownBy(() -> state.transition(Phase.DUAL_WRITE, Phase.CUTOVER)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> controller().rollback()).isInstanceOf(IllegalStateException.class);
    }

    // ------------------------------------------------------------------ short write pause

    /** State whose cutover transition first runs a hook: a change that lands after the pre-check. */
    private MigrationState stateWithHookBeforePause(Runnable hook, java.util.concurrent.atomic.AtomicInteger transitions) {
        return new MigrationState(pg) {
            @Override
            public boolean transition(Phase from, Phase to) {
                transitions.incrementAndGet();
                if (from == Phase.DUAL_WRITE && to == Phase.CUTTING_OVER) {
                    hook.run();
                }
                return super.transition(from, to);
            }
        };
    }

    private CutoverController liveCheckedController(MigrationState st) {
        var gate = new Verifier(source, dataWriter, dataWriter, 10, 0, Duration.ZERO);
        var live = new Verifier(source, dataWriter, dataWriter, 10, 2, Duration.ofMillis(1));
        return new CutoverController(
                st, targetStore, forwardRelay, outbox, new ReverseRelay(targetStore, source), gate, null, live);
    }

    @Test
    void preCheckRefusalNeverPausesWrites() throws Exception {
        migrateFiveRows();
        try (Connection c = pg.getConnection();
                PreparedStatement ps = c.prepareStatement(
                        "INSERT INTO source_records (record_key, version, deleted, attrs) VALUES ('orphan', 1, false, '{}')")) {
            ps.executeUpdate();
        }
        var transitions = new AtomicInteger();

        assertThatThrownBy(() -> liveCheckedController(stateWithHookBeforePause(() -> {}, transitions)).cutover())
                .isInstanceOf(CutoverRejectedException.class)
                .hasMessageContaining("writes were never paused");

        assertThat(transitions.get()).as("phase never left DUAL_WRITE, so no write was ever refused").isZero();
    }

    @Test
    void changeLandingAfterPreCheckIsDrainedAndRecheckedInsideThePause() {
        migrateFiveRows();
        var transitions = new AtomicInteger();
        var st = stateWithHookBeforePause(() -> source.upsert("k1", Map.of("n", "late")), transitions);

        liveCheckedController(st).cutover();

        assertThat(state.current()).isEqualTo(Phase.CUTOVER);
        assertThat(targetStore.read("k1").orElseThrow().attributes()).containsEntry("n", "late");
    }

    @Test
    void scopedCheckInsidePauseStillCatchesCorruptionOfAChangedKey() {
        migrateFiveRows();
        var transitions = new AtomicInteger();
        var st = stateWithHookBeforePause(() -> {
            long v = source.upsert("k1", Map.of("n", "late"));
            rawPut(VersionedRecord.live("k1", v + 3, Map.of("n", "from-the-future"))); // target ahead
        }, transitions);

        assertThatThrownBy(() -> liveCheckedController(st).cutover())
                .isInstanceOf(CutoverRejectedException.class)
                .satisfies(e -> assertThat(((CutoverRejectedException) e).report().diffs())
                        .extracting(Diff::type)
                        .containsExactly(DiffType.MISMATCHED));

        assertThat(state.current()).isEqualTo(Phase.DUAL_WRITE);
    }

    @Test
    void rollbackScopedCheckCatchesABrokenReverseSync() {
        migrateFiveRows();
        controller().cutover();
        router.upsert("k1", Map.of("n", "t1"));

        var sabotaged = new ReverseRelay(targetStore, source) {
            @Override
            public Stats drain() {
                var keys = new java.util.TreeSet<String>();
                for (VersionedRecord r : targetStore.pendingReverse()) {
                    // applies the wrong content at the right version, then acknowledges it
                    source.applyReplicated(new VersionedRecord(r.key(), r.version(), r.deleted(), Map.of("n", "WRONG")));
                    targetStore.ackReverse(r);
                    keys.add(r.key());
                }
                return new Stats(keys.size(), keys.size(), 0, keys);
            }
        };

        assertThatThrownBy(() -> controller(sabotaged).rollback())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("rollback blocked");
        assertThat(state.current()).isEqualTo(Phase.ROLLING_BACK); // stays fenced, does not hand back
    }

    // ------------------------------------------------------------------ the stress test

    /**
     * Writers hammer the router the whole time while the migration cuts over and rolls back. Each
     * key has exactly one writer, so after every acknowledged write the key's version must be
     * exactly one higher. If any write is lost, duplicated, applied to the wrong store, or
     * reordered across the switches, the final version or content of some key will be wrong.
     */
    @Test
    void noAcknowledgedWriteIsLostAcrossCutoverAndRollback() throws Exception {
        final int threads = 4;
        final int keysPerThread = 4;
        Map<String, long[]> acked = new ConcurrentHashMap<>(); // key -> {version, deleted(1/0)}
        for (int t = 0; t < threads; t++) {
            for (int k = 0; k < keysPerThread; k++) {
                String key = "w" + t + "-" + k;
                acked.put(key, new long[] {router.upsert(key, Map.of("n", "seed")), 0});
            }
        }
        new Backfiller(source, dataWriter, new CheckpointStore(pg), "job", 3).run();
        forwardRelay.drain();

        AtomicBoolean stop = new AtomicBoolean();
        AtomicInteger paused = new AtomicInteger();
        AtomicInteger written = new AtomicInteger();
        List<Thread> workers = new java.util.ArrayList<>();
        List<Throwable> failures = java.util.Collections.synchronizedList(new java.util.ArrayList<>());

        Thread relayThread = new Thread(() -> {
            while (!stop.get()) {
                try {
                    forwardRelay.drain();
                    Thread.sleep(3);
                } catch (Exception e) {
                    failures.add(e);
                    return;
                }
            }
        });
        relayThread.start();

        for (int t = 0; t < threads; t++) {
            final int id = t;
            Thread w = new Thread(() -> {
                int i = 0;
                while (!stop.get()) {
                    String key = "w" + id + "-" + (i % keysPerThread);
                    try {
                        long[] last = acked.get(key);
                        if (i % 5 == 4) {
                            var v = router.delete(key);
                            if (v.isPresent()) {
                                acked.put(key, new long[] {v.get(), 1});
                            }
                        } else {
                            acked.put(key, new long[] {router.upsert(key, Map.of("n", "w" + i)), 0});
                        }
                        i++;
                        written.incrementAndGet();
                        assertThat(acked.get(key)[0]).isGreaterThanOrEqualTo(last[0]);
                    } catch (WritesPausedException e) {
                        paused.incrementAndGet();
                        sleep(2);
                    } catch (Throwable e) {
                        failures.add(e);
                        return;
                    }
                }
            });
            workers.add(w);
            w.start();
        }

        var ctl = controller();
        sleep(400);
        ctl.cutover();
        assertThat(state.current()).isEqualTo(Phase.CUTOVER);
        sleep(400);
        assertThat(targetStore.pendingReverse()).as("writes really landed on the target").isNotEmpty();
        ctl.rollback();
        assertThat(state.current()).isEqualTo(Phase.ROLLED_BACK);
        sleep(300);

        stop.set(true);
        for (Thread w : workers) {
            w.join(10_000);
        }
        relayThread.join(10_000);
        assertThat(failures).isEmpty();
        forwardRelay.drain();

        assertThat(written.get()).isGreaterThan(50);
        Map<String, String> problems = new HashMap<>();
        acked.forEach((key, last) -> {
            VersionedRecord r = source.read(key).orElseThrow();
            if (r.version() != last[0] || r.deleted() != (last[1] == 1)) {
                problems.put(key, "source=" + r.version() + "/" + r.deleted() + " acked=" + last[0] + "/" + last[1]);
            }
        });
        assertThat(problems).as("per-key last acknowledged write must be what the source holds").isEmpty();
        var report = verifier.verify();
        assertThat(report.isClean()).as(report.summary()).isTrue();
    }

    // ------------------------------------------------------------------ helpers

    private void rawPut(VersionedRecord r) {
        Map<String, AttributeValue> attrs = new HashMap<>();
        r.attributes().forEach((k, v) -> attrs.put(k, AttributeValue.fromS(v)));
        DynamoTestSupport.client().putItem(PutItemRequest.builder()
                .tableName(dataTable)
                .item(Map.of(
                        "pk", AttributeValue.fromS(r.key()),
                        "version", AttributeValue.fromN(Long.toString(r.version())),
                        "deleted", AttributeValue.fromBool(r.deleted()),
                        "attrs", AttributeValue.fromM(attrs)))
                .build());
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
