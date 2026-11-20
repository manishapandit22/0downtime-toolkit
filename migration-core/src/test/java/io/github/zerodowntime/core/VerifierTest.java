package io.github.zerodowntime.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

/** Module 4: verifier diff report and shadow reads. */
class VerifierTest {

    private DataSource pg;
    private SourceRepository source;
    private OutboxStore outbox;
    private DynamoTargetWriter target;
    private String table;

    @BeforeEach
    void setUp() {
        pg = PostgresTestSupport.newSchema();
        source = new SourceRepository(pg);
        outbox = new OutboxStore(pg);
        table = DynamoTestSupport.newTable();
        target = new DynamoTargetWriter(DynamoTestSupport.client(), table);
    }

    private Verifier verifier(TargetWriter reader, int rounds, Runnable onSettlePause) {
        return new Verifier(source, reader, target, 4, rounds, Duration.ofMillis(1), d -> onSettlePause.run());
    }

    /** 12 rows: some updated, some deleted; fully migrated via backfill. */
    private void seedAndBackfill() {
        for (int i = 1; i <= 12; i++) {
            String k = String.format("k%02d", i);
            source.upsert(k, Map.of("n", String.valueOf(i)));
            if (i % 3 == 0) {
                source.upsert(k, Map.of("n", i + "-updated"));
            }
            if (i % 5 == 0) {
                source.delete(k);
            }
        }
        new Backfiller(source, target, new CheckpointStore(pg), "job", 4).run();
    }

    @Test
    void identicalStoresProduceCleanReport() {
        seedAndBackfill();

        var report = verifier(target, 2, () -> {}).verify();

        assertThat(report.isClean()).as(report.summary()).isTrue();
        assertThat(report.sourceRows()).isEqualTo(12);
        assertThat(report.transientDiffs()).isZero();
        assertThat(report.settleRounds()).isZero(); // nothing to settle
    }

    @Test
    void classifiesMissingStaleExtraAndMismatched() {
        seedAndBackfill();
        // MISSING: row exists on the source only
        source.upsert("new1", Map.of("n", "x"));
        // STALE: source moved on, target not told (outbox never drained)
        long v = source.upsert("k01", Map.of("n", "changed"));
        // EXTRA: target has a key the source never had
        target.write(VersionedRecord.live("ghost", 1, Map.of("n", "boo")));
        // MISMATCHED: same version, different content
        VersionedRecord k02 = source.read("k02").orElseThrow();
        forceOverwrite(new VersionedRecord("k02", k02.version(), false, Map.of("n", "evil")));
        // MISMATCHED: target AHEAD of the source
        VersionedRecord k04 = source.read("k04").orElseThrow();
        target.write(VersionedRecord.live("k04", k04.version() + 5, Map.of("n", "future")));

        var report = verifier(target, 2, () -> {}).verify();

        assertThat(report.diffs()).extracting(Diff::key).containsExactlyInAnyOrder("new1", "k01", "ghost", "k02", "k04");
        assertThat(report.diffs()).anySatisfy(d -> {
            assertThat(d.key()).isEqualTo("new1");
            assertThat(d.type()).isEqualTo(DiffType.MISSING);
        });
        assertThat(report.diffs()).anySatisfy(d -> {
            assertThat(d.key()).isEqualTo("k01");
            assertThat(d.type()).isEqualTo(DiffType.STALE);
            assertThat(d.sourceVersion()).isEqualTo(v);
        });
        assertThat(report.diffs()).anySatisfy(d -> {
            assertThat(d.key()).isEqualTo("ghost");
            assertThat(d.type()).isEqualTo(DiffType.EXTRA);
        });
        assertThat(report.countsByType().get(DiffType.MISMATCHED)).isEqualTo(2);
        assertThat(report.isClean()).isFalse();
    }

    /** Bypasses the conditional write to plant corrupt data (same version, different content). */
    private void forceOverwrite(VersionedRecord r) {
        Map<String, AttributeValue> attrs = new HashMap<>();
        r.attributes().forEach((k, v) -> attrs.put(k, AttributeValue.fromS(v)));
        DynamoTestSupport.client().putItem(PutItemRequest.builder()
                .tableName(table)
                .item(Map.of(
                        "pk", AttributeValue.fromS(r.key()),
                        "version", AttributeValue.fromN(Long.toString(r.version())),
                        "deleted", AttributeValue.fromBool(r.deleted()),
                        "attrs", AttributeValue.fromM(attrs)))
                .build());
    }

    @Test
    void lagThatClearsDuringSettleIsTransientNotReported() {
        seedAndBackfill();
        source.upsert("k01", Map.of("n", "live-1"));  // pending in outbox: target lags
        source.upsert("late", Map.of("n", "live-2")); // missing on target until relay runs
        var relay = new OutboxRelay(outbox, target, 100);

        // the "settle pause" is when the relay catches up
        var report = verifier(target, 3, relay::drain).verify();

        assertThat(report.isClean()).as(report.summary()).isTrue();
        assertThat(report.transientDiffs()).isGreaterThanOrEqualTo(2);
        assertThat(report.settleRounds()).isEqualTo(1);
    }

    @Test
    void lagThatNeverClearsIsReportedAfterAllRounds() {
        seedAndBackfill();
        source.upsert("k01", Map.of("n", "stuck")); // relay never runs

        var rounds = new AtomicInteger();
        var report = verifier(target, 3, rounds::incrementAndGet).verify();

        assertThat(rounds.get()).isEqualTo(3);
        assertThat(report.settleRounds()).isEqualTo(3);
        assertThat(report.diffs()).extracting(Diff::type).containsExactly(DiffType.STALE);
    }

    @Test
    void writeLandingBetweenSourceReadAndTargetReadIsNotAFalsePositive() {
        seedAndBackfill();
        var relay = new OutboxRelay(outbox, target, 100);
        relay.drain();
        var fired = new AtomicInteger();
        // after the verifier has read k03 from the source, a live write updates it on both sides
        TargetWriter racing = wrap(target, key -> {
            if (key.equals("k03") && fired.getAndIncrement() == 0) {
                source.upsert("k03", Map.of("n", "raced"));
                relay.drain();
            }
        });

        var report = verifier(racing, 2, () -> {}).verify();

        assertThat(fired.get()).isGreaterThan(0);
        assertThat(report.isClean()).as(report.summary()).isTrue();
        assertThat(report.transientDiffs()).isEqualTo(1); // target was ahead of the stale source snapshot
    }

    @Test
    void shadowReaderServesSourceAndCountsDifferences() {
        seedAndBackfill();
        source.upsert("k01", Map.of("n", "lagging")); // target stale
        var seen = new java.util.ArrayList<Diff>();
        var shadow = new ShadowReader(source, target, seen::add);

        var ok = shadow.read("k02");
        var lagging = shadow.read("k01");
        var absent = shadow.read("nope");

        assertThat(lagging).isPresent();
        assertThat(lagging.get().attributes()).containsEntry("n", "lagging"); // source wins
        assertThat(ok).isPresent();
        assertThat(absent).isEmpty();
        assertThat(shadow.reads()).isEqualTo(3);
        assertThat(shadow.matches()).isEqualTo(2); // k02 and the nonexistent key agree
        assertThat(shadow.diffs()).isEqualTo(1);
        assertThat(shadow.mismatched()).isZero(); // lag is not corruption
        assertThat(seen).extracting(Diff::type).containsExactly(DiffType.STALE);
    }

    @Test
    void shadowReaderSurvivesTargetFailureAndBrokenListener() {
        seedAndBackfill();
        TargetWriter broken = new TargetWriter() {
            @Override
            public WriteResult write(VersionedRecord r) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Optional<VersionedRecord> read(String key) {
                throw new IllegalStateException("dynamo down");
            }
        };
        var shadow = new ShadowReader(source, broken, d -> {
            throw new IllegalStateException("listener bug");
        });

        assertThat(shadow.read("k01")).isPresent();
        assertThat(shadow.targetErrors()).isEqualTo(1);

        var shadow2 = new ShadowReader(source, target, d -> {
            throw new IllegalStateException("listener bug");
        });
        source.upsert("k01", Map.of("n", "changed"));
        assertThat(shadow2.read("k01")).isPresent(); // listener threw, read still fine
        assertThat(shadow2.diffs()).isEqualTo(1);
    }

    @Test
    void shadowReaderFlagsRealCorruptionAsMismatched() {
        seedAndBackfill();
        VersionedRecord real = source.read("k01").orElseThrow();
        forceOverwrite(new VersionedRecord("k01", real.version(), false, Map.of("n", "evil")));
        var shadow = new ShadowReader(source, target, d -> {});

        shadow.read("k01");

        assertThat(shadow.mismatched()).isEqualTo(1);
    }

    private static TargetWriter wrap(TargetWriter delegate, Consumer<String> beforeRead) {
        return new TargetWriter() {
            @Override
            public WriteResult write(VersionedRecord record) {
                return delegate.write(record);
            }

            @Override
            public Optional<VersionedRecord> read(String key) {
                beforeRead.accept(key);
                return delegate.read(key);
            }
        };
    }
}
