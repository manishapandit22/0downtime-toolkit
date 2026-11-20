package io.github.zerodowntime.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Module 2: transactional outbox + relay, including crash and duplicate-delivery cases. */
class OutboxRelayTest {

    private DataSource pg;
    private SourceRepository source;
    private OutboxStore outbox;
    private TargetWriter target;

    @BeforeEach
    void setUp() {
        pg = PostgresTestSupport.newSchema();
        source = new SourceRepository(pg);
        outbox = new OutboxStore(pg);
        target = new DynamoTargetWriter(DynamoTestSupport.client(), DynamoTestSupport.newTable());
    }

    // ---- atomicity of source write + outbox row -------------------------------------------

    @Test
    void sourceChangeAndOutboxRowCommitTogether() {
        long v = source.upsert("u1", Map.of("name", "Asha"));

        assertThat(v).isEqualTo(1);
        assertThat(source.read("u1")).isPresent();
        assertThat(outbox.pendingCount()).isEqualTo(1);
    }

    @Test
    void ifOutboxInsertFailsTheSourceChangeRollsBack() {
        // Make the outbox reject one key, so the second half of the transaction fails.
        exec("ALTER TABLE outbox ADD CONSTRAINT no_poison CHECK (record_key <> 'poison')");

        assertThatThrownBy(() -> source.upsert("poison", Map.of("a", "b"))).isInstanceOf(RuntimeException.class);

        assertThat(source.read("poison")).isEmpty();
        assertThat(outbox.pendingCount()).isZero();
    }

    @Test
    void versionsIncreaseOnEveryChangeIncludingDelete() {
        assertThat(source.upsert("u1", Map.of("n", "1"))).isEqualTo(1);
        assertThat(source.upsert("u1", Map.of("n", "2"))).isEqualTo(2);
        assertThat(source.delete("u1")).contains(3L);
        assertThat(source.delete("u1")).isEmpty(); // already a tombstone, no new event
        assertThat(source.upsert("u1", Map.of("n", "3"))).isEqualTo(4); // re-create keeps counting up

        assertThat(outbox.pendingCount()).isEqualTo(4);
    }

    // ---- relay happy path ---------------------------------------------------------------------

    @Test
    void relayReplicatesUpdatesAndDeletesToTarget() {
        source.upsert("u1", Map.of("name", "Asha"));
        source.upsert("u1", Map.of("name", "Asha K"));
        source.upsert("u2", Map.of("name", "Ravi"));
        source.upsert("u3", Map.of("name", "Meera"));
        source.delete("u3");

        var stats = new OutboxRelay(outbox, target, 2).drain();

        assertThat(stats.delivered()).isEqualTo(5);
        assertThat(outbox.pendingCount()).isZero();
        for (String key : List.of("u1", "u2", "u3")) {
            assertThat(target.read(key)).as(key).isEqualTo(source.read(key));
        }
        assertThat(target.read("u3").orElseThrow().deleted()).isTrue();
    }

    // ---- crash: target written, but the event was never marked delivered ----------------------

    @Test
    void crashAfterTargetWriteBeforeMarkingDeliveredRedeliversSafely() {
        source.upsert("u1", Map.of("name", "Asha"));
        source.upsert("u2", Map.of("name", "Ravi"));

        var crashing = new OutboxStore(pg) {
            private boolean crashed;

            @Override
            public void markDelivered(List<Long> ids) {
                if (!crashed) {
                    crashed = true;
                    throw new IllegalStateException("simulated crash before commit of delivered flag");
                }
                super.markDelivered(ids);
            }
        };
        var relay = new OutboxRelay(crashing, target, 10);

        assertThatThrownBy(relay::pollOnce).hasMessageContaining("simulated crash");

        // Target already has the data, but the outbox still thinks both events are pending.
        assertThat(target.read("u1")).isPresent();
        assertThat(crashing.pendingCount()).isEqualTo(2);

        var retry = relay.drain();

        assertThat(retry.delivered()).isEqualTo(2);
        assertThat(retry.applied()).isZero();          // nothing new, all were already there
        assertThat(retry.skippedStale()).isEqualTo(2); // duplicates harmlessly rejected
        assertThat(target.read("u1")).isEqualTo(source.read("u1"));
        assertThat(target.read("u2")).isEqualTo(source.read("u2"));
    }

    // ---- crash: target fails in the middle of a batch ----------------------------------------

    @Test
    void targetFailureMidBatchLosesNothing() {
        for (int i = 1; i <= 5; i++) {
            source.upsert("k" + i, Map.of("i", String.valueOf(i)));
        }
        var calls = new AtomicInteger();
        TargetWriter flaky = new TargetWriter() {
            @Override
            public WriteResult write(VersionedRecord r) {
                if (calls.incrementAndGet() == 3) {
                    throw new IllegalStateException("simulated DynamoDB throttling");
                }
                return target.write(r);
            }

            @Override
            public Optional<VersionedRecord> read(String key) {
                return target.read(key);
            }
        };
        var relay = new OutboxRelay(outbox, flaky, 10);

        assertThatThrownBy(relay::pollOnce).hasMessageContaining("throttling");
        assertThat(outbox.pendingCount()).isEqualTo(3); // 2 delivered and marked, 3 still pending

        relay.drain();

        assertThat(outbox.pendingCount()).isZero();
        for (int i = 1; i <= 5; i++) {
            assertThat(target.read("k" + i)).isEqualTo(source.read("k" + i));
        }
    }

    // ---- duplicate delivery -------------------------------------------------------------------

    @Test
    void duplicateDeliveryOfEveryEventChangesNothing() {
        source.upsert("u1", Map.of("name", "Asha"));
        source.upsert("u1", Map.of("name", "Asha K"));
        source.delete("u2"); // no-op, row never existed
        source.upsert("u2", Map.of("name", "Ravi"));
        source.delete("u2");
        var relay = new OutboxRelay(outbox, target, 10);
        relay.drain();
        var before = List.of(target.read("u1"), target.read("u2"));

        exec("UPDATE outbox SET delivered_at = NULL"); // pretend every ack was lost
        var again = relay.drain();

        assertThat(again.applied()).isZero();
        assertThat(again.skippedStale()).isEqualTo(again.delivered());
        assertThat(List.of(target.read("u1"), target.read("u2"))).isEqualTo(before);
    }

    // ---- stale / reordered events -------------------------------------------------------------

    @Test
    void oldEventArrivingAfterNewerTargetStateCannotRollBackTheTarget() {
        source.upsert("u1", Map.of("name", "v1"));
        source.upsert("u1", Map.of("name", "v2"));
        // The newest state reaches the target first (e.g. via backfill), then old events arrive.
        target.write(source.read("u1").orElseThrow());

        var stats = new OutboxRelay(outbox, target, 10).drain();

        assertThat(stats.applied()).isZero();
        assertThat(target.read("u1").orElseThrow().version()).isEqualTo(2);
        assertThat(target.read("u1").orElseThrow().attributes()).containsEntry("name", "v2");
    }

    @Test
    void eventsDeliveredInReverseOrderStillConverge() {
        source.upsert("u1", Map.of("name", "v1"));
        source.upsert("u1", Map.of("name", "v2"));
        source.delete("u1");

        var events = outbox.fetchPending(10);
        for (int i = events.size() - 1; i >= 0; i--) {
            target.write(events.get(i).record());
        }

        assertThat(target.read("u1")).isEqualTo(source.read("u1"));
        assertThat(target.read("u1").orElseThrow().deleted()).isTrue();
    }

    private void exec(String sql) {
        try (Connection c = pg.getConnection(); Statement s = c.createStatement()) {
            s.execute(sql);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }
}
