package io.github.zerodowntime.demo;

import io.github.zerodowntime.cli.Cli;
import io.github.zerodowntime.cli.Services;
import io.github.zerodowntime.core.Backfiller;
import io.github.zerodowntime.core.CheckpointStore;
import io.github.zerodowntime.core.CutoverController;
import io.github.zerodowntime.core.MigratedStore;
import io.github.zerodowntime.core.OutboxRelay;
import io.github.zerodowntime.core.Phase;
import io.github.zerodowntime.core.ReverseRelay;
import io.github.zerodowntime.core.ShadowReader;
import io.github.zerodowntime.core.VerificationReport;
import io.github.zerodowntime.core.VersionedRecord;
import io.github.zerodowntime.core.Verifier;
import io.github.zerodowntime.core.WritesPausedException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * A whole migration under live traffic: seed existing rows, start writers, backfill, verify,
 * cut over, run on the target, roll back, run on the source, then prove nothing was lost.
 *
 * <p>Each key has exactly one writer, so after every ACKNOWLEDGED write the key's version must be
 * exactly what that writer last saw. The final check compares every written key on the source
 * with the writer's own record, then runs the verifier for a last source-vs-target diff.
 */
public final class DemoRunner {

    public record Config(int rows, int writers, int writesPerSecond, int phaseSeconds) {}

    public record Report(
            Config config,
            double backfillSeconds,
            double backfillRowsPerSecond,
            int backfillSkippedStale,
            int verifyRows,
            int verifyTransient,
            int verifyRemaining,
            double verifySeconds,
            long cutoverTotalMs,
            long cutoverWindowMs,
            long rollbackWindowMs,
            long maxWriterBlockedMs,
            long pausedRetries,
            Map<String, Long> writesByPhase,
            long totalAckedWrites,
            double lagP50Ms,
            double lagP99Ms,
            double lagMaxMs,
            int lagSamples,
            long shadowReads,
            long shadowMismatched,
            int lostOrWrongWrites,
            int finalDiffs) {

        public String toMarkdown() {
            StringBuilder b = new StringBuilder();
            b.append("| metric | value |\n|---|---|\n");
            b.append(row("seed rows (existing data)", config.rows()));
            b.append(row("writer threads / target live write rate", config.writers() + " / " + config.writesPerSecond() + " writes/s"));
            b.append(row("backfill (under live writes)", String.format("%.0f rows/s (%.1f s), %d stale snapshots rejected", backfillRowsPerSecond, backfillSeconds, backfillSkippedStale)));
            b.append(row("verify under load", String.format("%d rows in %.1f s, %d transient diffs settled, %d remaining", verifyRows, verifySeconds, verifyTransient, verifyRemaining)));
            b.append(row("replication lag source->target", String.format("p50 %.1f ms, p99 %.1f ms, max %.1f ms (%d probes)", lagP50Ms, lagP99Ms, lagMaxMs, lagSamples)));
            b.append(row("shadow reads", shadowReads + " reads, " + shadowMismatched + " mismatches"));
            b.append(row("cutover: total duration (incl. live pre-check, writes flowing)", cutoverTotalMs + " ms"));
            b.append(row("cutover: writes paused for", cutoverWindowMs + " ms"));
            b.append(row("rollback: writes paused for", rollbackWindowMs + " ms"));
            b.append(row("longest a single write was blocked", maxWriterBlockedMs + " ms (" + pausedRetries + " paused retries in total)"));
            b.append(row("acknowledged writes by phase", writesByPhase.toString()));
            b.append(row("total acknowledged writes", totalAckedWrites));
            b.append(row("acked writes lost or wrong at the end", lostOrWrongWrites));
            b.append(row("final source-vs-target diff", finalDiffs));
            return b.toString();
        }

        private static String row(String k, Object v) {
            return "| " + k + " | " + v + " |\n";
        }
    }

    private DemoRunner() {}

    public static Report run(Services s, Config cfg, PrintStream log) throws Exception {
        MigratedStore router = new MigratedStore(s.source(), s.targetStore(), s.state());
        int initCode = new Cli(s, log, log).run(new String[] {"init"});
        if (initCode != Cli.OK) {
            throw new IllegalStateException("init failed");
        }

        log.println("seeding " + cfg.rows() + " existing rows (bypassing the outbox, like a pre-existing table)");
        seed(s, cfg.rows());

        var relay = new OutboxRelay(s.outbox(), s.dataWriter(), 200);
        var shadow = new ShadowReader(s.source(), s.dataWriter(), d -> {});
        var reverse = new ReverseRelay(s.targetStore(), s.source());
        var liveVerifier = new Verifier(s.source(), s.dataWriter(), s.dataWriter(), 500, 5, Duration.ofMillis(500));
        var gateVerifier = new Verifier(s.source(), s.dataWriter(), s.dataWriter(), 500, 0, Duration.ZERO);
        var controller = new CutoverController(s.state(), s.targetStore(), relay, s.outbox(), reverse, gateVerifier, shadow, liveVerifier);

        AtomicBoolean stop = new AtomicBoolean();
        List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());
        Map<String, long[]> acked = new ConcurrentHashMap<>();
        Map<String, LongAdder> byPhase = new ConcurrentHashMap<>();
        LongAdder pausedRetries = new LongAdder();
        AtomicLong maxBlockedNanos = new AtomicLong();
        String[] label = {"warmup"};
        List<Thread> threads = new ArrayList<>();

        // forward replication, running the whole time like a real deployment
        threads.add(daemon("relay", failures, stop, () -> {
            relay.drain();
            Thread.sleep(2);
        }));

        // reverse replication, running the whole time too: keeps the rollback drain short
        threads.add(daemon("reverse-relay", failures, stop, () -> {
            reverse.drain();
            Thread.sleep(5);
        }));

        // live writers: each owns a slice of keys, updates / inserts / deletes at a fixed pace
        long intervalNanos = 1_000_000_000L * cfg.writers() / Math.max(cfg.writesPerSecond(), 1);
        for (int t = 0; t < cfg.writers(); t++) {
            final int id = t;
            threads.add(writer(id, cfg, router, acked, byPhase, label, pausedRetries, maxBlockedNanos, stop, failures, intervalNanos));
        }

        // replication-lag probe: commit on the source, time until the target shows that version
        List<Double> lagMs = Collections.synchronizedList(new ArrayList<>());
        threads.add(daemon("lag-probe", failures, stop, () -> {
            Phase p = s.state().current();
            if (p.acceptsSourceWrites()) {
                try {
                    long v = router.upsert("probe", Map.of("t", String.valueOf(System.nanoTime())));
                    long t0 = System.nanoTime();
                    while (!stop.get() && System.nanoTime() - t0 < 5_000_000_000L) {
                        var r = s.dataWriter().read("probe");
                        if (r.isPresent() && r.get().version() >= v) {
                            lagMs.add((System.nanoTime() - t0) / 1e6);
                            break;
                        }
                        Thread.sleep(1);
                    }
                } catch (WritesPausedException ignored) {
                    // a switch started; skip this probe
                }
            }
            Thread.sleep(40);
        }));

        // shadow reads while the source is authoritative
        threads.add(daemon("shadow", failures, stop, () -> {
            if (s.state().current().acceptsSourceWrites()) {
                shadow.read(key(ThreadLocalRandom.current().nextInt(cfg.rows())));
            }
            Thread.sleep(5);
        }));

        threads.forEach(Thread::start);

        label[0] = "dual-write";
        Thread.sleep(2000);

        log.println("backfill under live writes...");
        label[0] = "dual-write+backfill";
        long b0 = System.nanoTime();
        var bf = new Backfiller(s.source(), s.dataWriter(), new CheckpointStore(s.postgres()), "demo", 500).run();
        double backfillSecs = (System.nanoTime() - b0) / 1e9;
        log.printf("  %d rows in %.1f s (%.0f rows/s), %d stale snapshots rejected%n", bf.rows(), backfillSecs, bf.rows() / backfillSecs, bf.skippedStale());

        log.println("verify under live writes...");
        label[0] = "dual-write+verify";
        long v0 = System.nanoTime();
        VerificationReport vr = liveVerifier.verify();
        double verifySecs = (System.nanoTime() - v0) / 1e9;
        log.println("  " + vr.summary());

        label[0] = "dual-write";
        Thread.sleep(1000);

        log.println("cutover under live writes...");
        label[0] = "cutover";
        long c0 = System.nanoTime();
        controller.cutover();
        long cutoverTotalMs = (System.nanoTime() - c0) / 1_000_000;
        long cutoverMs = controller.lastPauseMillis();
        log.println("  total " + cutoverTotalMs + " ms, of which writes were paused for " + cutoverMs + " ms");

        label[0] = "target-authoritative";
        Thread.sleep(cfg.phaseSeconds() * 1000L);

        log.println("rollback under live writes...");
        label[0] = "rollback";
        controller.rollback();
        long rollbackMs = controller.lastPauseMillis();
        log.println("  writes were paused for " + rollbackMs + " ms");

        label[0] = "rolled-back";
        Thread.sleep(cfg.phaseSeconds() * 1000L);

        stop.set(true);
        for (Thread t : threads) {
            t.join(30_000);
        }
        if (!failures.isEmpty()) {
            throw new IllegalStateException("background thread failed", failures.get(0));
        }
        relay.drain();

        log.println("final check...");
        int bad = 0;
        for (Map.Entry<String, long[]> e : acked.entrySet()) {
            VersionedRecord r = s.source().read(e.getKey()).orElse(null);
            if (r == null || r.version() != e.getValue()[0] || r.deleted() != (e.getValue()[1] == 1)) {
                bad++;
            }
        }
        VerificationReport fin = gateVerifier.verify();
        log.println("  " + fin.summary());

        Collections.sort(lagMs);
        long total = byPhase.values().stream().mapToLong(LongAdder::sum).sum();
        Map<String, Long> phases = new TreeMap<>();
        byPhase.forEach((k, v) -> phases.put(k, v.sum()));
        return new Report(
                cfg,
                backfillSecs,
                bf.rows() / backfillSecs,
                bf.skippedStale(),
                vr.sourceRows(),
                vr.transientDiffs(),
                vr.diffs().size(),
                verifySecs,
                cutoverTotalMs,
                cutoverMs,
                rollbackMs,
                maxBlockedNanos.get() / 1_000_000,
                pausedRetries.sum(),
                phases,
                total,
                percentile(lagMs, 0.50),
                percentile(lagMs, 0.99),
                lagMs.isEmpty() ? 0 : lagMs.get(lagMs.size() - 1),
                lagMs.size(),
                shadow.reads(),
                shadow.mismatched(),
                bad,
                fin.diffs().size());
    }

    private static Thread writer(
            int id,
            Config cfg,
            MigratedStore router,
            Map<String, long[]> acked,
            Map<String, LongAdder> byPhase,
            String[] label,
            LongAdder pausedRetries,
            AtomicLong maxBlockedNanos,
            AtomicBoolean stop,
            List<Throwable> failures,
            long intervalNanos) {
        Thread t = new Thread(() -> {
            long next = System.nanoTime();
            long seq = 0;
            long pausedSince = 0;
            ThreadLocalRandom rnd = ThreadLocalRandom.current();
            while (!stop.get()) {
                // keys owned by this writer: rows i with i % writers == id, plus its own new keys
                int dice = rnd.nextInt(10);
                String key;
                if (dice < 2) {
                    key = "n" + id + "-" + (seq % 500); // insert / re-insert its own new keys
                } else {
                    key = key(rnd.nextInt(cfg.rows() / cfg.writers()) * cfg.writers() + id);
                }
                try {
                    long version;
                    boolean deleted = false;
                    if (dice == 9) {
                        var v = router.delete(key);
                        if (v.isEmpty()) {
                            continue;
                        }
                        version = v.get();
                        deleted = true;
                    } else {
                        version = router.upsert(key, Map.of("name", "u-" + key, "n", String.valueOf(seq)));
                    }
                    acked.put(key, new long[] {version, deleted ? 1 : 0});
                    byPhase.computeIfAbsent(label[0], k -> new LongAdder()).increment();
                    seq++;
                    if (pausedSince != 0) {
                        maxBlockedNanos.accumulateAndGet(System.nanoTime() - pausedSince, Math::max);
                        pausedSince = 0;
                    }
                    next += intervalNanos;
                    long wait = next - System.nanoTime();
                    if (wait > 0) {
                        Thread.sleep(wait / 1_000_000, (int) (wait % 1_000_000));
                    } else {
                        next = System.nanoTime();
                    }
                } catch (WritesPausedException e) {
                    pausedRetries.increment();
                    if (pausedSince == 0) {
                        pausedSince = System.nanoTime();
                    }
                    sleep(1);
                } catch (Throwable e) {
                    failures.add(e);
                    return;
                }
            }
        }, "writer-" + id);
        t.setDaemon(true);
        return t;
    }

    private interface Step {
        void run() throws Exception;
    }

    private static Thread daemon(String name, List<Throwable> failures, AtomicBoolean stop, Step step) {
        Thread t = new Thread(() -> {
            while (!stop.get()) {
                try {
                    step.run();
                } catch (Throwable e) {
                    failures.add(e);
                    return;
                }
            }
        }, name);
        t.setDaemon(true);
        return t;
    }

    private static void seed(Services s, int rows) throws SQLException {
        try (Connection c = s.postgres().getConnection();
                PreparedStatement ps = c.prepareStatement(
                        "INSERT INTO source_records (record_key, version, deleted, attrs) VALUES (?, 1, false, ?::jsonb)")) {
            c.setAutoCommit(false);
            for (int i = 0; i < rows; i++) {
                ps.setString(1, key(i));
                ps.setString(2, "{\"name\":\"user " + i + "\",\"email\":\"user" + i + "@example.com\",\"plan\":\"free\"}");
                ps.addBatch();
                if (i % 2000 == 1999) {
                    ps.executeBatch();
                }
            }
            ps.executeBatch();
            c.commit();
        }
    }

    static String key(int i) {
        return String.format("u%08d", i);
    }

    private static double percentile(List<Double> sorted, double p) {
        if (sorted.isEmpty()) {
            return 0;
        }
        return sorted.get(Math.min(sorted.size() - 1, (int) Math.ceil(p * sorted.size()) - 1));
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
