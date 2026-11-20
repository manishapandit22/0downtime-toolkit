package io.github.zerodowntime.core;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Compares source and target and classifies every difference as missing / stale / extra /
 * mismatched.
 *
 * <p>A live system never holds still, so a single pass over-reports: a row can be read from the
 * source, then changed and replicated before the target is read. Therefore:
 * <ol>
 *   <li>Pass 1 walks the source in key order, comparing each row to its target row.
 *   <li>Pass 2 scans the target for keys the source does not have.
 *   <li>Every difference found is a <em>candidate</em>. After a settle delay each candidate is
 *       re-read from BOTH sides and re-classified. Only candidates that still differ after the
 *       final round are reported; the rest are counted as transient.
 * </ol>
 * Comparison is by version first, so an older snapshot is recognised as lag rather than
 * corruption. Same version with different content is never explained by timing.
 */
public final class Verifier {

    /** Pauses between settle rounds. Injectable so tests need not sleep. */
    @FunctionalInterface
    public interface Sleeper {
        void sleep(Duration d) throws InterruptedException;
    }

    private final SourceRepository source;
    private final TargetWriter target;
    private final TargetScanner scanner;
    private final int chunkSize;
    private final int settleRounds;
    private final Duration settleDelay;
    private final Sleeper sleeper;

    public Verifier(
            SourceRepository source,
            TargetWriter target,
            TargetScanner scanner,
            int chunkSize,
            int settleRounds,
            Duration settleDelay) {
        this(source, target, scanner, chunkSize, settleRounds, settleDelay, Thread::sleep);
    }

    public Verifier(
            SourceRepository source,
            TargetWriter target,
            TargetScanner scanner,
            int chunkSize,
            int settleRounds,
            Duration settleDelay,
            Sleeper sleeper) {
        if (chunkSize < 1) {
            throw new IllegalArgumentException("chunkSize must be >= 1");
        }
        if (settleRounds < 0) {
            throw new IllegalArgumentException("settleRounds must be >= 0");
        }
        this.source = source;
        this.target = target;
        this.scanner = scanner;
        this.chunkSize = chunkSize;
        this.settleRounds = settleRounds;
        this.settleDelay = settleDelay;
        this.sleeper = sleeper;
    }

    public VerificationReport verify() {
        final Map<String, Diff> firstPass = new LinkedHashMap<>();
        int rows = 0;

        String cursor = null;
        while (true) {
            List<VersionedRecord> chunk = source.readChunk(cursor, chunkSize);
            if (chunk.isEmpty()) {
                break;
            }
            for (VersionedRecord s : chunk) {
                Diff d = Diff.classify(s.key(), s, target.read(s.key()).orElse(null));
                if (d != null) {
                    firstPass.put(s.key(), d);
                }
                rows++;
            }
            cursor = chunk.get(chunk.size() - 1).key();
        }

        scanner.forEachKey(key -> {
            if (source.read(key).isEmpty()) {
                long tv = target.read(key).map(VersionedRecord::version).orElse(0L);
                firstPass.put(key, new Diff(key, DiffType.EXTRA, 0, tv));
            }
        });

        return settle(firstPass, rows);
    }

    /**
     * Verifies only the given keys (both sides compared, extras included). Used inside the write
     * pause, where the cost must depend on how much changed, not on the size of the table.
     */
    public VerificationReport verifyKeys(java.util.Collection<String> keys) {
        final Map<String, Diff> firstPass = new LinkedHashMap<>();
        for (String key : keys) {
            VersionedRecord t = target.read(key).orElse(null);
            VersionedRecord s = source.read(key).orElse(null);
            Diff d = Diff.classify(key, s, t);
            if (d != null) {
                firstPass.put(key, d);
            }
        }
        return settle(firstPass, keys.size());
    }

    private VerificationReport settle(Map<String, Diff> firstPass, int rows) {
        Map<String, Diff> candidates = firstPass;
        int firstPassCount = firstPass.size();
        int rounds = 0;
        while (!candidates.isEmpty() && rounds < settleRounds) {
            pause();
            rounds++;
            candidates = recheck(candidates);
        }

        int transientCount = firstPassCount - candidates.size();
        return new VerificationReport(rows, transientCount, rounds, new ArrayList<>(candidates.values()));
    }

    private Map<String, Diff> recheck(Map<String, Diff> candidates) {
        Map<String, Diff> still = new LinkedHashMap<>();
        for (String key : candidates.keySet()) {
            // Target first, then source: the source can only have moved forward in between, so
            // timing shows up as STALE (lag, retried) and never as a bogus "target ahead".
            VersionedRecord t = target.read(key).orElse(null);
            VersionedRecord s = source.read(key).orElse(null);
            Diff d = Diff.classify(key, s, t);
            if (d != null) {
                still.put(key, d);
            }
        }
        return still;
    }

    private void pause() {
        try {
            sleeper.sleep(settleDelay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("verification interrupted", e);
        }
    }
}
