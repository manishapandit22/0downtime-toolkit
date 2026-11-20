package io.github.zerodowntime.core;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Shadow reads: every read is served from the source (still authoritative), and the same key is
 * also read from the target and compared. Differences are reported to a listener and counted, but
 * never affect what the caller sees and never throw.
 *
 * <p>Lag ({@link DiffType#STALE}, {@link DiffType#MISSING}) is expected while the outbox drains;
 * {@link DiffType#MISMATCHED} is not. A target failure is counted as an error and ignored, so
 * shadowing cannot take down reads.
 */
public final class ShadowReader {

    private final SourceRepository source;
    private final TargetWriter target;
    private final Consumer<Diff> onDiff;

    private final AtomicLong reads = new AtomicLong();
    private final AtomicLong matches = new AtomicLong();
    private final AtomicLong diffs = new AtomicLong();
    private final AtomicLong mismatched = new AtomicLong();
    private final AtomicLong targetErrors = new AtomicLong();

    public ShadowReader(SourceRepository source, TargetWriter target, Consumer<Diff> onDiff) {
        this.source = source;
        this.target = target;
        this.onDiff = onDiff;
    }

    /** Returns the source's answer, whatever the target says. */
    public Optional<VersionedRecord> read(String key) {
        // Target first, then source: see Verifier#recheck for why this order avoids false alarms.
        VersionedRecord t = null;
        boolean targetOk = true;
        try {
            t = target.read(key).orElse(null);
        } catch (RuntimeException e) {
            targetOk = false;
            targetErrors.incrementAndGet();
        }
        Optional<VersionedRecord> s = source.read(key);
        reads.incrementAndGet();

        if (targetOk) {
            Diff d = Diff.classify(key, s.orElse(null), t);
            if (d == null) {
                matches.incrementAndGet();
            } else {
                diffs.incrementAndGet();
                if (d.type() == DiffType.MISMATCHED) {
                    mismatched.incrementAndGet();
                }
                try {
                    onDiff.accept(d);
                } catch (RuntimeException ignored) {
                    // a broken listener must not break reads
                }
            }
        }
        return s;
    }

    public long reads() {
        return reads.get();
    }

    public long matches() {
        return matches.get();
    }

    public long diffs() {
        return diffs.get();
    }

    /** Differences that lag cannot explain. Should be zero at cutover. */
    public long mismatched() {
        return mismatched.get();
    }

    public long targetErrors() {
        return targetErrors.get();
    }
}
