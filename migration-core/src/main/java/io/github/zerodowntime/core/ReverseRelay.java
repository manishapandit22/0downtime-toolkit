package io.github.zerodowntime.core;

import java.util.List;

/**
 * The rollback path: copies rows written on the authoritative target back to Postgres.
 *
 * <p>Same recipe as the forward {@link OutboxRelay}: at-least-once delivery (an entry is removed
 * from the log only AFTER it was applied) made safe by a monotonic conditional apply on the
 * source. A crash can re-deliver an entry; it can never lose one.
 */
public class ReverseRelay {

    /** Totals for one {@link #drain()}. */
    public record Stats(int delivered, int applied, int skippedStale, java.util.Set<String> keys) {}

    private final TargetStore target;
    private final SourceRepository source;

    public ReverseRelay(TargetStore target, SourceRepository source) {
        this.target = target;
        this.source = source;
    }

    /** Applies pending entries until the log is empty. Only terminates if writes are fenced. */
    public Stats drain() {
        int delivered = 0;
        int applied = 0;
        int skipped = 0;
        java.util.Set<String> keys = new java.util.TreeSet<>();
        while (true) {
            List<VersionedRecord> batch = target.pendingReverse();
            if (batch.isEmpty()) {
                return new Stats(delivered, applied, skipped, keys);
            }
            for (VersionedRecord r : batch) {
                if (source.applyReplicated(r)) {
                    applied++;
                } else {
                    skipped++;
                }
                target.ackReverse(r);
                keys.add(r.key());
                delivered++;
            }
        }
    }
}
