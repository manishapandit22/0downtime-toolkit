package io.github.zerodowntime.core;

import java.util.ArrayList;
import java.util.List;

/**
 * Copies outbox events to the target.
 *
 * <p>Delivery is <b>at-least-once</b>: an event is marked delivered only AFTER the target write
 * succeeded. A crash between those two steps means the event is sent again. That is safe because
 * {@link TargetWriter#write} is idempotent and monotonic, so a repeat is just
 * {@link WriteResult#SKIPPED_STALE}.
 */
public final class OutboxRelay {

    /** What one poll did. {@code delivered == applied + skippedStale}. */
    public record Stats(int delivered, int applied, int skippedStale) {
        static final Stats NONE = new Stats(0, 0, 0);

        Stats plus(Stats o) {
            return new Stats(delivered + o.delivered, applied + o.applied, skippedStale + o.skippedStale);
        }
    }

    private final OutboxStore outbox;
    private final TargetWriter target;
    private final int batchSize;

    public OutboxRelay(OutboxStore outbox, TargetWriter target, int batchSize) {
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must be >= 1");
        }
        this.outbox = outbox;
        this.target = target;
        this.batchSize = batchSize;
    }

    /**
     * Delivers one batch. If a target write throws, events already written in this batch are still
     * marked delivered (best effort) before the exception propagates; the rest stay pending.
     */
    public Stats pollOnce() {
        List<OutboxStore.Event> batch = outbox.fetchPending(batchSize);
        if (batch.isEmpty()) {
            return Stats.NONE;
        }
        List<Long> done = new ArrayList<>();
        int applied = 0;
        int skipped = 0;
        try {
            for (OutboxStore.Event e : batch) {
                WriteResult r = target.write(e.record());
                done.add(e.id());
                if (r == WriteResult.APPLIED) {
                    applied++;
                } else {
                    skipped++;
                }
            }
        } finally {
            outbox.markDelivered(done);
        }
        return new Stats(done.size(), applied, skipped);
    }

    /** Polls until nothing is pending. */
    public Stats drain() {
        Stats total = Stats.NONE;
        Stats last;
        do {
            last = pollOnce();
            total = total.plus(last);
        } while (last.delivered() > 0);
        return total;
    }
}
