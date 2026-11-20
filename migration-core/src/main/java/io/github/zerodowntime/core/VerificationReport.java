package io.github.zerodowntime.core;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * Result of a verification run: only differences that survived every settle re-check.
 *
 * @param sourceRows rows examined on the source (tombstones included)
 * @param transientDiffs differences seen on the first pass that disappeared by themselves
 *     (in-flight writes); a healthy live migration normally shows a few
 * @param settleRounds number of re-check rounds performed
 */
public record VerificationReport(int sourceRows, int transientDiffs, int settleRounds, List<Diff> diffs) {

    public VerificationReport {
        diffs = List.copyOf(diffs);
    }

    /** True only when nothing is left after settling. This is the cutover gate's input. */
    public boolean isClean() {
        return diffs.isEmpty();
    }

    public Map<DiffType, Long> countsByType() {
        Map<DiffType, Long> counts = new EnumMap<>(DiffType.class);
        for (DiffType t : DiffType.values()) {
            counts.put(t, 0L);
        }
        diffs.forEach(d -> counts.merge(d.type(), 1L, Long::sum));
        return counts;
    }

    public String summary() {
        return "rows=" + sourceRows + " transient=" + transientDiffs + " settleRounds=" + settleRounds
                + " remaining=" + countsByType();
    }
}
