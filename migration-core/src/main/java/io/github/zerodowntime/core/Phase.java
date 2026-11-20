package io.github.zerodowntime.core;

/**
 * Where the migration is. Backfill, verify and shadow reads are activities that happen during
 * {@link #DUAL_WRITE}; they do not change who is authoritative, so they are not phases.
 *
 * <pre>
 * DUAL_WRITE ──► CUTTING_OVER ──► CUTOVER ──► ROLLING_BACK ──► ROLLED_BACK
 *      ▲              │                                              │
 *      └── (gate failed, abort)                                      └─► CUTTING_OVER (try again)
 * </pre>
 */
public enum Phase {
    /** Source is authoritative; every change also goes to the outbox for the target. */
    DUAL_WRITE,
    /** Writes are paused while the gate (outbox empty, diff clean) is checked. Reads use the source. */
    CUTTING_OVER,
    /** Target is authoritative; every change is also logged for the reverse path. */
    CUTOVER,
    /** Writes are paused while the reverse log drains back into the source. Reads use the target. */
    ROLLING_BACK,
    /** Source is authoritative again. Behaves like DUAL_WRITE. */
    ROLLED_BACK;

    public boolean canGo(Phase to) {
        return switch (this) {
            case DUAL_WRITE -> to == CUTTING_OVER;
            case CUTTING_OVER -> to == CUTOVER || to == DUAL_WRITE;
            case CUTOVER -> to == ROLLING_BACK;
            case ROLLING_BACK -> to == ROLLED_BACK;
            case ROLLED_BACK -> to == CUTTING_OVER;
        };
    }

    public boolean acceptsSourceWrites() {
        return this == DUAL_WRITE || this == ROLLED_BACK;
    }

    public boolean acceptsTargetWrites() {
        return this == CUTOVER;
    }

    /** Which store answers reads. */
    public boolean readsFromTarget() {
        return this == CUTOVER || this == ROLLING_BACK;
    }
}
