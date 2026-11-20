package io.github.zerodowntime.core;

/** Outcome of a conditional target write. */
public enum WriteResult {
    /** The target had no row, or an older version; the write took effect. */
    APPLIED,
    /** The target already held the same or a newer version; nothing changed. Not an error. */
    SKIPPED_STALE
}
