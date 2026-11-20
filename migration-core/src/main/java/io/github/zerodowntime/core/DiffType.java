package io.github.zerodowntime.core;

/** How a key differs between source and target. */
public enum DiffType {
    /** Source has the row (or tombstone); target has nothing. */
    MISSING,
    /** Target holds an older version than the source. */
    STALE,
    /** Target holds a key the source has never had. */
    EXTRA,
    /** Same version but different content, or target AHEAD of the source. Never legitimate. */
    MISMATCHED
}
