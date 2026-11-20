package io.github.zerodowntime.core;

/** One difference found by the {@link Verifier}. Versions are 0 when that side has no row. */
public record Diff(String key, DiffType type, long sourceVersion, long targetVersion) {

    /**
     * Compares one key's two sides. Returns null when they agree. Pure function, shared by the
     * verifier and shadow reads so both classify identically.
     */
    static Diff classify(String key, VersionedRecord source, VersionedRecord target) {
        if (source == null && target == null) {
            return null;
        }
        if (source == null) {
            return new Diff(key, DiffType.EXTRA, 0, target.version());
        }
        if (target == null) {
            return new Diff(key, DiffType.MISSING, source.version(), 0);
        }
        if (target.version() < source.version()) {
            return new Diff(key, DiffType.STALE, source.version(), target.version());
        }
        if (target.version() > source.version() || !source.equals(target)) {
            return new Diff(key, DiffType.MISMATCHED, source.version(), target.version());
        }
        return null;
    }
}
