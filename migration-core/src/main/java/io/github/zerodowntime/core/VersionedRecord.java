package io.github.zerodowntime.core;

import java.util.Map;
import java.util.Objects;

/**
 * A row as seen by the migration engine.
 *
 * <p>{@code version} is the per-row monotonic counter incremented by the source on every
 * change (including delete). Ordering between writers is decided by this number only,
 * never by wall-clock time.
 *
 * <p>A delete is represented as a tombstone: {@code deleted = true} with its own version,
 * so a stale writer cannot resurrect the row.
 */
public record VersionedRecord(String key, long version, boolean deleted, Map<String, String> attributes) {

    public VersionedRecord {
        Objects.requireNonNull(key, "key");
        if (key.isBlank()) {
            throw new IllegalArgumentException("key must not be blank");
        }
        if (version < 1) {
            throw new IllegalArgumentException("version must be >= 1, was " + version);
        }
        attributes = attributes == null ? Map.of() : Map.copyOf(attributes);
    }

    public static VersionedRecord live(String key, long version, Map<String, String> attributes) {
        return new VersionedRecord(key, version, false, attributes);
    }

    public static VersionedRecord tombstone(String key, long version) {
        return new VersionedRecord(key, version, true, Map.of());
    }
}
