package io.github.zerodowntime.core;

import java.util.Optional;

/**
 * Storage interface for the migration target.
 *
 * <p>Contract: {@link #write} is idempotent and monotonic. Calling it any number of times,
 * in any order, from any writer (backfill, relay, retry) leaves the row at the highest
 * version ever offered.
 */
public interface TargetWriter {

    WriteResult write(VersionedRecord record);

    Optional<VersionedRecord> read(String key);
}
