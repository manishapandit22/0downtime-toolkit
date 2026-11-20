package io.github.zerodowntime.core;

import java.util.List;

/**
 * Copies existing source rows to the target in key order, one chunk at a time, saving a
 * checkpoint after each chunk so a restart resumes instead of starting over.
 *
 * <p>Safety does not depend on the checkpoint being perfect. The checkpoint is saved only AFTER
 * the chunk was written, so a crash can cause a chunk to be written twice, never skipped. The
 * second write is harmless: {@link TargetWriter#write} is idempotent and monotonic, and it also
 * rejects the backfill's older snapshot if a live write already delivered a newer version.
 */
public final class Backfiller {

    /** Totals for one {@link #run()}. {@code rows == applied + skippedStale}. */
    public record Stats(int rows, int applied, int skippedStale, int chunks) {}

    private final SourceRepository source;
    private final TargetWriter target;
    private final CheckpointStore checkpoints;
    private final String jobName;
    private final int chunkSize;

    public Backfiller(
            SourceRepository source, TargetWriter target, CheckpointStore checkpoints, String jobName, int chunkSize) {
        if (chunkSize < 1) {
            throw new IllegalArgumentException("chunkSize must be >= 1");
        }
        this.source = source;
        this.target = target;
        this.checkpoints = checkpoints;
        this.jobName = jobName;
        this.chunkSize = chunkSize;
    }

    /** Runs until the whole table has been processed, resuming from the saved checkpoint. */
    public Stats run() {
        String cursor = checkpoints.load(jobName).orElse(null);
        int rows = 0;
        int applied = 0;
        int skipped = 0;
        int chunks = 0;
        while (true) {
            List<VersionedRecord> chunk = source.readChunk(cursor, chunkSize);
            if (chunk.isEmpty()) {
                return new Stats(rows, applied, skipped, chunks);
            }
            for (VersionedRecord r : chunk) {
                if (target.write(r) == WriteResult.APPLIED) {
                    applied++;
                } else {
                    skipped++;
                }
                rows++;
            }
            cursor = chunk.get(chunk.size() - 1).key();
            checkpoints.save(jobName, cursor); // only after the whole chunk is on the target
            chunks++;
        }
    }
}
