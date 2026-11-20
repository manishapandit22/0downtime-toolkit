package io.github.zerodowntime.core;

/**
 * A write was refused because the migration is switching authority. Always safe to retry: the
 * pause lasts only while the cutover gate or rollback drain runs.
 */
public class WritesPausedException extends RuntimeException {

    public WritesPausedException(String message) {
        super(message);
    }
}
