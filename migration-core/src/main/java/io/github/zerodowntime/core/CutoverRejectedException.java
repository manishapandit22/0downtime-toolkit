package io.github.zerodowntime.core;

/** The cutover gate refused to switch. The migration was put back to {@link Phase#DUAL_WRITE}. */
public class CutoverRejectedException extends RuntimeException {

    private final transient VerificationReport report;

    public CutoverRejectedException(String reason, VerificationReport report) {
        super(reason);
        this.report = report;
    }

    /** The diff that blocked the cutover, or null if the gate failed for another reason. */
    public VerificationReport report() {
        return report;
    }
}
