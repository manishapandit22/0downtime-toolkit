package io.github.zerodowntime.core;

/**
 * Drives the authority switch in both directions. Every step is idempotent and resumable: calling
 * {@link #cutover()} or {@link #rollback()} again after a crash continues from the persisted phase.
 *
 * <p><b>Cutover</b>
 * <ol>
 *   <li>{@code DUAL_WRITE -> CUTTING_OVER}. Waits for in-flight source writes, then new writes pause.
 *   <li>Gate: forward outbox drained to zero, verifier report clean, no shadow-read mismatches.
 *       If the gate fails the phase goes back to DUAL_WRITE and writes resume.
 *   <li>Open the target's write fence (authority SOURCE to TARGET), then {@code -> CUTOVER}.
 * </ol>
 * <b>Rollback</b>
 * <ol>
 *   <li>{@code CUTOVER -> ROLLING_BACK}: router stops accepting writes.
 *   <li>Close the target's write fence (authority TARGET to DRAINING). Writes that raced the
 *       router either committed before this (and are in the reverse log) or fail.
 *   <li>Drain the reverse log into Postgres, verify source equals target, hand authority back.
 * </ol>
 * Writes are paused (reads are not) for the duration of the gate or drain. That window is as long
 * as one outbox or reverse-log drain plus one verification, and is the honest cost of a switch
 * that cannot lose or reorder a write.
 */
public final class CutoverController {

    private final MigrationState state;
    private final TargetStore target;
    private final OutboxRelay forwardRelay;
    private final OutboxStore outbox;
    private final ReverseRelay reverseRelay;
    private final Verifier verifier;
    private final ShadowReader shadow; // optional
    private final Verifier liveVerifier; // optional: full diff BEFORE writes are paused
    private volatile long lastPauseMillis = -1;

    /** Pause is over when the delta is this small; each catch-up round shrinks it. */
    private static final int CATCH_UP_TARGET_KEYS = 50;
    private static final int MAX_CATCH_UP_ROUNDS = 10;

    /**
     * @param verifier should be built with {@code settleRounds = 0}: writes are paused when it runs,
     *     so there is nothing in flight to wait for, and a leftover diff is real
     * @param shadow optional; if given, any shadow-read mismatch blocks cutover
     */
    public CutoverController(
            MigrationState state,
            TargetStore target,
            OutboxRelay forwardRelay,
            OutboxStore outbox,
            ReverseRelay reverseRelay,
            Verifier verifier,
            ShadowReader shadow) {
        this(state, target, forwardRelay, outbox, reverseRelay, verifier, shadow, null);
    }

    /**
     * @param liveVerifier if given, cutover first runs a FULL verification while writes still flow
     *     (so a refusal never pauses anyone), and the pause then only re-checks the keys that
     *     changed since. Without it the pause does a full verification and lasts O(table size).
     */
    public CutoverController(
            MigrationState state,
            TargetStore target,
            OutboxRelay forwardRelay,
            OutboxStore outbox,
            ReverseRelay reverseRelay,
            Verifier verifier,
            ShadowReader shadow,
            Verifier liveVerifier) {
        this.liveVerifier = liveVerifier;
        this.state = state;
        this.target = target;
        this.forwardRelay = forwardRelay;
        this.outbox = outbox;
        this.reverseRelay = reverseRelay;
        this.verifier = verifier;
        this.shadow = shadow;
    }

    public void cutover() {
        Phase p = state.current();
        if (p == Phase.CUTOVER) {
            return;
        }

        // Pre-check, writes still flowing. Any change after `watermark` is re-checked in the pause;
        // everything older was covered by this full pass (changes made DURING it have id > watermark).
        Long watermark = null;
        if (p != Phase.CUTTING_OVER && liveVerifier != null) {
            forwardRelay.drain();
            long mark = outbox.maxId();
            VerificationReport pre = liveVerifier.verify();
            if (!pre.isClean()) {
                throw new CutoverRejectedException(
                        "source and target differ (pre-check; writes were never paused): " + pre.summary(), pre);
            }
            watermark = mark;

            // Catch-up: the full pass took a while, so many keys changed meanwhile. Re-check just
            // those (writes still flowing) until the leftover delta is small, so the pause that
            // follows depends on a tiny delta rather than on how long the full pass took.
            for (int round = 0; round < MAX_CATCH_UP_ROUNDS; round++) {
                forwardRelay.drain();
                long next = outbox.maxId();
                java.util.Set<String> delta = outbox.keysChangedAfter(watermark);
                if (delta.size() <= CATCH_UP_TARGET_KEYS) {
                    break;
                }
                VerificationReport r = liveVerifier.verifyKeys(delta);
                if (!r.isClean()) {
                    throw new CutoverRejectedException(
                            "source and target differ (catch-up; writes were never paused): " + r.summary(), r);
                }
                watermark = next;
            }
        }

        if (p != Phase.CUTTING_OVER && !state.transition(p, Phase.CUTTING_OVER)) {
            throw new IllegalStateException("phase changed concurrently, was " + p);
        }
        long pauseStart = System.nanoTime();

        try {
            forwardRelay.drain();
            long pending = outbox.pendingCount();
            if (pending > 0) {
                throw new CutoverRejectedException(pending + " outbox events still undelivered", null);
            }
            // watermark == null (no pre-check, or resuming a crashed cutover): fail safe, check all
            VerificationReport report = watermark == null
                    ? verifier.verify()
                    : verifier.verifyKeys(outbox.keysChangedAfter(watermark));
            if (!report.isClean()) {
                throw new CutoverRejectedException("source and target differ: " + report.summary(), report);
            }
            if (shadow != null && shadow.mismatched() > 0) {
                throw new CutoverRejectedException(shadow.mismatched() + " shadow-read mismatches", null);
            }
        } catch (CutoverRejectedException rejected) {
            state.transition(Phase.CUTTING_OVER, Phase.DUAL_WRITE);
            lastPauseMillis = (System.nanoTime() - pauseStart) / 1_000_000;
            throw rejected;
        }

        moveAuthority(TargetStore.SOURCE, TargetStore.TARGET);
        state.transition(Phase.CUTTING_OVER, Phase.CUTOVER);
        lastPauseMillis = (System.nanoTime() - pauseStart) / 1_000_000;
    }

    /** How long writes were paused by the most recent cutover or rollback, in ms (-1 if none yet). */
    public long lastPauseMillis() {
        return lastPauseMillis;
    }

    /** Gives up an interrupted cutover. Only legal while the target fence is still closed. */
    public void abortCutover() {
        if (state.current() != Phase.CUTTING_OVER) {
            throw new IllegalStateException("no cutover in progress");
        }
        if (!TargetStore.SOURCE.equals(target.authority())) {
            throw new IllegalStateException("target fence already open; finish the cutover or roll back");
        }
        state.transition(Phase.CUTTING_OVER, Phase.DUAL_WRITE);
    }

    public void rollback() {
        Phase p = state.current();
        if (p == Phase.ROLLED_BACK) {
            return;
        }
        boolean resuming = p == Phase.ROLLING_BACK;
        long pauseStart = System.nanoTime();
        if (p == Phase.CUTOVER) {
            if (!state.transition(Phase.CUTOVER, Phase.ROLLING_BACK)) {
                throw new IllegalStateException("phase changed concurrently");
            }
        } else if (p != Phase.ROLLING_BACK) {
            throw new IllegalStateException("cannot roll back from " + p);
        }

        moveAuthority(TargetStore.TARGET, TargetStore.DRAINING);
        ReverseRelay.Stats drained = reverseRelay.drain();
        int left = target.pendingReverse().size();
        if (left > 0) {
            throw new IllegalStateException("rollback blocked: " + left + " reverse-log entries remain");
        }
        // Stores were verified equal at cutover, so only keys written since can differ: exactly the
        // reverse-log keys. That keeps the pause independent of table size. A resumed rollback may
        // have lost track of keys drained before the crash, so it fails safe with a full check.
        VerificationReport report = resuming ? verifier.verify() : verifier.verifyKeys(drained.keys());
        if (!report.isClean()) {
            throw new IllegalStateException("rollback blocked: stores differ after reverse sync: " + report.summary());
        }
        moveAuthority(TargetStore.DRAINING, TargetStore.SOURCE);
        state.transition(Phase.ROLLING_BACK, Phase.ROLLED_BACK);
        lastPauseMillis = (System.nanoTime() - pauseStart) / 1_000_000;
    }

    /** Idempotent authority-flag move: fine if it is already at {@code to}, an error otherwise. */
    private void moveAuthority(String from, String to) {
        if (target.casAuthority(from, to)) {
            return;
        }
        String now = target.authority();
        if (!now.equals(to)) {
            throw new IllegalStateException("authority is " + now + ", expected " + from + " or " + to);
        }
    }
}
