package io.github.zerodowntime.core;

import java.util.Map;
import java.util.Optional;

/**
 * What the application talks to. Routes every call to whichever store is authoritative in the
 * current {@link Phase}. Writes during a switch throw {@link WritesPausedException}; callers retry.
 *
 * <p>The phase read here is only a routing hint. Correctness comes from the fences underneath: the
 * source write re-checks the phase inside its own transaction, and the target write carries a
 * condition check on the authority flag. A stale hint therefore causes a refused write, never a
 * write to the wrong store.
 */
public final class MigratedStore {

    private final SourceRepository source;
    private final TargetStore target;
    private final MigrationState state;

    public MigratedStore(SourceRepository source, TargetStore target, MigrationState state) {
        this.source = source;
        this.target = target;
        this.state = state;
    }

    public long upsert(String key, Map<String, String> attributes) {
        Phase p = state.current();
        if (p.acceptsSourceWrites()) {
            return source.upsert(key, attributes, state.writeGuard());
        }
        if (p.acceptsTargetWrites()) {
            return target.upsert(key, attributes);
        }
        throw new WritesPausedException("writes paused in phase " + p);
    }

    public Optional<Long> delete(String key) {
        Phase p = state.current();
        if (p.acceptsSourceWrites()) {
            return source.delete(key, state.writeGuard());
        }
        if (p.acceptsTargetWrites()) {
            return target.delete(key);
        }
        throw new WritesPausedException("writes paused in phase " + p);
    }

    public Optional<VersionedRecord> read(String key) {
        return state.current().readsFromTarget() ? target.read(key) : source.read(key);
    }
}
