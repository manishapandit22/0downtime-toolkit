package io.github.zerodowntime.core;

import java.util.function.Consumer;

/** Lets the verifier find rows that exist on the target but not on the source. */
public interface TargetScanner {

    /** Calls {@code consumer} once for every key on the target, in no particular order. */
    void forEachKey(Consumer<String> consumer);
}
