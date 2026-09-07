// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.common.util.concurrent.lock;

/**
 * How many FE metadata locks the current thread is holding.
 *
 * <p>This is the one fact a caller several frames deep cannot otherwise obtain. {@link Locker} is a
 * per-acquisition object -- {@code new Locker()} appears at every lock site -- and it delegates to
 * the global {@link LockManager}, which is keyed by {@code rid} and knows nothing about "the
 * current thread". So a method like {@code MetadataMgr#getTableStatistics}, seven or eight frames
 * below whoever took the lock, has no way to ask whether it is inside a critical section. Static
 * analysis cannot answer it either: the FE is interfaces all the way down, and virtual dispatch
 * blows the reachable set up until the result is a suppression file. The information is dynamic, so
 * it is tracked dynamically.
 *
 * <h3>Why the counter is maintained unconditionally</h3>
 *
 * It would be tempting to skip the bookkeeping while
 * {@link BlockingCallValidator} is switched off. That would make the switch a lie: turn the check
 * on at runtime while a critical section is open and the depth would read 0 -- the check would
 * report nothing and look clean. A depth that is only sometimes right is worse than no depth, so
 * both ends are maintained always. The cost is one {@link ThreadLocal} read plus an {@code int}
 * increment on each side of a lock acquisition, next to a {@code ConcurrentHashMap} lookup and a
 * possible park in {@link LockManager}.
 *
 * <h3>Accuracy</h3>
 *
 * Both ends move only on success, which is what keeps the count equal to the number of locks the
 * thread actually holds. An acquisition that timed out or was refused holds nothing, so it must not
 * raise the depth; a release that threw released nothing -- {@link LockManager} refuses before
 * touching the lock table when the rid is not held, and {@code MultiUserLock} throws only where no
 * refcount was decremented -- so it must not lower it.
 * <p>
 * Of the two ways to be wrong, too low is the dangerous one. Too high makes the check report
 * violations that were not committed: noisy, and visible. Too low makes it wave real ones through
 * without a word, which is indistinguishable from the code being correct. So the count errs high:
 * it is clamped at zero rather than going negative, and a genuine lock leak leaves it raised --
 * which is a symptom of that leak, not a defect of this counter.
 */
public final class LockHoldDepth {
    /**
     * A one-element array rather than {@code ThreadLocal<Integer>}: the value is mutated on every
     * lock acquisition and release, and boxing each new count would allocate on the FE's hottest
     * metadata path.
     */
    private static final ThreadLocal<int[]> DEPTH = ThreadLocal.withInitial(() -> new int[1]);

    private LockHoldDepth() {
    }

    /** Record that this thread has just acquired a metadata lock. */
    static void enter() {
        DEPTH.get()[0]++;
    }

    /**
     * Record that this thread has just released one. Called only after the release succeeded, and
     * clamped at zero so a stray extra call cannot make the depth negative and blind the check for
     * the next lock this thread takes.
     */
    static void exit() {
        int[] depth = DEPTH.get();
        if (depth[0] > 0) {
            depth[0]--;
        }
    }

    /** Number of metadata locks held by the current thread. */
    public static int current() {
        return DEPTH.get()[0];
    }

    /** Whether the current thread is inside a critical section. */
    public static boolean isUnderLock() {
        return DEPTH.get()[0] > 0;
    }

    /**
     * Drop this thread's count. For tests that simulate a lock leak, and for a thread that is being
     * handed back to a pool after an unwind whose lock bookkeeping cannot be trusted.
     */
    public static void reset() {
        DEPTH.get()[0] = 0;
    }
}
