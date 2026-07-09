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

import com.google.common.base.Preconditions;

import java.util.concurrent.TimeUnit;

/**
 * A metadata lock scope that keeps both sides of the lock lifecycle in one place: the
 * locks are acquired on construction, re-acquired after every temporary release, and
 * released on {@link #close()}. It is meant for long-running scan paths (e.g. tablet
 * checking) that must periodically step aside for other metadata operations
 * ({@link #refresh()}) or wait for some external condition without holding any
 * metadata lock ({@link #sleepUnlocked(long)}).
 *
 * Outside of those two methods the locks are always held until {@link #close()}, so
 * a scope can be handed to a callee as proof of lock ownership without exposing raw
 * unlock entry points. After {@link #refresh()} or {@link #sleepUnlocked(long)}
 * returns, the locks have been re-acquired from scratch: the caller must re-validate
 * any metadata read before the call, because the db, table or partition may have been
 * dropped or altered in between.
 *
 * The scope also tracks how long its locks have actually been held, see
 * {@link #getHeldTimeNs()}, so callers can report lock-hold statistics without
 * keeping their own timing around the yields (which would wrongly count yielded
 * windows or failed re-acquisitions as hold time).
 *
 * Instances are not thread-safe and must only be used by the creating thread, like
 * the underlying {@link Locker}.
 */
public class YieldableLock implements AutoCloseable {
    private final Locker locker = new Locker();
    private final long dbId;
    /* Null when this scope is a plain database lock. */
    private final Long tableId;
    private final LockType lockType;
    private boolean locked;
    /* System.nanoTime() of the most recent acquisition, valid while locked. */
    private long lastAcquireTimeNs;
    /* Total held time over all completed hold segments. */
    private long heldTimeNs;

    private YieldableLock(long dbId, Long tableId, LockType lockType) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.lockType = lockType;
        lock();
    }

    /**
     * Acquire a plain database lock scope, see {@link Locker#lockDatabase}.
     */
    public static YieldableLock lockDatabase(long dbId, LockType lockType) {
        return new YieldableLock(dbId, null, lockType);
    }

    /**
     * Acquire an intensive table lock scope (intention lock on the database plus the
     * lock on the table), see {@link Locker#lockTableWithIntensiveDbLock}.
     */
    public static YieldableLock lockTableWithIntensiveDbLock(long dbId, long tableId, LockType lockType) {
        return new YieldableLock(dbId, tableId, lockType);
    }

    /**
     * Release the locks and immediately re-acquire them, so that queued waiters can
     * cut in. The caller must re-validate cached metadata afterwards.
     */
    public void refresh() {
        Preconditions.checkState(locked, "lock scope is not held");
        unlock();
        lock();
    }

    /**
     * Sleep with the locks released and re-acquire them before returning, also when
     * the sleep is interrupted. The caller must re-validate cached metadata
     * afterwards.
     */
    public void sleepUnlocked(long sleepMs) throws InterruptedException {
        Preconditions.checkState(locked, "lock scope is not held");
        unlock();
        try {
            Thread.sleep(sleepMs);
        } finally {
            lock();
        }
    }

    @Override
    public void close() {
        if (locked) {
            unlock();
        }
    }

    /**
     * Total time the locks have been held by this scope in nanoseconds, including the
     * current hold segment while the scope is held. Yielded windows and the time after
     * a failed re-acquisition are not counted.
     */
    public long getHeldTimeNs() {
        return heldTimeNs + (locked ? System.nanoTime() - lastAcquireTimeNs : 0);
    }

    /**
     * Same as {@link #getHeldTimeNs()}, in milliseconds.
     */
    public long getHeldTimeMs() {
        return TimeUnit.NANOSECONDS.toMillis(getHeldTimeNs());
    }

    /**
     * Time since the locks were last (re-)acquired in milliseconds, 0 when not held.
     */
    public long getCurrentHoldTimeMs() {
        return locked ? TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastAcquireTimeNs) : 0;
    }

    private void lock() {
        if (tableId == null) {
            locker.lockDatabase(dbId, lockType);
        } else {
            locker.lockTableWithIntensiveDbLock(dbId, tableId, lockType);
        }
        lastAcquireTimeNs = System.nanoTime();
        locked = true;
    }

    private void unlock() {
        // Clear the flag first: if a release fails halfway, close() must not try to
        // release again (preferring a leaked lock over an unbalanced release).
        locked = false;
        heldTimeNs += System.nanoTime() - lastAcquireTimeNs;
        if (tableId == null) {
            locker.unLockDatabase(dbId, lockType);
        } else {
            locker.unLockTableWithIntensiveDbLock(dbId, tableId, lockType);
        }
    }
}
