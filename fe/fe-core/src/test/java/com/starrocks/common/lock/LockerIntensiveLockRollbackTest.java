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
package com.starrocks.common.lock;

import com.google.common.collect.ImmutableList;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.util.concurrent.lock.LockException;
import com.starrocks.common.util.concurrent.lock.LockManager;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.server.GlobalStateMgr;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Regression tests for POST-1561: when {@code Locker.lock(Table|Tables)WithIntensiveDbLock}
 * fails partway, it must roll back any partially-acquired locks before re-throwing. Without
 * this rollback, the caller's matching {@code unLock*} call would attempt to release rids that
 * were never held and crash with
 * {@code IllegalMonitorStateException: "Attempt to unlock lock, not locked by current locker"}
 * (surfaced to clients as {@code ERROR 5600}).
 */
public class LockerIntensiveLockRollbackTest {

    @BeforeEach
    public void setUp() {
        GlobalStateMgr.getCurrentState().setLockManager(new LockManager());
        Config.lock_manager_enabled = true;
    }

    @AfterEach
    public void tearDown() {
        // Reset to default so other tests are unaffected.
        GlobalStateMgr.getCurrentState().setLockManager(new LockManager());
    }

    /**
     * lockTablesWithIntensiveDbLock: failure on an inner table-rid lock must roll back
     * the previously-acquired db intent lock and any earlier table-rid locks.
     */
    @Test
    public void lockTablesRollsBackOnRidFailure() throws LockException {
        long dbId = 1_000_001L;
        long ridA = 1_000_002L;
        long ridB = 1_000_003L; // We make the inner lock(ridB, ...) throw.
        long ridC = 1_000_004L;

        Locker spy = Mockito.spy(new Locker());
        Mockito.doThrow(new LockException("simulated mid-iteration LockException on " + ridB))
                .when(spy).lock(Mockito.eq(ridB), Mockito.eq(LockType.READ), Mockito.anyLong());

        Assertions.assertThrows(ErrorReportException.class, () ->
                spy.lockTablesWithIntensiveDbLock(dbId, ImmutableList.of(ridA, ridB, ridC), LockType.READ));

        // After rollback the LockManager must be clean: a fresh Locker on a different thread
        // must be able to acquire WRITE on dbId immediately. WRITE conflicts with INTENTION_SHARED,
        // so if the IS leaked, this would block / time out.
        assertCanAcquireExclusively(dbId);
        // ridA should also be fully released. READ does not conflict with READ, so use WRITE
        // to detect leaks.
        assertCanAcquireExclusively(ridA);
    }

    /**
     * lockTablesWithIntensiveDbLock: failure on the very first table-rid lock must still
     * release the db intent that was acquired moments earlier.
     */
    @Test
    public void lockTablesRollsBackWhenFirstRidFails() throws LockException {
        long dbId = 1_001_001L;
        long ridA = 1_001_002L;

        Locker spy = Mockito.spy(new Locker());
        Mockito.doThrow(new LockException("simulated on first rid"))
                .when(spy).lock(Mockito.eq(ridA), Mockito.eq(LockType.READ), Mockito.anyLong());

        Assertions.assertThrows(ErrorReportException.class, () ->
                spy.lockTablesWithIntensiveDbLock(dbId, ImmutableList.of(ridA), LockType.READ));

        assertCanAcquireExclusively(dbId);
    }

    /**
     * lockTablesWithIntensiveDbLock: failure on the very first acquire (db intent itself)
     * must not leak anything.
     */
    @Test
    public void lockTablesRollsBackWhenDbIntentFails() throws LockException {
        long dbId = 1_002_001L;
        long ridA = 1_002_002L;

        Locker spy = Mockito.spy(new Locker());
        Mockito.doThrow(new LockException("simulated on db intent"))
                .when(spy).lock(Mockito.eq(dbId), Mockito.eq(LockType.INTENTION_SHARED), Mockito.anyLong());

        Assertions.assertThrows(ErrorReportException.class, () ->
                spy.lockTablesWithIntensiveDbLock(dbId, ImmutableList.of(ridA), LockType.READ));

        // Nothing should have been acquired, so a fresh acquire is immediate.
        assertCanAcquireExclusively(dbId);
        assertCanAcquireExclusively(ridA);
    }

    /**
     * lockTableWithIntensiveDbLock (single-table variant): same rollback contract.
     */
    @Test
    public void lockTableRollsBackOnTableFailure() throws LockException {
        long dbId = 1_003_001L;
        long tableId = 1_003_002L;

        Locker spy = Mockito.spy(new Locker());
        Mockito.doThrow(new LockException("simulated table-lock failure"))
                .when(spy).lock(Mockito.eq(tableId), Mockito.eq(LockType.READ), Mockito.anyLong());

        Assertions.assertThrows(ErrorReportException.class, () ->
                spy.lockTableWithIntensiveDbLock(dbId, tableId, LockType.READ));

        assertCanAcquireExclusively(dbId);
        assertCanAcquireExclusively(tableId);
    }

    /**
     * WRITE-mode rollback: the same logic with INTENTION_EXCLUSIVE on the db side.
     */
    @Test
    public void lockTablesWriteModeRollback() throws LockException {
        long dbId = 1_004_001L;
        long ridA = 1_004_002L;
        long ridB = 1_004_003L;

        Locker spy = Mockito.spy(new Locker());
        Mockito.doThrow(new LockException("simulated"))
                .when(spy).lock(Mockito.eq(ridB), Mockito.eq(LockType.WRITE), Mockito.anyLong());

        Assertions.assertThrows(ErrorReportException.class, () ->
                spy.lockTablesWithIntensiveDbLock(dbId, ImmutableList.of(ridA, ridB), LockType.WRITE));

        assertCanAcquireExclusively(dbId);
        assertCanAcquireExclusively(ridA);
    }

    /**
     * Success path remains unchanged: acquire, then matching unlock, no exceptions, no leaks.
     */
    @Test
    public void successfulLockUnlockPathUnchanged() throws LockException {
        long dbId = 1_005_001L;
        long ridA = 1_005_002L;
        long ridB = 1_005_003L;

        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(dbId, ImmutableList.of(ridA, ridB), LockType.READ);
        locker.unLockTablesWithIntensiveDbLock(dbId, ImmutableList.of(ridA, ridB), LockType.READ);

        // Both rids and dbId must be free now.
        assertCanAcquireExclusively(dbId);
        assertCanAcquireExclusively(ridA);
        assertCanAcquireExclusively(ridB);
    }

    /**
     * Acquire an exclusive WRITE lock on {@code rid} <b>from a different thread</b> with a
     * short timeout.
     *
     * <p>This MUST run on a different thread than the test thread. {@code Locker.equals} /
     * {@code hashCode} use {@code threadId} (Locker.java:508-522), so a fresh {@code Locker}
     * created on the test thread is treated by {@link LockManager} as the same owner as any
     * Locker leaked by the code under test. {@link com.starrocks.common.util.concurrent.lock.MultiUserLock#tryLock}
     * (lines 98-130) then either re-enters / upgrades the lock (vacuous pass) or throws
     * {@code NotSupportLockException} (failure for the wrong reason). Running on a different
     * thread gives a different {@code threadId} so the LockManager applies the normal
     * conflict matrix: a leaked IS/IX/READ/WRITE will conflict with a fresh-thread WRITE,
     * cause the acquire to wait, and time out — which is exactly the signal we want.
     */
    private void assertCanAcquireExclusively(long rid) {
        LockManager mgr = GlobalStateMgr.getCurrentState().getLockManager();
        ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "assertCanAcquireExclusively-rid" + rid);
            t.setDaemon(true);
            return t;
        });
        try {
            Future<?> future = executor.submit(() -> {
                Locker fresh = new Locker();
                try {
                    mgr.lock(rid, fresh, LockType.WRITE, 1_000);
                } catch (LockException e) {
                    throw new RuntimeException(e);
                }
                try {
                    mgr.release(rid, fresh, LockType.WRITE);
                } catch (Exception releaseEx) {
                    // Best-effort: the assertion below uses the acquire result; release
                    // failure here would only mask a (different) bug.
                }
            });
            try {
                future.get(5, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                Assertions.fail("Expected to acquire exclusive WRITE on rid " + rid
                        + " from a different thread, but acquisition failed — a leaked lock"
                        + " on this rid likely remains. cause=" + e.getCause(), e.getCause());
            } catch (TimeoutException e) {
                future.cancel(true);
                Assertions.fail("Acquiring exclusive WRITE on rid " + rid
                        + " from a different thread did not return within 5s — the inner lock"
                        + " timeout (1s) should have surfaced as a LockException long before"
                        + " this; the test setup is wrong or the LockManager is stuck.");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                Assertions.fail("Interrupted while waiting for exclusive WRITE on rid " + rid);
            }
        } finally {
            executor.shutdownNow();
        }
    }
}
