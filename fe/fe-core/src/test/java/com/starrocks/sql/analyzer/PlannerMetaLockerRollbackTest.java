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

package com.starrocks.sql.analyzer;

import com.starrocks.common.Config;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.util.concurrent.lock.LockManager;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Deque;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;

/**
 * Regression tests for POST-1561: when {@link PlannerMetaLocker#lock()} fails partway
 * (typically because the inner {@code Locker.lockTablesWithIntensiveDbLock} throws under
 * concurrent pressure / deadlock detection), the {@link PlannerMetaLocker} must roll itself
 * back so that the caller's matching {@link PlannerMetaLocker#unlock()} in finally does NOT
 * attempt to release rids that were never acquired — which would otherwise surface as
 * {@code IllegalMonitorStateException: "Attempt to unlock lock, not locked by current locker"}
 * (error code 5600, masquerading as ERR_NO_FILES_FOUND in customer reports).
 *
 * <p>This exercises the stack-based reentrance bookkeeping: each successful lock()/tryLock()
 * pushes a snapshot of the entries it acquired; the matching unlock() pops one snapshot and
 * releases exactly those entries in reverse order.
 */
public class PlannerMetaLockerRollbackTest extends PlanTestBase {

    @BeforeEach
    public void resetLockManager() {
        GlobalStateMgr.getCurrentState().setLockManager(new LockManager());
        Config.lock_manager_enabled = true;
    }

    @AfterEach
    public void cleanupLockManager() {
        GlobalStateMgr.getCurrentState().setLockManager(new LockManager());
    }

    /**
     * Calling unlock() before any successful lock() must be a safe no-op. This is the direct
     * regression check for the SmartNews 4.0.9 incident: when planning fails before lock() is
     * called (or after it has rolled itself back), the outer try/finally still invokes unlock();
     * pre-fix, that hit the LockManager mismatch path.
     */
    @Test
    public void unlockWithoutLockIsNoOp() throws Exception {
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser("select * from t0", connectContext);
        PlannerMetaLocker locker = new PlannerMetaLocker(connectContext, stmt);
        Assertions.assertDoesNotThrow(locker::unlock);
        // Idempotent: a second unlock is still a no-op.
        Assertions.assertDoesNotThrow(locker::unlock);
    }

    /**
     * AutoCloseable contract: {@code close()} on a PlannerMetaLocker that was never locked
     * must not throw — this allows try-with-resources patterns to fail-fast in the body without
     * polluting the failure with a release-side exception.
     */
    @Test
    public void closeWithoutLockIsNoOp() throws Exception {
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser("select * from t0", connectContext);
        Assertions.assertDoesNotThrow(() -> {
            try (PlannerMetaLocker ignored = new PlannerMetaLocker(connectContext, stmt)) {
                // intentionally do not call lock()
            }
        });
    }

    /**
     * Successful lock() then unlock() round-trip must work end-to-end without throwing,
     * and a subsequent unlock() must be a no-op (the stack is empty after the first unlock).
     */
    @Test
    public void lockThenUnlockRoundTrip() throws Exception {
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser("select * from t0", connectContext);
        PlannerMetaLocker locker = new PlannerMetaLocker(connectContext, stmt);
        Assertions.assertDoesNotThrow(locker::lock);
        Assertions.assertDoesNotThrow(locker::unlock);
        // Second unlock is a no-op (stack already drained).
        Assertions.assertDoesNotThrow(locker::unlock);
    }

    /**
     * If {@link PlannerMetaLocker#lock()} fails because the inner {@code Locker} throws
     * mid-acquisition, the outer caller's standard {@code finally { unlock(); }} pattern must
     * not crash with {@code IllegalMonitorStateException}. This mirrors the SmartNews failure
     * mode exactly: ERR_NO_FILES_FOUND or any other planning-time exception triggers cleanup;
     * if the lock state is asymmetric, cleanup crashes and overwrites the original error.
     */
    @Test
    public void lockRollsBackOnInnerFailureLeavingUnlockNoOp() throws Exception {
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser("select * from t0", connectContext);

        try (MockedConstruction<Locker> mocked = Mockito.mockConstruction(Locker.class,
                (mock, ctx) -> Mockito.doThrow(ErrorReportException.report(
                                com.starrocks.common.ErrorCode.ERR_LOCK_ERROR, "simulated inner failure"))
                        .when(mock)
                        .lockTablesWithIntensiveDbLock(anyLong(), anyList(), any(LockType.class)))) {

            PlannerMetaLocker locker = new PlannerMetaLocker(connectContext, stmt);
            Assertions.assertThrows(ErrorReportException.class, locker::lock);

            // After a failed lock() that rolled itself back, unlock() must be a no-op even though
            // PlannerMetaLocker has `tables` populated. Before the POST-1561 fix this threw
            // IllegalMonitorStateException because unlock() would iterate `tables` and try to
            // release rids that were never held.
            Assertions.assertDoesNotThrow(locker::unlock);

            // Verify the rollback was invoked exactly as many times as needed: i.e., we did NOT
            // attempt extra release calls on rids we never held. With single-db single-table SQL
            // ("select * from t0"), the failing lock acquired nothing before throwing (inner Locker
            // already rolled itself back), so the outer unLockTablesWithIntensiveDbLock is never called.
            List<Locker> constructed = mocked.constructed();
            Assertions.assertFalse(constructed.isEmpty(), "Locker should have been constructed");
            Mockito.verify(constructed.get(0), Mockito.never())
                    .unLockTablesWithIntensiveDbLock(anyLong(), anyList(), any(LockType.class));
        }
    }

    /**
     * Multi-db variant: when the inner Locker throws on the SECOND db, PlannerMetaLocker's
     * own rollback loop must release the FIRST db before re-throwing — otherwise the caller's
     * unlock() in finally would either skip release (post-fix, via an empty stack) and leak
     * the first db's lock, or (pre-fix) try to release the second db's never-acquired locks
     * and crash.
     */
    @Test
    public void lockRollsBackEarlierDbsOnLaterDbFailure() throws Exception {
        // A SQL that locks two databases worth of tables (default-db.t0 + db1.tbl1).
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(
                "select t0.v1 from t0 join db1.tbl1 on true", connectContext);

        try (MockedConstruction<Locker> mocked = Mockito.mockConstruction(Locker.class,
                (mock, ctx) -> {
                    // First Locker (created in lock()): 1st call succeeds, 2nd throws.
                    if (ctx.getCount() == 1) {
                        Mockito.doNothing()
                                .doThrow(ErrorReportException.report(
                                        com.starrocks.common.ErrorCode.ERR_LOCK_ERROR,
                                        "simulated failure on second db"))
                                .when(mock)
                                .lockTablesWithIntensiveDbLock(anyLong(), anyList(), any(LockType.class));
                    }
                })) {

            PlannerMetaLocker locker = new PlannerMetaLocker(connectContext, stmt);
            Assertions.assertThrows(ErrorReportException.class, locker::lock);

            // Verify the rollback released the first db (i.e., unLockTablesWithIntensiveDbLock
            // was invoked exactly once on the first Locker — for the only db that succeeded).
            List<Locker> constructed = mocked.constructed();
            Assertions.assertFalse(constructed.isEmpty());
            Mockito.verify(constructed.get(0), Mockito.times(1))
                    .unLockTablesWithIntensiveDbLock(anyLong(), anyList(), any(LockType.class));

            // After rollback, unlock() must still be a no-op (stack empty).
            Assertions.assertDoesNotThrow(locker::unlock);
            // And the unlock() call above must not have invoked unLockTablesWithIntensiveDbLock again.
            Mockito.verify(constructed.get(0), Mockito.times(1))
                    .unLockTablesWithIntensiveDbLock(anyLong(), anyList(), any(LockType.class));
        }
    }

    /**
     * Nested lock()/unlock() pattern (the optimistic-retry path:
     * {@code InsertPlanner.buildExecPlanWithRetry} unlocks during planning then re-locks,
     * and the retry iteration calls {@code StatementPlanner.reAnalyzeStmt}, which itself
     * does {@code try { lock(); ... } finally { unlock(); }} on the same instance).
     *
     * <p>With the stack design, each lock() pushes its own acquisition snapshot and each
     * unlock() pops exactly one. The inner pair must only touch its own LockManager refCount
     * slot and must not disturb the outer acquisition's snapshot (which sits at the bottom of
     * the stack), so the outer unlock() still releases the right rids. A pre-fix single-snapshot
     * design let the inner unlock() clear the outer's record, turning the outer unlock() into a
     * no-op and leaking the meta locks while the retry body kept planning.
     */
    @Test
    public void nestedLockUnlockReleasesAllReferences() throws Exception {
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser("select * from t0", connectContext);
        PlannerMetaLocker locker = new PlannerMetaLocker(connectContext, stmt);

        Assertions.assertEquals(0, readStackDepth(locker), "fresh instance must hold nothing");

        locker.lock();                                              // outer
        Assertions.assertEquals(1, readStackDepth(locker), "outer lock must push one acquisition");
        Object outerSnapshot = readStackBottom(locker);
        Assertions.assertNotNull(outerSnapshot, "outer lock must record its acquisition snapshot");

        locker.lock();                                              // inner (nested)
        Assertions.assertEquals(2, readStackDepth(locker), "nested lock must push a second acquisition");
        Assertions.assertSame(outerSnapshot, readStackBottom(locker),
                "nested lock must NOT disturb the outer acquisition snapshot — the outer unlock relies on it");

        locker.unlock();                                            // inner
        Assertions.assertEquals(1, readStackDepth(locker),
                "inner unlock must pop only the inner acquisition, not drain the stack");
        Assertions.assertSame(outerSnapshot, readStackBottom(locker),
                "inner unlock must leave the outer acquisition snapshot intact — outer still needs it");

        locker.unlock();                                            // outer
        Assertions.assertEquals(0, readStackDepth(locker), "outer unlock must pop the last acquisition");

        // After full unwind a new lock/unlock cycle must work cleanly — proves the LockManager
        // state is balanced (no leaked LockHolders carrying refCount from the previous cycle).
        Assertions.assertDoesNotThrow(locker::lock,
                "follow-up lock() after nested cycle must work — would fail if a stale LockHolder "
                        + "was left in LockManager and a hierarchical/upgrade guard fired.");
        Assertions.assertDoesNotThrow(locker::unlock,
                "follow-up unlock() must release cleanly.");
        Assertions.assertEquals(0, readStackDepth(locker));
    }

    /**
     * AutoCloseable contract: close() must release every pending acquisition, not just one
     * level. If a caller does multiple lock()s inside try-with-resources without matching
     * unlock()s, the implicit close() at the end of the block must drain them all — otherwise
     * the leftover acquisitions leak (LockManager keeps a non-zero refCount on every rid that
     * was acquired). No current caller does this today, but the AutoCloseable contract should
     * hold regardless.
     */
    @Test
    public void closeDrainsAllNestedLocks() throws Exception {
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser("select * from t0", connectContext);
        PlannerMetaLocker locker = new PlannerMetaLocker(connectContext, stmt);

        locker.lock();
        locker.lock();
        locker.lock();
        Assertions.assertEquals(3, readStackDepth(locker));

        locker.close();

        Assertions.assertEquals(0, readStackDepth(locker),
                "close() must drain all nested lock acquisitions, not just one level");

        // After close() drains, the LockManager state must allow a fresh lock/unlock cycle.
        // If close() left stale LockHolders behind, the next lock() would reenter at a higher
        // refCount than expected; if it left the stack inconsistent, the next unlock() would
        // either no-op or crash.
        Assertions.assertDoesNotThrow(locker::lock);
        Assertions.assertDoesNotThrow(locker::unlock);
        Assertions.assertEquals(0, readStackDepth(locker));
    }

    private static int readStackDepth(PlannerMetaLocker locker) throws Exception {
        return heldStack(locker).size();
    }

    /**
     * Returns the bottom (oldest / outermost) acquisition snapshot, or null when empty.
     * {@code Deque.push} adds to the head, so the first-pushed (outer) acquisition is the tail.
     */
    private static Object readStackBottom(PlannerMetaLocker locker) throws Exception {
        return heldStack(locker).peekLast();
    }

    private static Deque<?> heldStack(PlannerMetaLocker locker) throws Exception {
        Field f = PlannerMetaLocker.class.getDeclaredField("heldStack");
        f.setAccessible(true);
        return (Deque<?>) f.get(locker);
    }
}
