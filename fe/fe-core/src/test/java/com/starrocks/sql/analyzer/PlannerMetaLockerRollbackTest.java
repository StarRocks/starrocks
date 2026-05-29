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
     * and a subsequent unlock() must be a no-op (heldEntries was cleared by the first unlock).
     */
    @Test
    public void lockThenUnlockRoundTrip() throws Exception {
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser("select * from t0", connectContext);
        PlannerMetaLocker locker = new PlannerMetaLocker(connectContext, stmt);
        Assertions.assertDoesNotThrow(locker::lock);
        Assertions.assertDoesNotThrow(locker::unlock);
        // Second unlock is a no-op (heldEntries already cleared).
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
            // ("select * from t0"), the rollback inside PlannerMetaLocker.lock()'s catch has no
            // prior entries to release (inner Locker already rolled itself back), so the outer
            // unLockTablesWithIntensiveDbLock should never be called.
            List<Locker> constructed = mocked.constructed();
            Assertions.assertFalse(constructed.isEmpty(), "Locker should have been constructed");
            Mockito.verify(constructed.get(0), Mockito.never())
                    .unLockTablesWithIntensiveDbLock(anyLong(), anyList(), any(LockType.class));
        }
    }

    /**
     * Multi-db variant: when the inner Locker throws on the SECOND db, PlannerMetaLocker's
     * own rollback loop must release the FIRST db before re-throwing — otherwise the caller's
     * unlock() in finally would either skip release (post-fix, via heldEntries=null) and leak
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

            // After rollback, unlock() must still be a no-op (heldEntries cleared).
            Assertions.assertDoesNotThrow(locker::unlock);
            // And the unlock() call above must not have invoked unLockTablesWithIntensiveDbLock again.
            Mockito.verify(constructed.get(0), Mockito.times(1))
                    .unLockTablesWithIntensiveDbLock(anyLong(), anyList(), any(LockType.class));
        }
    }
}
