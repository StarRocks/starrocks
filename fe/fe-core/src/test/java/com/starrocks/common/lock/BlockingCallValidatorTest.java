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

import com.starrocks.common.Config;
import com.starrocks.common.util.concurrent.lock.BlockingCallUnderLock;
import com.starrocks.common.util.concurrent.lock.LockHoldDepth;
import com.starrocks.common.util.concurrent.lock.LockInvariantViolations;
import com.starrocks.common.util.concurrent.lock.LockInvariantViolations.Mode;
import com.starrocks.common.util.concurrent.lock.LockManager;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.server.GlobalStateMgr;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * The runtime half of the "no resolve inside a critical section" rule: whether the check fires
 * exactly when an external system is contacted under a metadata lock, and stays silent otherwise.
 * <p>
 * Every case takes a real lock through {@link Locker} rather than poking the depth counter,
 * because the counter's coupling to the lock lifecycle is half of what can break.
 */
public class BlockingCallValidatorTest {
    private static final long INTERNAL_DB_ID = 20001L;
    private static final long INTERNAL_TABLE_ID = 20002L;

    private String savedMode;
    private long savedLogInterval;

    @BeforeEach
    public void setUp() {
        GlobalStateMgr.getCurrentState().setLockManager(new LockManager());
        savedMode = Config.lock_blocking_call_validation_mode;
        savedLogInterval = Config.lock_invariant_violation_log_interval_ms;
        LockInvariantViolations.clearViolations();
        LockHoldDepth.reset();
    }

    @AfterEach
    public void tearDown() {
        Config.lock_blocking_call_validation_mode = savedMode;
        LockInvariantViolations.restoreBlockingCallTestEscalation();
        Config.lock_invariant_violation_log_interval_ms = savedLogInterval;
        LockInvariantViolations.clearViolations();
        LockHoldDepth.reset();
    }

    // --------------- the depth counter tracks the lock lifecycle ---------------

    @Test
    public void testDepthFollowsDatabaseLock() {
        Locker locker = new Locker();
        Assertions.assertEquals(0, LockHoldDepth.current());
        locker.lockDatabase(INTERNAL_DB_ID, LockType.READ);
        Assertions.assertEquals(1, LockHoldDepth.current());
        Assertions.assertTrue(LockHoldDepth.isUnderLock());
        locker.unLockDatabase(INTERNAL_DB_ID, LockType.READ);
        Assertions.assertEquals(0, LockHoldDepth.current());
        Assertions.assertFalse(LockHoldDepth.isUnderLock());
    }

    /**
     * The intensive-db-lock API takes an intention lock on the database plus one per table, and
     * holding an intention lock is holding a lock -- so the depth is 2, and more to the point it
     * returns to 0.
     */
    @Test
    public void testDepthFollowsIntensiveTableLock() {
        Locker locker = new Locker();
        locker.lockTableWithIntensiveDbLock(INTERNAL_DB_ID, INTERNAL_TABLE_ID, LockType.READ);
        Assertions.assertEquals(2, LockHoldDepth.current());
        locker.unLockTableWithIntensiveDbLock(INTERNAL_DB_ID, INTERNAL_TABLE_ID, LockType.READ);
        Assertions.assertEquals(0, LockHoldDepth.current());
    }

    @Test
    public void testDepthDoesNotGoNegativeOnAnExtraRelease() {
        Locker locker = new Locker();
        locker.lockDatabase(INTERNAL_DB_ID, LockType.READ);
        locker.unLockDatabase(INTERNAL_DB_ID, LockType.READ);
        // A second release is refused by the lock manager. The depth is already 0 and must stay
        // there: a negative depth would silence the check for the next lock this thread takes.
        Assertions.assertThrows(RuntimeException.class, () -> locker.release(INTERNAL_DB_ID, LockType.READ));
        Assertions.assertEquals(0, LockHoldDepth.current());
        locker.lockDatabase(INTERNAL_DB_ID, LockType.READ);
        Assertions.assertTrue(LockHoldDepth.isUnderLock());
        locker.unLockDatabase(INTERNAL_DB_ID, LockType.READ);
    }

    /**
     * A release that throws released nothing, so it must not lower the depth. Getting this wrong is
     * silent in the worst way: a thread still inside a critical section would be talked down to
     * depth 0 by bogus releases, and every remote call it then made under that lock would be waved
     * through as if no lock were held.
     */
    @Test
    public void testFailedReleaseLeavesTheDepthAloneWhileALockIsStillHeld() {
        Config.lock_blocking_call_validation_mode = "error";
        Locker locker = new Locker();
        locker.lockDatabase(INTERNAL_DB_ID, LockType.READ);
        try {
            // Wrong rid, then wrong lock type. Neither releases anything.
            Assertions.assertThrows(RuntimeException.class,
                    () -> locker.release(INTERNAL_DB_ID + 1, LockType.READ));
            Assertions.assertThrows(RuntimeException.class,
                    () -> locker.release(INTERNAL_DB_ID, LockType.WRITE));

            Assertions.assertEquals(1, LockHoldDepth.current());
            Assertions.assertThrows(IllegalStateException.class, FakeBlockingTransport::contact);
        } finally {
            locker.unLockDatabase(INTERNAL_DB_ID, LockType.READ);
        }
        Assertions.assertEquals(0, LockHoldDepth.current());
    }

    // --------------- the check itself ---------------

    @Test
    public void testContactingAnExternalSystemOutsideAnyLockIsFine() {
        Config.lock_blocking_call_validation_mode = "error";
        FakeBlockingTransport.contact();
        Assertions.assertEquals(0, LockInvariantViolations.totalViolations());
    }

    @Test
    public void testContactUnderLockIsRefusedInErrorMode() {
        Config.lock_blocking_call_validation_mode = "error";
        Locker locker = new Locker();
        locker.lockDatabase(INTERNAL_DB_ID, LockType.READ);
        try {
            IllegalStateException e =
                    Assertions.assertThrows(IllegalStateException.class, FakeBlockingTransport::contact);
            Assertions.assertTrue(e.getMessage().contains(FakeBlockingTransport.TAG), e.getMessage());
            Assertions.assertTrue(e.getMessage().contains("blocking_call_under_lock"), e.getMessage());
            Assertions.assertTrue(e.getMessage().contains("1 FE metadata lock(s)"), e.getMessage());
        } finally {
            locker.unLockDatabase(INTERNAL_DB_ID, LockType.READ);
        }
    }

    /** The reported depth is the real one, so a nested critical section is visible in the log. */
    @Test
    public void testReportCarriesTheLockDepth() {
        Config.lock_blocking_call_validation_mode = "error";
        Locker locker = new Locker();
        locker.lockTableWithIntensiveDbLock(INTERNAL_DB_ID, INTERNAL_TABLE_ID, LockType.READ);
        try {
            IllegalStateException e =
                    Assertions.assertThrows(IllegalStateException.class, FakeBlockingTransport::contact);
            Assertions.assertTrue(e.getMessage().contains("2 FE metadata lock(s)"), e.getMessage());
        } finally {
            locker.unLockTableWithIntensiveDbLock(INTERNAL_DB_ID, INTERNAL_TABLE_ID, LockType.READ);
        }
    }

    // --------------- which frame the report points at ---------------

    /**
     * The property the whole design rests on: the report must name the FE code that decided to go
     * remote while holding a lock, not the transport it went through. Naming the transport would be
     * useless -- it is the same frame every time and never the code that has to change -- and it
     * would also collapse every caller into one throttling bucket.
     * <p>
     * All four shapes are the transport's own layering: a plain call, one of its overloads, a
     * lambda inside it, and a nested class of it.
     */
    @Test
    public void testReportNamesTheCallerNotTheTransport() {
        Config.lock_blocking_call_validation_mode = "warn";
        LockInvariantViolations.suspendBlockingCallTestEscalation();
        Locker locker = new Locker();
        locker.lockDatabase(INTERNAL_DB_ID, LockType.READ);
        try {
            FakeBlockingTransport.contact();
            FakeBlockingTransport.contactThroughOwnOverload();
            FakeBlockingTransport.contactThroughOwnLambda();
            FakeBlockingTransport.contactThroughNestedClass();
        } finally {
            locker.unLockDatabase(INTERNAL_DB_ID, LockType.READ);
        }

        Map<String, Long> bySite = LockInvariantViolations.violationsBySite();
        Assertions.assertEquals(4, LockInvariantViolations.totalViolations());
        for (String site : bySite.keySet()) {
            Assertions.assertTrue(site.contains(BlockingCallValidatorTest.class.getName()),
                    "the report should name the caller, but named: " + site);
            Assertions.assertFalse(site.contains(FakeBlockingTransport.class.getSimpleName()),
                    "the report named the transport instead of its caller: " + site);
        }
    }

    // --------------- modes ---------------

    /**
     * The escalation follows its own property (on by default in {@code fe-core/pom.xml}). Written
     * against the property so it also holds when a run passes {@code -Dlock.blocking.strict=false}.
     */
    @Test
    public void testTestJvmEscalationFollowsItsOwnProperty() {
        boolean strict = Boolean.getBoolean("starrocks.lock.blocking.strict.in.test");
        Assertions.assertSame(strict ? Mode.ERROR : Mode.WARN,
                LockInvariantViolations.effectiveBlockingCallMode("warn"));
    }

    /**
     * A test of the warn path can get {@code warn} back, and only for itself: the escalation is
     * restored afterwards, and the lock-target rule is not touched by it.
     */
    @Test
    public void testSuspendingTheEscalationIsScopedToTheBlockingCallRule() {
        Mode lockTargetBefore = LockInvariantViolations.effectiveMode("warn");
        LockInvariantViolations.suspendBlockingCallTestEscalation();
        try {
            Assertions.assertSame(Mode.WARN, LockInvariantViolations.effectiveBlockingCallMode("warn"));
            Assertions.assertSame(lockTargetBefore, LockInvariantViolations.effectiveMode("warn"));
        } finally {
            LockInvariantViolations.restoreBlockingCallTestEscalation();
        }
        boolean strict = Boolean.getBoolean("starrocks.lock.blocking.strict.in.test");
        Assertions.assertSame(strict ? Mode.ERROR : Mode.WARN,
                LockInvariantViolations.effectiveBlockingCallMode("warn"));
    }

    @Test
    public void testOffAndErrorPassThroughTheTestEscalation() {
        Assertions.assertSame(Mode.OFF, LockInvariantViolations.effectiveBlockingCallMode("off"));
        Assertions.assertSame(Mode.ERROR, LockInvariantViolations.effectiveBlockingCallMode("error"));
    }

    /**
     * The shipped behaviour: the call goes through, and is counted. Counts are exact even though
     * logging is throttled, which is what makes "which sites, how often" answerable from a
     * production FE rather than from a reviewer's memory.
     */
    @Test
    public void testWarnModeCountsWithoutRefusing() {
        Config.lock_blocking_call_validation_mode = "warn";
        LockInvariantViolations.suspendBlockingCallTestEscalation();
        Locker locker = new Locker();
        locker.lockDatabase(INTERNAL_DB_ID, LockType.READ);
        try {
            FakeBlockingTransport.contact();
            FakeBlockingTransport.contact();
        } finally {
            locker.unLockDatabase(INTERNAL_DB_ID, LockType.READ);
        }
        Assertions.assertEquals(2, LockInvariantViolations.totalViolations());
        // Two different lines, so two call sites -- the throttle is per site, so a newly appearing
        // site always gets its first line printed.
        Assertions.assertEquals(2, LockInvariantViolations.violationsBySite().size());
    }

    @Test
    public void testOffModeChecksNothing() {
        Config.lock_blocking_call_validation_mode = "off";
        Locker locker = new Locker();
        locker.lockDatabase(INTERNAL_DB_ID, LockType.READ);
        try {
            FakeBlockingTransport.contact();
        } finally {
            locker.unLockDatabase(INTERNAL_DB_ID, LockType.READ);
        }
        Assertions.assertEquals(0, LockInvariantViolations.totalViolations());
    }

    @Test
    public void testDisableHelperOptsOut() {
        LockTestUtils.disableBlockingCallValidation();
        try {
            Locker locker = new Locker();
            locker.lockDatabase(INTERNAL_DB_ID, LockType.READ);
            try {
                FakeBlockingTransport.contact();
            } finally {
                locker.unLockDatabase(INTERNAL_DB_ID, LockType.READ);
            }
            Assertions.assertEquals(0, LockInvariantViolations.totalViolations());
        } finally {
            LockTestUtils.restoreBlockingCallValidation();
        }
    }

    // --------------- the attribution record the slow-lock trace reads ---------------

    /**
     * What LockManager's slow-lock trace turns into its {@code blockingCall} field. It has to be readable
     * from another thread -- the trace runs on a waiter and reports on the owners -- and it has to be gone
     * once the thread leaves its outermost critical section, so a pooled thread does not carry an old call
     * into the next lock it takes.
     */
    @Test
    public void testBlockingCallIsRecordedAndClearedOnRelease() {
        Config.lock_blocking_call_validation_mode = "warn";
        LockInvariantViolations.suspendBlockingCallTestEscalation();
        long threadId = Thread.currentThread().getId();
        Assertions.assertNull(BlockingCallUnderLock.of(threadId), "nothing recorded outside a critical section");

        long before = System.currentTimeMillis();
        Locker locker = new Locker();
        locker.lockDatabase(INTERNAL_DB_ID, LockType.READ);
        try {
            FakeBlockingTransport.contact();
            BlockingCallUnderLock.Record record = BlockingCallUnderLock.of(threadId);
            Assertions.assertNotNull(record);
            Assertions.assertEquals(FakeBlockingTransport.TAG, record.getTransport());
            Assertions.assertNull(record.getCatalog(), "no catalog was named, so none may be invented");
            Assertions.assertTrue(record.getStartTimeMs() >= before);
        } finally {
            locker.unLockDatabase(INTERNAL_DB_ID, LockType.READ);
        }

        Assertions.assertNull(BlockingCallUnderLock.of(threadId),
                "the record must not outlive the critical section it was made in");
    }

    @Test
    public void testCatalogIsRecordedWhenTheTransportKnowsIt() {
        Config.lock_blocking_call_validation_mode = "warn";
        LockInvariantViolations.suspendBlockingCallTestEscalation();
        long threadId = Thread.currentThread().getId();
        Locker locker = new Locker();
        locker.lockDatabase(INTERNAL_DB_ID, LockType.READ);
        try {
            FakeBlockingTransport.contact("hive_cat");
            Assertions.assertEquals("hive_cat", BlockingCallUnderLock.of(threadId).getCatalog());
        } finally {
            locker.unLockDatabase(INTERNAL_DB_ID, LockType.READ);
        }
    }

    /**
     * Attribution is not part of the check, so switching the check off must not take it away: the field is
     * what makes a slow-lock report nameable, and someone who silenced the warnings has if anything more
     * need of it.
     */
    @Test
    public void testRecordIsKeptEvenWhenTheCheckIsOff() {
        Config.lock_blocking_call_validation_mode = "off";
        long threadId = Thread.currentThread().getId();
        Locker locker = new Locker();
        locker.lockDatabase(INTERNAL_DB_ID, LockType.READ);
        try {
            FakeBlockingTransport.contact();
            Assertions.assertEquals(0, LockInvariantViolations.totalViolations());
            Assertions.assertNotNull(BlockingCallUnderLock.of(threadId));
        } finally {
            locker.unLockDatabase(INTERNAL_DB_ID, LockType.READ);
        }
    }

    @Test
    public void testNothingIsRecordedOutsideACriticalSection() {
        FakeBlockingTransport.contact("hive_cat");
        Assertions.assertNull(BlockingCallUnderLock.of(Thread.currentThread().getId()));
    }
}
