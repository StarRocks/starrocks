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
     * This rule is deliberately not escalated in CI yet: it is still collecting its violation set,
     * and refusing violations would fail whichever tests happen to cover a site rather than the
     * site itself.
     * <p>
     * The assertion is written against the property rather than against {@code WARN} so that the
     * day someone sets it in {@code fe-core/pom.xml}, this test does not have to be edited to
     * agree -- it already describes the wiring, not the current answer.
     */
    @Test
    public void testTestJvmEscalationFollowsItsOwnProperty() {
        boolean strict = Boolean.getBoolean("starrocks.lock.blocking.strict.in.test");
        Assertions.assertSame(strict ? Mode.ERROR : Mode.WARN,
                LockInvariantViolations.effectiveBlockingCallMode("warn"));
        // Independent of the lock-target rule, which *is* escalated. Sharing one property would
        // arm this one by accident.
        Assertions.assertFalse(strict, "the blocking-call rule is not armed in CI yet; if this is now "
                + "intentional, remove this assertion in the same change that sets the property, and say "
                + "which sites were fixed to make it possible");
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
}
