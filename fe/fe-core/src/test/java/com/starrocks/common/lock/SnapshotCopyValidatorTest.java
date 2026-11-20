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

import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.util.UnitTestUtil;
import com.starrocks.common.util.concurrent.lock.LockInvariantViolationException;
import com.starrocks.common.util.concurrent.lock.LockInvariantViolations;
import com.starrocks.common.util.concurrent.lock.LockInvariantViolations.Mode;
import com.starrocks.common.util.concurrent.lock.LockManager;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.common.util.concurrent.lock.SnapshotCopyValidator;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.ast.KeysType;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class SnapshotCopyValidatorTest {
    private static final long DB_ID = 70001L;
    private static final long TABLE_ID = 70002L;
    private static final long OTHER_TABLE_ID = 70010L;

    private String savedMode;
    private Database db;
    private OlapTable table;

    @BeforeEach
    public void setUp() {
        GlobalStateMgr.getCurrentState().setLockManager(new LockManager());
        savedMode = Config.lock_snapshot_copy_validation_mode;
        // Explicit, so the refusal tests do not depend on the surefire escalation being armed.
        Config.lock_snapshot_copy_validation_mode = "error";
        LockInvariantViolations.clearViolations();

        db = UnitTestUtil.createDb(DB_ID, TABLE_ID, 70003L, 70004L, 70005L, 70006L, 1L, KeysType.DUP_KEYS);
        table = (OlapTable) db.getTable(TABLE_ID);
        GlobalStateMgr.getCurrentState().getLocalMetastore().getIdToDb().put(DB_ID, db);
    }

    @AfterEach
    public void tearDown() {
        GlobalStateMgr.getCurrentState().getLocalMetastore().getIdToDb().remove(DB_ID);
        Config.lock_snapshot_copy_validation_mode = savedMode;
        LockInvariantViolations.restoreSnapshotCopyTestEscalation();
        LockInvariantViolations.clearViolations();
    }

    private void copyIsRefused() {
        LockInvariantViolationException e = Assertions.assertThrows(LockInvariantViolationException.class,
                () -> table.copyOnlyForQuery(new OlapTable()));
        Assertions.assertTrue(e.getMessage().contains("kind=snapshot_copy_without_lock"), e.getMessage());
        Assertions.assertTrue(e.getMessage().contains("id=" + TABLE_ID), e.getMessage());
    }

    private void copyIsAllowed() {
        OlapTable copy = new OlapTable();
        table.copyOnlyForQuery(copy);
        Assertions.assertTrue(copy.isPlanningSnapshot());
        Assertions.assertFalse(table.isPlanningSnapshot());
        Assertions.assertEquals(0, LockInvariantViolations.totalViolations());
    }

    @Test
    public void testCopyWithoutAnyLockIsRefused() {
        copyIsRefused();
    }

    @Test
    public void testTableReadLockAllowsTheCopy() {
        Locker locker = new Locker();
        locker.lockTableWithIntensiveDbLock(DB_ID, TABLE_ID, LockType.READ);
        try {
            copyIsAllowed();
        } finally {
            locker.unLockTableWithIntensiveDbLock(DB_ID, TABLE_ID, LockType.READ);
        }
    }

    @Test
    public void testTableWriteLockAllowsTheCopy() {
        Locker locker = new Locker();
        locker.lockTableWithIntensiveDbLock(DB_ID, TABLE_ID, LockType.WRITE);
        try {
            copyIsAllowed();
        } finally {
            locker.unLockTableWithIntensiveDbLock(DB_ID, TABLE_ID, LockType.WRITE);
        }
    }

    @Test
    public void testDatabaseLockAllowsTheCopy() {
        for (LockType lockType : List.of(LockType.READ, LockType.WRITE)) {
            Locker locker = new Locker();
            locker.lockDatabase(DB_ID, lockType);
            try {
                copyIsAllowed();
            } finally {
                locker.unLockDatabase(DB_ID, lockType);
            }
        }
    }

    /**
     * The intensive path takes IS on the database next to the table lock it actually needs. Holding it for another
     * table of the same database excludes no writer of this one.
     */
    @Test
    public void testAnotherTablesLockIsNotThisTablesLock() {
        Locker locker = new Locker();
        locker.lockTableWithIntensiveDbLock(DB_ID, OTHER_TABLE_ID, LockType.READ);
        try {
            copyIsRefused();
        } finally {
            locker.unLockTableWithIntensiveDbLock(DB_ID, OTHER_TABLE_ID, LockType.READ);
        }
    }

    @Test
    public void testLockHeldByAnotherThreadDoesNotCount() throws Exception {
        Locker locker = new Locker();
        locker.lockTableWithIntensiveDbLock(DB_ID, TABLE_ID, LockType.READ);
        try {
            Throwable failure = CompletableFuture.supplyAsync(() -> {
                try {
                    table.copyOnlyForQuery(new OlapTable());
                    return null;
                } catch (Throwable t) {
                    return t;
                }
            }).get();
            Assertions.assertInstanceOf(LockInvariantViolationException.class, failure);
        } finally {
            locker.unLockTableWithIntensiveDbLock(DB_ID, TABLE_ID, LockType.READ);
        }
    }

    @Test
    public void testCopyOfACopyIsPrivate() {
        OlapTable copy;
        Locker locker = new Locker();
        locker.lockTableWithIntensiveDbLock(DB_ID, TABLE_ID, LockType.READ);
        try {
            copy = new OlapTable();
            table.copyOnlyForQuery(copy);
        } finally {
            locker.unLockTableWithIntensiveDbLock(DB_ID, TABLE_ID, LockType.READ);
        }
        // Same id as the published table, and the metastore no longer returns this object -- neither says private.
        OlapTable copyOfCopy = new OlapTable();
        copy.copyOnlyForQuery(copyOfCopy);
        Assertions.assertTrue(copyOfCopy.isPlanningSnapshot());
    }

    @Test
    public void testUnregisteredTableIsNotChecked() {
        GlobalStateMgr.getCurrentState().getLocalMetastore().getIdToDb().remove(DB_ID);
        copyIsAllowed();
    }

    /**
     * Matched by id: a copier still holding an object whose id now names a different one -- replaced by a swap, or
     * a statement bound before a DROP + recreate under the same id -- reads what that id's lock protects.
     */
    @Test
    public void testReplacedObjectUnderTheSameIdIsStillChecked() {
        OlapTable stale = UnitTestUtil.createOlapTable(DB_ID, TABLE_ID, 70013L, 70014L, 70015L, 70006L, 1L,
                KeysType.DUP_KEYS, Table.TableType.OLAP);
        Assertions.assertNotSame(stale, db.getTable(TABLE_ID));
        Assertions.assertThrows(LockInvariantViolationException.class, () -> stale.copyOnlyForQuery(new OlapTable()));
    }

    /** ExternalOlapTable and the like: no lock protects them, so there is no lock to have held. */
    @Test
    public void testTableThatIsNotALockTargetIsNotChecked() {
        new MockUp<OlapTable>() {
            @Mock
            public boolean isMetaLockTarget() {
                return false;
            }
        };
        copyIsAllowed();
    }

    /** A known violation is exempt inside its scope only, and nesting restores the outer scope. */
    @Test
    public void testKnownViolationIsExemptOnlyInsideItsScope() {
        try (SnapshotCopyValidator.KnownViolation outer = SnapshotCopyValidator.exemptKnownViolation("outer")) {
            try (SnapshotCopyValidator.KnownViolation inner = SnapshotCopyValidator.exemptKnownViolation("inner")) {
                copyIsAllowed();
            }
            copyIsAllowed();
        }
        copyIsRefused();
    }

    /** An exempted copy is settled before the check looks anything up. */
    @Test
    public void testKnownViolationSkipsTheLookups() {
        AtomicInteger lookups = new AtomicInteger();
        new MockUp<LockManager>() {
            @Mock
            public boolean isReadOrWriteOwnedByCurrentThread(Invocation invocation, long rid) {
                lookups.incrementAndGet();
                return invocation.proceed();
            }
        };
        new MockUp<LocalMetastore>() {
            @Mock
            public ConcurrentHashMap<Long, Database> getIdToDb(Invocation invocation) {
                lookups.incrementAndGet();
                return invocation.proceed();
            }
        };
        Locker locker = new Locker();
        locker.lockDatabase(DB_ID, LockType.INTENTION_SHARED);
        try (SnapshotCopyValidator.KnownViolation ignored = SnapshotCopyValidator.exemptKnownViolation("test")) {
            copyIsAllowed();
        } finally {
            locker.unLockDatabase(DB_ID, LockType.INTENTION_SHARED);
        }
        Assertions.assertEquals(0, lookups.get());
    }

    /**
     * A copier holding only the database lock finds the database by the id the analyzer recorded on the table, not
     * by scanning every database; a missing or stale recorded id falls back to the scan.
     */
    @Test
    public void testDatabaseIsFoundByTheRecordedId() {
        AtomicInteger scans = new AtomicInteger();
        new MockUp<LocalMetastore>() {
            @Mock
            public ConcurrentHashMap<Long, Database> getIdToDb(Invocation invocation) {
                scans.incrementAndGet();
                return invocation.proceed();
            }
        };
        Locker locker = new Locker();
        locker.lockDatabase(DB_ID, LockType.READ);
        try {
            Assertions.assertNull(table.dbId);
            copyIsAllowed();
            Assertions.assertEquals(1, scans.get());

            table.dbId = DB_ID + 100;
            copyIsAllowed();
            Assertions.assertEquals(2, scans.get());

            table.dbId = DB_ID;
            copyIsAllowed();
            Assertions.assertEquals(2, scans.get());
        } finally {
            locker.unLockDatabase(DB_ID, LockType.READ);
        }
        // Found by the recorded id, the database is still the one whose lock must be held.
        copyIsRefused();
    }

    @Test
    public void testOffSkipsTheCheck() {
        Config.lock_snapshot_copy_validation_mode = "off";
        copyIsAllowed();
    }

    /** warn counts the violation, names the code that copied rather than the copy itself, and lets the copy run. */
    @Test
    public void testWarnCountsAndProceeds() {
        Config.lock_snapshot_copy_validation_mode = "warn";
        LockInvariantViolations.suspendSnapshotCopyTestEscalation();
        Assertions.assertSame(Mode.WARN, LockInvariantViolations.currentSnapshotCopyMode());

        OlapTable copy = new OlapTable();
        table.copyOnlyForQuery(copy);

        Assertions.assertEquals(TABLE_ID, copy.getId());
        Assertions.assertEquals(1, LockInvariantViolations.totalViolations());
        Map<String, Long> bySite = LockInvariantViolations.violationsBySite();
        String site = bySite.keySet().iterator().next();
        Assertions.assertTrue(site.startsWith("snapshot_copy_without_lock@" + getClass().getName()
                + ".testWarnCountsAndProceeds:"), site);
    }

    /** Draining takes each site's count once and leaves the site at zero, for the test hook in LockTestUtils. */
    @Test
    public void testDrainTakesEachViolationOnce() {
        Config.lock_snapshot_copy_validation_mode = "warn";
        LockInvariantViolations.suspendSnapshotCopyTestEscalation();
        // One call site, so both land on the same site's count.
        for (int i = 0; i < 2; i++) {
            table.copyOnlyForQuery(new OlapTable());
        }

        Map<String, Long> drained = LockInvariantViolations.drainViolations();
        Assertions.assertEquals(1, drained.size(), drained.toString());
        Assertions.assertEquals(2L, drained.values().iterator().next());
        Assertions.assertTrue(LockInvariantViolations.drainViolations().isEmpty());
        Assertions.assertEquals(0, LockInvariantViolations.totalViolations());
    }

    /** Guards the surefire wiring in fe-core/pom.xml, as LockTargetValidatorTest does for its own rule. */
    @Test
    public void testWarnIsEscalatedInsideTheTestJvm() {
        boolean strict = Boolean.getBoolean("starrocks.lock.snapshot.strict.in.test");
        Assertions.assertSame(strict ? Mode.ERROR : Mode.WARN, LockInvariantViolations.effectiveSnapshotCopyMode("warn"));
    }
}
