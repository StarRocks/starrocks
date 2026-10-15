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

package com.starrocks.sql;

import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.util.concurrent.lock.LockManager;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.PlannerMetaLocker;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Planning off private copies takes the copies in StatementPlanner while the meta lock is still held, which is
 * what makes them safe. But that lock is collected from the statement as parsed, where a view is a name and
 * nothing more: the tables it reads only appear once analysis expands it, so they used to be copied without
 * their lock. On a busy cluster that races INSERT OVERWRITE swapping partitions -- on the leader, or on a
 * follower's replayer -- and fails the query with a ConcurrentModificationException out of
 * {@link OlapTable#copyOnlyForQuery}.
 *
 * <p>The probe records, for every copy the planning thread takes, whether that thread holds the table's read
 * lock at that moment. Only the planning thread: the MV plan cache resolves the same tables on its own executor.
 *
 * <p>The DML statements read through a connector because that is what puts a DML on the snapshot path at all
 * ({@code AnalyzerUtils.CopyUnsafeTablesCollector#isCopySafe}); a purely local one plans with the lock held.
 */
public class PlanningSnapshotViewLockTest extends ConnectorPlanTestBase {
    private static final String CONNECTOR_TABLE = "hive0.partitioned_db.lineitem_par";
    private static final String VIEW = "test.v_snapshot_lock";
    private static final String NESTED_VIEW = "test.v_snapshot_lock_outer";

    private final Set<String> copied = new CopyOnWriteArraySet<>();
    private final List<String> copiedWithoutLock = new CopyOnWriteArrayList<>();

    @BeforeAll
    public static void createViews() throws Exception {
        starRocksAssert.withView("CREATE VIEW " + VIEW + " AS SELECT v1, v2, v3 FROM test.t0");
        starRocksAssert.withView("CREATE VIEW " + NESTED_VIEW + " AS SELECT v1, v2, v3 FROM " + VIEW);
    }

    @BeforeEach
    public void probeCopies() {
        copied.clear();
        copiedWithoutLock.clear();
        Thread planner = Thread.currentThread();
        LockManager lockManager = GlobalStateMgr.getCurrentState().getLockManager();
        new MockUp<OlapTable>() {
            @Mock
            public void copyOnlyForQuery(Invocation invocation, OlapTable copy) {
                OlapTable live = invocation.getInvokedInstance();
                if (Thread.currentThread() == planner) {
                    copied.add(live.getName());
                    if (!lockManager.isOwnedByCurrentThread(live.getId(), LockType.READ)) {
                        copiedWithoutLock.add(live.getName());
                    }
                }
                invocation.proceed(copy);
            }
        };
    }

    private void assertEveryCopyWasLocked(String sql) throws Exception {
        getExecPlan(sql);
        Assertions.assertTrue(copied.contains("t0"),
                "planning did not copy the view's base table, so this is not testing the snapshot path: " + sql);
        Assertions.assertTrue(copiedWithoutLock.isEmpty(),
                "copied without holding its read lock: " + copiedWithoutLock + ", for: " + sql);
        assertNothingLeftLocked(sql);
    }

    private void assertNothingLeftLocked(String sql) {
        LockManager lockManager = GlobalStateMgr.getCurrentState().getLockManager();
        for (String name : List.of("t0", "t1", "tprimary", "v_snapshot_lock", "v_snapshot_lock_outer")) {
            Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", name);
            Assertions.assertFalse(lockManager.isOwnedByCurrentThread(table.getId(), LockType.READ),
                    name + " is still read-locked after planning: " + sql);
        }
    }

    @Test
    public void testQueryOnAView() throws Exception {
        assertEveryCopyWasLocked("SELECT * FROM " + VIEW);
    }

    @Test
    public void testQueryOnAViewOfAView() throws Exception {
        assertEveryCopyWasLocked("SELECT * FROM " + NESTED_VIEW);
    }

    @Test
    public void testQueryJoiningATableAndAView() throws Exception {
        assertEveryCopyWasLocked("SELECT * FROM test.t1 JOIN " + VIEW + " v ON t1.v4 = v.v1");
    }

    @Test
    public void testInsertSelectFromAView() throws Exception {
        assertEveryCopyWasLocked("INSERT INTO test.t1 SELECT v.v1, v.v2, v.v3 FROM " + VIEW + " v "
                + "JOIN " + CONNECTOR_TABLE + " l ON v.v1 = l.l_orderkey");
    }

    @Test
    public void testUpdateFromAView() throws Exception {
        assertEveryCopyWasLocked("UPDATE test.tprimary SET v2 = v.v2 FROM " + VIEW + " v "
                + "JOIN " + CONNECTOR_TABLE + " l ON v.v1 = l.l_orderkey WHERE tprimary.pk = v.v1");
    }

    @Test
    public void testDeleteUsingAView() throws Exception {
        assertEveryCopyWasLocked("DELETE FROM test.tprimary USING " + VIEW + " v WHERE tprimary.pk = v.v1 "
                + "AND v.v1 IN (SELECT l_orderkey FROM " + CONNECTOR_TABLE + ")");
    }

    /** Counts the re-locks of the whole set, which only a table analysis brought in leads to. */
    private AtomicInteger countReLocks() {
        AtomicInteger reLocks = new AtomicInteger();
        new MockUp<PlannerMetaLocker>() {
            @Mock
            public boolean locksAll(Invocation invocation, Collection<? extends Table> boundTables) {
                reLocks.incrementAndGet();
                return invocation.proceed(boundTables);
            }
        };
        return reLocks;
    }

    /**
     * A table analysis brought in is not locked on top of the statement's own tables: everything is dropped and the
     * whole set taken again in one pass, which has to cover it too. The lock taken before analysis covers a view's
     * base tables, so pretend it covered nothing.
     */
    @Test
    public void testATableAnalysisBroughtInIsLockedByTakingTheWholeSetAgain() throws Exception {
        new MockUp<PlannerMetaLocker>() {
            @Mock
            public PlannerMetaLocker forTablesNotLockedOf(Invocation invocation, ConnectContext session,
                                                          StatementBase analyzedStatement) {
                PlannerMetaLocker locker = invocation.getInvokedInstance();
                return locker.forAllTablesOf(session, analyzedStatement);
            }
        };
        AtomicInteger reLocks = countReLocks();
        assertEveryCopyWasLocked("SELECT * FROM test.t1 JOIN " + VIEW + " v ON t1.v4 = v.v1");
        Assertions.assertEquals(1, reLocks.get());
    }

    /**
     * While nothing is held, a table the statement was bound to can be dropped or replaced, and the names it was
     * written with then resolve to something else. Copying the bound table would plan off metadata that is no
     * longer published, so the statement fails instead. Here the names resolve to t1 only, not to the bound t0.
     */
    @Test
    public void testATableReplacedWhileNothingWasHeldFailsTheStatement() throws Exception {
        StatementBase elsewhere = UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer("SELECT * FROM test.t1",
                connectContext);
        new MockUp<PlannerMetaLocker>() {
            @Mock
            public PlannerMetaLocker forAllTablesOf(Invocation invocation, ConnectContext session,
                                                    StatementBase analyzedStatement) {
                return invocation.proceed(session, elsewhere);
            }
        };
        Exception e = Assertions.assertThrows(Exception.class, () -> getExecPlan("SELECT * FROM " + VIEW));
        Assertions.assertTrue(e.getMessage().contains("dropped or replaced while it was being planned"),
                e.getMessage());
        Assertions.assertFalse(copied.contains("t0"), "copied a table it no longer holds the lock of");
        assertNothingLeftLocked("SELECT * FROM " + VIEW);
    }

    /**
     * The set is resolved by name before it is locked, so a table dropped or swapped in between leaves a stale id in
     * it, and locking that id says nothing about what the name stands for now. The names are resolved again once the
     * lock is held; here they have moved to t1 by then, away from the t0 the statement is bound to.
     */
    @Test
    public void testATableReplacedBeforeTheReLockTookHoldFailsTheStatement() throws Exception {
        StatementBase elsewhere = UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer("SELECT * FROM test.t1",
                connectContext);
        AtomicInteger resolutions = new AtomicInteger();
        new MockUp<PlannerMetaLocker>() {
            @Mock
            public PlannerMetaLocker forTablesNotLockedOf(Invocation invocation, ConnectContext session,
                                                          StatementBase analyzedStatement) {
                PlannerMetaLocker locker = invocation.getInvokedInstance();
                return locker.forAllTablesOf(session, analyzedStatement);
            }

            // 1: what forces the re-lock, 2: the set it locks, 3: the names resolved again under that lock
            @Mock
            public PlannerMetaLocker forAllTablesOf(Invocation invocation, ConnectContext session,
                                                    StatementBase analyzedStatement) {
                return invocation.proceed(session, resolutions.incrementAndGet() < 3 ? analyzedStatement : elsewhere);
            }
        };
        Exception e = Assertions.assertThrows(Exception.class, () -> getExecPlan("SELECT * FROM " + VIEW));
        Assertions.assertTrue(e.getMessage().contains("dropped or replaced while it was being planned"),
                e.getMessage());
        Assertions.assertEquals(3, resolutions.get());
        Assertions.assertFalse(copied.contains("t0"), "copied a table its name no longer stands for");
        assertNothingLeftLocked("SELECT * FROM " + VIEW);
    }

    /** A view does not take the whole set again: the lock taken before analysis already covers its base tables. */
    @Test
    public void testNoReLockForAView() throws Exception {
        AtomicInteger reLocks = countReLocks();
        assertEveryCopyWasLocked("SELECT * FROM " + NESTED_VIEW);
        assertEveryCopyWasLocked("SELECT * FROM test.t1 JOIN test.t0 ON t1.v4 = t0.v1");
        Assertions.assertEquals(0, reLocks.get());
    }
}
