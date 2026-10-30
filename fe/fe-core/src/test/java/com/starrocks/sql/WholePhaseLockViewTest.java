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

import com.starrocks.catalog.Table;
import com.starrocks.catalog.View;
import com.starrocks.common.util.concurrent.lock.LockManager;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.PlannerMetaLocker;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.rewrite.OptOlapPartitionPruner;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.PlanTestBase;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * When planning does not run off copies -- a purely local INSERT / UPDATE / DELETE, or {@code cbo_use_lock_db} --
 * it keeps the meta lock to the end and reads the live tables under it. That lock is collected from the statement
 * as parsed, and it used to stop at a view's name, so the tables the view reads were planned on without their
 * lock: partition pruning, statistics and scan ranges iterated partition maps that a concurrent INSERT OVERWRITE or
 * partition change was rewriting. The collector now walks the view's body.
 *
 * <p>The probe records, for every olap scan the planning thread prunes, whether that thread holds the table's read
 * lock at that moment. Only the planning thread: the MV plan cache resolves the same tables on its own executor.
 */
public class WholePhaseLockViewTest extends PlanTestBase {
    private static final String VIEW = "test.v_whole_phase_lock";
    private static final String NESTED_VIEW = "test.v_whole_phase_lock_outer";
    private static final String OTHER_DB_VIEW = "test.v_whole_phase_lock_other_db";

    private final Set<String> pruned = new CopyOnWriteArraySet<>();
    private final List<String> prunedWithoutLock = new CopyOnWriteArrayList<>();
    private boolean cboUseDBLock;

    @BeforeAll
    public static void createViews() throws Exception {
        starRocksAssert.withView("CREATE VIEW " + VIEW + " AS SELECT v1, v2, v3 FROM test.t0");
        starRocksAssert.withView("CREATE VIEW " + NESTED_VIEW + " AS SELECT v1, v2, v3 FROM " + VIEW);
        starRocksAssert.withDatabase("test_whole_phase_lock_other").withTable(
                "CREATE TABLE test_whole_phase_lock_other.t_other (k1 INT) DUPLICATE KEY(k1) "
                        + "DISTRIBUTED BY HASH(k1) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        starRocksAssert.useDatabase("test")
                .withView("CREATE VIEW " + OTHER_DB_VIEW + " AS SELECT k1 FROM test_whole_phase_lock_other.t_other");
    }

    @BeforeEach
    public void probePruning() {
        cboUseDBLock = connectContext.getSessionVariable().isCboUseDBLock();
        pruned.clear();
        prunedWithoutLock.clear();
        Thread planner = Thread.currentThread();
        LockManager lockManager = GlobalStateMgr.getCurrentState().getLockManager();
        new MockUp<OptOlapPartitionPruner>() {
            @Mock
            public LogicalOlapScanOperator prunePartitions(Invocation invocation, LogicalOlapScanOperator scan) {
                Table table = scan.getTable();
                if (Thread.currentThread() == planner) {
                    pruned.add(table.getName());
                    if (!lockManager.isOwnedByCurrentThread(table.getId(), LockType.READ)) {
                        prunedWithoutLock.add(table.getName());
                    }
                }
                return invocation.proceed(scan);
            }
        };
    }

    @AfterEach
    public void restoreSession() {
        connectContext.getSessionVariable().setCboUseDBLock(cboUseDBLock);
    }

    private void assertPlannedUnderTheLock(String sql) throws Exception {
        getExecPlan(sql);
        Assertions.assertTrue(pruned.contains("t0"),
                "planning did not prune the view's base table, so this is not testing it: " + sql);
        Assertions.assertTrue(prunedWithoutLock.isEmpty(),
                "planned on without holding its read lock: " + prunedWithoutLock + ", for: " + sql);
        assertNothingLeftLocked(sql);
    }

    private void assertNothingLeftLocked(String sql) {
        LockManager lockManager = GlobalStateMgr.getCurrentState().getLockManager();
        for (String name : List.of("t0", "t1", "tprimary", "v_whole_phase_lock", "v_whole_phase_lock_outer")) {
            Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", name);
            Assertions.assertFalse(lockManager.isOwnedByCurrentThread(table.getId(), LockType.READ),
                    name + " is still read-locked after planning: " + sql);
        }
    }

    @Test
    public void testInsertSelectFromAView() throws Exception {
        assertPlannedUnderTheLock("INSERT INTO test.t1 SELECT v1, v2, v3 FROM " + VIEW);
    }

    @Test
    public void testUpdateFromAView() throws Exception {
        assertPlannedUnderTheLock("UPDATE test.tprimary SET v2 = v.v2 FROM " + VIEW + " v WHERE tprimary.pk = v.v1");
    }

    @Test
    public void testDeleteUsingAView() throws Exception {
        assertPlannedUnderTheLock("DELETE FROM test.tprimary USING " + VIEW + " v WHERE tprimary.pk = v.v1");
    }

    @Test
    public void testQueryOnAViewWithTheDbLock() throws Exception {
        connectContext.getSessionVariable().setCboUseDBLock(true);
        assertPlannedUnderTheLock("SELECT * FROM test.t1 JOIN " + VIEW + " v ON t1.v4 = v.v1");
    }

    @Test
    public void testQueryOnAViewOfAViewWithTheDbLock() throws Exception {
        connectContext.getSessionVariable().setCboUseDBLock(true);
        assertPlannedUnderTheLock("SELECT * FROM " + NESTED_VIEW);
    }

    /**
     * A view's base tables are locked, but they do not join the databases the statement names: those classify
     * the query (resource groups and the like) and stay what the user wrote.
     */
    @Test
    public void testAViewOverAnotherDatabaseLeavesTheStatementDatabasesAlone() throws Exception {
        connectContext.getSessionVariable().setCboUseDBLock(true);
        getExecPlan("SELECT * FROM " + OTHER_DB_VIEW);
        Assertions.assertTrue(pruned.contains("t_other"), "planning did not prune the view's base table");
        Assertions.assertTrue(prunedWithoutLock.isEmpty(), "planned on without holding its read lock: "
                + prunedWithoutLock);
        long testDbId = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test").getId();
        Assertions.assertEquals(Set.of(testDbId), connectContext.getCurrentSqlDbIds());
    }

    private static final String REDEFINED_VIEW_SQL = "SELECT `v4` AS `v1`, `v5` AS `v2`, `v6` AS `v3` FROM `test`.`t1`";

    /**
     * A view's body is walked before the lock is taken. Redefined in between, the analyzer would expand it to tables
     * the lock does not hold, here t1 in place of t0; the statement fails instead, to be retried.
     */
    @Test
    public void testAViewRedefinedBeforeTheLockFailsTheStatement() throws Exception {
        connectContext.getSessionVariable().setCboUseDBLock(true);
        View view = (View) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "v_whole_phase_lock");
        String definition = view.getInlineViewDef();
        long sqlMode = view.getSqlMode();
        AtomicBoolean redefined = new AtomicBoolean();
        new MockUp<PlannerMetaLocker>() {
            @Mock
            public void lock(Invocation invocation) {
                if (redefined.compareAndSet(false, true)) {
                    view.setInlineViewDefWithSqlMode(REDEFINED_VIEW_SQL, sqlMode);
                }
                invocation.proceed();
            }
        };
        try {
            Exception e = Assertions.assertThrows(Exception.class, () -> getExecPlan("SELECT * FROM " + VIEW));
            Assertions.assertTrue(e.getMessage().contains("was changed while the statement was being planned"),
                    e.getMessage());
            Assertions.assertFalse(pruned.contains("t1"), "planned on the redefined view's table");
            assertNothingLeftLocked("SELECT * FROM " + VIEW);
        } finally {
            view.setInlineViewDefWithSqlMode(definition, sqlMode);
        }
    }

    /**
     * The check is the locker's, not the planner's: SPM, MV refresh and recursive CTEs take the lock through it
     * directly. A failed check leaves nothing locked, and a later acquisition does not check again.
     */
    @Test
    public void testTheLockerChecksTheWalkedViewsOnItsFirstAcquisition() throws Exception {
        View view = (View) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "v_whole_phase_lock");
        String definition = view.getInlineViewDef();
        long sqlMode = view.getSqlMode();
        try (PlannerMetaLocker locker = new PlannerMetaLocker(connectContext,
                SqlParser.parseSingleStatement("SELECT * FROM " + NESTED_VIEW, sqlMode))) {
            view.setInlineViewDefWithSqlMode(REDEFINED_VIEW_SQL, sqlMode);
            Exception e = Assertions.assertThrows(Exception.class, locker::lock);
            Assertions.assertTrue(e.getMessage().contains("v_whole_phase_lock"), e.getMessage());
            assertNothingLeftLocked("the direct acquisition");
        } finally {
            view.setInlineViewDefWithSqlMode(definition, sqlMode);
        }
        try (PlannerMetaLocker unchanged = new PlannerMetaLocker(connectContext,
                SqlParser.parseSingleStatement("SELECT * FROM " + NESTED_VIEW, sqlMode))) {
            unchanged.lock();
            unchanged.unlock();
            view.setInlineViewDefWithSqlMode(REDEFINED_VIEW_SQL, sqlMode);
            unchanged.lock();
            unchanged.unlock();
            assertNothingLeftLocked("the later acquisition");
        } finally {
            view.setInlineViewDefWithSqlMode(definition, sqlMode);
        }
    }
}
