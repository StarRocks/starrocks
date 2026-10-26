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

package com.starrocks.sql.spm;

import com.starrocks.catalog.Table;
import com.starrocks.common.util.concurrent.lock.LockManager;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.spm.CreateBaselinePlanStmt;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.rewrite.OptOlapPartitionPruner;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.PlanTestBase;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * CREATE BASELINE runs the whole optimizer on the statement it analyzed. Analysis holds the meta lock, planning
 * used not to and ran on the live tables, so partition pruning iterated partition maps a concurrent ADD / DROP
 * PARTITION or INSERT OVERWRITE was rewriting. It now plans off private copies, taken under the lock.
 *
 * <p>The probe records the table every olap scan the planning thread prunes is bound to. Only the planning
 * thread: the MV plan cache resolves the same tables on its own executor.
 */
public class SPMPlanBuilderLockTest extends PlanTestBase {
    private static final String VIEW = "test.v_spm_plan_builder_lock";

    private final Map<String, Table> pruned = new ConcurrentHashMap<>();

    @BeforeAll
    public static void createView() throws Exception {
        starRocksAssert.withView("CREATE VIEW " + VIEW + " AS SELECT v1, v2, v3 FROM test.t0");
    }

    @BeforeEach
    public void probePruning() {
        pruned.clear();
        Thread planner = Thread.currentThread();
        new MockUp<OptOlapPartitionPruner>() {
            @Mock
            public LogicalOlapScanOperator prunePartitions(Invocation invocation, LogicalOlapScanOperator scan) {
                if (Thread.currentThread() == planner) {
                    pruned.put(scan.getTable().getName(), scan.getTable());
                }
                return invocation.proceed(scan);
            }
        };
    }

    private void assertPlannedOffCopies(String sql, String... tables) {
        List<StatementBase> statements = SqlParser.parse("create baseline using " + sql,
                connectContext.getSessionVariable());
        BaselinePlan plan = SPMStmtExecutor.execute(connectContext, (CreateBaselinePlanStmt) statements.get(0));
        try {
            LockManager lockManager = GlobalStateMgr.getCurrentState().getLockManager();
            for (String name : tables) {
                Table live = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", name);
                Assertions.assertTrue(pruned.containsKey(name),
                        "planning did not prune " + name + ", so this is not testing it: " + sql);
                Assertions.assertNotSame(live, pruned.get(name),
                        "the baseline was planned on the live " + name + ", not on a copy: " + sql);
                Assertions.assertFalse(lockManager.isOwnedByCurrentThread(live.getId(), LockType.READ),
                        name + " is still read-locked after CREATE BASELINE: " + sql);
            }
        } finally {
            connectContext.getSqlPlanStorage().dropBaselinePlan(List.of(plan.getId()));
        }
    }

    @Test
    public void testBaselineIsPlannedOffCopies() {
        assertPlannedOffCopies("select * from t0 join t1 on t0.v1 = t1.v4 where t0.v2 = 1", "t0", "t1");
    }

    @Test
    public void testBaselineOverAViewIsPlannedOffCopies() {
        assertPlannedOffCopies("select * from " + VIEW + " where v1 = 1", "t0");
    }
}
