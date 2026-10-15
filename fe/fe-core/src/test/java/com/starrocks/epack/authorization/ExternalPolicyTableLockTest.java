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

package com.starrocks.epack.authorization;

import com.starrocks.catalog.Table;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.common.util.concurrent.lock.LockManager;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.ast.AstTraverser;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.rewrite.OptOlapPartitionPruner;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * A policy on an external table may read an internal one -- a mapping table. The meta lock is collected before
 * the external table is resolved, so its policies are not known then; on the paths that keep the lock to the end
 * the mapping table was planned on without its lock. The unlocked pre-pass resolves the external table, and the
 * tables its policies read join the lock set before the lock is taken.
 */
public class ExternalPolicyTableLockTest extends ConnectorPlanTestBase {
    private final Set<String> pruned = new CopyOnWriteArraySet<>();
    private final List<String> prunedWithoutLock = new CopyOnWriteArrayList<>();
    private boolean cboUseDBLock;

    @BeforeAll
    public static void beforeClass() throws Exception {
        ConnectorPlanTestBase.beforeClass();
        starRocksAssert.withDatabase("test_ext_policy_map").withTable("CREATE TABLE test_ext_policy_map.t_map "
                + "(k INT) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        execute("CREATE ROW ACCESS POLICY test.rp_ext_lock AS (x INT) RETURNS BOOLEAN -> "
                + "EXISTS (SELECT 1 FROM test_ext_policy_map.t_map m WHERE m.k = x)");
        execute("ALTER TABLE hive0.tpch.region ADD ROW ACCESS POLICY test.rp_ext_lock ON (r_regionkey)");
        starRocksAssert.useDatabase("test").withView("CREATE VIEW test.v_ext_region AS SELECT * FROM hive0.tpch.region");
    }

    @AfterAll
    public static void dropThePolicy() throws Exception {
        execute("ALTER TABLE hive0.tpch.region DROP ALL ROW ACCESS POLICIES");
        execute("DROP ROW ACCESS POLICY test.rp_ext_lock");
    }

    private static void execute(String sql) throws Exception {
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(sql, connectContext), connectContext);
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

    /** Plans the way a client statement is planned: ConnectProcessor marks every relation for the policy rewrite. */
    private static void planWithPolicies(String sql) throws Exception {
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        connectContext.setThreadLocalInfo();
        StatementBase stmt = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        new AstTraverser<Void, Void>() {
            @Override
            public Void visitRelation(Relation relation, Void context) {
                relation.setNeedRewrittenByPolicy(true);
                return null;
            }
        }.visit(stmt);
        StatementPlanner.plan(stmt, connectContext);
    }

    @Test
    public void testTableReadByAnExternalTablePolicyIsLocked() throws Exception {
        assertMapTablePlannedUnderTheLock("SELECT * FROM test.t0 JOIN hive0.tpch.region ON t0.v1 = region.r_regionkey");
    }

    /** No internal table of its own: the pre-pass runs all the same, because the external table may have a policy. */
    @Test
    public void testTableReadByThePolicyOfTheOnlyTableIsLocked() throws Exception {
        assertMapTablePlannedUnderTheLock("SELECT * FROM hive0.tpch.region");
    }

    /** Behind an internal view, the external table is found in the body the pre-pass resolved for the view. */
    @Test
    public void testTableReadByThePolicyOfAnExternalTableBehindAViewIsLocked() throws Exception {
        assertMapTablePlannedUnderTheLock("SELECT * FROM test.t0 JOIN test.v_ext_region v ON t0.v1 = v.r_regionkey");
    }

    @Test
    public void testTableReadByThePolicyOfAnExternalTableBehindAViewIsLockedForALocalInsert() throws Exception {
        assertMapTablePlannedUnderTheLock(
                "INSERT INTO test.t0 SELECT r_regionkey, r_regionkey, r_regionkey FROM test.v_ext_region");
    }

    private void assertMapTablePlannedUnderTheLock(String sql) throws Exception {
        connectContext.getSessionVariable().setCboUseDBLock(true);
        planWithPolicies(sql);
        Assertions.assertTrue(pruned.contains("t_map"),
                "planning did not prune the policy's mapping table, so this is not testing it: " + sql);
        Assertions.assertTrue(prunedWithoutLock.isEmpty(), "planned on without holding its read lock: "
                + prunedWithoutLock + ", for: " + sql);
        Table map = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test_ext_policy_map", "t_map");
        Assertions.assertFalse(GlobalStateMgr.getCurrentState().getLockManager()
                .isOwnedByCurrentThread(map.getId(), LockType.READ), "t_map is still read-locked after planning");
    }
}
