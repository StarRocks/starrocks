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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.common.util.concurrent.lock.LockManager;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.AstTraverser;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.rewrite.OptOlapPartitionPruner;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TExplainLevel;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * A row access or column masking policy is injected by the analyzer as a subquery over the relation it guards, and
 * its body may read other tables -- a mapping table, typically. The meta lock is collected from the statement as
 * parsed, where nothing of that shows, so on the paths that keep the lock to the end and read the live tables under
 * it ({@code cbo_use_lock_db}, a local INSERT) the mapping table was planned on without its lock. The collector now
 * walks the policy bodies of the relations the analyzer will apply them to.
 */
public class WholePhaseLockPolicyTest extends PlanTestBase {
    private static final String MAP_DB = "test_policy_lock_map";
    private static final String GUARDED = "test.t_policy_lock";
    private static final String VIEW = "test.v_policy_lock";

    private final Set<String> pruned = new CopyOnWriteArraySet<>();
    private final List<String> prunedWithoutLock = new CopyOnWriteArrayList<>();
    private boolean cboUseDBLock;

    @BeforeAll
    public static void createTables() throws Exception {
        starRocksAssert.withDatabase(MAP_DB).withTable("CREATE TABLE " + MAP_DB + ".t_map (k BIGINT) "
                + "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        starRocksAssert.useDatabase("test").withTable("CREATE TABLE " + GUARDED + " (k1 BIGINT, v1 BIGINT) "
                + "DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        starRocksAssert.withView("CREATE VIEW " + VIEW + " AS SELECT k1, v1 FROM " + GUARDED);
    }

    @BeforeEach
    public void installPolicy() {
        // A row access policy on the guarded table that reads the mapping table, as an access controller returns it.
        new MockUp<Authorizer>() {
            @Mock
            public Expr getRowAccessPolicy(ConnectContext context, TableName tableName) {
                if (!"t_policy_lock".equalsIgnoreCase(tableName.getTbl())) {
                    return null;
                }
                return SqlParser.parseSqlToExpr("EXISTS (SELECT 1 FROM " + MAP_DB + ".t_map m WHERE m.k = k1)",
                        context.getSessionVariable().getSqlMode());
            }

            @Mock
            public Map<String, Expr> getColumnMaskingPolicy(ConnectContext context, TableName tableName,
                                                                   List<Column> columns) {
                return null;
            }
        };
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
    private static String planWithPolicies(String sql) throws Exception {
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
        ExecPlan execPlan = StatementPlanner.plan(stmt, connectContext);
        return execPlan.getExplainString(TExplainLevel.NORMAL);
    }

    private void assertPolicyTablePlannedUnderTheLock(String sql) throws Exception {
        connectContext.getSessionVariable().setCboUseDBLock(true);
        planWithPolicies(sql);
        Assertions.assertTrue(pruned.contains("t_map"),
                "planning did not prune the policy's mapping table, so this is not testing it: " + sql);
        Assertions.assertTrue(prunedWithoutLock.isEmpty(),
                "planned on without holding its read lock: " + prunedWithoutLock + ", for: " + sql);
        Table map = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(MAP_DB, "t_map");
        Assertions.assertFalse(GlobalStateMgr.getCurrentState().getLockManager()
                .isOwnedByCurrentThread(map.getId(), LockType.READ), "t_map is still read-locked after planning");
        // Locked, but not something the statement names.
        Assertions.assertEquals(Set.of(GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test").getId()),
                connectContext.getCurrentSqlDbIds());
    }

    @Test
    public void testRowAccessPolicyTableIsLocked() throws Exception {
        assertPolicyTablePlannedUnderTheLock("SELECT * FROM " + GUARDED);
    }

    /** The analyzer applies the policy to the relations of a view's body too, as the view inherits the flag. */
    @Test
    public void testRowAccessPolicyTableBehindAViewIsLocked() throws Exception {
        assertPolicyTablePlannedUnderTheLock("SELECT * FROM " + VIEW);
    }

    @Test
    public void testRowAccessPolicyTableIsLockedForALocalInsert() throws Exception {
        assertPolicyTablePlannedUnderTheLock("INSERT INTO test.t0 SELECT k1, v1, v1 FROM " + GUARDED);
    }

    /**
     * Off the copies, the statement is walked again under the lock once analyzed, for what analysis brought in. The
     * policies it applied are in it by then, so the walk reaches the mapping table without asking the access
     * controller again -- which, through Ranger, costs a lookup per column of every table.
     */
    @Test
    public void testPoliciesAreLookedUpOnlyBeforeAnalysisAndByTheAnalyzer() throws Exception {
        Thread planner = Thread.currentThread();
        Map<String, Integer> lookups = new ConcurrentHashMap<>();
        new MockUp<Authorizer>() {
            @Mock
            public Expr getRowAccessPolicy(Invocation invocation, ConnectContext session, TableName name) {
                if (Thread.currentThread() == planner) {
                    lookups.merge("row:" + name.getTbl(), 1, Integer::sum);
                }
                return invocation.proceed(session, name);
            }

            @Mock
            public Map<String, Expr> getColumnMaskingPolicy(Invocation invocation, ConnectContext session,
                                                            TableName name, List<Column> columns) {
                if (Thread.currentThread() == planner) {
                    lookups.merge("mask:" + name.getTbl(), 1, Integer::sum);
                }
                return invocation.proceed(session, name, columns);
            }
        };
        Set<String> copied = new CopyOnWriteArraySet<>();
        List<String> copiedWithoutLock = new CopyOnWriteArrayList<>();
        LockManager lockManager = GlobalStateMgr.getCurrentState().getLockManager();
        new MockUp<OlapTable>() {
            @Mock
            public void copyOnlyForQuery(Invocation invocation, OlapTable copy) {
                OlapTable source = invocation.getInvokedInstance();
                if (Thread.currentThread() == planner) {
                    copied.add(source.getName());
                    if (!lockManager.isOwnedByCurrentThread(source.getId(), LockType.READ)) {
                        copiedWithoutLock.add(source.getName());
                    }
                }
                invocation.proceed(copy);
            }
        };
        connectContext.getSessionVariable().setCboUseDBLock(false);
        planWithPolicies("SELECT * FROM " + VIEW + " v JOIN test.t0 ON v.k1 = t0.v1");

        Assertions.assertTrue(copied.containsAll(Set.of("t_map", "t_policy_lock", "t0")),
                "planning did not run off copies, so this is not testing it: " + copied);
        Assertions.assertTrue(copiedWithoutLock.isEmpty(), "copied without its read lock: " + copiedWithoutLock);
        for (String key : List.of("row:t_policy_lock", "mask:t_policy_lock", "row:t0", "mask:t0")) {
            Assertions.assertEquals(2, lookups.getOrDefault(key, 0),
                    "once for the lock set, once by the analyzer: " + key + " in " + lookups);
        }
    }
}
