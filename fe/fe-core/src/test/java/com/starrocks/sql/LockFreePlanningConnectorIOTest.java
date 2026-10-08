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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.authorization.AccessControlProvider;
import com.starrocks.authorization.ExternalAccessController;
import com.starrocks.authorization.NativeAccessController;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.common.util.concurrent.lock.LockHoldDepth;
import com.starrocks.connector.ConnectorMetadataRequestContext;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.planner.OlapTableSink;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Two things used to keep connector metadata on the lock critical path for a statement that mixes internal
 * and external tables, both of them upstream of the optimizer the planner already knows to run unlocked.
 *
 * <p><b>The authorization check.</b> It runs between {@code analyzeStatement} and the point where the lock is
 * dropped. For a catalog with an {@code ExternalAccessController} (Ranger et al.),
 * {@code ColumnPrivilege.check} runs a whole rule-based optimization to get the pruned column list, and that
 * asks the connector for partitions, statistics and file lists -- an order of magnitude more work than the
 * single {@code getTable} that used to be there, and all of it under a lock that protects none of it.
 *
 * <p><b>INSERT.</b> {@code isLockFreeInsertStmt} has always answered false, because the INSERT target lands
 * in the copy-unsafe set unconditionally, so {@code INSERT INTO internal SELECT FROM external} planned the
 * entire statement -- optimizer included -- with the lock held.
 *
 * <p>Samples are OR-accumulated per entry point and confined to the test thread, because the asynchronous MV
 * plan cache reaches the same tables on its own executor.
 */
public class LockFreePlanningConnectorIOTest extends ConnectorPlanTestBase {

    private static final String HIVE_CATALOG = "hive0";

    private final Map<String, Boolean> underLock = Maps.newConcurrentMap();
    private volatile Thread testThread;

    private void record(String key) {
        if (Thread.currentThread() != testThread) {
            return;
        }
        underLock.merge(key, LockHoldDepth.isUnderLock(), Boolean::logicalOr);
    }

    /**
     * Mounted on MetadataMgr: it is the one door all of these go through, which keeps the probe independent
     * of which connector the statement happens to use.
     */
    private void probeConnectorCalls() {
        testThread = Thread.currentThread();
        new MockUp<MetadataMgr>() {
            @Mock
            public Table getTable(Invocation invocation, ConnectContext context, String catalogName, String dbName,
                                  String tblName) {
                if (!CatalogMgr.isInternalCatalog(catalogName)) {
                    record("getTable:" + catalogName + "." + tblName);
                }
                return invocation.proceed(context, catalogName, dbName, tblName);
            }

            @Mock
            public Statistics getTableStatistics(Invocation invocation, OptimizerContext session, String catalogName,
                                                 Table table, Map<ColumnRefOperator, Column> columns,
                                                 List<PartitionKey> partitionKeys, ScalarOperator predicate,
                                                 long limit, TvrVersionRange versionRange) {
                record("getTableStatistics:" + table.getName());
                return invocation.proceed(session, catalogName, table, columns, partitionKeys, predicate, limit,
                        versionRange);
            }

            @Mock
            public List<String> listPartitionNames(Invocation invocation, String catalogName, String dbName,
                                                   String tableName, ConnectorMetadataRequestContext context) {
                record("listPartitionNames:" + tableName);
                return invocation.proceed(catalogName, dbName, tableName, context);
            }

            @Mock
            public List<RemoteFileInfo> getRemoteFiles(Invocation invocation, Table table,
                                                       GetRemoteFilesParams params) {
                record("getRemoteFiles:" + table.getName());
                return invocation.proceed(table, params);
            }
        };
    }

    private void assertNothingWentRemoteUnderTheLock(String sql) {
        // Without this the check below passes on an empty map, i.e. whenever the probe never fired.
        Assertions.assertFalse(underLock.isEmpty(),
                "planning never reached the external catalog, the probe proves nothing, for: " + sql);
        underLock.forEach((site, held) -> Assertions.assertFalse(held,
                "an FE metadata lock was held while planning contacted the external catalog, at " + site
                        + ", for: " + sql + "; full samples: " + underLock));
    }

    private ExecPlan plan(String sql) throws Exception {
        // An INSERT derives its transaction label from the query id, so planning more than one per test
        // needs a fresh id or the second begin fails with "label has already been used".
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(sql, connectContext);
        return StatementPlanner.plan(stmt, connectContext);
    }

    /**
     * Allows everything, so the test measures where the check runs rather than what it decides. Installing
     * any ExternalAccessController is what makes ColumnPrivilege run the optimization.
     */
    private static ExternalAccessController permissiveExternalController() {
        return new ExternalAccessController() {
            @Override
            public void checkTableAction(ConnectContext context, TableName tableName, PrivilegeType privilegeType) {
            }

            @Override
            public void checkColumnAction(ConnectContext context, TableName tableName, String column,
                                          PrivilegeType privilegeType) {
            }

            @Override
            public void checkMaterializedViewAction(ConnectContext context, TableName tableName,
                                                    PrivilegeType privilegeType) {
            }
        };
    }

    /**
     * On both catalogs on purpose. {@code ColumnPrivilege} runs the optimization when <em>any</em> catalog
     * named by the query has one, and a statement whose SELECT reads only the external catalog would not
     * reach it if the controller sat on the internal one alone -- the INSERT case below is exactly that.
     */
    private void withExternalAccessController(String sql) throws Exception {
        AccessControlProvider provider = Authorizer.getInstance();
        provider.setAccessControl(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, permissiveExternalController());
        provider.setAccessControl(HIVE_CATALOG, permissiveExternalController());
        try {
            plan(sql);
        } finally {
            provider.setAccessControl(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, new NativeAccessController());
            provider.removeAccessControl(HIVE_CATALOG);
        }
    }

    /**
     * The column-pruning optimization ColumnPrivilege runs for a Ranger-style catalog must not happen with
     * the lock held.
     */
    @Test
    public void testTheColumnPrivilegeOptimizationDoesNotRunUnderTheLock() throws Exception {
        probeConnectorCalls();
        String sql = "SELECT t0.v1, l.l_orderkey FROM test.t0 JOIN hive0.partitioned_db.lineitem_par l "
                + "ON t0.v1 = l.l_orderkey";
        withExternalAccessController(sql);
        assertNothingWentRemoteUnderTheLock(sql);
    }

    /** The same for an INSERT, which reaches the check through a different visitor. */
    @Test
    public void testTheColumnPrivilegeOptimizationDoesNotRunUnderTheLockForAnInsert() throws Exception {
        probeConnectorCalls();
        String sql = "INSERT INTO test.t0 SELECT l_orderkey, l_partkey, l_suppkey "
                + "FROM hive0.partitioned_db.lineitem_par";
        withExternalAccessController(sql);
        assertNothingWentRemoteUnderTheLock(sql);
    }

    /**
     * INSERT INTO internal SELECT FROM external: the whole optimization, not just the privilege check, has to
     * run with the lock released.
     */
    @Test
    public void testAnInsertReadingAConnectorPlansWithoutTheLock() throws Exception {
        probeConnectorCalls();
        String sql = "INSERT INTO test.t0 SELECT l_orderkey, l_partkey, l_suppkey "
                + "FROM hive0.partitioned_db.lineitem_par";
        plan(sql);
        assertNothingWentRemoteUnderTheLock(sql);
    }

    /** The same across the shapes such a load is actually written in, and across two connectors. */
    @Test
    public void testAnInsertStaysOffTheLockForEveryMixedShape() throws Exception {
        probeConnectorCalls();
        for (String sql : List.of(
                "INSERT INTO test.t0 SELECT l_orderkey, l_partkey, l_suppkey "
                        + "FROM hive0.partitioned_db.lineitem_par WHERE l_orderkey > 10",
                "INSERT INTO test.t0 SELECT t.v4, t.v5, l.l_suppkey FROM test.t1 t "
                        + "JOIN hive0.partitioned_db.lineitem_par l ON t.v4 = l.l_orderkey",
                "INSERT INTO test.t0 WITH c AS (SELECT l_orderkey, l_partkey, l_suppkey "
                        + "FROM hive0.partitioned_db.lineitem_par) SELECT * FROM c",
                "INSERT INTO test.t0 SELECT l_orderkey, count(*), count(*) "
                        + "FROM hive0.partitioned_db.lineitem_par GROUP BY l_orderkey")) {
            underLock.clear();
            plan(sql);
            assertNothingWentRemoteUnderTheLock(sql);
        }
    }

    /**
     * Planning must hand the statement back the way it found it. Taking the private copies replaces the
     * target table on the statement itself, and a statement can be planned more than once:
     * {@code InsertOverwriteJobRunner} re-plans after creating the temporary partitions it will swap in, and
     * re-analysis reuses whatever target the statement carries rather than resolving it again. A copy left
     * behind therefore makes the second analysis look for those partitions in a table object that predates
     * them, which surfaces as "Unknown partition ... in table ...".
     */
    @Test
    public void testPlanningLeavesTheLiveTargetOnTheStatement() throws Exception {
        Table live = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "t0");
        for (String sql : List.of(
                // lock-free: the target is copied during planning and has to be put back
                "INSERT INTO test.t0 SELECT l_orderkey, l_partkey, l_suppkey "
                        + "FROM hive0.partitioned_db.lineitem_par",
                // purely local, so nothing is copied -- pinned as the control
                "INSERT INTO test.t0 SELECT v4, v5, v6 FROM test.t1")) {
            connectContext.setQueryId(UUIDUtil.genUUID());
            connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(sql, connectContext);
            StatementPlanner.plan(stmt, connectContext);
            Assertions.assertSame(live, ((InsertStmt) stmt).getTargetTable(),
                    "planning left a snapshot copy of the target on the statement, for: " + sql);
        }
    }

    private static final String RETRY_SQL = "INSERT INTO test.t0 SELECT l_orderkey, l_partkey, l_suppkey "
            + "FROM hive0.partitioned_db.lineitem_par";

    private StatementBase parseInsert(String sql) throws Exception {
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        return UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(sql, connectContext);
    }

    /**
     * A schema change that lands after the snapshot was taken has to be caught, wherever in the lock-free
     * window it lands. The version the copies are validated against is generated before they are taken, so
     * anything later sorts after it; generating it once planning is already under way instead would let a
     * change that raced the released lock sort before it and be accepted, returning a plan built on a table
     * that no longer exists in that shape.
     *
     * <p>The gap this pins is the widest one: between dropping the lock and starting to plan, the
     * authorization check runs, which for a Ranger-style catalog is a whole rule-based optimization.
     */
    @Test
    public void testASchemaChangeRacingTheAuthorizationCheckIsNotAccepted() throws Exception {
        OlapTable t0 = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "t0");
        long savedSchemaUpdateTime = t0.lastSchemaUpdateTime.get();
        AtomicInteger attempts = new AtomicInteger();
        new MockUp<Authorizer>() {
            @Mock
            public void check(Invocation invocation, StatementBase statement, ConnectContext context) {
                // Stands in for a concurrent ALTER landing while the lock is released.
                t0.lastSchemaUpdateTime.set(OptimisticVersion.generate());
                invocation.proceed(statement, context);
            }
        };
        new MockUp<OptimisticVersion>() {
            @Mock
            public boolean validateTableUpdate(Invocation invocation, OlapTable table, long candidateVersion) {
                attempts.incrementAndGet();
                return invocation.proceed(table, candidateVersion);
            }
        };
        try {
            StatementPlanner.plan(parseInsert(RETRY_SQL), connectContext);
        } finally {
            t0.lastSchemaUpdateTime.set(savedSchemaUpdateTime);
        }
        Assertions.assertTrue(attempts.get() >= 2,
                "the schema change that raced the released lock was accepted instead of forcing a retry");
    }

    /**
     * A retry has to re-plan, not just re-analyze. Everything the plan is derived from -- the output schema,
     * the logical plan, the sink -- comes from the table objects of one attempt, so reusing them across a
     * retry would re-validate the fresh copies while handing back a plan built for the schema that was just
     * rejected. Asserted on identity: the sink of the returned plan must be built from the table the retry
     * re-analyzed, not from the attempt that failed validation.
     */
    @Test
    public void testARetryRebuildsThePlanRatherThanRevalidatingTheOldOne() throws Exception {
        StatementBase stmt = parseInsert(RETRY_SQL);
        List<Table> targetPerAttempt = Lists.newArrayList();
        new MockUp<OptimisticVersion>() {
            @Mock
            public boolean validateTableUpdate(OlapTable table, long candidateVersion) {
                // The statement still carries this attempt's copy at validation time.
                targetPerAttempt.add(((InsertStmt) stmt).getTargetTable());
                // Reject the first attempt, accept the second.
                return targetPerAttempt.size() > 1;
            }
        };
        ExecPlan plan = StatementPlanner.plan(stmt, connectContext);

        Assertions.assertEquals(2, targetPerAttempt.size(), "the rejected attempt did not trigger a retry");
        Assertions.assertNotSame(targetPerAttempt.get(0), targetPerAttempt.get(1),
                "the retry did not re-analyze, so this proves nothing about what it planned against");
        OlapTableSink sink = (OlapTableSink) plan.getFragments().get(0).getSink();
        Assertions.assertSame(targetPerAttempt.get(1), sink.getDstTable(),
                "the returned plan's sink was built from the attempt that failed validation");
    }

    /**
     * Going lock-free must not change what comes out. The same INSERT is planned on both paths --
     * cbo_use_lock_db forces the locked one -- and the plans have to match.
     */
    @Test
    public void testTheLockFreeInsertPlansTheSameThing() throws Exception {
        String sql = "INSERT INTO test.t0 SELECT l_orderkey, l_partkey, l_suppkey "
                + "FROM hive0.partitioned_db.lineitem_par";
        String lockFree = getInsertExecPlan(sql);
        connectContext.getSessionVariable().setCboUseDBLock(true);
        try {
            Assertions.assertEquals(lockFree, getInsertExecPlan(sql),
                    "the plan differs depending on whether the lock was held for the whole phase");
        } finally {
            connectContext.getSessionVariable().setCboUseDBLock(false);
        }
    }

    private String getInsertExecPlan(String sql) throws Exception {
        return plan(sql).getExplainString(TExplainLevel.NORMAL);
    }

    /**
     * {@code cbo_use_lock_db} is deliberately left alone here too: it exists to force the locked path when
     * the optimistic one is the suspect. Pinned so the exclusion stays a decision rather than an oversight.
     */
    @Test
    public void testCboUseDbLockStillForcesTheLockedPathForAnInsert() throws Exception {
        probeConnectorCalls();
        connectContext.getSessionVariable().setCboUseDBLock(true);
        try {
            plan("INSERT INTO test.t0 SELECT l_orderkey, l_partkey, l_suppkey "
                    + "FROM hive0.partitioned_db.lineitem_par");
        } finally {
            connectContext.getSessionVariable().setCboUseDBLock(false);
        }
        Assertions.assertFalse(underLock.isEmpty(), "planning never reached the external catalog");
        Assertions.assertTrue(underLock.values().stream().anyMatch(Boolean::booleanValue),
                "cbo_use_lock_db no longer forces the whole planning phase under the lock: " + underLock);
    }
}
