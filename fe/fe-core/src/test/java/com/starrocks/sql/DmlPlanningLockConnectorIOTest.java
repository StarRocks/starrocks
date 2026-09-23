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

import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.common.util.concurrent.lock.LockHoldDepth;
import com.starrocks.connector.ConnectorMetadataRequestContext;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.DeleteStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UpdateStmt;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The same shape {@link LockFreePlanningConnectorIOTest} pins for SELECT and INSERT, for the three
 * statements that were never looked at: UPDATE, DELETE and MERGE INTO.
 *
 * <p>{@code StatementPlanner.takePlanningSnapshotAndUnlock} used to answer only for a QueryStatement and an
 * InsertStmt and return null for everything else, so these three planned with the meta lock held from
 * analysis to the end of fragment building -- statistics, partition lists and file lists of every external
 * table in the statement included. That is the T3 shape of the original report: an internal table's READ lock
 * conflicts with the WRITE lock transaction publishing needs, so
 * {@code DELETE FROM internal WHERE id IN (SELECT ... FROM hive)} stalls publishing on that table for as long
 * as the connector takes to answer.
 *
 * <p>Samples are OR-accumulated per entry point and confined to the test thread, for the reason spelled out
 * in {@link LockFreePlanningConnectorIOTest}: the asynchronous MV plan cache reaches the same tables on its
 * own executor.
 */
public class DmlPlanningLockConnectorIOTest extends ConnectorPlanTestBase {

    private final Map<String, Boolean> underLock = Maps.newConcurrentMap();
    private volatile Thread testThread;

    private void record(String key) {
        if (Thread.currentThread() != testThread) {
            return;
        }
        underLock.merge(key, LockHoldDepth.isUnderLock(), Boolean::logicalOr);
    }

    /** Mounted on MetadataMgr, the one door every connector metadata request goes through. */
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
        assertOnlyTheseWentRemoteUnderTheLock(sql);
    }

    /**
     * @param stillUnderTheLock entry points that are known to still run with the lock held, each of which
     *                          must actually have been sampled as such -- a residual that gets fixed has to
     *                          fail here rather than quietly widen the exemption
     */
    private void assertOnlyTheseWentRemoteUnderTheLock(String sql, String... stillUnderTheLock) {
        // Without this the check below passes on an empty map, i.e. whenever the probe never fired.
        Assertions.assertFalse(underLock.isEmpty(),
                "planning never reached the external catalog, the probe proves nothing, for: " + sql);
        Set<String> expected = Set.of(stillUnderTheLock);
        underLock.forEach((site, held) -> {
            if (expected.contains(site)) {
                return;
            }
            Assertions.assertFalse(held,
                    "an FE metadata lock was held while planning contacted the external catalog, at " + site
                            + ", for: " + sql + "; full samples: " + underLock);
        });
        for (String site : expected) {
            Assertions.assertEquals(Boolean.TRUE, underLock.get(site),
                    site + " no longer runs under the lock, which is good news: drop it from this test and "
                            + "from the residual list in the plan doc. Full samples: " + underLock);
        }
    }

    private void plan(String sql) throws Exception {
        // A DML statement derives its transaction label from the query id, so planning more than one per
        // test needs a fresh id or the second begin fails with "label has already been used".
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(sql, connectContext);
        StatementPlanner.plan(stmt, connectContext);
    }

    /**
     * The control the three below are held to: the same mixture written as a SELECT reaches the connector
     * without the lock at every entry point, the resolve during analysis included -- that one because
     * {@code QueryAnalyzer.analyzeExternalTablesOnly} pre-resolves external tables before the lock is taken.
     */
    @Test
    public void testAPlainSelectReadingAConnectorPlansWithoutTheLock() throws Exception {
        probeConnectorCalls();
        String sql = "SELECT t.pk FROM test.tprimary t JOIN hive0.partitioned_db.lineitem_par l "
                + "ON t.pk = l.l_orderkey";
        plan(sql);
        assertNothingWentRemoteUnderTheLock(sql);
    }

    @Test
    public void testADeleteReadingAConnectorPlansWithoutTheLock() throws Exception {
        probeConnectorCalls();
        String sql = "DELETE FROM test.tprimary WHERE pk IN "
                + "(SELECT l_orderkey FROM hive0.partitioned_db.lineitem_par)";
        plan(sql);
        assertNothingWentRemoteUnderTheLock(sql);
    }

    @Test
    public void testAnUpdateReadingAConnectorPlansWithoutTheLock() throws Exception {
        probeConnectorCalls();
        String sql = "UPDATE test.tprimary SET v2 = 1 WHERE pk IN "
                + "(SELECT l_orderkey FROM hive0.partitioned_db.lineitem_par)";
        plan(sql);
        assertNothingWentRemoteUnderTheLock(sql);
    }

    /**
     * MERGE INTO only targets an Iceberg table, so here the external table is the target and the internal
     * one is the source. The lock covers the internal source, and the target's statistics and file lists
     * used to be fetched while it was held.
     *
     * <p>One residual, pinned rather than asserted away: resolving the target itself still happens under the
     * lock. {@code MergeIntoAnalyzer} resolves it unconditionally, so unlike a table on the read side it
     * cannot be pre-resolved before the lock by handing the analyzer the answer -- closing it means changing
     * the analyzer, and what it costs today is one {@code getTable} against a synchronous connector cache,
     * not the unbounded statistics-and-file-list traffic this change moves out.
     */
    @Test
    public void testAMergeIntoReadingAConnectorPlansWithoutTheLock() throws Exception {
        probeConnectorCalls();
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t USING test.tprimary AS s "
                + "ON t.id = s.pk WHEN MATCHED THEN UPDATE SET data = s.v1";
        plan(sql);
        assertOnlyTheseWentRemoteUnderTheLock(sql, "getTable:iceberg0.t0_v2");
    }

    /**
     * A purely local DML keeps the lock for the whole planning phase, for the reason spelled out in
     * {@code AnalyzerUtils.CopyUnsafeTablesCollector#isCopySafe}: without a connector on the other side of
     * the scale, both sides are CPU and the snapshot is not worth taking. Pinned so the gate stays a
     * decision.
     */
    @Test
    public void testAPurelyLocalDmlStillPlansUnderTheLock() throws Exception {
        for (String sql : List.of(
                "DELETE FROM test.tprimary WHERE pk IN (SELECT v4 FROM test.t1)",
                "UPDATE test.tprimary SET v2 = 1 WHERE pk IN (SELECT v4 FROM test.t1)")) {
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            Assertions.assertFalse(AnalyzerUtils.areTablesCopySafe(stmt), sql);
        }
    }

    /**
     * {@code cbo_use_lock_db} keeps forcing the locked path for these three too: it exists to be the escape
     * hatch when the optimistic path is the suspect, so it must not be quietly bypassed.
     */
    @Test
    public void testCboUseDbLockStillForcesTheLockedPathForADelete() throws Exception {
        probeConnectorCalls();
        connectContext.getSessionVariable().setCboUseDBLock(true);
        try {
            plan("DELETE FROM test.tprimary WHERE pk IN "
                    + "(SELECT l_orderkey FROM hive0.partitioned_db.lineitem_par)");
        } finally {
            connectContext.getSessionVariable().setCboUseDBLock(false);
        }
        Assertions.assertFalse(underLock.isEmpty(), "planning never reached the external catalog");
        Assertions.assertTrue(underLock.values().stream().anyMatch(Boolean::booleanValue),
                "cbo_use_lock_db no longer forces the whole planning phase under the lock: " + underLock);
    }

    /**
     * Going lock-free must not change what comes out. The same statement is planned on both paths --
     * cbo_use_lock_db forces the locked one -- and the plans have to match.
     */
    @Test
    public void testTheLockFreeDmlPlansTheSameThing() throws Exception {
        for (String sql : List.of(
                "DELETE FROM test.tprimary WHERE pk IN "
                        + "(SELECT l_orderkey FROM hive0.partitioned_db.lineitem_par)",
                "UPDATE test.tprimary SET v2 = 1 WHERE pk IN "
                        + "(SELECT l_orderkey FROM hive0.partitioned_db.lineitem_par)")) {
            String lockFree = explain(sql);
            connectContext.getSessionVariable().setCboUseDBLock(true);
            try {
                Assertions.assertEquals(lockFree, explain(sql),
                        "the plan differs depending on whether the lock was held for the whole phase, for: " + sql);
            } finally {
                connectContext.getSessionVariable().setCboUseDBLock(false);
            }
        }
    }

    /**
     * Planning has to hand the statement back the way it found it. On the lock-free path the target is
     * replaced by a private copy ({@code AnalyzerUtils.OlapTableCollector}), and a statement object outlives
     * one planning pass -- {@code StmtExecutor} goes on to use it.
     */
    @Test
    public void testPlanningLeavesTheLiveTargetOnTheStatement() throws Exception {
        Table live = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "tprimary");
        for (String sql : List.of(
                // lock-free: the target is copied during planning and has to be put back
                "DELETE FROM test.tprimary WHERE pk IN "
                        + "(SELECT l_orderkey FROM hive0.partitioned_db.lineitem_par)",
                "UPDATE test.tprimary SET v2 = 1 WHERE pk IN "
                        + "(SELECT l_orderkey FROM hive0.partitioned_db.lineitem_par)",
                // purely local, so nothing is copied -- pinned as the control
                "DELETE FROM test.tprimary WHERE pk IN (SELECT v4 FROM test.t1)")) {
            connectContext.setQueryId(UUIDUtil.genUUID());
            connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(sql, connectContext);
            StatementPlanner.plan(stmt, connectContext);
            Table planned = stmt instanceof DeleteStmt deleteStmt
                    ? deleteStmt.getTable() : ((UpdateStmt) stmt).getTable();
            Assertions.assertSame(live, planned,
                    "planning left a snapshot copy of the target on the statement, for: " + sql);
        }
    }

    /**
     * A schema change that lands after the snapshot was taken has to be caught. The window pinned here is
     * the widest one: between dropping the lock and starting to plan, the authorization check runs, which
     * for a Ranger-style catalog is a whole rule-based optimization.
     */
    @Test
    public void testASchemaChangeRacingTheLockFreeWindowForcesARetry() throws Exception {
        OlapTable target = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable("test", "tprimary");
        long savedSchemaUpdateTime = target.lastSchemaUpdateTime.get();
        AtomicInteger attempts = new AtomicInteger();
        new MockUp<Authorizer>() {
            @Mock
            public void check(Invocation invocation, StatementBase statement, ConnectContext context) {
                // Stands in for a concurrent ALTER landing while the lock is released.
                target.lastSchemaUpdateTime.set(OptimisticVersion.generate());
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
            connectContext.setQueryId(UUIDUtil.genUUID());
            connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
            StatementPlanner.plan(UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(
                    "DELETE FROM test.tprimary WHERE pk IN "
                            + "(SELECT l_orderkey FROM hive0.partitioned_db.lineitem_par)", connectContext),
                    connectContext);
        } finally {
            target.lastSchemaUpdateTime.set(savedSchemaUpdateTime);
        }
        Assertions.assertTrue(attempts.get() >= 2,
                "the schema change that raced the released lock was accepted instead of forcing a retry");
    }

    private String explain(String sql) throws Exception {
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(sql, connectContext);
        return StatementPlanner.plan(stmt, connectContext).getExplainString(TExplainLevel.NORMAL);
    }
}
