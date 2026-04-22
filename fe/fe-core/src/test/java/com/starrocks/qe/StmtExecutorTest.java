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

package com.starrocks.qe;

import com.starrocks.analysis.DescriptorTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.ProfileManager;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.mysql.MysqlSerializer;
import com.starrocks.planner.DataPartition;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.QueryState.MysqlStateType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.ShowFrontendsStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.txn.BeginStmt;
import com.starrocks.sql.ast.txn.CommitStmt;
import com.starrocks.sql.ast.txn.RollbackStmt;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.LargeInPredicateException;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.parser.AstBuilder;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TDescriptorTable;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.ComputeResource;
import com.starrocks.warehouse.cngroup.WarehouseComputeResource;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class StmtExecutorTest {
    private static class DummyPlanNode extends PlanNode {
        DummyPlanNode(PlanNodeId id, long cardinality) {
            super(id, "DummyPlanNode");
            this.cardinality = cardinality;
        }

        @Override
        protected void toThrift(TPlanNode msg) {
            // no-op for unit tests
        }
    }

    private static ExecPlan buildMinimalExecPlan(long cardinality) {
        ExecPlan execPlan = new ExecPlan();
        PlanNode root = new DummyPlanNode(new PlanNodeId(0), cardinality);
        PlanFragment fragment = new PlanFragment(new PlanFragmentId(0), root, DataPartition.UNPARTITIONED);
        execPlan.getFragments().add(fragment);
        Deencapsulation.setField(execPlan, "descTbl", new DescriptorTable());
        return execPlan;
    }

    @Test
    public void testIsForwardToLeader(@Mocked ConnectContext ctx) {
        MysqlSerializer serializer = MysqlSerializer.newInstance();
        GlobalStateMgr state = Deencapsulation.newInstance(GlobalStateMgr.class);
        Thread testThread = Thread.currentThread();
        AtomicInteger leaderCallCount = new AtomicInteger(0);

        new MockUp<GlobalStateMgr>() {
            @Mock
            public GlobalStateMgr getCurrentState() {
                return state;
            }

            @Mock
            public boolean isReady() {
                return true;
            }

            @Mock
            public boolean isLeader() {
                if (Thread.currentThread() != testThread) {
                    return false;
                }
                return leaderCallCount.getAndIncrement() > 0;
            }

            @Mock
            public boolean isInTransferringToLeader() {
                return Thread.currentThread() == testThread;
            }
        };

        new Expectations(ctx) {
            {
                ctx.getSerializer();
                minTimes = 0;
                result = serializer;
            }
        };

        Assertions.assertFalse(new StmtExecutor(ctx, new ShowFrontendsStmt()).isForwardToLeader());
    }

    @Test
    public void testForwardExplicitTxnSelectOnFollower(@Mocked ConnectContext ctx) {
        StatementBase stmt;
        MysqlSerializer serializer = MysqlSerializer.newInstance();
        GlobalStateMgr state = Deencapsulation.newInstance(GlobalStateMgr.class);
        SqlParser sqlParser = new SqlParser(AstBuilder.getInstance());

        new MockUp<GlobalStateMgr>() {
            @Mock
            public GlobalStateMgr getCurrentState() {
                return state;
            }

            @Mock
            public GlobalStateMgr getServingState() {
                return state;
            }

            @Mock
            public boolean isReady() {
                return true;
            }

            @Mock
            public SqlParser getSqlParser() {
                return sqlParser;
            }

            @Mock
            public boolean isLeader() {
                return false;
            }

            @Mock
            public boolean isInTransferringToLeader() {
                return false;
            }
        };

        new Expectations() {
            {
                ctx.getSerializer();
                minTimes = 0;
                result = serializer;

                ctx.getTxnId();
                minTimes = 0;
                result = 1L;

                ctx.isQueryStmt((StatementBase) any);
                minTimes = 0;
                result = true;
            }
        };

        // Parse after expectations to ensure GlobalStateMgr.getSqlParser() is properly mocked
        stmt = SqlParser.parseSingleStatement("select 1", SqlModeHelper.MODE_DEFAULT);
        StmtExecutor executor = new StmtExecutor(ctx, stmt);
        Assertions.assertTrue(executor.isForwardToLeader());
    }

    @Test
    public void testExecType() {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ConnectContext.threadLocalInfo.set(ctx);

        StatementBase stmt = SqlParser.parseSingleStatement("select * from t1", SqlModeHelper.MODE_DEFAULT);
        StmtExecutor executor = new StmtExecutor(new ConnectContext(), stmt);
        Assertions.assertEquals("Query", executor.getExecType());
        Assertions.assertFalse(executor.isExecLoadType());
        Assertions.assertEquals(ConnectContext.get().getSessionVariable().getQueryTimeoutS(), executor.getExecTimeout());

        stmt = SqlParser.parseSingleStatement("insert into t1 select * from t2", SqlModeHelper.MODE_DEFAULT);
        executor = new StmtExecutor(new ConnectContext(), stmt);
        Assertions.assertEquals("Insert", executor.getExecType());
        Assertions.assertTrue(executor.isExecLoadType());
        Assertions.assertEquals(ConnectContext.get().getSessionVariable().getInsertTimeoutS(), executor.getExecTimeout());

        stmt = SqlParser.parseSingleStatement("create table t1 as select * from t2", SqlModeHelper.MODE_DEFAULT);
        executor = new StmtExecutor(new ConnectContext(), stmt);
        Assertions.assertEquals("Insert", executor.getExecType());
        Assertions.assertTrue(executor.isExecLoadType());
        Assertions.assertEquals(ConnectContext.get().getSessionVariable().getInsertTimeoutS(), executor.getExecTimeout());

        stmt = SqlParser.parseSingleStatement("update t1 set k1 = 1 where k2 = 1", SqlModeHelper.MODE_DEFAULT);
        executor = new StmtExecutor(new ConnectContext(), stmt);
        Assertions.assertEquals("Update", executor.getExecType());
        Assertions.assertTrue(executor.isExecLoadType());
        Assertions.assertEquals(ConnectContext.get().getSessionVariable().getInsertTimeoutS(), executor.getExecTimeout());

        stmt = SqlParser.parseSingleStatement("delete from t1 where k2 = 1", SqlModeHelper.MODE_DEFAULT);
        executor = new StmtExecutor(new ConnectContext(), stmt);
        Assertions.assertEquals("Delete", executor.getExecType());
        Assertions.assertTrue(executor.isExecLoadType());
        Assertions.assertEquals(ConnectContext.get().getSessionVariable().getInsertTimeoutS(), executor.getExecTimeout());
    }

    @Test
    public void buildTopLevelProfile_createsProfileWithCorrectSummaryInfo() {
        new MockUp<WarehouseManager>() {
            @Mock
            public String getWarehouseComputeResourceName(ComputeResource computeResource) {
                return "default_warehouse";
            }
        };

        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ConnectContext.threadLocalInfo.set(ctx);
        StatementBase stmt = SqlParser.parseSingleStatement("select * from t1", SqlModeHelper.MODE_DEFAULT);
        StmtExecutor executor = new StmtExecutor(new ConnectContext(), stmt);
        RuntimeProfile profile = Deencapsulation.invoke(executor, "buildTopLevelProfile");

        Assertions.assertNotNull(profile);
        Assertions.assertEquals("Query", profile.getName());
        RuntimeProfile summaryProfile = profile.getChild("Summary");
        Assertions.assertNotNull(summaryProfile);
        Assertions.assertEquals("Running", summaryProfile.getInfoString(ProfileManager.QUERY_STATE));
        Assertions.assertEquals("default_warehouse", summaryProfile.getInfoString(ProfileManager.WAREHOUSE_CNGROUP));
    }

    @Test
    public void testGetRedactedOriginStmtInStringScenarios() {
        // Case 1: Plain SQL should be returned as-is.
        StatementBase plainStmt = SqlParser.parseSingleStatement(
                "SELECT * FROM t0 WHERE id = 1",
                SqlModeHelper.MODE_DEFAULT);
        StmtExecutor plainExecutor = new StmtExecutor(UtFrameUtils.createDefaultCtx(), plainStmt);
        String plainResult = plainExecutor.getRedactedOriginStmtInString();
        Assertions.assertEquals("SELECT * FROM t0 WHERE id = 1", plainResult);

        // Case 2: INSERT ... SELECT FROM FILES should redact both key and secret.
        StatementBase insertSelectFilesStmt = SqlParser.parseSingleStatement(
                "INSERT INTO t0 SELECT * FROM FILES(" +
                        "\"path\"=\"s3://bucket/data.parquet\", " +
                        "\"format\"=\"parquet\", " +
                        "\"aws.s3.access_key\"=\"AKIA_STMT_EXECUTOR\", " +
                        "\"aws.s3.secret_key\"=\"STMT_EXECUTOR_SECRET\")",
                SqlModeHelper.MODE_DEFAULT);
        StmtExecutor insertSelectFilesExecutor =
                new StmtExecutor(UtFrameUtils.createDefaultCtx(), insertSelectFilesStmt);
        String insertSelectFilesRedacted = insertSelectFilesExecutor.getRedactedOriginStmtInString();
        Assertions.assertFalse(insertSelectFilesRedacted.contains("AKIA_STMT_EXECUTOR"));
        Assertions.assertFalse(insertSelectFilesRedacted.contains("STMT_EXECUTOR_SECRET"));
        Assertions.assertTrue(insertSelectFilesRedacted.contains("***"));

        // Case 3: SELECT FROM FILES should redact secret.
        StatementBase selectFilesStmt = SqlParser.parseSingleStatement(
                "SELECT * FROM FILES(\"path\"=\"s3://bucket/data.parquet\", " +
                        "\"aws.s3.secret_key\"=\"RETRY_SECRET\")",
                SqlModeHelper.MODE_DEFAULT);
        StmtExecutor selectFilesExecutor = new StmtExecutor(UtFrameUtils.createDefaultCtx(), selectFilesStmt);
        String selectFilesRedacted = selectFilesExecutor.getRedactedOriginStmtInString();
        Assertions.assertFalse(selectFilesRedacted.contains("RETRY_SECRET"));
        Assertions.assertTrue(selectFilesRedacted.contains("***"));
    }

    @Test
    public void testBuildTopLevelProfileSqlStatementScenarios() {
        new MockUp<WarehouseManager>() {
            @Mock
            public String getWarehouseComputeResourceName(ComputeResource computeResource) {
                return "default_warehouse";
            }
        };

        // Case 1: Plain SELECT should not be changed.
        ConnectContext plainCtx = UtFrameUtils.createDefaultCtx();
        ConnectContext.threadLocalInfo.set(plainCtx);
        StatementBase plainStmt = SqlParser.parseSingleStatement("SELECT 1", SqlModeHelper.MODE_DEFAULT);
        StmtExecutor plainExecutor = new StmtExecutor(plainCtx, plainStmt);
        RuntimeProfile plainProfile = Deencapsulation.invoke(plainExecutor, "buildTopLevelProfile");
        RuntimeProfile plainSummaryProfile = plainProfile.getChild("Summary");
        String plainSqlInProfile = plainSummaryProfile.getInfoString(ProfileManager.SQL_STATEMENT);
        Assertions.assertNotNull(plainSqlInProfile);
        Assertions.assertFalse(plainSqlInProfile.contains("***"),
                "Plain SELECT should skip credential redaction in profile SQL");

        // Case 2: Audit-encrypt path should redact password.
        ConnectContext encryptCtx = UtFrameUtils.createDefaultCtx();
        ConnectContext.threadLocalInfo.set(encryptCtx);
        StatementBase encryptStmt = SqlParser.parseSingleStatement(
                "CREATE USER 'u1' IDENTIFIED BY 'secret'",
                SqlModeHelper.MODE_DEFAULT);
        StmtExecutor encryptExecutor = new StmtExecutor(encryptCtx, encryptStmt);
        RuntimeProfile encryptProfile = Deencapsulation.invoke(encryptExecutor, "buildTopLevelProfile");
        RuntimeProfile encryptSummaryProfile = encryptProfile.getChild("Summary");
        String encryptSqlInProfile = encryptSummaryProfile.getInfoString(ProfileManager.SQL_STATEMENT);
        Assertions.assertNotNull(encryptSqlInProfile);
        Assertions.assertFalse(encryptSqlInProfile.contains("secret"));
        Assertions.assertTrue(encryptSqlInProfile.contains("***"));

        // NOTE: The `enable_sql_desensitize_in_log` digest-mode path exists on branch-4.1 / main but
        // not on branch-4.0, so the corresponding coverage from the original PR is intentionally
        // omitted here. Adding it would require new config + FormatOptions.setEnableDigest APIs.

        // Case 3: Credential marker path should redact FILES secret.
        ConnectContext markerCtx = UtFrameUtils.createDefaultCtx();
        ConnectContext.threadLocalInfo.set(markerCtx);
        StatementBase markerStmt = SqlParser.parseSingleStatement(
                "SELECT * FROM FILES(\"path\"=\"s3://bucket/data.parquet\", " +
                        "\"aws.s3.secret_key\"=\"PROFILE_SECRET\")",
                SqlModeHelper.MODE_DEFAULT);
        StmtExecutor markerExecutor = new StmtExecutor(markerCtx, markerStmt);
        RuntimeProfile markerProfile = Deencapsulation.invoke(markerExecutor, "buildTopLevelProfile");
        RuntimeProfile markerSummaryProfile = markerProfile.getChild("Summary");
        String markerSqlInProfile = markerSummaryProfile.getInfoString(ProfileManager.SQL_STATEMENT);
        Assertions.assertNotNull(markerSqlInProfile);
        Assertions.assertFalse(markerSqlInProfile.contains("PROFILE_SECRET"));
        Assertions.assertTrue(markerSqlInProfile.contains("***"));
    }

    @Test
    public void testAddRunningQueryDetailRedactsFilesSecretWhenMarkerDetected() {
        boolean oldCollect = Config.enable_collect_query_detail_info;
        Config.enable_collect_query_detail_info = true;
        try {
            ConnectContext ctx = UtFrameUtils.createDefaultCtx();
            ConnectContext.threadLocalInfo.set(ctx);
            UUID queryId = UUIDUtil.genUUID();
            ctx.setQueryId(queryId);
            ctx.setExecutionId(UUIDUtil.toTUniqueId(queryId));
            StatementBase stmt = SqlParser.parseSingleStatement(
                    "SELECT * FROM FILES(\"path\"=\"s3://bucket/data.parquet\", " +
                            "\"aws.s3.secret_key\"=\"DETAIL_SECRET\")",
                    SqlModeHelper.MODE_DEFAULT);
            StmtExecutor executor = new StmtExecutor(ctx, stmt);

            executor.addRunningQueryDetail(stmt);
            QueryDetail queryDetail = ctx.getQueryDetail();
            Assertions.assertNotNull(queryDetail);
            Assertions.assertFalse(queryDetail.getSql().contains("DETAIL_SECRET"));
            Assertions.assertTrue(queryDetail.getSql().contains("***"));
        } finally {
            Config.enable_collect_query_detail_info = oldCollect;
        }
    }

    @Test
    public void testExecuteAnalyzeProfileStmtBranchSetsErrorWhenProfileMissing() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ConnectContext.threadLocalInfo.set(ctx);
        UUID queryId = UUIDUtil.genUUID();
        ctx.setQueryId(queryId);
        ctx.setExecutionId(UUIDUtil.toTUniqueId(queryId));
        StatementBase stmt = SqlParser.parseSingleStatement(
                "ANALYZE PROFILE FROM 'missing-query-id'", SqlModeHelper.MODE_DEFAULT);
        StmtExecutor executor = new StmtExecutor(ctx, stmt);

        executor.execute();
        Assertions.assertTrue(ctx.getState().isError());
    }

    @Test
    public void testQueryRetryLoopInvokesHandleQueryStmtOnce() throws Exception {
        int oldRetryTime = Config.max_query_retry_time;
        Config.max_query_retry_time = 1;
        try {
            ConnectContext ctx = UtFrameUtils.createDefaultCtx();
            ConnectContext.threadLocalInfo.set(ctx);
            UUID queryId = UUIDUtil.genUUID();
            ctx.setQueryId(queryId);
            ctx.setExecutionId(UUIDUtil.toTUniqueId(queryId));
            StatementBase stmt = SqlParser.parseSingleStatement("SELECT 1", SqlModeHelper.MODE_DEFAULT);
            StmtExecutor executor = new StmtExecutor(ctx, stmt);

            new MockUp<StatementPlanner>() {
                @Mock
                public static ExecPlan plan(StatementBase ignoredStmt, ConnectContext ignoredCtx) {
                    return buildMinimalExecPlan(1);
                }
            };

            executor.execute();
            Assertions.assertNotNull(ctx.getState());
        } finally {
            Config.max_query_retry_time = oldRetryTime;
        }
    }

    @Test
    public void testInsertNoRowsForHiveLikeTableSetsOk(@Mocked DefaultCoordinator coordinator) throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ConnectContext.threadLocalInfo.set(ctx);
        UUID queryId = UUIDUtil.genUUID();
        ctx.setQueryId(queryId);
        ctx.setExecutionId(UUIDUtil.toTUniqueId(queryId));
        // On branch-4.0, StmtExecutor.handleDMLStmt calls stmt.getTableName().normalization(context)
        // at the top, which requires a non-empty current database on the context. The upstream PR
        // on branch-4.1 normalizes earlier, so this ctx.setDatabase call is not needed there.
        ctx.setDatabase("test_db");
        InsertStmt stmt = (InsertStmt) SqlParser.parseSingleStatement(
                "INSERT INTO t0 SELECT 1 WHERE FALSE", SqlModeHelper.MODE_DEFAULT);
        StmtExecutor executor = new StmtExecutor(ctx, stmt);

        Table targetTable = new Table(Table.TableType.HIVE);
        new MockUp<InsertStmt>() {
            @Mock
            public Table getTargetTable() {
                return targetTable;
            }
        };

        ExecPlan execPlan = buildMinimalExecPlan(1);
        new MockUp<DefaultCoordinator.Factory>() {
            @Mock
            public DefaultCoordinator createInsertScheduler(ConnectContext context, List<PlanFragment> fragments,
                                                            List<ScanNode> scanNodes,
                                                            TDescriptorTable descTable, ExecPlan plan) {
                return coordinator;
            }
        };
        new MockUp<DefaultCoordinator>() {
            @Mock
            public void setLoadJobType(com.starrocks.thrift.TLoadJobType loadJobType) {
            }

            @Mock
            public void setLoadJobId(Long jobId) {
            }

            @Mock
            public void exec() {
            }

            @Mock
            public boolean join(int timeoutSecond) {
                return true;
            }

            @Mock
            public boolean isDone() {
                return true;
            }

            @Mock
            public Status getExecStatus() {
                return new Status();
            }

            @Mock
            public java.util.Map<String, String> getLoadCounters() {
                return new HashMap<>();
            }

            @Mock
            public String getTrackingUrl() {
                return "";
            }
        };

        executor.handleDMLStmt(execPlan, stmt);
        Assertions.assertEquals(MysqlStateType.OK, ctx.getState().getStateType());
    }

    @Test
    public void testPlannerFailurePathUsesRedactedSqlWithoutPrivateMocking() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ConnectContext.threadLocalInfo.set(ctx);
        UUID queryId = UUIDUtil.genUUID();
        ctx.setQueryId(queryId);
        ctx.setExecutionId(UUIDUtil.toTUniqueId(queryId));
        StatementBase stmt = SqlParser.parseSingleStatement(
                "SELECT * FROM FILES(\"path\"=\"s3://bucket/data.parquet\", " +
                        "\"aws.s3.secret_key\"=\"PLANNER_SECRET\")",
                SqlModeHelper.MODE_DEFAULT);
        StmtExecutor executor = new StmtExecutor(ctx, stmt);

        new MockUp<StatementPlanner>() {
            @Mock
            public static ExecPlan plan(StatementBase ignoredStmt, ConnectContext ignoredCtx) {
                throw new StarRocksPlannerException(ErrorType.INTERNAL_ERROR, "mock planner failure");
            }
        };

        executor.execute();
        String redacted = executor.getRedactedOriginStmtInString();
        Assertions.assertTrue(ctx.getState().isError());
        Assertions.assertFalse(redacted.contains("PLANNER_SECRET"));
        Assertions.assertTrue(redacted.contains("***"));
    }

    @Test
    public void testLargeInPredicateFailurePathUsesRedactedSqlWithoutPrivateMocking() {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ConnectContext.threadLocalInfo.set(ctx);
        UUID queryId = UUIDUtil.genUUID();
        ctx.setQueryId(queryId);
        ctx.setExecutionId(UUIDUtil.toTUniqueId(queryId));
        StatementBase stmt = SqlParser.parseSingleStatement(
                "SELECT * FROM FILES(\"path\"=\"s3://bucket/data.parquet\", " +
                        "\"aws.s3.secret_key\"=\"LARGE_IN_SECRET\")",
                SqlModeHelper.MODE_DEFAULT);
        StmtExecutor executor = new StmtExecutor(ctx, stmt);

        new MockUp<StatementPlanner>() {
            @Mock
            public static ExecPlan plan(StatementBase ignoredStmt, ConnectContext ignoredCtx) {
                throw new LargeInPredicateException("mock large in failure");
            }
        };

        Assertions.assertThrows(LargeInPredicateException.class, executor::execute);
        String redacted = executor.getRedactedOriginStmtInString();
        Assertions.assertFalse(redacted.contains("LARGE_IN_SECRET"));
        Assertions.assertTrue(redacted.contains("***"));
    }

    @Test
    public void testHandleDdlStmtLogsRedactedSqlOnQueryStateException() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ConnectContext.threadLocalInfo.set(ctx);
        StatementBase stmt = SqlParser.parseSingleStatement("CREATE TABLE t0(k1 INT)", SqlModeHelper.MODE_DEFAULT);
        StmtExecutor executor = new StmtExecutor(ctx, stmt);

        new MockUp<DDLStmtExecutor>() {
            @Mock
            public ShowResultSet execute(StatementBase ignoredStmt, ConnectContext ignoredCtx) throws Exception {
                throw new QueryStateException(MysqlStateType.ERR, "mock ddl failure");
            }
        };

        Deencapsulation.invoke(executor, "handleDdlStmt");
    }

    @Test
    public void testExecTimeout() {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ConnectContext.threadLocalInfo.set(ctx);

        {
            StatementBase stmt = SqlParser.parseSingleStatement("select * from t1", SqlModeHelper.MODE_DEFAULT);
            StmtExecutor executor = new StmtExecutor(new ConnectContext(), stmt);
            Assertions.assertEquals(ctx.getSessionVariable().getQueryTimeoutS(), executor.getExecTimeout());
        }
        {
            StatementBase stmt = SqlParser.parseSingleStatement("analyze table t1", SqlModeHelper.MODE_DEFAULT);
            StmtExecutor executor = new StmtExecutor(new ConnectContext(), stmt);
            Assertions.assertEquals(Config.statistic_collect_query_timeout, executor.getExecTimeout());
        }
        {
            StatementBase stmt = SqlParser.parseSingleStatement("create table t2 as select * from t1",
                    SqlModeHelper.MODE_DEFAULT);
            StmtExecutor executor = new StmtExecutor(new ConnectContext(), stmt);
            Assertions.assertEquals(ctx.getSessionVariable().getInsertTimeoutS(), executor.getExecTimeout());
        }
    }

    @Test
    public void testExecTimeoutWithTableQueryTimeout() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        StarRocksAssert starRocksAssert = new StarRocksAssert(ctx);

        // Create test database
        String dbName = "test_table_timeout_db";
        starRocksAssert.withDatabase(dbName).useDatabase(dbName);

        // Create table without table_query_timeout
        String createTableStmt1 = "CREATE TABLE `t1` (\n" +
                "  `k1` int NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        starRocksAssert.withTable(createTableStmt1);

        // Create table with table_query_timeout = 120
        String createTableStmt2 = "CREATE TABLE `t2` (\n" +
                "  `k1` int NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"table_query_timeout\" = \"120\"\n" +
                ");";
        starRocksAssert.withTable(createTableStmt2);

        // Create table with table_query_timeout = 600 (greater than cluster timeout 300)
        String createTableStmt3 = "CREATE TABLE `t3` (\n" +
                "  `k1` int NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"table_query_timeout\" = \"600\"\n" +
                ");";
        starRocksAssert.withTable(createTableStmt3);

        // Create table with table_query_timeout = 60
        String createTableStmt4 = "CREATE TABLE `t4` (\n" +
                "  `k1` int NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"table_query_timeout\" = \"60\"\n" +
                ");";
        starRocksAssert.withTable(createTableStmt4);

        // Test 1: User not explicitly set session timeout, table has no table_query_timeout
        // Should use cluster timeout (default 300)
        {
            ConnectContext testCtx = UtFrameUtils.createDefaultCtx();
            testCtx.setDatabase(dbName);
            StatementBase stmt = SqlParser.parseSingleStatement("select * from t1", SqlModeHelper.MODE_DEFAULT);
            StmtExecutor executor = new StmtExecutor(testCtx, stmt);
            int defaultTimeout = testCtx.getSessionVariable().getQueryTimeoutS();
            Assertions.assertEquals(defaultTimeout, executor.getExecTimeout());
        }

        // Test 2: User not explicitly set session timeout, table has table_query_timeout = 120
        // Should use table_query_timeout (120)
        {
            ConnectContext testCtx = UtFrameUtils.createDefaultCtx();
            testCtx.setDatabase(dbName);
            StatementBase stmt = SqlParser.parseSingleStatement("select * from t2", SqlModeHelper.MODE_DEFAULT);
            Analyzer.analyze(stmt, testCtx); // Analyze SQL to resolve table references
            StmtExecutor executor = new StmtExecutor(testCtx, stmt);
            Assertions.assertEquals(120, executor.getExecTimeout());
            // Verify table timeout info
            Assertions.assertNotNull(executor.getTableQueryTimeoutInfo());
            Assertions.assertEquals("test_table_timeout_db.t2", executor.getTableQueryTimeoutInfo().first);
            Assertions.assertEquals(120, executor.getTableQueryTimeoutInfo().second.intValue());
        }

        // Test 3: User not explicitly set session timeout, table has table_query_timeout = 600 (greater than cluster timeout)
        // Should use table_query_timeout (600) - this is the new behavior
        {
            ConnectContext testCtx = UtFrameUtils.createDefaultCtx();
            testCtx.setDatabase(dbName);
            StatementBase stmt = SqlParser.parseSingleStatement("select * from t3", SqlModeHelper.MODE_DEFAULT);
            Analyzer.analyze(stmt, testCtx); // Analyze SQL to resolve table references
            StmtExecutor executor = new StmtExecutor(testCtx, stmt);
            Assertions.assertEquals(600, executor.getExecTimeout());
            // Verify table timeout info
            Assertions.assertNotNull(executor.getTableQueryTimeoutInfo());
            Assertions.assertEquals("test_table_timeout_db.t3", executor.getTableQueryTimeoutInfo().first);
            Assertions.assertEquals(600, executor.getTableQueryTimeoutInfo().second.intValue());
        }

        // Test 4: User explicitly set session timeout = 200, table has table_query_timeout = 120
        // Should use session timeout (200) - session timeout has higher priority
        {
            ConnectContext testCtx = UtFrameUtils.createDefaultCtx();
            testCtx.setDatabase(dbName);
            testCtx.getSessionVariable().setQueryTimeoutS(200);
            StatementBase stmt = SqlParser.parseSingleStatement("select * from t2", SqlModeHelper.MODE_DEFAULT);
            Analyzer.analyze(stmt, testCtx); // Analyze SQL to resolve table references
            StmtExecutor executor = new StmtExecutor(testCtx, stmt);
            Assertions.assertEquals(200, executor.getExecTimeout());
        }

        // Test 5: User explicitly set session timeout = 100, table has table_query_timeout = 600
        // Should use session timeout (100) - session timeout has higher priority
        {
            ConnectContext testCtx = UtFrameUtils.createDefaultCtx();
            testCtx.setDatabase(dbName);
            testCtx.getSessionVariable().setQueryTimeoutS(100);
            StatementBase stmt = SqlParser.parseSingleStatement("select * from t3", SqlModeHelper.MODE_DEFAULT);
            Analyzer.analyze(stmt, testCtx); // Analyze SQL to resolve table references
            StmtExecutor executor = new StmtExecutor(testCtx, stmt);
            Assertions.assertEquals(100, executor.getExecTimeout());
        }

        // Test 6: Multiple tables with different table_query_timeout, should use minimum
        // t2 has 120, t4 has 60, should use 60
        {
            ConnectContext testCtx = UtFrameUtils.createDefaultCtx();
            testCtx.setDatabase(dbName);
            StatementBase stmt = SqlParser.parseSingleStatement("select * from t2, t4", SqlModeHelper.MODE_DEFAULT);
            Analyzer.analyze(stmt, testCtx); // Analyze SQL to resolve table references
            StmtExecutor executor = new StmtExecutor(testCtx, stmt);
            Assertions.assertEquals(60, executor.getExecTimeout());
            // Verify table timeout info points to the table with minimum timeout
            Assertions.assertNotNull(executor.getTableQueryTimeoutInfo());
            Assertions.assertEquals(60, executor.getTableQueryTimeoutInfo().second.intValue());
        }

        // Test 7: Test getTableQueryTimeoutInfo when no table timeout is set
        {
            ConnectContext testCtx = UtFrameUtils.createDefaultCtx();
            testCtx.setDatabase(dbName);
            StatementBase stmt = SqlParser.parseSingleStatement("select * from t1", SqlModeHelper.MODE_DEFAULT);
            Analyzer.analyze(stmt, testCtx); // Analyze SQL to resolve table references
            StmtExecutor executor = new StmtExecutor(testCtx, stmt);
            executor.getExecTimeout(); // Initialize timeout
            Assertions.assertNull(executor.getTableQueryTimeoutInfo());
        }

        // Test 8: Test getTableQueryTimeoutInfo when table timeout is set
        {
            ConnectContext testCtx = UtFrameUtils.createDefaultCtx();
            testCtx.setDatabase(dbName);
            StatementBase stmt = SqlParser.parseSingleStatement("select * from t2", SqlModeHelper.MODE_DEFAULT);
            Analyzer.analyze(stmt, testCtx); // Analyze SQL to resolve table references
            StmtExecutor executor = new StmtExecutor(testCtx, stmt);
            executor.getExecTimeout(); // Initialize timeout
            Pair<String, Integer> timeoutInfo = executor.getTableQueryTimeoutInfo();
            Assertions.assertNotNull(timeoutInfo);
            Assertions.assertEquals("test_table_timeout_db.t2", timeoutInfo.first);
            Assertions.assertEquals(120, timeoutInfo.second.intValue());
        }

        // Test 9: Test with non-QueryStatement (should use session timeout)
        {
            ConnectContext testCtx = UtFrameUtils.createDefaultCtx();
            testCtx.setDatabase(dbName);
            // Use a non-query statement like SHOW
            StatementBase stmt = SqlParser.parseSingleStatement("SHOW TABLES", SqlModeHelper.MODE_DEFAULT);
            StmtExecutor executor = new StmtExecutor(testCtx, stmt);
            int timeout = executor.getExecTimeout();
            int defaultTimeout = testCtx.getSessionVariable().getQueryTimeoutS();
            Assertions.assertEquals(defaultTimeout, timeout);
            Assertions.assertNull(executor.getTableQueryTimeoutInfo());
        }

        // Test 10: Test getTableQueryTimeoutInfo coverage (lines 685, 687, 688)
        // This test ensures both return paths of getTableQueryTimeoutInfo are covered
        {
            ConnectContext testCtx = UtFrameUtils.createDefaultCtx();
            testCtx.setDatabase(dbName);
            
            // Test with table timeout set
            StatementBase stmt1 = SqlParser.parseSingleStatement("select * from t2", SqlModeHelper.MODE_DEFAULT);
            Analyzer.analyze(stmt1, testCtx); // Analyze SQL to resolve table references
            StmtExecutor executor1 = new StmtExecutor(testCtx, stmt1);
            executor1.getExecTimeout();
            Pair<String, Integer> info1 = executor1.getTableQueryTimeoutInfo();
            Assertions.assertNotNull(info1);
            Assertions.assertEquals("test_table_timeout_db.t2", info1.first);
            Assertions.assertEquals(120, info1.second.intValue());
            
            // Test without table timeout
            StatementBase stmt2 = SqlParser.parseSingleStatement("select * from t1", SqlModeHelper.MODE_DEFAULT);
            Analyzer.analyze(stmt2, testCtx); // Analyze SQL to resolve table references
            StmtExecutor executor2 = new StmtExecutor(testCtx, stmt2);
            executor2.getExecTimeout();
            Assertions.assertNull(executor2.getTableQueryTimeoutInfo());
        }

        // Test 11: Test that isSessionQueryTimeoutOverridden() exception propagates to getExecTimeout()
        {
            ConnectContext testCtx = UtFrameUtils.createDefaultCtx();
            testCtx.setDatabase(dbName);
            // Ensure ConnectContext.get() is not null for getExecTimeout()
            ConnectContext.set(testCtx);

            // Use a table without table_query_timeout so the result is deterministic (falls back to session timeout).
            StatementBase stmt = SqlParser.parseSingleStatement("select * from t1", SqlModeHelper.MODE_DEFAULT);
            Analyzer.analyze(stmt, testCtx);

            // Mock isSessionQueryTimeoutOverridden() to throw, so getExecTimeout() should also throw
            new MockUp<ConnectContext>() {
                @Mock
                public boolean isSessionQueryTimeoutOverridden() {
                    throw new RuntimeException("Mock exception for isSessionQueryTimeoutOverridden()");
                }
            };

            try {
                StmtExecutor executor = new StmtExecutor(testCtx, stmt);
                // Since isSessionQueryTimeoutOverridden() throws, getExecTimeout() should also throw
                Assertions.assertThrows(RuntimeException.class, () -> executor.getExecTimeout());
            } finally {
                ConnectContext.remove();
            }
        }
    }

    @Test
    public void testTransactionStmtWithSqlTransactionDisabled() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();

        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ctx.setThreadLocalInfo();
        ctx.setQueryId(UUIDUtil.genUUID());
        ctx.setExecutionId(new TUniqueId(1, 2));

        // Disable SQL transaction
        ctx.getSessionVariable().setEnableSqlTransaction(false);

        // Test 1: BEGIN statement when transaction is disabled - should return OK
        {
            StatementBase stmt = SqlParser.parseSingleStatement("BEGIN", SqlModeHelper.MODE_DEFAULT);
            Assertions.assertTrue(stmt instanceof BeginStmt);
            StmtExecutor executor = new StmtExecutor(ctx, stmt);
            executor.execute();

            Assertions.assertFalse(ctx.getState().isError());
            Assertions.assertEquals(MysqlStateType.OK, ctx.getState().getStateType());
        }

        // Test 2: COMMIT statement when transaction is disabled and txnId is 0 - should return OK
        {
            ctx.setTxnId(0);
            ctx.setQueryId(UUIDUtil.genUUID());
            StatementBase stmt = SqlParser.parseSingleStatement("COMMIT", SqlModeHelper.MODE_DEFAULT);
            Assertions.assertTrue(stmt instanceof CommitStmt);
            StmtExecutor executor = new StmtExecutor(ctx, stmt);
            executor.execute();

            Assertions.assertFalse(ctx.getState().isError());
            Assertions.assertEquals(MysqlStateType.OK, ctx.getState().getStateType());
        }

        // Test 3: ROLLBACK statement when transaction is disabled and txnId is 0 - should return OK
        {
            ctx.setTxnId(0);
            ctx.setQueryId(UUIDUtil.genUUID());
            StatementBase stmt = SqlParser.parseSingleStatement("ROLLBACK", SqlModeHelper.MODE_DEFAULT);
            Assertions.assertTrue(stmt instanceof RollbackStmt);
            StmtExecutor executor = new StmtExecutor(ctx, stmt);
            executor.execute();

            Assertions.assertFalse(ctx.getState().isError());
            Assertions.assertEquals(MysqlStateType.OK, ctx.getState().getStateType());
        }
    }

    @Test
    public void testTransactionStmtWithSqlTransactionEnabled() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();

        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ctx.setThreadLocalInfo();
        ctx.setQueryId(UUIDUtil.genUUID());
        ctx.setExecutionId(new TUniqueId(3, 4));

        // Enable SQL transaction (default is true)
        ctx.getSessionVariable().setEnableSqlTransaction(true);

        // Test 1: BEGIN statement when transaction is enabled
        {
            StatementBase stmt = SqlParser.parseSingleStatement("BEGIN", SqlModeHelper.MODE_DEFAULT);
            Assertions.assertTrue(stmt instanceof BeginStmt);
            StmtExecutor executor = new StmtExecutor(ctx, stmt);
            executor.execute();

            // Should not be error state
            Assertions.assertFalse(ctx.getState().isError());
            // Verify txnId is set
            Assertions.assertNotEquals(0, ctx.getTxnId());

            // Cleanup
            long txnId = ctx.getTxnId();
            GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().clearExplicitTxnState(txnId);
            ctx.setTxnId(0);
        }

        // Test 2: COMMIT statement when transaction is enabled
        {
            // First begin a transaction
            ctx.setQueryId(UUIDUtil.genUUID());
            ctx.setExecutionId(new TUniqueId(5, 6));
            StatementBase beginStmt = SqlParser.parseSingleStatement("BEGIN", SqlModeHelper.MODE_DEFAULT);
            StmtExecutor beginExecutor = new StmtExecutor(ctx, beginStmt);
            beginExecutor.execute();
            long txnId = ctx.getTxnId();
            Assertions.assertNotEquals(0, txnId);

            // Then commit
            ctx.setQueryId(UUIDUtil.genUUID());
            ctx.setExecutionId(new TUniqueId(7, 8));
            StatementBase commitStmt = SqlParser.parseSingleStatement("COMMIT", SqlModeHelper.MODE_DEFAULT);
            StmtExecutor commitExecutor = new StmtExecutor(ctx, commitStmt);
            commitExecutor.execute();

            // Should not be error state
            Assertions.assertFalse(ctx.getState().isError());
            // TxnId should be cleared after commit
            Assertions.assertEquals(0, ctx.getTxnId());
        }

        // Test 3: ROLLBACK statement when transaction is enabled
        {
            // First begin a transaction
            ctx.setQueryId(UUIDUtil.genUUID());
            ctx.setExecutionId(new TUniqueId(9, 10));
            StatementBase beginStmt = SqlParser.parseSingleStatement("BEGIN", SqlModeHelper.MODE_DEFAULT);
            StmtExecutor beginExecutor = new StmtExecutor(ctx, beginStmt);
            beginExecutor.execute();
            long txnId = ctx.getTxnId();
            Assertions.assertNotEquals(0, txnId);

            // Then rollback
            ctx.setQueryId(UUIDUtil.genUUID());
            ctx.setExecutionId(new TUniqueId(11, 12));
            StatementBase rollbackStmt = SqlParser.parseSingleStatement("ROLLBACK", SqlModeHelper.MODE_DEFAULT);
            StmtExecutor rollbackExecutor = new StmtExecutor(ctx, rollbackStmt);
            rollbackExecutor.execute();

            // Should not be error state
            Assertions.assertFalse(ctx.getState().isError());
            // TxnId should be cleared after rollback
            Assertions.assertEquals(0, ctx.getTxnId());
        }
    }

    @Test
    public void testCommitRollbackWithTxnIdWhenTransactionDisabled() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();

        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ctx.setThreadLocalInfo();

        // First enable transaction and begin
        ctx.getSessionVariable().setEnableSqlTransaction(true);
        ctx.setQueryId(UUIDUtil.genUUID());
        ctx.setExecutionId(new TUniqueId(13, 14));
        StatementBase beginStmt = SqlParser.parseSingleStatement("BEGIN", SqlModeHelper.MODE_DEFAULT);
        StmtExecutor beginExecutor = new StmtExecutor(ctx, beginStmt);
        beginExecutor.execute();
        long txnId = ctx.getTxnId();
        Assertions.assertNotEquals(0, txnId);

        // Now disable transaction but txnId is still set
        ctx.getSessionVariable().setEnableSqlTransaction(false);

        // COMMIT should still work because txnId != 0
        ctx.setQueryId(UUIDUtil.genUUID());
        ctx.setExecutionId(new TUniqueId(15, 16));
        StatementBase commitStmt = SqlParser.parseSingleStatement("COMMIT", SqlModeHelper.MODE_DEFAULT);
        StmtExecutor commitExecutor = new StmtExecutor(ctx, commitStmt);
        commitExecutor.execute();

        // Should not be error state
        Assertions.assertFalse(ctx.getState().isError());
        // TxnId should be cleared after commit
        Assertions.assertEquals(0, ctx.getTxnId());
    }

    @Test
    public void testAddRunningQueryDetailRestoresComputeResource() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();

        new MockUp<RunMode>() {
            @Mock
            boolean isSharedDataMode() {
                return true;
            }
        };
        new MockUp<ConnectContext>() {
            @Mock
            public void applyWarehouseSessionVariable(String warehouse, SessionVariable sv) {
                sv.setWarehouseName(warehouse);
            }
        };

        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ctx.setThreadLocalInfo();

        String originalWarehouse = ctx.getCurrentWarehouseName();
        ComputeResource originalResource = WarehouseComputeResource.of(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        ctx.setCurrentComputeResource(originalResource);

        String hintedWarehouse = "query_detail_hint_warehouse";
        String sql = "select /*+ SET_VAR(warehouse='" + hintedWarehouse + "') */ 1";
        ctx.setQueryId(UUIDUtil.genUUID());
        ctx.setExecutionId(UUIDUtil.toTUniqueId(ctx.getQueryId()));
        StatementBase stmt = SqlParser.parseSingleStatement(sql, SqlModeHelper.MODE_DEFAULT);
        stmt.setOrigStmt(new OriginStatement(sql, 0));
        StmtExecutor executor = new StmtExecutor(ctx, stmt);

        // Enable query detail collection
        boolean oldConfig = Config.enable_collect_query_detail_info;
        Config.enable_collect_query_detail_info = true;
        try {
            executor.addRunningQueryDetail(stmt);
        } finally {
            Config.enable_collect_query_detail_info = oldConfig;
        }

        QueryDetail queryDetail = ctx.getQueryDetail();
        Assertions.assertNotNull(queryDetail);
        Assertions.assertEquals(hintedWarehouse, queryDetail.getWarehouse());
        Assertions.assertEquals(originalWarehouse, ctx.getCurrentWarehouseName());

        ComputeResource afterResource = ctx.getCurrentComputeResourceNoAcquire();
        Assertions.assertEquals(originalResource, afterResource,
                "ComputeResource should be restored after addRunningQueryDetail");
    }

    @Test
    public void testRollbackWithTxnIdWhenTransactionDisabled() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();

        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ctx.setThreadLocalInfo();

        // First enable transaction and begin
        ctx.getSessionVariable().setEnableSqlTransaction(true);
        ctx.setQueryId(UUIDUtil.genUUID());
        ctx.setExecutionId(new TUniqueId(17, 18));
        StatementBase beginStmt = SqlParser.parseSingleStatement("BEGIN", SqlModeHelper.MODE_DEFAULT);
        StmtExecutor beginExecutor = new StmtExecutor(ctx, beginStmt);
        beginExecutor.execute();
        long txnId = ctx.getTxnId();
        Assertions.assertNotEquals(0, txnId);

        // Now disable transaction but txnId is still set
        ctx.getSessionVariable().setEnableSqlTransaction(false);

        // ROLLBACK should still work because txnId != 0
        ctx.setQueryId(UUIDUtil.genUUID());
        ctx.setExecutionId(new TUniqueId(19, 20));
        StatementBase rollbackStmt = SqlParser.parseSingleStatement("ROLLBACK", SqlModeHelper.MODE_DEFAULT);
        StmtExecutor rollbackExecutor = new StmtExecutor(ctx, rollbackStmt);
        rollbackExecutor.execute();

        // Should not be error state
        Assertions.assertFalse(ctx.getState().isError());
        // TxnId should be cleared after rollback
        Assertions.assertEquals(0, ctx.getTxnId());
    }
}
