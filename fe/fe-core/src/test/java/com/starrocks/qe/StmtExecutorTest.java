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

import com.starrocks.common.Config;
<<<<<<< HEAD
import com.starrocks.mysql.MysqlSerializer;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.AstBuilder;
import com.starrocks.sql.parser.SqlParser;
=======
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.ProfileManager;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.mysql.MysqlSerializer;
import com.starrocks.planner.DataPartition;
import com.starrocks.planner.DescriptorTable;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.ScanNode;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.proto.PQueryStatistics;
import com.starrocks.qe.QueryDetail.QueryMemState;
import com.starrocks.qe.QueryState.MysqlStateType;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.AnalyzerUtils;
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
>>>>>>> cd8e9f362a ([BugFix] insert files credential redaction (#71245))
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

<<<<<<< HEAD
=======
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

>>>>>>> cd8e9f362a ([BugFix] insert files credential redaction (#71245))
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
    public void testIsForwardToLeader(@Mocked GlobalStateMgr state) {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = state;

                state.isInTransferringToLeader();
                times = 1;
                result = true;

                state.getSqlParser();
                result = new SqlParser(AstBuilder.getInstance());

                state.isLeader();
                times = 2;
                result = false;
                result = true;
            }
        };

        Assertions.assertFalse(new StmtExecutor(new ConnectContext(),
                SqlParser.parseSingleStatement("show frontends", SqlModeHelper.MODE_DEFAULT)).isForwardToLeader());
    }

    @Test
    public void testForwardExplicitTxnSelectOnFollower(@Mocked GlobalStateMgr state,
                                                       @Mocked ConnectContext ctx) {
        StatementBase stmt;
        MysqlSerializer serializer = MysqlSerializer.newInstance();

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = state;

                state.getSqlParser();
                minTimes = 0;
                result = new SqlParser(AstBuilder.getInstance());

                state.isLeader();
                minTimes = 0;
                result = false;

                state.isInTransferringToLeader();
                minTimes = 0;
                result = false;

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
<<<<<<< HEAD
=======
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

        // Case 3: Desensitize path should digest literals.
        boolean oldDesensitize = Config.enable_sql_desensitize_in_log;
        Config.enable_sql_desensitize_in_log = true;
        try {
            ConnectContext desensitizeCtx = UtFrameUtils.createDefaultCtx();
            ConnectContext.threadLocalInfo.set(desensitizeCtx);
            StatementBase desensitizeStmt = SqlParser.parseSingleStatement(
                    "SELECT * FROM t0 WHERE k1 = 12345",
                    SqlModeHelper.MODE_DEFAULT);
            StmtExecutor desensitizeExecutor = new StmtExecutor(desensitizeCtx, desensitizeStmt);
            RuntimeProfile desensitizeProfile = Deencapsulation.invoke(desensitizeExecutor, "buildTopLevelProfile");
            RuntimeProfile desensitizeSummaryProfile = desensitizeProfile.getChild("Summary");
            String desensitizedSqlInProfile = desensitizeSummaryProfile.getInfoString(ProfileManager.SQL_STATEMENT);
            Assertions.assertNotNull(desensitizedSqlInProfile);
            Assertions.assertFalse(desensitizedSqlInProfile.contains("12345"),
                    "Digest mode should desensitize SQL literals in profile");
        } finally {
            Config.enable_sql_desensitize_in_log = oldDesensitize;
        }

        // Case 4: Credential marker path should redact FILES secret.
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
>>>>>>> cd8e9f362a ([BugFix] insert files credential redaction (#71245))
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
}
