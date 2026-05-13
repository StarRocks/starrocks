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
=======
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.ProfileKeyDictionary;
>>>>>>> e360d41aca ([Enhancement] Allow including EXPLAIN COSTS output in profile (#73005))
import com.starrocks.common.util.ProfileManager;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.mysql.MysqlSerializer;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.AstBuilder;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StmtExecutorTest {

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
    public void testEmbedExplainPlanInProfileScenarios() throws Exception {
        new MockUp<WarehouseManager>() {
            @Mock
            public String getWarehouseComputeResourceName(ComputeResource computeResource) {
                return "default_warehouse";
            }
        };

        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();
        ConnectContext setupCtx = UtFrameUtils.createDefaultCtx();
        StarRocksAssert starRocksAssert = new StarRocksAssert(setupCtx);
        String dbName = "test_explain_in_profile_db";
        starRocksAssert.withDatabase(dbName).useDatabase(dbName);
        starRocksAssert.withTable("CREATE TABLE `embed_explain_t` (\n" +
                "  `k1` int NULL,\n" +
                "  `k2` int NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");");

        String querySql = "SELECT k1, k2 FROM embed_explain_t WHERE k2 = 12345";

        // Case 1: Flag off -> Summary must NOT carry ExplainPlan info-string.
        ConnectContext offCtx = UtFrameUtils.createDefaultCtx();
        offCtx.setDatabase(dbName);
        ConnectContext.threadLocalInfo.set(offCtx);
        offCtx.getSessionVariable().setEnableProfile(true);
        offCtx.getSessionVariable().setEnableExplainInProfile(false);
        ExecPlan offPlan = UtFrameUtils.getPlanAndFragment(offCtx, querySql).second;
        StatementBase offStmt = SqlParser.parseSingleStatement(querySql, SqlModeHelper.MODE_DEFAULT);
        StmtExecutor offExecutor = new StmtExecutor(offCtx, offStmt);
        RuntimeProfile offProfile = Deencapsulation.invoke(offExecutor, "buildTopLevelProfile");
        Deencapsulation.invoke(offExecutor, "maybeEmbedExplainPlanInProfile", offProfile, offPlan);
        RuntimeProfile offSummary = offProfile.getChild("Summary");
        Assertions.assertNotNull(offSummary);
        Assertions.assertNull(offSummary.getInfoString(ProfileKeyDictionary.EXPLAIN_PLAN),
                "ExplainPlan must be absent when enable_explain_in_profile is false");

        // Case 2: Flag on -> Summary should carry ExplainPlan info-string with COSTS-level content
        // produced by the real optimizer (not a synthetic plan).
        ConnectContext onCtx = UtFrameUtils.createDefaultCtx();
        onCtx.setDatabase(dbName);
        ConnectContext.threadLocalInfo.set(onCtx);
        onCtx.getSessionVariable().setEnableProfile(true);
        onCtx.getSessionVariable().setEnableExplainInProfile(true);
        ExecPlan onPlan = UtFrameUtils.getPlanAndFragment(onCtx, querySql).second;
        StatementBase onStmt = SqlParser.parseSingleStatement(querySql, SqlModeHelper.MODE_DEFAULT);
        StmtExecutor onExecutor = new StmtExecutor(onCtx, onStmt);
        RuntimeProfile onProfile = Deencapsulation.invoke(onExecutor, "buildTopLevelProfile");
        Deencapsulation.invoke(onExecutor, "maybeEmbedExplainPlanInProfile", onProfile, onPlan);
        RuntimeProfile onSummary = onProfile.getChild("Summary");
        Assertions.assertNotNull(onSummary);
        String embedded = onSummary.getInfoString(ProfileKeyDictionary.EXPLAIN_PLAN);
        Assertions.assertNotNull(embedded,
                "ExplainPlan must be embedded when enable_explain_in_profile is true");
        Assertions.assertTrue(embedded.contains("PLAN FRAGMENT"),
                "Embedded explain plan should contain rendered fragment headers");
        Assertions.assertTrue(embedded.contains("OlapScanNode"),
                "Embedded explain plan should describe the OLAP scan against the test table");
        Assertions.assertTrue(embedded.contains("embed_explain_t"),
                "Embedded explain plan should reference the table being scanned");
        // COSTS-level output renders cardinality/column-statistics that NORMAL/VERBOSE-only paths skip.
        Assertions.assertTrue(embedded.contains("cardinality:"),
                "Embedded explain plan should render COSTS-level content (cardinality)");
        // Sanity-check the literal predicate is rendered (not digested) when desensitization is off.
        Assertions.assertTrue(embedded.contains("= 12345"),
                "Embedded explain plan should render predicate literals as-is when desensitization is off");

        // Case 3: Flag on + Config.enable_sql_desensitize_in_log -> embedded plan must digest predicate
        // literals via the EnableDigest path, even though the session-local enable_desensitize_explain
        // signal starts off. The helper must also restore the session signal afterward.
        boolean prevConfigDesensitize = Config.enable_sql_desensitize_in_log;
        Config.enable_sql_desensitize_in_log = true;
        try {
            ConnectContext desensitizeCtx = UtFrameUtils.createDefaultCtx();
            desensitizeCtx.setDatabase(dbName);
            ConnectContext.threadLocalInfo.set(desensitizeCtx);
            desensitizeCtx.getSessionVariable().setEnableProfile(true);
            desensitizeCtx.getSessionVariable().setEnableExplainInProfile(true);
            desensitizeCtx.getSessionVariable().setEnableDesensitizeExplain(false);
            ExecPlan desensitizePlan = UtFrameUtils.getPlanAndFragment(desensitizeCtx, querySql).second;
            StatementBase desensitizeStmt = SqlParser.parseSingleStatement(querySql, SqlModeHelper.MODE_DEFAULT);
            StmtExecutor desensitizeExecutor = new StmtExecutor(desensitizeCtx, desensitizeStmt);
            RuntimeProfile desensitizeProfile = Deencapsulation.invoke(desensitizeExecutor, "buildTopLevelProfile");
            Deencapsulation.invoke(desensitizeExecutor, "maybeEmbedExplainPlanInProfile",
                    desensitizeProfile, desensitizePlan);
            String desensitizedEmbedded =
                    desensitizeProfile.getChild("Summary").getInfoString(ProfileKeyDictionary.EXPLAIN_PLAN);
            Assertions.assertNotNull(desensitizedEmbedded);
            // The predicate literal must be digested via the explainExpr path, not rendered as 12345.
            Assertions.assertFalse(desensitizedEmbedded.contains("= 12345"),
                    "Predicate literals must be digested when desensitization is enabled");
            Assertions.assertTrue(desensitizedEmbedded.contains("= ?"),
                    "Predicate literals must be rendered as a digest marker when desensitization is enabled");
            // The helper must restore the previous value of enable_desensitize_explain so that
            // subsequent code in the session does not see a leaked override.
            Assertions.assertFalse(desensitizeCtx.getSessionVariable().isEnableDesensitizeExplain(),
                    "Embedding must restore enable_desensitize_explain after rendering");
        } finally {
            Config.enable_sql_desensitize_in_log = prevConfigDesensitize;
        }
    }

    @Test
>>>>>>> e360d41aca ([Enhancement] Allow including EXPLAIN COSTS output in profile (#73005))
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
    public void testGetRedactedOriginStmtInStringScenarios() {
        // Case 1: Plain SQL should not be rewritten.
        StatementBase plainStmt = SqlParser.parseSingleStatement("SELECT * FROM t0 WHERE id = 1",
                SqlModeHelper.MODE_DEFAULT);
        StmtExecutor plainExecutor = new StmtExecutor(UtFrameUtils.createDefaultCtx(), plainStmt);
        Assertions.assertEquals("SELECT * FROM t0 WHERE id = 1", plainExecutor.getRedactedOriginStmtInString());

        // Case 2: FILES credentials should be redacted.
        StatementBase filesStmt = SqlParser.parseSingleStatement(
                "SELECT * FROM FILES(\"path\"=\"s3://bucket/data.parquet\", " +
                        "\"aws.s3.secret_key\"=\"RETRY_SECRET\")",
                SqlModeHelper.MODE_DEFAULT);
        StmtExecutor filesExecutor = new StmtExecutor(UtFrameUtils.createDefaultCtx(), filesStmt);
        String redacted = filesExecutor.getRedactedOriginStmtInString();
        Assertions.assertFalse(redacted.contains("RETRY_SECRET"));
        Assertions.assertTrue(redacted.contains("***"));
    }

    @Test
    public void testBuildTopLevelProfileSqlStatementScenarios() {
        // Case 1: Plain SQL keeps original text.
        ConnectContext plainCtx = UtFrameUtils.createDefaultCtx();
        ConnectContext.threadLocalInfo.set(plainCtx);
        StatementBase plainStmt = SqlParser.parseSingleStatement("SELECT 1", SqlModeHelper.MODE_DEFAULT);
        StmtExecutor plainExecutor = new StmtExecutor(plainCtx, plainStmt);
        RuntimeProfile plainProfile = com.starrocks.common.jmockit.Deencapsulation.invoke(plainExecutor,
                "buildTopLevelProfile");
        String plainSql = plainProfile.getChild("Summary").getInfoString(ProfileManager.SQL_STATEMENT);
        Assertions.assertEquals("SELECT 1", plainSql);

        // Case 2: Credential SQL is redacted in profile.
        ConnectContext filesCtx = UtFrameUtils.createDefaultCtx();
        ConnectContext.threadLocalInfo.set(filesCtx);
        StatementBase filesStmt = SqlParser.parseSingleStatement(
                "SELECT * FROM FILES(\"path\"=\"s3://bucket/data.parquet\", " +
                        "\"aws.s3.secret_key\"=\"PROFILE_SECRET\")",
                SqlModeHelper.MODE_DEFAULT);
        StmtExecutor filesExecutor = new StmtExecutor(filesCtx, filesStmt);
        RuntimeProfile filesProfile = com.starrocks.common.jmockit.Deencapsulation.invoke(filesExecutor,
                "buildTopLevelProfile");
        String filesSql = filesProfile.getChild("Summary").getInfoString(ProfileManager.SQL_STATEMENT);
        Assertions.assertFalse(filesSql.contains("PROFILE_SECRET"));
        Assertions.assertTrue(filesSql.contains("***"));
    }
}
