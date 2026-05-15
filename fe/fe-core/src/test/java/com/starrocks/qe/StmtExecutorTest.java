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
import com.starrocks.common.FeConstants;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.ProfileManager;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.mysql.MysqlSerializer;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.AstBuilder;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.utframe.StarRocksAssert;
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
    public void testEmbedExplainPlanInProfileScenarios() throws Exception {
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
        Assertions.assertNull(offSummary.getInfoString("ExplainPlan"),
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
        String embedded = onSummary.getInfoString("ExplainPlan");
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
