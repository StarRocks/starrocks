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

package com.starrocks.sql.plan;

import com.starrocks.common.Config;
import com.starrocks.context.ai.AIProviderType;
import com.starrocks.planner.AIProjectNode;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.AIProviderMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.txn.BeginStmt;
import com.starrocks.sql.common.AIModelConfigs;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.transaction.ExplicitTxnState;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import com.starrocks.transaction.TransactionStmtExecutor;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.Map;

public class AIInputTokenPlanTest extends PlanTestBase {
    private static String oldEndpoint;
    private static String oldModel;
    private static String oldProvider;
    private long oldLimit;
    private StatementBase.ExplainLevel oldExplainLevel;

    @BeforeAll
    public static void configureAI() throws Exception {
        oldEndpoint = Config.ai_default_chat_endpoint;
        oldModel = Config.ai_default_chat_model;
        oldProvider = Config.ai_default_chat_provider;
        Config.ai_default_chat_endpoint = "https://unit.test.example/v1/chat/completions";
        Config.ai_default_chat_model = "unit-test-model";
        Config.ai_default_chat_provider = AIModelConfigs.OPENAI_COMPATIBLE_PROVIDER;
        starRocksAssert.withTable("CREATE TABLE ai_admission_sink (v VARCHAR(1000)) "
                + "DISTRIBUTED BY HASH(v) BUCKETS 1 PROPERTIES ('replication_num'='1')");
    }

    @BeforeEach
    public void saveAdmissionState() {
        oldLimit = Config.ai_query_admission_max_estimated_input_tokens;
        oldExplainLevel = connectContext.getExplainLevel();
        connectContext.getState().reset();
    }

    @AfterEach
    public void restoreAdmissionState() {
        Config.ai_query_admission_max_estimated_input_tokens = oldLimit;
        connectContext.setExplainLevel(oldExplainLevel);
    }

    @AfterAll
    public static void restoreAI() {
        Config.ai_default_chat_endpoint = oldEndpoint;
        Config.ai_default_chat_model = oldModel;
        Config.ai_default_chat_provider = oldProvider;
    }

    @Test
    public void testLiteralEstimateInExplainCosts() throws Exception {
        ExecPlan plan = getExecPlan("select ai_complete('hello')");
        Assertions.assertTrue(plan.getExplainString(TExplainLevel.COSTS).contains("AI INPUT TOKENS: 21"));
        Assertions.assertFalse(plan.getExplainString(TExplainLevel.NORMAL).contains("AI INPUT TOKENS"));
    }

    @Test
    public void testUnknownDoesNotExposePartialEstimate() throws Exception {
        ExecPlan plan = getExecPlan("select ai_complete('hello'), ai_summarize('hello')");
        Assertions.assertTrue(plan.getExplainString(TExplainLevel.COSTS).contains("AI INPUT TOKENS: UNKNOWN"));
    }

    @Test
    public void testOrdinaryQueriesAreUnchanged() throws Exception {
        Config.ai_query_admission_max_estimated_input_tokens = 1;
        ExecPlan plan = planSql("select 1");
        Assertions.assertEquals(AIInputTokenEstimate.Status.NONE, plan.getAIInputTokenEstimate().getStatus());
        Assertions.assertFalse(plan.getExplainString(TExplainLevel.COSTS).contains("AI INPUT TOKENS"));
    }

    @Test
    public void testPlanningRejectsBeforeReturningAnExecutablePlan() {
        Config.ai_query_admission_max_estimated_input_tokens = 10;
        StarRocksPlannerException failure = Assertions.assertThrows(StarRocksPlannerException.class,
                () -> planSql("select ai_complete('hello')"));
        Assertions.assertEquals(ErrorType.USER_ERROR, failure.getType());
        Assertions.assertTrue(failure.getMessage().contains("Tokens allowed: 10"), failure.getMessage());
        Assertions.assertTrue(failure.getMessage().contains("estimated tokens: 21"), failure.getMessage());
        Assertions.assertTrue(failure.getMessage().contains("ai_query_admission_max_estimated_input_tokens"));
    }

    @Test
    public void testPlanningChecksTheAggregateAndUnknownEstimate() {
        Config.ai_query_admission_max_estimated_input_tokens = 30;
        StarRocksPlannerException aggregate = Assertions.assertThrows(StarRocksPlannerException.class,
                () -> planSql("select ai_complete('hello'), ai_complete('world!')"));
        Assertions.assertTrue(aggregate.getMessage().contains("estimated tokens: 43"), aggregate.getMessage());
        StarRocksPlannerException unknown = Assertions.assertThrows(StarRocksPlannerException.class,
                () -> planSql("select ai_summarize('hello')"));
        Assertions.assertTrue(unknown.getMessage().contains("UNKNOWN"), unknown.getMessage());
        Assertions.assertTrue(unknown.getMessage().contains("unsupported prompt template"), unknown.getMessage());
    }

    @Test
    public void testPlanningChecksAllAIProjectsBeforeAdmission() {
        String sql = "select ai_complete('hello') union all select ai_complete('world!')";
        Config.ai_query_admission_max_estimated_input_tokens = 43;
        ExecPlan plan = planSql(sql);
        Assertions.assertEquals(2, plan.getFragments().stream()
                .flatMap(fragment -> fragment.collectNodes().stream()).filter(AIProjectNode.class::isInstance).count());
        Assertions.assertEquals(BigInteger.valueOf(43), plan.getAIInputTokenEstimate().getTokens());

        Config.ai_query_admission_max_estimated_input_tokens = 30;
        StarRocksPlannerException failure = Assertions.assertThrows(StarRocksPlannerException.class, () -> planSql(sql));
        Assertions.assertEquals(ErrorType.USER_ERROR, failure.getType());
        Assertions.assertTrue(failure.getMessage().contains("estimated tokens: 43"), failure.getMessage());
    }

    @Test
    public void testPlanningUsesCurrentStatementExplainLevelAndRestoresContext() {
        Config.ai_query_admission_max_estimated_input_tokens = 10;
        connectContext.setExplainLevel(null);
        for (String prefix : new String[] {"explain ", "explain verbose ", "explain costs ", "explain scheduler "}) {
            ExecPlan plan = Assertions.assertDoesNotThrow(() -> planSql(prefix + "select ai_complete('hello')"));
            Assertions.assertEquals(BigInteger.valueOf(21), plan.getAIInputTokenEstimate().getTokens());
            Assertions.assertNull(connectContext.getExplainLevel());
        }
        Assertions.assertThrows(StarRocksPlannerException.class,
                () -> planSql("explain analyze select ai_complete('hello')"));
        Assertions.assertNull(connectContext.getExplainLevel());
    }

    @Test
    public void testPlanningDoesNotInheritOuterExplainExemption() {
        Config.ai_query_admission_max_estimated_input_tokens = 10;
        connectContext.setExplainLevel(StatementBase.ExplainLevel.COSTS);
        Assertions.assertThrows(StarRocksPlannerException.class, () -> planSql("select ai_complete('hello')"));
        Assertions.assertEquals(StatementBase.ExplainLevel.COSTS, connectContext.getExplainLevel());
        Config.ai_query_admission_max_estimated_input_tokens = 21;
        Assertions.assertDoesNotThrow(() -> planSql("select ai_complete('hello')"));
        Assertions.assertEquals(StatementBase.ExplainLevel.COSTS, connectContext.getExplainLevel());
    }

    @Test
    public void testPlanningRejectionIsAUserErrorThroughExecutor() throws Exception {
        Config.ai_query_admission_max_estimated_input_tokens = 10;
        StmtExecutor executor = new StmtExecutor(connectContext, parseSql("select ai_complete('hello')"));
        executor.execute();
        Assertions.assertTrue(connectContext.getState().isError());
        Assertions.assertTrue(connectContext.getState().getErrorMessage().contains("Tokens allowed: 10"),
                connectContext.getState().getErrorMessage());
        Assertions.assertEquals(QueryState.ErrType.ANALYSIS_ERR, connectContext.getState().getErrType());
    }

    @Test
    public void testPlanningHintSubqueryCannotInheritExplainExemption() throws Exception {
        Config.ai_query_admission_max_estimated_input_tokens = 10;
        connectContext.setExplainLevel(StatementBase.ExplainLevel.COSTS);
        for (String prefix : new String[] {"", "explain "}) {
            connectContext.getState().reset();
            StmtExecutor executor = new StmtExecutor(connectContext,
                    parseSql(prefix + "select /*+ SET_USER_VARIABLE(@x=ai_complete('hello')) */ @x"));
            executor.execute();
            Assertions.assertTrue(connectContext.getState().isError());
            Assertions.assertTrue(connectContext.getState().getErrorMessage().contains("Tokens allowed: 10"),
                    connectContext.getState().getErrorMessage());
        }
    }

    @Test
    public void testPlanningRejectionAbortsImplicitTransaction() {
        Config.ai_query_admission_max_estimated_input_tokens = 10;
        InsertStmt insert = (InsertStmt) parseSql("insert into ai_admission_sink select ai_complete('hello')");
        Assertions.assertThrows(StarRocksPlannerException.class, () -> StatementPlanner.plan(insert, connectContext));
        long dbId = connectContext.getGlobalStateMgr().getLocalMetastore().getDb("test").getId();
        TransactionState transaction = connectContext.getGlobalStateMgr().getGlobalTransactionMgr()
                .getTransactionState(dbId, insert.getTxnId());
        Assertions.assertNotNull(transaction);
        Assertions.assertEquals(TransactionStatus.ABORTED, transaction.getTransactionStatus());
    }

    @Test
    public void testPlanningRejectionPreservesUnboundExplicitTransaction() {
        Config.ai_query_admission_max_estimated_input_tokens = 10;
        GlobalTransactionMgr manager = connectContext.getGlobalStateMgr().getGlobalTransactionMgr();
        TransactionStmtExecutor.beginStmt(connectContext, (BeginStmt) parseSql("begin"));
        long txnId = connectContext.getTxnId();
        ExplicitTxnState original = manager.getExplicitTxnState(txnId);
        try {
            Assertions.assertThrows(StarRocksPlannerException.class,
                    () -> planSql("insert into ai_admission_sink select ai_complete('hello')"));
            Assertions.assertEquals(txnId, connectContext.getTxnId());
            Assertions.assertSame(original, manager.getExplicitTxnState(txnId));
            Assertions.assertEquals(0, original.getTransactionState().getDbId());
            Assertions.assertEquals(TransactionStatus.PREPARE, original.getTransactionState().getTransactionStatus());
            Assertions.assertTrue(original.getModifiedTableIds().isEmpty());
            Assertions.assertTrue(original.getTransactionStateItems().isEmpty());
        } finally {
            manager.clearExplicitTxnState(txnId);
            connectContext.setTxnId(0);
        }
    }

    private StatementBase parseSql(String sql) {
        return SqlParser.parseSingleStatement(sql, connectContext.getSessionVariable().getSqlMode());
    }

    private ExecPlan planSql(String sql) {
        return StatementPlanner.plan(parseSql(sql), connectContext);
    }

    @Test
    public void testProviderUsesTheSamePromptEstimate() throws Exception {
        AIProviderMgr manager = connectContext.getGlobalStateMgr().getAIProviderMgr();
        manager.createProvider("token_test_chat", AIProviderType.CHAT,
                Map.of("endpoint", "https://unit.test.example/v1/chat/completions", "model", "chat-model"), "");
        manager.createProvider("token_test_embed", AIProviderType.EMBEDDING,
                Map.of("endpoint", "https://unit.test.example/v1/embeddings", "model", "embed-model"), "");
        try {
            Assertions.assertEquals(BigInteger.valueOf(26), getExecPlan("select ai_custom_query('token_test_chat', '中🙂abc')")
                    .getAIInputTokenEstimate().getTokens());
            Assertions.assertEquals(BigInteger.TEN, getExecPlan("select ai_custom_embedding('token_test_embed', '中🙂abc')")
                    .getAIInputTokenEstimate().getTokens());
            Assertions.assertEquals(BigInteger.valueOf(26), getExecPlan("select ai_complete('other-model', '中🙂abc')")
                    .getAIInputTokenEstimate().getTokens());
        } finally {
            manager.dropProvider("token_test_chat", true);
            manager.dropProvider("token_test_embed", true);
        }
    }

    @Test
    public void testBudgetIsCheckedForEachNewPhysicalPlan() {
        Config.ai_query_admission_max_estimated_input_tokens = 21;
        ExecPlan original = Assertions.assertDoesNotThrow(() -> planSql("select ai_complete('hello')"));
        Config.ai_query_admission_max_estimated_input_tokens = 10;
        Assertions.assertThrows(StarRocksPlannerException.class, () -> planSql("select ai_complete('hello')"));
        Assertions.assertEquals(BigInteger.valueOf(21), original.getAIInputTokenEstimate().getTokens());
    }

    @Test
    public void testActualGlobalAndLocalTopNScopes() throws Exception {
        long oldThreshold = connectContext.getSessionVariable().getAiTopnPushdownMaxGlobalLimit();
        boolean oldEnabled = connectContext.getSessionVariable().isEnableAiTopnPushdown();
        UtFrameUtils.addMockBackend(10002);
        try {
            connectContext.getSessionVariable().setEnableAiTopnPushdown(true);
            connectContext.getSessionVariable().setAiTopnPushdownMaxGlobalLimit(1000);
            String sql = "select k1, ai_complete('hello') from t7 order by k1 limit 5";
            AIInputTokenEstimate global = getExecPlan(sql).getAIInputTokenEstimate();
            Assertions.assertEquals(AIInputTokenEstimate.Status.ESTIMATED, global.getStatus(), global.toString());
            Assertions.assertEquals(BigInteger.valueOf(105), global.getTokens());
            connectContext.getSessionVariable().setAiTopnPushdownMaxGlobalLimit(0);
            AIInputTokenEstimate local = getExecPlan(sql).getAIInputTokenEstimate();
            Assertions.assertEquals(AIInputTokenEstimate.Status.UNKNOWN, local.getStatus(), local.toString());
        } finally {
            connectContext.getSessionVariable().setAiTopnPushdownMaxGlobalLimit(oldThreshold);
            connectContext.getSessionVariable().setEnableAiTopnPushdown(oldEnabled);
            UtFrameUtils.dropMockBackend(10002);
        }
    }

    @Test
    public void testOptionsAndNestedAIOutputsAreUnknown() throws Exception {
        for (String sql : new String[] {"select ai_complete('hello', map{'temperature':0.5})",
                "select ai_complete(ai_complete('hello'))"}) {
            Assertions.assertEquals(AIInputTokenEstimate.Status.UNKNOWN, getExecPlan(sql).getAIInputTokenEstimate().getStatus());
        }
    }
}
