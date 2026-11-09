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

import com.starrocks.catalog.ScalarFunction;
import com.starrocks.catalog.SqlFunction;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorReportException;
import com.starrocks.context.ai.AIProvider;
import com.starrocks.context.ai.AIProviderType;
import com.starrocks.planner.AIProjectNode;
import com.starrocks.planner.SlotId;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.common.AIModelConfigs;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.thrift.TAIProjectNode;
import com.starrocks.thrift.TAIProvider;
import com.starrocks.thrift.TAIProviderProtocol;
import com.starrocks.thrift.TAIProviderType;
import com.starrocks.thrift.TFunctionBinaryType;
import com.starrocks.type.VarcharType;
import com.starrocks.utframe.AIProviderTestUtil;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AIDecisionPlanTest extends PlanTestBase {
    private static final String QUESTIONS = "parse_json('{\"q\":{\"type\":\"noul\"}}')";

    @BeforeEach
    public void createDecisionProvider() {
        AIProviderTestUtil.setDecisionProperty("endpoint", "https://decision.example.test/v1/systemone");
        AIProviderTestUtil.setDecisionProperty("model", "registry-model");
        AIProviderTestUtil.setDecisionProperty("api_key", "decision-test-key");
    }

    @AfterEach
    public void clearDecisionProvider() {
        AIProviderTestUtil.clearDefaults();
        connectContext.setThreadLocalInfo();
    }

    @Test
    public void testCanonicalSignaturesAndNamedArguments() throws Exception {
        for (String state : List.of("'state'", "parse_json('{}')")) {
            long id = state.equals("'state'") ? 200160 : 200161;
            for (String arguments : List.of(state + ", " + QUESTIONS,
                    state + ", " + QUESTIONS + ", 'test_default_decision'",
                    state + ", " + QUESTIONS + ", '', 'override-model'",
                    "questions => " + QUESTIONS + ", state => " + state + ", model => 'override-model'",
                    "provider => NULL, state => " + state + ", questions => " + QUESTIONS + ", model => NULL")) {
                FunctionCallExpr call = analyzeCall(arguments);
                Assertions.assertEquals(id, call.getFn().getFunctionId());
                Assertions.assertEquals(4, call.getChildren().size());
                Assertions.assertEquals(2, AIModelConfigs.getProviderArgument(call.getFn()));
                Assertions.assertEquals(3, AIModelConfigs.getModelArgument(call.getFn()));
                Assertions.assertArrayEquals(new String[] {"state", "questions", "provider", "model"},
                        call.getFn().getArgNames());
                Assertions.assertTrue(call.getType().isJsonType());
            }
        }
        FunctionCallExpr defaults = analyzeCall("'state', " + QUESTIONS + ", NULL, NULL");
        Assertions.assertTrue(AIModelConfigs.shouldUseDefaultProvider(defaults));
        Assertions.assertEquals("", ((StringLiteral) defaults.getChild(2)).getValue());
        Assertions.assertEquals("", ((StringLiteral) defaults.getChild(3)).getValue());
    }

    @Test
    public void testTypedInputsAndConstantControls() throws Exception {
        assertRejected("select ai_decide(1, " + QUESTIONS + ")", "state must have VARCHAR or JSON type");
        assertRejected("select ai_decide('state', '{}')", "questions must have JSON type");
        assertRejected("select ai_decide(k1, parse_json(k1)) from t7", "questions must be constant");
        assertRejected("select ai_decide('state', " + QUESTIONS + ", k1) from t7", "constant provider");
        assertRejected("select ai_decide('state', " + QUESTIONS + ", '', k1) from t7", "constant model");
        assertRejected("select ai_decide('state', " + QUESTIONS + ", '  ')", "whitespace");
        assertRejected("select ai_decide('state', " + QUESTIONS + ", '', '  ')", "whitespace");
        FunctionCallExpr nullState = analyzeCall("NULL, " + QUESTIONS);
        Assertions.assertInstanceOf(NullLiteral.class, ExprUtils.analyzeAndCastFold(nullState.getChild(0).clone()));
        FunctionCallExpr nullQuestions = analyzeCall("'state', NULL");
        Assertions.assertTrue(nullQuestions.getFn().getArgs()[1].isJsonType());
        Assertions.assertInstanceOf(NullLiteral.class, ExprUtils.analyzeAndCastFold(nullQuestions.getChild(1).clone()));
    }

    @Test
    public void testProviderTypeAndProtocolAreValidatedBeforePruning() throws Exception {
        AIProviderTestUtil.setChatProperty("endpoint", "https://chat.example.test/v1/chat/completions");
        assertRejected("select ai_decide('state', " + QUESTIONS + ", 'test_default_chat')", "requires type DECISION");
        AIProviderTestUtil.setDecisionProperty("protocol", "openai");
        assertRejected("select 1 from (select ai_decide('state', " + QUESTIONS + ") as unused) t", "BAILIAN_DECISION");
    }

    @Test
    public void testOccurrencesAliasAndTypedWire() throws Exception {
        String call = "ai_decide(k1, " + QUESTIONS + ")";
        ExecPlan plan = getExecPlan("select " + call + ", " + call + " from t7");
        AIProjectNode node = onlyNode(plan);
        Assertions.assertEquals(2, countCalls(node));
        TAIProjectNode wire = node.treeToThrift().getNodes().get(0).getAi_project_node();
        AIProvider provider = decisionProvider();
        Assertions.assertEquals(Set.of(provider.getId()), wire.getAi_provider_mapping().keySet());
        TAIProvider config = wire.getAi_provider_mapping().get(provider.getId());
        Assertions.assertEquals(TAIProviderType.DECISION, config.getType());
        Assertions.assertEquals(TAIProviderProtocol.BAILIAN_DECISION, config.getProtocol());
        Assertions.assertEquals("registry-model", config.getParams().get("model"));
        Assertions.assertEquals("decision-test-key", config.getParams().get("api_key"));
        node.getSlotMap().values().stream().filter(FunctionCallExpr.class::isInstance)
                .map(FunctionCallExpr.class::cast).forEach(expression -> {
                    Assertions.assertEquals(4, expression.getChildren().size());
                    Assertions.assertEquals(provider.getId(), expression.getAIProviderId());
                });
        ExecPlan alias = getExecPlan("select result, result from (select " + call + " as result from t7) t");
        Assertions.assertEquals(1, countCalls(onlyNode(alias)));
    }

    @Test
    public void testDefaultAndNamedUseMainProviderBindingAndModelOverride() throws Exception {
        ExecPlan plan = getExecPlan("select ai_decide(k1, " + QUESTIONS + "), "
                + "ai_decide(k1, " + QUESTIONS + ", 'test_default_decision', 'override-model') from t7");
        AIProjectNode node = onlyNode(plan);
        Map<SlotId, Expr> calls = new HashMap<>(node.getSlotMap());
        Map<String, AIProvider> first = plan.bindAIProviders(calls);
        Assertions.assertEquals(Set.of(decisionProvider().getId()), first.keySet());
        AIProviderTestUtil.setDecisionProperty("model", "updated-model");
        Map<String, AIProvider> second = plan.bindAIProviders(new HashMap<>(node.getSlotMap()));
        Assertions.assertEquals("registry-model", first.values().iterator().next().getModel());
        Assertions.assertEquals("updated-model", second.values().iterator().next().getModel());
        Assertions.assertEquals("registry-model", node.treeToThrift().getNodes().get(0).getAi_project_node()
                .getAi_provider_mapping().values().iterator().next().getParams().get("model"));
        Assertions.assertTrue(calls.values().stream().filter(FunctionCallExpr.class::isInstance)
                .map(FunctionCallExpr.class::cast).anyMatch(call -> call.getChild(3) instanceof StringLiteral literal
                        && literal.getValue().equals("override-model")));
    }

    @Test
    public void testAIProjectRejectsWrongProviderType() throws Exception {
        ExecPlan plan = getExecPlan("select ai_decide(k1, " + QUESTIONS + ") from t7");
        AIProjectNode node = onlyNode(plan);
        AIProvider wrong = new AIProvider(decisionProvider());
        wrong.setType(AIProviderType.CHAT);
        Assertions.assertThrows(IllegalStateException.class, () -> new AIProjectNode(plan.getNextNodeId(),
                plan.getDescTbl().getTupleDesc(node.getTupleIds().get(0)), node.getChild(0), node.getSlotMap(),
                node.getCommonSlotMap(), Map.of(wrong.getId(), wrong)));
    }

    @Test
    public void testSameNameUdfIsReusableInsideAIProject() throws Exception {
        ExecPlan plan = getExecPlan("select ai_decide(k1, " + QUESTIONS + ") from t7");
        AIProjectNode node = onlyNode(plan);
        ScalarFunction function = ScalarFunction.createVectorizedBuiltin(900001, "ai_decide",
                List.of(VarcharType.VARCHAR), false, VarcharType.VARCHAR);
        function.setBinaryType(TFunctionBinaryType.SRJAR);
        FunctionCallExpr expression = new FunctionCallExpr("ai_decide", List.of(new StringLiteral("text")));
        expression.setFn(function);
        expression.setType(VarcharType.VARCHAR);
        Assertions.assertDoesNotThrow(() -> new AIProjectNode(plan.getNextNodeId(),
                plan.getDescTbl().getTupleDesc(node.getTupleIds().get(0)), node.getChild(0), node.getSlotMap(),
                Map.of(new SlotId(900001), expression), Map.of(decisionProvider().getId(), decisionProvider())));
    }

    @Test
    public void testResolvedSqlUdfDoesNotInheritDecisionSemantics() throws Exception {
        boolean previous = Config.enable_udf;
        Config.enable_udf = true;
        try {
            execute("CREATE FUNCTION ai_decide(x STRING) RETURNS upper(x)", connectContext);
            QueryStatement statement = (QueryStatement) UtFrameUtils.parseStmtWithNewParser(
                    "select test.ai_decide('text')", connectContext);
            FunctionCallExpr call = (FunctionCallExpr) statement.getQueryRelation().getOutputExpression().get(0);
            Assertions.assertInstanceOf(SqlFunction.class, call.getFn());
            Assertions.assertFalse(call.getFn().isAI());
            Assertions.assertFalse(call.isNondeterministicBuiltinFnName());
            Assertions.assertEquals(1, call.getChildren().size());
        } finally {
            execute("DROP FUNCTION IF EXISTS ai_decide(STRING)", connectContext);
            Config.enable_udf = previous;
        }
    }

    @Test
    public void testDecisionRequiresBothFunctionAndProviderUsageBeforePruning() throws Exception {
        String user = "decision_normalized_caller";
        execute("CREATE USER '" + user + "' IDENTIFIED BY ''", connectContext);
        try {
            ConnectContext caller = UtFrameUtils.initCtxForNewPrivilege(new UserIdentity(user, "%"));
            caller.setCurrentRoleIds(Set.of());
            String sql = "select 1 from (select ai_decide('state', " + QUESTIONS + ") as unused) t";
            Assertions.assertThrows(ErrorReportException.class, () -> UtFrameUtils.parseStmtWithNewParser(sql, caller));
            execute("GRANT USAGE ON AI FUNCTION ai_decide TO '" + user + "'", connectContext);
            Assertions.assertThrows(ErrorReportException.class, () -> UtFrameUtils.parseStmtWithNewParser(sql, caller));
            execute("GRANT USAGE ON AI PROVIDER test_default_decision TO '" + user + "'", connectContext);
            Assertions.assertDoesNotThrow(() -> UtFrameUtils.parseStmtWithNewParser(sql, caller));
        } finally {
            execute("DROP USER '" + user + "'", connectContext);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testQueryDumpDoesNotCaptureCredentialsOrRebindLiveProvider(boolean named) throws Exception {
        String sql = "select ai_decide('state', " + QUESTIONS + (named ? ", 'test_default_decision'" : "") + ")";
        boolean previous = connectContext.getSessionVariable().getEnableQueryDump();
        String database = connectContext.getDatabase();
        connectContext.getSessionVariable().setEnableQueryDump(true);
        try {
            String serialized = getDumpString(sql);
            Assertions.assertFalse(serialized.contains("decision-test-key"));
            Assertions.assertFalse(serialized.contains("ai_provider_mapping"));
            QueryDumpInfo dump = new QueryDumpInfo(connectContext);
            dump.setOriginStmt(sql);
            SemanticException error = Assertions.assertThrows(SemanticException.class,
                    () -> UtFrameUtils.getPlanFragmentFromQueryDump(connectContext, dump));
            Assertions.assertTrue(error.getMessage().contains("Query dump replay does not support AI providers"));
        } finally {
            connectContext.getSessionVariable().setEnableQueryDump(previous);
            connectContext.setDatabase(database);
        }
    }

    private static AIProvider decisionProvider() {
        return GlobalStateMgr.getCurrentState().getAIProviderMgr().getDefaultProvider(AIProviderType.DECISION);
    }

    private FunctionCallExpr analyzeCall(String arguments) throws Exception {
        QueryStatement statement = (QueryStatement) UtFrameUtils.parseStmtWithNewParser(
                "select ai_decide(" + arguments + ")", connectContext);
        return (FunctionCallExpr) statement.getQueryRelation().getOutputExpression().get(0);
    }

    private void assertRejected(String sql, String message) {
        Exception error = Assertions.assertThrows(Exception.class, () -> getExecPlan(sql));
        Assertions.assertTrue(error.getMessage().contains(message), error.getMessage());
    }

    private static AIProjectNode onlyNode(ExecPlan plan) {
        List<AIProjectNode> nodes = plan.getFragments().stream().flatMap(fragment -> fragment.collectNodes().stream())
                .filter(AIProjectNode.class::isInstance).map(AIProjectNode.class::cast).toList();
        Assertions.assertEquals(1, nodes.size());
        return nodes.get(0);
    }

    private static long countCalls(AIProjectNode node) {
        return node.getSlotMap().values().stream().filter(FunctionCallExpr.class::isInstance).count();
    }

    private static void execute(String sql, ConnectContext context) throws Exception {
        context.setThreadLocalInfo();
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(sql, context), context);
    }
}
