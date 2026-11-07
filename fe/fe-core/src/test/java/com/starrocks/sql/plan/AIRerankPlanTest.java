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

import com.starrocks.builtins.VectorizedBuiltinFunctions;
import com.starrocks.catalog.FunctionSet;
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
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.common.AIModelConfigs;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.thrift.TAIProjectNode;
import com.starrocks.thrift.TAIProvider;
import com.starrocks.thrift.TAIProviderProtocol;
import com.starrocks.thrift.TAIProviderType;
import com.starrocks.thrift.TFunctionBinaryType;
import com.starrocks.type.ArrayType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StructType;
import com.starrocks.type.VarcharType;
import com.starrocks.utframe.AIProviderTestUtil;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AIRerankPlanTest extends PlanTestBase {
    private static final String DOCUMENTS = "['first', 'second']";

    @BeforeAll
    public static void createRerankInputsTable() throws Exception {
        starRocksAssert.withTable("CREATE TABLE rerank_inputs (query_text VARCHAR(100), documents ARRAY<VARCHAR(100)>) "
                + "DUPLICATE KEY(query_text) DISTRIBUTED BY HASH(query_text) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1')");
    }

    @BeforeEach
    public void createRerankProvider() {
        AIProviderTestUtil.setRerankProperty("endpoint", "https://rerank.example.test/v2/rerank");
        AIProviderTestUtil.setRerankProperty("model", "registry-model");
        AIProviderTestUtil.setRerankProperty("api_key", "rerank-test-key");
    }

    @AfterEach
    public void clearRerankProvider() {
        AIProviderTestUtil.clearDefaults();
        connectContext.setThreadLocalInfo();
    }

    @Test
    public void testCanonicalSignatureAndNamedArguments() throws Exception {
        for (String arguments : List.of("'query', " + DOCUMENTS,
                "'query', " + DOCUMENTS + ", 'test_default_rerank'",
                "'query', " + DOCUMENTS + ", '', 'override-model'",
                "documents => " + DOCUMENTS + ", query => 'query', model => 'override-model'",
                "provider => NULL, query => 'query', documents => " + DOCUMENTS + ", model => NULL",
                "query => 'query', documents => " + DOCUMENTS + ", options => json_object('top_n', 1)")) {
            FunctionCallExpr call = analyzeCall(arguments);
            Assertions.assertEquals(200170, call.getFn().getFunctionId());
            Assertions.assertEquals(5, call.getChildren().size());
            Assertions.assertEquals(2, AIModelConfigs.getProviderArgument(call.getFn()));
            Assertions.assertEquals(3, AIModelConfigs.getModelArgument(call.getFn()));
            Assertions.assertArrayEquals(new String[] {"query", "documents", "provider", "model", "options"},
                    call.getFn().getArgNames());
            StructType result = (StructType)
                    ((ArrayType) call.getType()).getItemType();
            Assertions.assertEquals("index", result.getFields().get(0).getName());
            Assertions.assertEquals(IntegerType.INT, result.getFields().get(0).getType());
            Assertions.assertEquals("relevance_score", result.getFields().get(1).getName());
            Assertions.assertEquals(FloatType.DOUBLE, result.getFields().get(1).getType());
        }
        FunctionCallExpr defaults = analyzeCall("'query', " + DOCUMENTS + ", NULL, NULL");
        Assertions.assertTrue(AIModelConfigs.shouldUseDefaultProvider(defaults));
        Assertions.assertEquals("", ((StringLiteral) defaults.getChild(2)).getValue());
        Assertions.assertEquals("", ((StringLiteral) defaults.getChild(3)).getValue());
    }

    @Test
    public void testRowInputsAndConstantControls() throws Exception {
        for (String documents : List.of("NULL", "[]", "['']", "[' ', 'a', 'a']", "[1, 2]", "['a', NULL]")) {
            Assertions.assertEquals(200170, analyzeCall("'', " + documents).getFn().getFunctionId());
        }
        Assertions.assertEquals(200170, analyzeCall("NULL, " + DOCUMENTS).getFn().getFunctionId());
        Assertions.assertEquals(1, countCalls(onlyNode(getExecPlan("select ai_rerank(k1, [k1, '']) from t7"))));
        Assertions.assertEquals(1, countCalls(onlyNode(getExecPlan("select ai_rerank('q', docs) from "
                + "(select [k1, ''] docs from t7) t"))));
        assertRejected("select ai_rerank('q', '[]')", "documents must have ARRAY type");
        assertRejected("select ai_rerank('q', parse_json('[]'))", "documents must have ARRAY type");
        assertRejected("select ai_rerank('q', " + DOCUMENTS + ", k1) from t7", "constant provider");
        assertRejected("select ai_rerank('q', " + DOCUMENTS + ", '', k1) from t7", "constant model");
        assertRejected("select ai_rerank(query => 'q', documents => " + DOCUMENTS
                + ", options => parse_json(k1)) from t7", "constant options");
        assertRejected("select ai_rerank(query => 'q', documents => " + DOCUMENTS + ", options => '{}')", "constant JSON");
        assertRejected("select ai_rerank('q', " + DOCUMENTS + ", '  ')", "whitespace");
        assertRejected("select ai_classify('q', [])", "nonempty ARRAY");
        assertRejected("select ai_classify('q', [''])", "nonempty ARRAY");
        assertRejected("select ai_classify('q', [k1]) from t7", "constant ARRAY");
    }

    @Test
    public void testComplexOutputProjectionUnnestAndCte() throws Exception {
        for (String sql : List.of(
                "select (ai_rerank(k1, [k1]))[1].`index` from t7",
                "select (ai_rerank(k1, [k1]))[1].relevance_score from t7",
                "select (ai_rerank(k1, [k1]))[1].`index`, (ai_rerank(k1, [k1]))[1].relevance_score from t7",
                "with ranked as (select ai_rerank(k1, [k1]) hits from t7) select hits[1].`index` from ranked",
                "with ranked as (select ai_rerank(k1, [k1]) hits from t7) "
                        + "select hit.`index`, hit.relevance_score from ranked, unnest(hits) as u(hit)",
                "select hits from (select ai_rerank(k1, [k1]) hits from t7) t order by hits[1].`index`")) {
            ExecPlan plan = getExecPlan(sql);
            AIProjectNode node = onlyNode(plan);
            node.getSlotMap().values().stream().filter(FunctionCallExpr.class::isInstance)
                    .map(FunctionCallExpr.class::cast).forEach(call -> {
                        Assertions.assertEquals(200170, call.getFn().getFunctionId());
                        Assertions.assertTrue(call.getType().isArrayType());
                        Assertions.assertEquals(2, ((StructType)
                                ((ArrayType) call.getType()).getItemType()).getFields().size());
                    });
            Assertions.assertDoesNotThrow(() -> node.treeToThrift());
        }
    }

    @Test
    public void testGeneratedReturnTreesAreIndependent() {
        FunctionSet first = new FunctionSet();
        FunctionSet second = new FunctionSet();
        VectorizedBuiltinFunctions.initBuiltins(first);
        VectorizedBuiltinFunctions.initBuiltins(second);
        ArrayType left = (ArrayType) first.getBuiltinFunctions().stream()
                .filter(fn -> fn.getFunctionId() == 200170).findFirst().orElseThrow().getReturnType();
        ArrayType right = (ArrayType) second.getBuiltinFunctions().stream()
                .filter(fn -> fn.getFunctionId() == 200170).findFirst().orElseThrow().getReturnType();
        Assertions.assertNotSame(left, right);
        Assertions.assertNotSame(left.getItemType(), right.getItemType());
        Assertions.assertNotSame(((StructType) left.getItemType()).getFields().get(0),
                ((StructType) right.getItemType()).getFields().get(0));
    }

    @Test
    public void testColumnDocumentsNativeProtocolAndNestedAi() throws Exception {
        AIProviderTestUtil.setRerankProperty("protocol", "dashscope_rerank");
        AIProjectNode node = onlyNode(getExecPlan("select ai_rerank(query_text, documents) from rerank_inputs"));
        TAIProvider wire = node.treeToThrift().getNodes().get(0).getAi_project_node()
                .getAi_provider_mapping().values().iterator().next();
        Assertions.assertEquals(TAIProviderProtocol.DASHSCOPE_RERANK, wire.getProtocol());
        AIProviderTestUtil.setChatProperty("endpoint", "https://chat.example.test/v1/chat/completions");
        ExecPlan nested = getExecPlan("select ai_rerank(ai_complete(query_text), documents) from rerank_inputs");
        long projects = nested.getFragments().stream().flatMap(fragment -> fragment.collectNodes().stream())
                .filter(AIProjectNode.class::isInstance).count();
        Assertions.assertEquals(2, projects);
    }

    @Test
    public void testProviderTypeAndProtocolAreValidatedBeforePruning() throws Exception {
        AIProviderTestUtil.setChatProperty("endpoint", "https://chat.example.test/v1/chat/completions");
        assertRejected("select ai_rerank('state', " + DOCUMENTS + ", 'test_default_chat')", "requires type RERANK");
        AIProviderTestUtil.setRerankProperty("protocol", "openai");
        assertRejected("select 1 from (select ai_rerank('state', " + DOCUMENTS + ") as unused) t", "DASHSCOPE_RERANK");
    }

    @Test
    public void testOccurrencesAliasAndTypedWire() throws Exception {
        String call = "ai_rerank(k1, " + DOCUMENTS + ")";
        ExecPlan plan = getExecPlan("select " + call + ", " + call + " from t7");
        AIProjectNode node = onlyNode(plan);
        Assertions.assertEquals(2, countCalls(node));
        TAIProjectNode wire = node.treeToThrift().getNodes().get(0).getAi_project_node();
        AIProvider provider = rerankProvider();
        Assertions.assertEquals(Set.of(provider.getId()), wire.getAi_provider_mapping().keySet());
        TAIProvider config = wire.getAi_provider_mapping().get(provider.getId());
        Assertions.assertEquals(TAIProviderType.RERANK, config.getType());
        Assertions.assertEquals(TAIProviderProtocol.COHERE, config.getProtocol());
        Assertions.assertEquals("registry-model", config.getParams().get("model"));
        Assertions.assertEquals("rerank-test-key", config.getParams().get("api_key"));
        node.getSlotMap().values().stream().filter(FunctionCallExpr.class::isInstance)
                .map(FunctionCallExpr.class::cast).forEach(expression -> {
                    Assertions.assertEquals(5, expression.getChildren().size());
                    Assertions.assertEquals(provider.getId(), expression.getAIProviderId());
                });
        ExecPlan alias = getExecPlan("select result, result from (select " + call + " as result from t7) t");
        Assertions.assertEquals(1, countCalls(onlyNode(alias)));
    }

    @Test
    public void testDefaultAndNamedUseMainProviderBindingAndModelOverride() throws Exception {
        ExecPlan plan = getExecPlan("select ai_rerank(k1, " + DOCUMENTS + "), "
                + "ai_rerank(k1, " + DOCUMENTS + ", 'test_default_rerank', 'override-model') from t7");
        AIProjectNode node = onlyNode(plan);
        Map<SlotId, Expr> calls = new HashMap<>(node.getSlotMap());
        Map<String, AIProvider> first = plan.bindAIProviders(calls);
        Assertions.assertEquals(Set.of(rerankProvider().getId()), first.keySet());
        AIProviderTestUtil.setRerankProperty("model", "updated-model");
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
        ExecPlan plan = getExecPlan("select ai_rerank(k1, " + DOCUMENTS + ") from t7");
        AIProjectNode node = onlyNode(plan);
        AIProvider wrong = new AIProvider(rerankProvider());
        wrong.setType(AIProviderType.CHAT);
        Assertions.assertThrows(IllegalStateException.class, () -> new AIProjectNode(plan.getNextNodeId(),
                plan.getDescTbl().getTupleDesc(node.getTupleIds().get(0)), node.getChild(0), node.getSlotMap(),
                node.getCommonSlotMap(), Map.of(wrong.getId(), wrong)));
    }

    @Test
    public void testSameNameUdfIsReusableInsideAIProject() throws Exception {
        ExecPlan plan = getExecPlan("select ai_rerank(k1, " + DOCUMENTS + ") from t7");
        AIProjectNode node = onlyNode(plan);
        ScalarFunction function = ScalarFunction.createVectorizedBuiltin(900001, "ai_rerank",
                List.of(VarcharType.VARCHAR), false, VarcharType.VARCHAR);
        function.setBinaryType(TFunctionBinaryType.SRJAR);
        FunctionCallExpr expression = new FunctionCallExpr("ai_rerank", List.of(new StringLiteral("text")));
        expression.setFn(function);
        expression.setType(VarcharType.VARCHAR);
        Assertions.assertDoesNotThrow(() -> new AIProjectNode(plan.getNextNodeId(),
                plan.getDescTbl().getTupleDesc(node.getTupleIds().get(0)), node.getChild(0), node.getSlotMap(),
                Map.of(new SlotId(900001), expression), Map.of(rerankProvider().getId(), rerankProvider())));
    }

    @Test
    public void testResolvedSqlUdfDoesNotInheritRerankSemantics() throws Exception {
        boolean previous = Config.enable_udf;
        Config.enable_udf = true;
        try {
            execute("CREATE FUNCTION ai_rerank(x STRING) RETURNS upper(x)", connectContext);
            QueryStatement statement = (QueryStatement) UtFrameUtils.parseStmtWithNewParser(
                    "select test.ai_rerank('text')", connectContext);
            FunctionCallExpr call = (FunctionCallExpr) statement.getQueryRelation().getOutputExpression().get(0);
            Assertions.assertInstanceOf(SqlFunction.class, call.getFn());
            Assertions.assertFalse(call.getFn().isAI());
            Assertions.assertFalse(call.isNondeterministicBuiltinFnName());
            Assertions.assertEquals(1, call.getChildren().size());
        } finally {
            execute("DROP FUNCTION IF EXISTS ai_rerank(STRING)", connectContext);
            Config.enable_udf = previous;
        }
    }

    @Test
    public void testRerankRequiresBothFunctionAndProviderUsageBeforePruning() throws Exception {
        String user = "rerank_normalized_caller";
        execute("CREATE USER '" + user + "' IDENTIFIED BY ''", connectContext);
        try {
            ConnectContext caller = UtFrameUtils.initCtxForNewPrivilege(new UserIdentity(user, "%"));
            caller.setCurrentRoleIds(Set.of());
            String sql = "select 1 from (select ai_rerank('state', " + DOCUMENTS + ") as unused) t";
            Assertions.assertThrows(ErrorReportException.class, () -> UtFrameUtils.parseStmtWithNewParser(sql, caller));
            execute("GRANT USAGE ON AI FUNCTION ai_rerank TO '" + user + "'", connectContext);
            Assertions.assertThrows(ErrorReportException.class, () -> UtFrameUtils.parseStmtWithNewParser(sql, caller));
            execute("GRANT USAGE ON AI PROVIDER test_default_rerank TO '" + user + "'", connectContext);
            Assertions.assertDoesNotThrow(() -> UtFrameUtils.parseStmtWithNewParser(sql, caller));
        } finally {
            execute("DROP USER '" + user + "'", connectContext);
        }
    }

    @Test
    public void testInvokerViewRechecksFunctionAndProviderUsage() throws Exception {
        String user = "rerank_view_caller";
        execute("CREATE USER '" + user + "' IDENTIFIED BY ''", connectContext);
        execute("CREATE VIEW rerank_invoker SECURITY INVOKER AS SELECT ai_rerank('q', "
                + DOCUMENTS + ") AS hits", connectContext);
        try {
            ConnectContext caller = UtFrameUtils.initCtxForNewPrivilege(new UserIdentity(user, "%"));
            caller.setCurrentRoleIds(Set.of());
            caller.setDatabase("test");
            execute("GRANT SELECT ON VIEW test.rerank_invoker TO '" + user + "'", connectContext);
            String sql = "select hits[1].`index` from rerank_invoker";
            Assertions.assertThrows(ErrorReportException.class,
                    () -> UtFrameUtils.parseStmtWithNewParser(sql, caller));
            execute("GRANT USAGE ON AI FUNCTION ai_rerank TO '" + user + "'", connectContext);
            Assertions.assertThrows(ErrorReportException.class,
                    () -> UtFrameUtils.parseStmtWithNewParser(sql, caller));
            execute("GRANT USAGE ON AI PROVIDER test_default_rerank TO '" + user + "'", connectContext);
            Assertions.assertDoesNotThrow(() -> UtFrameUtils.parseStmtWithNewParser(sql, caller));
            execute("REVOKE USAGE ON AI FUNCTION ai_rerank FROM '" + user + "'", connectContext);
            Assertions.assertThrows(ErrorReportException.class,
                    () -> UtFrameUtils.parseStmtWithNewParser(sql, caller));
        } finally {
            execute("DROP VIEW IF EXISTS rerank_invoker", connectContext);
            execute("DROP USER '" + user + "'", connectContext);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testQueryDumpDoesNotCaptureCredentialsOrRebindLiveProvider(boolean named) throws Exception {
        String sql = "select ai_rerank('state', " + DOCUMENTS + (named ? ", 'test_default_rerank'" : "") + ")";
        boolean previous = connectContext.getSessionVariable().getEnableQueryDump();
        String database = connectContext.getDatabase();
        connectContext.getSessionVariable().setEnableQueryDump(true);
        try {
            String serialized = getDumpString(sql);
            Assertions.assertFalse(serialized.contains("rerank-test-key"));
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

    private static AIProvider rerankProvider() {
        return GlobalStateMgr.getCurrentState().getAIProviderMgr().getDefaultProvider(AIProviderType.RERANK);
    }

    private FunctionCallExpr analyzeCall(String arguments) throws Exception {
        QueryStatement statement = (QueryStatement) UtFrameUtils.parseStmtWithNewParser(
                "select ai_rerank(" + arguments + ")", connectContext);
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
