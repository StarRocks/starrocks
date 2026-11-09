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

import com.google.common.base.Stopwatch;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.connector.ConnectorProperties;
import com.starrocks.connector.ConnectorType;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.MockedMetadataMgr;
import com.starrocks.connector.paimon.PaimonMetadata;
import com.starrocks.context.ai.AIProvider;
import com.starrocks.planner.AIProjectNode;
import com.starrocks.planner.AnalyticEvalNode;
import com.starrocks.planner.ExchangeNode;
import com.starrocks.planner.JoinNode;
import com.starrocks.planner.PaimonScanNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.ProjectNode;
import com.starrocks.planner.SlotId;
import com.starrocks.planner.SortNode;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.Explain;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.DropCatalogStmt;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.FunctionParams;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.optimizer.Memo;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.SortPhase;
import com.starrocks.sql.optimizer.operator.logical.LogicalAIProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalLimitOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.MockOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalAIProjectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalLimitOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.Binder;
import com.starrocks.sql.optimizer.rule.transformation.MergeProjectWithChildRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownLimitDirectRule;
import com.starrocks.sql.optimizer.rule.transformation.SplitLimitRule;
import com.starrocks.thrift.TAIProjectNode;
import com.starrocks.thrift.TAIProvider;
import com.starrocks.thrift.TAIProviderProtocol;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TExpr;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.thrift.TFunctionBinaryType;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TPrimitiveType;
import com.starrocks.type.FileType;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.AIProviderTestUtil;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.BlobType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class AIProjectPlanTest extends PlanTestBase {
    private static String oldEndpoint;
    private static String oldModel;
    private static String oldProvider;

    @BeforeAll
    public static void setUpAIConfiguration() {
        oldEndpoint = AIProviderTestUtil.chatProperty("endpoint");
        oldModel = AIProviderTestUtil.chatProperty("model");
        oldProvider = AIProviderTestUtil.chatProperty("protocol");
        AIProviderTestUtil.setChatProperty("endpoint", "https://unit.test.example/v1/chat/completions");
        AIProviderTestUtil.setChatProperty("model", "unit-test-model");
        AIProviderTestUtil.setChatProperty("protocol", "openai");
    }

    @AfterAll
    public static void restoreAIConfiguration() {
        AIProviderTestUtil.setChatProperty("endpoint", oldEndpoint);
        AIProviderTestUtil.setChatProperty("model", oldModel);
        AIProviderTestUtil.setChatProperty("protocol", oldProvider);
    }

    @Test
    public void testAITranslateAutoDetectSourceLanguagePreservesInvocation() throws Exception {
        for (String suffix : List.of("", ", '', 'explicit-model'", ", 'test_default_chat', 'explicit-model'")) {
            for (String source : List.of("''", "NULL", "cast(NULL as varchar)")) {
                ExecPlan plan = getExecPlan("select ai_translate('hello', " + source + ", 'zh'" + suffix + ")");
                TAIProjectNode node = findOnlyAIProjectThriftNode(plan).getAi_project_node();
                Assertions.assertEquals("ai_translate",
                        findOnlyAIExpression(node.getSlot_map()).getNodes().get(0).getFn().getName().getFunction_name());
            }
        }
    }

    @Test
    public void testAICompleteNullOptionsPreserveInvocation() throws Exception {
        for (String provider : List.of("NULL", "''", "'test_default_chat'")) {
            ExecPlan plan = getExecPlan("select ai_complete('hello', " + provider + ", 'explicit-model', cast(NULL as json))");
            TAIProjectNode node = findOnlyAIProjectThriftNode(plan).getAi_project_node();
            Assertions.assertEquals("ai_complete",
                    findOnlyAIExpression(node.getSlot_map()).getNodes().get(0).getFn().getName().getFunction_name());
        }
    }

    @Test
    public void testAIProjectWireNodeAndSemanticArity() throws Exception {
        ExecPlan execPlan = getExecPlan("select ai_complete(k1) from t7");
        TPlanNode aiNode = findOnlyAIProjectThriftNode(execPlan);

        Assertions.assertEquals(TPlanNodeType.AI_PROJECT_NODE, aiNode.getNode_type());
        Assertions.assertTrue(aiNode.isSetAi_project_node());
        Assertions.assertFalse(aiNode.isSetProject_node());

        TAIProjectNode project = aiNode.getAi_project_node();
        Assertions.assertFalse(project.getSlot_map().isEmpty());
        Assertions.assertNotNull(project.getCommon_slot_map());
        TExpr aiExpression = findOnlyAIExpression(project.getSlot_map());
        TExprNode call = aiExpression.getNodes().get(0);
        Assertions.assertEquals(4, call.getNum_children());
        Assertions.assertTrue(project.getAi_provider_mapping().containsKey(call.getAi_provider_id()));
        Assertions.assertTrue(aiExpression.getNodes().stream()
                .noneMatch(node -> node.getNode_type() == TExprNodeType.INT_LITERAL));

        Map<String, TAIProvider> configs = project.getAi_provider_mapping();
        Assertions.assertEquals(1, configs.size());
        TAIProvider chat = configs.get(call.getAi_provider_id());
        Assertions.assertEquals("https://unit.test.example/v1/chat/completions", chat.getParams().get("endpoint"));
        Assertions.assertEquals("unit-test-model", chat.getParams().get("model"));
        Assertions.assertEquals(TAIProviderProtocol.OPENAI, chat.getProtocol());
    }

    @Test
    public void testFileScanColumnReachesAIProjectWirePlan(@TempDir Path warehouse) throws Exception {
        String catalogName = "ai_file_plan";
        GlobalStateMgr state = connectContext.getGlobalStateMgr();
        MetadataMgr previousMetadata = state.getMetadataMgr();
        String previousChatProvider = AIProviderTestUtil.chatProperty("protocol");
        String previousEmbeddingEndpoint = AIProviderTestUtil.embeddingProperty("endpoint");
        String previousEmbeddingModel = AIProviderTestUtil.embeddingProperty("model");
        String previousEmbeddingProvider = AIProviderTestUtil.embeddingProperty("protocol");
        boolean catalogCreated = false;
        Options options = new Options();
        options.set(CatalogOptions.WAREHOUSE, warehouse.toUri().toString());
        try (Catalog nativeCatalog = CatalogFactory.createCatalog(CatalogContext.create(options))) {
            nativeCatalog.createDatabase("media_db", false);
            nativeCatalog.createTable(Identifier.create("media_db", "images"),
                    new Schema(List.of(new DataField(0, "id", new IntType()), new DataField(1, "media", new BlobType())),
                            List.of(), List.of(), Map.of("data-evolution.enabled", "true",
                                    "row-tracking.enabled", "true", "bucket", "-1"), ""), false);
            BatchWriteBuilder writer = nativeCatalog.getTable(Identifier.create("media_db", "images"))
                    .newBatchWriteBuilder();
            try (BatchTableWrite write = writer.newWrite(); BatchTableCommit commit = writer.newCommit()) {
                write.write(GenericRow.of(1, null));
                commit.commit(write.prepareCommit());
            }
            Map<String, String> properties = Map.of("type", "paimon", "paimon.catalog.type", "filesystem",
                    "paimon.catalog.warehouse", warehouse.toUri().toString());
            state.getCatalogMgr().createCatalog("paimon", catalogName, "", properties);
            catalogCreated = true;
            MockedMetadataMgr metadata = new MockedMetadataMgr(state.getLocalMetastore(), state.getConnectorMgr());
            metadata.registerMockedMetadata(catalogName, new PaimonMetadata(catalogName, new HdfsEnvironment(),
                    nativeCatalog, new ConnectorProperties(ConnectorType.PAIMON, properties)));
            state.setMetadataMgr(metadata);
            AIProviderTestUtil.setChatProperty("protocol", "openai");
            AIProviderTestUtil.setEmbeddingProperty("endpoint", "https://unit.test.example/multimodal-embedding");
            AIProviderTestUtil.setEmbeddingProperty("model", "unit-test-embedding");
            AIProviderTestUtil.setEmbeddingProperty("protocol", "openai");

            for (boolean embedding : List.of(false, true)) {
                ExecPlan plan = getExecPlan("select " + (embedding ? "ai_embed(media)" :
                        "ai_complete('describe', media)") + " from " + catalogName + ".media_db.images");
                Assertions.assertEquals(1, plan.getScanNodes().size());
                Assertions.assertInstanceOf(PaimonScanNode.class, plan.getScanNodes().get(0));
                Assertions.assertTrue(plan.getScanNodes().get(0).getDesc().getSlots().stream()
                        .anyMatch(slot -> slot.getType().equals(FileType.FILE)));
                TAIProjectNode project = findOnlyAIProjectThriftNode(plan).getAi_project_node();
                TExpr expression = findOnlyAIExpression(project.getSlot_map());
                TExprNode call = expression.getNodes().get(0);
                Assertions.assertEquals(embedding ? 200003 : 200001, call.getFn().getId());
                Assertions.assertEquals(embedding ? 4 : 5, call.getNum_children());
                String configId = call.getAi_provider_id();
                Assertions.assertEquals(configId, call.getAi_provider_id());
                TExprNode fileArgument = expression.getNodes().get(embedding ? 1 : 2);
                Assertions.assertEquals(TExprNodeType.SLOT_REF, fileArgument.getNode_type());
                Assertions.assertEquals(TPrimitiveType.FILE,
                        fileArgument.getType().getTypes().get(0).getScalar_type().getType());
                TAIProvider model = project.getAi_provider_mapping().get(configId);
                Assertions.assertEquals(TAIProviderProtocol.OPENAI, model.getProtocol());
            }
        } finally {
            state.setMetadataMgr(previousMetadata);
            AIProviderTestUtil.setChatProperty("protocol", previousChatProvider);
            AIProviderTestUtil.setEmbeddingProperty("endpoint", previousEmbeddingEndpoint);
            AIProviderTestUtil.setEmbeddingProperty("model", previousEmbeddingModel);
            AIProviderTestUtil.setEmbeddingProperty("protocol", previousEmbeddingProvider);
            if (catalogCreated) {
                state.getCatalogMgr().dropCatalog(new DropCatalogStmt(catalogName));
            }
        }
    }

    @Test
    public void testEveryAICompleteOverloadHasSemanticWireArityAndFixedConfiguration() throws Exception {
        Map<String, Integer> overloads = new LinkedHashMap<>();
        overloads.put("select ai_complete(k1) from t7", 4);
        overloads.put("select ai_complete(prompt => k1, options => json_object('temperature', 0.5)) from t7", 4);
        overloads.put("select ai_complete(prompt => 'prompt', options => json_object())", 4);
        overloads.put("select ai_complete(k1, '', 'explicit-model') from t7", 4);
        overloads.put("select ai_complete(k1, '', 'explicit-model', json_object('temperature', 0.5)) from t7", 4);

        overloads.forEach((sql, expectedArity) -> {
            ExecPlan execPlan = Assertions.assertDoesNotThrow(() -> getExecPlan(sql));
            TExpr expression = findOnlyAIExpression(
                    findOnlyAIProjectThriftNode(execPlan).getAi_project_node().getSlot_map());
            TExprNode call = expression.getNodes().get(0);
            Assertions.assertEquals(expectedArity, call.getNum_children(), sql);
            Assertions.assertTrue(findOnlyAIProjectThriftNode(execPlan).getAi_project_node()
                    .getAi_provider_mapping().containsKey(call.getAi_provider_id()), sql);
            Assertions.assertTrue(expression.getNodes().stream()
                    .noneMatch(node -> node.getNode_type() == TExprNodeType.INT_LITERAL), sql);
        });
    }

    @Test
    public void testDeterministicAICommonExpressionIsSerializedAndReferenced() throws Exception {
        ExecPlan execPlan = getExecPlan("select ai_complete(concat(k1, k2)), "
                + "ai_complete(concat(k1, k2)) from t7");
        TAIProjectNode project = findOnlyAIProjectThriftNode(execPlan).getAi_project_node();
        Assertions.assertFalse(project.getCommon_slot_map().isEmpty());

        project.getCommon_slot_map().values().forEach(expression -> expression.getNodes().forEach(node -> {
            if (node.isSetFn()) {
                Assertions.assertNotEquals(TFunctionBinaryType.AI, node.getFn().getBinary_type());
                Assertions.assertFalse(FunctionSet.allNonDeterministicFunctions.contains(
                        node.getFn().getName().getFunction_name().toLowerCase(Locale.ROOT)));
            }
        }));

        Set<Integer> commonSlotIds = project.getCommon_slot_map().keySet();
        List<TExpr> aiExpressions = project.getSlot_map().values().stream()
                .filter(expression -> expression.getNodes().get(0).isSetFn())
                .filter(expression -> expression.getNodes().get(0).getFn().getBinary_type() == TFunctionBinaryType.AI)
                .toList();
        Assertions.assertEquals(2, aiExpressions.size());
        aiExpressions.forEach(expression -> Assertions.assertTrue(expression.getNodes().stream()
                .filter(TExprNode::isSetSlot_ref)
                .map(node -> node.getSlot_ref().getSlot_id())
                .anyMatch(commonSlotIds::contains)));
    }

    @Test
    public void testAIProjectConfigurationIsConstructionSnapshot() throws Exception {
        ExecPlan execPlan = getExecPlan("select ai_complete(k1) from t7");
        AIProjectNode node = findOnlyAIProjectNode(execPlan);
        TAIProvider first = findOnlyAIProjectThriftNode(node).getAi_project_node()
                .getAi_provider_mapping().values().iterator().next();

        AIProviderTestUtil.setChatProperty("endpoint", "https://changed.example/v1/chat/completions");
        AIProviderTestUtil.setChatProperty("model", "changed-model");
        AIProviderTestUtil.setChatProperty("protocol", "changed-provider");
        try {
            TAIProvider second = findOnlyAIProjectThriftNode(node).getAi_project_node()
                    .getAi_provider_mapping().values().iterator().next();
            Assertions.assertEquals(first, second);
            Assertions.assertEquals("https://unit.test.example/v1/chat/completions", second.getParams().get("endpoint"));
            Assertions.assertEquals("unit-test-model", second.getParams().get("model"));
            Assertions.assertEquals(TAIProviderProtocol.OPENAI, second.getProtocol());
        } finally {
            AIProviderTestUtil.setChatProperty("endpoint", "https://unit.test.example/v1/chat/completions");
            AIProviderTestUtil.setChatProperty("model", "unit-test-model");
            AIProviderTestUtil.setChatProperty("protocol", "openai");
        }
    }

    @Test
    public void testOrdinaryPlanDoesNotCaptureAIConfiguration() {
        String configuredEndpoint = AIProviderTestUtil.chatProperty("endpoint");
        String configuredModel = AIProviderTestUtil.chatProperty("model");
        String configuredProvider = AIProviderTestUtil.chatProperty("protocol");
        AIProviderTestUtil.setChatProperty("endpoint", "");
        AIProviderTestUtil.setChatProperty("model", "");
        AIProviderTestUtil.setChatProperty("protocol", "");
        try {
            Assertions.assertDoesNotThrow(() -> getExecPlan("select k1 from t7"));
        } finally {
            AIProviderTestUtil.setChatProperty("endpoint", configuredEndpoint);
            AIProviderTestUtil.setChatProperty("model", configuredModel);
            AIProviderTestUtil.setChatProperty("protocol", configuredProvider);
        }
    }

    @Test
    public void testRepeatedBindingsCaptureIndependentProviders() throws Exception {
        ExecPlan plan = getExecPlan("select ai_complete(k1) from t7");
        AIProjectNode node = findOnlyAIProjectNode(plan);
        Map<String, AIProvider> first = plan.bindAIProviders(new LinkedHashMap<>(node.getSlotMap()));
        Map<String, AIProvider> second = plan.bindAIProviders(new LinkedHashMap<>(node.getSlotMap()));
        Assertions.assertEquals(first.keySet(), second.keySet());
        String id = first.keySet().iterator().next();
        Assertions.assertNotSame(first.get(id), second.get(id));
        Assertions.assertEquals(first.get(id).toThrift(), second.get(id).toThrift());
        Assertions.assertThrows(UnsupportedOperationException.class, first::clear);
    }

    @Test
    public void testNestedAIProjectsEachCarryProviderMetadata() throws Exception {
        ExecPlan plan = getExecPlan("select ai_complete(ai_complete(k1)) from t7");
        List<TAIProvider> providers = findAIProjectThriftNodes(plan).stream()
                .map(TPlanNode::getAi_project_node)
                .map(TAIProjectNode::getAi_provider_mapping)
                .map(mapping -> mapping.values().iterator().next())
                .toList();
        Assertions.assertEquals(2, providers.size());
        Assertions.assertEquals(providers.get(0), providers.get(1));
        Assertions.assertNotSame(providers.get(0), providers.get(1));
        Assertions.assertEquals("unit-test-model", providers.get(0).getParams().get("model"));
    }

    @Test
    public void testOrdinaryProjectDoesNotMergeIntoAIProjectBoundary() {
        ColumnRefOperator column = new ColumnRefOperator(1, IntegerType.INT, "column", true);
        LogicalProjectOperator project = new LogicalProjectOperator(Map.of(column, column));
        LogicalAIProjectOperator aiProject = new LogicalAIProjectOperator(Map.of(column, column));
        OptExpression expression = OptExpression.create(project, OptExpression.create(aiProject));

        Assertions.assertFalse(new MergeProjectWithChildRule().check(expression, null));
    }

    @Test
    public void testAIProjectIsRuntimeFilterHardBoundary() throws Exception {
        AIProjectNode aiProject = findOnlyAIProjectNode(getExecPlan("select ai_complete(k1) from t7"));
        Expr aiExpression = aiProject.getSlotMap().values().stream()
                .filter(expression -> containsFunction(expression, "ai_complete"))
                .findFirst().orElseThrow();

        Assertions.assertFalse(aiProject.canPushDownRuntimeFilter());
        Assertions.assertTrue(aiProject.candidatesOfSlotExpr(aiExpression, ignored -> true).isEmpty());
        Assertions.assertTrue(aiProject.candidatesOfSlotExprs(List.of(aiExpression), ignored -> true).isEmpty());
        Assertions.assertFalse(aiProject.pushDownRuntimeFilters(null, aiExpression, List.of()));
    }

    @Test
    public void testAIJoinProbeDoesNotCrossAIProjectRuntimeFilterBoundary() throws Exception {
        boolean globalRuntimeFilter = connectContext.getSessionVariable().getEnableGlobalRuntimeFilter();
        long probeMinSize = connectContext.getSessionVariable().getGlobalRuntimeFilterProbeMinSize();
        boolean runningUnitTest = FeConstants.runningUnitTest;
        connectContext.getSessionVariable().setEnableGlobalRuntimeFilter(true);
        connectContext.getSessionVariable().setGlobalRuntimeFilterProbeMinSize(0);
        FeConstants.runningUnitTest = true;
        try {
            ExecPlan execPlan = getExecPlan("select a.v1 from "
                    + "(select v1, ai_complete(cast(v2 as varchar)) as answer from t0) a "
                    + "join t1 on a.answer = cast(t1.v5 as varchar)");
            List<AIProjectNode> aiProjects = findAIProjectNodes(execPlan);
            List<JoinNode> joins = execPlan.getFragments().stream()
                    .flatMap(fragment -> fragment.collectNodes().stream())
                    .filter(JoinNode.class::isInstance)
                    .map(JoinNode.class::cast)
                    .toList();

            Assertions.assertTrue(joins.stream().anyMatch(join -> !join.getBuildRuntimeFilters().isEmpty()),
                    execPlan.getExplainString(TExplainLevel.NORMAL));
            Assertions.assertFalse(aiProjects.isEmpty());
            aiProjects.forEach(aiProject -> Assertions.assertFalse(
                    subtreeHasProbeRuntimeFilter(aiProject.getChild(0)),
                    execPlan.getExplainString(TExplainLevel.NORMAL)));
        } finally {
            connectContext.getSessionVariable().setEnableGlobalRuntimeFilter(globalRuntimeFilter);
            connectContext.getSessionVariable().setGlobalRuntimeFilterProbeMinSize(probeMinSize);
            FeConstants.runningUnitTest = runningUnitTest;
        }
    }

    @Test
    public void testTimeFunctionsNeverReachAIProjectCommonExpressions() {
        for (String timeFunction : List.of("now()", "unix_timestamp()")) {
            String sql = "select ai_complete(concat(k1, cast(" + timeFunction + " as string))) as answer1, "
                    + "ai_complete(concat(k1, cast(" + timeFunction + " as string))) as answer2 from t7";
            ExecPlan execPlan = Assertions.assertDoesNotThrow(() -> getExecPlan(sql), sql);
            AIProjectNode aiProject = findOnlyAIProjectNode(execPlan);
            aiProject.getCommonSlotMap().values().forEach(expression -> {
                Assertions.assertFalse(containsFunction(expression, "now"), expression.toString());
                Assertions.assertFalse(containsFunction(expression, "unix_timestamp"), expression.toString());
            });
        }
    }

    @Test
    public void testProviderAlwaysRequiresADefaultModel() throws Exception {
        String configuredModel = AIProviderTestUtil.chatProperty("model");
        AIProviderTestUtil.setChatProperty("model", "");
        try {
            Assertions.assertThrows(SemanticException.class,
                    () -> getExecPlan("select ai_complete('explicit-model', k1) from t7"));
            Assertions.assertThrows(SemanticException.class,
                    () -> getExecPlan("select ai_complete(k1) from t7"));
        } finally {
            AIProviderTestUtil.setChatProperty("model", configuredModel);
        }
    }

    @Test
    public void testExplainContainsOnlySemanticArgumentsAndNoConfiguration() throws Exception {
        ExecPlan execPlan = getExecPlan("select ai_complete(k1) from t7");
        String optimizerExplain = Explain.toString(execPlan.getPhysicalPlan(), execPlan.getOutputColumns());
        assertNormalizedAIComplete(optimizerExplain);
        List<String> explains = List.of(
                optimizerExplain,
                execPlan.getExplainString(TExplainLevel.NORMAL),
                execPlan.getExplainString(TExplainLevel.VERBOSE),
                execPlan.getExplainString(TExplainLevel.COSTS));

        for (String explain : explains) {
            Assertions.assertTrue(explain.contains("ai_complete"), explain);
            Assertions.assertFalse(explain.contains(AIProviderTestUtil.chatProperty("endpoint")), explain);
            Assertions.assertFalse(explain.contains(AIProviderTestUtil.chatProperty("model")), explain);
            Assertions.assertFalse(explain.contains(AIProviderTestUtil.chatProperty("protocol")), explain);
            Assertions.assertFalse(explain.contains("provider:0"), explain);
        }
    }

    @Test
    public void testAIProjectDisablesQueryCacheAboveAndBelowDigestPoint() throws Exception {
        boolean queryCacheEnabled = connectContext.getSessionVariable().isEnableQueryCache();
        boolean metaScanEnabled = connectContext.getSessionVariable().isEnableRewriteSimpleAggToMetaScan();
        boolean runningUnitTest = FeConstants.runningUnitTest;
        connectContext.getSessionVariable().setEnableQueryCache(true);
        connectContext.getSessionVariable().setEnableRewriteSimpleAggToMetaScan(false);
        FeConstants.runningUnitTest = true;
        try {
            String source = " from t7";
            List<String> deterministicBaselines = List.of(
                    "select /*+ SET_VAR(new_planner_agg_stage='1') */ "
                            + "upper(cast(count(k1) as string))" + source,
                    "select /*+ SET_VAR(new_planner_agg_stage='1') */ "
                            + "count(k1)" + source + " where upper(k1) = 'ACCEPTED'");
            for (String sql : deterministicBaselines) {
                ExecPlan baseline = getExecPlan(sql);
                Assertions.assertTrue(baseline.getFragments().stream()
                                .anyMatch(fragment -> fragment.getCacheParam() != null),
                        baseline.getExplainString(TExplainLevel.NORMAL));
            }

            List<String> aiQueries = List.of(
                    "select /*+ SET_VAR(new_planner_agg_stage='1') */ "
                            + "ai_complete(cast(count(k1) as string))" + source,
                    "select /*+ SET_VAR(new_planner_agg_stage='1') */ "
                            + "count(k1)" + source + " where ai_complete(k1) = 'accepted'");
            for (String sql : aiQueries) {
                ExecPlan execPlan = getExecPlan(sql);
                Assertions.assertTrue(execPlan.getFragments().stream()
                        .noneMatch(fragment -> fragment.getCacheParam() != null),
                        execPlan.getExplainString(TExplainLevel.NORMAL));
            }
        } finally {
            connectContext.getSessionVariable().setEnableQueryCache(queryCacheEnabled);
            connectContext.getSessionVariable().setEnableRewriteSimpleAggToMetaScan(metaScanEnabled);
            FeConstants.runningUnitTest = runningUnitTest;
        }
    }

    @Test
    public void testAIProjectRejectsUntrustedModelConfigIdsWithoutEchoingThem() throws Exception {
        ExecPlan execPlan = getExecPlan("select ai_complete(k1) from t7");
        AIProjectNode original = findOnlyAIProjectNode(execPlan);
        TupleDescriptor tupleDescriptor = execPlan.getDescTbl().getTupleDesc(original.getTupleIds().get(0));
        Map<String, AIProvider> modelConfigs = execPlan.bindAIProviders(
                new LinkedHashMap<>(original.getSlotMap()));
        Map.Entry<SlotId, Expr> aiEntry = original.getSlotMap().entrySet().stream()
                .filter(entry -> containsFunction(entry.getValue(), "ai_complete"))
                .findFirst().orElseThrow();
        FunctionCallExpr aiCall = (FunctionCallExpr) aiEntry.getValue();

        FunctionCallExpr missingIdCall = copyAIPlannerCall(aiCall, null);
        Assertions.assertThrows(IllegalStateException.class,
                () -> new AIProjectNode(execPlan.getNextNodeId(), tupleDescriptor, original.getChild(0),
                        Map.of(aiEntry.getKey(), missingIdCall), Map.of(), modelConfigs));

        List<String> untrustedIds = List.of(
                "", "arbitrary-config", "provider-api-key", "credential-looking-value");
        for (String untrustedId : untrustedIds) {
            FunctionCallExpr untrustedCall = copyAIPlannerCall(aiCall, untrustedId);
            IllegalStateException failure = Assertions.assertThrows(IllegalStateException.class,
                    () -> new AIProjectNode(execPlan.getNextNodeId(), tupleDescriptor, original.getChild(0),
                            Map.of(aiEntry.getKey(), untrustedCall), Map.of(), modelConfigs));
            if (!untrustedId.isEmpty()) {
                Assertions.assertFalse(failure.getMessage().contains(untrustedId));
            }
        }

        FunctionCallExpr cloned = (FunctionCallExpr) aiCall.clone();
        Assertions.assertEquals(aiCall.getAIProviderId(), cloned.getAIProviderId());
    }

    @Test
    public void testAIProjectDefensivelyOwnsImmutableExpressionMaps() throws Exception {
        ExecPlan execPlan = getExecPlan("select ai_complete(k1) from t7");
        AIProjectNode original = findOnlyAIProjectNode(execPlan);
        TupleDescriptor tupleDescriptor = execPlan.getDescTbl().getTupleDesc(original.getTupleIds().get(0));
        Map<String, AIProvider> modelConfigs = execPlan.bindAIProviders(
                new LinkedHashMap<>(original.getSlotMap()));
        Map<SlotId, Expr> callerSlotMap = deepCloneExpressions(original.getSlotMap());
        Map<SlotId, Expr> callerCommonMap = deepCloneExpressions(original.getCommonSlotMap());
        AIProjectNode isolated = new AIProjectNode(execPlan.getNextNodeId(), tupleDescriptor, original.getChild(0),
                callerSlotMap, callerCommonMap, modelConfigs);
        String beforeMutation = findOnlyAIProjectThriftNode(isolated).toString();

        FunctionCallExpr callerCall = (FunctionCallExpr) callerSlotMap.values().stream()
                .filter(expression -> containsFunction(expression, "ai_complete"))
                .findFirst().orElseThrow();
        callerCall.setChild(0, new StringLiteral("caller-mutated"));
        callerSlotMap.clear();
        callerCommonMap.clear();
        Assertions.assertEquals(beforeMutation, findOnlyAIProjectThriftNode(isolated).toString());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> isolated.getSlotMap().clear());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> isolated.getCommonSlotMap().clear());
    }

    @Test
    public void testAIProjectRejectsUnsafeSlotAndCommonMapShapes() throws Exception {
        ExecPlan execPlan = getExecPlan("select ai_complete(k1) from t7");
        AIProjectNode original = findOnlyAIProjectNode(execPlan);
        TupleDescriptor tupleDescriptor = execPlan.getDescTbl().getTupleDesc(original.getTupleIds().get(0));
        Map<String, AIProvider> modelConfigs = execPlan.bindAIProviders(
                new LinkedHashMap<>(original.getSlotMap()));
        Map.Entry<SlotId, Expr> aiEntry = original.getSlotMap().entrySet().stream()
                .filter(entry -> containsFunction(entry.getValue(), "ai_complete"))
                .findFirst().orElseThrow();

        Map<SlotId, Expr> nonAIOutput = deepCloneExpressions(original.getSlotMap());
        nonAIOutput.put(new SlotId(10_000), findOnlyFunctionCall(getExecPlan("select rand() from t7"), "rand"));
        Assertions.assertThrows(IllegalStateException.class, () -> new AIProjectNode(
                execPlan.getNextNodeId(), tupleDescriptor, original.getChild(0), nonAIOutput, Map.of(),
                modelConfigs));

        FunctionCallExpr aiCall = (FunctionCallExpr) aiEntry.getValue();
        SlotRef inputSlot = (SlotRef) aiCall.getChild(0);

        FunctionCallExpr nestedAICall = (FunctionCallExpr) aiCall.clone();
        nestedAICall.setChild(0, aiCall.clone());
        Assertions.assertAll(
                () -> Assertions.assertThrows(IllegalStateException.class, () -> new AIProjectNode(
                        execPlan.getNextNodeId(), tupleDescriptor, original.getChild(0),
                        Map.of(aiEntry.getKey(), nestedAICall), Map.of(), modelConfigs)),
                () -> Assertions.assertThrows(IllegalStateException.class, () -> new AIProjectNode(
                        execPlan.getNextNodeId(), tupleDescriptor, original.getChild(0), original.getSlotMap(),
                        Map.of(aiEntry.getKey(), inputSlot.clone()), modelConfigs)));

        Map<SlotId, Expr> mismatchedIdentity = deepCloneExpressions(original.getSlotMap());
        mismatchedIdentity.put(new SlotId(10_001), inputSlot.clone());
        Assertions.assertThrows(IllegalStateException.class, () -> new AIProjectNode(
                execPlan.getNextNodeId(), tupleDescriptor, original.getChild(0), mismatchedIdentity, Map.of(),
                modelConfigs));

        SlotRef aiOutputReference = new SlotRef(aiEntry.getKey());
        Assertions.assertThrows(IllegalStateException.class, () -> new AIProjectNode(
                execPlan.getNextNodeId(), tupleDescriptor, original.getChild(0), original.getSlotMap(),
                Map.of(new SlotId(10_002), aiOutputReference), modelConfigs));
    }

    @Test
    public void testAIFunctionDisablesShortCircuitWithoutNameMatching() throws Exception {
        boolean shortCircuitEnabled = connectContext.getSessionVariable().isEnableShortCircuit();
        connectContext.getSessionVariable().setEnableShortCircuit(true);
        try {
            Assertions.assertFalse(getExecPlan(
                    "select ai_complete(v3) from tprimary1 where pk1 = 20").isShortCircuit());
            Assertions.assertTrue(getExecPlan(
                    "select lower(v3) from tprimary1 where pk1 = 20").isShortCircuit());
        } finally {
            connectContext.getSessionVariable().setEnableShortCircuit(shortCircuitEnabled);
        }
    }

    @Test
    public void testAIProjectRejectsAIAndNondeterministicCommonExpressions() throws Exception {
        ExecPlan aiPlan = getExecPlan("select ai_complete(k1) from t7");
        AIProjectNode aiProject = findOnlyAIProjectNode(aiPlan);
        TupleDescriptor tupleDescriptor = aiPlan.getDescTbl().getTupleDesc(aiProject.getTupleIds().get(0));
        Map<String, AIProvider> modelConfigs = aiPlan.bindAIProviders(
                new LinkedHashMap<>(aiProject.getSlotMap()));
        Map.Entry<SlotId, Expr> aiExpression = aiProject.getSlotMap().entrySet().stream()
                .filter(entry -> containsFunction(entry.getValue(), "ai_complete"))
                .findFirst().orElseThrow();

        Assertions.assertThrows(IllegalStateException.class, () -> new AIProjectNode(
                aiPlan.getNextNodeId(), tupleDescriptor, aiProject.getChild(0), aiProject.getSlotMap(),
                Map.of(aiExpression.getKey(), aiExpression.getValue()), modelConfigs));

        FunctionCallExpr rand = findOnlyFunctionCall(getExecPlan("select rand() from t7"), "rand");
        Assertions.assertThrows(IllegalStateException.class, () -> new AIProjectNode(
                aiPlan.getNextNodeId(), tupleDescriptor, aiProject.getChild(0), aiProject.getSlotMap(),
                Map.of(aiExpression.getKey(), rand), modelConfigs));
    }

    @Test
    public void testRealAIProjectNodeKeepsPhysicalOperatorMapping() throws Exception {
        ExecPlan execPlan = getExecPlan("select ai_complete(concat(k1, 'x')) from t7");
        AIProjectNode plannerNode = findOnlyAIProjectNode(execPlan);
        List<OptExpression> physicalProjects = findPhysicalOperators(
                execPlan.getPhysicalPlan(), OperatorType.PHYSICAL_AI_PROJECT);

        Assertions.assertEquals(1, physicalProjects.size());
        OptExpression mapped = execPlan.getOptExpression(plannerNode.getId().asInt());
        Assertions.assertSame(physicalProjects.get(0), mapped);
        Assertions.assertInstanceOf(PhysicalAIProjectOperator.class, mapped.getOp());

        PhysicalAIProjectOperator physical = mapped.getOp().cast();
        List<Integer> plannerSlotIds = plannerNode.getSlotMap().keySet().stream()
                .map(SlotId::asInt).sorted().toList();
        List<Integer> physicalSlotIds = physical.getColumnRefMap().keySet().stream()
                .map(ColumnRefOperator::getId).sorted().toList();
        Assertions.assertEquals(physicalSlotIds, plannerSlotIds);
    }

    @Test
    public void testAIProjectPlacementAcrossMainPlanShapes() throws Exception {
        ExecPlan where = getExecPlan("select k1 from t7 where ai_complete(k1) = 'x'");
        assertStrictAncestor(where, OperatorType.PHYSICAL_FILTER, OperatorType.PHYSICAL_AI_PROJECT);

        ExecPlan having = getExecPlan("select count(*) from t7 having ai_complete('p') = 'x'");
        assertStrictAncestor(having, OperatorType.PHYSICAL_FILTER, OperatorType.PHYSICAL_AI_PROJECT);
        assertStrictAncestor(having, OperatorType.PHYSICAL_AI_PROJECT, OperatorType.PHYSICAL_HASH_AGG);

        ExecPlan order = getExecPlan("select ai_complete(k1) as answer from t7 order by answer");
        assertStrictAncestor(order, OperatorType.PHYSICAL_TOPN, OperatorType.PHYSICAL_AI_PROJECT);
        Assertions.assertEquals(1, countPhysicalOperators(order, OperatorType.PHYSICAL_AI_PROJECT));

        ExecPlan nested = getExecPlan("select ai_complete(ai_complete(k1)) from t7");
        Assertions.assertEquals(2, countPhysicalOperators(nested, OperatorType.PHYSICAL_AI_PROJECT));
        assertStrictAncestor(nested, OperatorType.PHYSICAL_AI_PROJECT, OperatorType.PHYSICAL_AI_PROJECT);

        ExecPlan innerResidual = getExecPlan("select t0.v1 from t0 join t1 "
                + "on t0.v1 = t1.v4 and ai_complete(cast(t0.v2 as varchar)) = cast(t1.v5 as varchar)");
        assertStrictAncestor(innerResidual, OperatorType.PHYSICAL_FILTER, OperatorType.PHYSICAL_AI_PROJECT);
        assertStrictAncestor(innerResidual, OperatorType.PHYSICAL_AI_PROJECT, OperatorType.PHYSICAL_HASH_JOIN);

        ExecPlan crossResidual = getExecPlan("select t0.v1 from t0 cross join t1 "
                + "where ai_complete(cast(t0.v2 as varchar)) = cast(t1.v5 as varchar)");
        assertStrictAncestor(crossResidual, OperatorType.PHYSICAL_FILTER, OperatorType.PHYSICAL_AI_PROJECT);
        assertStrictAncestor(crossResidual, OperatorType.PHYSICAL_AI_PROJECT, OperatorType.PHYSICAL_NESTLOOP_JOIN);
    }

    @Test
    public void testLimitPropagatesLocalBoundThroughAIProject() throws Exception {
        ExecPlan execPlan = getExecPlan("select ai_complete(k1) from t7 limit 5");
        assertStrictAncestor(execPlan, OperatorType.PHYSICAL_LIMIT, OperatorType.PHYSICAL_AI_PROJECT);

        AIProjectNode aiProject = findOnlyAIProjectNode(execPlan);
        Assertions.assertEquals(5, aiProject.getLimit());
        Assertions.assertEquals(5, aiProject.getChild(0).getLimit());
    }

    @Test
    public void testOffsetLimitKeepsGlobalBoundsAndPropagatesExpandedLocalBound() throws Exception {
        ExecPlan execPlan = getExecPlan("select ai_complete(k1) from t7 limit 5 offset 3");
        AIProjectNode aiProject = findOnlyAIProjectNode(execPlan);
        Assertions.assertEquals(8, aiProject.getLimit());
        Assertions.assertEquals(8, aiProject.getChild(0).getLimit());

        List<PhysicalLimitOperator> limits = findPhysicalOperators(
                execPlan.getPhysicalPlan(), OperatorType.PHYSICAL_LIMIT).stream()
                .map(expression -> (PhysicalLimitOperator) expression.getOp())
                .toList();
        Assertions.assertTrue(limits.stream()
                        .anyMatch(limit -> limit.getLimit() == 5 && limit.getOffset() == 3),
                Explain.toString(execPlan.getPhysicalPlan(), execPlan.getOutputColumns()));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testTopNProjectionDependenciesForTextAndEmbedding(boolean enabled) throws Exception {
        boolean oldEnabled = connectContext.getSessionVariable().isEnableAITopnPushdown();
        String oldEmbeddingEndpoint = AIProviderTestUtil.embeddingProperty("endpoint");
        String oldEmbeddingModel = AIProviderTestUtil.embeddingProperty("model");
        AIProviderTestUtil.setEmbeddingProperty("endpoint", "https://models.example.test/v1/embeddings");
        AIProviderTestUtil.setEmbeddingProperty("model", "embedding-model");
        try {
            connectContext.getSessionVariable().setEnableAITopnPushdown(enabled);
            for (String sql : List.of(
                    "select k1, ai_embed(k2) from t7 order by k1 limit 1",
                    "select k1, ai_embed(k2) from t7 order by k1 desc limit 1 offset 1",
                    "select k1, ai_sentiment(k2) from t7 order by k1 limit 1",
                    "select k1, array_length(ai_embed(k2)) from t7 order by k1 limit 1",
                    "select k1, ai_embed(ai_complete(k2)) from t7 order by k1 limit 1",
                    "select k1, ai_embed(concat(k2, 'x')), ai_embed(concat(k2, 'x')) from t7 order by k1 limit 1")) {
                ExecPlan plan = getExecPlan(sql);
                Assertions.assertFalse(findPhysicalOperators(plan.getPhysicalPlan(),
                        OperatorType.PHYSICAL_AI_PROJECT).isEmpty(), sql);
                com.starrocks.sql.optimizer.validate.InputDependenciesChecker.getInstance()
                        .validate(plan.getPhysicalPlan(), null);
            }
        } finally {
            connectContext.getSessionVariable().setEnableAITopnPushdown(oldEnabled);
            AIProviderTestUtil.setEmbeddingProperty("model", oldEmbeddingModel);
            AIProviderTestUtil.setEmbeddingProperty("endpoint", oldEmbeddingEndpoint);
        }
    }

    @ParameterizedTest
    @CsvSource({"asc, first", "desc, last", "asc, last", "desc, first"})
    public void testTopNReducesAIInputByPassthroughKey(String direction, String nullOrder) throws Exception {
        ExecPlan execPlan = getExecPlan("select ai_complete(k1) from t7 order by k1 "
                + direction + " nulls " + nullOrder + " limit 5");
        assertTopNBoundsAIInput(execPlan, 5);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "select concat(ai_complete(k1), 'x') from t7 order by k1 limit 5",
            "select ai_complete(concat(k1, k2)) from t7 order by k2 desc, k1 asc limit 5",
            "select ai_complete(ai_complete(k1)) from t7 order by k1 limit 5",
            "select ai_complete(k1), ai_complete(k1) from t7 order by k1 limit 5"
    })
    public void testTopNReducesAIInputAcrossProjectionShapes(String sql) throws Exception {
        ExecPlan execPlan = getExecPlan(sql);
        assertTopNBoundsAIInput(execPlan, 5);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "select ai_complete(k1) from t7 order by k1 limit 5 offset 3",
            "select ai_complete(k1) from t7 order by k1",
            "select ai_complete(k1) as answer from t7 order by k1, answer limit 5",
            "select ai_complete(k1) as answer from t7 order by length(answer) limit 5",
            "select k1 from t7 where ai_complete(k1) = 'x' order by k1 limit 5"
    })
    public void testTopNKeepsUnsafeAIInputsUnbounded(String sql) throws Exception {
        ExecPlan execPlan = getExecPlan(sql);
        assertAIProjectHasNoPushedLimit(execPlan);
        OptExpression aiProject = findPhysicalOperators(
                execPlan.getPhysicalPlan(), OperatorType.PHYSICAL_AI_PROJECT).get(0);
        Assertions.assertTrue(findPhysicalOperators(aiProject.inputAt(0), OperatorType.PHYSICAL_TOPN).isEmpty(),
                Explain.toString(execPlan.getPhysicalPlan(), execPlan.getOutputColumns()));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    public void testTopNProvidesGlobalAIInputBound(int dop) throws Exception {
        int oldDop = connectContext.getSessionVariable().getPipelineDop();
        UtFrameUtils.addMockBackend(10002);
        try {
            connectContext.getSessionVariable().setPipelineDop(dop);
            ExecPlan execPlan = getExecPlan("select ai_complete(cast(v2 as varchar)) from t0 order by v1 limit 5");
            assertTopNBoundsAIInput(execPlan, 5);
            AIProjectNode aiProject = findOnlyAIProjectNode(execPlan);
            ExchangeNode merge = Assertions.assertInstanceOf(ExchangeNode.class, aiProject.getChild(0));
            Assertions.assertTrue(merge.isMerge());
            Assertions.assertEquals(DistributionSpec.DistributionType.GATHER, merge.getDistributionType());
            Assertions.assertEquals(5, merge.getLimit());
        } finally {
            connectContext.getSessionVariable().setPipelineDop(oldDop);
            UtFrameUtils.dropMockBackend(10002);
        }
    }

    @ParameterizedTest
    @CsvSource({"true, 1, false", "true, 1, true", "true, 4, false", "true, 4, true",
            "false, 1, false", "false, 1, true", "false, 4, false", "false, 4, true"})
    public void testWindowConsumesPreservedAIInputOrdering(boolean enabled, int dop, boolean parallelMerge) throws Exception {
        boolean oldEnable = connectContext.getSessionVariable().isEnableAITopnPushdown();
        int oldDop = connectContext.getSessionVariable().getPipelineDop();
        boolean oldParallelMerge = connectContext.getSessionVariable().isEnableParallelMerge();
        UtFrameUtils.addMockBackend(10002);
        try {
            connectContext.getSessionVariable().setEnableAITopnPushdown(enabled);
            connectContext.getSessionVariable().setPipelineDop(dop);
            connectContext.getSessionVariable().setEnableParallelMerge(parallelMerge);
            ExecPlan execPlan = getExecPlan("select a, row_number() over (order by k1) rn "
                    + "from (select k1, ai_complete(k1) a "
                    + "from (select k1 from t7 order by k1 limit 100) x) s");

            assertStrictAncestor(execPlan, OperatorType.PHYSICAL_WINDOW, OperatorType.PHYSICAL_AI_PROJECT);
            List<OptExpression> windows = findPhysicalOperators(execPlan.getPhysicalPlan(), OperatorType.PHYSICAL_WINDOW);
            List<OptExpression> aiProjects = findPhysicalOperators(
                    execPlan.getPhysicalPlan(), OperatorType.PHYSICAL_AI_PROJECT);
            Assertions.assertEquals(1, windows.size());
            Assertions.assertEquals(1, aiProjects.size());
            OptExpression window = windows.get(0);
            OptExpression physicalAI = aiProjects.get(0);
            Assertions.assertFalse(physicalAI.getOutputProperty().getSortProperty().isEmpty());
            Assertions.assertEquals(physicalAI.inputAt(0).getOutputProperty(), physicalAI.getOutputProperty());
            Assertions.assertTrue(window.inputAt(0).getOutputProperty().isSatisfy(window.getRequiredProperties().get(0)));
            Assertions.assertFalse(hasStrictAncestor(
                    execPlan.getPhysicalPlan(), OperatorType.PHYSICAL_TOPN, OperatorType.PHYSICAL_AI_PROJECT));

            AIProjectNode aiProject = findOnlyAIProjectNode(execPlan);
            ExchangeNode merge = Assertions.assertInstanceOf(ExchangeNode.class, aiProject.getChild(0));
            Assertions.assertTrue(merge.isMerge());
            Assertions.assertEquals(DistributionSpec.DistributionType.GATHER, merge.getDistributionType());
            Assertions.assertEquals(100, merge.getLimit());
            Assertions.assertFalse(aiProject.getFragment().isPartitioned());

            List<AnalyticEvalNode> analytics = new ArrayList<>();
            aiProject.getFragment().getPlanRoot().collect(AnalyticEvalNode.class, analytics);
            Assertions.assertEquals(1, analytics.size());
            PlanNode windowInput = analytics.get(0).getChild(0);
            while (windowInput instanceof ProjectNode && !(windowInput instanceof AIProjectNode)) {
                windowInput = windowInput.getChild(0);
            }
            Assertions.assertSame(aiProject, windowInput,
                    "Window must consume AI output without an intervening Sort or Exchange");
        } finally {
            connectContext.getSessionVariable().setEnableAITopnPushdown(oldEnable);
            connectContext.getSessionVariable().setPipelineDop(oldDop);
            connectContext.getSessionVariable().setEnableParallelMerge(oldParallelMerge);
            UtFrameUtils.dropMockBackend(10002);
        }
    }

    @Test
    public void testTopNKeepsExistingInputLimit() throws Exception {
        ExecPlan execPlan = getExecPlan("select ai_complete(cast(v2 as varchar)) "
                + "from (select v1, v2 from t0 limit 20) s order by v1 limit 5");
        OptExpression aiProject = findPhysicalOperators(
                execPlan.getPhysicalPlan(), OperatorType.PHYSICAL_AI_PROJECT).get(0);
        Assertions.assertTrue(findPhysicalOperators(aiProject.inputAt(0), OperatorType.PHYSICAL_TOPN).isEmpty(),
                Explain.toString(execPlan.getPhysicalPlan(), execPlan.getOutputColumns()));
        Assertions.assertEquals(20, aiProject.inputAt(0).getOp().getLimit());
    }

    @ParameterizedTest
    @CsvSource({"1, false", "1, true", "4, false", "4, true"})
    public void testLargeTopNKeepsAIDistributed(int dop, boolean parallelMerge) throws Exception {
        int oldDop = connectContext.getSessionVariable().getPipelineDop();
        boolean oldParallelMerge = connectContext.getSessionVariable().isEnableParallelMerge();
        UtFrameUtils.addMockBackend(10002);
        try {
            connectContext.getSessionVariable().setPipelineDop(dop);
            connectContext.getSessionVariable().setEnableParallelMerge(parallelMerge);
            ExecPlan execPlan = getExecPlan(
                    "select ai_complete(cast(v2 as varchar)) from t0 order by v1 limit 50000");
            assertLocalTopNBoundsAIInput(execPlan, 50000);
            AIProjectNode aiProject = findOnlyAIProjectNode(execPlan);
            Assertions.assertSame(execPlan.getScanNodes().get(0).getFragment(), aiProject.getFragment());
            Assertions.assertTrue(aiProject.getFragment().isPartitioned());
            SortNode localSort = (SortNode) aiProject.getChild(0);
            Assertions.assertEquals(parallelMerge, localSort.isUseParallelMerge());
        } finally {
            connectContext.getSessionVariable().setPipelineDop(oldDop);
            connectContext.getSessionVariable().setEnableParallelMerge(oldParallelMerge);
            UtFrameUtils.dropMockBackend(10002);
        }
    }

    @ParameterizedTest
    @CsvSource({"0, 5, false", "4, 5, false", "5, 5, true", "6, 5, true",
            "1000, 1000, true", "1000, 1001, false"})
    public void testTopNThresholdHint(long threshold, long limit, boolean global) throws Exception {
        long oldThreshold = connectContext.getSessionVariable().getAITopnPushdownMaxGlobalLimit();
        ExecPlan execPlan = getExecPlan("select /*+ SET_VAR(ai_topn_pushdown_max_global_limit=" + threshold
                + ") */ ai_complete(k1) from t7 order by k1 limit " + limit);
        if (global) {
            assertTopNBoundsAIInput(execPlan, limit);
        } else {
            assertLocalTopNBoundsAIInput(execPlan, limit);
        }
        Assertions.assertEquals(oldThreshold, connectContext.getSessionVariable().getAITopnPushdownMaxGlobalLimit());
        assertTopNBoundsAIInput(getExecPlan("select ai_complete(k1) from t7 order by k1 limit 5"), 5);
    }

    @ParameterizedTest
    @ValueSource(longs = {5, 50000})
    public void testTopNSwitchHintRestoresSession(long limit) throws Exception {
        boolean oldEnable = connectContext.getSessionVariable().isEnableAITopnPushdown();
        ExecPlan execPlan = getExecPlan("select /*+ SET_VAR(enable_ai_topn_pushdown=false) */ "
                + "ai_complete(k1) from t7 order by k1 limit " + limit);
        OptExpression aiProject = findPhysicalOperators(
                execPlan.getPhysicalPlan(), OperatorType.PHYSICAL_AI_PROJECT).get(0);
        Assertions.assertTrue(findPhysicalOperators(aiProject.inputAt(0), OperatorType.PHYSICAL_TOPN).isEmpty());
        Assertions.assertEquals(Operator.DEFAULT_LIMIT, findOnlyAIProjectNode(execPlan).getLimit());
        Assertions.assertEquals(oldEnable, connectContext.getSessionVariable().isEnableAITopnPushdown());
        assertTopNBoundsAIInput(getExecPlan("select ai_complete(k1) from t7 order by k1 limit 5"), 5);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "select concat(ai_complete(k1), 'x') from t7 order by k1 desc nulls last limit 50000",
            "select ai_complete(concat(k1, k2)) from t7 order by k2 desc nulls first, k1 asc limit 50000",
            "select ai_complete(k1), ai_complete(k1) from t7 order by k1 limit 50000",
            "select ai_complete(cast(v1 as varchar)) from "
                    + "(select v1 from t0 union all select v4 from t1) u order by v1 limit 50000",
            "select ai_complete(cast(t0.v2 as varchar)) from t0 left join t1 on t0.v1=t1.v4 "
                    + "order by t0.v1 limit 50000",
            "select ai_complete(cast(total as varchar)) from "
                    + "(select v1, sum(v2) total from t0 group by v1) a order by v1 limit 50000"
    })
    public void testLocalTopNAcrossProjectionAndChildShapes(String sql) throws Exception {
        assertLocalTopNBoundsAIInput(getExecPlan(sql), 50000);
    }

    @Test
    public void testLocalTopNDoesNotBoundNestedAIInput() throws Exception {
        ExecPlan execPlan = getExecPlan("select /*+ SET_VAR(ai_topn_pushdown_max_global_limit=0) */ "
                + "ai_complete(ai_complete(k1)) from t7 order by k1 limit 5");
        List<OptExpression> aiProjects = findPhysicalOperators(
                execPlan.getPhysicalPlan(), OperatorType.PHYSICAL_AI_PROJECT);
        Assertions.assertEquals(2, aiProjects.size());
        PhysicalTopNOperator candidates = Assertions.assertInstanceOf(
                PhysicalTopNOperator.class, aiProjects.get(0).inputAt(0).getOp());
        Assertions.assertEquals(SortPhase.PARTIAL, candidates.getSortPhase());
        Assertions.assertEquals(5, candidates.getLimit());
        Assertions.assertFalse(candidates.isPerPipeline());
        Assertions.assertTrue(findPhysicalOperators(
                aiProjects.get(1).inputAt(0), OperatorType.PHYSICAL_TOPN).isEmpty());
        Assertions.assertEquals(Operator.DEFAULT_LIMIT, aiProjects.get(1).inputAt(0).getOp().getLimit());
        assertTopNSortsAIOutput(execPlan, 5);
    }

    @ParameterizedTest
    @ValueSource(strings = {"-1", "9223372036854775808", "invalid"})
    public void testInvalidTopNThresholdHintDoesNotChangeSession(String value) {
        long oldThreshold = connectContext.getSessionVariable().getAITopnPushdownMaxGlobalLimit();
        Assertions.assertThrows(DdlException.class, () -> getExecPlan(
                "select /*+ SET_VAR(ai_topn_pushdown_max_global_limit='" + value
                        + "') */ ai_complete(k1) from t7 order by k1 limit 5"));
        Assertions.assertEquals(oldThreshold, connectContext.getSessionVariable().getAITopnPushdownMaxGlobalLimit());
    }

    @Test
    public void testAITopNThresholdDoesNotChangeOrdinaryTopNPlan() throws Exception {
        String query = "v1 from t0 union all select v4 from t1 order by v1 limit 5";
        Assertions.assertEquals(
                getFragmentPlan("select /*+ SET_VAR(ai_topn_pushdown_max_global_limit=0) */ " + query),
                getFragmentPlan("select /*+ SET_VAR(ai_topn_pushdown_max_global_limit=50000) */ " + query));
        Assertions.assertEquals(getFragmentPlan("select " + query),
                getFragmentPlan("select /*+ SET_VAR(enable_ai_topn_pushdown=false) */ " + query));
    }

    private static void assertLocalTopNBoundsAIInput(ExecPlan execPlan, long limit) {
        assertStrictAncestor(execPlan, OperatorType.PHYSICAL_TOPN, OperatorType.PHYSICAL_AI_PROJECT);
        OptExpression physicalAI = findPhysicalOperators(
                execPlan.getPhysicalPlan(), OperatorType.PHYSICAL_AI_PROJECT).get(0);
        PhysicalTopNOperator candidates = Assertions.assertInstanceOf(
                PhysicalTopNOperator.class, physicalAI.inputAt(0).getOp());
        Assertions.assertEquals(SortPhase.PARTIAL, candidates.getSortPhase());
        Assertions.assertEquals(limit, candidates.getLimit());
        Assertions.assertFalse(candidates.isPerPipeline());
        AIProjectNode aiProject = findOnlyAIProjectNode(execPlan);
        SortNode localSort = Assertions.assertInstanceOf(SortNode.class, aiProject.getChild(0));
        Assertions.assertEquals(limit, localSort.getLimit());
        Assertions.assertFalse(localSort.isPerPipeline());
        Assertions.assertSame(localSort.getFragment(), aiProject.getFragment());
        Assertions.assertEquals(limit, findOnlyAIProjectThriftNode(execPlan).getLimit());
        TPlanNode serializedSort = localSort.treeToThrift().getNodes().get(0);
        Assertions.assertEquals(limit, serializedSort.getLimit());
        Assertions.assertFalse(serializedSort.getSort_node().isPer_pipeline());
        assertTopNSortsAIOutput(execPlan, limit);
    }

    private static void assertTopNBoundsAIInput(ExecPlan execPlan, long limit) {
        assertStrictAncestor(execPlan, OperatorType.PHYSICAL_TOPN, OperatorType.PHYSICAL_AI_PROJECT);
        for (OptExpression aiProject : findPhysicalOperators(
                execPlan.getPhysicalPlan(), OperatorType.PHYSICAL_AI_PROJECT)) {
            PhysicalTopNOperator candidates = Assertions.assertInstanceOf(
                    PhysicalTopNOperator.class, aiProject.inputAt(0).getOp(),
                    Explain.toString(execPlan.getPhysicalPlan(), execPlan.getOutputColumns()));
            Assertions.assertTrue(candidates.getSortPhase().isFinal());
            Assertions.assertEquals(limit, candidates.getLimit());
        }
        assertTopNSortsAIOutput(execPlan, limit);
    }

    private static void assertTopNSortsAIOutput(ExecPlan execPlan, long limit) {
        List<PlanNode> nodes = execPlan.getFragments().stream()
                .flatMap(fragment -> fragment.collectNodes().stream()).toList();
        List<TPlanNode> thriftNodes = execPlan.getFragments().stream()
                .flatMap(fragment -> fragment.getPlanRoot().treeToThrift().getNodes().stream()).toList();
        for (AIProjectNode aiProject : findAIProjectNodes(execPlan)) {
            PlanNode upperSort = nodes.stream().filter(SortNode.class::isInstance).filter(sort -> {
                List<AIProjectNode> descendants = new ArrayList<>();
                sort.collect(AIProjectNode.class, descendants);
                return sort.getLimit() == limit && descendants.contains(aiProject);
            }).findFirst().orElseThrow(() -> new AssertionError("The original TopN must still sort the AI output"));
            TPlanNode thriftSort = thriftNodes.stream()
                    .filter(node -> node.getNode_id() == upperSort.getId().asInt()).findFirst().orElseThrow();
            Assertions.assertEquals(TPlanNodeType.SORT_NODE, thriftSort.getNode_type());
            Assertions.assertEquals(limit, thriftSort.getLimit());
        }
    }

    @Test
    public void testTopNDoesNotPrelimitAIProject() throws Exception {
        ExecPlan execPlan = getExecPlan(
                "select ai_complete(k1) as answer from t7 order by answer limit 5");
        assertStrictAncestor(execPlan, OperatorType.PHYSICAL_TOPN, OperatorType.PHYSICAL_AI_PROJECT);
        assertAIProjectHasNoPushedLimit(execPlan);
        Assertions.assertEquals(0, countPhysicalOperators(execPlan, OperatorType.PHYSICAL_LIMIT),
                Explain.toString(execPlan.getPhysicalPlan(), execPlan.getOutputColumns()));

        AIProjectNode aiProject = findOnlyAIProjectNode(execPlan);
        Assertions.assertEquals(Operator.DEFAULT_LIMIT, aiProject.getLimit());
        Assertions.assertEquals(Operator.DEFAULT_LIMIT, aiProject.getChild(0).getLimit());
    }

    @Test
    public void testLimitDoesNotCrossAIDependentFilter() throws Exception {
        ExecPlan execPlan = getExecPlan(
                "select k1 from t7 where ai_complete(k1) = 'x' limit 5");
        assertStrictAncestor(execPlan, OperatorType.PHYSICAL_LIMIT, OperatorType.PHYSICAL_FILTER);
        assertStrictAncestor(execPlan, OperatorType.PHYSICAL_FILTER, OperatorType.PHYSICAL_AI_PROJECT);

        AIProjectNode aiProject = findOnlyAIProjectNode(execPlan);
        Assertions.assertEquals(Operator.DEFAULT_LIMIT, aiProject.getLimit());
        Assertions.assertEquals(Operator.DEFAULT_LIMIT, aiProject.getChild(0).getLimit());
    }

    @ParameterizedTest(name = "limit={0}, offset={1}, localLimit={2}")
    @CsvSource({"5, 0, 5", "5, 3, 8"})
    public void testSplitLimitCreatesGlobalAndLocalAroundAIProject(
            long limit, long offset, long localLimit) {
        ColumnRefOperator column = new ColumnRefOperator(1, IntegerType.INT, "column", true);
        OptExpression leaf = OptExpression.create(new LogicalProjectOperator(Map.of(column, column)));
        LogicalAIProjectOperator aiProject = new LogicalAIProjectOperator(Map.of(column, column));
        OptExpression aiExpression = OptExpression.create(aiProject, leaf);

        OptExpression result = new SplitLimitRule().transform(
                OptExpression.create(LogicalLimitOperator.init(limit, offset), aiExpression), null).get(0);

        LogicalLimitOperator global = (LogicalLimitOperator) result.getOp();
        Assertions.assertTrue(global.isGlobal());
        Assertions.assertEquals(limit, global.getLimit());
        Assertions.assertEquals(offset, global.getOffset());
        Assertions.assertInstanceOf(LogicalLimitOperator.class, result.inputAt(0).getOp());
        LogicalLimitOperator local = (LogicalLimitOperator) result.inputAt(0).getOp();
        Assertions.assertTrue(local.isLocal());
        Assertions.assertEquals(localLimit, local.getLimit());
        Assertions.assertEquals(0, local.getOffset());
        Assertions.assertSame(aiExpression, result.inputAt(0).inputAt(0));
        Assertions.assertEquals(Operator.DEFAULT_LIMIT, aiProject.getLimit());
        Assertions.assertEquals(Operator.DEFAULT_LIMIT, leaf.getOp().getLimit());
    }

    @Test
    public void testDirectLocalLimitPushesThroughAIProjectAndMatchesRulePattern() {
        ColumnRefOperator column = new ColumnRefOperator(1, IntegerType.INT, "column", true);
        OptExpression leaf = OptExpression.create(new MockOperator(OperatorType.LOGICAL_VALUES));
        LogicalAIProjectOperator aiProject = new LogicalAIProjectOperator(Map.of(column, column));
        OptExpression input = OptExpression.create(
                LogicalLimitOperator.local(5), OptExpression.create(aiProject, leaf));
        PushDownLimitDirectRule rule = new PushDownLimitDirectRule();

        Memo memo = new Memo();
        Binder binder = new Binder(
                OptimizerFactory.mockContext(new ColumnRefFactory()),
                rule.getPattern(),
                memo.init(input),
                Stopwatch.createStarted());
        OptExpression bound = binder.next();
        Assertions.assertNotNull(bound, "PushDownLimitDirectRule pattern must bind LOGICAL_LIMIT -> LOGICAL_AI_PROJECT");

        OptExpression result = rule.transform(bound, null).get(0);
        Assertions.assertEquals(OperatorType.LOGICAL_AI_PROJECT, result.getOp().getOpType());
        Assertions.assertEquals(5, result.getOp().getLimit());
        Assertions.assertInstanceOf(LogicalLimitOperator.class, result.inputAt(0).getOp());
        LogicalLimitOperator local = (LogicalLimitOperator) result.inputAt(0).getOp();
        Assertions.assertTrue(local.isLocal());
        Assertions.assertEquals(5, local.getLimit());
        Assertions.assertSame(leaf.getOp(), result.inputAt(0).inputAt(0).getOp());
    }

    @Test
    public void testSplitLimitKeepsOrdinaryMainBehavior() {
        ColumnRefOperator column = new ColumnRefOperator(1, IntegerType.INT, "column", true);
        OptExpression leaf = OptExpression.create(new LogicalProjectOperator(Map.of(column, column)));

        OptExpression ordinaryResult = new SplitLimitRule().transform(
                OptExpression.create(LogicalLimitOperator.init(5), leaf), null).get(0);
        Assertions.assertTrue(((LogicalLimitOperator) ordinaryResult.getOp()).isGlobal());
        Assertions.assertTrue(((LogicalLimitOperator) ordinaryResult.inputAt(0).getOp()).isLocal());
        Assertions.assertSame(leaf, ordinaryResult.inputAt(0).inputAt(0));
    }

    private static TExpr findOnlyAIExpression(Map<Integer, TExpr> expressions) {
        List<TExpr> aiExpressions = expressions.values().stream()
                .filter(expression -> expression.getNodes().get(0).isSetFn())
                .filter(expression -> expression.getNodes().get(0).getFn().getBinary_type() == TFunctionBinaryType.AI)
                .toList();
        Assertions.assertEquals(1, aiExpressions.size());
        return aiExpressions.get(0);
    }

    private static void assertNormalizedAIComplete(String explain) {
        int start = explain.indexOf("ai_complete(");
        Assertions.assertTrue(start >= 0, explain);
        while (start >= 0) {
            int end = explain.indexOf(')', start);
            Assertions.assertTrue(end > start, explain);
            Assertions.assertTrue(explain.substring(start, end).contains(", '', '', json_object("), explain);
            start = explain.indexOf("ai_complete(", end);
        }
    }

    private static AIProjectNode findOnlyAIProjectNode(ExecPlan execPlan) {
        List<AIProjectNode> nodes = findAIProjectNodes(execPlan);
        Assertions.assertEquals(1, nodes.size());
        return nodes.get(0);
    }

    private static List<AIProjectNode> findAIProjectNodes(ExecPlan execPlan) {
        return execPlan.getFragments().stream()
                .flatMap(fragment -> fragment.collectNodes().stream())
                .filter(AIProjectNode.class::isInstance)
                .map(AIProjectNode.class::cast)
                .toList();
    }

    private static FunctionCallExpr findOnlyFunctionCall(ExecPlan execPlan, String functionName) {
        List<ProjectNode> projectNodes = new ArrayList<>();
        for (PlanFragment fragment : execPlan.getFragments()) {
            fragment.getPlanRoot().collect(ProjectNode.class, projectNodes);
        }
        List<FunctionCallExpr> calls = new ArrayList<>();
        projectNodes.forEach(project -> project.getSlotMap().values()
                .forEach(expression -> collectFunctionCalls(expression, functionName, calls)));
        Assertions.assertEquals(1, calls.size());
        return calls.get(0);
    }

    private static boolean containsFunction(Expr expression, String functionName) {
        if (expression instanceof FunctionCallExpr call && call.getFn() != null
                && functionName.equals(call.getFn().functionName())) {
            return true;
        }
        return expression.getChildren().stream().anyMatch(child -> containsFunction(child, functionName));
    }

    private static void collectFunctionCalls(Expr expression, String functionName, List<FunctionCallExpr> calls) {
        if (expression instanceof FunctionCallExpr call && call.getFn() != null
                && functionName.equals(call.getFn().functionName())) {
            calls.add(call);
        }
        expression.getChildren().forEach(child -> collectFunctionCalls(child, functionName, calls));
    }

    private static FunctionCallExpr copyAIPlannerCall(FunctionCallExpr source, String configId) {
        List<Expr> arguments = source.getChildren().stream().map(Expr::clone).toList();
        FunctionCallExpr copy = new FunctionCallExpr(source.getFn().functionName(),
                new FunctionParams(false, arguments), configId);
        copy.setFn(source.getFn());
        return copy;
    }

    private static Map<SlotId, Expr> deepCloneExpressions(Map<SlotId, Expr> expressions) {
        Map<SlotId, Expr> copies = new LinkedHashMap<>();
        expressions.forEach((slot, expression) -> copies.put(slot, expression.clone()));
        return copies;
    }

    private static void assertStrictAncestor(ExecPlan execPlan, OperatorType ancestor, OperatorType descendant) {
        Assertions.assertTrue(hasStrictAncestor(execPlan.getPhysicalPlan(), ancestor, descendant),
                () -> Explain.toString(execPlan.getPhysicalPlan(), execPlan.getOutputColumns()));
    }

    private static boolean hasStrictAncestor(OptExpression expression, OperatorType ancestor, OperatorType descendant) {
        if (expression.getOp().getOpType() == ancestor
                && expression.getInputs().stream().anyMatch(child -> containsPhysicalOperator(child, descendant))) {
            return true;
        }
        return expression.getInputs().stream()
                .anyMatch(child -> hasStrictAncestor(child, ancestor, descendant));
    }

    private static boolean containsPhysicalOperator(OptExpression expression, OperatorType type) {
        return expression.getOp().getOpType() == type
                || expression.getInputs().stream().anyMatch(child -> containsPhysicalOperator(child, type));
    }

    private static boolean subtreeHasProbeRuntimeFilter(PlanNode node) {
        return !node.getProbeRuntimeFilters().isEmpty()
                || node.getChildren().stream().anyMatch(AIProjectPlanTest::subtreeHasProbeRuntimeFilter);
    }

    private static int countPhysicalOperators(ExecPlan execPlan, OperatorType type) {
        return findPhysicalOperators(execPlan.getPhysicalPlan(), type).size();
    }

    private static List<OptExpression> findPhysicalOperators(OptExpression expression, OperatorType type) {
        List<OptExpression> matches = new ArrayList<>();
        if (expression.getOp().getOpType() == type) {
            matches.add(expression);
        }
        expression.getInputs().forEach(child -> matches.addAll(findPhysicalOperators(child, type)));
        return matches;
    }

    private static void assertAIProjectHasNoPushedLimit(ExecPlan execPlan) {
        List<OptExpression> projects = findPhysicalOperators(execPlan.getPhysicalPlan(),
                OperatorType.PHYSICAL_AI_PROJECT);
        Assertions.assertFalse(projects.isEmpty());
        projects.forEach(project -> Assertions.assertEquals(Operator.DEFAULT_LIMIT, project.getOp().getLimit()));
    }

    private static TPlanNode findOnlyAIProjectThriftNode(ExecPlan execPlan) {
        List<TPlanNode> nodes = findAIProjectThriftNodes(execPlan);
        Assertions.assertEquals(1, nodes.size());
        return nodes.get(0);
    }

    private static List<TPlanNode> findAIProjectThriftNodes(ExecPlan execPlan) {
        return execPlan.getFragments().stream()
                .flatMap(fragment -> fragment.getPlanRoot().treeToThrift().getNodes().stream())
                .filter(node -> node.getNode_type() == TPlanNodeType.AI_PROJECT_NODE)
                .toList();
    }

    private static TPlanNode findOnlyAIProjectThriftNode(AIProjectNode node) {
        return node.treeToThrift().getNodes().stream()
                .filter(planNode -> planNode.getNode_type() == TPlanNodeType.AI_PROJECT_NODE)
                .findFirst().orElseThrow();
    }
}
