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
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.planner.AIProjectNode;
import com.starrocks.planner.JoinNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.ProjectNode;
import com.starrocks.planner.SlotId;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.sql.Explain;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.FunctionParams;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.common.AIModelConfigs;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.Memo;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAIProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalLimitOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.MockOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalAIProjectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalLimitOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.Binder;
import com.starrocks.sql.optimizer.rule.transformation.MergeProjectWithChildRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownLimitDirectRule;
import com.starrocks.sql.optimizer.rule.transformation.SplitLimitRule;
import com.starrocks.thrift.TAIEndpointConfig;
import com.starrocks.thrift.TAIModelConfiguration;
import com.starrocks.thrift.TAIProjectNode;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TExpr;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.thrift.TFunctionBinaryType;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class AIProjectPlanTest extends PlanTestBase {
    private static String oldEndpoint;
    private static String oldModel;
    private static String oldProvider;

    @BeforeAll
    public static void setUpAIConfiguration() {
        oldEndpoint = Config.ai_default_chat_endpoint;
        oldModel = Config.ai_default_chat_model;
        oldProvider = Config.ai_default_chat_provider;
        Config.ai_default_chat_endpoint = "https://unit.test.example/v1/chat/completions";
        Config.ai_default_chat_model = "unit-test-model";
        Config.ai_default_chat_provider = AIModelConfigs.OPENAI_COMPATIBLE_PROVIDER;
    }

    @AfterAll
    public static void restoreAIConfiguration() {
        Config.ai_default_chat_endpoint = oldEndpoint;
        Config.ai_default_chat_model = oldModel;
        Config.ai_default_chat_provider = oldProvider;
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
        Assertions.assertEquals(1, call.getNum_children());
        Assertions.assertEquals(AIModelConfigs.SYSTEM_CHAT_CONFIG_ID, call.getAi_model_config_id());
        Assertions.assertTrue(aiExpression.getNodes().stream()
                .noneMatch(node -> node.getNode_type() == TExprNodeType.INT_LITERAL));

        Map<String, TAIModelConfiguration> configs = project.getAi_model_configs();
        Assertions.assertEquals(1, configs.size());
        TAIEndpointConfig chat = configs.get(AIModelConfigs.SYSTEM_CHAT_CONFIG_ID).getChat();
        Assertions.assertEquals("https://unit.test.example/v1/chat/completions", chat.getEndpoint());
        Assertions.assertEquals("unit-test-model", chat.getModel());
        Assertions.assertEquals(AIModelConfigs.OPENAI_COMPATIBLE_PROVIDER, chat.getProvider());
        Assertions.assertFalse(aiNode.toString().contains("AI_FUNCTION_MODEL_API_KEY"));
    }

    @Test
    public void testEveryAICompleteOverloadHasSemanticWireArityAndFixedConfiguration() throws Exception {
        Map<String, Integer> overloads = new LinkedHashMap<>();
        overloads.put("select ai_complete(k1) from t7", 1);
        overloads.put("select ai_complete(k1, map{'temperature': 0.5}) from t7", 2);
        overloads.put("select ai_complete('prompt', cast(null as map<varchar, json>))", 2);
        overloads.put("select ai_complete('explicit-model', k1) from t7", 2);
        overloads.put("select ai_complete('explicit-model', k1, map{'temperature': 0.5}) from t7", 3);

        overloads.forEach((sql, expectedArity) -> {
            ExecPlan execPlan = Assertions.assertDoesNotThrow(() -> getExecPlan(sql));
            TExpr expression = findOnlyAIExpression(
                    findOnlyAIProjectThriftNode(execPlan).getAi_project_node().getSlot_map());
            TExprNode call = expression.getNodes().get(0);
            Assertions.assertEquals(expectedArity, call.getNum_children(), sql);
            Assertions.assertEquals(AIModelConfigs.SYSTEM_CHAT_CONFIG_ID, call.getAi_model_config_id(), sql);
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
        TAIEndpointConfig first = findOnlyAIProjectThriftNode(node).getAi_project_node()
                .getAi_model_configs().get(AIModelConfigs.SYSTEM_CHAT_CONFIG_ID).getChat();

        Config.ai_default_chat_endpoint = "https://changed.example/v1/chat/completions";
        Config.ai_default_chat_model = "changed-model";
        Config.ai_default_chat_provider = "changed-provider";
        try {
            TAIEndpointConfig second = findOnlyAIProjectThriftNode(node).getAi_project_node()
                    .getAi_model_configs().get(AIModelConfigs.SYSTEM_CHAT_CONFIG_ID).getChat();
            Assertions.assertEquals(first, second);
            Assertions.assertEquals("https://unit.test.example/v1/chat/completions", second.getEndpoint());
            Assertions.assertEquals("unit-test-model", second.getModel());
            Assertions.assertEquals(AIModelConfigs.OPENAI_COMPATIBLE_PROVIDER, second.getProvider());
        } finally {
            Config.ai_default_chat_endpoint = "https://unit.test.example/v1/chat/completions";
            Config.ai_default_chat_model = "unit-test-model";
            Config.ai_default_chat_provider = AIModelConfigs.OPENAI_COMPATIBLE_PROVIDER;
        }
    }

    @Test
    public void testExecPlanCachesSystemChatConfigurationPerPlan() {
        ExecPlan firstPlan = new ExecPlan();
        AIModelConfigs.SystemChatConfig first = firstPlan.getOrCreateSystemChatConfig();

        Config.ai_default_chat_endpoint = "https://changed.example/v1/chat/completions";
        Config.ai_default_chat_model = "changed-model";
        Config.ai_default_chat_provider = AIModelConfigs.OPENAI_COMPATIBLE_PROVIDER;
        try {
            AIModelConfigs.SystemChatConfig second = firstPlan.getOrCreateSystemChatConfig();
            Assertions.assertSame(first, second);
            Assertions.assertEquals("https://unit.test.example/v1/chat/completions", second.endpoint());
            Assertions.assertEquals("unit-test-model", second.model());

            ExecPlan secondPlan = new ExecPlan();
            AIModelConfigs.SystemChatConfig changed = secondPlan.getOrCreateSystemChatConfig();
            Assertions.assertEquals("https://changed.example/v1/chat/completions", changed.endpoint());
            Assertions.assertEquals("changed-model", changed.model());

            Config.ai_default_chat_endpoint = "";
            Config.ai_default_chat_model = "";
            Config.ai_default_chat_provider = "";
            Assertions.assertSame(first, firstPlan.getOrCreateSystemChatConfig());
            Assertions.assertSame(changed, secondPlan.getOrCreateSystemChatConfig());
        } finally {
            Config.ai_default_chat_endpoint = "https://unit.test.example/v1/chat/completions";
            Config.ai_default_chat_model = "unit-test-model";
            Config.ai_default_chat_provider = AIModelConfigs.OPENAI_COMPATIBLE_PROVIDER;
        }
    }

    @Test
    public void testOrdinaryPlanDoesNotCaptureSystemChatConfiguration() {
        String configuredEndpoint = Config.ai_default_chat_endpoint;
        String configuredModel = Config.ai_default_chat_model;
        String configuredProvider = Config.ai_default_chat_provider;
        Config.ai_default_chat_endpoint = "";
        Config.ai_default_chat_model = "";
        Config.ai_default_chat_provider = "";
        try {
            Assertions.assertDoesNotThrow(() -> getExecPlan("select k1 from t7"));
        } finally {
            Config.ai_default_chat_endpoint = configuredEndpoint;
            Config.ai_default_chat_model = configuredModel;
            Config.ai_default_chat_provider = configuredProvider;
        }
    }

    @Test
    public void testNestedAIProjectsCaptureSystemChatConfigurationOncePerPlan() throws Exception {
        ExecPlan template = getExecPlan("select ai_complete(ai_complete(k1)) from t7");
        AtomicInteger captureCount = new AtomicInteger();

        try (MockedStatic<AIModelConfigs> configs = Mockito.mockStatic(
                AIModelConfigs.class, Mockito.CALLS_REAL_METHODS)) {
            configs.when(() -> AIModelConfigs.systemChatSnapshot(
                            Mockito.any(AIModelConfigs.DefaultModelRequirement.class)))
                    .thenAnswer(invocation -> {
                        AIModelConfigs.SystemChatConfig snapshot =
                                (AIModelConfigs.SystemChatConfig) invocation.callRealMethod();
                        if (captureCount.incrementAndGet() == 1) {
                            Config.ai_default_chat_endpoint = "https://changed.example/v1/chat/completions";
                            Config.ai_default_chat_model = "changed-model";
                        }
                        return snapshot;
                    });

            ExecPlan rebuilt = PlanFragmentBuilder.createPhysicalPlan(
                    template.getPhysicalPlan(), connectContext, template.getOutputColumns(),
                    template.getColumnRefFactory(), template.getColNames(), TResultSinkType.MYSQL_PROTOCAL, true);
            List<TAIEndpointConfig> capturedConfigs = findAIProjectThriftNodes(rebuilt).stream()
                    .map(TPlanNode::getAi_project_node)
                    .map(TAIProjectNode::getAi_model_configs)
                    .map(modelConfigs -> modelConfigs.get(AIModelConfigs.SYSTEM_CHAT_CONFIG_ID))
                    .map(TAIModelConfiguration::getChat)
                    .toList();

            Assertions.assertEquals(2, capturedConfigs.size());
            Assertions.assertEquals(1, captureCount.get());
            Assertions.assertEquals(capturedConfigs.get(0), capturedConfigs.get(1));
            capturedConfigs.forEach(config -> {
                Assertions.assertEquals("https://unit.test.example/v1/chat/completions", config.getEndpoint());
                Assertions.assertEquals("unit-test-model", config.getModel());
            });
        } finally {
            Config.ai_default_chat_endpoint = "https://unit.test.example/v1/chat/completions";
            Config.ai_default_chat_model = "unit-test-model";
            Config.ai_default_chat_provider = AIModelConfigs.OPENAI_COMPATIBLE_PROVIDER;
        }
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
    public void testExplicitModelDoesNotRequireConfiguredDefaultModel() throws Exception {
        String configuredModel = Config.ai_default_chat_model;
        Config.ai_default_chat_model = "";
        try {
            ExecPlan explicitModelPlan = getExecPlan("select ai_complete('explicit-model', k1) from t7");
            TAIEndpointConfig chat = findOnlyAIProjectThriftNode(explicitModelPlan).getAi_project_node()
                    .getAi_model_configs().get(AIModelConfigs.SYSTEM_CHAT_CONFIG_ID).getChat();
            Assertions.assertEquals("", chat.getModel());
            Assertions.assertThrows(SemanticException.class,
                    () -> getExecPlan("select ai_complete(k1) from t7"));
        } finally {
            Config.ai_default_chat_model = configuredModel;
        }
    }

    @Test
    public void testAIProjectValidatesRequirementAgainstPassedSnapshot() throws Exception {
        AIModelConfigs.SystemChatConfig snapshotWithoutDefault = new AIModelConfigs.SystemChatConfig(
                "https://captured.example/v1/chat/completions", "", AIModelConfigs.OPENAI_COMPATIBLE_PROVIDER);

        ExecPlan explicitPlan = getExecPlan("select ai_complete('explicit-model', k1) from t7");
        AIProjectNode explicitProject = findOnlyAIProjectNode(explicitPlan);
        TupleDescriptor explicitTuple = explicitPlan.getDescTbl().getTupleDesc(explicitProject.getTupleIds().get(0));
        Assertions.assertDoesNotThrow(() -> new AIProjectNode(
                explicitPlan.getNextNodeId(), explicitTuple, explicitProject.getChild(0),
                explicitProject.getSlotMap(), explicitProject.getCommonSlotMap(), snapshotWithoutDefault));

        ExecPlan promptPlan = getExecPlan("select ai_complete(k1) from t7");
        AIProjectNode promptProject = findOnlyAIProjectNode(promptPlan);
        TupleDescriptor promptTuple = promptPlan.getDescTbl().getTupleDesc(promptProject.getTupleIds().get(0));
        StarRocksPlannerException failure = Assertions.assertThrows(StarRocksPlannerException.class,
                () -> new AIProjectNode(promptPlan.getNextNodeId(), promptTuple, promptProject.getChild(0),
                        promptProject.getSlotMap(), promptProject.getCommonSlotMap(), snapshotWithoutDefault));
        Assertions.assertTrue(failure.getMessage().contains("ai_default_chat_model"));
    }

    @Test
    public void testExplainContainsOnlySemanticArgumentsAndNoConfiguration() throws Exception {
        ExecPlan execPlan = getExecPlan("select ai_complete(k1) from t7");
        String optimizerExplain = Explain.toString(execPlan.getPhysicalPlan(), execPlan.getOutputColumns());
        assertUnaryAIComplete(optimizerExplain);
        List<String> explains = List.of(
                optimizerExplain,
                execPlan.getExplainString(TExplainLevel.NORMAL),
                execPlan.getExplainString(TExplainLevel.VERBOSE),
                execPlan.getExplainString(TExplainLevel.COSTS));

        for (String explain : explains) {
            Assertions.assertTrue(explain.contains("ai_complete"), explain);
            Assertions.assertFalse(explain.contains(Config.ai_default_chat_endpoint), explain);
            Assertions.assertFalse(explain.contains(Config.ai_default_chat_model), explain);
            Assertions.assertFalse(explain.contains(Config.ai_default_chat_provider), explain);
            Assertions.assertFalse(explain.contains(AIModelConfigs.SYSTEM_CHAT_CONFIG_ID), explain);
            Assertions.assertFalse(explain.contains("AI_FUNCTION_MODEL_API_KEY"), explain);
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
        AIModelConfigs.SystemChatConfig systemChatConfig = execPlan.getOrCreateSystemChatConfig();
        Map.Entry<SlotId, Expr> aiEntry = original.getSlotMap().entrySet().stream()
                .filter(entry -> containsFunction(entry.getValue(), "ai_complete"))
                .findFirst().orElseThrow();
        FunctionCallExpr aiCall = (FunctionCallExpr) aiEntry.getValue();

        FunctionCallExpr missingIdCall = copyAIPlannerCall(aiCall, null);
        Assertions.assertThrows(IllegalStateException.class,
                () -> new AIProjectNode(execPlan.getNextNodeId(), tupleDescriptor, original.getChild(0),
                        Map.of(aiEntry.getKey(), missingIdCall), Map.of(), systemChatConfig));

        List<String> untrustedIds = List.of(
                "", "arbitrary-config", "AI_FUNCTION_MODEL_API_KEY", "credential-looking-value");
        for (String untrustedId : untrustedIds) {
            FunctionCallExpr untrustedCall = copyAIPlannerCall(aiCall, untrustedId);
            IllegalStateException failure = Assertions.assertThrows(IllegalStateException.class,
                    () -> new AIProjectNode(execPlan.getNextNodeId(), tupleDescriptor, original.getChild(0),
                            Map.of(aiEntry.getKey(), untrustedCall), Map.of(), systemChatConfig));
            if (!untrustedId.isEmpty()) {
                Assertions.assertFalse(failure.getMessage().contains(untrustedId));
            }
        }

        FunctionCallExpr cloned = (FunctionCallExpr) aiCall.clone();
        Assertions.assertEquals(AIModelConfigs.SYSTEM_CHAT_CONFIG_ID, cloned.getAiModelConfigId());
    }

    @Test
    public void testAIProjectDefensivelyOwnsImmutableExpressionMaps() throws Exception {
        ExecPlan execPlan = getExecPlan("select ai_complete(k1) from t7");
        AIProjectNode original = findOnlyAIProjectNode(execPlan);
        TupleDescriptor tupleDescriptor = execPlan.getDescTbl().getTupleDesc(original.getTupleIds().get(0));
        AIModelConfigs.SystemChatConfig systemChatConfig = execPlan.getOrCreateSystemChatConfig();
        Map<SlotId, Expr> callerSlotMap = deepCloneExpressions(original.getSlotMap());
        Map<SlotId, Expr> callerCommonMap = deepCloneExpressions(original.getCommonSlotMap());
        AIProjectNode isolated = new AIProjectNode(execPlan.getNextNodeId(), tupleDescriptor, original.getChild(0),
                callerSlotMap, callerCommonMap, systemChatConfig);
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
        AIModelConfigs.SystemChatConfig systemChatConfig = execPlan.getOrCreateSystemChatConfig();
        Map.Entry<SlotId, Expr> aiEntry = original.getSlotMap().entrySet().stream()
                .filter(entry -> containsFunction(entry.getValue(), "ai_complete"))
                .findFirst().orElseThrow();

        Map<SlotId, Expr> nonAIOutput = deepCloneExpressions(original.getSlotMap());
        nonAIOutput.put(new SlotId(10_000), findOnlyFunctionCall(getExecPlan("select rand() from t7"), "rand"));
        Assertions.assertThrows(IllegalStateException.class, () -> new AIProjectNode(
                execPlan.getNextNodeId(), tupleDescriptor, original.getChild(0), nonAIOutput, Map.of(),
                systemChatConfig));

        FunctionCallExpr aiCall = (FunctionCallExpr) aiEntry.getValue();
        SlotRef inputSlot = (SlotRef) aiCall.getChild(0);

        FunctionCallExpr nestedAICall = (FunctionCallExpr) aiCall.clone();
        nestedAICall.setChild(0, aiCall.clone());
        Assertions.assertAll(
                () -> Assertions.assertThrows(IllegalStateException.class, () -> new AIProjectNode(
                        execPlan.getNextNodeId(), tupleDescriptor, original.getChild(0),
                        Map.of(aiEntry.getKey(), nestedAICall), Map.of(), systemChatConfig)),
                () -> Assertions.assertThrows(IllegalStateException.class, () -> new AIProjectNode(
                        execPlan.getNextNodeId(), tupleDescriptor, original.getChild(0), original.getSlotMap(),
                        Map.of(aiEntry.getKey(), inputSlot.clone()), systemChatConfig)));

        Map<SlotId, Expr> mismatchedIdentity = deepCloneExpressions(original.getSlotMap());
        mismatchedIdentity.put(new SlotId(10_001), inputSlot.clone());
        Assertions.assertThrows(IllegalStateException.class, () -> new AIProjectNode(
                execPlan.getNextNodeId(), tupleDescriptor, original.getChild(0), mismatchedIdentity, Map.of(),
                systemChatConfig));

        SlotRef aiOutputReference = new SlotRef(aiEntry.getKey());
        Assertions.assertThrows(IllegalStateException.class, () -> new AIProjectNode(
                execPlan.getNextNodeId(), tupleDescriptor, original.getChild(0), original.getSlotMap(),
                Map.of(new SlotId(10_002), aiOutputReference), systemChatConfig));
    }

    @Test
    public void testAIFunctionDisablesShortCircuitWithoutNameMatching() throws Exception {
        boolean shortCircuitEnabled = connectContext.getSessionVariable().isEnableShortCircuit();
        connectContext.getSessionVariable().setEnableShortCircuit(true);
        try {
            Assertions.assertFalse(getExecPlan(
                    "select ai_complete(v3) from tprimary1 where pk1 = 20").isShortCircuit());
            Assertions.assertTrue(getExecPlan(
                    "select ai_query(v3, parse_json('{}')) from tprimary1 where pk1 = 20").isShortCircuit());
        } finally {
            connectContext.getSessionVariable().setEnableShortCircuit(shortCircuitEnabled);
        }
    }

    @Test
    public void testAIProjectRejectsAIAndNondeterministicCommonExpressions() throws Exception {
        ExecPlan aiPlan = getExecPlan("select ai_complete(k1) from t7");
        AIProjectNode aiProject = findOnlyAIProjectNode(aiPlan);
        TupleDescriptor tupleDescriptor = aiPlan.getDescTbl().getTupleDesc(aiProject.getTupleIds().get(0));
        AIModelConfigs.SystemChatConfig systemChatConfig = aiPlan.getOrCreateSystemChatConfig();
        Map.Entry<SlotId, Expr> aiExpression = aiProject.getSlotMap().entrySet().stream()
                .filter(entry -> containsFunction(entry.getValue(), "ai_complete"))
                .findFirst().orElseThrow();

        Assertions.assertThrows(IllegalStateException.class, () -> new AIProjectNode(
                aiPlan.getNextNodeId(), tupleDescriptor, aiProject.getChild(0), aiProject.getSlotMap(),
                Map.of(aiExpression.getKey(), aiExpression.getValue()), systemChatConfig));

        FunctionCallExpr rand = findOnlyFunctionCall(getExecPlan("select rand() from t7"), "rand");
        Assertions.assertThrows(IllegalStateException.class, () -> new AIProjectNode(
                aiPlan.getNextNodeId(), tupleDescriptor, aiProject.getChild(0), aiProject.getSlotMap(),
                Map.of(aiExpression.getKey(), rand), systemChatConfig));
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

    private static void assertUnaryAIComplete(String explain) {
        int start = explain.indexOf("ai_complete(");
        Assertions.assertTrue(start >= 0, explain);
        while (start >= 0) {
            int end = explain.indexOf(')', start);
            Assertions.assertTrue(end > start, explain);
            Assertions.assertFalse(explain.substring(start, end).contains(","), explain);
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
