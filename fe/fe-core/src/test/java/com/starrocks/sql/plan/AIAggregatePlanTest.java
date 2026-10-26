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

import com.starrocks.catalog.AggregateFunction;
import com.starrocks.common.Config;
import com.starrocks.planner.AggregationNode;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.physical.PhysicalAIProjectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalRepeatOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.thrift.TExpr;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.type.VarbinaryType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AIAggregatePlanTest extends PlanTestBase {
    private String endpoint;
    private String model;
    private String provider;

    @BeforeEach
    public void configureAI() {
        endpoint = Config.ai_default_chat_endpoint;
        model = Config.ai_default_chat_model;
        provider = Config.ai_default_chat_provider;
        Config.ai_default_chat_endpoint = "https://unit.test.example/v1/chat/completions";
        Config.ai_default_chat_model = "unit-test-model";
        Config.ai_default_chat_provider = "openai_compatible";
    }

    @AfterEach
    public void restoreAI() {
        Config.ai_default_chat_endpoint = endpoint;
        Config.ai_default_chat_model = model;
        Config.ai_default_chat_provider = provider;
    }

    @Test
    public void testIndependentOccurrencesHaveDistinctReducerOutputs() throws Exception {
        ExecPlan plan = getExecPlan("select ai_agg(cast(v2 as varchar), 'summarize'), "
                + "ai_agg(cast(v2 as varchar), 'summarize') from t0");
        TPlanNode project = aiProjects(plan).get(0);
        List<TExpr> calls = project.getAi_project_node().getSlot_map().values().stream()
                .filter(expr -> expr.getNodes().get(0).isSetFn()).toList();
        Assertions.assertEquals(2, calls.size());
        for (TExpr call : calls) {
            Assertions.assertEquals("__ai_agg_reduce", call.getNodes().get(0).getFn().getName().getFunction_name());
            Assertions.assertEquals(2, call.getNodes().get(0).getNum_children());
        }
    }

    @Test
    public void testInstructionGroupingConstantsBindNormally() throws Exception {
        ExecPlan plan = getExecPlan("select ai_agg(cast(v2 as varchar), 'summarize', map{'temperature': 0.1}), "
                + "sum(v2) from t0 group by grouping sets (('summarize'), ())");
        TPlanNode project = aiProjects(plan).get(0);
        Assertions.assertEquals(1, project.getAi_project_node().getAi_model_configsSize());
        Assertions.assertEquals("unit-test-model", project.getAi_project_node().getAi_model_configs()
                .values().iterator().next().getChat().getModel());
        TExpr reducer = project.getAi_project_node().getSlot_map().values().stream()
                .filter(expr -> expr.getNodes().get(0).isSetFn()).findFirst().orElseThrow();
        Assertions.assertEquals(3, reducer.getNodes().get(0).getNum_children());
        Assertions.assertEquals("__ai_agg_reduce", reducer.getNodes().get(0).getFn().getName().getFunction_name());
    }

    @Test
    public void testHavingAndAISortConsumeReducedResults() throws Exception {
        ExecPlan plan = getExecPlan("select v1, ai_agg(cast(v2 as varchar), 'summarize') a from t0 "
                + "group by v1 having a != '' order by a limit 2");
        OptExpression ai = onlyPhysicalAI(plan);
        PhysicalAIProjectOperator project = ai.getOp().cast();
        ColumnRefOperator result = project.getColumnRefMap().entrySet().stream()
                .filter(entry -> entry.getValue() instanceof CallOperator)
                .map(Map.Entry::getKey).findFirst().orElseThrow();
        List<OptExpression> ancestors = physicalNodes(plan.getPhysicalPlan()).stream()
                .filter(node -> node != ai && physicalNodes(node).contains(ai)).toList();
        Assertions.assertTrue(ancestors.stream().anyMatch(node -> node.getOp().getPredicate() != null
                && node.getOp().getPredicate().getUsedColumns().contains(result)));
        Assertions.assertTrue(ancestors.stream()
                .anyMatch(node -> node.getOp().getOpType() == OperatorType.PHYSICAL_TOPN));
        Assertions.assertTrue(physicalNodes(ai.inputAt(0)).stream()
                .noneMatch(node -> node.getOp().getOpType() == OperatorType.PHYSICAL_TOPN));
        assertCollectorBelow(ai);
    }

    @Test
    public void testGroupingKeyTopNBoundsGroupsWithoutTruncatingCollectorInput() throws Exception {
        ExecPlan plan = getExecPlan("select v1, ai_agg(cast(v2 as varchar), 'summarize') a from t0 "
                + "group by v1 order by v1 limit 2");
        OptExpression ai = onlyPhysicalAI(plan);
        List<OptExpression> boundedGroups = physicalNodes(ai.inputAt(0)).stream()
                .filter(node -> node.getOp().getOpType() == OperatorType.PHYSICAL_TOPN).toList();
        Assertions.assertFalse(boundedGroups.isEmpty());
        boundedGroups.forEach(AIAggregatePlanTest::assertCollectorBelow);
        for (OptExpression collector : collectorNodes(ai)) {
            Assertions.assertFalse(collector.getOp().hasLimit());
            Assertions.assertTrue(physicalNodes(collector.inputAt(0)).stream()
                    .noneMatch(node -> node.getOp().getOpType() == OperatorType.PHYSICAL_TOPN));
        }
    }

    @Test
    public void testDistributedCollectorSerializesAndMergesVarbinaryState() throws Exception {
        int oldStage = connectContext.getSessionVariable().getNewPlannerAggStage();
        try {
            connectContext.getSessionVariable().setNewPlanerAggStage(2);
            ExecPlan plan = getExecPlan("select v2, count(*), ai_agg(cast(v3 as varchar), 'summarize') "
                    + "from t0 group by v2");
            List<FunctionCallExpr> collectors = plan.getFragments().stream()
                    .flatMap(fragment -> fragment.collectNodes().stream())
                    .filter(AggregationNode.class::isInstance).map(AggregationNode.class::cast)
                    .flatMap(node -> node.getAggInfo().getMaterializedAggregateExprs().stream())
                    .filter(call -> call.getFunctionName().equals("__ai_collect_text")).toList();
            Assertions.assertTrue(collectors.stream().anyMatch(FunctionCallExpr::isMergeAggFn));
            Assertions.assertTrue(collectors.stream().anyMatch(call -> !call.isMergeAggFn()));
            for (FunctionCallExpr call : collectors) {
                AggregateFunction function = Assertions.assertInstanceOf(AggregateFunction.class, call.getFn());
                Assertions.assertEquals(VarbinaryType.VARBINARY, call.getType());
                Assertions.assertEquals(VarbinaryType.VARBINARY, function.getIntermediateTypeOrReturnType());
                Assertions.assertEquals(call.isMergeAggFn() ? VarbinaryType.VARBINARY : VarcharType.VARCHAR,
                        call.getChild(0).getType());
            }
            assertCollectorBelow(onlyPhysicalAI(plan));
        } finally {
            connectContext.getSessionVariable().setNewPlanerAggStage(oldStage);
        }
    }

    @Test
    public void testEmptyNullAndUniqueKeyInputsKeepNativeAggregateSemantics() throws Exception {
        ExecPlan empty = getExecPlan("select ai_agg(cast(v2 as varchar), 'summarize'), count(*) from t0 where false");
        Assertions.assertEquals(1, empty.getPhysicalPlan().getStatistics().getOutputRowCount());
        for (ExecPlan plan : List.of(empty,
                getExecPlan("select ai_agg(cast(null as varchar), 'summarize') from t0"),
                getExecPlan("select k, ai_agg(x, 'summarize') from "
                        + "(values (cast(null as bigint), 'a'), (null, 'b')) t(k, x) group by k"),
                getExecPlan("select id, ai_agg(varchar_value, 'summarize') "
                        + "from test_agg_group_single_unique_key group by id"))) {
            OptExpression ai = onlyPhysicalAI(plan);
            assertCollectorBelow(ai);
            PhysicalAIProjectOperator project = ai.getOp().cast();
            project.getColumnRefMap().values().stream().filter(CallOperator.class::isInstance)
                    .map(CallOperator.class::cast).forEach(call -> {
                        Assertions.assertEquals("__ai_agg_reduce", call.getFnName());
                        Assertions.assertEquals(VarbinaryType.VARBINARY, call.getChild(0).getType());
                        Assertions.assertTrue(call.isNullable());
                    });
        }
    }

    @Test
    public void testRepeatedGroupingSetsKeepSeparateGroupingIds() throws Exception {
        ExecPlan plan = getExecPlan("select v1, ai_agg(cast(v2 as varchar), 'summarize') "
                + "from t0 group by grouping sets ((v1), (v1), ())");
        OptExpression ai = onlyPhysicalAI(plan);
        PhysicalRepeatOperator repeat = physicalNodes(ai).stream().map(OptExpression::getOp)
                .filter(PhysicalRepeatOperator.class::isInstance).map(PhysicalRepeatOperator.class::cast)
                .findFirst().orElseThrow();
        Assertions.assertEquals(3, repeat.getRepeatColumnRef().size());
        Assertions.assertEquals(repeat.getRepeatColumnRef().get(0), repeat.getRepeatColumnRef().get(1));
        Assertions.assertEquals(3, repeat.getGroupingIds().get(0).stream().distinct().count());
        Assertions.assertTrue(collectorNodes(ai).stream().anyMatch(node ->
                ((PhysicalHashAggregateOperator) node.getOp()).getGroupBys().containsAll(repeat.getOutputGrouping())));
    }

    @Test
    public void testMultipleCTEReferencesShareOneReduction() throws Exception {
        double oldRatio = connectContext.getSessionVariable().getCboCTERuseRatio();
        try {
            connectContext.getSessionVariable().setCboCTERuseRatio(0);
            ExecPlan plan = getExecPlan("with a as (select ai_agg(cast(v2 as varchar), 'summarize') x from t0) "
                    + "select a.x, b.x from a cross join a b");
            OptExpression ai = onlyPhysicalAI(plan);
            assertCollectorBelow(ai);
            Assertions.assertEquals(2, physicalNodes(plan.getPhysicalPlan()).stream()
                    .filter(node -> node.getOp().getOpType() == OperatorType.PHYSICAL_CTE_CONSUME).count());
            Assertions.assertTrue(physicalNodes(plan.getPhysicalPlan()).stream()
                    .filter(node -> node.getOp().getOpType() == OperatorType.PHYSICAL_CTE_PRODUCE)
                    .anyMatch(node -> physicalNodes(node).contains(ai)));
        } finally {
            connectContext.getSessionVariable().setCboCTERuseRatio(oldRatio);
        }
    }

    private static OptExpression onlyPhysicalAI(ExecPlan plan) {
        List<OptExpression> nodes = physicalNodes(plan.getPhysicalPlan()).stream()
                .filter(node -> node.getOp() instanceof PhysicalAIProjectOperator).toList();
        Assertions.assertEquals(1, nodes.size());
        return nodes.get(0);
    }

    private static void assertCollectorBelow(OptExpression root) {
        Assertions.assertFalse(collectorNodes(root).isEmpty());
    }

    private static List<OptExpression> collectorNodes(OptExpression root) {
        return physicalNodes(root).stream()
                .filter(node -> node.getOp() instanceof PhysicalHashAggregateOperator aggregate
                        && aggregate.getAggregations().values().stream()
                        .anyMatch(call -> call.getFnName().equals("__ai_collect_text"))).toList();
    }

    private static List<OptExpression> physicalNodes(OptExpression root) {
        List<OptExpression> nodes = new ArrayList<>();
        nodes.add(root);
        root.getInputs().forEach(child -> nodes.addAll(physicalNodes(child)));
        return nodes;
    }

    private static List<TPlanNode> aiProjects(ExecPlan plan) {
        return plan.getFragments().stream()
                .flatMap(fragment -> fragment.getPlanRoot().treeToThrift().getNodes().stream())
                .filter(node -> node.getNode_type() == TPlanNodeType.AI_PROJECT_NODE).toList();
    }

    @Test
    public void testAggregateLowersWithOrdinaryAggregatesAcrossStages() throws Exception {
        int oldStage = connectContext.getSessionVariable().getNewPlannerAggStage();
        try {
            for (int stage : List.of(0, 1, 2, 3, 4)) {
                connectContext.getSessionVariable().setNewPlanerAggStage(stage);
                String plan = getFragmentPlan("select v1, count(*), ai_agg(cast(v2 as varchar), 'summarize') "
                        + "from t0 group by v1");
                assertContains(plan, "AIProject", "__ai_agg_reduce", "__ai_collect_text", "count");
            }
        } finally {
            connectContext.getSessionVariable().setNewPlanerAggStage(oldStage);
        }
    }

    @Test
    public void testAggregateContextsKeepReducerAboveGrouping() throws Exception {
        for (String sql : List.of(
                "select count(distinct v1), ai_agg(cast(v2 as varchar), 'summarize') from t0",
                "select ai_complete(ai_agg(cast(v2 as varchar), 'summarize')) from t0",
                "select v1, ai_agg(cast(v2 as varchar), 'summarize') a from t0 group by v1 "
                        + "having v1 > 0 and a != ''",
                "select v1, ai_agg(cast(v2 as varchar), 'summarize') a from t0 group by v1 "
                        + "having v1 > 0 or a != ''",
                "select v1, ai_agg(cast(v2 as varchar), 'summarize') a from t0 group by v1 having a != ''",
                "select v1, ai_agg(cast(v2 as varchar), 'summarize') a from t0 group by v1 order by v1 limit 2",
                "select v1, ai_agg(cast(v2 as varchar), 'summarize') a from t0 group by v1 order by a limit 2",
                "select v1, sum(v2), ai_agg(cast(v2 as varchar), 'summarize') from t0 "
                        + "group by grouping sets ((v1), ())",
                "with a as (select ai_agg(cast(v2 as varchar), 'summarize') x from t0) select x from a")) {
            assertContains(getFragmentPlan(sql), "AIProject", "__ai_agg_reduce", "__ai_collect_text");
        }
    }
}
