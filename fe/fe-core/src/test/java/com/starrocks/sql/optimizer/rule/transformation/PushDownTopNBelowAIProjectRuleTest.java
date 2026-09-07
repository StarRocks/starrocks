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

package com.starrocks.sql.optimizer.rule.transformation;

import com.starrocks.catalog.ScalarFunction;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.SortPhase;
import com.starrocks.sql.optimizer.operator.TopNType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAIProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTreeAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.task.RewriteTreeTask;
import com.starrocks.sql.optimizer.task.TaskContext;
import com.starrocks.thrift.TFunctionBinaryType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class PushDownTopNBelowAIProjectRuleTest {
    private final ColumnRefFactory factory = new ColumnRefFactory();
    private final ColumnRefOperator key = factory.create("key", IntegerType.INT, true);
    private final ColumnRefOperator secondKey = factory.create("second_key", IntegerType.INT, true);
    private final ColumnRefOperator prompt = factory.create("prompt", VarcharType.VARCHAR, true);
    private final ColumnRefOperator common = factory.create("common", VarcharType.VARCHAR, true);
    private final ColumnRefOperator answer = factory.create("answer", VarcharType.VARCHAR, true);
    private final ColumnRefOperator secondAnswer = factory.create("second_answer", VarcharType.VARCHAR, true);
    private final List<Ordering> orderings = List.of(new Ordering(key, true, false), new Ordering(secondKey, false, true));
    private final OptExpression source = OptExpression.create(new LogicalValuesOperator(List.of(key, secondKey, prompt)));
    private final LogicalAIProjectOperator aiProject = new LogicalAIProjectOperator(
            Map.of(key, key, secondKey, secondKey, answer, aiCall(101), secondAnswer, aiCall(102)),
            Map.of(common, new CallOperator("concat", VarcharType.VARCHAR,
                    List.of(prompt, ConstantOperator.createVarchar("!")))));
    private final PushDownTopNBelowAIProjectRule rule = new PushDownTopNBelowAIProjectRule();
    private final OptimizerContext optimizerContext = OptimizerFactory.mockContext(factory);

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testLargeLimitUsesLocalCandidatesByDefault(boolean withProject) {
        OptExpression input = input(withProject);
        input.getOp().setLimit(50000);
        Assertions.assertTrue(rule.check(input, optimizerContext));
        OptExpression result = rule.transform(input, optimizerContext).get(0);
        OptExpression rewrittenAI = withProject ? result.inputAt(0).inputAt(0) : result.inputAt(0);
        LogicalTopNOperator candidates = rewrittenAI.inputAt(0).getOp().cast();
        Assertions.assertEquals(SortPhase.PARTIAL, candidates.getSortPhase());
        Assertions.assertEquals(50000, candidates.getLimit());
        Assertions.assertFalse(candidates.isPerPipeline());
        assertSkipped(result);
    }

    @ParameterizedTest
    @CsvSource({"0, PARTIAL", "1, PARTIAL", "4, PARTIAL", "5, FINAL", "6, FINAL", "9223372036854775807, FINAL"})
    public void testThresholdControlsBothProjectionShapes(long threshold, SortPhase expectedPhase) {
        optimizerContext.getSessionVariable().setAiTopnPushdownMaxGlobalLimit(threshold);
        for (boolean withProject : List.of(false, true)) {
            OptExpression input = input(withProject);
            OptExpression child = input.inputAt(0);
            Assertions.assertTrue(rule.check(input, optimizerContext));
            List<OptExpression> results = rule.transform(input, optimizerContext);
            Assertions.assertEquals(1, results.size());
            Assertions.assertSame(child, input.inputAt(0));
            Assertions.assertEquals(Operator.DEFAULT_LIMIT, source.getOp().getLimit());
            OptExpression resultAI = results.get(0).inputAt(0);
            if (withProject) {
                resultAI = resultAI.inputAt(0);
            }
            LogicalTopNOperator candidate = resultAI.inputAt(0).getOp().cast();
            Assertions.assertEquals(5, candidate.getLimit());
            Assertions.assertEquals(expectedPhase, candidate.getSortPhase());
            Assertions.assertFalse(candidate.isPerPipeline());
            assertSkipped(results.get(0));
        }
    }

    @Test
    public void testThresholdIsIndependentOfOtherTopNRules() {
        OptExpression input = input(false);
        optimizerContext.getSessionVariable().setCboPushDownTopNLimit(0);
        Assertions.assertTrue(rule.check(input, optimizerContext));

        optimizerContext.getSessionVariable().setCboPushDownTopNLimit(Long.MAX_VALUE);
        optimizerContext.getSessionVariable().setEnableAiTopnPushdown(false);
        assertSkipped(input);
    }

    @ParameterizedTest
    @CsvSource({"false, 5", "true, 5", "false, 50000", "true, 50000"})
    public void testSwitchDisablesBothProjectionShapes(boolean withProject, long limit) {
        OptExpression input = input(withProject);
        input.getOp().setLimit(limit);
        optimizerContext.getSessionVariable().setEnableAiTopnPushdown(false);
        assertSkipped(input);
    }

    @ParameterizedTest
    @CsvSource({"false, 0", "true, 0", "false, 1000", "true, 1000"})
    public void testRewriteTreePreservesOrderingOutputsAndAIOccurrences(boolean withProject, long threshold) {
        optimizerContext.getSessionVariable().setAiTopnPushdownMaxGlobalLimit(threshold);
        OptExpression input = input(withProject);
        Projection projection = new Projection(Map.of(answer, answer));
        input.getOp().setProjection(projection);
        Map<ColumnRefOperator, ScalarOperator> slots = Map.copyOf(aiProject.getColumnRefMap());
        Map<ColumnRefOperator, ScalarOperator> commonSlots = Map.copyOf(aiProject.getCommonSubOperatorMap());
        OptExpression anchor = OptExpression.create(new LogicalTreeAnchorOperator(), input);
        TaskContext context = new TaskContext(optimizerContext, PhysicalPropertySet.EMPTY,
                source.getOutputColumns(), Double.MAX_VALUE);
        RewriteTreeTask task = new RewriteTreeTask(context, anchor, rule, true);

        task.execute();

        Assertions.assertTrue(task.hasChange());
        OptExpression result = task.getResult();
        Assertions.assertEquals(input.getOp(), result.getOp());
        Assertions.assertNotSame(input.getOp(), result.getOp());
        Assertions.assertSame(projection, result.getOp().getProjection());
        OptExpression rewrittenAI = result.inputAt(0);
        if (withProject) {
            LogicalProjectOperator project = Assertions.assertInstanceOf(LogicalProjectOperator.class, rewrittenAI.getOp());
            Assertions.assertEquals(((LogicalProjectOperator) input.inputAt(0).getOp()).getColumnRefMap(),
                    project.getColumnRefMap());
            rewrittenAI = rewrittenAI.inputAt(0);
        }
        LogicalAIProjectOperator rewritten = Assertions.assertInstanceOf(LogicalAIProjectOperator.class, rewrittenAI.getOp());
        Assertions.assertNotSame(aiProject, rewritten);
        Assertions.assertEquals(slots, rewritten.getColumnRefMap());
        Assertions.assertEquals(commonSlots, rewritten.getCommonSubOperatorMap());
        for (ColumnRefOperator output : List.of(answer, secondAnswer)) {
            Assertions.assertSame(slots.get(output), rewritten.getColumnRefMap().get(output));
        }
        Assertions.assertEquals(ConstantOperator.createInt(101), rewritten.getColumnRefMap().get(answer).getChild(1));
        Assertions.assertEquals(ConstantOperator.createInt(102), rewritten.getColumnRefMap().get(secondAnswer).getChild(1));
        LogicalTopNOperator lower = Assertions.assertInstanceOf(LogicalTopNOperator.class, rewrittenAI.inputAt(0).getOp());
        Assertions.assertEquals(orderings, lower.getOrderByElements());
        Assertions.assertEquals(5, lower.getLimit());
        Assertions.assertEquals(0, lower.getOffset());
        Assertions.assertEquals(threshold == 0 ? SortPhase.PARTIAL : SortPhase.FINAL, lower.getSortPhase());
        Assertions.assertEquals(TopNType.ROW_NUMBER, lower.getTopNType());
        Assertions.assertEquals(Operator.DEFAULT_LIMIT, lower.getPartitionLimit());
        Assertions.assertFalse(lower.isSplit());
        Assertions.assertFalse(lower.isPerPipeline());
        Assertions.assertNull(lower.getProjection());
        Assertions.assertNull(lower.getPredicate());
        Assertions.assertEquals(source.getOutputColumns(), rewrittenAI.inputAt(0).getOutputColumns());
        Assertions.assertSame(source, rewrittenAI.inputAt(0).inputAt(0));
        Assertions.assertEquals(slots, aiProject.getColumnRefMap());
        Assertions.assertEquals(commonSlots, aiProject.getCommonSubOperatorMap());
        Assertions.assertEquals(Operator.DEFAULT_LIMIT, source.getOp().getLimit());
        Assertions.assertSame(source, (withProject ? input.inputAt(0).inputAt(0) : input.inputAt(0)).inputAt(0));
        assertSkipped(result);
    }

    @ParameterizedTest
    @ValueSource(strings = {"unbounded", "zero", "offset", "partial", "split", "rank", "dense_rank", "partition",
            "partition_limit", "per_pipeline", "pre_aggregation", "empty_order", "predicate", "predicate_common"})
    public void testSkipsNonOrdinaryTopN(String state) {
        OptExpression input = input(false);
        LogicalTopNOperator.Builder builder = LogicalTopNOperator.builder().withOperator(input.getOp().cast());
        switch (state) {
            case "unbounded" -> builder.setLimit(Operator.DEFAULT_LIMIT);
            case "zero" -> builder.setLimit(0);
            case "offset" -> builder.setOffset(3);
            case "partial" -> builder.setSortPhase(SortPhase.PARTIAL);
            case "split" -> builder.setIsSplit(true);
            case "rank" -> builder.setTopNType(TopNType.RANK);
            case "dense_rank" -> builder.setTopNType(TopNType.DENSE_RANK);
            case "partition" -> builder.setPartitionByColumns(List.of(key));
            case "partition_limit" -> builder.setPartitionLimit(5);
            case "per_pipeline" -> builder.setPerPipeline(true);
            case "pre_aggregation" -> builder.setPartitionPreAggCall(
                    Map.of(key, new CallOperator("count", IntegerType.BIGINT, List.of(key))));
            case "empty_order" -> builder.setOrderByElements(List.of());
            case "predicate" -> builder.setPredicate(ConstantOperator.TRUE);
            case "predicate_common" -> { }
            default -> throw new AssertionError(state);
        }
        LogicalTopNOperator topN = builder.build();
        if (state.equals("predicate_common")) {
            topN.setPredicateCommonOperators(Map.of(key, ConstantOperator.createInt(1)));
        }
        assertSkipped(OptExpression.create(topN, input.getInputs()));
    }

    @ParameterizedTest
    @CsvSource({"false, limit", "false, predicate", "false, projection", "false, common",
            "true, limit", "true, predicate", "true, projection", "true, common"})
    public void testSkipsDecoratedAIAndMiddleProject(boolean withProject, String decoration) {
        OptExpression input = input(withProject);
        Operator barrier = input.inputAt(0).getOp();
        switch (decoration) {
            case "limit" -> barrier.setLimit(10);
            case "predicate" -> barrier.setPredicate(ConstantOperator.TRUE);
            case "projection" -> barrier.setProjection(new Projection(Map.of(key, key)));
            case "common" -> barrier.setPredicateCommonOperators(Map.of(key, ConstantOperator.createInt(1)));
            default -> throw new AssertionError(decoration);
        }
        assertSkipped(input);
    }

    @ParameterizedTest
    @ValueSource(strings = {"ai_key", "mixed_ai_key", "missing_ai", "missing_child", "remapped_ai", "remapped_project",
            "computed_project", "nondeterministic_project"})
    public void testRequiresIdentityOrderingAcrossBothProjects(String shape) {
        OptExpression input = input(true);
        LogicalProjectOperator project = input.inputAt(0).getOp().cast();
        switch (shape) {
            case "ai_key", "mixed_ai_key" -> {
                List<Ordering> keys = shape.equals("ai_key") ? List.of(new Ordering(answer, true, true))
                        : List.of(orderings.get(0), new Ordering(answer, true, true));
                input = OptExpression.create(new LogicalTopNOperator(keys, 5, 0), input.getInputs());
            }
            case "missing_ai" -> aiProject.getColumnRefMap().remove(key);
            case "missing_child" -> source.getOp().setProjection(new Projection(Map.of(secondKey, secondKey, prompt, prompt)));
            case "remapped_ai" -> aiProject.getColumnRefMap().put(key, secondKey);
            case "remapped_project" -> project.getColumnRefMap().put(key, secondKey);
            case "computed_project" -> project.getColumnRefMap().put(key, ConstantOperator.createInt(1));
            case "nondeterministic_project" -> project.getColumnRefMap().put(
                    factory.create("random", FloatType.DOUBLE, false), new CallOperator("rand", FloatType.DOUBLE, List.of()));
            default -> throw new AssertionError(shape);
        }
        source.deriveLogicalPropertyItself();
        assertSkipped(input);
    }

    @ParameterizedTest
    @ValueSource(longs = {3, 5, 8})
    public void testSkipsAlreadyBoundedAIInput(long limit) {
        OptExpression input = input(false);
        source.getOp().setLimit(limit);
        assertSkipped(input);
        Assertions.assertEquals(limit, source.getOp().getLimit());
    }

    @Test
    public void testSkipsOrdinaryChildWithoutAI() {
        source.deriveLogicalPropertyItself();
        assertSkipped(OptExpression.create(new LogicalTopNOperator(orderings, 5, 0), source));
    }

    private OptExpression input(boolean withProject) {
        source.deriveLogicalPropertyItself();
        OptExpression child = OptExpression.create(aiProject, source);
        if (withProject) {
            Map<ColumnRefOperator, ScalarOperator> outputs = new LinkedHashMap<>(Map.of(
                    key, key, secondKey, secondKey, answer, answer, secondAnswer, secondAnswer));
            child = OptExpression.create(new LogicalProjectOperator(outputs), child);
        }
        return OptExpression.create(new LogicalTopNOperator(orderings, 5, 0), child);
    }

    private CallOperator aiCall(int occurrence) {
        ScalarFunction function = ScalarFunction.createVectorizedBuiltin(
                1, "ai_complete", List.of(VarcharType.VARCHAR), false, VarcharType.VARCHAR);
        function.setBinaryType(TFunctionBinaryType.AI);
        return new CallOperator("ai_complete", VarcharType.VARCHAR,
                List.of(common, ConstantOperator.createInt(occurrence)), function);
    }

    private void assertSkipped(OptExpression input) {
        long oldThreshold = optimizerContext.getSessionVariable().getAiTopnPushdownMaxGlobalLimit();
        try {
            for (long threshold : new long[] {0, 1000}) {
                optimizerContext.getSessionVariable().setAiTopnPushdownMaxGlobalLimit(threshold);
                Assertions.assertFalse(rule.check(input, optimizerContext));
                Assertions.assertTrue(rule.transform(input, optimizerContext).isEmpty());
            }
        } finally {
            optimizerContext.getSessionVariable().setAiTopnPushdownMaxGlobalLimit(oldThreshold);
        }
    }
}
