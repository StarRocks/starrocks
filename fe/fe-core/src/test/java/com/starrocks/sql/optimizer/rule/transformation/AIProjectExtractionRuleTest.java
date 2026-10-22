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

import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.OptimizerOptions;
import com.starrocks.sql.optimizer.RowOutputInfo;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.LogicalProperty;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAIProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalApplyOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalLimitOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTreeAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalAIProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.MultiInPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.Rule;
import com.starrocks.sql.optimizer.rule.RuleSet;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.implementation.AIProjectImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.CTEConsumeInlineImplementationRule;
import com.starrocks.sql.optimizer.rule.implementation.CTEConsumerReuseImplementationRule;
import com.starrocks.sql.optimizer.rule.tree.AIFunctionLoweringRule;
import com.starrocks.sql.optimizer.rule.tree.AIQuantifiedApplyLoweringRule;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.optimizer.task.TaskContext;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.type.DateType;
import com.starrocks.type.VarcharType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AIProjectExtractionRuleTest {
    private static ConnectContext connectContext;

    @BeforeAll
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
        connectContext = AnalyzeTestUtil.getConnectContext();
    }

    @BeforeEach
    public void setUpSystemChat() {
        Config.ai_default_chat_endpoint = "https://models.example.test/v1/chat/completions";
        Config.ai_default_chat_model = "default-model";
        Config.ai_default_chat_provider = "openai_compatible";
    }

    @AfterEach
    public void clearSystemChat() {
        Config.ai_default_chat_endpoint = "";
        Config.ai_default_chat_model = "";
        Config.ai_default_chat_provider = "";
    }

    @Test
    public void testProjectionKeepsSeparateSyntacticOccurrences() {
        RewriteResult result = rewrite("select ai_complete(ta), ai_complete(ta) from tall");
        List<LogicalAIProjectOperator> projects = logicalAIProjects(result.root);

        Assertions.assertEquals(1, projects.size());
        List<Map.Entry<ColumnRefOperator, ScalarOperator>> calls = aiEntries(projects.get(0));
        Assertions.assertEquals(2, calls.size());
        Assertions.assertNotEquals(calls.get(0).getKey(), calls.get(1).getKey());
        Assertions.assertNotEquals(hiddenOccurrenceId(calls.get(0).getValue().cast()),
                hiddenOccurrenceId(calls.get(1).getValue().cast()));
    }

    @Test
    public void testPredicatePushdownContainsNoAILoweringRules() {
        Assertions.assertTrue(RuleSet.PUSH_DOWN_PREDICATE_RULES.predecessorRules().stream()
                .map(Rule::getClass)
                .map(Class::getSimpleName)
                .noneMatch(name -> name.startsWith("ExtractAiFunction")));
    }

    @Test
    public void testMandatoryLoweringIsIndependentOfPredicatePushdownRuleGroup() {
        for (String sql : List.of(
                "select ai_complete(ta) from tall",
                "select ta from tall where ai_complete(ta) = 'x'",
                "select * from (values (ai_complete('a'), 1), (ai_complete('b'), 2)) v",
                "select t0.v1 from t0 join t1 "
                        + "on t0.v1 = t1.v4 and ai_complete(cast(t0.v2 as varchar)) = cast(t1.v5 as varchar)",
                "select t0.v1 from t0 where exists (select 1 from t1 "
                        + "where ai_complete(cast(t1.v4 as varchar)) = 'x')",
                "select (select ai_complete(cast(t1.v4 as varchar)) from t1 limit 1) from t0",
                "with ai_cte as (select ai_complete(ta) as answer from tall) "
                        + "select answer from ai_cte union all select answer from ai_cte")) {
            RuleInput input = ruleInput(sql);
            OptimizerOptions options = OptimizerOptions.newRuleBaseOpt();
            options.disableRule(RuleType.GP_PUSH_DOWN_PREDICATE);
            input.optimizerContext.setOptimizerOptions(options);

            OptExpression root = OptimizerFactory.create(input.optimizerContext).optimize(
                    input.logicalPlan.getRoot(), new PhysicalPropertySet(),
                    new ColumnRefSet(input.logicalPlan.getOutputColumn()));

            Assertions.assertFalse(logicalAIProjects(root).isEmpty(), sql);
            assertAIOnlyInAIProject(root);
        }
    }

    @Test
    public void testMandatoryLoweringHandlesLargeInRewriteOutput() {
        int originalThreshold = connectContext.getSessionVariable().getLargeInPredicateThreshold();
        try {
            connectContext.getSessionVariable().setLargeInPredicateThreshold(3);
            OptExpression root = optimizeWithRuleBasedPlan(
                    "select * from tall where ai_complete(ta) in ('a', 'b', 'c', 'd')");

            Assertions.assertFalse(logicalAIProjects(root).isEmpty());
            assertAIOnlyInAIProject(root);
        } finally {
            connectContext.getSessionVariable().setLargeInPredicateThreshold(originalThreshold);
        }
    }

    @Test
    public void testMandatoryLoweringHandlesQuantifiedApplyBeforeJoinRewrite() {
        OptExpression root = optimizeWithRuleBasedPlan(
                "select * from tall where ai_complete(ta) in "
                        + "(select cast(v1 as varchar) from t0)");

        Assertions.assertFalse(logicalAIProjects(root).isEmpty());
        assertAIOnlyInAIProject(root);
    }

    @Test
    public void testRepeatedAICTEIsReusedWithoutDuplicatingCalls() {
        OptimizedPlan result = optimizeWithRuleBasedPlanAndContext(
                "with ai_cte as (select ai_complete(ta) as answer from tall) "
                        + "select answer from ai_cte union all select answer from ai_cte");

        assertRepeatedAICTEUsesSingleExecutableProducer(result);
        assertAIOnlyInAIProject(result.root);
    }

    @Test
    public void testRepeatedAICTEForcesReuseWhenCTEReuseIsDisabled() {
        boolean originalCteReuse = connectContext.getSessionVariable().isCboCteReuse();
        try {
            connectContext.getSessionVariable().setCboCteReuse(false);
            OptimizedPlan result = optimizeWithRuleBasedPlanAndContext(
                    "with ai_cte as (select ai_complete(ta) as answer from tall) "
                            + "select answer from ai_cte union all select answer from ai_cte");

            assertRepeatedAICTEUsesSingleExecutableProducer(result);
            assertAIOnlyInAIProject(result.root);
        } finally {
            connectContext.getSessionVariable().setCboCteReuse(originalCteReuse);
        }
    }

    @Test
    public void testNonAICTEStillHonorsDisabledReuse() {
        boolean originalCteReuse = connectContext.getSessionVariable().isCboCteReuse();
        try {
            connectContext.getSessionVariable().setCboCteReuse(false);
            OptExpression root = optimizeWithRuleBasedPlan(
                    "with cte as (select rand() as value from tall) "
                            + "select value from cte union all select value from cte");

            Assertions.assertFalse(containsOperator(root, OperatorType.LOGICAL_CTE_PRODUCE));
            Assertions.assertEquals(2, findOperators(root, OperatorType.LOGICAL_OLAP_SCAN).size());
        } finally {
            connectContext.getSessionVariable().setCboCteReuse(originalCteReuse);
        }
    }

    @Test
    public void testRepeatedAICTEForcesReuseWhenCTERatioRequestsInlining() {
        double originalCteReuseRatio = connectContext.getSessionVariable().getCboCTERuseRatio();
        try {
            connectContext.getSessionVariable().setCboCTERuseRatio(-1);
            OptimizedPlan result = optimizeWithRuleBasedPlanAndContext(
                    "with ai_cte as (select ai_complete(ta) as answer from tall) "
                            + "select answer from ai_cte union all select answer from ai_cte");

            assertRepeatedAICTEUsesSingleExecutableProducer(result);
            assertAIOnlyInAIProject(result.root);
        } finally {
            connectContext.getSessionVariable().setCboCTERuseRatio(originalCteReuseRatio);
        }
    }

    @Test
    public void testRepeatedQuantifiedAICTEForcesReuseBeforeApplyRewrite() {
        boolean originalCteReuse = connectContext.getSessionVariable().isCboCteReuse();
        try {
            connectContext.getSessionVariable().setCboCteReuse(false);
            OptimizedPlan result = optimizeWithRuleBasedPlanAndContext(
                    "with ai_cte as (select ta from tall where ai_complete(ta) in "
                            + "(select cast(v1 as varchar) from t0)) "
                            + "select ta from ai_cte union all select ta from ai_cte");

            assertRepeatedAICTEUsesSingleExecutableProducer(result);
            Assertions.assertFalse(containsOperator(result.root, OperatorType.LOGICAL_APPLY));
            assertAIOnlyInAIProject(result.root);
        } finally {
            connectContext.getSessionVariable().setCboCteReuse(originalCteReuse);
        }
    }

    @Test
    public void testSingleConsumerAICTEDoesNotCreateExtraCalls() {
        OptExpression root = optimizeWithRuleBasedPlan(
                "with ai_cte as (select ai_complete(ta) as answer from tall) "
                        + "select answer from ai_cte");

        Assertions.assertEquals(1, countAIEntries(root));
        Assertions.assertFalse(containsOperator(root, OperatorType.LOGICAL_CTE_PRODUCE));
        assertAIOnlyInAIProject(root);
    }

    @Test
    public void testRepeatedCTEWithQuantifiedApplyCompletesTwoStageLowering() {
        OptimizedPlan result = optimizeWithRuleBasedPlanAndContext(
                "with ai_cte as (select ta from tall where ai_complete(ta) in "
                        + "(select cast(v1 as varchar) from t0)) "
                        + "select ta from ai_cte union all select ta from ai_cte");

        assertRepeatedAICTEUsesSingleExecutableProducer(result);
        Assertions.assertFalse(containsOperator(result.root, OperatorType.LOGICAL_APPLY));
        assertAIOnlyInAIProject(result.root);
    }

    @Test
    public void testQuantifiedApplyLowersAIOnlyIntoLeftChild() {
        for (String predicateSql : List.of("in", "not in")) {
            RuleInput input = ruleInput("select * from tall where ai_complete(ta) " + predicateSql
                    + " (select cast(v1 as varchar) from t0)");
            OptExpression inputRoot = input.logicalPlan.getRoot();
            OptExpression originalApplyExpression =
                    findOperators(inputRoot, OperatorType.LOGICAL_APPLY).get(0);
            LogicalApplyOperator originalApply = originalApplyExpression.getOp().cast();
            InPredicateOperator originalPredicate = originalApply.getSubqueryOperator().cast();
            OptExpression originalRightChild = originalApplyExpression.inputAt(1);
            ScalarOperator originalRightOperand = originalPredicate.getChild(1);

            OptExpression root = lowerQuantifiedApplyLeft(inputRoot, input.optimizerContext);
            OptExpression applyExpression = findOperators(root, OperatorType.LOGICAL_APPLY).get(0);
            LogicalApplyOperator apply = applyExpression.getOp().cast();
            InPredicateOperator predicate = apply.getSubqueryOperator().cast();

            Assertions.assertEquals(predicateSql.equals("not in"), predicate.isNotIn());
            Assertions.assertEquals(originalPredicate.isSubquery(), predicate.isSubquery());
            Assertions.assertSame(originalRightChild, applyExpression.inputAt(1));
            Assertions.assertSame(originalRightOperand, predicate.getChild(1));
            assertApplyStatePreserved(originalApply, apply);
            Assertions.assertFalse(AiFunctionExtractor.containsAI(apply.getSubqueryOperator()));
            Assertions.assertTrue(containsOperator(applyExpression.inputAt(0), OperatorType.LOGICAL_AI_PROJECT));
            Assertions.assertFalse(containsOperator(applyExpression.inputAt(1), OperatorType.LOGICAL_AI_PROJECT));
            assertAIOnlyInAIProject(root);
        }
    }

    @Test
    public void testMultiInApplyLowersAIOnlyIntoLeftChild() {
        for (String predicateSql : List.of("in", "not in")) {
            RuleInput input = ruleInput("select * from tall where (ai_complete(ta), tb) " + predicateSql
                    + " (select cast(v1 as varchar), cast(v2 as smallint) from t0)");
            OptExpression inputRoot = input.logicalPlan.getRoot();
            OptExpression originalApplyExpression =
                    findOperators(inputRoot, OperatorType.LOGICAL_APPLY).get(0);
            LogicalApplyOperator originalApply = originalApplyExpression.getOp().cast();
            MultiInPredicateOperator originalPredicate = originalApply.getSubqueryOperator().cast();
            OptExpression originalRightChild = originalApplyExpression.inputAt(1);
            List<ScalarOperator> originalRightOperands = new ArrayList<>(
                    originalPredicate.getChildren().subList(
                            originalPredicate.getTupleSize(), originalPredicate.getChildren().size()));

            OptExpression root = lowerQuantifiedApplyLeft(inputRoot, input.optimizerContext);
            OptExpression applyExpression = findOperators(root, OperatorType.LOGICAL_APPLY).get(0);
            LogicalApplyOperator apply = applyExpression.getOp().cast();
            MultiInPredicateOperator predicate = apply.getSubqueryOperator().cast();

            Assertions.assertEquals(predicateSql.equals("not in"), predicate.isNotIn());
            Assertions.assertEquals(originalPredicate.getTupleSize(), predicate.getTupleSize());
            Assertions.assertSame(originalRightChild, applyExpression.inputAt(1));
            for (int index = 0; index < originalRightOperands.size(); index++) {
                Assertions.assertSame(originalRightOperands.get(index),
                        predicate.getChild(predicate.getTupleSize() + index));
            }
            assertApplyStatePreserved(originalApply, apply);
            Assertions.assertFalse(AiFunctionExtractor.containsAI(apply.getSubqueryOperator()));
            Assertions.assertTrue(containsOperator(applyExpression.inputAt(0), OperatorType.LOGICAL_AI_PROJECT));
            Assertions.assertFalse(containsOperator(applyExpression.inputAt(1), OperatorType.LOGICAL_AI_PROJECT));
            assertAIOnlyInAIProject(root);
        }
    }

    @Test
    public void testQuantifiedApplyRejectsAIInputsOutsideLeftChild() {
        RuleInput input = ruleInput("select * from tall where ai_complete(ta) in "
                + "(select cast(v1 as varchar) from t0)");
        OptExpression applyExpression =
                findOperators(input.logicalPlan.getRoot(), OperatorType.LOGICAL_APPLY).get(0);
        LogicalApplyOperator apply = applyExpression.getOp().cast();
        InPredicateOperator predicate = apply.getSubqueryOperator().cast();
        CallOperator aiCall = predicate.getChild(0).cast();
        List<ScalarOperator> invalidArguments = new ArrayList<>(aiCall.getChildren());
        invalidArguments.set(0, predicate.getChild(1));
        CallOperator invalidCall = new CallOperator(
                aiCall.getFnName(), aiCall.getType(), invalidArguments, aiCall.getFunction(),
                aiCall.isDistinct(), aiCall.isRemovedDistinct());
        InPredicateOperator invalidPredicate = new InPredicateOperator(
                predicate.isNotIn(), predicate.isSubquery(), invalidCall, predicate.getChild(1));
        OptExpression invalidApply = OptExpression.create(
                LogicalApplyOperator.builder().withOperator(apply)
                        .setSubqueryOperator(invalidPredicate).build(),
                applyExpression.getInputs());

        IllegalStateException exception = Assertions.assertThrows(IllegalStateException.class,
                () -> lowerQuantifiedApplyLeft(invalidApply, input.optimizerContext));
        Assertions.assertEquals(
                "AI function in quantified predicate must reference only the left Apply input",
                exception.getMessage());
    }

    @Test
    public void testLoweringInvalidatesPrecomputedCaches() {
        RuleInput input = ruleInput("select ai_complete(ta) from tall");
        OptExpression inputRoot = input.logicalPlan.getRoot();
        OptExpression scan = findOperators(inputRoot, OperatorType.LOGICAL_OLAP_SCAN).get(0);
        Statistics staleStatistics = Statistics.builder().setOutputRowCount(123).build();
        RowOutputInfo staleOutputInfo = scan.getRowOutputInfo();
        scan.setStatistics(staleStatistics);

        TaskContext taskContext = new TaskContext(input.optimizerContext, new PhysicalPropertySet(),
                new ColumnRefSet(), Double.MAX_VALUE);
        input.optimizerContext.setTaskContext(taskContext);
        AIFunctionLoweringRule rule = new AIFunctionLoweringRule();
        OptExpression root = rule.rewrite(inputRoot, taskContext);

        Assertions.assertTrue(rule.hasRewrite());
        Assertions.assertNotSame(staleStatistics, scan.getStatistics());
        Assertions.assertNotSame(staleOutputInfo, scan.getRowOutputInfo());
        assertAIOnlyInAIProject(root);

        Statistics preservedStatistics = Statistics.builder().setOutputRowCount(456).build();
        RowOutputInfo preservedOutputInfo = scan.getRowOutputInfo();
        LogicalProperty preservedLogicalProperty = scan.getLogicalProperty();
        scan.setStatistics(preservedStatistics);
        OptExpression unchanged = rule.rewrite(root, taskContext);

        Assertions.assertFalse(rule.hasRewrite());
        Assertions.assertSame(root, unchanged);
        Assertions.assertSame(preservedStatistics, scan.getStatistics());
        Assertions.assertSame(preservedOutputInfo, scan.getRowOutputInfo());
        Assertions.assertSame(preservedLogicalProperty, scan.getLogicalProperty());
    }

    @Test
    public void testQuantifiedApplyNoOpPreservesPrecomputedCaches() {
        RuleInput input = ruleInput("select * from tall where ta in "
                + "(select cast(v1 as varchar) from t0)");
        OptExpression root = input.logicalPlan.getRoot();
        deriveLogicalProperties(root);
        OptExpression scan = findOperators(root, OperatorType.LOGICAL_OLAP_SCAN).get(0);
        Statistics preservedStatistics = Statistics.builder().setOutputRowCount(456).build();
        RowOutputInfo preservedOutputInfo = scan.getRowOutputInfo();
        LogicalProperty preservedLogicalProperty = scan.getLogicalProperty();
        scan.setStatistics(preservedStatistics);
        TaskContext taskContext = new TaskContext(input.optimizerContext, new PhysicalPropertySet(),
                new ColumnRefSet(), Double.MAX_VALUE);
        input.optimizerContext.setTaskContext(taskContext);
        AIQuantifiedApplyLoweringRule rule = new AIQuantifiedApplyLoweringRule();

        OptExpression unchanged = rule.rewrite(root, taskContext);

        Assertions.assertFalse(rule.hasRewrite());
        Assertions.assertSame(root, unchanged);
        Assertions.assertSame(preservedStatistics, scan.getStatistics());
        Assertions.assertSame(preservedOutputInfo, scan.getRowOutputInfo());
        Assertions.assertSame(preservedLogicalProperty, scan.getLogicalProperty());
    }

    @Test
    public void testDirectLoweringIsIdempotent() {
        RuleInput input = ruleInput("select ai_complete(ai_complete(ta)), "
                + "ai_complete(concat(ta, 'x')), ai_complete(concat(ta, 'x')) from tall");
        TaskContext taskContext = new TaskContext(input.optimizerContext, new PhysicalPropertySet(),
                new ColumnRefSet(), Double.MAX_VALUE);
        input.optimizerContext.setTaskContext(taskContext);
        AIFunctionLoweringRule rule = new AIFunctionLoweringRule();
        OptExpression first = rule.rewrite(input.logicalPlan.getRoot(), taskContext);
        deriveLogicalProperties(first);
        Assertions.assertTrue(rule.hasRewrite());
        String firstShape = planShape(first);
        List<AIProjectSnapshot> firstProjects = snapshotAIProjects(first);
        int firstProjectCount = logicalAIProjects(first).size();
        int firstOccurrenceCount = countAIEntries(first);

        OptExpression second = rule.rewrite(first, taskContext);
        deriveLogicalProperties(second);

        Assertions.assertFalse(rule.hasRewrite());
        Assertions.assertSame(first, second);
        Assertions.assertEquals(firstShape, planShape(second));
        Assertions.assertEquals(firstProjects, snapshotAIProjects(second));
        Assertions.assertEquals(firstProjectCount, logicalAIProjects(second).size());
        Assertions.assertEquals(firstOccurrenceCount, countAIEntries(second));
        assertAIOnlyInAIProject(second);
    }

    @Test
    public void testOrderByAliasReusesOneOccurrence() {
        RewriteResult result = rewrite("select ai_complete(ta) as answer from tall order by answer");
        LogicalAIProjectOperator aiProject = onlyAIProject(result.root);
        ColumnRefOperator aiSlot = aiEntries(aiProject).get(0).getKey();
        LogicalTopNOperator topN = findOperators(result.root, OperatorType.LOGICAL_TOPN).get(0).getOp().cast();
        ColumnRefOperator orderBySlot = topN.getOrderByElements().get(0).getColumnRef();
        List<ScalarOperator> aliasTargets = findOperators(result.root, OperatorType.LOGICAL_PROJECT).stream()
                .map(expression -> (LogicalProjectOperator) expression.getOp())
                .map(LogicalProjectOperator::getColumnRefMap)
                .filter(map -> map.containsKey(orderBySlot))
                .map(map -> map.get(orderBySlot))
                .filter(target -> !target.equals(orderBySlot))
                .collect(Collectors.toList());

        Assertions.assertEquals(1, countAIEntries(result.root));
        Assertions.assertEquals(List.of(aiSlot), aliasTargets);
    }

    @Test
    public void testNestedCallsAreExtractedInnermostFirst() {
        RewriteResult result = rewrite("select ai_complete(ai_complete(ta)) from tall");
        List<LogicalAIProjectOperator> projects = logicalAIProjects(result.root);

        Assertions.assertEquals(2, projects.size());
        LogicalAIProjectOperator outer = projects.get(0);
        LogicalAIProjectOperator inner = projects.get(1);
        CallOperator outerCall = aiEntries(outer).get(0).getValue().cast();
        ColumnRefOperator innerOutput = aiEntries(inner).get(0).getKey();
        Assertions.assertEquals(innerOutput, outerCall.getChild(0));
    }

    @Test
    public void testIndependentCallsShareOneLayerAndOnlyDeterministicCommonSlots() {
        RewriteResult result = rewrite("select ai_complete(concat(ta, 'x')), "
                + "ai_complete(concat(ta, 'x')) from tall");
        LogicalAIProjectOperator project = onlyAIProject(result.root);

        Assertions.assertEquals(2, aiEntries(project).size());
        assertAIProjectMapInvariant(project);
        Assertions.assertFalse(project.getCommonSubOperatorMap().isEmpty());
    }

    @Test
    public void testTimeFunctionsAreNeverExtractedIntoAICommonSlots() {
        for (String timeFunction : List.of("now()", "unix_timestamp()")) {
            RewriteResult result = rewrite("select ai_complete(concat(ta, cast(" + timeFunction
                    + " as varchar))) as answer1, ai_complete(concat(ta, cast(" + timeFunction
                    + " as varchar))) as answer2 from tall");
            LogicalAIProjectOperator project = onlyAIProject(result.root);

            Assertions.assertEquals(2, aiEntries(project).size(), timeFunction);
            Assertions.assertTrue(project.getCommonSubOperatorMap().values().stream()
                            .noneMatch(expression -> containsCallNamed(expression,
                                    timeFunction.substring(0, timeFunction.length() - 2))),
                    timeFunction + ": " + project.getCommonSubOperatorMap());
        }
    }

    @Test
    public void testNestedMixedCaseTimeFunctionsAreClassifiedAsNonReusable() {
        for (String functionName : List.of("NoW", "UNIX_TIMESTAMP")) {
            CallOperator timeCall = new CallOperator(functionName, DateType.DATETIME, List.of());
            CallOperator wrapper = new CallOperator("concat", VarcharType.VARCHAR, List.of(timeCall));

            Assertions.assertTrue(AiFunctionExtractor.containsNonReusableExpression(wrapper), functionName);
        }
    }

    @Test
    public void testWhereAndHavingExtractBeforePredicateEvaluation() {
        for (String sql : List.of(
                "select ta from tall where ai_complete(ta) = 'x' and tb > 0",
                "select count(*) from tall having ai_complete('p') = 'x'")) {
            RewriteResult result = rewrite(sql);
            Assertions.assertEquals(1, logicalAIProjects(result.root).size(), sql);
            assertAIOnlyInAIProject(result.root);
            Assertions.assertEquals(new ColumnRefSet(result.logicalPlan.getOutputColumn()), result.root.getOutputColumns());
        }

        List<String> pushdownSql = List.of(
                "select ta from tall where ai_complete(ta) = 'x' and tb > 0",
                "select answer from (select ai_complete(ta) as answer, tb from tall) d where tb > 0");
        List<OptExpression> optimizedRoots = pushdownSql.stream()
                .map(this::optimizeWithRuleBasedPlan)
                .toList();
        for (int index = 0; index < pushdownSql.size(); index++) {
            String sql = pushdownSql.get(index);
            OptExpression root = optimizedRoots.get(index);
            OptExpression aiProjectExpression =
                    findOperators(root, OperatorType.LOGICAL_AI_PROJECT).get(0);
            LogicalAIProjectOperator aiProject = aiProjectExpression.getOp().cast();
            OptExpression scan =
                    findOperators(aiProjectExpression.inputAt(0), OperatorType.LOGICAL_OLAP_SCAN).get(0);
            ScalarOperator pushedPredicate = scan.getOp().getPredicate();
            ColumnRefSet aiOutputs = new ColumnRefSet(aiEntries(aiProject).stream()
                    .map(Map.Entry::getKey)
                    .toList());

            Assertions.assertNotNull(pushedPredicate, sql + ":\n" + root.debugString());
            Assertions.assertFalse(pushedPredicate.getUsedColumns().isIntersect(aiOutputs),
                    sql + ":\n" + root.debugString());
        }

        OptExpression directAIProject =
                findOperators(optimizedRoots.get(0), OperatorType.LOGICAL_AI_PROJECT).get(0);
        Assertions.assertTrue(findOperators(optimizedRoots.get(0), OperatorType.LOGICAL_FILTER).stream()
                        .anyMatch(filter -> filter.arity() == 1 && filter.inputAt(0) == directAIProject),
                optimizedRoots.get(0).debugString());
    }

    @Test
    public void testMultiRowValuesPreserveRowsAndOutputs() {
        RewriteResult result = rewrite("select * from (values "
                + "(ai_complete('a'), 1), (ai_complete('b'), 2)) v");

        Assertions.assertEquals(2, countAIEntries(result.root));
        Assertions.assertEquals(2, logicalAIProjects(result.root).size());
        List<LogicalAIProjectOperator> rowProjects = logicalAIProjects(result.root);
        Assertions.assertEquals("a", semanticStringArgument(aiEntries(rowProjects.get(0)).get(0).getValue().cast()));
        Assertions.assertEquals("b", semanticStringArgument(aiEntries(rowProjects.get(1)).get(0).getValue().cast()));
        Assertions.assertEquals(new ColumnRefSet(result.logicalPlan.getOutputColumn()), result.root.getOutputColumns());
        Assertions.assertTrue(containsOperator(result.root, OperatorType.LOGICAL_UNION));
        assertAIOnlyInAIProject(result.root);
    }

    @Test
    public void testValuesRulePreservesRowsPredicateProjectionAndLimit() {
        RuleInput input = ruleInput("select * from (values "
                + "(ai_complete('a'), 1), (ai_complete('b'), 2)) v");
        OptExpression valuesExpression = findOperators(input.logicalPlan.getRoot(), OperatorType.LOGICAL_VALUES).get(0);
        LogicalValuesOperator values = valuesExpression.getOp().cast();
        ColumnRefOperator projected = input.optimizerContext.getColumnRefFactory()
                .create("projected", values.getColumnRefSet().get(0).getType(), true);
        Projection projection = new Projection(Map.of(projected, values.getColumnRefSet().get(0)));
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.GT,
                values.getColumnRefSet().get(1), ConstantOperator.createInt(0));
        LogicalValuesOperator decorated = new LogicalValuesOperator.Builder()
                .withOperator(values)
                .setPredicate(predicate)
                .setProjection(projection)
                .setLimit(1)
                .build();

        OptExpression transformed = lowerExpression(OptExpression.create(decorated), input.optimizerContext);
        Assertions.assertInstanceOf(LogicalLimitOperator.class, transformed.getOp());
        Assertions.assertEquals(1, transformed.getOp().getLimit());
        transformed = transformed.inputAt(0);
        Assertions.assertInstanceOf(LogicalProjectOperator.class, transformed.getOp());
        Assertions.assertEquals(projection.getColumnRefMap(),
                ((LogicalProjectOperator) transformed.getOp()).getColumnRefMap());
        transformed = transformed.inputAt(0);
        Assertions.assertInstanceOf(LogicalFilterOperator.class, transformed.getOp());
        Assertions.assertEquals(predicate, transformed.getOp().getPredicate());
        transformed = transformed.inputAt(0);
        Assertions.assertInstanceOf(LogicalUnionOperator.class, transformed.getOp());

        for (int rowIndex = 0; rowIndex < 2; rowIndex++) {
            LogicalProjectOperator rowProject = transformed.inputAt(rowIndex).getOp().cast();
            Assertions.assertTrue(rowProject.getColumnRefMap().values().stream()
                    .noneMatch(AiFunctionExtractor::containsAI));
            OptExpression rowAIProjectExpression = transformed.inputAt(rowIndex).inputAt(0);
            LogicalAIProjectOperator rowAIProject = rowAIProjectExpression.getOp().cast();
            CallOperator call = aiEntries(rowAIProject).get(0).getValue().cast();
            Assertions.assertEquals(rowIndex == 0 ? "a" : "b", semanticStringArgument(call));
            LogicalValuesOperator placeholder = rowAIProjectExpression.inputAt(0).getOp().cast();
            Assertions.assertTrue(placeholder.getRows().get(0).stream()
                    .map(ConstantOperator.class::cast).allMatch(ConstantOperator::isNull));
        }
        assertAIOnlyInAIProject(transformed);
    }

    @Test
    public void testInnerJoinMovesOnlyAIResidualAboveJoin() {
        RewriteResult result = rewrite("select t0.v1 from t0 join t1 "
                + "on t0.v1 = t1.v4 and ai_complete(cast(t0.v2 as varchar)) = cast(t1.v5 as varchar)");
        LogicalJoinOperator join = findOperators(result.root, OperatorType.LOGICAL_JOIN).get(0).getOp().cast();

        Assertions.assertEquals(JoinOperator.INNER_JOIN, join.getJoinType());
        Assertions.assertFalse(AiFunctionExtractor.containsAI(join.getOnPredicate()));
        Assertions.assertNotNull(join.getOnPredicate());
        Assertions.assertEquals(1, logicalAIProjects(result.root).size());
        assertAIOnlyInAIProject(result.root);
    }

    @Test
    public void testPureAIInnerJoinOnBecomesCrossJoin() {
        RuleInput input = ruleInput("select t0.v1 from t0 join t1 "
                + "on ai_complete(cast(t0.v2 as varchar)) = cast(t1.v5 as varchar)");
        OptExpression joinExpression = findOperators(input.logicalPlan.getRoot(), OperatorType.LOGICAL_JOIN).get(0);
        deriveLogicalProperties(joinExpression);

        OptExpression transformed = lowerExpression(joinExpression, input.optimizerContext);
        LogicalJoinOperator rewritten = findOperators(transformed, OperatorType.LOGICAL_JOIN).get(0).getOp().cast();
        Assertions.assertEquals(JoinOperator.CROSS_JOIN, rewritten.getJoinType());
        Assertions.assertNull(rewritten.getOnPredicate());
        Assertions.assertEquals(1, logicalAIProjects(transformed).size());
    }

    @Test
    public void testRuleLevelCrossJoinWithAIOnIsExtracted() {
        RuleInput input = ruleInput("select t0.v1 from t0 join t1 "
                + "on ai_complete(cast(t0.v2 as varchar)) = cast(t1.v5 as varchar)");
        OptExpression original = findOperators(input.logicalPlan.getRoot(), OperatorType.LOGICAL_JOIN).get(0);
        LogicalJoinOperator crossJoin = LogicalJoinOperator.builder()
                .withOperator(original.getOp().cast())
                .setJoinType(JoinOperator.CROSS_JOIN)
                .build();
        OptExpression crossExpression = OptExpression.create(crossJoin, original.getInputs());
        deriveLogicalProperties(crossExpression);

        OptExpression transformed = lowerExpression(crossExpression, input.optimizerContext);
        LogicalJoinOperator rewritten = findOperators(transformed, OperatorType.LOGICAL_JOIN).get(0).getOp().cast();
        Assertions.assertEquals(JoinOperator.CROSS_JOIN, rewritten.getJoinType());
        Assertions.assertNull(rewritten.getOnPredicate());
        Assertions.assertEquals(1, logicalAIProjects(transformed).size());
    }

    @Test
    public void testCrossJoinResidualIsExtractedButUnsupportedJoinsAreDefensivelyRejected() {
        RewriteResult result = rewrite("select t0.v1 from t0 cross join t1 "
                + "where ai_complete(cast(t0.v2 as varchar)) = cast(t1.v5 as varchar)");
        Assertions.assertEquals(1, logicalAIProjects(result.root).size());

        ColumnRefFactory factory = new ColumnRefFactory();
        CallOperator aiCall = firstAICall(result.root);
        for (JoinOperator joinType : List.of(JoinOperator.LEFT_OUTER_JOIN, JoinOperator.RIGHT_OUTER_JOIN,
                JoinOperator.FULL_OUTER_JOIN, JoinOperator.LEFT_SEMI_JOIN, JoinOperator.LEFT_ANTI_JOIN,
                JoinOperator.RIGHT_SEMI_JOIN, JoinOperator.RIGHT_ANTI_JOIN,
                JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN)) {
            LogicalJoinOperator join = new LogicalJoinOperator(joinType, aiCall);
            OptExpression input = OptExpression.create(join,
                    OptExpression.create(new LogicalValuesOperator(List.of(), List.of())),
                    OptExpression.create(new LogicalValuesOperator(List.of(), List.of())));
            Assertions.assertThrows(SemanticException.class,
                    () -> lowerExpression(input, OptimizerFactory.mockContext(factory)), joinType.toString());
        }
    }

    @Test
    public void testUnsupportedJoinOnClausesFailDuringAnalysis() {
        for (String sql : List.of(
                "select t0.v1 from t0 left join t1 on ai_complete(cast(t0.v2 as varchar)) = cast(t1.v5 as varchar)",
                "select t0.v1 from t0 right join t1 on ai_complete(cast(t0.v2 as varchar)) = cast(t1.v5 as varchar)",
                "select t0.v1 from t0 full join t1 on ai_complete(cast(t0.v2 as varchar)) = cast(t1.v5 as varchar)",
                "select t0.v1 from t0 left semi join t1 "
                        + "on ai_complete(cast(t0.v2 as varchar)) = cast(t1.v5 as varchar)",
                "select t0.v1 from t0 left anti join t1 "
                        + "on ai_complete(cast(t0.v2 as varchar)) = cast(t1.v5 as varchar)",
                "select t1.v4 from t0 right semi join t1 "
                        + "on ai_complete(cast(t0.v2 as varchar)) = cast(t1.v5 as varchar)",
                "select t1.v4 from t0 right anti join t1 "
                        + "on ai_complete(cast(t0.v2 as varchar)) = cast(t1.v5 as varchar)")) {
            analyzeFail(sql, "AI functions are supported only for INNER/CROSS joins");
        }
    }

    @Test
    public void testImplementationCopiesBothMaps() {
        RewriteResult result = rewrite("select ai_complete(concat(ta, 'x')), "
                + "ai_complete(concat(ta, 'x')) from tall");
        LogicalAIProjectOperator logical = onlyAIProject(result.root);
        OptExpression logicalExpression = findOperators(result.root, OperatorType.LOGICAL_AI_PROJECT).get(0);

        OptExpression physicalExpression = new AIProjectImplementationRule()
                .transform(logicalExpression, result.optimizerContext).get(0);
        PhysicalAIProjectOperator physical = physicalExpression.getOp().cast();
        Assertions.assertEquals(logical.getColumnRefMap(), physical.getColumnRefMap());
        Assertions.assertEquals(logical.getCommonSubOperatorMap(), physical.getCommonSubOperatorMap());
        Assertions.assertEquals(logicalExpression.getInputs(), physicalExpression.getInputs());
    }

    @Test
    public void testAIProjectColumnPruningPreservesRequiredPassthroughAndInputs() {
        RewriteResult result = rewrite("select ta, ai_complete(ta), ai_complete(cast(tb as varchar)) from tall");
        OptExpression expression = findOperators(result.root, OperatorType.LOGICAL_AI_PROJECT).get(0);
        LogicalAIProjectOperator project = expression.getOp().cast();
        ColumnRefOperator passthrough = project.getColumnRefMap().entrySet().stream()
                .filter(entry -> entry.getKey().equals(entry.getValue()))
                .filter(entry -> entry.getKey().getName().equals("ta"))
                .map(Map.Entry::getKey).findFirst().orElseThrow();
        ColumnRefOperator keptAI = aiEntries(project).stream()
                .filter(entry -> entry.getValue().getUsedColumns().contains(passthrough))
                .map(Map.Entry::getKey).findFirst().orElseThrow();

        ColumnRefSet required = new ColumnRefSet(List.of(passthrough, keptAI));
        TaskContext taskContext = new TaskContext(result.optimizerContext, new PhysicalPropertySet(),
                required, Double.MAX_VALUE);
        result.optimizerContext.setTaskContext(taskContext);
        LogicalAIProjectOperator pruned = new PruneAIProjectColumnsRule()
                .transform(expression, result.optimizerContext).get(0).getOp().cast();

        Assertions.assertEquals(2, pruned.getColumnRefMap().size());
        Assertions.assertEquals(passthrough, pruned.getColumnRefMap().get(passthrough));
        Assertions.assertTrue(pruned.getColumnRefMap().containsKey(keptAI));
        Assertions.assertEquals(new ColumnRefSet(List.of(passthrough)), pruned.getRequiredChildInputColumns());
        Assertions.assertTrue(required.contains(passthrough));
    }

    @Test
    public void testAIProjectColumnPruningRemovesUnusedAIExecution() {
        RewriteResult result = rewrite("select ta, ai_complete(ta) from tall");
        OptExpression expression = findOperators(result.root, OperatorType.LOGICAL_AI_PROJECT).get(0);
        LogicalAIProjectOperator project = expression.getOp().cast();
        ColumnRefOperator passthrough = project.getColumnRefMap().entrySet().stream()
                .filter(entry -> entry.getKey().equals(entry.getValue()))
                .filter(entry -> entry.getKey().getName().equals("ta"))
                .map(Map.Entry::getKey).findFirst().orElseThrow();

        ColumnRefSet required = new ColumnRefSet(List.of(passthrough));
        TaskContext taskContext = new TaskContext(result.optimizerContext, new PhysicalPropertySet(),
                required, Double.MAX_VALUE);
        result.optimizerContext.setTaskContext(taskContext);
        OptExpression pruned = new PruneAIProjectColumnsRule()
                .transform(expression, result.optimizerContext).get(0);

        Assertions.assertInstanceOf(LogicalProjectOperator.class, pruned.getOp());
        LogicalProjectOperator ordinaryProject = pruned.getOp().cast();
        Assertions.assertEquals(Map.of(passthrough, passthrough), ordinaryProject.getColumnRefMap());
        Assertions.assertEquals(expression.getInputs(), pruned.getInputs());
    }

    @Test
    public void testSplitJoinOrToUnionTreatsAIProjectAsDuplicationBarrier() {
        RewriteResult result = rewrite("select a.answer, t1.v4 from "
                + "(select v1, v2, ai_complete(cast(v3 as varchar)) as answer from t0) a "
                + "join t1 on a.v1 = t1.v4 or a.v2 = t1.v5");
        OptExpression join = findOperators(result.root, OperatorType.LOGICAL_JOIN).get(0);
        Assertions.assertTrue(join.getInputs().stream()
                .anyMatch(child -> containsOperator(child, OperatorType.LOGICAL_AI_PROJECT)));

        SplitJoinORToUnionRule rule = SplitJoinORToUnionRule.getInstance();
        boolean originalSetting = result.optimizerContext.getSessionVariable().isEnabledRewriteOrToUnionAllJoin();
        result.optimizerContext.getSessionVariable().setEnabledRewriteOrToUnionAllJoin(true);
        try {
            OptExpression transformed = Assertions.assertDoesNotThrow(() -> {
                if (!rule.check(join, result.optimizerContext)) {
                    return join;
                }
                List<OptExpression> alternatives = rule.transform(join, result.optimizerContext);
                return alternatives.isEmpty() ? join : alternatives.get(0);
            });
            Assertions.assertSame(join, transformed);
            Assertions.assertFalse(rule.check(join, result.optimizerContext));
        } finally {
            result.optimizerContext.getSessionVariable().setEnabledRewriteOrToUnionAllJoin(originalSetting);
        }
    }

    @Test
    public void testSplitWindowSkewTreatsAIProjectAsDuplicationBarrier() {
        RewriteResult result = rewrite("select ta, row_number() over "
                + "([skew|ta(NULL)] partition by ta order by tb) "
                + "from tall where ai_complete(ta) = 'x'");
        OptExpression window = findOperators(result.root, OperatorType.LOGICAL_WINDOW).get(0);

        Assertions.assertTrue(containsOperator(window.inputAt(0), OperatorType.LOGICAL_AI_PROJECT));
        Assertions.assertFalse(SplitWindowSkewToUnionRule.getInstance()
                .check(window, result.optimizerContext));
    }

    @Test
    public void testFineGrainedRangePredicateTreatsAIProjectAsDuplicationBarrier() {
        RewriteResult result = rewrite("select count(*) from tall "
                + "where ti > '2021-01-02' and ti < '2021-06-17' and ai_complete(ta) = 'x'");
        OptExpression aggregate = findOperators(result.root, OperatorType.LOGICAL_AGGR).get(0);

        Assertions.assertTrue(containsOperator(aggregate, OperatorType.LOGICAL_AI_PROJECT));
        Assertions.assertFalse(FineGrainedRangePredicateRule.INSTANCE
                .check(aggregate, result.optimizerContext));
        Assertions.assertFalse(FineGrainedRangePredicateRule.PROJECTION_INSTANCE
                .check(aggregate, result.optimizerContext));
    }

    @Test
    public void testFineGrainedRangePredicateDoesNotDuplicateAIWhenPredicatePushdownIsDisabled() {
        boolean originalSetting = connectContext.getSessionVariable().isEnableFineGrainedRangePredicate();
        try {
            connectContext.getSessionVariable().setEnableFineGrainedRangePredicate(true);
            OptimizerOptions options = OptimizerOptions.newRuleBaseOpt();
            options.disableRule(RuleType.GP_PUSH_DOWN_PREDICATE);

            OptExpression root = optimizeWithRuleBasedPlanAndContext(
                    "select count(*) from tall "
                            + "where ti > '2021-01-02' and ti < '2021-06-17' and ai_complete(ta) = 'x'",
                    options).root;

            Assertions.assertEquals(1, countAIEntries(root), root::debugString);
            Assertions.assertFalse(containsOperator(root, OperatorType.LOGICAL_UNION), root::debugString);
            assertAIOnlyInAIProject(root);
        } finally {
            connectContext.getSessionVariable().setEnableFineGrainedRangePredicate(originalSetting);
        }
    }

    @Test
    public void testFineGrainedRangePredicateStillRewritesDeterministicPredicate() {
        boolean originalSetting = connectContext.getSessionVariable().isEnableFineGrainedRangePredicate();
        try {
            connectContext.getSessionVariable().setEnableFineGrainedRangePredicate(true);
            OptimizerOptions options = OptimizerOptions.newRuleBaseOpt();
            options.disableRule(RuleType.GP_PUSH_DOWN_PREDICATE);

            OptExpression root = optimizeWithRuleBasedPlanAndContext(
                    "select count(*) from tall "
                            + "where ti > '2021-01-02' and ti < '2021-06-17' and ta = 'x'",
                    options).root;

            Assertions.assertTrue(containsOperator(root, OperatorType.LOGICAL_UNION), root::debugString);
        } finally {
            connectContext.getSessionVariable().setEnableFineGrainedRangePredicate(originalSetting);
        }
    }

    private RewriteResult rewrite(String sql) {
        RuleInput input = ruleInput(sql);
        LogicalPlan logicalPlan = input.logicalPlan;
        OptimizerContext optimizerContext = input.optimizerContext;
        OptExpression anchor = OptExpression.create(new LogicalTreeAnchorOperator(), logicalPlan.getRoot());
        OptExpression root = lowerExpression(anchor, optimizerContext).inputAt(0);
        return new RewriteResult(root, logicalPlan, optimizerContext);
    }

    private OptExpression optimizeWithRuleBasedPlan(String sql) {
        return optimizeWithRuleBasedPlanAndContext(sql).root;
    }

    private OptimizedPlan optimizeWithRuleBasedPlanAndContext(String sql) {
        return optimizeWithRuleBasedPlanAndContext(sql, OptimizerOptions.newRuleBaseOpt());
    }

    private OptimizedPlan optimizeWithRuleBasedPlanAndContext(String sql, OptimizerOptions options) {
        RuleInput input = ruleInput(sql);
        input.optimizerContext.setOptimizerOptions(options);
        OptExpression root = OptimizerFactory.create(input.optimizerContext).optimize(
                input.logicalPlan.getRoot(), new PhysicalPropertySet(),
                new ColumnRefSet(input.logicalPlan.getOutputColumn()));
        return new OptimizedPlan(root, input.optimizerContext);
    }

    private OptExpression lowerExpression(OptExpression root, OptimizerContext optimizerContext) {
        deriveLogicalProperties(root);
        TaskContext taskContext = new TaskContext(optimizerContext, new PhysicalPropertySet(),
                new ColumnRefSet(), Double.MAX_VALUE);
        optimizerContext.setTaskContext(taskContext);
        OptExpression lowered = new AIFunctionLoweringRule().rewrite(root, taskContext);
        deriveLogicalProperties(lowered);
        return lowered;
    }

    private OptExpression lowerQuantifiedApplyLeft(OptExpression root, OptimizerContext optimizerContext) {
        deriveLogicalProperties(root);
        TaskContext taskContext = new TaskContext(optimizerContext, new PhysicalPropertySet(),
                new ColumnRefSet(), Double.MAX_VALUE);
        optimizerContext.setTaskContext(taskContext);
        OptExpression lowered = new AIQuantifiedApplyLoweringRule().rewrite(root, taskContext);
        deriveLogicalProperties(lowered);
        return lowered;
    }

    private static void assertApplyStatePreserved(LogicalApplyOperator expected, LogicalApplyOperator actual) {
        Assertions.assertSame(expected.getOutput(), actual.getOutput());
        Assertions.assertSame(expected.getCorrelationColumnRefs(), actual.getCorrelationColumnRefs());
        Assertions.assertSame(expected.getCorrelationConjuncts(), actual.getCorrelationConjuncts());
        Assertions.assertEquals(expected.isNeedCheckMaxRows(), actual.isNeedCheckMaxRows());
        Assertions.assertEquals(expected.isUseSemiAnti(), actual.isUseSemiAnti());
        Assertions.assertSame(expected.getUnCorrelationSubqueryPredicateColumns(),
                actual.getUnCorrelationSubqueryPredicateColumns());
        Assertions.assertSame(expected.getPredicate(), actual.getPredicate());
        Assertions.assertSame(expected.getProjection(), actual.getProjection());
        Assertions.assertEquals(expected.getLimit(), actual.getLimit());
    }

    private RuleInput ruleInput(String sql) {
        QueryStatement statement = (QueryStatement) analyzeSuccess(sql);
        ColumnRefFactory factory = new ColumnRefFactory();
        LogicalPlan logicalPlan = UtFrameUtils.getQueryLogicalPlan(connectContext, factory, statement);
        OptimizerContext optimizerContext = OptimizerFactory.mockContext(connectContext, factory);
        deriveLogicalProperties(logicalPlan.getRoot());
        return new RuleInput(logicalPlan, optimizerContext);
    }

    private void deriveLogicalProperties(OptExpression expression) {
        expression.getInputs().forEach(this::deriveLogicalProperties);
        expression.deriveLogicalPropertyItself();
    }

    private static List<LogicalAIProjectOperator> logicalAIProjects(OptExpression root) {
        return findOperators(root, OperatorType.LOGICAL_AI_PROJECT).stream()
                .map(expression -> (LogicalAIProjectOperator) expression.getOp())
                .collect(Collectors.toList());
    }

    private static LogicalAIProjectOperator onlyAIProject(OptExpression root) {
        List<LogicalAIProjectOperator> projects = logicalAIProjects(root);
        Assertions.assertEquals(1, projects.size());
        return projects.get(0);
    }

    private static int countAIEntries(OptExpression root) {
        return logicalAIProjects(root).stream().mapToInt(project -> aiEntries(project).size()).sum();
    }

    private static void assertRepeatedAICTEUsesSingleExecutableProducer(OptimizedPlan result) {
        List<OptExpression> producers = findOperators(result.root, OperatorType.LOGICAL_CTE_PRODUCE);
        List<OptExpression> consumers = findOperators(result.root, OperatorType.LOGICAL_CTE_CONSUME);
        Assertions.assertEquals(1, producers.size());
        Assertions.assertEquals(2, consumers.size());

        LogicalCTEProduceOperator producer = producers.get(0).getOp().cast();
        Assertions.assertTrue(result.optimizerContext.getCteContext().isForceCTE(producer.getCteId()));
        Assertions.assertEquals(1, countAIEntries(producers.get(0)));

        // A logical consume retains its child plan as an inline alternative. Once AI marks the CTE
        // non-deterministic, that alternative is not implementable and the reuse implementation
        // drops it, so only the canonical producer executes the AI call.
        CTEConsumeInlineImplementationRule inlineRule = new CTEConsumeInlineImplementationRule();
        CTEConsumerReuseImplementationRule reuseRule = new CTEConsumerReuseImplementationRule();
        for (OptExpression consumer : consumers) {
            LogicalCTEConsumeOperator consumeOperator = consumer.getOp().cast();
            Assertions.assertEquals(producer.getCteId(), consumeOperator.getCteId());
            Assertions.assertFalse(inlineRule.check(consumer, result.optimizerContext));
            OptExpression physicalConsume = reuseRule.transform(consumer, result.optimizerContext).get(0);
            Assertions.assertEquals(OperatorType.PHYSICAL_CTE_CONSUME, physicalConsume.getOp().getOpType());
            Assertions.assertEquals(0, physicalConsume.arity());
        }
    }

    private static List<AIProjectSnapshot> snapshotAIProjects(OptExpression root) {
        return logicalAIProjects(root).stream()
                .map(project -> new AIProjectSnapshot(
                        new LinkedHashMap<>(project.getColumnRefMap()),
                        new LinkedHashMap<>(project.getCommonSubOperatorMap()),
                        aiEntries(project).stream()
                                .map(entry -> hiddenOccurrenceId(entry.getValue().cast()))
                                .collect(Collectors.toList())))
                .collect(Collectors.toList());
    }

    private static String planShape(OptExpression root) {
        StringBuilder builder = new StringBuilder();
        appendPlanShape(root, builder);
        return builder.toString();
    }

    private static void appendPlanShape(OptExpression root, StringBuilder builder) {
        builder.append(root.getOp().getOpType()).append('(');
        root.getInputs().forEach(child -> appendPlanShape(child, builder));
        builder.append(')');
    }

    private static List<Map.Entry<ColumnRefOperator, ScalarOperator>> aiEntries(
            LogicalAIProjectOperator project) {
        return project.getColumnRefMap().entrySet().stream()
                .filter(entry -> AiFunctionExtractor.isAICall(entry.getValue()))
                .collect(Collectors.toList());
    }

    private static int hiddenOccurrenceId(CallOperator call) {
        int semanticArity = call.getFunction().getNumArgs();
        Assertions.assertTrue(call.getChildren().size() > semanticArity);
        return ((ConstantOperator) call.getChild(semanticArity)).getInt();
    }

    private static String semanticStringArgument(CallOperator call) {
        Assertions.assertTrue(call.getFunction().getNumArgs() > 0);
        return ((ConstantOperator) call.getChild(0)).getVarchar();
    }

    private static void assertAIProjectMapInvariant(LogicalAIProjectOperator project) {
        project.getColumnRefMap().forEach((output, expression) ->
                Assertions.assertTrue(output.equals(expression) || AiFunctionExtractor.isAICall(expression),
                        output + " := " + expression));
        project.getCommonSubOperatorMap().values().forEach(expression -> {
            Assertions.assertFalse(AiFunctionExtractor.containsAI(expression), expression.toString());
            Assertions.assertFalse(AiFunctionExtractor.containsNonReusableExpression(expression),
                    expression.toString());
        });
    }

    private static CallOperator firstAICall(OptExpression root) {
        return aiEntries(logicalAIProjects(root).get(0)).get(0).getValue().cast();
    }

    private static boolean containsCallNamed(ScalarOperator expression, String functionName) {
        if (expression instanceof CallOperator call && call.getFnName().equalsIgnoreCase(functionName)) {
            return true;
        }
        return expression.getChildren().stream().anyMatch(child -> containsCallNamed(child, functionName));
    }

    private static void assertAIOnlyInAIProject(OptExpression root) {
        walk(root, expression -> {
            if (expression.getOp() instanceof LogicalAIProjectOperator) {
                return;
            }
            Operator operator = expression.getOp();
            Assertions.assertFalse(AiFunctionExtractor.containsAI(operator.getPredicate()), operator.toString());
            if (operator instanceof LogicalProjectOperator project) {
                project.getColumnRefMap().values().forEach(value ->
                        Assertions.assertFalse(AiFunctionExtractor.containsAI(value), value.toString()));
            }
            if (operator.getProjection() != null) {
                operator.getProjection().getColumnRefMap().values().forEach(value ->
                        Assertions.assertFalse(AiFunctionExtractor.containsAI(value), value.toString()));
                operator.getProjection().getCommonSubOperatorMap().values().forEach(value ->
                        Assertions.assertFalse(AiFunctionExtractor.containsAI(value), value.toString()));
            }
            if (operator instanceof LogicalJoinOperator join) {
                Assertions.assertFalse(AiFunctionExtractor.containsAI(join.getOnPredicate()), join.toString());
            }
            if (operator instanceof LogicalApplyOperator apply) {
                Assertions.assertFalse(AiFunctionExtractor.containsAI(apply.getSubqueryOperator()), apply.toString());
                Assertions.assertFalse(AiFunctionExtractor.containsAI(apply.getCorrelationConjuncts()),
                        apply.toString());
            }
            if (operator instanceof LogicalValuesOperator values) {
                values.getRows().forEach(row -> row.forEach(value ->
                        Assertions.assertFalse(AiFunctionExtractor.containsAI(value), value.toString())));
            }
        });
    }

    private static boolean containsOperator(OptExpression root, OperatorType type) {
        return !findOperators(root, type).isEmpty();
    }

    private static List<OptExpression> findOperators(OptExpression root, OperatorType type) {
        List<OptExpression> result = new ArrayList<>();
        walk(root, expression -> {
            if (expression.getOp().getOpType() == type) {
                result.add(expression);
            }
        });
        return result;
    }

    private static void walk(OptExpression root, java.util.function.Consumer<OptExpression> consumer) {
        consumer.accept(root);
        root.getInputs().forEach(child -> walk(child, consumer));
    }

    private record RewriteResult(OptExpression root, LogicalPlan logicalPlan, OptimizerContext optimizerContext) {
    }

    private record RuleInput(LogicalPlan logicalPlan, OptimizerContext optimizerContext) {
    }

    private record OptimizedPlan(OptExpression root, OptimizerContext optimizerContext) {
    }

    private record AIProjectSnapshot(Map<ColumnRefOperator, ScalarOperator> slots,
                                     Map<ColumnRefOperator, ScalarOperator> commonSlots,
                                     List<Integer> occurrenceIds) {
    }
}
