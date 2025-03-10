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

package com.starrocks.sql.optimizer;

import com.google.common.collect.Lists;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.logical.LogicalTreeAnchorOperator;
import com.starrocks.sql.optimizer.rule.RuleSet;
import com.starrocks.sql.optimizer.rule.join.JoinReorderFactory;
import com.starrocks.sql.optimizer.rule.join.ReorderJoinRule;
import com.starrocks.sql.optimizer.rule.transformation.ApplyExceptionRule;
import com.starrocks.sql.optimizer.rule.transformation.ConvertToEqualForNullRule;
import com.starrocks.sql.optimizer.rule.transformation.EliminateAggRule;
import com.starrocks.sql.optimizer.rule.transformation.EliminateConstantCTERule;
import com.starrocks.sql.optimizer.rule.transformation.ForceCTEReuseRule;
import com.starrocks.sql.optimizer.rule.transformation.JoinLeftAsscomRule;
import com.starrocks.sql.optimizer.rule.transformation.MergeProjectWithChildRule;
import com.starrocks.sql.optimizer.rule.transformation.MergeTwoAggRule;
import com.starrocks.sql.optimizer.rule.transformation.MergeTwoProjectRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneEmptyWindowRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownJoinOnExpressionToChildProject;
import com.starrocks.sql.optimizer.rule.transformation.PushDownProjectLimitRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownTopNBelowOuterJoinRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownTopNBelowUnionRule;
import com.starrocks.sql.optimizer.rule.transformation.PushLimitAndFilterToCTEProduceRule;
import com.starrocks.sql.optimizer.rule.transformation.RewriteMultiDistinctRule;
import com.starrocks.sql.optimizer.rule.transformation.SeparateProjectRule;
import com.starrocks.sql.optimizer.rule.transformation.UnionToValuesRule;
import com.starrocks.sql.optimizer.rule.transformation.pruner.CboTablePruneRule;
import com.starrocks.sql.optimizer.rule.tree.PushDownAggregateRule;
import com.starrocks.sql.optimizer.rule.tree.PushDownDistinctAggregateRule;
import com.starrocks.sql.optimizer.task.OptimizeGroupTask;
import com.starrocks.sql.optimizer.task.TaskContext;
import com.starrocks.sql.optimizer.task.TaskScheduler;

import java.util.List;

public class SPMOptimizer extends Optimizer {
    private final TaskScheduler scheduler = new TaskScheduler();
    private final Memo memo = new Memo();
    private ColumnRefSet requiredColumns;

    SPMOptimizer(OptimizerContext context) {
        super(context);
    }

    @Override
    public OptExpression optimize(OptExpression tree, PhysicalPropertySet requiredProperty,
                                  ColumnRefSet requiredColumns) {

        context.setMemo(memo);
        context.setTaskScheduler(scheduler);
        this.requiredColumns = requiredColumns;

        TaskContext taskContext = new TaskContext(context, requiredProperty, requiredColumns.clone(), Double.MAX_VALUE);
        tree = optimizeByRule(tree, taskContext);

        memo.init(tree);
        memo.deriveAllGroupLogicalProperty();
        memoOptimize(memo, taskContext);

        return extractBestPlan(requiredProperty, memo.getRootGroup());
    }

    /*
    Remove:
    isEnableFineGrainedRangePredicate
    isEnableRewriteGroupingsetsToUnionAll
    isCboPushDownGroupingSet
    isEnableStatsToOptimizeSkewJoin
    MVRewrite
    PruneTable
    PRUNE_UKFK_JOIN_RULES
     */
    private OptExpression optimizeByRule(OptExpression tree,
                                         TaskContext rootTaskContext) {
        tree = OptExpression.create(new LogicalTreeAnchorOperator(), tree);
        deriveLogicalProperty(tree);

        CTEContext cteContext = context.getCteContext();
        CTEUtils.collectCteOperators(tree, context);

        // inline CTE if consume use once
        while (cteContext.hasInlineCTE()) {
            scheduler.rewriteOnce(tree, rootTaskContext, RuleSet.INLINE_CTE_RULES);
            CTEUtils.collectCteOperators(tree, context);
        }

        scheduler.rewriteIterative(tree, rootTaskContext, new EliminateConstantCTERule());
        CTEUtils.collectCteOperators(tree, context);

        scheduler.rewriteIterative(tree, rootTaskContext, RuleSet.AGGREGATE_REWRITE_RULES);
        scheduler.rewriteIterative(tree, rootTaskContext, RuleSet.PUSH_DOWN_SUBQUERY_RULES);
        scheduler.rewriteIterative(tree, rootTaskContext, RuleSet.SUBQUERY_REWRITE_COMMON_RULES);
        scheduler.rewriteIterative(tree, rootTaskContext, RuleSet.SUBQUERY_REWRITE_TO_WINDOW_RULES);
        scheduler.rewriteIterative(tree, rootTaskContext, RuleSet.SUBQUERY_REWRITE_TO_JOIN_RULES);
        scheduler.rewriteOnce(tree, rootTaskContext, new ApplyExceptionRule());
        CTEUtils.collectCteOperators(tree, context);

        // Note: PUSH_DOWN_PREDICATE tasks should be executed before MERGE_LIMIT tasks
        // because of the Filter node needs to be merged first to avoid the Limit node
        // cannot merge
        scheduler.rewriteIterative(tree, rootTaskContext, RuleSet.PUSH_DOWN_PREDICATE_RULES);

        scheduler.rewriteIterative(tree, rootTaskContext, new MergeTwoProjectRule());
        scheduler.rewriteOnce(tree, rootTaskContext, RuleSet.ELIMINATE_OP_WITH_CONSTANT_RULES);

        scheduler.rewriteOnce(tree, rootTaskContext, new ConvertToEqualForNullRule());
        scheduler.rewriteOnce(tree, rootTaskContext, RuleSet.PRUNE_COLUMNS_RULES);
        // Put EliminateAggRule after PRUNE_COLUMNS to give a chance to prune group bys before eliminate aggregations.
        scheduler.rewriteOnce(tree, rootTaskContext, EliminateAggRule.getInstance());
        deriveLogicalProperty(tree);

        scheduler.rewriteOnce(tree, rootTaskContext, new PushDownJoinOnExpressionToChildProject());

        scheduler.rewriteIterative(tree, rootTaskContext, new PruneEmptyWindowRule());
        // @todo: resolve recursive optimization question:
        //  MergeAgg -> PruneColumn -> PruneEmptyWindow -> MergeAgg/Project -> PruneColumn...
        scheduler.rewriteIterative(tree, rootTaskContext, new MergeTwoAggRule());

        rootTaskContext.setRequiredColumns(requiredColumns.clone());
        scheduler.rewriteOnce(tree, rootTaskContext, RuleSet.PRUNE_COLUMNS_RULES);

        scheduler.rewriteIterative(tree, rootTaskContext, new PruneEmptyWindowRule());
        scheduler.rewriteIterative(tree, rootTaskContext, new MergeTwoProjectRule());

        // Limit push must be after the column prune,
        // otherwise the Node containing limit may be prune
        scheduler.rewriteIterative(tree, rootTaskContext, RuleSet.MERGE_LIMIT_RULES);
        scheduler.rewriteIterative(tree, rootTaskContext, new PushDownProjectLimitRule());

        scheduler.rewriteIterative(tree, rootTaskContext, RuleSet.PRUNE_ASSERT_ROW_RULES);
        scheduler.rewriteIterative(tree, rootTaskContext, RuleSet.PRUNE_PROJECT_RULES);

        CTEUtils.collectCteOperators(tree, context);
        if (cteContext.needOptimizeCTE()) {
            cteContext.reset();
            scheduler.rewriteOnce(tree, rootTaskContext, RuleSet.COLLECT_CTE_RULES);
            rootTaskContext.setRequiredColumns(requiredColumns.clone());
            scheduler.rewriteOnce(tree, rootTaskContext, RuleSet.PRUNE_COLUMNS_RULES);
            if (cteContext.needPushLimit() || cteContext.needPushPredicate()) {
                scheduler.rewriteOnce(tree, rootTaskContext, new PushLimitAndFilterToCTEProduceRule());
            }

            if (cteContext.needPushPredicate()) {
                scheduler.rewriteOnce(tree, rootTaskContext, RuleSet.PUSH_DOWN_PREDICATE_RULES);
            }

            if (cteContext.needPushLimit()) {
                scheduler.rewriteOnce(tree, rootTaskContext, RuleSet.MERGE_LIMIT_RULES);
            }

            scheduler.rewriteOnce(tree, rootTaskContext, new ForceCTEReuseRule());
        }

        scheduler.rewriteOnce(tree, rootTaskContext, RuleSet.PARTITION_PRUNE_RULES);
        scheduler.rewriteIterative(tree, rootTaskContext, new RewriteMultiDistinctRule());
        scheduler.rewriteIterative(tree, rootTaskContext, RuleSet.PUSH_DOWN_PREDICATE_RULES);
        scheduler.rewriteIterative(tree, rootTaskContext, RuleSet.PRUNE_PROJECT_RULES);

        tree = pushDownAggregation(tree, rootTaskContext, requiredColumns);
        scheduler.rewriteOnce(tree, rootTaskContext, RuleSet.MERGE_LIMIT_RULES);

        CTEUtils.collectCteOperators(tree, context);
        // inline CTE if consume use once
        while (cteContext.hasInlineCTE()) {
            scheduler.rewriteOnce(tree, rootTaskContext, RuleSet.INLINE_CTE_RULES);
            CTEUtils.collectCteOperators(tree, context);
        }

        scheduler.rewriteIterative(tree, rootTaskContext, new MergeTwoProjectRule());

        // After this rule, we shouldn't generate logical project operator
        scheduler.rewriteIterative(tree, rootTaskContext, new MergeProjectWithChildRule());

        scheduler.rewriteOnce(tree, rootTaskContext, new PushDownTopNBelowOuterJoinRule());
        // intersect rewrite depend on statistics
        Utils.calculateStatistics(tree, rootTaskContext.getOptimizerContext());
        scheduler.rewriteOnce(tree, rootTaskContext, RuleSet.INTERSECT_REWRITE_RULES);
        scheduler.rewriteOnce(tree, rootTaskContext, new PushDownTopNBelowUnionRule());

        scheduler.rewriteOnce(tree, rootTaskContext, UnionToValuesRule.getInstance());
        deriveLogicalProperty(tree);

        tree.getInputs().get(0).clearStatsAndInitOutputInfo();
        return tree.getInputs().get(0);
    }

    private OptExpression pushDownAggregation(OptExpression tree, TaskContext rootTaskContext,
                                              ColumnRefSet requiredColumns) {
        boolean pushDistinctFlag = false;
        boolean pushAggFlag = false;
        if (context.getSessionVariable().isCboPushDownDistinctBelowWindow()) {
            // TODO(by satanson): in future, PushDownDistinctAggregateRule and PushDownAggregateRule should be
            //  fused one rule to tackle with all scenarios of agg push-down.
            PushDownDistinctAggregateRule rule = new PushDownDistinctAggregateRule(rootTaskContext);
            tree = rule.rewrite(tree, rootTaskContext);
            pushDistinctFlag = rule.getRewriter().hasRewrite();
        }

        if (context.getSessionVariable().getCboPushDownAggregateMode() != -1) {
            if (context.getSessionVariable().isCboPushDownAggregateOnBroadcastJoin()) {
                // Reorder joins before applying PushDownAggregateRule to better decide where to push down aggregator.
                // For example, do not push down a not very efficient aggregator below a very small broadcast join.
                scheduler.rewriteOnce(tree, rootTaskContext, RuleSet.PARTITION_PRUNE_RULES);
                scheduler.rewriteIterative(tree, rootTaskContext, new MergeTwoProjectRule());
                scheduler.rewriteIterative(tree, rootTaskContext, new MergeProjectWithChildRule());
                CTEUtils.collectForceCteStatisticsOutsideMemo(tree, context);
                deriveLogicalProperty(tree);
                tree = new ReorderJoinRule().rewrite(tree, JoinReorderFactory.createJoinReorderAdaptive(), context);
                tree = new SeparateProjectRule().rewrite(tree, rootTaskContext);
                deriveLogicalProperty(tree);
                Utils.calculateStatistics(tree, context);
            }

            PushDownAggregateRule rule = new PushDownAggregateRule(rootTaskContext);
            rule.getRewriter().collectRewriteContext(tree);
            if (rule.getRewriter().isNeedRewrite()) {
                pushAggFlag = true;
                tree = rule.rewrite(tree, rootTaskContext);
            }
        }

        if (pushDistinctFlag || pushAggFlag) {
            deriveLogicalProperty(tree);
            rootTaskContext.setRequiredColumns(requiredColumns.clone());
            scheduler.rewriteOnce(tree, rootTaskContext, RuleSet.PRUNE_COLUMNS_RULES);
            scheduler.rewriteOnce(tree, rootTaskContext, EliminateAggRule.getInstance());
        }

        return tree;
    }

    private void memoOptimize(Memo memo, TaskContext rootTaskContext) {
        context.setInMemoPhase(true);
        OptExpression tree = memo.getRootGroup().extractLogicalTree();
        SessionVariable sessionVariable = context.getSessionVariable();
        // add CboTablePruneRule
        if (Utils.countJoinNodeSize(tree, CboTablePruneRule.JOIN_TYPES) < 10 &&
                sessionVariable.isEnableCboTablePrune()) {
            context.getRuleSet().addCboTablePruneRule();
        }
        // Join reorder
        int innerCrossJoinNode = Utils.countJoinNodeSize(tree, JoinOperator.innerCrossJoinSet());
        if (!sessionVariable.isDisableJoinReorder() && innerCrossJoinNode < sessionVariable.getCboMaxReorderNode()) {
            if (innerCrossJoinNode > sessionVariable.getCboMaxReorderNodeUseExhaustive()) {
                CTEUtils.collectForceCteStatistics(memo, context);

                OptimizerTraceUtil.logOptExpression("before ReorderJoinRule:\n%s", tree);
                new ReorderJoinRule().transform(tree, context);
                OptimizerTraceUtil.logOptExpression("after ReorderJoinRule:\n%s", tree);

                context.getRuleSet().addJoinCommutativityWithoutInnerRule();
            } else {
                if (Utils.countJoinNodeSize(tree, JoinOperator.semiAntiJoinSet()) <
                        sessionVariable.getCboMaxReorderNodeUseExhaustive()) {
                    context.getRuleSet().getTransformRules().add(JoinLeftAsscomRule.INNER_JOIN_LEFT_ASSCOM_RULE);
                }
                context.getRuleSet().addJoinTransformationRules();
            }
        }

        if (!sessionVariable.isDisableJoinReorder() && sessionVariable.isEnableOuterJoinReorder()
                && Utils.capableOuterReorder(tree, sessionVariable.getCboReorderThresholdUseExhaustive())) {
            context.getRuleSet().addOuterJoinTransformationRules();
        }

        // add join implementRule
        String joinImplementationMode = context.getSessionVariable().getJoinImplementationMode();
        if ("merge".equalsIgnoreCase(joinImplementationMode)) {
            context.getRuleSet().addMergeJoinImplementationRule();
        } else if ("hash".equalsIgnoreCase(joinImplementationMode)) {
            context.getRuleSet().addHashJoinImplementationRule();
        } else if ("nestloop".equalsIgnoreCase(joinImplementationMode)) {
            context.getRuleSet().addNestLoopJoinImplementationRule();
        } else {
            context.getRuleSet().addAutoJoinImplementationRule();
        }

        scheduler.pushTask(new OptimizeGroupTask(rootTaskContext, memo.getRootGroup()));
        scheduler.executeTasks(rootTaskContext);
    }

    private OptExpression extractBestPlan(PhysicalPropertySet requiredProperty,
                                          Group rootGroup) {
        GroupExpression groupExpression = rootGroup.getBestExpression(requiredProperty);
        if (groupExpression == null) {
            String msg = "no executable plan for this sql. group: %s. required property: %s";
            throw new IllegalArgumentException(String.format(msg, rootGroup, requiredProperty));
        }
        List<PhysicalPropertySet> inputProperties = groupExpression.getInputProperties(requiredProperty);

        List<OptExpression> childPlans = Lists.newArrayList();
        for (int i = 0; i < groupExpression.arity(); ++i) {
            OptExpression childPlan = extractBestPlan(inputProperties.get(i), groupExpression.inputAt(i));
            childPlans.add(childPlan);
        }

        OptExpression expression = OptExpression.create(groupExpression.getOp(), childPlans);
        expression.setCost(groupExpression.getCost(requiredProperty));
        expression.setRequiredProperties(inputProperties);
        return expression;
    }
}
