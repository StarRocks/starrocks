// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.optimizer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.Explain;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.cost.CostEstimate;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.rewrite.AddDecodeNodeForDictStringRule;
import com.starrocks.sql.optimizer.rewrite.ExchangeSortToMergeRule;
import com.starrocks.sql.optimizer.rewrite.PredicateReorderRule;
import com.starrocks.sql.optimizer.rewrite.PruneAggregateNodeRule;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorsReuseRule;
import com.starrocks.sql.optimizer.rule.Rule;
import com.starrocks.sql.optimizer.rule.RuleSetType;
import com.starrocks.sql.optimizer.rule.implementation.PreAggregateTurnOnRule;
import com.starrocks.sql.optimizer.rule.join.ReorderJoinRule;
import com.starrocks.sql.optimizer.rule.mv.MaterializedViewRule;
import com.starrocks.sql.optimizer.rule.transformation.LimitPruneTabletsRule;
import com.starrocks.sql.optimizer.rule.transformation.MergeProjectWithChildRule;
import com.starrocks.sql.optimizer.rule.transformation.MergeTwoAggRule;
import com.starrocks.sql.optimizer.rule.transformation.MergeTwoProjectRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneEmptyWindowRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownAggToMetaScanRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownJoinOnExpressionToChildProject;
import com.starrocks.sql.optimizer.rule.transformation.PushDownPredicateWindowRankRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownProjectLimitRule;
import com.starrocks.sql.optimizer.rule.transformation.PushLimitAndFilterToCTEProduceRule;
import com.starrocks.sql.optimizer.rule.transformation.ReorderIntersectRule;
import com.starrocks.sql.optimizer.rule.transformation.SemiReorderRule;
import com.starrocks.sql.optimizer.task.DeriveStatsTask;
import com.starrocks.sql.optimizer.task.OptimizeGroupTask;
import com.starrocks.sql.optimizer.task.TaskContext;
import com.starrocks.sql.optimizer.task.TopDownRewriteIterativeTask;
import com.starrocks.sql.optimizer.task.TopDownRewriteOnceTask;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;

/**
 * Optimizer's entrance class
 */
public class Optimizer {
    private static final Logger LOG = LogManager.getLogger(Optimizer.class);
    private OptimizerContext context;

    public OptimizerContext getContext() {
        return context;
    }

    /**
     * Optimizer will transform and implement the logical operator based on
     * the {@see Rule}, then cost the physical operator, and finally find the
     * lowest cost physical operator tree
     *
     * @param logicOperatorTree the input for query Optimizer
     * @param requiredProperty  the required physical property from sql or groupExpression
     * @param requiredColumns   the required output columns from sql or groupExpression
     * @return the lowest cost physical operator for this query
     */
    public OptExpression optimize(ConnectContext connectContext,
                                  OptExpression logicOperatorTree,
                                  PhysicalPropertySet requiredProperty,
                                  ColumnRefSet requiredColumns,
                                  ColumnRefFactory columnRefFactory) {
        // Phase 1: none
        OptimizerTraceUtil.logOptExpression(connectContext, "origin logicOperatorTree:\n%s", logicOperatorTree);
        // Phase 2: rewrite based on memo and group
        Memo memo = new Memo();
        memo.init(logicOperatorTree);
        OptimizerTraceUtil.log(connectContext, "initial root group:\n%s", memo.getRootGroup());

        context = new OptimizerContext(memo, columnRefFactory, connectContext);
        context.setTraceInfo(new OptimizerTraceInfo(connectContext.getQueryId()));
        TaskContext rootTaskContext =
                new TaskContext(context, requiredProperty, (ColumnRefSet) requiredColumns.clone(), Double.MAX_VALUE);

        // Note: root group of memo maybe change after rewrite,
        // so we should always get root group and root group expression
        // directly from memo.
        logicalRuleRewrite(memo, rootTaskContext);
        OptimizerTraceUtil.log(connectContext, "after logical rewrite, root group:\n%s", memo.getRootGroup());

        // collect all olap scan operator
        collectAllScanOperators(memo, rootTaskContext);

        // Currently, we cache output columns in logic property.
        // We derive logic property Bottom Up firstly when new group added to memo,
        // but we do column prune rewrite top down later.
        // So after column prune rewrite, the output columns for each operator maybe change,
        // but the logic property is cached and never change.
        // So we need to explicitly derive all group logic property again
        memo.deriveAllGroupLogicalProperty();

        // Phase 3: optimize based on memo and group
        memoOptimize(connectContext, memo, rootTaskContext);

        OptExpression result;
        if (!connectContext.getSessionVariable().isSetUseNthExecPlan()) {
            result = extractBestPlan(requiredProperty, memo.getRootGroup());
        } else {
            // extract the nth execution plan
            int nthExecPlan = connectContext.getSessionVariable().getUseNthExecPlan();
            result = EnumeratePlan.extractNthPlan(requiredProperty, memo.getRootGroup(), nthExecPlan);
        }
        OptimizerTraceUtil.logOptExpression(connectContext, "after extract best plan:\n%s", result);

        // set costs audio log before physicalRuleRewrite
        // statistics won't set correctly after physicalRuleRewrite.
        // we need set plan costs before physical rewrite stage.
        final CostEstimate costs = Explain.buildCost(result);
        connectContext.getAuditEventBuilder().setPlanCpuCosts(costs.getCpuCost())
                .setPlanMemCosts(costs.getMemoryCost());

        OptExpression finalPlan = physicalRuleRewrite(rootTaskContext, result);
        OptimizerTraceUtil.logOptExpression(connectContext, "final plan after physical rewrite:\n%s", finalPlan);
        OptimizerTraceUtil.log(connectContext, context.getTraceInfo());
        return finalPlan;
    }

    void memoOptimize(ConnectContext connectContext, Memo memo, TaskContext rootTaskContext) {
        OptExpression tree = memo.getRootGroup().extractLogicalTree();

        // Join reorder
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        if (!sessionVariable.isDisableJoinReorder()
                && Utils.countInnerJoinNodeSize(tree) < sessionVariable.getCboMaxReorderNode()) {
            if (Utils.countInnerJoinNodeSize(tree) > sessionVariable.getCboMaxReorderNodeUseExhaustive()) {
                new ReorderJoinRule().transform(tree, context);
                context.getRuleSet().addJoinCommutativityWithOutInnerRule();
            } else {
                if (Utils.capableSemiReorder(tree, false, 0, sessionVariable.getCboMaxReorderNodeUseExhaustive())) {
                    context.getRuleSet().getTransformRules().add(new SemiReorderRule());
                }
                context.getRuleSet().addJoinTransformationRules();
            }
        }

        //add join implementRule
        String joinImplementationMode = ConnectContext.get().getSessionVariable().getJoinImplementationMode();
        if ("merge".equalsIgnoreCase(joinImplementationMode)) {
            context.getRuleSet().addMergeJoinImplementationRule();
        } else if ("hash".equalsIgnoreCase(joinImplementationMode)) {
            context.getRuleSet().addHashJoinImplementationRule();
        } else {
            context.getRuleSet().addAutoJoinImplementationRule();
        }

        context.getTaskScheduler().pushTask(new OptimizeGroupTask(
                rootTaskContext, memo.getRootGroup()));

        context.getTaskScheduler().pushTask(new DeriveStatsTask(
                rootTaskContext, memo.getRootGroup().getFirstLogicalExpression()));

        context.getTaskScheduler().executeTasks(rootTaskContext, memo.getRootGroup());
    }

    OptExpression physicalRuleRewrite(TaskContext rootTaskContext, OptExpression result) {
        Preconditions.checkState(result.getOp().isPhysical());

        // Since there may be many different plans in the logic phase, it's possible
        // that this switch can't turned on after logical optimization, so we only determine
        // whether the PreAggregate can be turned on in the final
        PreAggregateTurnOnRule.tryOpenPreAggregate(result);

        // Rewrite Exchange on top of Sort to Final Sort
        result = new ExchangeSortToMergeRule().rewrite(result);
        result = new PruneAggregateNodeRule().rewrite(result, rootTaskContext);
        result = new AddDecodeNodeForDictStringRule().rewrite(result, rootTaskContext);
        // This rule should be last
        result = new ScalarOperatorsReuseRule().rewrite(result, rootTaskContext);
        // Reorder predicates
        result = new PredicateReorderRule(rootTaskContext.getOptimizerContext().getSessionVariable()).rewrite(result,
                rootTaskContext);
        return result;
    }

    void logicalRuleRewrite(Memo memo, TaskContext rootTaskContext) {
        CTEContext cteContext = context.getCteContext();
        CTEUtils.collectCteOperatorsWithoutCosts(memo, context);
        // inline CTE if consume use once
        while (cteContext.hasInlineCTE()) {
            ruleRewriteOnlyOnce(memo, rootTaskContext, RuleSetType.INLINE_ONE_CTE);
            CTEUtils.collectCteOperatorsWithoutCosts(memo, context);
        }
        cleanUpMemoGroup(memo);

        ruleRewriteIterative(memo, rootTaskContext, RuleSetType.AGGREGATE_REWRITE);
        ruleRewriteIterative(memo, rootTaskContext, RuleSetType.SUBQUERY_REWRITE);
        CTEUtils.collectCteOperatorsWithoutCosts(memo, context);

        // Add full cte required columns, and save orig required columns
        // If cte was inline, the columns don't effect normal prune
        ColumnRefSet requiredColumns = (ColumnRefSet) rootTaskContext.getRequiredColumns().clone();
        rootTaskContext.getRequiredColumns().union(cteContext.getAllRequiredColumns());

        // Note: PUSH_DOWN_PREDICATE tasks should be executed before MERGE_LIMIT tasks
        // because of the Filter node needs to be merged first to avoid the Limit node
        // cannot merge
        ruleRewriteIterative(memo, rootTaskContext, RuleSetType.PUSH_DOWN_PREDICATE);
        cleanUpMemoGroup(memo);

        ruleRewriteIterative(memo, rootTaskContext, new MergeTwoProjectRule());
        ruleRewriteOnlyOnce(memo, rootTaskContext, new PushDownAggToMetaScanRule());
        ruleRewriteOnlyOnce(memo, rootTaskContext, new PushDownPredicateWindowRankRule());
        ruleRewriteOnlyOnce(memo, rootTaskContext, new PushDownJoinOnExpressionToChildProject());
        ruleRewriteOnlyOnce(memo, rootTaskContext, RuleSetType.PRUNE_COLUMNS);
        cleanUpMemoGroup(memo);

        // After prune columns, the output column in the logical property may outdated, because of the following rule
        // will use the output column, we need to derive the logical property here.
        memo.deriveAllGroupLogicalProperty();

        ruleRewriteIterative(memo, rootTaskContext, new PruneEmptyWindowRule());
        ruleRewriteIterative(memo, rootTaskContext, new MergeTwoProjectRule());
        //Limit push must be after the column prune,
        //otherwise the Node containing limit may be prune
        ruleRewriteIterative(memo, rootTaskContext, RuleSetType.MERGE_LIMIT);
        ruleRewriteIterative(memo, rootTaskContext, new MergeTwoAggRule());
        ruleRewriteIterative(memo, rootTaskContext, new PushDownProjectLimitRule());

        ruleRewriteIterative(memo, rootTaskContext, RuleSetType.PRUNE_ASSERT_ROW);
        ruleRewriteIterative(memo, rootTaskContext, RuleSetType.PRUNE_PROJECT);
        ruleRewriteIterative(memo, rootTaskContext, RuleSetType.PRUNE_SET_OPERATOR);

        CTEUtils.collectCteOperatorsWithoutCosts(memo, context);
        if (cteContext.needOptimizeCTE()) {
            cteContext.reset();
            ruleRewriteOnlyOnce(memo, rootTaskContext, RuleSetType.COLLECT_CTE);

            // Prune CTE produce plan columns
            requiredColumns.union(cteContext.getAllRequiredColumns());
            rootTaskContext.setRequiredColumns(requiredColumns);
            ruleRewriteOnlyOnce(memo, rootTaskContext, RuleSetType.PRUNE_COLUMNS);
            // After prune columns, the output column in the logical property may outdated, because of the following rule
            // will use the output column, we need to derive the logical property here.
            memo.deriveAllGroupLogicalProperty();

            if (cteContext.needPushLimit() || cteContext.needPushPredicate()) {
                ruleRewriteOnlyOnce(memo, rootTaskContext, new PushLimitAndFilterToCTEProduceRule());
            }

            if (cteContext.needPushPredicate()) {
                ruleRewriteOnlyOnce(memo, rootTaskContext, RuleSetType.PUSH_DOWN_PREDICATE);
            }

            if (cteContext.needPushLimit()) {
                ruleRewriteOnlyOnce(memo, rootTaskContext, RuleSetType.MERGE_LIMIT);
            }
        }

        OptExpression tree = memo.getRootGroup().extractLogicalTree();
        tree = new MaterializedViewRule().transform(tree, context).get(0);
        memo.replaceRewriteExpression(memo.getRootGroup(), tree);

        ruleRewriteIterative(memo, rootTaskContext, RuleSetType.MULTI_DISTINCT_REWRITE);
        ruleRewriteIterative(memo, rootTaskContext, RuleSetType.PUSH_DOWN_PREDICATE);
        CTEUtils.collectCteOperatorsWithoutCosts(memo, context);

        ruleRewriteOnlyOnce(memo, rootTaskContext, RuleSetType.PARTITION_PRUNE);
        ruleRewriteOnlyOnce(memo, rootTaskContext, LimitPruneTabletsRule.getInstance());
        ruleRewriteIterative(memo, rootTaskContext, RuleSetType.PRUNE_PROJECT);

        cleanUpMemoGroup(memo);

        // compute CTE inline by costs
        if (cteContext.needOptimizeCTE()) {
            CTEUtils.collectCteOperators(memo, context);
        }

        // compute CTE inline by costs
        while (cteContext.needOptimizeCTE() && cteContext.hasInlineCTE()) {
            ruleRewriteOnlyOnce(memo, rootTaskContext, RuleSetType.INLINE_CTE);
            CTEUtils.collectCteOperators(memo, context);
        }

        ruleRewriteIterative(memo, rootTaskContext, new PruneEmptyWindowRule());
        ruleRewriteIterative(memo, rootTaskContext, new MergeTwoProjectRule());
        ruleRewriteIterative(memo, rootTaskContext, new MergeProjectWithChildRule());
        ruleRewriteOnlyOnce(memo, rootTaskContext, new ReorderIntersectRule());

        cleanUpMemoGroup(memo);
    }

    private void cleanUpMemoGroup(Memo memo) {
        // Rewrite maybe produce empty groups, we need to remove them.
        memo.removeAllEmptyGroup();
        memo.removeUnreachableGroup();
    }

    /**
     * Extract the lowest cost physical operator tree from memo
     *
     * @param requiredProperty the required physical property from sql or groupExpression
     * @param rootGroup        the current group to find the lowest cost physical operator
     * @return the lowest cost physical operator for this query
     */
    private OptExpression extractBestPlan(PhysicalPropertySet requiredProperty,
                                          Group rootGroup) {
        GroupExpression groupExpression = rootGroup.getBestExpression(requiredProperty);
        List<PhysicalPropertySet> inputProperties = groupExpression.getInputProperties(requiredProperty);

        List<OptExpression> childPlans = Lists.newArrayList();
        for (int i = 0; i < groupExpression.arity(); ++i) {
            OptExpression childPlan = extractBestPlan(inputProperties.get(i), groupExpression.inputAt(i));
            childPlans.add(childPlan);
        }

        OptExpression expression = OptExpression.create(groupExpression.getOp(),
                childPlans);
        // record inputProperties at optExpression, used for planFragment builder to determine join type
        expression.setRequiredProperties(inputProperties);
        expression.setStatistics(groupExpression.getGroup().hasConfidenceStatistic(requiredProperty) ?
                groupExpression.getGroup().getConfidenceStatistic(requiredProperty) :
                groupExpression.getGroup().getStatistics());

        // When build plan fragment, we need the output column of logical property
        expression.setLogicalProperty(rootGroup.getLogicalProperty());
        return expression;
    }

    private void collectAllScanOperators(Memo memo, TaskContext rootTaskContext) {
        OptExpression tree = memo.getRootGroup().extractLogicalTree();
        List<LogicalOlapScanOperator> list = Lists.newArrayList();
        Utils.extractOlapScanOperator(tree.getGroupExpression(), list);
        rootTaskContext.setAllScanOperators(Collections.unmodifiableList(list));
    }

    void ruleRewriteIterative(Memo memo, TaskContext rootTaskContext, RuleSetType ruleSetType) {
        context.getTaskScheduler().pushTask(new TopDownRewriteIterativeTask(rootTaskContext,
                memo.getRootGroup(), ruleSetType));
        context.getTaskScheduler().executeTasks(rootTaskContext, memo.getRootGroup());
    }

    void ruleRewriteIterative(Memo memo, TaskContext rootTaskContext, Rule rule) {
        context.getTaskScheduler().pushTask(new TopDownRewriteIterativeTask(rootTaskContext,
                memo.getRootGroup(), rule));
        context.getTaskScheduler().executeTasks(rootTaskContext, memo.getRootGroup());
    }

    void ruleRewriteOnlyOnce(Memo memo, TaskContext rootTaskContext, RuleSetType ruleSetType) {
        context.getTaskScheduler().pushTask(new TopDownRewriteOnceTask(rootTaskContext,
                memo.getRootGroup(), ruleSetType));
        context.getTaskScheduler().executeTasks(rootTaskContext, memo.getRootGroup());
    }

    void ruleRewriteOnlyOnce(Memo memo, TaskContext rootTaskContext, Rule rule) {
        context.getTaskScheduler().pushTask(new TopDownRewriteOnceTask(rootTaskContext,
                memo.getRootGroup(), rule));
        context.getTaskScheduler().executeTasks(rootTaskContext, memo.getRootGroup());
    }
}
