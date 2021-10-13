// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.optimizer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.rewrite.AddDecodeNodeForDictStringRule;
import com.starrocks.sql.optimizer.rewrite.ExchangeSortToMergeRule;
import com.starrocks.sql.optimizer.rule.Rule;
import com.starrocks.sql.optimizer.rule.RuleSetType;
import com.starrocks.sql.optimizer.rule.implementation.PreAggregateTurnOnRule;
import com.starrocks.sql.optimizer.rule.join.ReorderJoinRule;
import com.starrocks.sql.optimizer.rule.mv.MaterializedViewRule;
import com.starrocks.sql.optimizer.rule.transformation.JoinForceLimitRule;
import com.starrocks.sql.optimizer.rule.transformation.MergeProjectWithChildRule;
import com.starrocks.sql.optimizer.rule.transformation.MergeTwoAggRule;
import com.starrocks.sql.optimizer.rule.transformation.MergeTwoProjectRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneEmptyWindowRule;
import com.starrocks.sql.optimizer.rule.transformation.PruneProjectRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownAggToMetaScanRule;
import com.starrocks.sql.optimizer.rule.transformation.PushDownJoinOnExpressionToChildProject;
import com.starrocks.sql.optimizer.rule.transformation.ScalarOperatorsReuseRule;
import com.starrocks.sql.optimizer.task.DeriveStatsTask;
import com.starrocks.sql.optimizer.task.OptimizeGroupTask;
import com.starrocks.sql.optimizer.task.TaskContext;
import com.starrocks.sql.optimizer.task.TopDownRewriteIterativeTask;
import com.starrocks.sql.optimizer.task.TopDownRewriteOnceTask;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Optimizer's entrance class
 */
public class Optimizer {
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
        // Phase 2: rewrite based on memo and group
        Memo memo = new Memo();
        memo.init(logicOperatorTree);

        context = new OptimizerContext(memo, columnRefFactory, connectContext.getSessionVariable(),
                connectContext.getDumpInfo());

        TaskContext rootTaskContext = new TaskContext(context,
                requiredProperty, (ColumnRefSet) requiredColumns.clone(), Double.MAX_VALUE);
        context.addTaskContext(rootTaskContext);

        // Note: root group of memo maybe change after rewrite,
        // so we should always get root group and root group expression
        // directly from memo.

        ruleRewriteIterative(memo, rootTaskContext, RuleSetType.MULTI_DISTINCT_REWRITE);
        ruleRewriteIterative(memo, rootTaskContext, RuleSetType.SUBQUERY_REWRITE);
        // Note: PUSH_DOWN_PREDICATE tasks should be executed before MERGE_LIMIT tasks
        // because of the Filter node needs to be merged first to avoid the Limit node
        // cannot merge
        ruleRewriteIterative(memo, rootTaskContext, RuleSetType.PUSH_DOWN_PREDICATE);
        ruleRewriteIterative(memo, rootTaskContext, new MergeTwoProjectRule());
        ruleRewriteOnlyOnce(memo, rootTaskContext, new PushDownAggToMetaScanRule());

        ruleRewriteOnlyOnce(memo, rootTaskContext, new PushDownJoinOnExpressionToChildProject());
        ruleRewriteOnlyOnce(memo, rootTaskContext, RuleSetType.PRUNE_COLUMNS);
        ruleRewriteIterative(memo, rootTaskContext, new PruneEmptyWindowRule());
        ruleRewriteIterative(memo, rootTaskContext, new MergeTwoProjectRule());
        //Limit push must be after the column prune,
        //otherwise the Node containing limit may be prune
        ruleRewriteIterative(memo, rootTaskContext, RuleSetType.MERGE_LIMIT);
        ruleRewriteIterative(memo, rootTaskContext, new MergeTwoAggRule());
        //After the MERGE_LIMIT, ProjectNode that can be merged may appear.
        //So we do another MergeTwoProjectRule
        ruleRewriteIterative(memo, rootTaskContext, RuleSetType.PRUNE_ASSERT_ROW);
        ruleRewriteIterative(memo, rootTaskContext, new MergeTwoProjectRule());

        OptExpression tree = memo.getRootGroup().extractLogicalTree();
        tree = new MaterializedViewRule().transform(tree, context).get(0);
        memo.replaceRewriteExpression(memo.getRootGroup(), tree);

        ruleRewriteOnlyOnce(memo, rootTaskContext, RuleSetType.PARTITION_PRUNE);
        ruleRewriteIterative(memo, rootTaskContext, new PruneProjectRule());
        ruleRewriteOnlyOnce(memo, rootTaskContext, new ScalarOperatorsReuseRule());
        ruleRewriteIterative(memo, rootTaskContext, new MergeProjectWithChildRule());
        ruleRewriteOnlyOnce(memo, rootTaskContext, new JoinForceLimitRule());

        // Rewrite maybe produce empty groups, we need to remove them.
        memo.removeAllEmptyGroup();
        memo.removeUnreachableGroup();

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
        tree = memo.getRootGroup().extractLogicalTree();

        if (!connectContext.getSessionVariable().isDisableJoinReorder()) {
            if (Utils.countInnerJoinNodeSize(tree) >
                    connectContext.getSessionVariable().getCboMaxReorderNodeUseExhaustive()) {
                //If there is no statistical information, the DP and greedy reorder algorithm are disabled,
                //and the query plan degenerates to the left deep tree
                if (Utils.hasUnknownColumnsStats(tree) && !FeConstants.runningUnitTest) {
                    connectContext.getSessionVariable().disableDPJoinReorder();
                    connectContext.getSessionVariable().disableGreedyJoinReorder();
                }
                new ReorderJoinRule().transform(tree, context);
                context.getRuleSet().addJoinCommutativityWithOutInnerRule();
            } else {
                context.getRuleSet().addJoinTransformationRules();
            }
        }

        if (connectContext.getSessionVariable().isEnableNewPlannerPushDownJoinToAgg()) {
            context.getRuleSet().addPushDownJoinToAggRule();
        }

        context.getTaskScheduler().pushTask(new OptimizeGroupTask(
                rootTaskContext, memo.getRootGroup()));

        context.getTaskScheduler().pushTask(new DeriveStatsTask(
                rootTaskContext, memo.getRootGroup().getFirstLogicalExpression()));

        context.getTaskScheduler().executeTasks(rootTaskContext, memo.getRootGroup());

        OptExpression result;
        if (!connectContext.getSessionVariable().isSetUseNthExecPlan()) {
            result = extractBestPlan(requiredProperty, memo.getRootGroup());
        } else {
            // extract the nth execution plan
            int nthExecPlan = connectContext.getSessionVariable().getUseNthExecPlan();
            result = extractNthPlan(requiredProperty, memo.getRootGroup(), nthExecPlan);
            if (result == null) {
                throw new StarRocksPlannerException("Can not extract " + nthExecPlan + " from Memo",
                        ErrorType.INTERNAL_ERROR);
            }
        }
        tryOpenPreAggregate(result);
        // Rewrite Exchange on top of Sort to Final Sort
        result = new ExchangeSortToMergeRule().rewrite(result);
        result = new AddDecodeNodeForDictStringRule().rewrite(result, rootTaskContext);
        return result;
    }

    // refers to Waas F, Galindo-Legaria C. Counting, enumerating, and sampling of execution plans in a cost-based query optimizer.
    private OptExpression extractNthPlan(PhysicalPropertySet requiredProperty, Group rootGroup, int nthExecPlan) {
        if (nthExecPlan < 0) {
            return null;
        }
        // 1. count the valid plan in Memo and record in GroupExpression
        int planCount = countGroupValidPlan(rootGroup, requiredProperty);
        if (nthExecPlan > planCount) {
            throw new StarRocksPlannerException(
                    "plan count is " + planCount + ", can not extract the " + nthExecPlan + "th plan",
                    ErrorType.USER_ERROR);
        }
        // 2. use unranking method to extract the nth plan
        OptExpression expression = extractNthPlanImpl(requiredProperty, rootGroup, nthExecPlan);
        expression.setPlanCount(planCount);
        return expression;
    }

    private OptExpression extractNthPlanImpl(PhysicalPropertySet requiredProperty, Group group, int nthExecPlan) {
        int planCountOfKGroupExpression = 0;
        List<OptExpression> childPlans = Lists.newArrayList();
        // 1. Calculate the GroupExpression of nthExecPlan in the Group
        GroupExpression chooseGroupExpression = null;
        for (GroupExpression physicalExpression : group.getSatisfyRequiredGroupExpressions(requiredProperty)) {
            if (planCountOfKGroupExpression + physicalExpression.getRequiredPropertyPlanCount(requiredProperty) >=
                    nthExecPlan) {
                chooseGroupExpression = physicalExpression;
                break;
            }
            planCountOfKGroupExpression += physicalExpression.getRequiredPropertyPlanCount(requiredProperty);
        }

        Preconditions.checkState(chooseGroupExpression != null);
        // 2. compute the local-rank in the chooseGroupExpression.
        int localRankOfGroupExpression = nthExecPlan - planCountOfKGroupExpression;
        // 3. compute use which output/input properties
        int planCountOfKProperties = 0;
        for (Map.Entry<Pair<PhysicalPropertySet, List<PhysicalPropertySet>>, Integer> entry : chooseGroupExpression
                .getPropertiesPlanCountMap().entrySet()) {
            List<PhysicalPropertySet> inputProperties = entry.getKey().second;

            if (planCountOfKProperties + entry.getValue() >= localRankOfGroupExpression) {
                // 4. compute the localProperty-rank in the property.
                int localRankOfProperties = localRankOfGroupExpression - planCountOfKProperties;
                // 5. compute sub-ranks of children groups
                List<Integer> childRankList =
                        computeNthOfChildGroups(chooseGroupExpression, localRankOfProperties, inputProperties);
                // 6. computes the child group recursively
                for (int childIndex = 0; childIndex < chooseGroupExpression.arity(); ++childIndex) {
                    OptExpression childPlan = extractNthPlanImpl(inputProperties.get(childIndex),
                            chooseGroupExpression.inputAt(childIndex), childRankList.get(childIndex));
                    childPlans.add(childPlan);
                }
                break;
            }
            planCountOfKProperties += entry.getValue();
        }
        // 7. construct the OptExpression
        OptExpression chooseExpression = OptExpression.create(chooseGroupExpression.getOp(), childPlans);
        chooseExpression.setStatistics(group.getConfidenceStatistics() != null ?
                group.getConfidenceStatistics() :
                group.getStatistics());

        // When build plan fragment, we need the output column of logical property
        chooseExpression.setLogicalProperty(group.getLogicalProperty());
        return chooseExpression;
    }

    // compute sub-ranks of children groups
    private List<Integer> computeNthOfChildGroups(GroupExpression groupExpression, int localNth,
                                                  List<PhysicalPropertySet> inputProperties) {
        // 1. compute the plan count of each child group which satisfied the inputProperties
        List<Integer> childGroupExprCountList = Lists.newArrayList();
        for (int childIndex = 0; childIndex < groupExpression.arity(); ++childIndex) {
            PhysicalPropertySet childInputProperty = inputProperties.get(childIndex);
            Set<GroupExpression> childGroupExpressions =
                    groupExpression.inputAt(childIndex).getSatisfyRequiredGroupExpressions(childInputProperty);
            int childGroupExprPropertiesCount = childGroupExpressions.stream()
                    .mapToInt(childGroupExpression -> childGroupExpression
                            .getRequiredPropertyPlanCount(childInputProperty)).sum();
            childGroupExprCountList.add(childGroupExprPropertiesCount);
        }

        // 2. use the childGroupExprPropertiesCount to compute nth rank of each child group
        List<Integer> childNthList = Lists.newArrayList();
        // use this variable as R(i+1) to avoid the recursive calculation.
        AtomicInteger subsequentChildGroupRank = new AtomicInteger();
        for (int childIndex = groupExpression.arity() - 1; childIndex >= 0; --childIndex) {
            childNthList.add(computeNthOfChildGroup(childIndex, groupExpression.arity(), localNth,
                    childGroupExprCountList, subsequentChildGroupRank));
        }
        Collections.reverse(childNthList);
        return childNthList;
    }

    // compute sub-rank of child group
    private int computeNthOfChildGroup(int childIndex, int childrenSize, int localNth,
                                       List<Integer> childGroupExprCountList, AtomicInteger subsequentChildGroupRank) {
        if (childIndex == 0) {
            return computeNthOfChildGroupImpl(childIndex, childrenSize, localNth, childGroupExprCountList,
                    subsequentChildGroupRank);
        } else {
            return (int) Math.ceil((computeNthOfChildGroupImpl(childIndex, childrenSize, localNth,
                    childGroupExprCountList, subsequentChildGroupRank) * 1.0) /
                    (computeKChildrenPlanCount(childIndex - 1, childGroupExprCountList) * 1.0));
        }
    }

    private int computeNthOfChildGroupImpl(int childIndex, int childrenSize, int localNth,
                                           List<Integer> childGroupExprCountList,
                                           AtomicInteger subsequentChildGroupRank) {
        if (childIndex + 1 == childrenSize) {
            subsequentChildGroupRank.set(localNth);
            return localNth;
        } else {
            int kChildrenPlanCount = computeKChildrenPlanCount(childIndex, childGroupExprCountList);
            int result = subsequentChildGroupRank.get() % kChildrenPlanCount;
            // the localNth is start from 1, special treated.
            if (result == 0) {
                result = kChildrenPlanCount;
            }
            subsequentChildGroupRank.set(result);
            return result;
        }
    }

    // Compute the product of the plan count of the first k child nodes
    private int computeKChildrenPlanCount(int k, List<Integer> childGroupExprCountList) {
        int result = 1;
        for (int i = 0; i <= k; ++i) {
            result *= childGroupExprCountList.get(i);
        }
        return result;
    }

    // Calculate the valid plan count for the group with required property.
    private int countGroupValidPlan(Group group, PhysicalPropertySet requiredProperty) {
        int groupPlanCount = 0;
        for (GroupExpression physicalExpression : group.getSatisfyRequiredGroupExpressions(requiredProperty)) {
            if (physicalExpression.getInputs().isEmpty()) {
                // It's leaf node of the plan
                physicalExpression.addPlanCountOfProperties(new Pair<>(requiredProperty, Lists.newArrayList()), 1);
            } else if (physicalExpression.hasValidSubPlan()) {
                // count the plan count of this group expression
                countGroupExpressionValidPlan(physicalExpression, requiredProperty);
            }
            groupPlanCount += physicalExpression.getRequiredPropertyPlanCount(requiredProperty);
        }
        return groupPlanCount;
    }

    // Calculate the valid plan count for the group expression with required property.
    private int countGroupExpressionValidPlan(GroupExpression groupExpression, PhysicalPropertySet requiredProperty) {
        int groupExpressionPlanCount = 0;
        for (List<PhysicalPropertySet> inputProperties : groupExpression.getRequiredInputProperties(requiredProperty)) {
            int childPlanCount = 1;
            for (int childIndex = 0; childIndex < groupExpression.arity(); ++childIndex) {
                childPlanCount *=
                        countGroupValidPlan(groupExpression.inputAt(childIndex), inputProperties.get(childIndex));
            }
            groupExpression.addPlanCountOfProperties(new Pair<>(requiredProperty, inputProperties), childPlanCount);
            groupExpressionPlanCount += childPlanCount;
        }
        return groupExpressionPlanCount;
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
        expression.setStatistics(groupExpression.getGroup().getConfidenceStatistics() != null ?
                groupExpression.getGroup().getConfidenceStatistics() :
                groupExpression.getGroup().getStatistics());

        // When build plan fragment, we need the output column of logical property
        expression.setLogicalProperty(rootGroup.getLogicalProperty());
        return expression;
    }

    // Since there may be many different plans in the logic phase, it's possible
    // that this switch can't turned on after logical optimization, so we only determine
    // whether the PreAggregate can be turned on in the final
    private void tryOpenPreAggregate(OptExpression optExpression) {
        Preconditions.checkState(optExpression.getOp().isPhysical());
        PreAggregateTurnOnRule.tryOpenPreAggregate(optExpression);
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
