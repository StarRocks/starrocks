// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.task;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Catalog;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.ChildPropertyDeriver;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.Group;
import com.starrocks.sql.optimizer.GroupExpression;
import com.starrocks.sql.optimizer.base.DistributionProperty;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.GatherDistributionSpec;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.base.SortProperty;
import com.starrocks.sql.optimizer.cost.CostModel;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProjectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.optimizer.statistics.StatisticsCalculator;

import java.util.List;

/**
 * EnforceAndCostTask costs a physical expression.
 * The root operator is cost first
 * and the lowest cost of each child group is added.
 * <p>
 * Finally, properties are enforced to meet requirement in the context.
 * <p>
 * We apply pruning by terminating if
 * the current expression's cost is larger than the upper bound of the current group
 * <p>
 * EnforceAndCostTask implementation inspire by Cascades paper and CMU noisepage project
 */
public class EnforceAndCostTask extends OptimizerTask implements Cloneable {
    private final GroupExpression groupExpression;
    // The Pair first is output PropertySet
    // The Pair second is multi input PropertySets
    private List<Pair<PhysicalPropertySet, List<PhysicalPropertySet>>> outputInputProperties;
    // localCost + sum of all InputCost entries.
    private double curTotalCost;
    // the local cost of the group expression
    private double localCost;
    // Current stage of enumeration through child groups
    private int curChildIndex = -1;
    // Indicator of last child group that we waited for optimization
    private int prevChildIndex = -1;
    // Current stage of enumeration through outputInputProperties
    private int curPropertyPairIndex = 0;

    EnforceAndCostTask(TaskContext context, GroupExpression expression) {
        super(context);
        this.groupExpression = expression;
    }

    // Shallow Clone here
    // We don't need to clone outputInputProperties and groupExpression
    @Override
    public Object clone() {
        EnforceAndCostTask task = null;
        try {
            task = (EnforceAndCostTask) super.clone();
        } catch (CloneNotSupportedException ignored) {
        }
        return task;
    }

    @Override
    public String toString() {
        return "EnforceAndCostTask for groupExpression " + groupExpression +
                "\n curChildIndex " + curChildIndex +
                "\n prevChildIndex " + prevChildIndex +
                "\n curTotalCost " + curTotalCost;
    }

    @Override
    public void execute() {
        if (groupExpression.isUnused()) {
            return;
        }

        initOutputProperties();

        for (; curPropertyPairIndex < outputInputProperties.size(); curPropertyPairIndex++) {
            PhysicalPropertySet outputProperty = outputInputProperties.get(curPropertyPairIndex).first;
            List<PhysicalPropertySet> inputProperties = outputInputProperties.get(curPropertyPairIndex).second;

            // Calculate local cost and update total cost
            if (curChildIndex == 0 && prevChildIndex == -1) {
                localCost = CostModel.calculateCost(groupExpression);
                curTotalCost += localCost;
            }

            for (; curChildIndex < groupExpression.getInputs().size(); curChildIndex++) {
                PhysicalPropertySet inputProperty = inputProperties.get(curChildIndex);
                Group childGroup = groupExpression.getInputs().get(curChildIndex);

                // Check whether the child group is already optimized for the property
                GroupExpression childBestExpr = childGroup.getBestExpression(inputProperty);

                if (childBestExpr == null && prevChildIndex >= curChildIndex) {
                    // If there can not find best child expr or push child's OptimizeGroupTask, The child has been
                    // pruned because of UpperBound cost prune, and parent task can break here and return
                    break;
                }

                if (childBestExpr == null) {
                    // We haven't optimized child group
                    prevChildIndex = curChildIndex;
                    optimizeChildGroup(inputProperty, childGroup);
                    return;
                }

                // Directly get back the best expr if the child group is optimized
                // Don't allow enforce sort and distribution below project node
                if (!inputProperty.isEmpty() && groupExpression.getOp() instanceof PhysicalProjectOperator &&
                        (childBestExpr.getOp() instanceof PhysicalDistributionOperator
                                || childBestExpr.getOp() instanceof PhysicalTopNOperator)) {
                    break;
                }

                // check if we can generate one stage agg
                if (!canGenerateOneStageAgg(childBestExpr)) {
                    break;
                }

                if (!doBroadcastHint(inputProperty, childBestExpr)) {
                    break;
                }

                curTotalCost += childBestExpr.getCost(inputProperty);
                if (curTotalCost > context.getUpperBoundCost()) {
                    break;
                }
            }

            // Successfully optimize all child group
            if (curChildIndex == groupExpression.getInputs().size()) {
                // update current group statistics and re-compute costs
                if (!computeCurrentGroupStatistics()) {
                    // child group has been prune
                    return;
                }

                recordCostsAndEnforce(outputProperty, inputProperties);
            }
            // Reset child idx and total cost
            prevChildIndex = -1;
            curChildIndex = 0;
            curTotalCost = 0;
        }
    }

    private void initOutputProperties() {
        if (curChildIndex != -1) {
            // Has been init output properties, is optimizer the operator again
            return;
        }

        localCost = 0;
        curTotalCost = 0;

        // TODO(kks): do Lower Bound Pruning here
        ChildPropertyDeriver childPropertyDeriver = new ChildPropertyDeriver(context);
        outputInputProperties = childPropertyDeriver.getOutputInputProps(
                context.getRequiredProperty(),
                groupExpression);
        curChildIndex = 0;
    }

    private void optimizeChildGroup(PhysicalPropertySet inputProperty, Group childGroup) {
        pushTask((EnforceAndCostTask) clone());
        double newUpperBound = context.getUpperBoundCost() - curTotalCost;
        TaskContext taskContext = new TaskContext(context.getOptimizerContext(),
                inputProperty, context.getRequiredColumns(), newUpperBound, context.getAllScanOperators());
        pushTask(new OptimizeGroupTask(taskContext, childGroup));
        context.getOptimizerContext().addTaskContext(taskContext);
    }

    private boolean doBroadcastHint(PhysicalPropertySet inputProperty, GroupExpression childBestExpr) {
        if (!inputProperty.getDistributionProperty().isBroadcast()) {
            return true;
        }

        if (!OperatorType.PHYSICAL_HASH_JOIN.equals(groupExpression.getOp().getOpType())) {
            return true;
        }

        Statistics leftChildStats = groupExpression.getInputs().get(curChildIndex - 1).getStatistics();
        Statistics rightChildStats = groupExpression.getInputs().get(curChildIndex).getStatistics();
        if (leftChildStats == null || rightChildStats == null) {
            return false;
        }
        // Only when right table is not significantly smaller than left table, consider the
        // broadcastRowCountLimit, Otherwise, this limit is not considered, which can avoid
        // shuffling large left-hand table data
        int parallelExecInstance = Math.max(1,
                Math.min(groupExpression.getGroup().getLogicalProperty().getLeftMostScanTabletsNum(),
                        ConnectContext.get().getSessionVariable().getParallelExecInstanceNum()));
        int beNum = Math.max(1, Catalog.getCurrentSystemInfo().getBackendIds(true).size());
        PhysicalHashJoinOperator operator = (PhysicalHashJoinOperator) groupExpression.getOp();
        if (leftChildStats.getOutputSize() < rightChildStats.getOutputSize() * parallelExecInstance * beNum * 10
                && rightChildStats.getOutputRowCount() > ConnectContext.get().getSessionVariable()
                .getBroadcastRowCountLimit() && !operator.getJoinHint().equalsIgnoreCase("BROADCAST")) {
            return false;
        }

        // If broadcast child has hint, need to change the cost to zero
        double childCost = childBestExpr.getCost(inputProperty);
        if (operator.getJoinHint().equalsIgnoreCase("BROADCAST")
                && childCost == Double.POSITIVE_INFINITY) {
            List<PhysicalPropertySet> childInputProperties =
                    childBestExpr.getInputProperties(inputProperty);
            childBestExpr.setPropertyWithCost(inputProperty, childInputProperties, 0);
        }

        return true;
    }

    private void recordCostsAndEnforce(PhysicalPropertySet outputProperty, List<PhysicalPropertySet> inputProperties) {
        // re-calculate local cost and update total cost
        curTotalCost -= localCost;
        localCost = CostModel.calculateCost(groupExpression);
        curTotalCost += localCost;

        setPropertyWithCost(groupExpression, outputProperty, inputProperties);

        PhysicalPropertySet requiredProperty = context.getRequiredProperty();
        // Enforce property if outputProperty doesn't satisfy context requiredProperty
        if (!outputProperty.isSatisfy(requiredProperty)) {
            PhysicalPropertySet enforcedProperty = enforceProperty(outputProperty, requiredProperty);
            // enforcedProperty is superset of requiredProperty
            if (!enforcedProperty.equals(requiredProperty)) {
                setPropertyWithCost(groupExpression.getGroup().getBestExpression(enforcedProperty),
                        requiredProperty, Lists.newArrayList(outputProperty));
            }
        } else {
            // outputProperty is superset of requiredProperty
            if (!outputProperty.equals(requiredProperty)) {
                setPropertyWithCost(groupExpression, requiredProperty, inputProperties);
            }
        }

        if (curTotalCost < context.getUpperBoundCost()) {
            // update context upperbound cost
            context.setUpperBoundCost(curTotalCost);
        }
    }

    // Disable one phase Agg node with unknown column statistics or table row count may not accurate because of
    // fe meta may not get real row count from be.
    // NOTE: Not include one phase local Agg node
    private boolean canGenerateOneStageAgg(GroupExpression childBestExpr) {
        if (!OperatorType.PHYSICAL_HASH_AGG.equals(groupExpression.getOp().getOpType())) {
            return true;
        }
        // respect session variable new_planner_agg_stage
        int aggStage = ConnectContext.get().getSessionVariable().getNewPlannerAggStage();
        if (aggStage == 1) {
            return true;
        }

        PhysicalHashAggregateOperator aggregate = (PhysicalHashAggregateOperator) groupExpression.getOp();
        // 1. check the agg node is global aggregation without split and child expr is PhysicalDistributionOperator
        if (aggregate.getType().isGlobal() && !aggregate.isSplit() &&
                childBestExpr.getOp() instanceof PhysicalDistributionOperator) {
            // 2. check default column statistics or child output row may not be accurate
            if (groupExpression.getGroup().getStatistics().getColumnStatistics().values().stream()
                    .allMatch(ColumnStatistic::isUnknown) ||
                    childBestExpr.getGroup().getStatistics().isTableRowCountMayInaccurate()) {
                // 3. check child expr distribution, if it is shuffle or gather without limit, could disable this plan
                PhysicalDistributionOperator distributionOperator =
                        (PhysicalDistributionOperator) childBestExpr.getOp();
                if (distributionOperator.getDistributionSpec().getType()
                        .equals(DistributionSpec.DistributionType.SHUFFLE) ||
                        (distributionOperator.getDistributionSpec().getType()
                                .equals(DistributionSpec.DistributionType.GATHER) &&
                                !((GatherDistributionSpec) distributionOperator.getDistributionSpec()).hasLimit())) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean computeCurrentGroupStatistics() {
        ExpressionContext expressionContext = new ExpressionContext(groupExpression);
        if (groupExpression.getInputs().stream().anyMatch(group -> group.getStatistics() == null)) {
            return false;
        }

        StatisticsCalculator statisticsCalculator = new StatisticsCalculator(expressionContext,
                groupExpression.getGroup().getLogicalProperty().getOutputColumns(),
                context.getOptimizerContext().getColumnRefFactory(), context.getOptimizerContext().getDumpInfo());
        statisticsCalculator.estimatorStats();
        groupExpression.getGroup().setStatistics(expressionContext.getStatistics());
        return true;
    }

    private void setPropertyWithCost(GroupExpression groupExpression,
                                     PhysicalPropertySet outputProperty,
                                     List<PhysicalPropertySet> inputProperties) {
        groupExpression.setPropertyWithCost(outputProperty, inputProperties, curTotalCost);
        this.groupExpression.getGroup().setBestExpression(groupExpression,
                curTotalCost, outputProperty);
    }

    private PhysicalPropertySet enforceProperty(PhysicalPropertySet outputProperty,
                                                PhysicalPropertySet requiredProperty) {
        boolean satisfyOrderProperty =
                outputProperty.getSortProperty().isSatisfy(requiredProperty.getSortProperty());
        boolean satisfyDistributionProperty =
                outputProperty.getDistributionProperty().isSatisfy(requiredProperty.getDistributionProperty());

        PhysicalPropertySet enforcedProperty = null;
        if (!satisfyDistributionProperty && satisfyOrderProperty) {
            if (requiredProperty.getSortProperty().isEmpty()) {
                enforcedProperty = enforceDistribute(outputProperty);
            } else {
                /*
                 * The sorting attribute does not make sense when the sort property is not empty,
                 * because after the data is redistributed, the original order requirements cannot be guaranteed.
                 * So we need to enforce "SortNode" here
                 *
                 * Because we build a parent-child relationship based on property.
                 * So here we hack to eliminate the original property to prevent an endless loop
                 * eg: [order by v1, gather] -> [order by v1, shuffle] -> [order by v1, shuffle] may endless loop,
                 * because repartition require sort again
                 */
                PhysicalPropertySet newProperty =
                        new PhysicalPropertySet(DistributionProperty.EMPTY, SortProperty.EMPTY);
                groupExpression.getGroup().replaceBestExpressionProperty(outputProperty, newProperty,
                        groupExpression.getCost(outputProperty));
                enforcedProperty = enforceSortAndDistribute(newProperty, requiredProperty);
            }
        } else if (satisfyDistributionProperty && !satisfyOrderProperty) {
            enforcedProperty = enforceSort(outputProperty);
        } else if (!satisfyDistributionProperty) {
            enforcedProperty = enforceSortAndDistribute(outputProperty, requiredProperty);
        }
        return enforcedProperty;
    }

    private PhysicalPropertySet enforceDistribute(PhysicalPropertySet oldOutputProperty) {
        PhysicalPropertySet newOutputProperty = oldOutputProperty.copy();
        newOutputProperty.setDistributionProperty(context.getRequiredProperty().getDistributionProperty());
        GroupExpression enforcer =
                context.getRequiredProperty().getDistributionProperty().appendEnforcers(groupExpression.getGroup());

        updateCostWithEnforcer(enforcer, oldOutputProperty, newOutputProperty);

        return newOutputProperty;
    }

    private PhysicalPropertySet enforceSort(PhysicalPropertySet oldOutputProperty) {
        PhysicalPropertySet newOutputProperty = oldOutputProperty.copy();
        newOutputProperty.setSortProperty(context.getRequiredProperty().getSortProperty());
        GroupExpression enforcer =
                context.getRequiredProperty().getSortProperty().appendEnforcers(groupExpression.getGroup());

        updateCostWithEnforcer(enforcer, oldOutputProperty, newOutputProperty);

        return newOutputProperty;
    }

    private PhysicalPropertySet enforceSortAndDistribute(PhysicalPropertySet outputProperty,
                                                         PhysicalPropertySet requiredProperty) {
        PhysicalPropertySet enforcedProperty;
        if (requiredProperty.getDistributionProperty().getSpec()
                .equals(DistributionSpec.createGatherDistributionSpec())) {
            enforcedProperty = enforceSort(outputProperty);
            enforcedProperty = enforceDistribute(enforcedProperty);
        } else {
            enforcedProperty = enforceDistribute(outputProperty);
            enforcedProperty = enforceSort(enforcedProperty);
        }

        return enforcedProperty;
    }

    private void updateCostWithEnforcer(GroupExpression enforcer,
                                        PhysicalPropertySet oldOutputProperty,
                                        PhysicalPropertySet newOutputProperty) {
        context.getOptimizerContext().getMemo().
                insertEnforceExpression(enforcer, groupExpression.getGroup());
        curTotalCost += CostModel.calculateCost(enforcer);

        enforcer.setPropertyWithCost(newOutputProperty, Lists.newArrayList(oldOutputProperty), curTotalCost);
        groupExpression.getGroup().setBestExpression(enforcer, curTotalCost, newOutputProperty);
    }
}
