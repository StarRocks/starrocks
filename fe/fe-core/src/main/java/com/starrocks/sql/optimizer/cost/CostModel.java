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

package com.starrocks.sql.optimizer.cost;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.Group;
import com.starrocks.sql.optimizer.GroupExpression;
import com.starrocks.sql.optimizer.JoinHelper;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.DataSkewInfo;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.physical.PhysicalAssertOneRowOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalMergeJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalNestLoopJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalNoCTEOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProjectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalWindowOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.optimizer.statistics.StatisticsEstimateCoefficient;
import com.starrocks.statistic.StatisticUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class CostModel {

    private static final Logger LOG = LogManager.getLogger(CostModel.class);

    public static double calculateCost(GroupExpression expression) {
        ExpressionContext expressionContext = new ExpressionContext(expression);
        return calculateCost(expressionContext);
    }

    private static double calculateCost(ExpressionContext expressionContext) {
        CostEstimator costEstimator = new CostEstimator(ImmutableList.of());
        CostEstimate costEstimate = expressionContext.getOp().accept(costEstimator, expressionContext);
        double realCost = getRealCost(costEstimate);
        LOG.debug("operator: {}, outputRowCount: {}, outPutSize: {}, costEstimate: {}, realCost: {}",
                expressionContext.getOp(),
                expressionContext.getStatistics().getOutputRowCount(),
                expressionContext.getStatistics().getComputeSize(),
                costEstimate, realCost);
        return realCost;
    }

    public static CostEstimate calculateCostEstimate(ExpressionContext expressionContext) {
        CostEstimator costEstimator = new CostEstimator(ImmutableList.of());
        return expressionContext.getOp().accept(costEstimator, expressionContext);
    }

    public static double calculateCostWithChildrenOutProperty(GroupExpression expression,
                                                              List<PhysicalPropertySet> childrenOutputProperties) {
        ExpressionContext expressionContext = new ExpressionContext(expression);
        CostEstimator costEstimator = new CostEstimator(childrenOutputProperties);
        CostEstimate costEstimate = expressionContext.getOp().accept(costEstimator, expressionContext);
        double realCost = getRealCost(costEstimate);

        LOG.debug("operator: {}, group id: {}, child group id: {}, " +
                        "inputProperties: {}, outputRowCount: {}, outPutSize: {}, costEstimate: {}, realCost: {}",
                expressionContext.getOp(), expression.getGroup().getId(),
                expression.getInputs().stream().map(Group::getId).collect(Collectors.toList()),
                childrenOutputProperties,
                expressionContext.getStatistics().getOutputRowCount(),
                expressionContext.getStatistics().getComputeSize(),
                costEstimate, realCost);
        return realCost;
    }

    public static double getRealCost(CostEstimate costEstimate) {
        double cpuCostWeight = 0.5;
        double memoryCostWeight = 2;
        double networkCostWeight = 1.5;
        return costEstimate.getCpuCost() * cpuCostWeight +
                costEstimate.getMemoryCost() * memoryCostWeight +
                costEstimate.getNetworkCost() * networkCostWeight;
    }

    private static class CostEstimator extends OperatorVisitor<CostEstimate, ExpressionContext> {

        private final List<PhysicalPropertySet> inputProperties;

        private CostEstimator(List<PhysicalPropertySet> inputProperties) {
            this.inputProperties = inputProperties;
        }

        @Override
        public CostEstimate visitOperator(Operator node, ExpressionContext context) {
            return CostEstimate.zero();
        }

        private CostEstimate adjustCostForMV(ExpressionContext context) {
            Statistics mvStatistics = context.getStatistics();
            Group group = context.getGroupExpression().getGroup();
            List<Double> costs = Lists.newArrayList();
            // get the costs of all expression in this group
            for (Pair<Double, GroupExpression> pair : group.getAllBestExpressionWithCost()) {
                if (!(pair.second.getOp() instanceof PhysicalOlapScanOperator)) {
                    costs.add(pair.first);
                }
            }
            double groupMinCost = Double.MAX_VALUE;
            if (costs.size() > 0) {
                groupMinCost = Collections.min(costs);
            }
            // use row count as the adjust cost
            double adjustCost = mvStatistics.getOutputRowCount();
            return CostEstimate.of(Math.min(Math.max(groupMinCost - 1, 0), adjustCost), 0, 0);
        }

        @Override
        public CostEstimate visitPhysicalOlapScan(PhysicalOlapScanOperator node, ExpressionContext context) {
            Statistics statistics = context.getStatistics();
            Preconditions.checkNotNull(statistics);
            if (node.getTable().isMaterializedView()) {
                Statistics groupStatistics = context.getGroupStatistics();
                Statistics mvStatistics = context.getStatistics();
                // only adjust cost for mv scan operator when group statistics is unknown and mv group expression
                // statistics is not unknown
                if (groupStatistics != null && groupStatistics.getColumnStatistics().values().stream().
                        anyMatch(ColumnStatistic::isUnknown) && mvStatistics.getColumnStatistics().values().stream().
                        noneMatch(ColumnStatistic::isUnknown)) {
                    return adjustCostForMV(context);
                } else {
                    ColumnRefSet usedColumns = statistics.getUsedColumns();
                    Projection projection = node.getProjection();
                    if (projection != null) {
                        // we will add a projection on top of rewritten mv plan to keep the output columns the same as
                        // original query.
                        // excludes this projection keys when costing mv,
                        // or the cost of mv may be larger than origal query,
                        // which will lead to mismatch of mv
                        usedColumns.except(projection.getColumnRefMap().keySet());
                    }
                    // use the used columns to calculate the cost of mv
                    return CostEstimate.of(statistics.getOutputSize(usedColumns), 0, 0);
                }
            }
            return CostEstimate.of(statistics.getComputeSize(), 0, 0);
        }

        @Override
        public CostEstimate visitPhysicalHiveScan(PhysicalHiveScanOperator node, ExpressionContext context) {
            Statistics statistics = context.getStatistics();
            Preconditions.checkNotNull(statistics);

            return CostEstimate.of(statistics.getComputeSize(), statistics.getComputeSize(),
                    statistics.getComputeSize());
        }

        @Override
        public CostEstimate visitPhysicalProject(PhysicalProjectOperator node, ExpressionContext context) {
            Statistics statistics = context.getStatistics();
            Preconditions.checkNotNull(statistics);

            return CostEstimate.ofCpu(statistics.getComputeSize());
        }

        @Override
        public CostEstimate visitPhysicalTopN(PhysicalTopNOperator node, ExpressionContext context) {
            // Disable one phased sort, Currently, we always use two phase sort
            if (!node.isEnforced() && !node.isSplit()
                    && node.getSortPhase().isFinal()
                    && !context.getChildOperator(0).hasLimit()) {
                return CostEstimate.infinite();
            }

            Statistics statistics = context.getStatistics();
            Statistics inputStatistics = context.getChildStatistics(0);

            return CostEstimate.of(inputStatistics.getComputeSize(), statistics.getComputeSize(),
                    inputStatistics.getComputeSize());
        }

        boolean canGenerateOneStageAggNode(ExpressionContext context) {
            // 1. Must do two stage aggregate if child operator is LogicalRepeatOperator
            //   If the repeat node is used as the input node of the Exchange node.
            //   Will cause the node to be unable to confirm whether it is const during serialization
            //   (BE does this for efficiency reasons).
            //   Therefore, it is forcibly ensured that no one-stage aggregation nodes are generated
            //   on top of the repeat node.
            if (context.getChildOperator(0).getOpType().equals(OperatorType.LOGICAL_REPEAT)) {
                return false;
            }

            // 2. Must do multi stage aggregate when aggregate distinct function has array type
            if (context.getOp() instanceof PhysicalHashAggregateOperator) {
                PhysicalHashAggregateOperator operator = (PhysicalHashAggregateOperator) context.getOp();
                if (operator.getAggregations().values().stream().anyMatch(callOperator
                        -> callOperator.getChildren().stream().anyMatch(c -> c.getType().isArrayType()) &&
                        callOperator.isDistinct())) {
                    return false;
                }
            }

            // 3. agg distinct function with multi columns can not generate one stage aggregate
            if (context.getOp() instanceof PhysicalHashAggregateOperator) {
                PhysicalHashAggregateOperator operator = (PhysicalHashAggregateOperator) context.getOp();
                if (operator.getAggregations().values().stream().anyMatch(callOperator -> callOperator.isDistinct() &&
                        callOperator.getChildren().size() > 1)) {
                    return false;
                }
            }
            return true;
        }

        boolean mustGenerateOneStageAggNode(ExpressionContext context) {
            // Must do one stage aggregate If the child contains limit,
            // the aggregation must be a single node to ensure correctness.
            // eg. select count(*) from (select * table limit 2) t
            if (context.getChildOperator(0).hasLimit()) {
                return true;
            }
            return false;
        }

        // Note: This method logic must consistent with SplitAggregateRule::needGenerateMultiStageAggregate
        boolean needGenerateOneStageAggNode(ExpressionContext context) {
            if (!canGenerateOneStageAggNode(context)) {
                return false;
            }
            if (mustGenerateOneStageAggNode(context)) {
                return true;
            }
            // respect user hint
            int aggStage = ConnectContext.get().getSessionVariable().getNewPlannerAggStage();
            return aggStage == 1 || aggStage == 0;
        }

        @Override
        public CostEstimate visitPhysicalHashAggregate(PhysicalHashAggregateOperator node, ExpressionContext context) {
            if (!needGenerateOneStageAggNode(context) && node.getDistinctColumnDataSkew() == null && !node.isSplit() &&
                    node.getType().isGlobal()) {
                return CostEstimate.infinite();
            }

            Statistics statistics = context.getStatistics();
            Statistics inputStatistics = context.getChildStatistics(0);
            double penalty = 1.0;
            if (node.getDistinctColumnDataSkew() != null) {
                penalty = computeDataSkewPenaltyOfGroupByCountDistinct(node, inputStatistics);
            }

            return CostEstimate.of(inputStatistics.getComputeSize() * penalty, statistics.getComputeSize() * penalty,
                    0);
        }

        // Compute penalty factor for GroupByCountDistinctDataSkewEliminateRule
        // Reward good cases(give a penaltyFactor=0.5) while punish bad cases(give a penaltyFactor=1.5)
        // Good cases as follows:
        // 1. distinct cardinality of group-by column is less than 100
        // 2. distinct cardinality of group-by column is less than 10000 and avgDistValuesPerGroup > 100
        // Bad cases as follows: this Rule is conservative, the cases except good cases are all bad cases.
        private double computeDataSkewPenaltyOfGroupByCountDistinct(PhysicalHashAggregateOperator node,
                                                                    Statistics inputStatistics) {
            DataSkewInfo skewInfo = node.getDistinctColumnDataSkew();
            if (skewInfo.getStage() != 1) {
                return skewInfo.getPenaltyFactor();
            }

            if (inputStatistics.isTableRowCountMayInaccurate()) {
                return 1.5;
            }

            ColumnStatistic distColStat = inputStatistics.getColumnStatistic(skewInfo.getSkewColumnRef());
            List<ColumnStatistic> groupByStats = node.getGroupBys().subList(0, node.getGroupBys().size() - 1)
                    .stream().map(inputStatistics::getColumnStatistic).collect(Collectors.toList());

            if (distColStat.isUnknownValue() || distColStat.isUnknown() ||
                    groupByStats.stream().anyMatch(groupStat -> groupStat.isUnknown() || groupStat.isUnknownValue())) {
                return 1.5;
            }
            double groupByColDistinctValues = 1.0;
            for (ColumnStatistic groupStat : groupByStats) {
                groupByColDistinctValues *= groupStat.getDistinctValuesCount();
            }
            groupByColDistinctValues =
                    Math.max(1.0, Math.min(groupByColDistinctValues, inputStatistics.getOutputRowCount()));

            final double groupByColDistinctHighWaterMark = 10000;
            final double groupByColDistinctLowWaterMark = 100;
            final double distColDistinctValuesCountWaterMark = 10000000;
            final double distColDistinctValuesCount = distColStat.getDistinctValuesCount();
            final double avgDistValuesPerGroup = distColDistinctValuesCount / groupByColDistinctValues;

            if (distColDistinctValuesCount > distColDistinctValuesCountWaterMark &&
                    ((groupByColDistinctValues <= groupByColDistinctLowWaterMark) ||
                            (groupByColDistinctValues < groupByColDistinctHighWaterMark &&
                                    avgDistValuesPerGroup > 100))) {
                return 0.5;
            } else {
                return 1.5;
            }
        }

        @Override
        public CostEstimate visitPhysicalDistribution(PhysicalDistributionOperator node, ExpressionContext context) {
            ColumnRefSet outputColumns = context.getChildOutputColumns(0);

            Statistics statistics = context.getStatistics();
            Preconditions.checkNotNull(statistics);

            CostEstimate result;
            ConnectContext ctx = ConnectContext.get();
            SessionVariable sessionVariable = ctx.getSessionVariable();
            DistributionSpec distributionSpec = node.getDistributionSpec();
            double outputSize = statistics.getOutputSize(outputColumns);
            double penalty = 1.0;
            Operator childOp = context.getChildOperator(0);
            if (childOp instanceof PhysicalHashAggregateOperator) {
                PhysicalHashAggregateOperator childAggOp = (PhysicalHashAggregateOperator) childOp;
                DataSkewInfo skewInfo = childAggOp.getDistinctColumnDataSkew();
                if (skewInfo != null && skewInfo.getStage() == 3) {
                    penalty = skewInfo.getPenaltyFactor();
                }
            }
            // set network start cost 1 at least
            // avoid choose network plan when the cost is same as colocate plans
            switch (distributionSpec.getType()) {
                case ANY:
                    result = CostEstimate.ofCpu(outputSize);
                    break;
                case BROADCAST:
                    // beNum is the number of right table should broadcast, now use alive backends
                    int aliveBackendNumber = ctx.getAliveBackendNumber();
                    int beNum = Math.max(1, aliveBackendNumber);

                    result = CostEstimate.of(outputSize * aliveBackendNumber,
                            outputSize * beNum,
                            Math.max(outputSize * beNum, 1));
                    if (outputSize > sessionVariable.getMaxExecMemByte()) {
                        result = result.multiplyBy(StatisticsEstimateCoefficient.BROADCAST_JOIN_MEM_EXCEED_PENALTY);
                    }
                    LOG.debug("beNum: {}, aliveBeNum: {}, outputSize: {}.", aliveBackendNumber, beNum, outputSize);
                    break;
                case SHUFFLE:
                    // This is used to generate "ScanNode->LocalShuffle->OnePhaseLocalAgg" for the single backend,
                    // which contains two steps:
                    // 1. Ignore the network cost for ExchangeNode when estimating cost model.
                    // 2. Remove ExchangeNode between AggNode and ScanNode when building fragments.
                    boolean ignoreNetworkCost = sessionVariable.isEnableLocalShuffleAgg()
                            && sessionVariable.isEnablePipelineEngine()
                            && GlobalStateMgr.getCurrentSystemInfo().isSingleBackendAndComputeNode();
                    double networkCost = ignoreNetworkCost ? 0 : Math.max(outputSize, 1);

                    result = CostEstimate.of(outputSize * penalty, 0, networkCost * penalty);
                    break;
                case GATHER:
                    result = CostEstimate.of(outputSize, 0,
                            Math.max(statistics.getOutputSize(outputColumns), 1));
                    break;
                default:
                    throw new StarRocksPlannerException(
                            "not support " + distributionSpec.getType() + "distribution type",
                            ErrorType.UNSUPPORTED);
            }
            LOG.debug("distribution type {}, cost: {}.", distributionSpec.getType(), result);
            return result;
        }

        @Override
        public CostEstimate visitPhysicalHashJoin(PhysicalHashJoinOperator join, ExpressionContext context) {
            Preconditions.checkState(context.arity() == 2);
            Statistics statistics = context.getStatistics();
            Preconditions.checkNotNull(statistics);

            Statistics leftStatistics = context.getChildStatistics(0);
            Statistics rightStatistics = context.getChildStatistics(1);

            List<BinaryPredicateOperator> eqOnPredicates =
                    JoinHelper.getEqualsPredicate(leftStatistics.getUsedColumns(),
                            rightStatistics.getUsedColumns(),
                            Utils.extractConjuncts(join.getOnPredicate()));

            Preconditions.checkState(!(join.getJoinType().isCrossJoin() || eqOnPredicates.isEmpty()),
                    "should be handled by nestloopjoin");
            HashJoinCostModel joinCostModel = new HashJoinCostModel(context, inputProperties, eqOnPredicates, statistics);
            return CostEstimate.of(joinCostModel.getCpuCost(), joinCostModel.getMemCost(), 0);
        }

        @Override
        public CostEstimate visitPhysicalMergeJoin(PhysicalMergeJoinOperator join, ExpressionContext context) {
            Preconditions.checkState(context.arity() == 2);

            Statistics statistics = context.getStatistics();
            Preconditions.checkNotNull(statistics);

            Statistics leftStatistics = context.getChildStatistics(0);
            Statistics rightStatistics = context.getChildStatistics(1);

            List<BinaryPredicateOperator> eqOnPredicates =
                    JoinHelper.getEqualsPredicate(leftStatistics.getUsedColumns(), rightStatistics.getUsedColumns(),
                            Utils.extractConjuncts(join.getOnPredicate()));
            if (join.getJoinType().isCrossJoin() || eqOnPredicates.isEmpty()) {
                return CostEstimate.of(leftStatistics.getOutputSize(context.getChildOutputColumns(0))
                                + rightStatistics.getOutputSize(context.getChildOutputColumns(1)),
                        rightStatistics.getOutputSize(context.getChildOutputColumns(1))
                                * StatisticsEstimateCoefficient.CROSS_JOIN_COST_PENALTY * 100D, 0);
            } else {
                return CostEstimate.of((leftStatistics.getOutputSize(context.getChildOutputColumns(0))
                                + rightStatistics.getOutputSize(context.getChildOutputColumns(1)) / 2),
                        0, 0);

            }
        }

        @Override
        public CostEstimate visitPhysicalNestLoopJoin(PhysicalNestLoopJoinOperator join, ExpressionContext context) {
            Statistics leftStatistics = context.getChildStatistics(0);
            Statistics rightStatistics = context.getChildStatistics(1);

            double leftSize = leftStatistics.getOutputSize(context.getChildOutputColumns(0));
            double rightSize = rightStatistics.getOutputSize(context.getChildOutputColumns(1));
            double cpuCost = StatisticUtils.multiplyOutputSize(StatisticUtils.multiplyOutputSize(leftSize, rightSize),
                    StatisticsEstimateCoefficient.CROSS_JOIN_COST_PENALTY);
            double memCost = StatisticUtils.multiplyOutputSize(rightSize,
                    StatisticsEstimateCoefficient.CROSS_JOIN_COST_PENALTY * 100D);

            // Right cross join could not be parallelized, so apply more punishment
            if (join.getJoinType().isRightJoin()) {
                // Add more punishment when right size is 10x greater than left size.
                if (rightSize > 10 * leftSize) {
                    cpuCost *= StatisticsEstimateCoefficient.CROSS_JOIN_RIGHT_COST_PENALTY;
                } else {
                    cpuCost += StatisticsEstimateCoefficient.CROSS_JOIN_RIGHT_COST_PENALTY;
                }
                memCost += rightSize;
            }
            if (join.getJoinType().isOuterJoin() || join.getJoinType().isSemiJoin() ||
                    join.getJoinType().isAntiJoin()) {
                cpuCost += leftSize;
            }

            return CostEstimate.of(cpuCost, memCost, 0);
        }

        @Override
        public CostEstimate visitPhysicalAssertOneRow(PhysicalAssertOneRowOperator node, ExpressionContext context) {
            return CostEstimate.zero();
        }

        @Override
        public CostEstimate visitPhysicalAnalytic(PhysicalWindowOperator node, ExpressionContext context) {
            Statistics statistics = context.getStatistics();
            Preconditions.checkNotNull(statistics);

            return CostEstimate.ofCpu(statistics.getComputeSize());
        }

        @Override
        public CostEstimate visitPhysicalCTEProduce(PhysicalCTEProduceOperator node, ExpressionContext context) {
            return CostEstimate.zero();
        }

        @Override
        public CostEstimate visitPhysicalCTEAnchor(PhysicalCTEAnchorOperator node, ExpressionContext context) {
            // memory cost
            Statistics cteStatistics = context.getChildStatistics(0);
            double ratio = ConnectContext.get().getSessionVariable().getCboCTERuseRatio();
            double produceSize = cteStatistics.getOutputSize(context.getChildOutputColumns(0));
            return CostEstimate.of(produceSize * node.getConsumeNum() * 0.5, produceSize * (1 + ratio), 0);
        }

        @Override
        public CostEstimate visitPhysicalCTEConsume(PhysicalCTEConsumeOperator node, ExpressionContext context) {
            return CostEstimate.zero();
        }

        @Override
        public CostEstimate visitPhysicalNoCTE(PhysicalNoCTEOperator node, ExpressionContext context) {
            return CostEstimate.zero();
        }
    }
}
