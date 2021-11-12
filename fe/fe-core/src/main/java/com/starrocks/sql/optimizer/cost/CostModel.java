// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.cost;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.Catalog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.GroupExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalAssertOneRowOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProjectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalWindowOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.transformation.JoinPredicateUtils;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.statistic.Constants;

import java.util.List;
import java.util.Map;

public class CostModel {
    public static double calculateCost(GroupExpression expression) {
        ExpressionContext expressionContext = new ExpressionContext(expression);
        return calculateCost(expressionContext);
    }

    private static double calculateCost(ExpressionContext expressionContext) {
        CostEstimator costEstimator = new CostEstimator();
        CostEstimate costEstimate = expressionContext.getOp().accept(costEstimator, expressionContext);
        return getRealCost(costEstimate);
    }

    public static CostEstimate calculateCostEstimate(ExpressionContext expressionContext) {
        CostEstimator costEstimator = new CostEstimator();
        return expressionContext.getOp().accept(costEstimator, expressionContext);
    }

    public static double getRealCost(CostEstimate costEstimate) {
        double cpuCostWeight = 0.5;
        double memoryCostWeight = 2;
        double networkCostWeight = 1.5;
        return costEstimate.getCpuCost() * cpuCostWeight +
                costEstimate.getMemoryCost() * memoryCostWeight +
                costEstimate.getNetworkCost() * networkCostWeight;
    }

    public static class CostEstimator extends OperatorVisitor<CostEstimate, ExpressionContext> {
        @Override
        public CostEstimate visitOperator(Operator node, ExpressionContext context) {
            return CostEstimate.zero();
        }

        @Override
        public CostEstimate visitPhysicalOlapScan(PhysicalOlapScanOperator node, ExpressionContext context) {
            Statistics statistics = context.getStatistics();
            Preconditions.checkNotNull(statistics);

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
                    && !((LogicalOperator) context.getChildOperator(0)).hasLimit()) {
                return CostEstimate.infinite();
            }

            Statistics statistics = context.getStatistics();
            Statistics inputStatistics = context.getChildStatistics(0);

            return CostEstimate.of(inputStatistics.getComputeSize(), statistics.getComputeSize(),
                    inputStatistics.getComputeSize());
        }

        // Note: This method logic must consistent with SplitAggregateRule::canGenerateTwoStageAggregate
        boolean canGenerateOneStageAggNode(ExpressionContext context) {
            // 1 Must do two stage aggregate if child operator is LogicalRepeatOperator
            //   If the repeat node is used as the input node of the Exchange node.
            //   Will cause the node to be unable to confirm whether it is const during serialization
            //   (BE does this for efficiency reasons).
            //   Therefore, it is forcibly ensured that no one-stage aggregation nodes are generated
            //   on top of the repeat node.
            if (context.getChildOperator(0).getOpType().equals(OperatorType.LOGICAL_REPEAT)) {
                return false;
            }

            // 2 Must do two stage aggregate is aggregate function has array type
            if (context.getOp() instanceof PhysicalHashAggregateOperator) {
                PhysicalHashAggregateOperator operator = (PhysicalHashAggregateOperator) context.getOp();
                if (operator.getAggregations().values().stream().anyMatch(callOperator
                        -> callOperator.getChildren().stream().anyMatch(c -> c.getType().isArrayType()))) {
                    return false;
                }
            }

            // 3 Must do one stage aggregate If the child contains limit,
            // the aggregation must be a single node to ensure correctness.
            // eg. select count(*) from (select * table limit 2) t
            if (context.getChildOperator(0).hasLimit()) {
                return true;
            }

            // 4. agg distinct function with multi columns can not generate one stage aggregate
            if (context.getOp() instanceof PhysicalHashAggregateOperator) {
                PhysicalHashAggregateOperator operator = (PhysicalHashAggregateOperator) context.getOp();
                if (operator.getAggregations().values().stream().anyMatch(callOperator -> callOperator.isDistinct() &&
                        callOperator.getChildren().size() > 1)) {
                    return false;
                }
            }

            int aggStage = ConnectContext.get().getSessionVariable().getNewPlannerAggStage();
            return aggStage == 1 || aggStage == 0;
        }

        public boolean isDistinctAggFun(CallOperator aggOperator, PhysicalHashAggregateOperator node) {
            if (aggOperator.getFnName().equalsIgnoreCase("MULTI_DISTINCT_COUNT") ||
                    aggOperator.getFnName().equalsIgnoreCase("MULTI_DISTINCT_SUM")) {
                return true;
            }
            // only one stage agg node has not rewrite distinct function here
            return node.getType().isGlobal() && !node.isSplit() &&
                    (aggOperator.getFnName().equalsIgnoreCase("COUNT") ||
                            aggOperator.getFnName().equalsIgnoreCase("SUM")) &&
                    aggOperator.isDistinct();
        }

        // some agg function has extra cost, we need compute here
        // eg. MULTI_DISTINCT_COUNT function needs compute extra memory cost
        public CostEstimate computeAggFunExtraCost(PhysicalHashAggregateOperator node, Statistics statistics,
                                                   Statistics inputStatistics) {
            CostEstimate costEstimate = CostEstimate.zero();
            for (Map.Entry<ColumnRefOperator, CallOperator> entry : node.getAggregations().entrySet()) {
                CallOperator aggregation = entry.getValue();
                if (isDistinctAggFun(aggregation, node)) {
                    Preconditions.checkState(aggregation.getChildren().size() >= 1);
                    ColumnStatistic distinctColumnStats;
                    // only compute column extra costs
                    if (!(aggregation.getChild(0).isColumnRef())) {
                        continue;
                    }

                    ColumnRefOperator distinctColumn = (ColumnRefOperator) aggregation.getChild(0);
                    distinctColumnStats = inputStatistics.getColumnStatistic(distinctColumn);
                    // use output row count as bucket
                    double buckets = statistics.getOutputRowCount();
                    double rowSize = distinctColumnStats.getAverageRowSize();
                    // In second phase of aggregation, do not compute extra row size costs
                    if (distinctColumn.getType().isStringType() && !(node.getType().isGlobal() && node.isSplit())) {
                        rowSize = rowSize + 16;
                    }
                    // To avoid OOM
                    if (buckets >= 15000000 && rowSize >= 20) {
                        return CostEstimate.infinite();
                    }

                    double hashSetSize;
                    if (distinctColumnStats.isUnknown()) {
                        hashSetSize = rowSize * inputStatistics.getOutputRowCount() / statistics.getOutputRowCount();
                    } else {
                        // we need to estimate the distinct values in each bucket because of do not know the
                        // correlation between the Group BY column and the DISTINCT column.
                        // There are estimated to (DistinctValuesCount / buckets * 2) entries distinct values in each bucket
                        // except when bucket number equals 1.
                        double distinctValuesPerBucket = buckets == 1 ? distinctColumnStats.getDistinctValuesCount() :
                                distinctColumnStats.getDistinctValuesCount() / buckets * 2;
                        // 40 bytes is the state cost of hashset
                        hashSetSize = rowSize * distinctValuesPerBucket + 40;
                    }
                    costEstimate = CostEstimate.addCost(costEstimate, CostEstimate.ofMemory(buckets * hashSetSize));
                }
            }
            return costEstimate;
        }

        @Override
        public CostEstimate visitPhysicalHashAggregate(PhysicalHashAggregateOperator node, ExpressionContext context) {
            if (!canGenerateOneStageAggNode(context) && !node.isSplit() && node.getType().isGlobal()) {
                return CostEstimate.infinite();
            }

            Statistics statistics = context.getStatistics();
            Statistics inputStatistics = context.getChildStatistics(0);
            CostEstimate otherExtraCost = computeAggFunExtraCost(node, statistics, inputStatistics);
            return CostEstimate.addCost(CostEstimate.of(inputStatistics.getComputeSize(),
                    CostEstimate.isZero(otherExtraCost) ? statistics.getComputeSize() : 0, 0),
                    otherExtraCost);
        }

        @Override
        public CostEstimate visitPhysicalDistribution(PhysicalDistributionOperator node, ExpressionContext context) {
            ColumnRefSet outputColumns = context.getChildOutputColumns(0);

            Statistics statistics = context.getStatistics();
            Preconditions.checkNotNull(statistics);

            CostEstimate result;
            DistributionSpec distributionSpec = node.getDistributionSpec();
            switch (distributionSpec.getType()) {
                case ANY:
                    result = CostEstimate.ofCpu(statistics.getOutputSize(outputColumns));
                    break;
                case BROADCAST:
                    if (statistics.getOutputSize(outputColumns) >
                            ConnectContext.get().getSessionVariable().getMaxExecMemByte()) {
                        return CostEstimate.infinite();
                    }
                    int parallelExecInstanceNum = Math.max(1, getParallelExecInstanceNum(context));
                    // beNum is the number of right table should broadcast, now use alive backends
                    int beNum = Math.max(1, Catalog.getCurrentSystemInfo().getBackendIds(true).size());
                    result = CostEstimate
                            .of(statistics.getOutputSize(outputColumns) *
                                            Catalog.getCurrentSystemInfo().getBackendIds(true).size(),
                                    statistics.getOutputSize(outputColumns) * beNum * parallelExecInstanceNum,
                                    statistics.getOutputSize(outputColumns) * beNum * parallelExecInstanceNum);
                    break;
                case SHUFFLE:
                case GATHER:
                    result = CostEstimate.of(statistics.getOutputSize(outputColumns), 0,
                            statistics.getOutputSize(outputColumns));
                    break;
                default:
                    throw new StarRocksPlannerException(
                            "not support " + distributionSpec.getType() + "distribution type",
                            ErrorType.UNSUPPORTED);
            }
            return result;
        }

        private int getParallelExecInstanceNum(ExpressionContext context) {
            return Math.min(ConnectContext.get().getSessionVariable().getParallelExecInstanceNum(),
                    context.getRootProperty().getLeftMostScanTabletsNum());
        }

        @Override
        public CostEstimate visitPhysicalHashJoin(PhysicalHashJoinOperator join, ExpressionContext context) {
            Preconditions.checkState(context.arity() == 2);
            // For broadcast join, use leftExecInstanceNum as right child real destinations num.
            int leftExecInstanceNum = context.getChildLeftMostScanTabletsNum(0);
            context.getChildLogicalProperty(1).setLeftMostScanTabletsNum(leftExecInstanceNum);

            Statistics statistics = context.getStatistics();
            Preconditions.checkNotNull(statistics);

            Statistics leftStatistics = context.getChildStatistics(0);
            Statistics rightStatistics = context.getChildStatistics(1);

            List<BinaryPredicateOperator> eqOnPredicates = JoinPredicateUtils.getEqConj(leftStatistics.getUsedColumns(),
                    rightStatistics.getUsedColumns(),
                    Utils.extractConjuncts(join.getJoinPredicate()));

            if (join.getJoinType().isCrossJoin() || eqOnPredicates.isEmpty()) {
                return CostEstimate.of(leftStatistics.getOutputSize(context.getChildOutputColumns(0))
                                + rightStatistics.getOutputSize(context.getChildOutputColumns(1)),
                        rightStatistics.getOutputSize(context.getChildOutputColumns(1))
                                * Constants.CrossJoinCostPenalty, 0);
            } else {
                return CostEstimate.of(leftStatistics.getOutputSize(context.getChildOutputColumns(0))
                                + rightStatistics.getOutputSize(context.getChildOutputColumns(1)),
                        rightStatistics.getOutputSize(context.getChildOutputColumns(1)), 0);
            }
        }

        @Override
        public CostEstimate visitPhysicalAssertOneRow(PhysicalAssertOneRowOperator node, ExpressionContext context) {
            //TODO: Add cost estimate
            return CostEstimate.zero();
        }

        @Override
        public CostEstimate visitPhysicalAnalytic(PhysicalWindowOperator node, ExpressionContext context) {
            Statistics statistics = context.getStatistics();
            Preconditions.checkNotNull(statistics);

            return CostEstimate.ofCpu(statistics.getComputeSize());
        }
    }
}
