// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.cost;

import com.google.common.base.Preconditions;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.GroupExpression;
import com.starrocks.sql.optimizer.JoinHelper;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
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
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.statistic.StatsConstants;

import java.util.List;

public class CostModel {
    public static double calculateCost(GroupExpression expression) {
        ExpressionContext expressionContext = new ExpressionContext(expression);
        return calculateCost(expressionContext);
    }

    private static double calculateCost(ExpressionContext expressionContext) {
        CostEstimator costEstimator = new CostEstimator();
        CostEstimate costEstimate = expressionContext.getOp().accept(costEstimator, expressionContext);
        if (expressionContext.groupExpression.getOp() instanceof PhysicalHashJoinOperator) {
            PhysicalHashJoinOperator op = expressionContext.groupExpression.getOp().cast();
            System.out.printf("eqPredicate=%s, cost=%s\n", op.getOnPredicate(), costEstimate);
        }
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

    public static int getParallelExecInstanceNum(int leftMostScanTabletsNum) {
        if (ConnectContext.get().getSessionVariable().isEnablePipelineEngine()) {
            return 1;
        }
        return Math.min(ConnectContext.get().getSessionVariable().getDegreeOfParallelism(), leftMostScanTabletsNum);
    }

    private static class CostEstimator extends OperatorVisitor<CostEstimate, ExpressionContext> {
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
            if (!needGenerateOneStageAggNode(context) && !node.isSplit() && node.getType().isGlobal()) {
                return CostEstimate.infinite();
            }

            Statistics statistics = context.getStatistics();
            Statistics inputStatistics = context.getChildStatistics(0);
            return CostEstimate.of(inputStatistics.getComputeSize(), statistics.getComputeSize(), 0);
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
            // set network start cost 1 at least
            // avoid choose network plan when the cost is same as colocate plans
            switch (distributionSpec.getType()) {
                case ANY:
                    result = CostEstimate.ofCpu(statistics.getOutputSize(outputColumns));
                    break;
                case BROADCAST:
                    int parallelExecInstanceNum = getParallelExecInstanceNum(
                            context.getRootProperty().getLeftMostScanTabletsNum());
                    // beNum is the number of right table should broadcast, now use alive backends
                    int aliveBackendNumber = ctx.getAliveBackendNumber();
                    int beNum = Math.max(1, aliveBackendNumber);
                    result = CostEstimate.of(statistics.getOutputSize(outputColumns) * aliveBackendNumber,
                            statistics.getOutputSize(outputColumns) * beNum * parallelExecInstanceNum,
                            Math.max(statistics.getOutputSize(outputColumns) * beNum * parallelExecInstanceNum, 1));
                    if (statistics.getOutputSize(outputColumns) > sessionVariable.getMaxExecMemByte()) {
                        return CostEstimate.of(result.getCpuCost() * StatsConstants.BROADCAST_JOIN_MEM_EXCEED_PENALTY,
                                result.getMemoryCost() * StatsConstants.BROADCAST_JOIN_MEM_EXCEED_PENALTY,
                                result.getNetworkCost() * StatsConstants.BROADCAST_JOIN_MEM_EXCEED_PENALTY);
                    }
                    break;
                case SHUFFLE:
                    // This is used to generate "ScanNode->LocalShuffle->OnePhaseLocalAgg" for the single backend,
                    // which contains two steps:
                    // 1. Ignore the network cost for ExchangeNode when estimating cost model.
                    // 2. Remove ExchangeNode between AggNode and ScanNode when building fragments.
                    boolean ignoreNetworkCost = sessionVariable.isEnableLocalShuffleAgg()
                            && sessionVariable.isEnablePipelineEngine()
                            && GlobalStateMgr.getCurrentSystemInfo().isSingleBackendAndComputeNode();
                    double networkCost = ignoreNetworkCost ? 0 : Math.max(statistics.getOutputSize(outputColumns), 1);

                    result = CostEstimate.of(statistics.getOutputSize(outputColumns), 0, networkCost);
                    break;
                case GATHER:
                    result = CostEstimate.of(statistics.getOutputSize(outputColumns), 0,
                            Math.max(statistics.getOutputSize(outputColumns), 1));
                    break;
                default:
                    throw new StarRocksPlannerException(
                            "not support " + distributionSpec.getType() + "distribution type",
                            ErrorType.UNSUPPORTED);
            }
            return result;
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

            List<BinaryPredicateOperator> eqOnPredicates =
                    JoinHelper.getEqualsPredicate(leftStatistics.getUsedColumns(),
                            rightStatistics.getUsedColumns(),
                            Utils.extractConjuncts(join.getOnPredicate()));

            if (join.getJoinType().isCrossJoin() || eqOnPredicates.isEmpty()) {
                return CostEstimate.of(leftStatistics.getOutputSize(context.getChildOutputColumns(0))
                                + rightStatistics.getOutputSize(context.getChildOutputColumns(1)),
                        rightStatistics.getOutputSize(context.getChildOutputColumns(1))
                                * StatsConstants.CROSS_JOIN_COST_PENALTY, 0);
            } else {
                return CostEstimate.of(leftStatistics.getOutputSize(context.getChildOutputColumns(0))
                                + rightStatistics.getOutputSize(context.getChildOutputColumns(1)),
                        rightStatistics.getOutputSize(context.getChildOutputColumns(1)), 0);
            }
        }

        @Override
        public CostEstimate visitPhysicalMergeJoin(PhysicalMergeJoinOperator join, ExpressionContext context) {
            Preconditions.checkState(context.arity() == 2);
            // For broadcast join, use leftExecInstanceNum as right child real destinations num.
            int leftExecInstanceNum = context.getChildLeftMostScanTabletsNum(0);
            context.getChildLogicalProperty(1).setLeftMostScanTabletsNum(leftExecInstanceNum);

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
                                * StatsConstants.CROSS_JOIN_COST_PENALTY * 2, 0);
            } else {
                return CostEstimate.of((leftStatistics.getOutputSize(context.getChildOutputColumns(0))
                                + rightStatistics.getOutputSize(context.getChildOutputColumns(1)) / 2),
                        0, 0);

            }
        }

        @Override
        public CostEstimate visitPhysicalNestLoopJoin(PhysicalNestLoopJoinOperator join, ExpressionContext context) {
            final double nestLoopPunishment = 1000000.0;
            Statistics leftStatistics = context.getChildStatistics(0);
            Statistics rightStatistics = context.getChildStatistics(1);

            double leftSize = leftStatistics.getOutputSize(context.getChildOutputColumns(0));
            double rightSize = rightStatistics.getOutputSize(context.getChildOutputColumns(1));
            return CostEstimate.of(leftSize * rightSize + nestLoopPunishment,
                    rightSize * StatsConstants.CROSS_JOIN_COST_PENALTY * 2, 0);
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
