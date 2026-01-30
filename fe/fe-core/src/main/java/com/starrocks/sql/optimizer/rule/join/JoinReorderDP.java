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

package com.starrocks.sql.optimizer.rule.join;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.statistics.StatisticsCalculator;
import com.starrocks.sql.optimizer.statistics.StatisticsEstimateCoefficient;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class JoinReorderDP extends JoinOrder {
    public JoinReorderDP(OptimizerContext context) {
        super(context);
    }

    @Override
    protected void calculateStatistics(OptExpression expr) {
        if (StatisticsCalculator.isInSkipPredicateColumnsCollectionScope()) {
            super.calculateStatistics(expr);
            return;
        }
        try (var ignore = StatisticsCalculator.skipPredicateColumnsCollectionScope()) {
            super.calculateStatistics(expr);
        }
    }

    private final Map<BitSet, GroupInfo> bestPlanMemo = new HashMap<>();
    List<GroupInfo> groups = new ArrayList<>();

    @Override
    protected void enumerate() {
        groups = joinLevels.get(1).groups;
        BitSet joinKeys = new BitSet();
        joinKeys.set(0, groups.size(), true);
        getBestExpr(joinKeys);
    }

    @Override
    public List<OptExpression> getResult() {
        groups = joinLevels.get(1).groups;
        BitSet joinKeys = new BitSet();
        joinKeys.set(0, groups.size(), true);

        GroupInfo g = getBestExpr(joinKeys);
        return Lists.newArrayList(g.bestExprInfo.expr);
    }

    private GroupInfo getBestExpr(BitSet joinKeys) {
        if (joinKeys.cardinality() == 1) {
            int index = joinKeys.nextSetBit(0);
            return groups.get(index);
        }

        GroupInfo bestPlan = bestPlanMemo.get(joinKeys);
        if (bestPlan == null) {
            double bestCostSoFar = Double.MAX_VALUE;
            ExpressionInfo bestExprInfo = null;
            List<BitSet> partitions = generatePartitions(joinKeys);
            for (BitSet partition : partitions) {
                GroupInfo leftGroup = getBestExpr(partition);
                if (bestExprInfo != null && leftGroup.bestExprInfo.cost > bestCostSoFar) {
                    continue;
                }

                BitSet otherPartition = (BitSet) joinKeys.clone();
                otherPartition.andNot(partition);

                GroupInfo rightGroup = getBestExpr(otherPartition);
                if (bestExprInfo != null && rightGroup.bestExprInfo.cost > bestCostSoFar) {
                    continue;
                }

                double childLowerBound = saturatingAdd(leftGroup.bestExprInfo.cost, rightGroup.bestExprInfo.cost);
                if (childLowerBound > bestCostSoFar) {
                    continue;
                }

                Optional<ExpressionInfo> joinExpr = buildJoinExpr(leftGroup, rightGroup);
                if (joinExpr.isEmpty())  {
                    continue;
                }

                double joinLowerBound = childLowerBound;
                LogicalJoinOperator joinOp = joinExpr.get().expr.getOp().cast();
                if (joinOp.getJoinType().isCrossJoin()) {
                    long penalty = ConnectContext.get().getSessionVariable().getCrossJoinCostPenalty();
                    if (joinOp.getOnPredicate() == null && joinOp.getPredicate() == null) {
                        double crossRows = saturatingMul(leftGroup.bestExprInfo.rowCount, rightGroup.bestExprInfo.rowCount);
                        joinLowerBound = saturatingMul(saturatingAdd(joinLowerBound, crossRows), penalty);
                    } else {
                        joinLowerBound = saturatingMul(joinLowerBound, penalty);
                    }
                } else if (!existsEqOnPredicate(joinExpr.get().expr)) {
                    joinLowerBound = saturatingMul(joinLowerBound, StatisticsEstimateCoefficient.EXECUTE_COST_PENALTY);
                }
                if (joinLowerBound > bestCostSoFar) {
                    continue;
                }

                joinExpr.get().expr.deriveLogicalPropertyItself();
                calculateStatistics(joinExpr.get().expr);
                computeCost(joinExpr.get());
                if (joinExpr.get().cost < bestCostSoFar) {
                    bestCostSoFar = joinExpr.get().cost;
                    bestExprInfo = joinExpr.get();
                }
            }
            Preconditions.checkState(bestExprInfo != null);

            BitSet atoms = new BitSet();
            atoms.or(bestExprInfo.leftChildExpr.atoms);
            atoms.or(bestExprInfo.rightChildExpr.atoms);
            GroupInfo g = new GroupInfo(atoms);
            g.bestExprInfo = bestExprInfo;
            g.lowestExprCost = bestExprInfo.cost;

            bestPlanMemo.put(joinKeys, g);
            return g;
        }

        return bestPlan;
    }

    public static List<BitSet> generatePartitions(BitSet totalNodes) {
        int first = totalNodes.nextSetBit(0);
        if (first < 0 || totalNodes.cardinality() <= 1) {
            return List.of();
        }

        int card = totalNodes.cardinality();
        int[] rest = new int[card - 1];
        int n = 0;
        for (int b = totalNodes.nextSetBit(first + 1); b >= 0; b = totalNodes.nextSetBit(b + 1)) {
            rest[n++] = b;
        }
        if (n == 0) {
            return List.of();
        }

        long count = (1L << n) - 1;
        List<BitSet> partitions = count <= Integer.MAX_VALUE ? new ArrayList<>((int) count) : new ArrayList<>();
        for (long mask = count - 1; mask >= 0; mask--) {
            BitSet p = new BitSet();
            p.set(first);
            for (int i = 0; i < n; i++) {
                if ((mask & (1L << i)) != 0) {
                    p.set(rest[i]);
                }
            }
            partitions.add(p);
        }
        return partitions;
    }
}