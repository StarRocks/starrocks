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

import com.google.common.collect.Lists;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.JoinHelper;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
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

    private final Map<BitSet, GroupInfo> bestPlanMemo = new HashMap<>();
    List<GroupInfo> groups = new ArrayList<>();

    // A lightweight check used for pruning before expensive statistics estimation.
    // Equivalent to JoinOrder.existsEqOnPredicate(), but scoped to DP reorder.
    private boolean existsEqOnPredicate(OptExpression optExpression) {
        LogicalJoinOperator joinOp = optExpression.getOp().cast();
        List<ScalarOperator> onPredicates = Utils.extractConjuncts(joinOp.getOnPredicate());

        ColumnRefSet leftChildColumns = optExpression.inputAt(0).getOutputColumns();
        ColumnRefSet rightChildColumns = optExpression.inputAt(1).getOutputColumns();

        List<BinaryPredicateOperator> eqOnPredicates = JoinHelper.getEqualsPredicate(
                leftChildColumns, rightChildColumns, onPredicates);
        return !eqOnPredicates.isEmpty();
    }

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
                if (leftGroup.bestExprInfo.cost > bestCostSoFar) {
                    continue;
                }

                BitSet otherPartition = (BitSet) joinKeys.clone();
                otherPartition.andNot(partition);

                GroupInfo rightGroup = getBestExpr(otherPartition);
                if (rightGroup.bestExprInfo.cost > bestCostSoFar) {
                    continue;
                }

                // A safe lower bound before building join & estimating stats.
                // Join cost is monotonic with respect to children costs (with saturation), and penalties are >= 1.
                double childLowerBound = saturatingAdd(leftGroup.bestExprInfo.cost, rightGroup.bestExprInfo.cost);
                if (childLowerBound > bestCostSoFar) {
                    continue;
                }

                Optional<ExpressionInfo> joinExpr = buildJoinExpr(leftGroup, rightGroup);
                if (joinExpr.isEmpty()) {
                    continue;
                }

                // More pruning before stats estimation (DP_CALC). This targets expensive candidates like CROSS JOIN
                // or joins that will be penalized as nestloop due to missing equi predicates.
                double joinLowerBound = childLowerBound;
                LogicalJoinOperator joinOp = joinExpr.get().expr.getOp().cast();
                if (joinOp.getJoinType().isCrossJoin()) {
                    long penalty = ConnectContext.get().getSessionVariable().getCrossJoinCostPenalty();
                    joinLowerBound = saturatingMul(joinLowerBound, penalty);
                    if (joinOp.getOnPredicate() == null && joinOp.getPredicate() == null) {
                        double crossRows = saturatingMul(leftGroup.bestExprInfo.rowCount, rightGroup.bestExprInfo.rowCount);
                        joinLowerBound = saturatingMul(saturatingAdd(childLowerBound, crossRows), penalty);
                    }
                } else if (!existsEqOnPredicate(joinExpr.get().expr)) {
                    joinLowerBound = saturatingMul(joinLowerBound, StatisticsEstimateCoefficient.EXECUTE_COST_PENALTY);
                }
                if (joinLowerBound > bestCostSoFar) {
                    continue;
                }

                joinExpr.get().expr.deriveLogicalPropertyItself();
                try (Timer ignore = Tracers.watchScope(Tracers.Module.OPTIMIZER, "DP_CALC")) {
                    try (AutoCloseable ignored = StatisticsCalculator.skipPredicateColumnsCollectionScope()) {
                        calculateStatistics(joinExpr.get().expr);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                computeCost(joinExpr.get());
                if (joinExpr.get().cost < bestCostSoFar) {
                    bestCostSoFar = joinExpr.get().cost;
                    bestExprInfo = joinExpr.get();
                }
            }
            if (bestExprInfo == null) {
                throw new IllegalStateException("DP join reorder failed to build any join expression, atoms=" +
                        joinKeys.cardinality());
            }

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

    private static double saturatingAdd(double a, double b) {
        double max = StatisticsEstimateCoefficient.MAXIMUM_COST;
        if (a >= max || b >= max) {
            return max;
        }
        return a > (max - b) ? max : (a + b);
    }

    private static double saturatingMul(double a, double b) {
        double max = StatisticsEstimateCoefficient.MAXIMUM_COST;
        if (a <= 0 || b <= 0) {
            return 0;
        }
        if (a >= max || b >= max) {
            return max;
        }
        return a > (max / b) ? max : (a * b);
    }

    private List<BitSet> generatePartitions(BitSet totalNodes) {
        // DP join reorder enumerates bipartitions of `totalNodes`.
        // Avoid symmetric duplicates: only keep one side of (S, totalNodes \\ S) by forcing S to contain the first set bit.
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

        // Enumerate subsets of `rest`; `first` is always included.
        // Exclude the full set (mask == all ones) to avoid returning `totalNodes` itself.
        // Enumerate in descending mask order (try larger subsets earlier).
        // DP join reorder is hard-capped at callers (atoms <= 62), therefore n (= atoms - 1) <= 61 here.
        long count = (1L << n) - 1; // number of subsets excluding the full set (all-ones)
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
