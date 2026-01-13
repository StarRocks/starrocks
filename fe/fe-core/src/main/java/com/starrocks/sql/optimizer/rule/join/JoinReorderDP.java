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
import com.google.common.collect.Ordering;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
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
            int index = 0;
            while (!joinKeys.get(index)) {
                index++;
            }

            return groups.get(index);
        }

        GroupInfo bestPlan = bestPlanMemo.get(joinKeys);
        if (bestPlan == null) {
            Ordering<ExpressionInfo> resultComparator = Ordering.from(Comparator.comparing(ExpressionInfo::getCost));

            List<ExpressionInfo> results = new ArrayList<>();
            List<BitSet> partitions = generatePartitions(joinKeys);
            for (BitSet partition : partitions) {
                GroupInfo leftGroup = getBestExpr(partition);
                if (!results.isEmpty() && leftGroup.bestExprInfo.cost > resultComparator.min(results).cost) {
                    continue;
                }

                BitSet otherPartition = (BitSet) joinKeys.clone();
                otherPartition.andNot(partition);

                GroupInfo rightGroup = getBestExpr(otherPartition);
                if (!results.isEmpty() && rightGroup.bestExprInfo.cost > resultComparator.min(results).cost) {
                    continue;
                }

                Optional<ExpressionInfo> joinExpr = buildJoinExpr(leftGroup, rightGroup);
                if (!joinExpr.isPresent())  {
                    continue;
                }

                joinExpr.get().expr.deriveLogicalPropertyItself();
                calculateStatistics(joinExpr.get().expr);
                computeCost(joinExpr.get());
                results.add(joinExpr.get());
            }
            ExpressionInfo minCostPlan = resultComparator.min(results);

            BitSet atoms = new BitSet();
            atoms.or(minCostPlan.leftChildExpr.atoms);
            atoms.or(minCostPlan.rightChildExpr.atoms);
            GroupInfo g = new GroupInfo(atoms);
            g.bestExprInfo = minCostPlan;
            g.lowestExprCost = minCostPlan.cost;

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