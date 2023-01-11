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
import java.util.Set;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.powerSet;

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

                ExpressionInfo joinExpr = buildJoinExpr(leftGroup, rightGroup);

                joinExpr.expr.deriveLogicalPropertyItself();
                calculateStatistics(joinExpr.expr);
                computeCost(joinExpr);
                results.add(joinExpr);
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

    private List<BitSet> generatePartitions(BitSet totalNodes) {
        Set<Integer> numbers = IntStream.range(0, totalNodes.cardinality()).boxed().collect(toImmutableSet());
        Set<Set<Integer>> sets = powerSet(numbers).stream()
                .filter(subSet -> subSet.size() > 0)
                .filter(subSet -> subSet.size() < numbers.size())
                .collect(toImmutableSet());

        List<Integer> l = bitSet2Array(totalNodes);
        List<BitSet> partitions = new ArrayList<>();
        for (Set<Integer> s : sets) {
            BitSet b = new BitSet();
            for (Integer i : s) {
                b.set(l.get(i));
            }
            partitions.add(b);
        }
        return partitions;
    }

    List<Integer> bitSet2Array(BitSet bitSet) {
        List<Integer> l = Lists.newArrayList();
        for (int i = 0; i < bitSet.size(); ++i) {
            if (bitSet.get(i)) {
                l.add(i);
            }
        }
        return l;
    }
}