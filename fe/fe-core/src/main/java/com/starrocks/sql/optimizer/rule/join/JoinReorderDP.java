// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.optimizer.rule.join;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.powerSet;
import static java.util.stream.Collectors.toCollection;

public class JoinReorderDP extends JoinOrder {
    public JoinReorderDP(OptimizerContext context) {
        super(context);
    }

    private final Map<Set<OptExpression>, GroupInfo> bestPlanMemo = new HashMap<>();

    @Override
    protected void enumerate() {
        Set<OptExpression> joinKeys = new HashSet<>();
        for (GroupInfo groupInfo : joinLevels.get(1).groups) {
            joinKeys.add(groupInfo.bestExprInfo.expr);
        }

        getBestExpr(joinKeys, joinLevels.get(1).groups);
    }

    @Override
    public List<OptExpression> getResult() {
        Set<OptExpression> joinKeys = new HashSet<>();
        for (GroupInfo groupInfo : joinLevels.get(1).groups) {
            joinKeys.add(groupInfo.bestExprInfo.expr);
        }

        GroupInfo g = getBestExpr(joinKeys, joinLevels.get(1).groups);
        return Lists.newArrayList(g.bestExprInfo.expr);
    }

    private GroupInfo getBestExpr(Set<OptExpression> joinKeys, List<GroupInfo> groups) {
        if (groups.size() == 1) {
            return groups.get(0);
        }

        GroupInfo bestPlan = bestPlanMemo.get(joinKeys);
        if (bestPlan == null) {
            Ordering<ExpressionInfo> resultComparator = Ordering.from(Comparator.comparing(ExpressionInfo::getCost));

            List<ExpressionInfo> results = new ArrayList<>();
            Set<Set<Integer>> partitions = generatePartitions(groups.size());
            for (Set<Integer> partition : partitions) {
                List<GroupInfo> sourceList = ImmutableList.copyOf(groups);
                ArrayList<GroupInfo> leftSources = partition.stream()
                        .map(sourceList::get)
                        .collect(toCollection(ArrayList::new));
                Set<OptExpression> leftJoinKeys =
                        leftSources.stream().map(g -> g.bestExprInfo.expr).collect(Collectors.toSet());

                ArrayList<GroupInfo> rightSources = groups.stream()
                        .filter(g -> !leftSources.contains(g))
                        .collect(toCollection(ArrayList::new));
                Set<OptExpression> rightJoinKeys =
                        rightSources.stream().map(g -> g.bestExprInfo.expr).collect(Collectors.toSet());

                GroupInfo leftGroup = getBestExpr(leftJoinKeys, leftSources);
                if (!results.isEmpty() && leftGroup.bestExprInfo.cost > resultComparator.min(results).cost) {
                    continue;
                }
                GroupInfo rightGroup = getBestExpr(rightJoinKeys, rightSources);
                if (!results.isEmpty() && rightGroup.bestExprInfo.cost > resultComparator.min(results).cost) {
                    continue;
                }
                ExpressionInfo joinExpr = buildJoinExpr(leftGroup, rightGroup);

                joinExpr.expr.deriveLogicalPropertyItself();
                calculateStatistics(joinExpr.expr);
                computeCost(joinExpr, false);
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

    private Set<Set<Integer>> generatePartitions(int totalNodes) {
        checkArgument(totalNodes > 1, "totalNodes must be greater than 1");
        Set<Integer> numbers = IntStream.range(0, totalNodes).boxed().collect(toImmutableSet());
        return powerSet(numbers).stream()
                .filter(subSet -> subSet.size() > 0)
                .filter(subSet -> subSet.size() < numbers.size())
                .collect(toImmutableSet());
    }
}