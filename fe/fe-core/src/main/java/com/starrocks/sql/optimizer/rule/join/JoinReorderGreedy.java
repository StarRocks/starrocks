// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.rule.join;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;

import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Reorder multi join node by greedy algorithm
 * This implementation refer to gporca CJoinOrderDPv2 and CJoinOrderGreedy
 */
public class JoinReorderGreedy extends JoinOrder {
    protected final MinMaxPriorityQueue<ExpressionInfo> topKExpr;

    public JoinReorderGreedy(OptimizerContext context) {
        super(context);
        this.topKExpr = MinMaxPriorityQueue.orderedBy(Comparator.comparing(JoinOrder.ExpressionInfo::getCost))
                .maximumSize(10).create();
    }

    @Override
    protected void enumerate() {
        for (int curJoinLevel = 2; curJoinLevel <= atomSize; curJoinLevel++) {
            searchJoinOrders(curJoinLevel - 1, 1);
        }
    }

    @Override
    public List<OptExpression> getResult() {
        return topKExpr.stream().map(p -> p.expr).collect(Collectors.toList());
    }

    private void searchJoinOrders(int leftLevel, int rightLevel) {
        List<GroupInfo> leftGroupInfos = getGroupForLevel(leftLevel);
        List<GroupInfo> rightGroupInfos = getGroupForLevel(rightLevel);
        JoinLevel curLevel = joinLevels.get(leftLevel + rightLevel);
        List<GroupInfo> bestLeftGroups = getBestExpr(leftGroupInfos);
        for (GroupInfo leftGroup : bestLeftGroups) {
            BitSet leftBitset = leftGroup.atoms;

            GroupInfo bestGroupInfo = null;
            ExpressionInfo bestExprInfo = null;
            double bestCost = Double.MAX_VALUE;

            for (GroupInfo rightGroup : rightGroupInfos) {
                BitSet rightBitset = rightGroup.atoms;
                if (leftBitset.intersects(rightBitset)) {
                    continue;
                }

                ExpressionInfo joinExpr = buildJoinExpr(leftGroup, rightGroup);
                joinExpr.expr.deriveLogicalPropertyItself();
                calculateStatistics(joinExpr.expr);

                BitSet joinBitSet = new BitSet();
                joinBitSet.or(leftBitset);
                joinBitSet.or(rightBitset);

                computeCost(joinExpr, true);
                GroupInfo groupInfo = getOrCreateGroupInfo(curLevel, joinBitSet, joinExpr);
                double joinCost = joinExpr.cost;
                if (joinCost < bestCost) {
                    bestGroupInfo = groupInfo;
                    bestExprInfo = joinExpr;
                    bestCost = joinCost;
                }
            }

            if (bestExprInfo != null) {
                addExprToGroup(bestGroupInfo, bestExprInfo);
            }
        }
    }

    private List<GroupInfo> getBestExpr(List<GroupInfo> groupInfos) {
        // For top level, we need to return top k expression.
        if (groupInfos.size() == atomSize - 1) {
            return groupInfos;
        } else {
            double bestCost = Double.MAX_VALUE;
            GroupInfo bestExpr = null;
            for (GroupInfo groupInfo : groupInfos) {
                if (groupInfo.bestExprInfo.cost < bestCost) {
                    bestExpr = groupInfo;
                    bestCost = groupInfo.bestExprInfo.cost;
                }
            }
            return Lists.newArrayList(bestExpr);
        }
    }

    protected GroupInfo getOrCreateGroupInfo(JoinLevel joinLevel, BitSet atoms,
                                             ExpressionInfo exprInfo) {
        if (bitSetToGroupInfo.containsKey(atoms)) {
            return bitSetToGroupInfo.get(atoms);
        } else {
            GroupInfo groupInfo = new GroupInfo(atoms);
            addExprToGroup(groupInfo, exprInfo);
            joinLevel.groups.add(groupInfo);

            if (joinLevel.level > 1) {
                bitSetToGroupInfo.put(groupInfo.atoms, groupInfo);
            }
            return groupInfo;
        }
    }

    protected void addExprToGroup(GroupInfo groupInfo, ExpressionInfo expr) {
        Preconditions.checkState(expr.cost != -1);
        double cost = expr.cost;

        // For top group, we keep multi best join expressions
        if (groupInfo.atoms.cardinality() == atomSize) {
            topKExpr.offer(expr);
        } else {
            if (cost < groupInfo.lowestExprCost) {
                groupInfo.bestExprInfo = expr;
                groupInfo.lowestExprCost = cost;
            }
        }
    }
}
