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
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Sets;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;

import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Reorder multi join node by greedy algorithm
 * This implementation refer to gporca CJoinOrderDPv2 and CJoinOrderGreedy
 */
public class JoinReorderGreedy extends JoinOrder {
    protected final MinMaxPriorityQueue<ExpressionInfo> topKExpr;

    public JoinReorderGreedy(OptimizerContext context) {
        super(context);
        // Ensure that topk's ExpressionInfo is returned in the same order, the final plan will be different if the
        // cost is same and get ExpressionInfo in different order.
        this.topKExpr = MinMaxPriorityQueue.orderedBy((Comparator<ExpressionInfo>) (left, right) -> {
            double leftCost = left.cost;
            double rightCost = right.cost;
            int result = Double.compare(leftCost, rightCost);
            if (result == 0) {
                return Double.compare(left.hashCode(), right.hashCode());
            } else {
                return result;
            }
        }).maximumSize(10).create();
    }

    @Override
    protected void enumerate() {
        for (int curJoinLevel = 2; curJoinLevel <= atomSize; curJoinLevel++) {
            searchJoinOrders(curJoinLevel - 1, 1, false);
            searchBushyJoinOrders(curJoinLevel);
        }
    }

    private void searchBushyJoinOrders(int curJoinLevel) {
        // Search bushy joins tree fro level x and y, where
        // x + y = curJoinLevel and x > 1 and y > 1 and x >= y.
        // Note that join trees of level 3 and below are never bushy,
        // so this loop only executes at curJoinLevel >= 4
        for (int rightLevel = 2; rightLevel <= curJoinLevel / 2; rightLevel++) {
            searchJoinOrders(curJoinLevel - rightLevel, rightLevel, true);
        }
    }

    @Override
    public List<OptExpression> getResult() {
        List<OptExpression> result = Lists.newArrayList();
        while (!topKExpr.isEmpty()) {
            result.add(topKExpr.pollFirst().expr);
        }
        return result;
    }

    private void searchJoinOrders(int leftLevel, int rightLevel, boolean isSearchBushyJoin) {
        List<GroupInfo> leftGroupInfos = getGroupForLevel(leftLevel);
        List<GroupInfo> rightGroupInfos = getGroupForLevel(rightLevel);
        JoinLevel curLevel = joinLevels.get(leftLevel + rightLevel);
        if (isSearchBushyJoin) {
            rightGroupInfos = getBestGroupList(rightGroupInfos, curLevel);
        }
        List<GroupInfo> bestLeftGroups = getBestGroupList(leftGroupInfos, curLevel);
        for (GroupInfo leftGroup : bestLeftGroups) {
            BitSet leftBitset = leftGroup.atoms;
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

                computeCost(joinExpr);
                getOrCreateGroupInfo(curLevel, joinBitSet, joinExpr);
                double joinCost = joinExpr.cost;
                if (joinCost < bestCost) {
                    bestCost = joinCost;
                }
            }
        }
    }

    private List<GroupInfo> getBestGroupList(List<GroupInfo> groupInfos, JoinLevel curLevel) {
        // Do not use greedy algorithms to select the first table, otherwise it is easy to fall into local optimality
        if (curLevel.level == 1) {
            return groupInfos;
        } else {
            Set<GroupInfo> bestGroupInfos = Sets.newHashSet();
            // Get join level 1 used atoms
            List<BitSet> levelOneGroups = Lists.newArrayList();
            getGroupForLevel(1).forEach(groupInfo -> levelOneGroups.add(groupInfo.atoms));
            // For each atom, choose at least one group info to return.
            for (BitSet levelOneGroup : levelOneGroups) {
                List<GroupInfo> candidateGroups = groupInfos.stream().filter(
                                groupInfo -> groupInfo.atoms.intersects(levelOneGroup) && !bestGroupInfos.contains(groupInfo)).
                        collect(Collectors.toList());
                // Get best group info from candidate group info
                if (!candidateGroups.isEmpty()) {
                    bestGroupInfos.add(getBestGroupInfo(candidateGroups));
                }
            }
            return Lists.newArrayList(bestGroupInfos);
        }
    }

    private GroupInfo getBestGroupInfo(List<GroupInfo> groupInfos) {
        double bestCost = Double.MAX_VALUE;
        GroupInfo bestExpr = null;
        for (GroupInfo groupInfo : groupInfos) {
            if (groupInfo.bestExprInfo.cost < bestCost) {
                bestExpr = groupInfo;
                bestCost = groupInfo.bestExprInfo.cost;
            }
        }
        return bestExpr;
    }

    protected GroupInfo getOrCreateGroupInfo(JoinLevel joinLevel, BitSet atoms,
                                             ExpressionInfo exprInfo) {
        GroupInfo groupInfo;
        if (bitSetToGroupInfo.containsKey(atoms)) {
            groupInfo = bitSetToGroupInfo.get(atoms);
        } else {
            groupInfo = new GroupInfo(atoms);
            joinLevel.groups.add(groupInfo);
            if (joinLevel.level > 1) {
                bitSetToGroupInfo.put(groupInfo.atoms, groupInfo);
            }
        }

        if (groupInfo.bestExprInfo == null || groupInfo.bestExprInfo != exprInfo) {
            addExprToGroup(groupInfo, exprInfo);
        }
        return groupInfo;
    }

    protected void addExprToGroup(GroupInfo groupInfo, ExpressionInfo expr) {
        Preconditions.checkState(expr.cost != -1);
        double cost = expr.cost;

        // For top group, we keep multi best join expressions
        if (groupInfo.atoms.cardinality() == atomSize) {
            // avoid repeated put, check object is enough
            if (!topKExpr.contains(expr)) {
                topKExpr.offer(expr);
            }
        } else {
            if (cost < groupInfo.lowestExprCost) {
                groupInfo.bestExprInfo = expr;
                groupInfo.lowestExprCost = cost;
            }
        }
    }
}
