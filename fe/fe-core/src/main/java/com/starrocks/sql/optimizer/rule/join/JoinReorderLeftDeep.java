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
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;

import java.util.BitSet;
import java.util.List;

public class JoinReorderLeftDeep extends JoinOrder {
    OptExpression bestPlanRoot;

    public JoinReorderLeftDeep(OptimizerContext context) {
        super(context);
    }

    public boolean isSameTableJoin(GroupInfo left, GroupInfo right) {
        if (!(left.bestExprInfo.expr.getOp() instanceof LogicalScanOperator)) {
            return false;
        }
        if (!(right.bestExprInfo.expr.getOp() instanceof LogicalScanOperator)) {
            return false;
        }
        LogicalScanOperator l = (LogicalScanOperator) left.bestExprInfo.expr.getOp();
        LogicalScanOperator r = (LogicalScanOperator) right.bestExprInfo.expr.getOp();
        return l.getTable().getId() == r.getTable().getId();
    }

    @Override
    protected void enumerate() {
        List<GroupInfo> atoms = joinLevels.get(1).groups;
        atoms.sort((a, b) -> {
            double diff = b.bestExprInfo.cost - a.bestExprInfo.cost;
            return (diff < 0 ? -1 : (diff > 0 ? 1 : 0));
        });

        boolean useHeuristic = true;
        boolean[] used = new boolean[atomSize];
        GroupInfo leftGroup = atoms.get(0);
        used[0] = true;
        int next = 1;
        while (next < atomSize) {
            if (used[next]) {
                next++;
                continue;
            }
            int index = next;
            Preconditions.checkState(!used[index]);
            if (useHeuristic) {
                // search the next group which:
                // 1. has never been used
                // 2. can inner join with leftGroup
                // 3. not same table inner join (happens only the first time).
                // inner join on same tables possibly degrades to cross join.
                for (; index < atomSize; ++index) {
                    GroupInfo rightGroup = atoms.get(index);
                    if (next == 1 && isSameTableJoin(leftGroup, rightGroup)) {
                        continue;
                    }
                    if (!used[index] && canBuildInnerJoinPredicate(leftGroup, rightGroup)) {
                        break;
                    }
                }
                // if not found, fallback to old strategy
                if (index == atomSize) {
                    index = next;
                }
            }
            Preconditions.checkState(!used[index]);
            used[index] = true;

            GroupInfo rightGroup = atoms.get(index);
            ExpressionInfo joinExpr = buildJoinExpr(leftGroup, atoms.get(index));
            joinExpr.expr.deriveLogicalPropertyItself();
            calculateStatistics(joinExpr.expr);
            computeCost(joinExpr);

            BitSet joinBitSet = new BitSet();
            joinBitSet.or(leftGroup.atoms);
            joinBitSet.or(rightGroup.atoms);

            leftGroup = new GroupInfo(joinBitSet);
            leftGroup.bestExprInfo = joinExpr;
            leftGroup.lowestExprCost = joinExpr.cost;
        }
        bestPlanRoot = leftGroup.bestExprInfo.expr;
    }

    @Override
    public List<OptExpression> getResult() {
        return Lists.newArrayList(bestPlanRoot);
    }
}
