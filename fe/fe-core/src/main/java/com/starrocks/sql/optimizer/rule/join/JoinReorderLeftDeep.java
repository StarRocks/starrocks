// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.rule.join;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;

import java.util.BitSet;
import java.util.List;

public class JoinReorderLeftDeep extends JoinOrder {
    OptExpression bestPlanRoot;

    public JoinReorderLeftDeep(OptimizerContext context) {
        super(context);
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
                for (; index < atomSize; ++index) {
                    if (!used[index] && canBuildInnerJoinPredicate(leftGroup, atoms.get(index))) {
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
            computeCost(joinExpr, true);

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
