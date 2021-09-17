// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rule.join;

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

        GroupInfo leftGroup = atoms.get(0);
        for (int index = 1; index < atomSize; ++index) {
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
