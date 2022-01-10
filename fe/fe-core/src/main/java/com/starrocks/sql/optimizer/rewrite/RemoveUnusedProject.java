// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.optimizer.rewrite;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProjectOperator;

import java.util.ArrayList;

public class RemoveUnusedProject extends OptExpressionVisitor<OptExpression, Void> {
    public OptExpression rewrite(OptExpression optExpression) {
        return optExpression.getOp().accept(this, optExpression, null);
    }

    @Override
    public OptExpression visit(OptExpression optExpr, Void context) {
        for (int idx = 0; idx < optExpr.arity(); ++idx) {
            optExpr.setChild(idx, rewrite(optExpr.inputAt(idx)));
        }
        return optExpr;
    }

    @Override
    public OptExpression visitPhysicalProject(OptExpression optExpr, Void context) {
        PhysicalProjectOperator physicalProjectOperator = (PhysicalProjectOperator) optExpr.getOp();
        OptExpression childOpt = visit(optExpr.inputAt(0), context);

        //if (physicalProjectOperator.getColumnRefMap().isEmpty()) {
        //    return optExpr.inputAt(0);
        //}

        ColumnRefSet childOutputColumns = new ColumnRefSet();
        if (childOpt.getOp() instanceof PhysicalHashJoinOperator) {
            PhysicalHashJoinOperator joinOperator = (PhysicalHashJoinOperator) childOpt.getOp();
            if (joinOperator.getJoinType().isLeftSemiAntiJoin()) {
                childOutputColumns.union(childOpt.inputAt(0).getOutputColumns());
            } else if (joinOperator.getJoinType().isRightSemiAntiJoin()) {
                childOutputColumns.union(childOpt.inputAt(1).getOutputColumns());
            } else {
                childOutputColumns.union(childOpt.inputAt(0).getOutputColumns());
                childOutputColumns.union(childOpt.inputAt(1).getOutputColumns());
            }
        } else {
            childOutputColumns.union(childOpt.getOutputColumns());
        }

        ColumnRefSet projectColumns =
                new ColumnRefSet(new ArrayList<>(physicalProjectOperator.getColumnRefMap().keySet()));

        if (projectColumns.equals(childOutputColumns)) {
            return childOpt;
        }
        return optExpr;
    }
}
