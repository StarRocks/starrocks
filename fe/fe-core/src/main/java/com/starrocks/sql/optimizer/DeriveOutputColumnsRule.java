// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer;

import com.google.common.base.Preconditions;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOperator;
import com.starrocks.sql.optimizer.rewrite.PhysicalOperatorTreeRewriteRule;
import com.starrocks.sql.optimizer.task.TaskContext;

public class DeriveOutputColumnsRule extends OptExpressionVisitor<Void, Void> implements
        PhysicalOperatorTreeRewriteRule {
    private final ColumnRefSet requiredColumns;

    public DeriveOutputColumnsRule(ColumnRefSet requiredColumns) {
        this.requiredColumns = requiredColumns;
    }

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        root.getOp().accept(this, root, null);
        return root;
    }

    @Override
    public Void visit(OptExpression optExpression, Void context) {
        Preconditions.checkState(optExpression.getOp().isPhysical());

        if (!requiredColumns.contains(optExpression.getOutputColumns())) {
            optExpression.getOutputColumns().intersect(requiredColumns);
        }

        PhysicalOperator physical = (PhysicalOperator) optExpression.getOp();
        requiredColumns.union(physical.getUsedColumns());

        for (OptExpression child : optExpression.getInputs()) {
            visit(child, context);
        }

        return null;
    }
}
