// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.optimizer.operator.physical;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;

public class PhysicalIntersectOperator extends PhysicalSetOperation {
    public PhysicalIntersectOperator(List<ColumnRefOperator> columnRef,
                                     List<List<ColumnRefOperator>> childOutputColumns,
                                     long limit,
                                     ScalarOperator predicate) {
        super(OperatorType.PHYSICAL_INTERSECT, columnRef, childOutputColumns, limit, predicate);
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalIntersect(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalIntersect(optExpression, context);
    }
}
