// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.operator.physical;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;

public class PhysicalExceptOperator extends PhysicalSetOperation {
    public PhysicalExceptOperator(List<ColumnRefOperator> columnRef, List<List<ColumnRefOperator>> childOutputColumns,
                                  long limit,
                                  ScalarOperator predicate,
                                  Projection projection) {
        super(OperatorType.PHYSICAL_EXCEPT, columnRef, childOutputColumns, limit, predicate, projection);
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalExcept(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalExcept(optExpression, context);
    }
}
