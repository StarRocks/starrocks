// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.operator.physical;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

public class PhysicalFilterOperator extends PhysicalOperator {
    public PhysicalFilterOperator(ScalarOperator predicate,
                                  long limit,
                                  Projection projection) {
        super(OperatorType.PHYSICAL_FILTER);
        this.predicate = predicate;
        this.limit = limit;
        this.projection = projection;
    }

    public ScalarOperator getPredicate() {
        return predicate;
    }

    @Override
    public boolean equals(Object o) {
        return this == o;
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalFilter(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalFilter(optExpression, context);
    }
}
