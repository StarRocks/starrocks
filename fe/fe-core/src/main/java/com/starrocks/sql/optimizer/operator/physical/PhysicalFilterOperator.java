// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
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
    public int hashCode() {
        return predicate.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof PhysicalFilterOperator)) {
            return false;
        }
        PhysicalFilterOperator rhs = (PhysicalFilterOperator) obj;
        if (this == rhs) {
            return true;
        }

        return predicate.equals(rhs.getPredicate());
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
