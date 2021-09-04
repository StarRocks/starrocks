// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.optimizer.operator.physical;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

public class PhysicalFilter extends PhysicalOperator {
    public PhysicalFilter(ScalarOperator predicate) {
        super(OperatorType.PHYSICAL_FILTER);
        this.predicate = predicate;
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
        if (!(obj instanceof PhysicalFilter)) {
            return false;
        }
        PhysicalFilter rhs = (PhysicalFilter) obj;
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
