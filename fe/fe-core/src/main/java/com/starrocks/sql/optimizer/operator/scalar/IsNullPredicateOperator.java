// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.operator.scalar;

import com.starrocks.sql.optimizer.operator.OperatorType;

import java.util.Objects;

public class IsNullPredicateOperator extends PredicateOperator {
    private final boolean isNotNull;

    public IsNullPredicateOperator(ScalarOperator arguments) {
        this(false, arguments, false);
    }

    public IsNullPredicateOperator(boolean isNotNull, ScalarOperator arguments) {
        this(isNotNull, arguments, false);

    }
    public IsNullPredicateOperator(boolean isNotNull, ScalarOperator arguments, boolean isRedundant) {
        super(OperatorType.IS_NULL, arguments);
        this.isNotNull = isNotNull;
        this.isRedundant = isRedundant;
    }

    public boolean isNotNull() {
        return isNotNull;
    }

    @Override
    public boolean isNullable() {
        return false;
    }

    @Override
    public String toString() {
        if (!isNotNull) {
            return getChild(0).toString() + " IS NULL";
        } else {
            return getChild(0).toString() + " IS NOT NULL";
        }
    }

    @Override
    public <R, C> R accept(ScalarOperatorVisitor<R, C> visitor, C context) {
        return visitor.visitIsNullPredicate(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        IsNullPredicateOperator that = (IsNullPredicateOperator) o;
        return isNotNull == that.isNotNull;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), isNotNull);
    }
}
