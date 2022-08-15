// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.operator.scalar;

import com.starrocks.sql.optimizer.operator.OperatorType;

import java.util.List;
import java.util.Objects;

public class ExistsPredicateOperator extends PredicateOperator {
    private final boolean isNotExists;

    public ExistsPredicateOperator(boolean isNotExists, ScalarOperator... arguments) {
        super(OperatorType.EXISTS, arguments);
        this.isNotExists = isNotExists;
    }

    public ExistsPredicateOperator(boolean isNotExists, List<ScalarOperator> arguments) {
        super(OperatorType.EXISTS, arguments);
        this.isNotExists = isNotExists;
    }

    public boolean isNotExists() {
        return isNotExists;
    }

    @Override
    public <R, C> R accept(ScalarOperatorVisitor<R, C> visitor, C context) {
        return visitor.visitExistsPredicate(this, context);
    }

    @Override
    public String toString() {
        StringBuilder strBuilder = new StringBuilder();
        if (isNotExists) {
            strBuilder.append("NOT ");

        }
        strBuilder.append("EXISTS ");
        strBuilder.append(getChild(0).toString());
        return strBuilder.toString();
    }

    @Override
    public String debugString() {
        StringBuilder strBuilder = new StringBuilder();
        if (isNotExists) {
            strBuilder.append("NOT ");

        }
        strBuilder.append("EXISTS ");
        strBuilder.append(getChild(0).debugString());
        return strBuilder.toString();
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
        ExistsPredicateOperator that = (ExistsPredicateOperator) o;
        return isNotExists == that.isNotExists;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), isNotExists);
    }
}
