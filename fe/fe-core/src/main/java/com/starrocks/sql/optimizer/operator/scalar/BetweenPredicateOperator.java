// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.operator.scalar;

import com.google.common.base.Preconditions;
import com.starrocks.sql.optimizer.operator.OperatorType;

import java.util.List;
import java.util.Objects;

public class BetweenPredicateOperator extends PredicateOperator {

    private final boolean notBetween;

    public BetweenPredicateOperator(boolean notBetween, ScalarOperator... arguments) {
        super(OperatorType.BETWEEN, arguments);
        this.notBetween = notBetween;
        Preconditions.checkState(arguments.length == 3);
    }

    public BetweenPredicateOperator(boolean notBetween, List<ScalarOperator> arguments) {
        super(OperatorType.BETWEEN, arguments);
        this.notBetween = notBetween;
        Preconditions.checkState(arguments != null && arguments.size() == 3);
    }

    public boolean isNotBetween() {
        return notBetween;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getChild(0).toString()).append(" ");

        if (isNotBetween()) {
            sb.append("NOT ");
        }

        sb.append("BETWEEN ");
        sb.append(getChild(1)).append(" AND ").append(getChild(2));
        return sb.toString();
    }

    @Override
    public <R, C> R accept(ScalarOperatorVisitor<R, C> visitor, C context) {
        return visitor.visitBetweenPredicate(this, context);
    }

    @Override
    public String debugString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getChild(0).debugString()).append(" ");

        if (isNotBetween()) {
            sb.append("NOT ");
        }

        sb.append("BETWEEN ");
        sb.append(getChild(1)).append(" AND ").append(getChild(2));
        return sb.toString();
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
        BetweenPredicateOperator that = (BetweenPredicateOperator) o;
        return notBetween == that.notBetween;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), notBetween);
    }
}
