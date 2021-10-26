// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.optimizer.operator.scalar;

import com.google.common.base.Preconditions;
import com.starrocks.sql.optimizer.operator.OperatorType;

import java.util.Objects;

public class CompoundPredicateOperator extends PredicateOperator {
    private final CompoundType type;

    public CompoundPredicateOperator(CompoundType compoundType, ScalarOperator... arguments) {
        super(OperatorType.COMPOUND, arguments);
        this.type = compoundType;
        Preconditions.checkState(arguments.length >= 1);
    }

    public CompoundType getCompoundType() {
        return type;
    }

    @Override
    public <R, C> R accept(ScalarOperatorVisitor<R, C> visitor, C context) {
        return visitor.visitCompoundPredicate(this, context);
    }

    public enum CompoundType {
        AND,
        OR,
        NOT
    }

    public boolean isAnd() {
        return CompoundType.AND.equals(type);
    }

    public boolean isOr() {
        return CompoundType.OR.equals(type);
    }

    public boolean isNot() {
        return CompoundType.NOT.equals(type);
    }

    @Override
    public String toString() {
        if (CompoundType.NOT.equals(type)) {
            return "NOT " + getChild(0).toString();
        } else {
            return getChild(0).toString() + " " + type.toString() + " " + getChild(1).toString();
        }
    }

    @Override
    public String debugString() {
        if (CompoundType.NOT.equals(type)) {
            return "NOT " + getChild(0).debugString();
        } else {
            return getChild(0).debugString() + " " + type.toString() + " " + getChild(1).debugString();
        }
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
        CompoundPredicateOperator that = (CompoundPredicateOperator) o;
        return type == that.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), type);
    }
}
