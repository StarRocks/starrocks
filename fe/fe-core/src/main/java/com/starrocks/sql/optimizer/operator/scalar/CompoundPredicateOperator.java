// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.optimizer.operator.scalar;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;

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
        CompoundPredicateOperator that = (CompoundPredicateOperator) o;
        if (type != that.type) {
            return false;
        }
        if (type.equals(CompoundType.OR) || type.equals(CompoundType.AND)) {
            Set<ScalarOperator> thisArgs = Sets.newHashSet(this.getChildren());
            Set<ScalarOperator> thatArgs = Sets.newHashSet(that.getChildren());
            return thisArgs.equals(thatArgs);
        } else {
            return Objects.equals(this.getChildren(), that.getChildren());
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(opType, type, Sets.newHashSet(this.getChildren()).hashCode());
    }

    public static ScalarOperator or(List<ScalarOperator> nodes) {
        return Utils.createCompound(CompoundPredicateOperator.CompoundType.OR, nodes);
    }

    public static ScalarOperator or(ScalarOperator... nodes) {
        return Utils.createCompound(CompoundPredicateOperator.CompoundType.OR, Arrays.asList(nodes));
    }

    public static ScalarOperator and(List<ScalarOperator> nodes) {
        return Utils.createCompound(CompoundPredicateOperator.CompoundType.AND, nodes);
    }

    public static ScalarOperator and(ScalarOperator... nodes) {
        return Utils.createCompound(CompoundPredicateOperator.CompoundType.AND, Arrays.asList(nodes));
    }
}
