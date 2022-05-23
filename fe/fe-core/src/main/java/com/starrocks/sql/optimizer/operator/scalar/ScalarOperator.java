// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.optimizer.operator.scalar;

import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;

import java.util.List;

import static java.util.Objects.requireNonNull;

public abstract class ScalarOperator implements Cloneable {
    protected final OperatorType opType;
    protected Type type;
    // this operator will not eval in predicate estimate
    protected boolean notEvalEstimate = false;
    // Used to determine if it is derive from predicate range extractor
    protected boolean fromPredicateRangeDerive = false;

    public ScalarOperator(OperatorType opType, Type type) {
        this.opType = requireNonNull(opType, "opType is null");
        this.type = requireNonNull(type, "type is null");
    }

    @SuppressWarnings("unchecked")
    public <T extends ScalarOperator> T cast() {
        return (T) this;
    }

    public boolean isConstant() {
        for (ScalarOperator child : getChildren()) {
            if (!child.isConstant()) {
                return false;
            }
        }

        return true;
    }

    public abstract boolean isNullable();

    public OperatorType getOpType() {
        return opType;
    }

    public boolean isVariable() {
        for (ScalarOperator child : getChildren()) {
            if (child.isVariable()) {
                return true;
            }
        }

        return false;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public boolean isNotEvalEstimate() {
        return this.notEvalEstimate;
    }

    public void setNotEvalEstimate(boolean notEvalInPredicateEstimate) {
        this.notEvalEstimate = notEvalInPredicateEstimate;
    }

    public boolean isFromPredicateRangeDerive() {
        return fromPredicateRangeDerive;
    }

    public void setFromPredicateRangeDerive(boolean fromPredicateRangeDerive) {
        this.fromPredicateRangeDerive = fromPredicateRangeDerive;
    }

    public abstract List<ScalarOperator> getChildren();

    public abstract ScalarOperator getChild(int index);

    public abstract void setChild(int index, ScalarOperator child);

    @Override
    public abstract String toString();

    @Override
    public abstract int hashCode();

    @Override
    public abstract boolean equals(Object other);

    public abstract <R, C> R accept(ScalarOperatorVisitor<R, C> visitor, C context);

    // Default Shallow Clone for ConstantOperator and ColumnRefOperator
    @Override
    public ScalarOperator clone() {
        ScalarOperator operator = null;
        try {
            operator = (ScalarOperator) super.clone();
        } catch (CloneNotSupportedException ignored) {
        }
        return operator;
    }

    /**
     * Return the columns that this scalar operator used.
     * For a + b, the used columns are a and b.
     */
    public abstract ColumnRefSet getUsedColumns();

    public String debugString() {
        return toString();
    }

    public boolean isColumnRef() {
        return this instanceof ColumnRefOperator;
    }

    public boolean isConstantRef() {
        return this instanceof ConstantOperator;
    }
}