// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.operator.scalar;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;

import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

import static java.util.Objects.requireNonNull;

public abstract class ScalarOperator implements Cloneable {
    protected final OperatorType opType;
    protected Type type;
    // this operator will not eval in predicate estimate
    protected boolean notEvalEstimate = false;
    // Used to determine if it is derive from predicate range extractor
    protected boolean fromPredicateRangeDerive = false;
    // Check weather the scalar operator is redundant which will not affect the final
    // if it's not considered. eg, `IsNullPredicateOperator` which is pushed down
    // from JoinNode.
    protected boolean isRedundant = false;

    private List<String> hints = Collections.emptyList();

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

    public static boolean isTrue(@Nullable ScalarOperator op) {
        if (op == null) {
            return false;
        }
        return op.isTrue();
    }

    public static boolean isFalse(@Nullable ScalarOperator op) {
        if (op == null) {
            return false;
        }
        return op.isFalse();
    }

    public boolean isTrue() {
        return this.equals(ConstantOperator.TRUE);
    }

    public boolean isFalse() {
        return this.equals(ConstantOperator.FALSE);
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

    /**
     * equivalent means logical equals, but may physical different, such as with different id
     */
    public boolean equivalent(Object other) {
        return equals(other);
    }

    public abstract <R, C> R accept(ScalarOperatorVisitor<R, C> visitor, C context);

    // Default Shallow Clone for ConstantOperator and ColumnRefOperator
    @Override
    public ScalarOperator clone() {
        ScalarOperator operator = null;
        try {
            operator = (ScalarOperator) super.clone();
            operator.hints = Lists.newArrayList(hints);
            operator.isRedundant = this.isRedundant;
        } catch (CloneNotSupportedException ignored) {
        }
        return operator;
    }

    /**
     * Return the columns that this scalar operator used.
     * For a + b, the used columns are a and b.
     */
    public abstract ColumnRefSet getUsedColumns();

    public List<ColumnRefOperator> getColumnRefs() {
        List<ColumnRefOperator> columns = Lists.newArrayList();
        getColumnRefs(columns);
        return columns;
    }

    public void getColumnRefs(List<ColumnRefOperator> columns) {
        for (ScalarOperator child : getChildren()) {
            child.getColumnRefs(columns);
        }
    }

    public String debugString() {
        return toString();
    }

    public boolean isColumnRef() {
        return this instanceof ColumnRefOperator;
    }

    public boolean isConstantRef() {
        return this instanceof ConstantOperator;
    }

    public boolean isConstantNull() {
        return this instanceof ConstantOperator && ((ConstantOperator) this).isNull();
    }

    public boolean isConstantZero() {
        return this instanceof ConstantOperator && ((ConstantOperator) this).isZero();
    }

    public boolean isConstantFalse() {
        return this instanceof ConstantOperator && this.getType() == Type.BOOLEAN &&
                !((ConstantOperator) this).getBoolean();
    }

    public boolean isConstantNullOrFalse() {
        return isConstantNull() || isConstantFalse();
    }

    public void setHints(List<String> hints) {
        this.hints = hints;
    }

    public List<String> getHints() {
        return hints;
    }

    public boolean isRedundant() {
        return isRedundant;
    }

    public void setRedundant(boolean redundant) {
        isRedundant = redundant;
    }

    // whether ScalarOperator are equals without id
    public static boolean isEquivalent(ScalarOperator left, ScalarOperator right) {
        if (!left.getOpType().equals(right.getOpType())) {
            return false;
        }

        boolean ret = left.equivalent(right);
        if (!ret) {
            return false;
        }
        Preconditions.checkState(left.getChildren().size() == right.getChildren().size());
        for (int i = 0; i < left.getChildren().size(); i++) {
            if (!isEquivalent(left.getChild(i), right.getChild(i))) {
                return false;
            }
        }
        return true;
    }

    public static boolean isColumnEqualBinaryPredicate(ScalarOperator predicate) {
        if (predicate instanceof BinaryPredicateOperator) {
            BinaryPredicateOperator binaryPredicate = (BinaryPredicateOperator) predicate;
            return binaryPredicate.getBinaryType().isEquivalence()
                    && binaryPredicate.getChild(0).isColumnRef() && binaryPredicate.getChild(1).isColumnRef();
        }
        return false;
    }
}
