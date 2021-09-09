// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.optimizer.operator;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;

public abstract class Operator {
    protected final OperatorType opType;

    public Operator(OperatorType opType) {
        this.opType = opType;
    }

    public boolean isLogical() {
        return false;
    }

    public boolean isPhysical() {
        return false;
    }

    public boolean isPattern() {
        return false;
    }

    public OperatorType getOpType() {
        return opType;
    }

    @Override
    public int hashCode() {
        return opType.ordinal();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Operator)) {
            return false;
        }
        Operator rhs = (Operator) obj;
        if (this == rhs) {
            return true;
        }

        return opType == rhs.getOpType();
    }

    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitOperator(this, context);
    }

    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visit(optExpression, context);
    }

    @Override
    public String toString() {
        return opType.name();
    }
}
