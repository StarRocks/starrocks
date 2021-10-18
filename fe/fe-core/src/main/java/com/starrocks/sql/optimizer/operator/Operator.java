// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.optimizer.operator;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Objects;

public abstract class Operator {
    protected final OperatorType opType;
    protected long limit = -1;
    protected ScalarOperator predicate;

    public Operator(OperatorType opType) {
        this.opType = opType;
    }

    public Operator(OperatorType opType, long limit, ScalarOperator predicate) {
        this.opType = opType;
        this.limit = limit;
        this.predicate = predicate;
    }

    public boolean isLogical() {
        return false;
    }

    public boolean isPhysical() {
        return false;
    }

    public OperatorType getOpType() {
        return opType;
    }

    public long getLimit() {
        return limit;
    }

    @Deprecated
    public void setLimit(long limit) {
        this.limit = limit;
    }

    public boolean hasLimit() {
        return limit != -1;
    }

    public ScalarOperator getPredicate() {
        return predicate;
    }

    @Deprecated
    public void setPredicate(ScalarOperator predicate) {
        this.predicate = predicate;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Operator operator = (Operator) o;
        return limit == operator.limit && opType == operator.opType &&
                Objects.equals(predicate, operator.predicate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(opType.ordinal(), limit, predicate);
    }

    public abstract static class Builder<O extends Operator, B extends Builder> {
        protected OperatorType opType;
        protected long limit = -1;
        protected ScalarOperator predicate;

        public B withOperator(O operator) {
            this.opType = operator.opType;
            this.limit = operator.limit;
            this.predicate = operator.predicate;
            return (B) this;
        }

        public abstract O build();

        public OperatorType getOpType() {
            return opType;
        }

        public B setOpType(OperatorType opType) {
            this.opType = opType;
            return (B) this;
        }

        public long getLimit() {
            return limit;
        }

        public B setLimit(long limit) {
            this.limit = limit;
            return (B) this;
        }

        public ScalarOperator getPredicate() {
            return predicate;
        }

        public B setPredicate(ScalarOperator predicate) {
            this.predicate = predicate;
            return (B) this;
        }
    }
}
