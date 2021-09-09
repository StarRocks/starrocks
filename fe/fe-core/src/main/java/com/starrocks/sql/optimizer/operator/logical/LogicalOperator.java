// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.operator.logical;

import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Objects;

public abstract class LogicalOperator extends Operator {
    protected long limit = -1;

    protected ScalarOperator predicate;

    protected LogicalOperator(OperatorType opType) {
        super(opType);
    }

    @Override
    public boolean isLogical() {
        return true;
    }

    public abstract ColumnRefSet getOutputColumns(ExpressionContext expressionContext);

    public ScalarOperator getPredicate() {
        return predicate;
    }

    public void setPredicate(ScalarOperator predicate) {
        this.predicate = predicate;
    }

    public long getLimit() {
        return limit;
    }

    public boolean hasLimit() {
        return limit != -1;
    }

    public void setLimit(long limit) {
        this.limit = limit;
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
        LogicalOperator that = (LogicalOperator) o;
        return limit == that.limit &&
                Objects.equals(predicate, that.predicate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), limit, predicate);
    }
}
