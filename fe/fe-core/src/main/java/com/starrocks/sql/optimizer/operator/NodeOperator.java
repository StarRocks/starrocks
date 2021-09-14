// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.optimizer.operator;

import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Objects;

public class NodeOperator extends Operator {
    protected long limit = -1;
    protected ScalarOperator predicate;
    protected Projection projection;

    public NodeOperator(OperatorType type) {
        super(type);
    }

    public ColumnRefSet getOutputColumns(ExpressionContext expressionContext) {
        return expressionContext.getChildOutputColumns(0);
    }

    public long getLimit() {
        return limit;
    }

    public void setLimit(long limit) {
        this.limit = limit;
    }

    public boolean hasLimit() {
        return limit != -1;
    }

    public ScalarOperator getPredicate() {
        return predicate;
    }

    public void setPredicate(ScalarOperator predicate) {
        this.predicate = predicate;
    }

    public void setProjection(Projection projection) {
        this.projection = projection;
    }

    public Projection getProjection() {
        return projection;
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
        NodeOperator that = (NodeOperator) o;
        return limit == that.limit && Objects.equals(predicate, that.predicate) &&
                Objects.equals(projection, that.projection);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), limit, predicate, projection);
    }
}
