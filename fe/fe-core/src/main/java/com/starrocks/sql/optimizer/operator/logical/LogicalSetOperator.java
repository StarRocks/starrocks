// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.optimizer.operator.logical;

import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.List;
import java.util.Objects;

public abstract class LogicalSetOperator extends LogicalOperator {
    protected List<ColumnRefOperator> outputColumnRefOp;
    protected List<List<ColumnRefOperator>> childOutputColumns;

    public LogicalSetOperator(OperatorType type, List<ColumnRefOperator> result,
                              List<List<ColumnRefOperator>> childOutputColumns) {
        super(type);
        this.outputColumnRefOp = result;
        this.childOutputColumns = childOutputColumns;
    }

    public List<ColumnRefOperator> getOutputColumnRefOp() {
        return outputColumnRefOp;
    }

    public List<List<ColumnRefOperator>> getChildOutputColumns() {
        return childOutputColumns;
    }

    @Override
    public ColumnRefSet getOutputColumns(ExpressionContext expressionContext) {
        return new ColumnRefSet(outputColumnRefOp);
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
        LogicalSetOperator that = (LogicalSetOperator) o;
        return Objects.equals(outputColumnRefOp, that.outputColumnRefOp) && opType == that.opType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), outputColumnRefOp);
    }
}
