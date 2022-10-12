// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.operator.logical;

import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;

import java.util.Objects;

public class MockOperator extends LogicalOperator {
    private int value;

    public MockOperator(OperatorType opType) {
        super(opType);
        this.value = 0;
    }

    public MockOperator(OperatorType opType, int value) {
        super(opType);
        this.value = value;
    }

    @Override
    public boolean isLogical() {
        return true;
    }

    public int getValue() {
        return value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(opType, value);
    }

    @Override
    public ColumnRefSet getOutputColumns(ExpressionContext expressionContext) {
        return new ColumnRefSet();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Operator)) {
            return false;
        }
        MockOperator rhs = (MockOperator) obj;
        if (this == rhs) {
            return true;
        }

        return opType == rhs.getOpType() && value == rhs.value;
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitMockOperator(this, context);
    }
}
