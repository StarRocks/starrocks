// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.operator.logical;

import com.starrocks.analysis.AssertNumRowsElement;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;

import java.util.ArrayList;
import java.util.Objects;

public class LogicalAssertOneRowOperator extends LogicalOperator {

    private final AssertNumRowsElement.Assertion assertion;

    private final long checkRows;

    // Error sql message, use for throw exception in BE
    private final String tips;

    private LogicalAssertOneRowOperator(AssertNumRowsElement.Assertion assertion, long checkRows, String tips) {
        super(OperatorType.LOGICAL_ASSERT_ONE_ROW);
        this.assertion = assertion;
        this.checkRows = checkRows;
        this.tips = tips;
    }

    public static LogicalAssertOneRowOperator createLessEqOne(String tips) {
        return new LogicalAssertOneRowOperator(AssertNumRowsElement.Assertion.LE, 1, tips);
    }

    public AssertNumRowsElement.Assertion getAssertion() {
        return assertion;
    }

    public long getCheckRows() {
        return checkRows;
    }

    public String getTips() {
        return tips;
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
        LogicalAssertOneRowOperator that = (LogicalAssertOneRowOperator) o;
        return checkRows == that.checkRows &&
                assertion == that.assertion &&
                Objects.equals(tips, that.tips);
    }

    @Override
    public ColumnRefSet getOutputColumns(ExpressionContext expressionContext) {
        if (projection != null) {
            return new ColumnRefSet(new ArrayList<>(projection.getColumnRefMap().keySet()));
        } else {
            return expressionContext.getChildLogicalProperty(0).getOutputColumns();
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), assertion, checkRows, tips);
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalAssertOneRow(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitLogicalAssertOneRow(optExpression, context);
    }
}
