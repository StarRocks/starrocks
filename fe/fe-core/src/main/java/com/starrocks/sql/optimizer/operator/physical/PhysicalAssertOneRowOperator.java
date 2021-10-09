// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.operator.physical;

import com.google.common.base.Objects;
import com.starrocks.analysis.AssertNumRowsElement;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

public class PhysicalAssertOneRowOperator extends PhysicalOperator {
    private final AssertNumRowsElement.Assertion assertion;

    private final long checkRows;

    private final String tips;

    public PhysicalAssertOneRowOperator(AssertNumRowsElement.Assertion assertion, long checkRows, String tips,
                                        long limit,
                                        ScalarOperator predicate,
                                        Projection projection) {
        super(OperatorType.PHYSICAL_ASSERT_ONE_ROW);
        this.assertion = assertion;
        this.checkRows = checkRows;
        this.tips = tips;
        this.limit = limit;
        this.predicate = predicate;
        this.projection = projection;
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
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalAssertOneRow(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalAssertOneRow(optExpression, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PhysicalAssertOneRowOperator that = (PhysicalAssertOneRowOperator) o;
        return checkRows == that.checkRows && assertion == that.assertion && Objects.equal(tips, that.tips);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(assertion, checkRows, tips);
    }
}
