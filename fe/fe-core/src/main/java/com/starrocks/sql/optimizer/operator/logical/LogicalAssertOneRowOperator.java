// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.starrocks.sql.optimizer.operator.logical;

import com.starrocks.sql.ast.AssertNumRowsElement;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.RowOutputInfo;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;

import java.util.ArrayList;
import java.util.List;

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

    private LogicalAssertOneRowOperator(Builder builder) {
        super(OperatorType.LOGICAL_ASSERT_ONE_ROW, builder.getLimit(), builder.getPredicate(), builder.getProjection());
        this.assertion = builder.assertion;
        this.checkRows = builder.checkRows;
        this.tips = builder.tips;
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
        return this == o;
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
    public RowOutputInfo deriveRowOutputInfo(List<OptExpression> inputs) {
        return projectInputRow(inputs.get(0).getRowOutputInfo());
    }

    @Override
    public int hashCode() {
        return System.identityHashCode(this);
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalAssertOneRow(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitLogicalAssertOneRow(optExpression, context);
    }

    public static class Builder
            extends LogicalOperator.Builder<LogicalAssertOneRowOperator, LogicalAssertOneRowOperator.Builder> {
        private AssertNumRowsElement.Assertion assertion;
        private long checkRows;
        private String tips;

        @Override
        public LogicalAssertOneRowOperator build() {
            return new LogicalAssertOneRowOperator(this);
        }

        @Override
        public LogicalAssertOneRowOperator.Builder withOperator(LogicalAssertOneRowOperator assertOneRowOperator) {
            super.withOperator(assertOneRowOperator);
            this.assertion = assertOneRowOperator.assertion;
            this.checkRows = assertOneRowOperator.checkRows;
            this.tips = assertOneRowOperator.tips;
            return this;
        }
    }
}
