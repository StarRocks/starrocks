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

import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.RowOutputInfo;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.ArrayList;
import java.util.List;

public class LogicalFilterOperator extends LogicalOperator {
    public LogicalFilterOperator(ScalarOperator predicate) {
        super(OperatorType.LOGICAL_FILTER);
        this.predicate = predicate;
    }

    private LogicalFilterOperator(Builder builder) {
        super(OperatorType.LOGICAL_FILTER, builder.getLimit(), builder.getPredicate(), builder.getProjection());
    }

    public ScalarOperator getPredicate() {
        return predicate;
    }

    public ColumnRefSet getRequiredChildInputColumns() {
        return predicate.getUsedColumns();
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
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalFilter(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitLogicalFilter(optExpression, context);
    }

    @Override
    public int hashCode() {
        return System.identityHashCode(this);
    }

    @Override
    public boolean equals(Object o) {
        return this == o;
    }

    public static class Builder
            extends LogicalOperator.Builder<LogicalFilterOperator, LogicalFilterOperator.Builder> {
        @Override
        public LogicalFilterOperator build() {
            return new LogicalFilterOperator(this);
        }

        @Override
        public LogicalFilterOperator.Builder withOperator(LogicalFilterOperator operator) {
            super.withOperator(operator);
            return this;
        }
    }
}
