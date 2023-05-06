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

import com.google.common.base.Objects;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.RowOutputInfo;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class LogicalValuesOperator extends LogicalOperator {
    private List<ColumnRefOperator> columnRefSet;
    private List<List<ScalarOperator>> rows;

    public LogicalValuesOperator(List<ColumnRefOperator> columnRefSet, List<List<ScalarOperator>> rows) {
        super(OperatorType.LOGICAL_VALUES);
        this.columnRefSet = columnRefSet;
        this.rows = rows;
    }

    private LogicalValuesOperator() {
        super(OperatorType.LOGICAL_VALUES);
    }

    public List<ColumnRefOperator> getColumnRefSet() {
        return columnRefSet;
    }

    public List<List<ScalarOperator>> getRows() {
        return rows;
    }

    @Override
    public ColumnRefSet getOutputColumns(ExpressionContext expressionContext) {
        if (projection != null) {
            return new ColumnRefSet(new ArrayList<>(projection.getColumnRefMap().keySet()));
        } else {
            return new ColumnRefSet(columnRefSet);
        }
    }

    @Override
    public RowOutputInfo deriveRowOutputInfo(List<OptExpression> inputs) {
        return new RowOutputInfo(columnRefSet.stream().collect(Collectors.toMap(Function.identity(), Function.identity())));
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalValues(this, context);
    }

    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitLogicalValues(optExpression, context);
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
        LogicalValuesOperator that = (LogicalValuesOperator) o;
        return Objects.equal(columnRefSet, that.columnRefSet) && Objects.equal(rows, that.rows);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), columnRefSet);
    }

    public static class Builder extends LogicalOperator.Builder<LogicalValuesOperator, LogicalValuesOperator.Builder> {

        @Override
        protected LogicalValuesOperator newInstance() {
            return new LogicalValuesOperator();
        }

        @Override
        public Builder withOperator(LogicalValuesOperator valuesOperator) {
            super.withOperator(valuesOperator);

            builder.columnRefSet = valuesOperator.columnRefSet;
            builder.rows = valuesOperator.rows;
            return this;
        }
    }
}
