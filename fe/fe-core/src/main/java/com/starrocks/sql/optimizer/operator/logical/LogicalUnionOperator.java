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

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.List;

public class LogicalUnionOperator extends LogicalSetOperator {
    private boolean isUnionAll;

    public LogicalUnionOperator(List<ColumnRefOperator> result, List<List<ColumnRefOperator>> childOutputColumns,
                                boolean isUnionAll) {
        super(OperatorType.LOGICAL_UNION, result, childOutputColumns, Operator.DEFAULT_LIMIT, null);
        this.isUnionAll = isUnionAll;
    }

    private LogicalUnionOperator() {
        super(OperatorType.LOGICAL_UNION);
    }

    public boolean isUnionAll() {
        return isUnionAll;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!super.equals(o)) {
            return false;
        }

        LogicalUnionOperator that = (LogicalUnionOperator) o;
        return isUnionAll == that.isUnionAll;
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalUnion(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitLogicalUnion(optExpression, context);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends LogicalSetOperator.Builder<LogicalUnionOperator, LogicalUnionOperator.Builder> {
        @Override
        protected LogicalUnionOperator newInstance() {
            return new LogicalUnionOperator();
        }

        @Override
        public Builder withOperator(LogicalUnionOperator unionOperator) {
            super.withOperator(unionOperator);
            builder.isUnionAll = unionOperator.isUnionAll;
            return this;
        }

        public Builder isUnionAll(boolean isUnionAll) {
            builder.isUnionAll = isUnionAll;
            return this;
        }
    }
}
