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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/*
 * This operator denotes where a particular CTE is defined in the query.
 * It defines the scope of that CTE. A CTE can be referenced only in the
 * subtree rooted by the corresponding CTEAnchor operator
 *
 * */
public class LogicalCTEAnchorOperator extends LogicalOperator {
    private int cteId;

    public LogicalCTEAnchorOperator(int cteId) {
        super(OperatorType.LOGICAL_CTE_ANCHOR);
        this.cteId = cteId;
    }

    private LogicalCTEAnchorOperator() {
        super(OperatorType.LOGICAL_CTE_ANCHOR);
    }

    @Override
    public ColumnRefSet getOutputColumns(ExpressionContext expressionContext) {
        if (projection != null) {
            return new ColumnRefSet(new ArrayList<>(projection.getColumnRefMap().keySet()));
        } else {
            return expressionContext.getChildLogicalProperty(1).getOutputColumns();
        }
    }

    @Override
    public RowOutputInfo deriveRowOutputInfo(List<OptExpression> inputs) {
        return projectInputRow(inputs.get(1).getRowOutputInfo());
    }

    public int getCteId() {
        return cteId;
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalCTEAnchor(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitLogicalCTEAnchor(optExpression, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!super.equals(o)) {
            return false;
        }

        LogicalCTEAnchorOperator that = (LogicalCTEAnchorOperator) o;
        return Objects.equals(cteId, that.cteId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), cteId);
    }

    @Override
    public String toString() {
        return "LogicalCTEAnchorOperator{" +
                "cteId='" + cteId + '\'' +
                '}';
    }

    public static class Builder
            extends LogicalOperator.Builder<LogicalCTEAnchorOperator, LogicalCTEAnchorOperator.Builder> {

        @Override
        protected LogicalCTEAnchorOperator newInstance() {
            return new LogicalCTEAnchorOperator();
        }

        @Override
        public LogicalCTEAnchorOperator.Builder withOperator(LogicalCTEAnchorOperator operator) {
            super.withOperator(operator);
            builder.cteId = operator.cteId;
            return this;
        }
    }
}
