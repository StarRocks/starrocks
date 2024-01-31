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

import java.util.List;
import java.util.Objects;

/*
 * This operator is initially set as the root of a separate logical tree which corresponds to
 * the CTE definition. There is one such tree – and one such CTEProducer operator – for every
 * CTE defined in the query. These trees are not initially connected to the main logical query
 * tree. Each CTEProducer has a unique id.
 *
 * */
public class LogicalCTEProduceOperator extends LogicalOperator {
    private int cteId;

    public LogicalCTEProduceOperator(int cteId) {
        super(OperatorType.LOGICAL_CTE_PRODUCE);
        this.cteId = cteId;
    }

    @Override
    public ColumnRefSet getOutputColumns(ExpressionContext expressionContext) {
        return expressionContext.getChildLogicalProperty(0).getOutputColumns();
    }

    @Override
    public RowOutputInfo deriveRowOutputInfo(List<OptExpression> inputs) {
        return projectInputRow(inputs.get(0).getRowOutputInfo());
    }

    public int getCteId() {
        return cteId;
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalCTEProduce(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitLogicalCTEProduce(optExpression, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!super.equals(o)) {
            return false;
        }
        LogicalCTEProduceOperator that = (LogicalCTEProduceOperator) o;
        return Objects.equals(cteId, that.cteId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), cteId);
    }

    @Override
    public String toString() {
        return "LogicalCTEProduceOperator{" +
                "cteId='" + cteId + '\'' +
                '}';
    }

    public static class Builder
            extends LogicalOperator.Builder<LogicalCTEProduceOperator, LogicalCTEProduceOperator.Builder> {
        @Override
        protected LogicalCTEProduceOperator newInstance() {
            return new LogicalCTEProduceOperator(-1);
        }

        public void setCteId(int cteId) {
            builder.cteId = cteId;
        }

        @Override
        public LogicalCTEProduceOperator.Builder withOperator(LogicalCTEProduceOperator operator) {
            builder.cteId = operator.cteId;
            return super.withOperator(operator);
        }
    }
}
