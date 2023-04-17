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
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Map;

public class LogicalLandingPadOperator extends LogicalOperator {
    private final OptExpression plan;


    private final Map<ColumnRefOperator, ScalarOperator> columnRefMap;

    public LogicalLandingPadOperator(OptExpression plan, Map<ColumnRefOperator, ScalarOperator> columnRefMap) {
        super(OperatorType.LOGICAL_LANDING_PAD);
        this.plan = plan;
        this.columnRefMap = columnRefMap;
    }

    public LogicalLandingPadOperator(Builder builder) {
        super(OperatorType.LOGICAL_LANDING_PAD);
        this.plan = builder.plan;
        this.columnRefMap = builder.columnRefMap;
    }

    public OptExpression getPlan() {
        return plan;
    }

    public Map<ColumnRefOperator, ScalarOperator> getColumnRefMap() {
        return columnRefMap;
    }
    @Override
    public ColumnRefSet getOutputColumns(ExpressionContext expressionContext) {
        ColumnRefSet columns = new ColumnRefSet();
        for (Map.Entry<ColumnRefOperator, ScalarOperator> kv : columnRefMap.entrySet()) {
            columns.union(kv.getKey());
        }
        return columns;
    }

    public static class Builder
            extends LogicalOperator.Builder<LogicalLandingPadOperator, LogicalLandingPadOperator.Builder> {
        private OptExpression plan;
        private Map<ColumnRefOperator, ScalarOperator> columnRefMap;

        @Override
        public LogicalLandingPadOperator build() {
            return new LogicalLandingPadOperator(this);
        }

        @Override
        public LogicalLandingPadOperator.Builder withOperator(LogicalLandingPadOperator landingPadOperator) {
            super.withOperator(landingPadOperator);
            this.plan = landingPadOperator.plan;
            this.columnRefMap = landingPadOperator.columnRefMap;
            return this;
        }
    }
}
