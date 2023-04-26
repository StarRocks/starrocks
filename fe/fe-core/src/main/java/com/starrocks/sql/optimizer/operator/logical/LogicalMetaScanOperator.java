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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class LogicalMetaScanOperator extends LogicalScanOperator {
    private final ImmutableMap<Integer, String> aggColumnIdToNames;

    public LogicalMetaScanOperator(Table table,
                                   Map<ColumnRefOperator, Column> columnRefMap) {
        super(OperatorType.LOGICAL_META_SCAN, table, columnRefMap, Maps.newHashMap(),
                Operator.DEFAULT_LIMIT, null, null);
        aggColumnIdToNames = ImmutableMap.of();
    }

    public LogicalMetaScanOperator(Table table,
                                   Map<ColumnRefOperator, Column> columnRefMap,
                                   Map<Integer, String> aggColumnIdToNames) {
        super(OperatorType.LOGICAL_META_SCAN, table, columnRefMap, Maps.newHashMap(),
                Operator.DEFAULT_LIMIT, null, null);
        this.aggColumnIdToNames = ImmutableMap.copyOf(aggColumnIdToNames);
    }

    private LogicalMetaScanOperator(LogicalMetaScanOperator.Builder builder) {
        super(OperatorType.LOGICAL_META_SCAN,
                builder.table,
                builder.colRefToColumnMetaMap,
                builder.columnMetaToColRefMap,
                builder.getLimit(),
                builder.getPredicate(),
                builder.getProjection());
        this.aggColumnIdToNames = ImmutableMap.copyOf(builder.aggColumnIdToNames);
    }

    public Map<Integer, String> getAggColumnIdToNames() {
        return aggColumnIdToNames;
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalMetaScan(this, context);
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
        LogicalMetaScanOperator that = (LogicalMetaScanOperator) o;
        return Objects.equals(aggColumnIdToNames, that.aggColumnIdToNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), aggColumnIdToNames);
    }

    public static class Builder
            extends LogicalScanOperator.Builder<LogicalMetaScanOperator, LogicalMetaScanOperator.Builder> {
        private Map<Integer, String> aggColumnIdToNames;

        @Override
        public LogicalMetaScanOperator build() {
            return new LogicalMetaScanOperator(this);
        }

        @Override
        public LogicalMetaScanOperator.Builder withOperator(LogicalMetaScanOperator operator) {
            super.withOperator(operator);
            this.aggColumnIdToNames = new HashMap<>(operator.aggColumnIdToNames);
            return this;
        }
    }
}