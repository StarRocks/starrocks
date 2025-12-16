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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.Column;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

// @TODO getName maynot be the base column name
public class LogicalMetaScanOperator extends LogicalScanOperator {
    private long selectedIndexId = -1;
    // agg column id -> (agg_function_name, column)
    private Map<Integer, Pair<String, Column>> aggColumnIdToColumns = ImmutableMap.of();
    private List<String> selectPartitionNames = Collections.emptyList();

    private LogicalMetaScanOperator() {
        super(OperatorType.LOGICAL_META_SCAN);
    }

    public Map<Integer, Pair<String, Column>> getAggColumnIdToColumns() {
        return aggColumnIdToColumns;
    }

    public List<String> getSelectPartitionNames() {
        return selectPartitionNames;
    }

    public long getSelectedIndexId() {
        return selectedIndexId;
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
        return Objects.equals(aggColumnIdToColumns, that.aggColumnIdToColumns) &&
                Objects.equals(selectPartitionNames, that.selectPartitionNames) &&
                selectedIndexId == that.selectedIndexId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), aggColumnIdToColumns);
    }

    public static LogicalMetaScanOperator.Builder builder() {
        return new LogicalMetaScanOperator.Builder();
    }

    public static class Builder
            extends LogicalScanOperator.Builder<LogicalMetaScanOperator, LogicalMetaScanOperator.Builder> {

        @Override
        protected LogicalMetaScanOperator newInstance() {
            return new LogicalMetaScanOperator();
        }

        @Override
        public LogicalMetaScanOperator.Builder withOperator(LogicalMetaScanOperator operator) {
            super.withOperator(operator);
            builder.aggColumnIdToColumns = ImmutableMap.copyOf(operator.aggColumnIdToColumns);
            builder.selectPartitionNames = operator.selectPartitionNames;
            builder.columnAccessPaths = ImmutableList.copyOf(operator.columnAccessPaths);
            return this;
        }

        public LogicalMetaScanOperator.Builder setAggColumnIdToColumns(
                Map<Integer, Pair<String, Column>> aggColumnIdToColumns) {
            builder.aggColumnIdToColumns = aggColumnIdToColumns;
            return this;
        }

        public LogicalMetaScanOperator.Builder setSelectPartitionNames(List<String> selectPartitionNames) {
            builder.selectPartitionNames = selectPartitionNames;
            return this;
        }

        public LogicalMetaScanOperator.Builder setSelectedIndexId(long selectedIndexId) {
            builder.selectedIndexId = selectedIndexId;
            return this;
        }
    }
}