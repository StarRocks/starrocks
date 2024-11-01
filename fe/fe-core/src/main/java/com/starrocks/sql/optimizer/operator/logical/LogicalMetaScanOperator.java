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
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class LogicalMetaScanOperator extends LogicalScanOperator {
    private Map<Integer, String> aggColumnIdToNames = ImmutableMap.of();
    private List<String> selectPartitionNames = Collections.emptyList();

    private LogicalMetaScanOperator() {
        super(OperatorType.LOGICAL_META_SCAN);
    }

    public Map<Integer, String> getAggColumnIdToNames() {
        return aggColumnIdToNames;
    }

    public List<String> getSelectPartitionNames() {
        return selectPartitionNames;
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
        return Objects.equals(aggColumnIdToNames, that.aggColumnIdToNames) &&
                Objects.equals(selectPartitionNames, that.selectPartitionNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), aggColumnIdToNames);
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
            builder.aggColumnIdToNames = ImmutableMap.copyOf(operator.aggColumnIdToNames);
            builder.selectPartitionNames = operator.selectPartitionNames;
            return this;
        }

        public LogicalMetaScanOperator.Builder setAggColumnIdToNames(Map<Integer, String> aggColumnIdToNames) {
            builder.aggColumnIdToNames = aggColumnIdToNames;
            return this;
        }

        public LogicalMetaScanOperator.Builder setSelectPartitionNames(List<String> selectPartitionNames) {
            builder.selectPartitionNames = selectPartitionNames;
            return this;
        }
    }
}