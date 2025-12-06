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

import java.util.Map;

public class LogicalCacheStatsScanOperator extends LogicalScanOperator {

    private Map<Integer, String> aggColumnIdToNames = ImmutableMap.of();

    private LogicalCacheStatsScanOperator() {
        super(OperatorType.LOGICAL_CACHE_STATS_SCAN);
    }

    public Map<Integer, String> getAggColumnIdToNames() {
        return aggColumnIdToNames;
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalCacheStatsScan(this, context);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder
            extends LogicalScanOperator.Builder<LogicalCacheStatsScanOperator, Builder> {

        @Override
        protected LogicalCacheStatsScanOperator newInstance() {
            return new LogicalCacheStatsScanOperator();
        }

        @Override
        public Builder withOperator(LogicalCacheStatsScanOperator operator) {
            super.withOperator(operator);
            builder.aggColumnIdToNames = ImmutableMap.copyOf(operator.aggColumnIdToNames);
            return this;
        }

        public Builder setAggColumnIdToNames(Map<Integer, String> aggColumnIdToNames) {
            builder.aggColumnIdToNames = aggColumnIdToNames;
            return this;
        }
    }
}
