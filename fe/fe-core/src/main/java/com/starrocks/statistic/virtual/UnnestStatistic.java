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
package com.starrocks.statistic.virtual;

import com.starrocks.qe.ConnectContext;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.type.ArrayType;
import com.starrocks.type.Type;

public class UnnestStatistic implements VirtualStatistic {
    @Override
    public String getName() {
        return "UNNEST";
    }

    @Override
    public String getAnalyzePropertyKey() {
        return StatsConstants.UNNEST_VIRTUAL_STATISTICS;
    }

    @Override
    public boolean appliesTo(Type columnType) {
        final var isArrayType = columnType.isArrayType();

        if (isArrayType) {
            final var itemType = ((ArrayType) columnType).getItemType();
            return itemType.canStatistic() && !itemType.isCollectionType();
        }

        return false;
    }

    @Override
    public Type getVirtualExpressionType(Type sourceType) {
        if (sourceType.isArrayType()) {
            return ((ArrayType) sourceType).getItemType();
        }

        throw new IllegalArgumentException("UnnestStatistic can only be applied to array types");
    }

    @Override
    public String getVirtualExpression(String columnName) {
        return "unnest(`" + columnName + "`)";
    }

    @Override
    public boolean isQueryingEnabled() {
        return ConnectContext.get() != null && ConnectContext.get().getSessionVariable().isUnnestVirtualStatisticsEnabled();
    }

    @Override
    public boolean requiresLateralJoin() {
        return true;
    }
}
