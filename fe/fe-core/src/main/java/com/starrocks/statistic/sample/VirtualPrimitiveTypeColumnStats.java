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

package com.starrocks.statistic.sample;

import com.starrocks.statistic.virtual.VirtualStatistic;
import com.starrocks.type.Type;

public class VirtualPrimitiveTypeColumnStats extends PrimitiveTypeColumnStats {
    private final VirtualStatistic virtualStatistic;
    private final String baseColumnName;

    public VirtualPrimitiveTypeColumnStats(String columnName, Type columnType, VirtualStatistic virtualStatistic) {
        super(virtualStatistic.getVirtualColumnName(columnName), virtualStatistic.getVirtualExpressionType(columnType));
        this.virtualStatistic = virtualStatistic;
        this.baseColumnName = columnName;
    }

    @Override
    public String getLateralJoin() {
        if (virtualStatistic.requiresLateralJoin()) {
            return ", %s `%s`(`%s`)".formatted(virtualStatistic.getVirtualExpression(baseColumnName), columnName, columnName);
        }
        return super.getLateralJoin();
    }
}
