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

package com.starrocks.statistic.base;

import com.starrocks.statistic.virtual.VirtualStatistic;
import com.starrocks.type.Type;
import org.apache.commons.lang.StringEscapeUtils;

/**
 * Represents virtual column statistics that are derived from a base column through a virtual transformation
 * (e.g., unnest of an array column).
 */
public class VirtualColumnStats extends PrimitiveTypeColumnStats {

    private final String baseColumnName;
    private final VirtualStatistic virtualStatistic;

    public VirtualColumnStats(String baseColumnName, Type baseColumnType, VirtualStatistic virtualStatistic) {
        // Pass the virtual column name and virtual type to the parent
        super(virtualStatistic.getVirtualColumnName(baseColumnName),
                virtualStatistic.getVirtualExpressionType(baseColumnType));
        this.baseColumnName = baseColumnName;
        this.virtualStatistic = virtualStatistic;
    }

    @Override
    public String getColumnNameStr() {
        return StringEscapeUtils.escapeSql(virtualStatistic.getVirtualColumnName(baseColumnName));
    }

    @Override
    public String getQuotedColumnName() {
        return "`" + getColumnNameStr() + "`";
    }

    public String getVirtualExpression() {
        return virtualStatistic.getVirtualExpression(baseColumnName);
    }

    @Override
    public boolean supportMeta() {
        return false;
    }

    @Override
    public String getLateralJoin() {
        if (virtualStatistic.requiresLateralJoin()) {
            return ", %s `%s`(`%s`)".formatted(getVirtualExpression(), getColumnNameStr(), getColumnNameStr());
        }
        return super.getLateralJoin();
    }
}


