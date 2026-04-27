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

import com.starrocks.catalog.Column;
import com.starrocks.statistic.expression.ExpressionStatistic;
import org.apache.commons.lang.StringEscapeUtils;

public class ExpressionColumnStats extends PrimitiveTypeColumnStats {
    private final Column baseColumn;
    private final ExpressionStatistic expressionStatistic;

    public ExpressionColumnStats(Column baseColumn, ExpressionStatistic expressionStatistic) {
        super(expressionStatistic.getStorageColumnName(baseColumn),
                expressionStatistic.getExpressionType(baseColumn.getType()));
        this.baseColumn = baseColumn;
        this.expressionStatistic = expressionStatistic;
    }

    @Override
    public String getColumnNameStr() {
        return StringEscapeUtils.escapeSql(expressionStatistic.getStorageColumnName(baseColumn));
    }

    @Override
    public String getQuotedColumnName() {
        if (expressionStatistic.requiresLateralJoin()) {
            return "`" + expressionStatistic.getValueColumnName(baseColumn.getName()) + "`";
        }
        return expressionStatistic.getExpressionSql(baseColumn.getName());
    }

    @Override
    public boolean supportMeta() {
        return false;
    }

    @Override
    public String getLateralJoin() {
        if (!expressionStatistic.requiresLateralJoin()) {
            return "";
        }
        String storageColumnName = expressionStatistic.getStorageColumnName(baseColumn);
        String valueColumnName = expressionStatistic.getValueColumnName(baseColumn.getName());
        return ", " + expressionStatistic.getExpressionSql(baseColumn.getName()) + " `" + storageColumnName + "`(`" +
                valueColumnName + "`)";
    }
}
