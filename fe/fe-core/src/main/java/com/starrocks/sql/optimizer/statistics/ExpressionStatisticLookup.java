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

package com.starrocks.sql.optimizer.statistics;

import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.statistic.BasicStatsMeta;
import com.starrocks.statistic.ExpressionStatsMeta;
import com.starrocks.statistic.expression.BuiltinExpressionStatistics;
import com.starrocks.statistic.expression.ExpressionStatistic;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ExpressionStatisticLookup {
    private ExpressionStatisticLookup() {
    }

    public static Optional<ColumnStatistic> lookupScalar(CallOperator call, ColumnRefFactory columnRefFactory) {
        if (columnRefFactory == null) {
            return Optional.empty();
        }

        Optional<ExpressionStatistic> expressionStatistic = BuiltinExpressionStatistics.find(call);
        if (expressionStatistic.isEmpty()) {
            return Optional.empty();
        }

        ColumnRefOperator columnRef = (ColumnRefOperator) call.getChild(0);
        Pair<Table, Column> tableAndColumn = columnRefFactory.getTableAndColumn(columnRef);
        return lookup(tableAndColumn, expressionStatistic.get());
    }

    public static Map<ColumnRefOperator, ColumnStatistic> lookupTableFunction(
            String functionName, List<ColumnRefOperator> fnParamColumnRefs, List<ColumnRefOperator> fnResultColRefs,
            ColumnRefFactory columnRefFactory) {
        Map<ColumnRefOperator, ColumnStatistic> result = Maps.newHashMap();
        if (columnRefFactory == null) {
            return result;
        }

        Optional<ExpressionStatistic> expressionStatistic = BuiltinExpressionStatistics.findTableFunction(functionName);
        if (expressionStatistic.isEmpty()) {
            return result;
        }

        int count = Math.min(fnParamColumnRefs.size(), fnResultColRefs.size());
        for (int i = 0; i < count; i++) {
            Pair<Table, Column> tableAndColumn = columnRefFactory.getTableAndColumn(fnParamColumnRefs.get(i));
            lookup(tableAndColumn, expressionStatistic.get())
                    .ifPresent(statistic -> result.put(fnResultColRefs.get(i), statistic));
        }
        return result;
    }

    private static Optional<ColumnStatistic> lookup(Pair<Table, Column> tableAndColumn,
                                                    ExpressionStatistic expressionStatistic) {
        if (tableAndColumn == null || tableAndColumn.first == null || tableAndColumn.second == null) {
            return Optional.empty();
        }
        if (!expressionStatistic.appliesTo(tableAndColumn.second.getType())) {
            return Optional.empty();
        }

        BasicStatsMeta basicStatsMeta = GlobalStateMgr.getCurrentState().getAnalyzeMgr()
                .getTableBasicStatsMeta(tableAndColumn.first.getId());
        if (basicStatsMeta == null) {
            return Optional.empty();
        }

        Optional<ExpressionStatsMeta> meta = basicStatsMeta.getExpressionStatsMeta(
                expressionStatistic.getKey(tableAndColumn.second));
        if (meta.isEmpty()) {
            return Optional.empty();
        }

        ColumnStatistic statistic = GlobalStateMgr.getCurrentState().getStatisticStorage()
                .getColumnStatistic(tableAndColumn.first, meta.get().getStorageColumnName());
        if (statistic == null || statistic.isUnknown()) {
            return Optional.empty();
        }
        return Optional.of(statistic);
    }
}
