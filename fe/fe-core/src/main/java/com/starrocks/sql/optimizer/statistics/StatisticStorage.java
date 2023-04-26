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
import com.starrocks.catalog.Table;

import java.util.List;
import java.util.Map;

public interface StatisticStorage {
    default TableStatistic getTableStatistic(Long tableId, Long partitionId) {
        return TableStatistic.unknown();
    }

    default void refreshTableStatistic(Table table) {
    }

    default void refreshTableStatisticSync(Table table) {
    }

    ColumnStatistic getColumnStatistic(Table table, String column);

    List<ColumnStatistic> getColumnStatistics(Table table, List<String> columns);

    default List<ColumnStatistic> getColumnStatisticsSync(Table table, List<String> columns) {
        return getColumnStatistics(table, columns);
    }

    default Map<String, Histogram> getHistogramStatistics(Table table, List<String> columns) {
        return Maps.newHashMap();
    }

    default Map<String, Histogram> getHistogramStatisticsSync(Table table, List<String> columns) {
        return getHistogramStatistics(table, columns);
    }

    default void expireHistogramStatistics(Long tableId, List<String> columns) {
    }

    default void expireTableAndColumnStatistics(Table table, List<String> columns) {
    }

    void addColumnStatistic(Table table, String column, ColumnStatistic columnStatistic);
}
