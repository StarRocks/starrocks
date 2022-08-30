// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.statistics;

import com.google.common.collect.Maps;
import com.starrocks.catalog.Table;

import java.util.List;
import java.util.Map;

public interface StatisticStorage {
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

    default void expireColumnStatistics(Table table, List<String> columns) {
    }

    void addColumnStatistic(Table table, String column, ColumnStatistic columnStatistic);
}
