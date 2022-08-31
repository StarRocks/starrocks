// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.statistics;

import com.starrocks.catalog.Table;

import java.util.List;

public interface StatisticStorage {
    ColumnStatistic getColumnStatistic(Table table, String column);

    List<ColumnStatistic> getColumnStatistics(Table table, List<String> columns);

    void expireColumnStatistics(Table table, List<String> columns);

    void addColumnStatistic(Table table, String column, ColumnStatistic columnStatistic);

    default List<ColumnStatistic> getColumnStatisticsSync(Table table, List<String> columns) {
        return getColumnStatistics(table, columns);
    }
}
