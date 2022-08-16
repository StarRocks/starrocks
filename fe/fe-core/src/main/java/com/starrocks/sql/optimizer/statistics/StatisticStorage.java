// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.statistics;

import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.List;
import java.util.Map;

public interface StatisticStorage {
    ColumnStatistic getColumnStatistic(Table table, String column);

    List<ColumnStatistic> getColumnStatistics(Table table, List<String> columns);

    Map<ColumnRefOperator, Histogram> getHistogramStatistics(Table table, List<ColumnRefOperator> columns);

    default void expireHistogramStatistics(Long tableId, List<String> columns) {
    }

    default void expireColumnStatistics(Table table, List<String> columns) {
    }

    void addColumnStatistic(Table table, String column, ColumnStatistic columnStatistic);
}
