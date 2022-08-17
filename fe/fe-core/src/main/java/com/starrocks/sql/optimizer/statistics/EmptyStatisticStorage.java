// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.statistics;

import com.google.common.collect.Maps;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

// Only for debug
public class EmptyStatisticStorage implements StatisticStorage {
    @Override
    public ColumnStatistic getColumnStatistic(Table table, String column) {
        return ColumnStatistic.unknown();
    }

    @Override
    public List<ColumnStatistic> getColumnStatistics(Table table, List<String> columns) {
        return columns.stream().map(k -> getColumnStatistic(table, k)).collect(Collectors.toList());
    }

    @Override
    public Map<ColumnRefOperator, Histogram> getHistogramStatistics(Table table, List<ColumnRefOperator> columns) {
        return Maps.newHashMap();
    }

    @Override
    public void addColumnStatistic(Table table, String column, ColumnStatistic columnStatistic) {
    }
}
