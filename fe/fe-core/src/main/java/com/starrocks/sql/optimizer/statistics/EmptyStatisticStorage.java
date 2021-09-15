// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.statistics;

import com.starrocks.catalog.Table;

import java.util.List;
import java.util.stream.Collectors;

// Only for debug
public class EmptyStatisticStorage implements StatisticStorage {
    @Override
    public ColumnStatistic getColumnStatistic(Table table, String column) {
        return ColumnStatistic.unknown();
    }

    @Override
    public List<ColumnStatistic> getColumnStatistics(Table table, List<String> columns) {
        return columns.stream().map(k -> ColumnStatistic.unknown()).collect(Collectors.toList());
    }

    @Override
    public void expireColumnStatistics(Table table, List<String> columns) {
    }

    @Override
    public void addColumnStatistic(Table table, String column, ColumnStatistic columnStatistic) {
    }
}
