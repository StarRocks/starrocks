package com.starrocks.external.hive;

import com.starrocks.sql.optimizer.statistics.ColumnStatistic;

import java.util.Map;

public class HiveStatistics {
    private final double outputRowCount;
    private final Map<String, ColumnStatistic> columnStatistics;


    public HiveStatistics(double outputRowCount, Map<String, ColumnStatistic> columnStatistics) {
        this.outputRowCount = outputRowCount;
        this.columnStatistics = columnStatistics;
    }
}
