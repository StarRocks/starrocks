package com.starrocks.external.elasticsearch;

import com.google.common.collect.ImmutableMap;
import com.starrocks.external.HiveColumnStatistics;
import com.starrocks.external.hive.HiveCommonStats;

import java.util.Map;

public class HivePartitionStatistics {
    private static final HivePartitionStatistics EMPTY = new HivePartitionStatistics(HiveCommonStats.empty(), ImmutableMap.of());

    private final HiveCommonStats commonStats;
    private final Map<String, HiveColumnStatistics> columnStats;

    public static HivePartitionStatistics empty() {
        return EMPTY;
    }

    public HivePartitionStatistics(HiveCommonStats commonStats, Map<String, HiveColumnStatistics> columnStats) {
        this.commonStats = commonStats;
        this.columnStats = columnStats;
    }

    public HiveCommonStats getCommonStats() {
        return commonStats;
    }

    public Map<String, HiveColumnStatistics> getColumnStats() {
        return columnStats;
    }
}
