// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive;

import com.google.common.collect.ImmutableMap;

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

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("HivePartitionStatistics{");
        sb.append("commonStats=").append(commonStats);
        sb.append(", columnStats=").append(columnStats);
        sb.append('}');
        return sb.toString();
    }
}
