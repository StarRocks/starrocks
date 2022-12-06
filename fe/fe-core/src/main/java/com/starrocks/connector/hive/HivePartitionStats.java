// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hive;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class HivePartitionStats {
    private static final HivePartitionStats EMPTY = new HivePartitionStats(HiveCommonStats.empty(), ImmutableMap.of());

    private final HiveCommonStats commonStats;
    private final Map<String, HiveColumnStats> columnStats;

    public static HivePartitionStats empty() {
        return EMPTY;
    }

    public HivePartitionStats(HiveCommonStats commonStats, Map<String, HiveColumnStats> columnStats) {
        this.commonStats = commonStats;
        this.columnStats = columnStats;
    }

    public HiveCommonStats getCommonStats() {
        return commonStats;
    }

    public Map<String, HiveColumnStats> getColumnStats() {
        return columnStats;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("HivePartitionStats{");
        sb.append("commonStats=").append(commonStats);
        sb.append(", columnStats=").append(columnStats);
        sb.append('}');
        return sb.toString();
    }
}
