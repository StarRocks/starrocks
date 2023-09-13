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

    public static HivePartitionStats fromCommonStats(long rowNums, long totalFileBytes) {
        HiveCommonStats commonStats = new HiveCommonStats(rowNums, totalFileBytes);
        return new HivePartitionStats(commonStats, ImmutableMap.of());
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

    // only used to update the parameters of partition or table.
    // TODO(stephen): collect and merge colum statistics.
    public static HivePartitionStats merge(HivePartitionStats current, HivePartitionStats update) {
        if (current.getCommonStats().getRowNums() == -1 || update.getCommonStats().getRowNums() <= 0) {
            return current;
        } else if (current.getCommonStats().getRowNums() == 0 && update.getCommonStats().getRowNums() > 0) {
            return update;
        }

        return new HivePartitionStats(
                reduce(current.getCommonStats(), update.getCommonStats(), ReduceOperator.ADD),
                // TODO(stephen): collect and merge column statistics
                current.getColumnStats());
    }

    public static HivePartitionStats reduce(HivePartitionStats first, HivePartitionStats second, ReduceOperator operator) {
        return HivePartitionStats.fromCommonStats(
                reduce(first.getCommonStats().getRowNums(), second.getCommonStats().getRowNums(), operator),
                reduce(first.getCommonStats().getTotalFileBytes(), second.getCommonStats().getTotalFileBytes(), operator));
    }

    public static HiveCommonStats reduce(HiveCommonStats current, HiveCommonStats update, ReduceOperator operator) {
        return new HiveCommonStats(
                reduce(current.getRowNums(), update.getRowNums(), operator),
                reduce(current.getTotalFileBytes(), update.getTotalFileBytes(), operator));
    }

    public static long reduce(long current, long update, ReduceOperator operator) {
        if (current >= 0 && update >= 0) {
            switch (operator) {
                case ADD:
                    return current + update;
                case SUBTRACT:
                    return current - update;
                case MAX:
                    return Math.max(current, update);
                case MIN:
                    return Math.min(current, update);
            }
            throw new IllegalArgumentException("Unexpected operator: " + operator);
        }

        return 0;
    }

    public enum ReduceOperator {
        ADD,
        SUBTRACT,
        MIN,
        MAX,
    }
}
