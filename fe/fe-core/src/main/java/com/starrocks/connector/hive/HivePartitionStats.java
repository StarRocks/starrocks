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
