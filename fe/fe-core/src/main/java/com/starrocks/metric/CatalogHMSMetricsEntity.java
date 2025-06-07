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

package com.starrocks.metric;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Set;

public class CatalogHMSMetricsEntity {

    private static final Logger LOG = LogManager.getLogger(CatalogHMSMetricsEntity.class);
    private static final String GET_PARTITIONS_BY_NAMES = "getPartitionsByNames";
    private static final String GET_PARTITION_COLUMN_STATS = "getPartitionColumnStats";
    private final Map<String, HistogramMetric> histogramMetricsMap = Maps.newHashMap();

    // The strings in this array need to strictly match the method name used in callRPC()
    public static final Set<String> SUPPORTED_RPC_METHODS = Sets.newHashSet(
            "getAllDatabases", "createDatabase", "dropDatabase", "getAllTables", "createTable", "dropTable",
            "alter_table", "alter_partition", "listPartitionNames", "getDatabase", "getTable", "tableExists",
            "getPartition", "add_partitions", "dropPartition", "getTableColumnStatistics"
    );

    private HistogramMetric getPartitionsByNamesLatency;
    private HistogramMetric getPartitionColumnStatsLatency;

    public CatalogHMSMetricsEntity() {
        initCatalogHMSMetrics();
    }

    protected void initCatalogHMSMetrics() {
        for (String method : SUPPORTED_RPC_METHODS) {
            histogramMetricsMap.put(method, new HistogramMetric("hms_" + method + "_latency"));
        }
        getPartitionsByNamesLatency = new HistogramMetric("hms_" + GET_PARTITIONS_BY_NAMES + "_latency");
        histogramMetricsMap.put(GET_PARTITIONS_BY_NAMES, getPartitionsByNamesLatency);

        getPartitionColumnStatsLatency = new HistogramMetric("hms_" + GET_PARTITION_COLUMN_STATS + "_latency");
        histogramMetricsMap.put(GET_PARTITION_COLUMN_STATS, getPartitionColumnStatsLatency);
    }

    public Map<String, HistogramMetric> getHistogramMetrics() {
        return this.histogramMetricsMap;
    }

    public HistogramMetric getHistogramMetric(String methodName) {
        if (!SUPPORTED_RPC_METHODS.contains(methodName)) {
            LOG.warn("Unsupported HMS method: {}", methodName);
        }
        return histogramMetricsMap.get(methodName);
    }

    public void updateGetPartitionsByNamesLatency(long value) {
        this.getPartitionsByNamesLatency.update(value);
    }

    public void updateGetPartitionColumnStatsLatency(long value) {
        this.getPartitionColumnStatsLatency.update(value);
    }
}
