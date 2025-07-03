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

package com.starrocks.epack.warehouse.cngroup;

import com.google.common.collect.Lists;
import com.starrocks.epack.warehouse.Cluster;
import com.starrocks.epack.warehouse.LocalWarehouse;
import com.starrocks.epack.warehouse.WarehouseSlotManager;
import com.starrocks.metric.GaugeMetric;
import com.starrocks.metric.LongCounterMetric;
import com.starrocks.metric.Metric;
import com.starrocks.metric.MetricLabel;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.warehouse.cngroup.ComputeResource;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class CNGroupMetricEntity {
    private final CNGroupResource cnGroupResource;
    private final long warehouseId;
    private final String warehouseName;
    private final Cluster cluster;
    private final WarehouseSlotManager warehouseSlotManager;

    public final GaugeMetric<Long> cnGroupNodeCount;
    public final GaugeMetric<Long> cnGroupAliveNodeCount;
    public final GaugeMetric<Long> cnGroupQueryRunningCount;
    public final GaugeMetric<Integer> cnGroupQueryStatus;

    public final LongCounterMetric cnGroupQueryScheduledCount;
    public final LongCounterMetric cnGroupQuerySuccessCount;
    public final LongCounterMetric cnGroupQueryFailedCount;
    public final GaugeMetric<Long> cngroupMaxRunningQueriesCount;

    private final AtomicLong cnGroupQueryMaxLatencyValueMs = new AtomicLong(0);
    private final AtomicLong cnGroupQuerySumLatencyValueMs = new AtomicLong(0);

    public final GaugeMetric<Double> cnGroupQueryMaxLatencyMs;
    public final GaugeMetric<Double> cnGroupQueryAvgLatencyMs;
    public final GaugeMetric<Double> cnGroupAvgCpuUsedPermille;

    private volatile long lastUpdateTimeMs = System.currentTimeMillis();
    private volatile CNGroupResourceUsage cnGroupResourceUsage;

    private final List<Metric> metrics = Lists.newArrayList();
    public CNGroupMetricEntity(LocalWarehouse localWarehouse,
                               Cluster cluster,
                               WarehouseSlotManager warehouseSlotManager) {
        this.cluster = cluster;
        this.warehouseSlotManager = warehouseSlotManager;

        this.cnGroupResource = CNGroupResource.of(localWarehouse.getId(), cluster.getWorkerGroupId());
        this.warehouseId = cnGroupResource.getWarehouseId();
        this.warehouseName = localWarehouse.getName();
        final WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();

        this.cnGroupNodeCount = new GaugeMetric<Long>("warehouse_cngroup", Metric.MetricUnit.NOUNIT,
                "current cngroup nodes count") {
            @Override
            public Long getValue() {
                return (long) warehouseManager.getAllComputeNodeIds(cnGroupResource).size();
            }
        };
        cnGroupNodeCount.addLabel(new MetricLabel("field", "cngroup_nodes_count"));
        metrics.add(cnGroupNodeCount);

        this.cnGroupAliveNodeCount = new GaugeMetric<Long>("warehouse_cngroup", Metric.MetricUnit.NOUNIT,
                "current cngroup alive nodes count") {
            @Override
            public Long getValue() {
                return (long) warehouseManager.getAliveComputeNodes(cnGroupResource).size();
            }
        };
        cnGroupAliveNodeCount.addLabel(new MetricLabel("field", "cngroup_alive_nodes_count"));
        metrics.add(cnGroupAliveNodeCount);

        this.cnGroupQueryRunningCount = new GaugeMetric<Long>("warehouse_cngroup", Metric.MetricUnit.NOUNIT,
                "current warehouse cngroup running queries count in current FE") {
            @Override
            public Long getValue() {
                Map<ComputeResource, List<ConnectContext>> currentContexts =
                        warehouseSlotManager.getCurrentConnectionsByComputeResource();
                return currentContexts.getOrDefault(cnGroupResource, List.of()).stream()
                        .filter(conn -> conn.getState().isRunning())
                        .count();
            }
        };
        cnGroupQueryRunningCount.addLabel(new MetricLabel("field", "running_queries_count"));
        metrics.add(cnGroupQueryRunningCount);

        this.cnGroupQueryStatus = new GaugeMetric<Integer>("warehouse_cngroup", Metric.MetricUnit.NOUNIT,
                "current warehouse cngroup query status") {
            @Override
            public Integer getValue() {
                return cluster.isEnabled() ? 1 : 0;
            }
        };
        cnGroupQueryStatus.addLabel(new MetricLabel("field", "cngroup_status"));
        metrics.add(cnGroupQueryStatus);

        this.cnGroupQueryScheduledCount = new LongCounterMetric("warehouse_cngroup", Metric.MetricUnit.NOUNIT,
                "current warehouse cngroup scheduled queries count");
        cnGroupQueryScheduledCount.addLabel(new MetricLabel("field", "scheduled_queries_count"));
        metrics.add(cnGroupQueryScheduledCount);

        this.cnGroupQuerySuccessCount = new LongCounterMetric("warehouse_cngroup", Metric.MetricUnit.NOUNIT,
                "current warehouse cngroup scheduled success queries count");
        cnGroupQuerySuccessCount.addLabel(new MetricLabel("field", "success_queries_count"));
        metrics.add(cnGroupQuerySuccessCount);

        this.cnGroupQueryFailedCount = new LongCounterMetric("warehouse_cngroup", Metric.MetricUnit.NOUNIT,
                "current warehouse cngroup scheduled failed queries count");
        cnGroupQueryFailedCount.addLabel(new MetricLabel("field", "failed_queries_count"));
        metrics.add(cnGroupQueryFailedCount);

        this.cnGroupQueryMaxLatencyMs = new GaugeMetric<Double>("warehouse_cngroup", Metric.MetricUnit.NOUNIT,
                "current cngroup max query latency in ms") {
            @Override
            public Double getValue() {
                return (double) cnGroupQueryMaxLatencyValueMs.get();
            }
        };
        cnGroupQueryMaxLatencyMs.addLabel(new MetricLabel("field", "query_max_latency_ms"));
        metrics.add(cnGroupQueryMaxLatencyMs);

        this.cnGroupQueryAvgLatencyMs = new GaugeMetric<Double>("warehouse_cngroup", Metric.MetricUnit.NOUNIT,
                "current cngroup average query latency in ms") {
            @Override
            public Double getValue() {
                if (cnGroupQueryScheduledCount.getValue() == 0) {
                    return 0.0;
                }
                return cnGroupQuerySumLatencyValueMs.get() / (double) cnGroupQueryScheduledCount.getValue();
            }
        };
        cnGroupQueryAvgLatencyMs.addLabel(new MetricLabel("field", "query_avg_latency_ms"));
        metrics.add(cnGroupQueryAvgLatencyMs);

        this.cnGroupAvgCpuUsedPermille = new GaugeMetric<Double>("warehouse_cngroup", Metric.MetricUnit.NOUNIT,
                "current cngroup average used cpu per mille") {
            @Override
            public Double getValue() {
                try {
                    CNGroupResourceUsage cnGroupResourceUsage = getCNGroupResourceUsage();
                    Double avgCpuUsedPermille = cnGroupResourceUsage.getAvgCpuUsedPermille();
                    if (avgCpuUsedPermille == null || avgCpuUsedPermille.isNaN() || avgCpuUsedPermille == Double.MAX_VALUE) {
                        // If the average CPU used is null or NaN, return 0.0
                        return -1.0;
                    }
                    return avgCpuUsedPermille;
                } catch (Exception e) {
                    // If there is an error in calculating the average CPU used, return 0.0
                    return 0.0;
                }
            }
        };
        cnGroupAvgCpuUsedPermille.addLabel(new MetricLabel("field", "avg_cpu_used_permille"));
        metrics.add(cnGroupAvgCpuUsedPermille);

        this.cngroupMaxRunningQueriesCount = new GaugeMetric<Long>("warehouse_cngroup", Metric.MetricUnit.NOUNIT,
                "current cngroup max running queries count for all compute nodes") {
            @Override
            public Long getValue() {
                CNGroupResourceUsage cnGroupResourceUsage = getCNGroupResourceUsage();
                Long maxRunningQueries = cnGroupResourceUsage.getMaxRunningQueries();
                if (maxRunningQueries == null || maxRunningQueries == Long.MAX_VALUE) {
                    // If the max running queries is null, return 0
                    return -1L;
                }
                return maxRunningQueries;
            }
        };
        cngroupMaxRunningQueriesCount.addLabel(new MetricLabel("field", "max_compute_node_running_queries_count"));
        metrics.add(cngroupMaxRunningQueriesCount);
    }

    private synchronized CNGroupResourceUsage getCNGroupResourceUsage() {
        if (System.currentTimeMillis() - lastUpdateTimeMs < 1000 && cnGroupResourceUsage != null) {
            // If the last update time is less than 1 second ago, return the cached value
            return cnGroupResourceUsage;
        }
        this.lastUpdateTimeMs = System.currentTimeMillis();
        List<ComputeNode> aliveComputeNodes =
                GlobalStateMgr.getCurrentState().getWarehouseMgr().getAliveComputeNodes(cnGroupResource);
        this.cnGroupResourceUsage = CNGroupResourceUsage.of(cnGroupResource, aliveComputeNodes);
        return this.cnGroupResourceUsage;
    }

    public List<Metric> getMetrics() {
        return this.metrics;
    }

    public long getWarehouseId() {
        return warehouseId;
    }

    public String getWarehouseName() {
        return warehouseName;
    }

    public String getCNGroupName() {
        return cluster.getName();
    }

    public void incrSuccessQueryLatencyMs(long latencyMs) {
        cnGroupQueryScheduledCount.increase(1L);
        cnGroupQuerySuccessCount.increase(1L);
        updateQueryLatency(latencyMs);
    }

    public void incrFailedQueryLatencyMs(long latencyMs) {
        cnGroupQueryScheduledCount.increase(1L);
        cnGroupQueryFailedCount.increase(1L);
        updateQueryLatency(latencyMs);
    }

    private void updateQueryLatency(long latencyMs) {
        // update max sum latency
        cnGroupQueryMaxLatencyValueMs.updateAndGet(current -> Math.max(current, latencyMs));

        // update sum latency
        cnGroupQuerySumLatencyValueMs.addAndGet(latencyMs);
    }
}
