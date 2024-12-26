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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/metric/JsonMetricVisitor.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.metric;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.starrocks.monitor.jvm.GcNames;
import com.starrocks.monitor.jvm.JvmStats;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.system.SystemInfoService;
import org.apache.commons.collections.ListUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class JsonMetricVisitor extends MetricVisitor {
    private boolean isFirstElement;
    private final StringBuilder sb;

    private static final String MILLISECONDS = "milliseconds";
    private static final String QUANTILE = "quantile";
    private static final String NOUNIT = "nounit";
    private static final String TYPE = "type";
    private static final String STATUS = "status";
    private static final String TOTAL = "total";
    private static final String FE_NODE_NUM = "fe_node_num";
    private static final String BE_NODE_NUM = "be_node_num";
    private static final String CN_NODE_NUM = "cn_node_num";
    private static final String BROKER_NODE_NUM = "broker_node_num";

    public JsonMetricVisitor(String prefix) {
        super(prefix);
        isFirstElement = true;
        sb = new StringBuilder();
    }

    private void addGcMetric(String metricName, JvmStats.GarbageCollector gc) {
        buildMetric(metricName, "nounit", String.valueOf(gc.getCollectionCount()),
                List.of(new MetricLabel("type", "count")));
        buildMetric(metricName, "milliseconds", String.valueOf(gc.getCollectionTime().getMillis()),
                List.of(new MetricLabel("type", "time")));
    }

    private void addMemPoolMetric(String metricName, JvmStats.MemoryPool memPool) {
        buildMetric(metricName, "bytes", String.valueOf(memPool.getCommitted()),
                List.of(new MetricLabel("type", "committed")));
        buildMetric(metricName, "bytes", String.valueOf(memPool.getUsed()),
                List.of(new MetricLabel("type", "used")));
    }

    @Override
    public void visitJvm(JvmStats jvmStats) {
        // gc
        for (JvmStats.GarbageCollector gc : jvmStats.getGc()) {
            if (gc.getName().equalsIgnoreCase(GcNames.YOUNG)) {
                addGcMetric("jvm_young_gc", gc);
            } else if (gc.getName().equalsIgnoreCase(GcNames.OLD)) {
                addGcMetric("jvm_old_gc", gc);
            }
        }

        // mem overall
        JvmStats.Mem mem = jvmStats.getMem();
        buildMetric("jvm_heap_size_bytes", "bytes", String.valueOf(mem.getHeapMax()),
                List.of(new MetricLabel("type", "max")));
        buildMetric("jvm_heap_size_bytes", "bytes", String.valueOf(mem.getHeapCommitted()),
                List.of(new MetricLabel("type", "committed")));
        buildMetric("jvm_heap_size_bytes", "bytes", String.valueOf(mem.getHeapUsed()),
                List.of(new MetricLabel("type", "used")));

        // mem pool
        for (JvmStats.MemoryPool memPool : jvmStats.getMem()) {
            if (memPool.getName().equalsIgnoreCase(GcNames.PERM)) {
                double percent = 0.0;
                if (memPool.getCommitted() > 0) {
                    percent = 100 * ((double) memPool.getUsed() / memPool.getCommitted());
                }
                buildMetric("jvm_size_percent", "percent", String.valueOf(percent),
                        List.of(new MetricLabel("type", GcNames.PERM)));
            } else if (memPool.getName().equalsIgnoreCase(GcNames.OLD)) {
                double percent = 0.0;
                if (memPool.getCommitted() > 0) {
                    percent = 100 * ((double) memPool.getUsed() / memPool.getCommitted());
                }
                // **NOTICE**: We shouldn't use 'jvm_size_percent' as a metric name, it should be a type,
                // but for compatibility reason, we won't remove it.
                buildMetric("jvm_size_percent", "percent", String.valueOf(percent),
                        List.of(new MetricLabel("type", GcNames.OLD)));

                // {"metric":"jvm_old_size_bytes","type":"committed","unit":"bytes"}
                // {"metric":"jvm_old_size_bytes","type":"used","unit":"bytes"}
                addMemPoolMetric("jvm_old_size_bytes", memPool);
            } else if (memPool.getName().equalsIgnoreCase(GcNames.YOUNG)) {
                // {"metric":"jvm_young_size_bytes","type":"committed","unit":"bytes"}
                // {"metric":"jvm_young_size_bytes","type":"used","unit":"bytes"}
                addMemPoolMetric("jvm_young_size_bytes", memPool);
            }
        }

        // buffer pool
        for (JvmStats.BufferPool pool : jvmStats.getBufferPools()) {
            if (pool.getName().equalsIgnoreCase("direct")) {
                buildMetric("jvm_direct_buffer_pool_size_bytes", "bytes", String.valueOf(pool.getTotalCapacity()),
                        List.of(new MetricLabel("type", "capacity")));
                buildMetric("jvm_direct_buffer_pool_size_bytes", "bytes", String.valueOf(pool.getUsed()),
                        List.of(new MetricLabel("type", "used")));
            }
        }
    }

    @Override
    public void visit(@SuppressWarnings("rawtypes") Metric metric) {
        @SuppressWarnings("unchecked")
        List<MetricLabel> labels = metric.getLabels();
        buildMetric(metric.getName(), metric.getUnit().name().toLowerCase(),
                metric.getValue().toString(), labels);
    }

    @Override
    public void visitHistogram(HistogramMetric histogram) {
        final String fullName = prefix + "_" + histogram.getName().replace("\\.", "_");
        Snapshot snapshot = histogram.getSnapshot();
        List<MetricLabel> labels = histogram.getLabels();
        buildMetric(fullName, MILLISECONDS, String.valueOf(snapshot.get75thPercentile()),
                ListUtils.union(labels, Collections.singletonList(new MetricLabel(QUANTILE, "0.75"))));
        buildMetric(fullName, MILLISECONDS, String.valueOf(snapshot.get95thPercentile()),
                ListUtils.union(labels, Collections.singletonList(new MetricLabel(QUANTILE, "0.95"))));
        buildMetric(fullName, MILLISECONDS, String.valueOf(snapshot.get98thPercentile()),
                ListUtils.union(labels, Collections.singletonList(new MetricLabel(QUANTILE, "0.98"))));
        buildMetric(fullName, MILLISECONDS, String.valueOf(snapshot.get99thPercentile()),
                ListUtils.union(labels, Collections.singletonList(new MetricLabel(QUANTILE, "0.99"))));
        buildMetric(fullName, MILLISECONDS, String.valueOf(snapshot.get999thPercentile()),
                ListUtils.union(labels, Collections.singletonList(new MetricLabel(QUANTILE, "0.999"))));

        buildMetric(fullName + "_sum", MILLISECONDS,
                String.valueOf(histogram.getCount() * snapshot.getMean()), labels);
        buildMetric(fullName + "_count", NOUNIT,
                String.valueOf(histogram.getCount()), labels);
    }

    @Override
    public void visitHistogram(String name, Histogram histogram) {
        // skip HistogramMetric since it needs extra processing
        if (histogram instanceof HistogramMetric) {
            visitHistogram((HistogramMetric) histogram);
            return;
        }
        final String fullName = prefix + "_" + name.replace("\\.", "_");
        Snapshot snapshot = histogram.getSnapshot();

        buildMetric(fullName, MILLISECONDS, String.valueOf(snapshot.get75thPercentile()),
                Collections.singletonList(new MetricLabel(QUANTILE, "0.75")));
        buildMetric(fullName, MILLISECONDS, String.valueOf(snapshot.get95thPercentile()),
                Collections.singletonList(new MetricLabel(QUANTILE, "0.95")));
        buildMetric(fullName, MILLISECONDS, String.valueOf(snapshot.get98thPercentile()),
                Collections.singletonList(new MetricLabel(QUANTILE, "0.98")));
        buildMetric(fullName, MILLISECONDS, String.valueOf(snapshot.get99thPercentile()),
                Collections.singletonList(new MetricLabel(QUANTILE, "0.99")));
        buildMetric(fullName, MILLISECONDS, String.valueOf(snapshot.get999thPercentile()),
                Collections.singletonList(new MetricLabel(QUANTILE, "0.999")));

        buildMetric(fullName + "_sum", MILLISECONDS, String.valueOf(histogram.getCount() * snapshot.getMean()),
                null);
        buildMetric(fullName + "_count", NOUNIT, String.valueOf(histogram.getCount()), null);
    }

    @Override
    public void getNodeInfo() {
        final String NODE_INFO = "node_info";
        final NodeMgr nodeMgr = GlobalStateMgr.getCurrentState().getNodeMgr();
        final SystemInfoService systemInfoService = nodeMgr.getClusterInfo();

        buildMetric(NODE_INFO, NOUNIT, String.valueOf(nodeMgr.getFrontends(null).size()),
                Arrays.asList(new MetricLabel(TYPE, FE_NODE_NUM), new MetricLabel(STATUS, TOTAL)));
        buildMetric(NODE_INFO, NOUNIT, String.valueOf(systemInfoService.getTotalBackendNumber()),
                Arrays.asList(new MetricLabel(TYPE, BE_NODE_NUM), new MetricLabel(STATUS, TOTAL)));
        buildMetric(NODE_INFO, NOUNIT, String.valueOf(systemInfoService.getAliveBackendNumber()),
                Arrays.asList(new MetricLabel(TYPE, BE_NODE_NUM), new MetricLabel(STATUS, "alive")));
        buildMetric(NODE_INFO, NOUNIT,
                String.valueOf(systemInfoService.getDecommissionedBackendIds().size()),
                Arrays.asList(new MetricLabel(TYPE, BE_NODE_NUM), new MetricLabel(STATUS, "decommissioned")));
        buildMetric(NODE_INFO, NOUNIT, String.valueOf(
                        GlobalStateMgr.getCurrentState().getBrokerMgr().getAllBrokers().stream().filter(b -> !b.isAlive)
                                .count()),
                Arrays.asList(new MetricLabel(TYPE, BROKER_NODE_NUM), new MetricLabel(STATUS, "dead")));

        buildMetric(NODE_INFO, NOUNIT,
                String.valueOf(systemInfoService.getTotalComputeNodeNumber()),
                Arrays.asList(new MetricLabel(TYPE, CN_NODE_NUM), new MetricLabel(STATUS, TOTAL)));
        buildMetric(NODE_INFO, NOUNIT,
                String.valueOf(systemInfoService.getAliveComputeNodeNumber()),
                Arrays.asList(new MetricLabel(TYPE, CN_NODE_NUM), new MetricLabel(STATUS, "alive")));

        // only master FE has this metrics, to help the Grafana knows who is the leader
        if (GlobalStateMgr.getCurrentState().isLeader()) {
            buildMetric(NODE_INFO, NOUNIT, String.valueOf(1),
                    Collections.singletonList(new MetricLabel(TYPE, "is_master")));
        }
    }

    @Override
    public String build() {
        if (isFirstElement) {
            sb.append("[]");
            isFirstElement = false;
        } else {
            sb.append("\n]");
        }
        return sb.toString();
    }

    private void buildMetric(String metricName, String unit, String value, List<MetricLabel> labels) {
        if (isFirstElement) {
            sb.append("[\n");
            isFirstElement = false;
        } else {
            sb.append(",\n");
        }
        sb.append("{\"tags\":{");
        sb.append("\"metric\":\"").append(metricName).append("\"");
        if (labels != null && !labels.isEmpty()) {
            sb.append(",");
            int i = 0;
            for (MetricLabel label : labels) {
                if (i++ > 0) {
                    sb.append(",");
                }
                sb.append("\"").append(label.getKey()).append("\":\"").append(label.getValue()).append("\"");
            }
        }
        sb.append("},");
        sb.append("\"unit\":\"").append(unit).append("\",");

        // value
        sb.append("\"value\":").append(value).append("}");
    }
}

