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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/metric/PrometheusMetricVisitor.java

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
import com.google.common.base.Joiner;
import com.starrocks.monitor.jvm.JvmStats;
import com.starrocks.monitor.jvm.JvmStats.BufferPool;
import com.starrocks.monitor.jvm.JvmStats.GarbageCollector;
import com.starrocks.monitor.jvm.JvmStats.MemoryPool;
import com.starrocks.monitor.jvm.JvmStats.Threads;
import com.starrocks.server.GlobalStateMgr;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/*
 * Like this:
 * # HELP starrocks_fe_job_load_broker_cost_ms starrocks_fe_job_load_broker_cost_ms
 * # TYPE starrocks_fe_job_load_broker_cost_ms gauge
 * starrocks_fe_job{job="load", type="mini", state="pending"} 0
 */
public class PrometheusMetricVisitor extends MetricVisitor {
    // jvm
    private static final String JVM_HEAP_SIZE_BYTES = "jvm_heap_size_bytes";
    private static final String JVM_NON_HEAP_SIZE_BYTES = "jvm_non_heap_size_bytes";
    private static final String JVM_YOUNG_SIZE_BYTES = "jvm_young_size_bytes";
    private static final String JVM_OLD_SIZE_BYTES = "jvm_old_size_bytes";
    private static final String JVM_DIRECT_BUFFER_POOL_SIZE_BYTES = "jvm_direct_buffer_pool_size_bytes";
    private static final String JVM_YOUNG_GC = "jvm_young_gc";
    private static final String JVM_OLD_GC = "jvm_old_gc";
    private static final String JVM_THREAD = "jvm_thread";

    private static final String HELP = "# HELP ";
    private static final String TYPE = "# TYPE ";

    private final StringBuilder sb;
    private final Set<String> metricNames = new HashSet<>();

    public PrometheusMetricVisitor(String prefix) {
        super(prefix);
        sb = new StringBuilder();
    }

    @Override
    public void visitJvm(JvmStats jvmStats) {
        // heap
        sb.append(Joiner.on(" ").join(HELP, JVM_HEAP_SIZE_BYTES, "jvm heap stat\n"));
        sb.append(Joiner.on(" ").join(TYPE, JVM_HEAP_SIZE_BYTES, "gauge\n"));
        sb.append(JVM_HEAP_SIZE_BYTES).append("{type=\"max\"} ").append(jvmStats.getMem().getHeapMax())
                .append("\n");
        sb.append(JVM_HEAP_SIZE_BYTES).append("{type=\"committed\"} ")
                .append(jvmStats.getMem().getHeapCommitted()).append("\n");
        sb.append(JVM_HEAP_SIZE_BYTES).append("{type=\"used\"} ").append(jvmStats.getMem().getHeapUsed())
                .append("\n");
        // non heap
        sb.append(Joiner.on(" ").join(HELP, JVM_NON_HEAP_SIZE_BYTES, "jvm non heap stat\n"));
        sb.append(Joiner.on(" ").join(TYPE, JVM_NON_HEAP_SIZE_BYTES, "gauge\n"));
        sb.append(JVM_NON_HEAP_SIZE_BYTES).append("{type=\"committed\"} ")
                .append(jvmStats.getMem().getNonHeapCommitted()).append("\n");
        sb.append(JVM_NON_HEAP_SIZE_BYTES).append("{type=\"used\"} ")
                .append(jvmStats.getMem().getNonHeapUsed()).append("\n");

        // mem pool
        for (MemoryPool memPool : jvmStats.getMem()) {
            if (memPool.getName().equalsIgnoreCase("young")) {
                addMemPoolMetrics(memPool, JVM_YOUNG_SIZE_BYTES, "jvm young mem pool stat\n");
            } else if (memPool.getName().equalsIgnoreCase("old")) {
                addMemPoolMetrics(memPool, JVM_OLD_SIZE_BYTES, "jvm old mem pool stat\n");
            }
        }

        // direct buffer pool
        for (BufferPool pool : jvmStats.getBufferPools()) {
            if (pool.getName().equalsIgnoreCase("direct")) {
                sb.append(Joiner.on(" ").join(HELP, JVM_DIRECT_BUFFER_POOL_SIZE_BYTES,
                        "jvm direct buffer pool stat\n"));
                sb.append(Joiner.on(" ").join(TYPE, JVM_DIRECT_BUFFER_POOL_SIZE_BYTES, "gauge\n"));
                sb.append(JVM_DIRECT_BUFFER_POOL_SIZE_BYTES).append("{type=\"count\"} ").append(pool.getCount())
                        .append("\n");
                sb.append(JVM_DIRECT_BUFFER_POOL_SIZE_BYTES).append("{type=\"used\"} ")
                        .append(pool.getUsed()).append("\n");
                sb.append(JVM_DIRECT_BUFFER_POOL_SIZE_BYTES).append("{type=\"capacity\"} ")
                        .append(pool.getTotalCapacity()).append("\n");
            }
        }

        // gc
        for (GarbageCollector gc : jvmStats.getGc()) {
            if (gc.getName().equalsIgnoreCase("young")) {
                addGcMetrics(gc, JVM_YOUNG_GC, "jvm young gc stat\n");
            } else if (gc.getName().equalsIgnoreCase("old")) {
                addGcMetrics(gc, JVM_OLD_GC, "jvm old gc stat\n");
            }
        }

        // threads
        Threads threads = jvmStats.getThreads();
        sb.append(Joiner.on(" ").join(HELP, JVM_THREAD, "jvm thread stat\n"));
        sb.append(Joiner.on(" ").join(TYPE, JVM_THREAD, "gauge\n"));
        sb.append(JVM_THREAD).append("{type=\"count\"} ").append(threads.getCount()).append("\n");
        sb.append(JVM_THREAD).append("{type=\"peak_count\"} ").append(threads.getPeakCount()).append("\n");
    }

    private void addGcMetrics(GarbageCollector gc, String metricName, String desc) {
        sb.append(Joiner.on(" ").join(HELP, metricName, desc));
        sb.append(Joiner.on(" ").join(TYPE, metricName, "gauge\n"));
        sb.append(metricName).append("{type=\"count\"} ").append(gc.getCollectionCount()).append("\n");
        sb.append(metricName).append("{type=\"time\"} ").append(gc.getCollectionTime().getMillis())
                .append("\n");

    }

    private void addMemPoolMetrics(MemoryPool memPool, String metricName, String desc) {
        sb.append(Joiner.on(" ").join(HELP, metricName, desc));
        sb.append(Joiner.on(" ").join(TYPE, metricName, "gauge\n"));
        sb.append(metricName).append("{type=\"committed\"} ").append(memPool.getCommitted())
                .append("\n");
        sb.append(metricName).append("{type=\"used\"} ").append(memPool.getUsed())
                .append("\n");
        sb.append(metricName).append("{type=\"peak_used\"} ").append(memPool.getPeakUsed())
                .append("\n");
        sb.append(metricName).append("{type=\"max\"} ").append(memPool.getMax())
                .append("\n");
    }

    @Override
    public void visit(@SuppressWarnings("rawtypes") Metric metric) {
        // title
        final String fullName = prefix + "_" + metric.getName();
        // SR-57 : Fix prometheus parse error : 'second HELP line for metric name ...'
        if (!metricNames.contains(fullName)) {
            sb.append(HELP).append(fullName).append(" ").append(metric.getDescription()).append("\n");
            sb.append(TYPE).append(fullName).append(" ").append(metric.getType().name().toLowerCase()).append("\n");
            metricNames.add(fullName);
        }
        sb.append(fullName);

        // name
        @SuppressWarnings("unchecked")
        List<MetricLabel> labels = metric.getLabels();
        if (!labels.isEmpty()) {
            sb.append("{");
            List<String> labelStrings = labels.stream().map(l -> l.getKey() + "=\"" + l.getValue()
                    + "\"").collect(Collectors.toList());
            sb.append(Joiner.on(", ").join(labelStrings));
            sb.append("}");
        }

        // value
        sb.append(" ").append(metric.getValue().toString()).append("\n");
    }

    @Override
    public void visitHistogram(String name, Histogram histogram) {
        final String fullName = prefix + "_" + name.replaceAll("\\.", "_");
        sb.append(HELP).append(fullName).append(" ").append("\n");
        sb.append(TYPE).append(fullName).append(" ").append("summary\n");

        Snapshot snapshot = histogram.getSnapshot();
        sb.append(fullName).append("{quantile=\"0.75\"} ").append(snapshot.get75thPercentile()).append("\n");
        sb.append(fullName).append("{quantile=\"0.95\"} ").append(snapshot.get95thPercentile()).append("\n");
        sb.append(fullName).append("{quantile=\"0.98\"} ").append(snapshot.get98thPercentile()).append("\n");
        sb.append(fullName).append("{quantile=\"0.99\"} ").append(snapshot.get99thPercentile()).append("\n");
        sb.append(fullName).append("{quantile=\"0.999\"} ").append(snapshot.get999thPercentile()).append("\n");
        sb.append(fullName).append("_sum ").append(histogram.getCount() * snapshot.getMean()).append("\n");
        sb.append(fullName).append("_count ").append(histogram.getCount()).append("\n");
    }

    @Override
    public void getNodeInfo() {
        final String NODE_INFO = "node_info";
        sb.append(Joiner.on(" ").join(TYPE, NODE_INFO, "gauge\n"));
        sb.append(NODE_INFO).append("{type=\"fe_node_num\", state=\"total\"} ")
                .append(GlobalStateMgr.getCurrentState().getNodeMgr().getFrontends(null).size()).append("\n");
        sb.append(NODE_INFO).append("{type=\"be_node_num\", state=\"total\"} ")
                .append(GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getTotalBackendNumber()).append("\n");
        sb.append(NODE_INFO).append("{type=\"be_node_num\", state=\"alive\"} ")
                .append(GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getAliveBackendNumber()).append("\n");
        sb.append(NODE_INFO).append("{type=\"be_node_num\", state=\"decommissioned\"} ")
                .append(GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getDecommissionedBackendIds().size())
                .append("\n");
        sb.append(NODE_INFO).append("{type=\"broker_node_num\", state=\"dead\"} ").append(
                        GlobalStateMgr.getCurrentState().getBrokerMgr().getAllBrokers().stream().filter(b -> !b.isAlive)
                                .count())
                .append("\n");

        sb.append(NODE_INFO).append("{type=\"cn_node_num\", state=\"total\"} ")
            .append(GlobalStateMgr.getCurrentSystemInfo().getTotalComputeNodeNumber()).append("\n");
        sb.append(NODE_INFO).append("{type=\"cn_node_num\", state=\"alive\"} ")
            .append(GlobalStateMgr.getCurrentSystemInfo().getAliveComputeNodeNumber()).append("\n");



        // only master FE has this metrics, to help the Grafana knows who is the leader
        if (GlobalStateMgr.getCurrentState().isLeader()) {
            sb.append(NODE_INFO).append("{type=\"is_master\"} ").append(1).append("\n");
        }
    }

    @Override
    public String build() {
        return sb.toString();
    }
}

