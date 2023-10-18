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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.starrocks.catalog.ResourceGroup;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.scheduler.slot.QueryQueueStatistics;
import com.starrocks.thrift.TWorkGroup;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ResourceGroupMetricMgr {
    private static final Logger LOG = LogManager.getLogger(ResourceGroupMetricMgr.class);

    private static final String QUERY_RESOURCE_GROUP = "query_resource_group";
    private static final String QUERY_RESOURCE_GROUP_ERR = "query_resource_group_err";
    private static final String QUERY_RESOURCE_GROUP_LATENCY = "query_resource_group_latency";

    private static final String RESOURCE_GROUP_QUERY_QUEUE_TOTAL = "resource_group_query_queue_total";
    private static final String RESOURCE_GROUP_QUERY_QUEUE_PENDING = "resource_group_query_queue_pending";
    private static final String RESOURCE_GROUP_QUERY_QUEUE_TIMEOUT = "resource_group_query_queue_timeout";

    private static final String QUERY_QUEUE_QUERIES = "query_queue_queries";
    private static final String QUERY_QUEUE_QUERIES_DESC = "the number of queries in query queue with the specific status";

    private static final ConcurrentHashMap<String, LongCounterMetric> RESOURCE_GROUP_QUERY_COUNTER_MAP
            = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, LongCounterMetric> RESOURCE_GROUP_QUERY_ERR_COUNTER_MAP
            = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, QueryResourceGroupLatencyMetrics> RESOURCE_GROUP_QUERY_LATENCY_MAP
            = new ConcurrentHashMap<>();

    private static final ConcurrentHashMap<String, LongCounterMetric> RESOURCE_GROUP_QUERY_QUEUE_TOTAL_MAP
            = new ConcurrentHashMap<>();

    private static final ConcurrentHashMap<String, LongCounterMetric> RESOURCE_GROUP_QUERY_QUEUE_PENDING_MAP
            = new ConcurrentHashMap<>();

    private static final ConcurrentHashMap<String, LongCounterMetric> RESOURCE_GROUP_QUERY_QUEUE_TIMEOUT_MAP
            = new ConcurrentHashMap<>();

    private static final LongCounterMetric QUERY_QUEUE_ADMITTING_QUERIES = new LongCounterMetric(QUERY_QUEUE_QUERIES,
            Metric.MetricUnit.REQUESTS, QUERY_QUEUE_QUERIES_DESC);
    private static final LongCounterMetric QUERY_QUEUE_PENDING_BY_GLOBAL_RESOURCE_QUERIES =
            new LongCounterMetric(QUERY_QUEUE_QUERIES,
                    Metric.MetricUnit.REQUESTS, QUERY_QUEUE_QUERIES_DESC);
    private static final LongCounterMetric QUERY_QUEUE_PENDING_BY_GLOBAL_SLOT_QUERIES = new LongCounterMetric(QUERY_QUEUE_QUERIES,
            Metric.MetricUnit.REQUESTS, QUERY_QUEUE_QUERIES_DESC);
    private static final LongCounterMetric QUERY_QUEUE_PENDING_BY_GROUP_RESOURCE_QUERIES =
            new LongCounterMetric(QUERY_QUEUE_QUERIES,
                    Metric.MetricUnit.REQUESTS, QUERY_QUEUE_QUERIES_DESC);
    private static final LongCounterMetric QUERY_QUEUE_PENDING_BY_GROUP_SLOT_QUERIES = new LongCounterMetric(QUERY_QUEUE_QUERIES,
            Metric.MetricUnit.REQUESTS, QUERY_QUEUE_QUERIES_DESC);

    public static volatile boolean isInit = false;

    public static void init() {
        if (isInit) {
            return;
        }

        QUERY_QUEUE_ADMITTING_QUERIES.addLabel(new MetricLabel("status", "admitting"));
        QUERY_QUEUE_PENDING_BY_GLOBAL_RESOURCE_QUERIES.addLabel(
                new MetricLabel("status", "pending_by_global_CPU_or_memory_limit"));
        QUERY_QUEUE_PENDING_BY_GLOBAL_SLOT_QUERIES.addLabel(new MetricLabel("status", "pending_by_global_concurrency_limit"));
        QUERY_QUEUE_PENDING_BY_GROUP_RESOURCE_QUERIES.addLabel(new MetricLabel("status", "pending_by_group_max_cpu_cores"));
        QUERY_QUEUE_PENDING_BY_GROUP_SLOT_QUERIES.addLabel(new MetricLabel("status", "pending_by_group_concurrency_limit"));

        MetricRepo.addMetric(QUERY_QUEUE_ADMITTING_QUERIES);
        MetricRepo.addMetric(QUERY_QUEUE_PENDING_BY_GLOBAL_RESOURCE_QUERIES);
        MetricRepo.addMetric(QUERY_QUEUE_PENDING_BY_GLOBAL_SLOT_QUERIES);
        MetricRepo.addMetric(QUERY_QUEUE_PENDING_BY_GROUP_RESOURCE_QUERIES);
        MetricRepo.addMetric(QUERY_QUEUE_PENDING_BY_GROUP_SLOT_QUERIES);

        isInit = true;
    }

    public static void setQueryQueueQueries(QueryQueueStatistics stats) {
        QUERY_QUEUE_ADMITTING_QUERIES.increase(-QUERY_QUEUE_ADMITTING_QUERIES.getValue() + stats.getAdmittingQueries());
        QUERY_QUEUE_PENDING_BY_GLOBAL_RESOURCE_QUERIES.increase(
                -QUERY_QUEUE_PENDING_BY_GLOBAL_RESOURCE_QUERIES.getValue() + stats.getPendingByGlobalResourceQueries());
        QUERY_QUEUE_PENDING_BY_GLOBAL_SLOT_QUERIES.increase(
                -QUERY_QUEUE_PENDING_BY_GLOBAL_SLOT_QUERIES.getValue() + stats.getPendingByGlobalSlotQueries());
        QUERY_QUEUE_PENDING_BY_GROUP_RESOURCE_QUERIES.increase(
                -QUERY_QUEUE_PENDING_BY_GROUP_RESOURCE_QUERIES.getValue() + stats.getPendingByGroupResourceQueries());
        QUERY_QUEUE_PENDING_BY_GROUP_SLOT_QUERIES.increase(
                -QUERY_QUEUE_PENDING_BY_GROUP_SLOT_QUERIES.getValue() + stats.getPendingByGroupSlotQueries());
    }

    /**
     * For the metric {@code starrocks_fe_query_resource_group}.
     */
    public static void increaseQuery(ConnectContext ctx, long delta) {
        LongCounterMetric metrics = createQueryResourceGroupMetrics(
                RESOURCE_GROUP_QUERY_COUNTER_MAP, QUERY_RESOURCE_GROUP, "query resource group", ctx);
        metrics.increase(delta);
    }

    /**
     * For the metric {@code starrocks_fe_query_resource_group_err}.
     */
    public static void increaseQueryErr(ConnectContext ctx, long delta) {
        LongCounterMetric metrics = createQueryResourceGroupMetrics(
                RESOURCE_GROUP_QUERY_ERR_COUNTER_MAP, QUERY_RESOURCE_GROUP_ERR, "query err resource group", ctx);
        metrics.increase(delta);
    }

    public static void increaseQueuedQuery(ConnectContext ctx, long delta) {
        if (delta > 0) {
            LongCounterMetric metrics = createQueryResourceGroupMetrics(
                    RESOURCE_GROUP_QUERY_QUEUE_TOTAL_MAP, RESOURCE_GROUP_QUERY_QUEUE_TOTAL,
                    "the number of total history queued queries of this resource group", ctx);
            metrics.increase(delta);
        }

        LongCounterMetric metrics = createQueryResourceGroupMetrics(
                RESOURCE_GROUP_QUERY_QUEUE_PENDING_MAP, RESOURCE_GROUP_QUERY_QUEUE_PENDING,
                "the number of pending queries of this resource group", ctx);
        metrics.increase(delta);
    }

    public static void increaseTimeoutQueuedQuery(ConnectContext ctx, long delta) {
        LongCounterMetric metrics = createQueryResourceGroupMetrics(
                RESOURCE_GROUP_QUERY_QUEUE_TIMEOUT_MAP, RESOURCE_GROUP_QUERY_QUEUE_TIMEOUT,
                "the number of pending timeout queries of this resource group", ctx);
        metrics.increase(delta);
    }

    public static void visitQueryLatency() {
        for (String resourceGroupName : RESOURCE_GROUP_QUERY_LATENCY_MAP.keySet()) {
            QueryResourceGroupLatencyMetrics metrics = RESOURCE_GROUP_QUERY_LATENCY_MAP.get(resourceGroupName);
            metrics.update();
        }
    }

    /**
     * For the metric {@code starrocks_fe_query_resource_group_latency}.
     */
    public static void updateQueryLatency(ConnectContext ctx, Long elapseMs) {
        QueryResourceGroupLatencyMetrics metrics = createQueryResourceGroupLatencyMetrics(ctx);
        metrics.histogram.update(elapseMs);
    }

    private static LongCounterMetric createQueryResourceGroupMetrics(Map<String, LongCounterMetric> cacheMap, String metricsName,
                                                                     String metricsMsg, ConnectContext ctx) {
        String groupName = getGroupName(ctx);
        if (!cacheMap.containsKey(groupName)) {
            synchronized (ResourceGroupMetricMgr.class) {
                if (!cacheMap.containsKey(groupName)) {
                    LongCounterMetric metric = new LongCounterMetric(metricsName, Metric.MetricUnit.REQUESTS, metricsMsg);
                    metric.addLabel(new MetricLabel("name", groupName));
                    cacheMap.put(groupName, metric);
                    MetricRepo.addMetric(metric);
                    LOG.info("Add {} metric, resource group name is {}", metricsName, groupName);
                }
            }
        }

        return cacheMap.get(groupName);
    }

    private static String getGroupName(ConnectContext ctx) {
        SessionVariable sessionVariable = ctx.getSessionVariable();
        if (!sessionVariable.isEnableResourceGroup() || !sessionVariable.isEnablePipelineEngine()) {
            return ResourceGroup.DISABLE_RESOURCE_GROUP_NAME;
        }

        TWorkGroup resourceGroup = ctx.getResourceGroup();
        return resourceGroup == null ? ResourceGroup.DEFAULT_RESOURCE_GROUP_NAME : resourceGroup.getName();
    }

    private static QueryResourceGroupLatencyMetrics createQueryResourceGroupLatencyMetrics(ConnectContext ctx) {
        String groupName = getGroupName(ctx);
        return RESOURCE_GROUP_QUERY_LATENCY_MAP.computeIfAbsent(groupName,
                currGroupName -> new QueryResourceGroupLatencyMetrics(QUERY_RESOURCE_GROUP_LATENCY, currGroupName));
    }

    private static final class QueryResourceGroupLatencyMetrics {
        private static final String[] QUERY_LATENCY_LABELS =
                {"mean", "75_quantile", "95_quantile", "98_quantile", "99_quantile", "999_quantile"};

        private final MetricRegistry metricRegistry;
        private Histogram histogram;
        private final List<GaugeMetricImpl<Double>> metricsList;
        private final String metricName;

        private QueryResourceGroupLatencyMetrics(String metricName, String resourceGroupName) {
            this.metricName = metricName;
            this.metricRegistry = new MetricRegistry();
            initHistogram(metricName);
            this.metricsList = new ArrayList<>();
            for (String label : QUERY_LATENCY_LABELS) {
                GaugeMetricImpl<Double> metrics = new GaugeMetricImpl<>(
                        metricName, Metric.MetricUnit.MILLISECONDS, label + " of resource group query latency");
                metrics.addLabel(new MetricLabel("type", label));
                metrics.addLabel(new MetricLabel("name", resourceGroupName));
                metrics.setValue(0.0);
                MetricRepo.addMetric(metrics);
                LOG.info("Add {} metric, resource group name is {}", QUERY_RESOURCE_GROUP_LATENCY, resourceGroupName);
                this.metricsList.add(metrics);
            }
        }

        private void initHistogram(String metricsName) {
            this.histogram = this.metricRegistry.histogram(metricsName);
        }

        private void update() {
            Histogram oldHistogram = this.histogram;
            this.metricRegistry.remove(this.metricName);
            initHistogram(metricName);

            Snapshot snapshot = oldHistogram.getSnapshot();
            metricsList.get(0).setValue(snapshot.getMedian());
            metricsList.get(1).setValue(snapshot.get75thPercentile());
            metricsList.get(2).setValue(snapshot.get95thPercentile());
            metricsList.get(3).setValue(snapshot.get98thPercentile());
            metricsList.get(4).setValue(snapshot.get99thPercentile());
            metricsList.get(5).setValue(snapshot.get999thPercentile());
        }
    }
}
