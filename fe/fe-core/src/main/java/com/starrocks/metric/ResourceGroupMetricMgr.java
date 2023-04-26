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
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.thrift.TWorkGroup;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class ResourceGroupMetricMgr {
    private static final Logger LOG = LogManager.getLogger(ResourceGroupMetricMgr.class);

    private static final String QUERY_RESOURCE_GROUP = "query_resource_group";
    private static final String QUERY_RESOURCE_GROUP_LATENCY = "query_resource_group_latency";
    private static final String QUERY_RESOURCE_GROUP_ERR = "query_resource_group_err";
    private static final ConcurrentHashMap<String, LongCounterMetric> RESOURCE_GROUP_QUERY_COUNTER_MAP
            = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, QueryResourceGroupLatencyMetrics> RESOURCE_GROUP_QUERY_LATENCY_MAP
            = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, LongCounterMetric> RESOURCE_GROUP_QUERY_ERR_COUNTER_MAP
            = new ConcurrentHashMap<>();

    //starrocks_fe_query_resource_group
    public static void increaseQuery(ConnectContext ctx, Long num) {
        LongCounterMetric metrics =
                createQeuryResourceGroupMetrics(RESOURCE_GROUP_QUERY_COUNTER_MAP, QUERY_RESOURCE_GROUP,
                        "query resource group", ctx);
        if (metrics != null) {
            metrics.increase(num);
        }
    }

    //starrocks_fe_query_resource_group_err
    public static void increaseQueryErr(ConnectContext ctx, Long num) {
        LongCounterMetric metrics =
                createQeuryResourceGroupMetrics(RESOURCE_GROUP_QUERY_ERR_COUNTER_MAP, QUERY_RESOURCE_GROUP_ERR,
                        "query err resource group", ctx);
        if (metrics != null) {
            metrics.increase(num);
        }
    }

    public static void visitQueryLatency() {
        for (String resourceGroupName : RESOURCE_GROUP_QUERY_LATENCY_MAP.keySet()) {
            QueryResourceGroupLatencyMetrics metrics = RESOURCE_GROUP_QUERY_LATENCY_MAP.get(resourceGroupName);
            metrics.update();
        }
    }

    //starrocks_fe_query_resource_group_latency
    public static void updateQueryLatency(ConnectContext ctx, Long elapseMs) {
        QueryResourceGroupLatencyMetrics metrics = createQueryResourceGroupLatencyMetrics(ctx);
        if (metrics != null) {
            metrics.histogram.update(elapseMs);
        }
    }

    private static LongCounterMetric createQeuryResourceGroupMetrics(Map cacheMap, String metricsName,
                                                                     String metricsMsg, ConnectContext ctx) {
        String resourceGroupName = checkAndGetWorkGroupName(ctx);
        if (resourceGroupName == null || resourceGroupName.isEmpty()) {
            return null;
        }
        if (!cacheMap.containsKey(resourceGroupName)) {
            synchronized (ResourceGroupMetricMgr.class) {
                if (!cacheMap.containsKey(resourceGroupName)) {
                    LongCounterMetric metric = new LongCounterMetric(metricsName, Metric.MetricUnit.REQUESTS,
                            metricsMsg);
                    metric.addLabel(new MetricLabel("name", resourceGroupName));
                    cacheMap.put(resourceGroupName, metric);
                    MetricRepo.addMetric(metric);
                    LOG.info("Add {} metric, resource group name is {}", metricsName, resourceGroupName);
                }
            }
        }
        return (LongCounterMetric) cacheMap.get(resourceGroupName);
    }

    private static String checkAndGetWorkGroupName(ConnectContext ctx) {
        SessionVariable sessionVariable = ctx.getSessionVariable();
        if (!sessionVariable.isEnableResourceGroup() || !sessionVariable.isEnablePipelineEngine()) {
            return null;
        }
        TWorkGroup resourceGroup = ctx.getResourceGroup();
        return resourceGroup == null ? "default_wg" : resourceGroup.getName();
    }

    private static QueryResourceGroupLatencyMetrics createQueryResourceGroupLatencyMetrics(ConnectContext ctx) {
        String resourceGroupName = checkAndGetWorkGroupName(ctx);
        if (resourceGroupName == null || resourceGroupName.isEmpty()) {
            return null;
        }
        RESOURCE_GROUP_QUERY_LATENCY_MAP.computeIfAbsent(resourceGroupName, RESOURCE_GROUP_LATENCY_METRICS_FUNCTION);
        return RESOURCE_GROUP_QUERY_LATENCY_MAP.get(resourceGroupName);
    }

    private static final Function<String, QueryResourceGroupLatencyMetrics> RESOURCE_GROUP_LATENCY_METRICS_FUNCTION =
            new Function<String, QueryResourceGroupLatencyMetrics>() {
                @Override
                public QueryResourceGroupLatencyMetrics apply(String resourceGroupName) {
                    return new QueryResourceGroupLatencyMetrics(QUERY_RESOURCE_GROUP_LATENCY, resourceGroupName);
                }
            };

    private static final class QueryResourceGroupLatencyMetrics {
        private static final String[] QUERY_LATENCY_LABLE =
                {"mean", "75_quantile", "95_quantile", "98_quantile", "99_quantile", "999_quantile"};

        private MetricRegistry metricRegistry;
        private Histogram histogram;
        private List<GaugeMetricImpl> metricsList;
        private String metricsName;

        private QueryResourceGroupLatencyMetrics(String metricsName, String resourceGroupName) {
            this.metricsName = metricsName;
            this.metricRegistry = new MetricRegistry();
            initHistogram(metricsName);
            this.metricsList = new ArrayList<>();
            for (String label : QUERY_LATENCY_LABLE) {
                GaugeMetricImpl<Double> metrics =
                        new GaugeMetricImpl<>(metricsName, Metric.MetricUnit.MILLISECONDS,
                                label + " of resource group query latency");
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
            this.metricRegistry.remove(this.metricsName);
            initHistogram(metricsName);

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
