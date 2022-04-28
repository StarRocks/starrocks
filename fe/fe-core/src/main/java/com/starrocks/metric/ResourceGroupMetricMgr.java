// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.metric;

import com.starrocks.catalog.WorkGroup;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryDetail;
import com.starrocks.qe.SessionVariable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ResourceGroupMetricMgr {
    private static final Logger LOG = LogManager.getLogger(ResourceGroupMetricMgr.class);

    private static final String QUERY_RESOURCE_GROUP = "query_resource_group";
    private static final String QUERY_RESOURCE_GROUP_LATENCY = "query_resource_group_latency";
    private static final String QUERY_RESOURCE_GROUP_ERR = "query_resource_group_err";

    private static final String[] QUERY_LATENCY_LABLE =
            {"mean", "50_quantile", "75_quantile", "90_quantile", "95_quantile", "99_quantile", "999_quantile"};
    private static final double[] QUERY_LATENCY_QUANTILE = {0.0 /*Useless*/, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999};

    private static final ConcurrentHashMap<String, LongCounterMetric> RESOURCE_GROUP_QUERY_COUNTER_MAP
            = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, List<GaugeMetricImpl>> RESOURCE_GROUP_QUERY_LATENCY_MAP
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

    //starrocks_fe_query_resource_group_latency
    public static void updateQueryLatency(List<QueryDetail> queryList) {
        Map<String, List<Long>> latencyMap = new HashMap<>();
        Map<String, Long> latencySumMap = new HashMap<>();
        for (QueryDetail queryDetail : queryList) {
            String workGroupName = queryDetail.getWorkGroupName();
            if (queryDetail.isQuery()
                    && queryDetail.getState() == QueryDetail.QueryMemState.FINISHED
                    && workGroupName != null
                    && !workGroupName.isEmpty()) {
                if (!latencyMap.containsKey(workGroupName)) {
                    latencyMap.put(workGroupName, new ArrayList<>());
                    latencySumMap.put(workGroupName, 0L);
                }
                latencyMap.get(workGroupName).add(queryDetail.getLatency());
                latencySumMap.put(workGroupName, latencySumMap.get(workGroupName) + queryDetail.getLatency());
            }
        }
        for (String resourceGroupName : latencyMap.keySet()) {
            List<Long> latencyList = latencyMap.get(resourceGroupName);
            Long latencySum = latencySumMap.get(resourceGroupName);

            if (!RESOURCE_GROUP_QUERY_LATENCY_MAP.containsKey(resourceGroupName)) {
                createQueryResourceGroupLatency(resourceGroupName);
            }
            List<GaugeMetricImpl> metricList = RESOURCE_GROUP_QUERY_LATENCY_MAP.get(resourceGroupName);
            if (latencyList.size() > 0) {
                latencyList.sort(Comparator.naturalOrder());

                for (int index = 0; index < QUERY_LATENCY_LABLE.length; index++) {
                    double value = 0.0;
                    if (index == 0) {
                        value = latencySum / latencyList.size();
                    } else {
                        value = latencyList.get(
                                (int) Math.round((latencyList.size() - 1) * QUERY_LATENCY_QUANTILE[index]));
                    }
                    metricList.get(index).setValue(value);
                }
            } else {
                for (GaugeMetricImpl metrics : metricList) {
                    metrics.setValue(0.0);
                }
            }
        }
    }

    private static LongCounterMetric createQeuryResourceGroupMetrics(Map cacheMap, String metricsName,
                                                                     String metricsMsg, ConnectContext ctx) {
        SessionVariable sessionVariable = ctx.getSessionVariable();
        if (!sessionVariable.isEnableResourceGroup()) {
            return null;
        }
        WorkGroup workGroup = ctx.getWorkGroup();
        if (workGroup == null) {
            LOG.warn("The resource group for calculating query metrics is empty");
            return null;
        }
        String resourceGroupName = workGroup.getName();
        if (!cacheMap.containsKey(resourceGroupName)) {
            synchronized (cacheMap) {
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

    private static void createQueryResourceGroupLatency(String resourceGroupName) {
        List<GaugeMetricImpl> metricsList = new ArrayList<>();
        for (String label : QUERY_LATENCY_LABLE) {
            GaugeMetricImpl<Double> metrics =
                    new GaugeMetricImpl<>(QUERY_RESOURCE_GROUP_LATENCY, Metric.MetricUnit.MILLISECONDS,
                            label + " of resource group query latency");
            metrics.addLabel(new MetricLabel("type", label));
            metrics.addLabel(new MetricLabel("name", resourceGroupName));
            metrics.setValue(0.0);
            MetricRepo.addMetric(metrics);
            metricsList.add(metrics);
        }
        RESOURCE_GROUP_QUERY_LATENCY_MAP.put(resourceGroupName, metricsList);
    }
}
