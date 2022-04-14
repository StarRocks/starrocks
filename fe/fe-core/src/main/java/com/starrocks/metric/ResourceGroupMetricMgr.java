// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/metric/MetricRepo.java

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

    private static final ConcurrentHashMap<String, LongCounterMetric> RESOURCE_GROUP_QUERY_COUNTER_MAP
            = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, List<GaugeMetricImpl>> RESOURCE_GROUP_QUERY_LATENCY_MAP
            = new ConcurrentHashMap<>();

    //starrocks_fe_query_resource_group
    public static void increaseQuery(ConnectContext ctx, Long num) {
        SessionVariable sessionVariable = ctx.getSessionVariable();
        if(!sessionVariable.isEnableResourceGroup()) {
            return;
        }
        WorkGroup workGroup = ctx.getWorkGroup();
        if(workGroup == null) {
            LOG.warn("The resource group for calculating query metrics is empty");
            return;
        }
        String resourceGroupName = workGroup.getName();
        if(!RESOURCE_GROUP_QUERY_COUNTER_MAP.containsKey(resourceGroupName)) {
            synchronized (RESOURCE_GROUP_QUERY_COUNTER_MAP) {
                if(!RESOURCE_GROUP_QUERY_COUNTER_MAP.containsKey(resourceGroupName)){
                    LongCounterMetric metric = new LongCounterMetric(QUERY_RESOURCE_GROUP, Metric.MetricUnit.REQUESTS, "query resource group");
                    metric.addLabel(new MetricLabel("name", resourceGroupName));
                    RESOURCE_GROUP_QUERY_COUNTER_MAP.put(resourceGroupName, metric);
                    MetricRepo.addMetric(metric);
                    LOG.info("Add {} metric, resource group name is {}", QUERY_RESOURCE_GROUP, resourceGroupName);
                }
            }
        }
        RESOURCE_GROUP_QUERY_COUNTER_MAP.get(resourceGroupName).increase(num);
    }

    public static void updateQueryLatency(List<QueryDetail> queryList) {
        Map<String, List<Long>> latencyMap = new HashMap<>();
        Map<String, Long> latencySumMap = new HashMap<>();
        for (QueryDetail queryDetail : queryList) {
            String workGroupName = queryDetail.getWorkGroupName();
            if (queryDetail.isQuery()
                    && queryDetail.getState() == QueryDetail.QueryMemState.FINISHED
                    && workGroupName != null
                    && !workGroupName.isEmpty()) {
                if(!latencyMap.containsKey(workGroupName)) {
                    latencyMap.put(workGroupName, new ArrayList<>());
                    latencySumMap.put(workGroupName, 0L);
                }
                latencyMap.get(workGroupName).add(queryDetail.getLatency());
                latencySumMap.put(workGroupName, latencySumMap.get(workGroupName) + queryDetail.getLatency());
            }
        }
        for(String resourceGroupName : latencyMap.keySet()){
            List<Long> latencyList = latencyMap.get(resourceGroupName);
            Long latencySum = latencySumMap.get(resourceGroupName);

            if(!RESOURCE_GROUP_QUERY_LATENCY_MAP.containsKey(resourceGroupName)) {
                createQueryResourceGroupLatency(resourceGroupName);
            }
            List<GaugeMetricImpl> metricList = RESOURCE_GROUP_QUERY_LATENCY_MAP.get(resourceGroupName);
            if (latencyList.size() > 0) {
                metricList.get(0).setValue(latencySum / latencyList.size());

                latencyList.sort(Comparator.naturalOrder());

                int index = (int) Math.round((latencyList.size() - 1) * 0.5);
                metricList.get(1).setValue((double) latencyList.get(index));
                index = (int) Math.round((latencyList.size() - 1) * 0.75);
                metricList.get(2).setValue((double) latencyList.get(index));
                index = (int) Math.round((latencyList.size() - 1) * 0.90);
                metricList.get(3).setValue((double) latencyList.get(index));
                index = (int) Math.round((latencyList.size() - 1) * 0.95);
                metricList.get(4).setValue((double) latencyList.get(index));
                index = (int) Math.round((latencyList.size() - 1) * 0.99);
                metricList.get(5).setValue((double) latencyList.get(index));
                index = (int) Math.round((latencyList.size() - 1) * 0.999);
                metricList.get(6).setValue((double) latencyList.get(index));
            } else {
                metricList.get(0).setValue(0.0);
                metricList.get(1).setValue(0.0);
                metricList.get(2).setValue(0.0);
                metricList.get(3).setValue(0.0);
                metricList.get(4).setValue(0.0);
                metricList.get(5).setValue(0.0);
                metricList.get(6).setValue(0.0);
            }
        }
    }

    private static void createQueryResourceGroupLatency(String resourceGroupName) {
        if(RESOURCE_GROUP_QUERY_LATENCY_MAP.containsKey(resourceGroupName)) {
            return;
        }else {
            GaugeMetricImpl metricMean =
                    new GaugeMetricImpl<>(QUERY_RESOURCE_GROUP_LATENCY, Metric.MetricUnit.MILLISECONDS, "mean of resource group query latency");
            metricMean.addLabel(new MetricLabel("type", "mean"));
            metricMean.addLabel(new MetricLabel("name", resourceGroupName));
            metricMean.setValue(0.0);
            MetricRepo.addMetric(metricMean);

            GaugeMetricImpl metric50Quantile =
                    new GaugeMetricImpl<>(QUERY_RESOURCE_GROUP_LATENCY, Metric.MetricUnit.MILLISECONDS, "median of resource group query latency");
            metric50Quantile.addLabel(new MetricLabel("type", "50_quantile"));
            metric50Quantile.addLabel(new MetricLabel("name", resourceGroupName));
            metric50Quantile.setValue(0.0);
            MetricRepo.addMetric(metric50Quantile);

            GaugeMetricImpl metric75Quantile =
                    new GaugeMetricImpl<>(QUERY_RESOURCE_GROUP_LATENCY, Metric.MetricUnit.MILLISECONDS, "p75 of resource group query latency");
            metric75Quantile.addLabel(new MetricLabel("type", "75_quantile"));
            metric75Quantile.addLabel(new MetricLabel("name", resourceGroupName));
            metric75Quantile.setValue(0.0);
            MetricRepo.addMetric(metric75Quantile);

            GaugeMetricImpl metric90Quantile =
                    new GaugeMetricImpl<>(QUERY_RESOURCE_GROUP_LATENCY, Metric.MetricUnit.MILLISECONDS, "p90 of resource group query latency");
            metric90Quantile.addLabel(new MetricLabel("type", "90_quantile"));
            metric90Quantile.addLabel(new MetricLabel("name", resourceGroupName));
            metric90Quantile.setValue(0.0);
            MetricRepo.addMetric(metric90Quantile);

            GaugeMetricImpl metric95Quantile =
                    new GaugeMetricImpl<>(QUERY_RESOURCE_GROUP_LATENCY, Metric.MetricUnit.MILLISECONDS, "p95 of resource group query latency");
            metric95Quantile.addLabel(new MetricLabel("type", "95_quantile"));
            metric95Quantile.addLabel(new MetricLabel("name", resourceGroupName));
            metric95Quantile.setValue(0.0);
            MetricRepo.addMetric(metric95Quantile);

            GaugeMetricImpl metric99Quantile =
                    new GaugeMetricImpl<>(QUERY_RESOURCE_GROUP_LATENCY, Metric.MetricUnit.MILLISECONDS, "p99 of resource group query latency");
            metric99Quantile.addLabel(new MetricLabel("type", "99_quantile"));
            metric99Quantile.addLabel(new MetricLabel("name", resourceGroupName));
            metric99Quantile.setValue(0.0);
            MetricRepo.addMetric(metric99Quantile);

            GaugeMetricImpl metric999Quantile =
                    new GaugeMetricImpl<>(QUERY_RESOURCE_GROUP_LATENCY, Metric.MetricUnit.MILLISECONDS, "p999 of resource group query latency");
            metric999Quantile.addLabel(new MetricLabel("type", "999_quantile"));
            metric999Quantile.addLabel(new MetricLabel("name", resourceGroupName));
            metric999Quantile.setValue(0.0);
            MetricRepo.addMetric(metric999Quantile);

            List<GaugeMetricImpl> metricList = new ArrayList<>();
            metricList.add(metricMean);
            metricList.add(metric50Quantile);
            metricList.add(metric75Quantile);
            metricList.add(metric90Quantile);
            metricList.add(metric95Quantile);
            metricList.add(metric99Quantile);
            metricList.add(metric999Quantile);

            RESOURCE_GROUP_QUERY_LATENCY_MAP.put(resourceGroupName, metricList);
        }
    }
}
