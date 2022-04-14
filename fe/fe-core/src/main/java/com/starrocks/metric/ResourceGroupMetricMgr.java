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
import com.starrocks.qe.SessionVariable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;

public class ResourceGroupMetricMgr {
    private static final Logger LOG = LogManager.getLogger(ResourceGroupMetricMgr.class);

    private static final String QUERY_RESOURCE_GROUP = "query_resource_group";

    private static final ConcurrentHashMap<String, LongCounterMetric> RESOURCE_GROUP_QUERY_COUNTER_MAP
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
}
