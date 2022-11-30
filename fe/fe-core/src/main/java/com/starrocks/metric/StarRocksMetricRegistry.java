// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/metric/StarRocksMetricRegistry.java

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

import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

public class StarRocksMetricRegistry {

    private Collection<Metric> metrics = new PriorityQueue<>(Comparator.comparing(Metric::getName));

    public StarRocksMetricRegistry() {

    }

    public synchronized void addMetric(Metric metric) {
        metrics.add(metric);
    }

    public synchronized List<Metric> getMetrics() {
        return Lists.newArrayList(metrics);
    }

    // the metrics by metric name
    public synchronized List<Metric> getMetricsByName(String name) {
        return metrics.stream().filter(m -> m.getName().equals(name)).collect(Collectors.toList());
    }

    public synchronized void removeMetrics(String name) {
        metrics = metrics.stream().filter(m -> !(m.getName().equals(name))).collect(Collectors.toList());
    }

    public synchronized void removeMetrics(String userName, String metricName) {
        Iterator<Metric> metricIterator = metrics.iterator();
        while (metricIterator.hasNext()) {
            Metric metric = metricIterator.next();
            if (metric.getName().equals(metricName)) {
                List<MetricLabel> labels = metric.getLabels();
                Iterator<MetricLabel> it = labels.iterator();
                while (it.hasNext()) {
                    if (it.next().getValue().equals(userName)) {
                        it.remove();
                    }
                }
                if (labels.size() == 0) {
                    metricIterator.remove();
                }
            }
        }
    }
}
