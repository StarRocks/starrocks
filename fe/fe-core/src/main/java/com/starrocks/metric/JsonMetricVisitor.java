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
import com.starrocks.monitor.jvm.GcNames;
import com.starrocks.monitor.jvm.JvmStats;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class JsonMetricVisitor extends MetricVisitor {

    private boolean isFirstElement;
    private StringBuilder sb;

    public JsonMetricVisitor(String prefix) {
        super(prefix);
        isFirstElement = true;
        sb = new StringBuilder();
    }

    @Override
    public void visitJvm(JvmStats jvmStats) {

        List<MetricLabel> labels = new ArrayList<>();
        //gc
        Iterator<JvmStats.GarbageCollector> gcIter = jvmStats.getGc().iterator();
        while (gcIter.hasNext()) {
            JvmStats.GarbageCollector gc = gcIter.next();
            if (gc.getName().equalsIgnoreCase(GcNames.YOUNG)) {
                labels.clear();
                labels.add(new MetricLabel("type", "count"));
                buildMetric("jvm_young_gc", "nounit", String.valueOf(gc.getCollectionCount()), labels);

                labels.clear();
                labels.add(new MetricLabel("type", "time"));
                buildMetric("jvm_young_gc", "milliseconds", String.valueOf(gc.getCollectionTime().getMillis()), labels);
            } else if (gc.getName().equalsIgnoreCase(GcNames.OLD)) {
                labels.clear();
                labels.add(new MetricLabel("type", "count"));
                buildMetric("jvm_old_gc", "nounit", String.valueOf(gc.getCollectionCount()), labels);

                labels.clear();
                labels.add(new MetricLabel("type", "time"));
                buildMetric("jvm_old_gc", "milliseconds", String.valueOf(gc.getCollectionTime().getMillis()), labels);
            }
        }

        // mem pool
        Iterator<JvmStats.MemoryPool> memIter = jvmStats.getMem().iterator();
        while (memIter.hasNext()) {
            JvmStats.MemoryPool memPool = memIter.next();
            if (memPool.getName().equalsIgnoreCase(GcNames.PERM)) {
                double percent = 0.0;
                if (memPool.getCommitted().getBytes() > 0) {
                    percent = 100 * ((double) memPool.getUsed().getBytes() / memPool.getCommitted().getBytes());
                }
                labels.clear();
                labels.add(new MetricLabel("type", GcNames.PERM));
                buildMetric("jvm_size_percent", "percent", String.valueOf(percent), labels);
            } else if (memPool.getName().equalsIgnoreCase(GcNames.OLD)) {
                double percent = 0.0;
                if (memPool.getCommitted().getBytes() > 0) {
                    percent = 100 * ((double) memPool.getUsed().getBytes() / memPool.getCommitted().getBytes());
                }
                labels.clear();
                labels.add(new MetricLabel("type", GcNames.OLD));
                buildMetric("jvm_size_percent", "percent", String.valueOf(percent), labels);
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
    public void visitHistogram(String name, Histogram histogram) {
        return;
    }

    @Override
    public void getNodeInfo() {
        return;
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

