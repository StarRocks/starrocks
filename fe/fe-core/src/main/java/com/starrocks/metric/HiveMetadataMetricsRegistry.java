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

import com.google.common.collect.Maps;

import java.util.Map;

public class HiveMetadataMetricsRegistry {

    private final Map<String, CatalogHMSMetricsEntity> nameToHMSMetrics;
    private static final HiveMetadataMetricsRegistry INSTANCE = new HiveMetadataMetricsRegistry();

    private HiveMetadataMetricsRegistry() {
        this.nameToHMSMetrics = Maps.newConcurrentMap();
    }

    public static HiveMetadataMetricsRegistry getInstance() {
        return INSTANCE;
    }

    public CatalogHMSMetricsEntity getHMSEntity(String catalogName) {
        return this.nameToHMSMetrics.computeIfAbsent(catalogName, k -> new CatalogHMSMetricsEntity());
    }

    public void removeHMSEntity(String catalogName) {
        this.nameToHMSMetrics.remove(catalogName);
    }

    public static void collectHiveMetadataMetrics(MetricVisitor visitor) {
        HiveMetadataMetricsRegistry registry = HiveMetadataMetricsRegistry.getInstance();
        for (Map.Entry<String, CatalogHMSMetricsEntity> entry : registry.nameToHMSMetrics.entrySet()) {
            String catalogName = entry.getKey();
            CatalogHMSMetricsEntity entity = entry.getValue();
            MetricLabel label = new MetricLabel("catalog", catalogName);

            for (HistogramMetric histogramMetric : entity.getHistogramMetrics().values()) {
                histogramMetric.addLabel(label);
                visitor.visitHistogram(histogramMetric);
            }
        }
    }
}
