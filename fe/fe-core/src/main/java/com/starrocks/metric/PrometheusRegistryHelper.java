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

import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.DataPointSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;

/**
 * A simple helper class to visitor a PrometheusRegistry with PrometheusMetric.
 */
public class PrometheusRegistryHelper {

    public static void visitPrometheusRegistry(PrometheusRegistry registry, MetricVisitor visitor) {
        MetricSnapshots snapshots = registry.scrape();
        for (MetricSnapshot snapshot : snapshots) {
            for (DataPointSnapshot dp : snapshot.getDataPoints()) {
                PrometheusMetric metric = PrometheusMetric.create(snapshot.getMetadata(), dp);
                visitor.visit(metric);
            }
        }
    }
}
