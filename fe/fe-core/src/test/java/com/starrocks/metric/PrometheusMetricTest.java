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

import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.MetricMetadata;
import org.junit.Assert;
import org.junit.Test;

public class PrometheusMetricTest {
    @Test
    public void testBasic() throws Exception {
        MetricMetadata data = new MetricMetadata("test_prometheus_metric");
        CounterSnapshot.CounterDataPointSnapshot s1 = CounterSnapshot.CounterDataPointSnapshot.builder().value(1.0).build();
        GaugeSnapshot.GaugeDataPointSnapshot s2 = GaugeSnapshot.GaugeDataPointSnapshot.builder().value(1.0).build();
        PrometheusMetric m1 = PrometheusMetric.create(data, s1);
        PrometheusMetric m2 = PrometheusMetric.create(data, s2);
        Assert.assertEquals(m1.getType(), Metric.MetricType.COUNTER);
        Assert.assertEquals(m2.getType(), Metric.MetricType.GAUGE);
    }
}
