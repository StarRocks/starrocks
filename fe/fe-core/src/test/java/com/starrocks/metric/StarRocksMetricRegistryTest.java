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

import com.starrocks.common.jmockit.Deencapsulation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.PriorityQueue;

public class StarRocksMetricRegistryTest {
    @Test
    public void testRemoveMetricsKeepsUnderlyingPriorityQueue() {
        // GIVEN
        final var registry = new StarRocksMetricRegistry();
        registry.addMetric(new LongCounterMetric("b_metric", Metric.MetricUnit.NOUNIT, "b"));
        registry.addMetric(new LongCounterMetric("a_metric", Metric.MetricUnit.NOUNIT, "a"));

        // WHEN
        registry.removeMetrics("b_metric");

        // THEN
        final var metrics = Deencapsulation.getField(registry, "metrics");
        Assertions.assertInstanceOf(PriorityQueue.class, metrics);
        Assertions.assertTrue(registry.getMetricsByName("b_metric").isEmpty());
        Assertions.assertEquals(1, registry.getMetricsByName("a_metric").size());
    }
}
