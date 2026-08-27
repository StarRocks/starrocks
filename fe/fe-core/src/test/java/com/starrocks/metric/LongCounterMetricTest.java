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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class LongCounterMetricTest {

    @Test
    public void testIncreaseAccumulates() {
        LongCounterMetric metric = new LongCounterMetric("test", Metric.MetricUnit.NOUNIT, "test");

        Assertions.assertEquals(0L, metric.getValue());
        metric.increase(10L);
        metric.increase(7L);
        Assertions.assertEquals(17L, metric.getValue());
        Assertions.assertEquals(Metric.MetricType.COUNTER, metric.getType());
    }

    /**
     * CounterMetric is documented as "can only be increased", and Prometheus reads any drop in a
     * counter as a process restart. Levels that legitimately go down - such as the number of edit
     * logs still retained in bdbje - therefore have to be gauges, not counters with a way to lower
     * them. This pins that: no API on LongCounterMetric may walk the value back.
     */
    @Test
    public void testNoApiCanLowerTheValue() {
        List<String> loweringMethods = Arrays.stream(LongCounterMetric.class.getDeclaredMethods())
                .map(m -> m.getName())
                .filter(n -> n.equals("reset") || n.equals("update") || n.equals("set")
                        || n.equals("decrease"))
                .toList();

        Assertions.assertTrue(loweringMethods.isEmpty(),
                "LongCounterMetric must stay increase-only, but found: " + loweringMethods);
    }
}
