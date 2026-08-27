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
     * reset()/update() exist for the "since the last X" counters (edit_log{type="current"} and
     * friends), which are re-baselined when a cleanup actually reclaims journals. Deliberately not
     * a gauge: recomputing that level on every scrape would mean a Database.count() full btree scan
     * on the metrics path.
     */
    @Test
    public void testResetAndUpdateReBaseline() {
        LongCounterMetric metric = new LongCounterMetric("test", Metric.MetricUnit.NOUNIT, "test");

        metric.increase(10L);
        metric.increase(5L);
        Assertions.assertEquals(15L, metric.getValue());

        metric.reset();
        Assertions.assertEquals(0L, metric.getValue());

        // accumulation resumes from the new baseline
        metric.increase(3L);
        Assertions.assertEquals(3L, metric.getValue());

        // update() replaces rather than adds
        metric.update(7L);
        Assertions.assertEquals(7L, metric.getValue());
        metric.update(2L);
        Assertions.assertEquals(2L, metric.getValue());
        metric.increase(1L);
        Assertions.assertEquals(3L, metric.getValue());
    }
}
