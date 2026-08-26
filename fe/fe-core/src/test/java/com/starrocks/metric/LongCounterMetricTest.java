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
    public void testResetAndUpdate() {
        LongCounterMetric metric = new LongCounterMetric("test", Metric.MetricUnit.NOUNIT, "test");

        metric.increase(10L);
        metric.reset();
        Assertions.assertEquals(0L, metric.getValue());

        metric.update(7L);
        Assertions.assertEquals(7L, metric.getValue());
    }
}
