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

package com.starrocks.alter;

import com.codahale.metrics.Histogram;
import com.starrocks.metric.MetricRepo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link AlterColumnMetrics}. Both writers are guarded by
 * {@link MetricRepo#hasInit}; the counter group and histogram registry are
 * static so they exist without {@code MetricRepo.init()} (which is not run in
 * unit tests). Assertions are before/after deltas, resilient to shared global
 * state.
 */
public class AlterColumnMetricsTest {

    private boolean savedHasInit;

    @BeforeEach
    public void setUp() {
        savedHasInit = MetricRepo.hasInit;
    }

    @AfterEach
    public void tearDown() {
        MetricRepo.hasInit = savedHasInit;
    }

    @Test
    public void recordColumnOpNoopWhenMetricsNotInit() {
        MetricRepo.hasInit = false;
        Assertions.assertDoesNotThrow(() -> AlterColumnMetrics.recordColumnOp("add"));
    }

    @Test
    public void recordColumnOpBumpsPerOpType() {
        MetricRepo.hasInit = true;
        for (String op : new String[] {"add", "drop", "modify"}) {
            long before = MetricRepo.COUNTER_ALTER_TABLE_COLUMN_OP.getMetric(op).getValue();
            AlterColumnMetrics.recordColumnOp(op);
            long after = MetricRepo.COUNTER_ALTER_TABLE_COLUMN_OP.getMetric(op).getValue();
            Assertions.assertEquals(before + 1L, after, "recordColumnOp must bump " + op + " exactly once");
        }
    }

    @Test
    public void recordJobDurationNoopWhenMetricsNotInit() {
        MetricRepo.hasInit = false;
        Assertions.assertDoesNotThrow(() -> AlterColumnMetrics.recordJobDuration("fse_v1", 123L));
    }

    @Test
    public void recordJobDurationBumpsHistogramCount() {
        MetricRepo.hasInit = true;
        for (String type : new String[] {"fse_v1", "fse_v2"}) {
            Histogram h = AlterColumnMetrics.getDurationHistogram(type);
            long before = h.getCount();
            AlterColumnMetrics.recordJobDuration(type, 42L);
            Assertions.assertEquals(before + 1L, AlterColumnMetrics.getDurationHistogram(type).getCount(),
                    "recordJobDuration must add one observation to " + type);
        }
    }
}
