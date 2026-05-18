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

import com.starrocks.common.FeConstants;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class WarehouseSlotMetricMgrTest {

    @BeforeAll
    public static void beforeAll() {
        FeConstants.runningUnitTest = true;
        MetricRepo.init();
    }

    @Test
    public void testGaugeReuseAcrossCalls() {
        GaugeMetricImpl<Long> a = WarehouseSlotMetricMgr.getMaxRawSlotsGauge(100L);
        GaugeMetricImpl<Long> b = WarehouseSlotMetricMgr.getMaxRawSlotsGauge(100L);
        assertSame(a, b, "getOrCreate should reuse the same metric per warehouse");
    }

    @Test
    public void testGaugeLabelsPresent() {
        GaugeMetricImpl<Long> gauge = WarehouseSlotMetricMgr.getMaxRawSlotsGauge(200L);
        assertTrue(gauge.getLabels().stream().anyMatch(l -> "warehouse_id".equals(l.getKey())),
                "gauge must carry warehouse_id label");
        assertTrue(gauge.getLabels().stream().anyMatch(l -> "warehouse_name".equals(l.getKey())),
                "gauge must carry warehouse_name label");
    }

    @Test
    public void testCounterAndHistogramAlsoLazyAndLabeled() {
        LongCounterMetric c1 = WarehouseSlotMetricMgr.getBigQueryCounter(300L);
        LongCounterMetric c2 = WarehouseSlotMetricMgr.getBigQueryCounter(300L);
        assertSame(c1, c2);

        HistogramMetric h1 = WarehouseSlotMetricMgr.getBigQueryWaitHistogram(300L);
        HistogramMetric h2 = WarehouseSlotMetricMgr.getBigQueryWaitHistogram(300L);
        assertSame(h1, h2);

        assertTrue(c1.getLabels().stream().anyMatch(l -> "warehouse_id".equals(l.getKey())));
        assertTrue(h1.getLabels().stream().anyMatch(l -> "warehouse_id".equals(l.getKey())));
    }
}
