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
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.WarehouseManager;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
public class WarehouseMetricTest {
    @BeforeClass
    public static void setUp() {
        FeConstants.runningUnitTest = true;
        MetricRepo.init();
    }
    @Test
    public void testWarehouseMetrics() {
        ConnectContext ctx = new ConnectContext();
        // no warehouse is set, use default warehouse
        WarehouseMetricMgr.increaseQuery(ctx, 1L);
        WarehouseMetricMgr.increaseQueryErr(ctx, 1L);
        WarehouseMetricMgr.updateQueryLatency(ctx, 10L);
        String warehouse1 = "warehouse_1";
        ctx.setCurrentWarehouse(warehouse1);
        WarehouseMetricMgr.increaseQuery(ctx, 2L);
        WarehouseMetricMgr.increaseQueryErr(ctx, 2L);
        WarehouseMetricMgr.updateQueryLatency(ctx, 11L);
        String warehouse2 = "warehouse_2";
        ctx.setCurrentWarehouse(warehouse2);
        WarehouseMetricMgr.increaseQuery(ctx, 3L);
        WarehouseMetricMgr.increaseQueryErr(ctx, 3L);
        WarehouseMetricMgr.updateQueryLatency(ctx, 12L);
        // warehouse is set to null, use default warehouse
        ctx.setCurrentWarehouse(null);
        WarehouseMetricMgr.increaseQuery(ctx, 4L);
        WarehouseMetricMgr.increaseQueryErr(ctx, 4L);
        WarehouseMetricMgr.updateQueryLatency(ctx, 20L);
        List<Metric> metricsWarehouse = MetricRepo.getMetricsByName("query_warehouse");
        List<Metric> metricsWarehouseErr = MetricRepo.getMetricsByName("query_warehouse_err");
        List<Metric> metricsWarehouseLatency = MetricRepo.getMetricsByName("query_warehouse_latency");
        Assert.assertEquals(3, metricsWarehouse.size());
        Assert.assertEquals(3, metricsWarehouseErr.size());
        Assert.assertEquals(3 * 6, metricsWarehouseLatency.size());
        for (Metric warehouseMetric : metricsWarehouse) {
            LongCounterMetric metric = (LongCounterMetric) warehouseMetric;
            if (warehouse1.equals(metric.getLabels().get(0).getValue())) {
                Assert.assertEquals(Long.valueOf(2L), metric.getValue());
            } else if (warehouse2.equals(metric.getLabels().get(0).getValue())) {
                Assert.assertEquals(Long.valueOf(3L), metric.getValue());
            } else if (WarehouseManager.DEFAULT_WAREHOUSE_NAME.equals(metric.getLabels().get(0).getValue())) {
                // warehouse0 + warehouse3
                Assert.assertEquals(Long.valueOf(5L), metric.getValue());
            } else {
                Assert.fail();
            }
        }
        for (Metric warehouseMetric : metricsWarehouseErr) {
            LongCounterMetric metric = (LongCounterMetric) warehouseMetric;
            if (warehouse1.equals(metric.getLabels().get(0).getValue())) {
                Assert.assertEquals(Long.valueOf(2L), metric.getValue());
            } else if (warehouse2.equals(metric.getLabels().get(0).getValue())) {
                Assert.assertEquals(Long.valueOf(3L), metric.getValue());
            } else if (WarehouseManager.DEFAULT_WAREHOUSE_NAME.equals(metric.getLabels().get(0).getValue())) {
                // warehouse0 + warehouse3
                Assert.assertEquals(Long.valueOf(5L), metric.getValue());
            } else {
                Assert.fail();
            }
        }
        WarehouseMetricMgr.visitQueryLatency();
        for (Metric warehouseMetric : metricsWarehouseLatency) {
            GaugeMetricImpl<Double> metric = (GaugeMetricImpl<Double>) warehouseMetric;
            if (warehouse1.equals(metric.getLabels().get(1).getValue())) {
                Assert.assertEquals(Double.valueOf(11d), Double.valueOf(String.valueOf(metric.getValue())));
            } else if (warehouse2.equals(metric.getLabels().get(1).getValue())) {
                Assert.assertEquals(Double.valueOf(12d), Double.valueOf(String.valueOf(metric.getValue())));
            } else if (WarehouseManager.DEFAULT_WAREHOUSE_NAME.equals(metric.getLabels().get(1).getValue())) {
                Assert.assertEquals(Double.valueOf(20d), Double.valueOf(String.valueOf(metric.getValue())));
            } else {
                Assert.fail();
            }
        }
    }
}