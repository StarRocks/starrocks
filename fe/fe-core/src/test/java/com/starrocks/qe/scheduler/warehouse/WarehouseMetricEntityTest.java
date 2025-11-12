// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.qe.scheduler.warehouse;

import com.google.common.collect.ImmutableList;
import com.starrocks.metric.Metric;
import com.starrocks.metric.PrometheusMetricVisitor;
import com.starrocks.qe.scheduler.slot.BaseSlotManager;
import com.starrocks.qe.scheduler.slot.DefaultSlotSelectionStrategy;
import com.starrocks.qe.scheduler.slot.ResourceUsageMonitor;
import com.starrocks.qe.scheduler.slot.SlotManager;
import com.starrocks.qe.scheduler.slot.SlotTracker;
import com.starrocks.server.WarehouseManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class WarehouseMetricEntityTest {
    @Test
    public void testBasic() {
        ResourceUsageMonitor resourceUsageMonitor = new ResourceUsageMonitor();
        BaseSlotManager slotManager = new SlotManager(resourceUsageMonitor);
        DefaultSlotSelectionStrategy strategy =
                new DefaultSlotSelectionStrategy(() -> false, (groupId) -> false);
        SlotTracker slotTracker = new SlotTracker(slotManager, ImmutableList.of(strategy));
        WarehouseMetricEntity entity = new WarehouseMetricEntity(slotTracker);

        List<Metric> metrics = entity.getMetrics();
        Assertions.assertEquals(9, metrics.size());
        Assertions.assertEquals(entity.getWarehouseId(), WarehouseManager.DEFAULT_WAREHOUSE_ID);
        Assertions.assertEquals(entity.getWarehouseName(), "");

        PrometheusMetricVisitor visitor = new PrometheusMetricVisitor("sr");
        for (Metric metric : metrics) {
            visitor.visit(metric);
        }
        String result = visitor.build();
        Assertions.assertTrue(result.contains("sr_warehouse_query_queue{field=\"query_pending_length\"} 0\n" +
                "sr_warehouse_query_queue{field=\"query_running_length\"} 0\n" +
                "sr_warehouse_query_queue{field=\"max_query_queue_length\"} 1024\n" +
                "sr_warehouse_query_queue{field=\"earliest_query_wait_time\"} 0.0\n" +
                "sr_warehouse_query_queue{field=\"max_query_pending_time_second\"} 300\n" +
                "sr_warehouse_query_queue{field=\"max_required_slots\"} 0\n" +
                "sr_warehouse_query_queue{field=\"sum_required_slots\"} 0\n" +
                "sr_warehouse_query_queue{field=\"remain_slots\"} 0\n" +
                "sr_warehouse_query_queue{field=\"max_slots\"} 0"));
    }
}
