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

package com.starrocks.qe.scheduler.slot;

import com.codahale.metrics.Histogram;
import com.starrocks.common.FeConstants;
import com.starrocks.metric.HistogramMetric;
import com.starrocks.metric.Metric;
import com.starrocks.metric.MetricRepo;
import com.starrocks.metric.MetricVisitor;
import com.starrocks.metric.WarehouseMetricMgr;
import com.starrocks.monitor.jvm.JvmStats;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.BackendResourceStat;
import com.starrocks.thrift.TUniqueId;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link SlotManager#collectWarehouseMetrics} must walk the warehouses currently tracked by
 * {@link WarehouseInFlightTracker} and emit, per warehouse, the pending-sum-raw-slots gauge,
 * the pre-scale-wait counter, and the pre-scale-wait histogram via {@link MetricVisitor}.
 */
public class SlotManagerMetricsScrapeTest {

    private static final long WH = 999L;

    @BeforeAll
    public static void beforeAll() {
        FeConstants.runningUnitTest = true;
        MetricRepo.init();
    }

    @BeforeEach
    public void before() {
        clearTrackerForWarehouse(WH);
        // Provide a BE so QueryQueueOptions.V2 can compute a non-zero totalSlots.
        BackendResourceStat.getInstance().setNumCoresOfBe(WH, 1, 8);
        BackendResourceStat.getInstance().setMemLimitBytesOfBe(WH, 1, 64L * 1024 * 1024 * 1024);

        // Force QueryQueueOptions.createFromEnv(WH) to return V2-enabled options.
        new MockUp<BaseSlotManager>() {
            @Mock
            public boolean isEnableQueryQueueV2(long warehouseId) {
                return warehouseId == WH;
            }
        };
    }

    @AfterEach
    public void after() {
        clearTrackerForWarehouse(WH);
        BackendResourceStat.getInstance().reset();
    }

    @Test
    public void testEmitsThreeMetricsPerTrackedWarehouse() {
        WarehouseInFlightTracker tracker = WarehouseInFlightTracker.getInstance();
        tracker.onEnterPending(WH, new TUniqueId(1, 1), 20, 8, 8);
        tracker.onEnterPending(WH, new TUniqueId(2, 2), 4, 4, 8);

        SlotManager sm = (SlotManager) GlobalStateMgr.getCurrentState().getSlotManager();
        CapturingVisitor visitor = new CapturingVisitor();
        sm.collectWarehouseMetrics(visitor);

        assertTrue(visitor.visitedNames.contains("query_queue_pending_sum_raw_slots"),
                "must visit sum_raw_slots gauge, saw=" + visitor.visitedNames);
        assertTrue(visitor.visitedNames.contains("query_queue_pre_scale_wait_total"),
                "must visit pre_scale_wait_total counter, saw=" + visitor.visitedNames);
        assertTrue(visitor.visitedHistogramNames.contains("query_queue_pre_scale_wait_ms"),
                "must visit pre_scale_wait_ms histogram, saw=" + visitor.visitedHistogramNames);

        // The sum gauge should reflect total raw demand from the two tracker entries.
        assertEquals(24L, WarehouseMetricMgr.getPendingSumRawSlotsGauge(WH).getValue().longValue());
    }

    @Test
    public void testEmptyTrackerEmitsNothing() {
        SlotManager sm = (SlotManager) GlobalStateMgr.getCurrentState().getSlotManager();
        CapturingVisitor visitor = new CapturingVisitor();
        sm.collectWarehouseMetrics(visitor);
        assertEquals(0, visitor.visitedNames.size(),
                "no tracked warehouses -> no metrics emitted, saw=" + visitor.visitedNames);
        assertEquals(0, visitor.visitedHistogramNames.size(),
                "no tracked warehouses -> no histograms emitted, saw=" + visitor.visitedHistogramNames);
    }

    @SuppressWarnings("unchecked")
    private static void clearTrackerForWarehouse(long warehouseId) {
        try {
            WarehouseInFlightTracker tracker = WarehouseInFlightTracker.getInstance();
            Field byWarehouseField = WarehouseInFlightTracker.class.getDeclaredField("byWarehouse");
            byWarehouseField.setAccessible(true);
            ((Map<Long, ?>) byWarehouseField.get(tracker)).remove(warehouseId);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("Failed to clear WarehouseInFlightTracker for test", e);
        }
    }

    /**
     * Minimal MetricVisitor that records the names of every metric/histogram passed
     * through it. Other visitor methods are no-ops; collectWarehouseMetrics only ever
     * calls visit(Metric) and visitHistogram(HistogramMetric).
     */
    private static class CapturingVisitor extends MetricVisitor {
        final List<String> visitedNames = new ArrayList<>();
        final List<String> visitedHistogramNames = new ArrayList<>();

        CapturingVisitor() {
            super("test");
        }

        @Override
        public void visitJvm(JvmStats jvmStats) {
        }

        @Override
        public void visit(Metric metric) {
            visitedNames.add(metric.getName());
        }

        @Override
        public void visitHistogram(String name, Histogram histogram) {
            visitedHistogramNames.add(name);
        }

        @Override
        public void visitHistogram(HistogramMetric histogram) {
            visitedHistogramNames.add(histogram.getName());
        }

        @Override
        public void getNodeInfo() {
        }

        @Override
        public String build() {
            return "";
        }
    }
}
