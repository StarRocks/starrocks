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
 * Tests for B3: {@link SlotManager#collectWarehouseMetrics} must walk the warehouses
 * currently tracked by {@link WarehouseInFlightTracker}, update the gauges in
 * {@link com.starrocks.metric.WarehouseSlotMetricMgr}, and emit the 5 gauges + 1 counter
 * via {@link MetricVisitor#visit} and 1 histogram via
 * {@link MetricVisitor#visitHistogram(HistogramMetric)}.
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
        // Provide a BE so QueryQueueOptions.V2 can compute a non-zero totalSlots; the
        // scrape path uses BackendResourceStat.getNumBes for slots-per-CN.
        BackendResourceStat.getInstance().setNumCoresOfBe(WH, 1, 8);
        BackendResourceStat.getInstance().setMemLimitBytesOfBe(WH, 1, 64L * 1024 * 1024 * 1024);

        // Force QueryQueueOptions.createFromEnv(WH) to return V2-enabled options. The real
        // path goes through BaseSlotManager.isEnableQueryQueueV2 which depends on
        // Config.enable_query_queue_v2 and a per-warehouse plan; mock the plumbing.
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
    public void testEmitsAllSevenMetricsPerTrackedWarehouse() {
        WarehouseInFlightTracker tracker = WarehouseInFlightTracker.getInstance();
        // One big query (raw=20, clamped=8) and one small (raw=4) on warehouse 999.
        tracker.onEnterPending(WH, new TUniqueId(1, 1), 20, 8, 8, true);
        tracker.onEnterPending(WH, new TUniqueId(2, 2), 4, 4, 8, false);

        SlotManager sm = (SlotManager) GlobalStateMgr.getCurrentState().getSlotManager();
        CapturingVisitor visitor = new CapturingVisitor();
        sm.collectWarehouseMetrics(visitor);

        // All five gauges + the counter must be visited.
        assertTrue(visitor.visitedNames.contains("query_queue_pending_max_raw_slots"),
                "must visit max_raw_slots gauge, saw=" + visitor.visitedNames);
        assertTrue(visitor.visitedNames.contains("query_queue_pending_sum_raw_slots"),
                "must visit sum_raw_slots gauge, saw=" + visitor.visitedNames);
        assertTrue(visitor.visitedNames.contains("query_queue_required_compute_node_count"),
                "must visit required_compute_node_count gauge, saw=" + visitor.visitedNames);
        assertTrue(visitor.visitedNames.contains("query_queue_max_raw_slots_ratio"),
                "must visit max_raw_slots_ratio gauge, saw=" + visitor.visitedNames);
        assertTrue(visitor.visitedNames.contains("query_queue_pending_big_query_count"),
                "must visit pending_big_query_count gauge, saw=" + visitor.visitedNames);
        assertTrue(visitor.visitedNames.contains("query_queue_big_query_total"),
                "must visit big_query_total counter, saw=" + visitor.visitedNames);

        // The histogram must be visited via visitHistogram(HistogramMetric).
        assertTrue(visitor.visitedHistogramNames.contains("query_queue_pending_big_query_wait_seconds"),
                "must visit big_query_wait_seconds histogram, saw=" + visitor.visitedHistogramNames);

        // The gauges should have been populated with computed values.
        assertEquals(20L, com.starrocks.metric.WarehouseSlotMetricMgr.getMaxRawSlotsGauge(WH).getValue().longValue());
        assertEquals(24L, com.starrocks.metric.WarehouseSlotMetricMgr.getSumRawSlotsGauge(WH).getValue().longValue());
        assertEquals(1L, com.starrocks.metric.WarehouseSlotMetricMgr.getPendingBigQueryCountGauge(WH).getValue().longValue());
    }

    @Test
    public void testEmptyTrackerEmitsNothing() {
        // Tracker has no entries for WH after clearTrackerForWarehouse.
        SlotManager sm = (SlotManager) GlobalStateMgr.getCurrentState().getSlotManager();
        CapturingVisitor visitor = new CapturingVisitor();
        sm.collectWarehouseMetrics(visitor);
        assertEquals(0, visitor.visitedNames.size(),
                "no tracked warehouses -> no metrics emitted, saw=" + visitor.visitedNames);
        assertEquals(0, visitor.visitedHistogramNames.size(),
                "no tracked warehouses -> no histograms emitted, saw=" + visitor.visitedHistogramNames);
    }

    /**
     * The tracker is a process-wide singleton; clear any entries for the test-specific
     * warehouse id so the test does not see leftovers from prior runs (mirrors the
     * pattern in QueryQueueManagerPreScaleTest).
     */
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
