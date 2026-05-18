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

package com.starrocks.qe;

import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.metric.MetricRepo;
import com.starrocks.metric.WarehouseSlotMetricMgr;
import com.starrocks.qe.scheduler.SchedulerTestBase;
import com.starrocks.qe.scheduler.slot.BaseSlotManager;
import com.starrocks.qe.scheduler.slot.LogicalSlot;
import com.starrocks.qe.scheduler.slot.SlotManager;
import com.starrocks.qe.scheduler.slot.WarehouseInFlightTracker;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.BackendResourceStat;
import mockit.Mock;
import mockit.MockUp;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for A3: registration / unregistration of pending queries in {@link WarehouseInFlightTracker}
 * driven by {@link QueryQueueManager#maybeWait}.
 *
 * <p>Only queries routed through the {@link com.starrocks.qe.scheduler.slot.GlobalSlotProvider}
 * path with query queue v2 enabled should ever appear in the tracker. The
 * {@link com.starrocks.qe.scheduler.slot.LocalSlotProvider} path (LOAD, STATISTICS, schema-only,
 * queue disabled) must never populate the tracker.
 */
public class QueryQueueManagerPreScaleTest extends SchedulerTestBase {
    private static final long WAREHOUSE_ID = WarehouseManager.DEFAULT_WAREHOUSE_ID;

    private boolean prevEnableQueryQueueV2;
    private boolean prevQueueEnableSelect;
    private int prevQueueConcurrencyHardLimit;
    private int prevQueuePendingTimeoutSecond;
    private int prevQueueMaxQueuedQueries;
    private int prevQueueTimeoutSecond;
    private double prevBigQueryThresholdRatio;
    private long prevPreScaleMaxWaitMs;
    private double prevPreScaleThresholdRatio;

    @BeforeAll
    public static void beforeClass() throws Exception {
        Config.proc_profile_cpu_enable = false;
        Config.proc_profile_mem_enable = false;
        SchedulerTestBase.beforeClass();

        MetricRepo.init();
    }

    @BeforeEach
    public void before() {
        prevEnableQueryQueueV2 = Config.enable_query_queue_v2;
        prevQueueEnableSelect = GlobalVariable.isEnableQueryQueueSelect();
        prevQueueConcurrencyHardLimit = GlobalVariable.getQueryQueueConcurrencyLimit();
        prevQueuePendingTimeoutSecond = GlobalVariable.getQueryQueuePendingTimeoutSecond();
        prevQueueMaxQueuedQueries = GlobalVariable.getQueryQueueMaxQueuedQueries();
        prevQueueTimeoutSecond = connectContext.getSessionVariable().getQueryTimeoutS();
        prevBigQueryThresholdRatio = Config.query_queue_big_query_slot_threshold_ratio;
        prevPreScaleMaxWaitMs = Config.query_queue_pre_scale_max_wait_ms;
        prevPreScaleThresholdRatio = Config.query_queue_pre_scale_slot_threshold_ratio;

        GlobalVariable.setQueryQueuePendingTimeoutSecond(Config.max_load_timeout_second);
        connectContext.getSessionVariable().setQueryTimeoutS(Config.max_load_timeout_second);

        mockFrontends(FRONTENDS);
        mockFrontendService(new MockFrontendServiceClient());

        // Reset and start a fresh slot manager so each test starts from a clean state.
        SlotManager slotManager = new SlotManager(GlobalStateMgr.getCurrentState().getResourceUsageMonitor());
        slotManager.start();
        new MockUp<GlobalStateMgr>() {
            @Mock
            public BaseSlotManager getSlotManager() {
                return slotManager;
            }
        };

        // Ensure tracker is empty at the start of each test.
        clearTracker();

        MetricRepo.COUNTER_QUERY_QUEUE_PENDING.increase(-MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue());

        connectContext.setStartTime();
    }

    @AfterEach
    public void after() {
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> 0 == MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue());
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> GlobalStateMgr.getCurrentState().getSlotManager().getSlots().isEmpty());

        clearTracker();
        BackendResourceStat.getInstance().reset();

        Config.enable_query_queue_v2 = prevEnableQueryQueueV2;
        GlobalVariable.setEnableQueryQueueSelect(prevQueueEnableSelect);
        GlobalVariable.setQueryQueueConcurrencyLimit(prevQueueConcurrencyHardLimit);
        GlobalVariable.setQueryQueuePendingTimeoutSecond(prevQueuePendingTimeoutSecond);
        GlobalVariable.setQueryQueueMaxQueuedQueries(prevQueueMaxQueuedQueries);
        connectContext.getSessionVariable().setQueryTimeoutS(prevQueueTimeoutSecond);
        Config.query_queue_big_query_slot_threshold_ratio = prevBigQueryThresholdRatio;
        Config.query_queue_pre_scale_max_wait_ms = prevPreScaleMaxWaitMs;
        Config.query_queue_pre_scale_slot_threshold_ratio = prevPreScaleThresholdRatio;
    }

    /**
     * Tracker is populated while the query is pending on the GlobalSlotProvider path
     * (query queue v2 enabled, select queue enabled, real OlapScan) and cleared after the
     * query finishes.
     */
    @Test
    public void testTrackerPopulatedDuringPendingForGlobalSlotProvider() throws Exception {
        final int concurrencyLimit = 1;
        Config.enable_query_queue_v2 = true;
        GlobalVariable.setEnableQueryQueueSelect(true);
        GlobalVariable.setQueryQueueConcurrencyLimit(concurrencyLimit);
        GlobalVariable.setQueryQueueMaxQueuedQueries(8);

        // Configure BackendResourceStat so V2 has a non-zero totalSlots; needed for opts.v2() and tracking.
        BackendResourceStat.getInstance().setNumCoresOfBe(WAREHOUSE_ID, 1, 8);
        BackendResourceStat.getInstance().setMemLimitBytesOfBe(WAREHOUSE_ID, 1, 64L * 1024 * 1024 * 1024);

        // Fill the queue so the next query becomes pending and stays pending.
        List<DefaultCoordinator> runningCoords = new ArrayList<>();
        for (int i = 0; i < concurrencyLimit; i++) {
            DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
            manager.maybeWait(connectContext, coord);
            Assertions.assertEquals(LogicalSlot.State.ALLOCATED, coord.getSlot().getState());
            runningCoords.add(coord);
        }

        // Tracker should be empty before the pending query enters.
        Assertions.assertEquals(0, WarehouseInFlightTracker.getInstance().getMaxRawSlots(WAREHOUSE_ID));

        // Submit a query that will pend.
        DefaultCoordinator pendingCoord = getSchedulerWithQueryId("select count(1) from lineitem");
        Thread thread = new Thread(() -> {
            try {
                manager.maybeWait(connectContext, pendingCoord);
            } catch (StarRocksException | InterruptedException ignored) {
                // Expected when we cancel below.
            }
        });
        thread.start();

        // Wait until the pending query is observable via the metric.
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(
                () -> MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue() > 0);
        Assertions.assertEquals(LogicalSlot.State.REQUIRING, pendingCoord.getSlot().getState());

        // Tracker should now have an entry for the pending query.
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(
                () -> WarehouseInFlightTracker.getInstance().getMaxRawSlots(WAREHOUSE_ID) > 0);
        assertThat(WarehouseInFlightTracker.getInstance().getMaxRawSlots(WAREHOUSE_ID)).isGreaterThan(0);
        WarehouseInFlightTracker.InFlightEntry entry =
                WarehouseInFlightTracker.getInstance().getEntry(WAREHOUSE_ID, pendingCoord.getSlot().getSlotId());
        Assertions.assertNotNull(entry, "tracker entry must exist while pending");
        assertThat(entry.rawSlots).isGreaterThanOrEqualTo(entry.clampedSlots);
        assertThat(entry.clampedSlots).isEqualTo(pendingCoord.getSlot().getNumPhysicalSlots());

        // Release the head running query and cancel the pending one to drain.
        pendingCoord.cancel("Cancel by test");
        runningCoords.forEach(DefaultCoordinator::onFinished);
        thread.join(5_000);

        // After pending exits (cancelled or otherwise), tracker entry must be cleared.
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(
                () -> WarehouseInFlightTracker.getInstance().getMaxRawSlots(WAREHOUSE_ID) == 0);
        Assertions.assertEquals(0, WarehouseInFlightTracker.getInstance().getMaxRawSlots(WAREHOUSE_ID));
        Assertions.assertNull(
                WarehouseInFlightTracker.getInstance().getEntry(WAREHOUSE_ID, pendingCoord.getSlot().getSlotId()));
    }

    /**
     * LocalSlotProvider path (e.g. select with queue disabled, or schema-only) does not pass through
     * the GlobalSlotProvider, so the tracker must NOT be populated.
     */
    @Test
    public void testLocalSlotProviderPathDoesNotPopulateTracker() throws Exception {
        // Disable select query queue -> JobSpec.getSlotProvider() returns LocalSlotProvider.
        Config.enable_query_queue_v2 = true;
        GlobalVariable.setEnableQueryQueueSelect(false);

        BackendResourceStat.getInstance().setNumCoresOfBe(WAREHOUSE_ID, 1, 8);
        BackendResourceStat.getInstance().setMemLimitBytesOfBe(WAREHOUSE_ID, 1, 64L * 1024 * 1024 * 1024);

        DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
        manager.maybeWait(connectContext, coord);

        // The query never enters pending state (LocalSlotProvider just allocates).
        Assertions.assertEquals(0L, MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue().longValue());
        // Tracker should remain empty on the LocalSlotProvider path.
        Assertions.assertEquals(0, WarehouseInFlightTracker.getInstance().getMaxRawSlots(WAREHOUSE_ID));
        Assertions.assertEquals(0L, WarehouseInFlightTracker.getInstance().getSumRawSlots(WAREHOUSE_ID));
    }

    /**
     * Schema-only path (information_schema) routes through LocalSlotProvider because the query is not
     * queue-eligible (isNeedQueued() == false). The tracker must remain empty.
     */
    @Test
    public void testSchemaOnlyQueryDoesNotPopulateTracker() throws Exception {
        Config.enable_query_queue_v2 = true;
        GlobalVariable.setEnableQueryQueueSelect(true);

        BackendResourceStat.getInstance().setNumCoresOfBe(WAREHOUSE_ID, 1, 8);
        BackendResourceStat.getInstance().setMemLimitBytesOfBe(WAREHOUSE_ID, 1, 64L * 1024 * 1024 * 1024);

        DefaultCoordinator coord =
                getSchedulerWithQueryId("select TABLE_CATALOG from information_schema.tables");
        manager.maybeWait(connectContext, coord);

        Assertions.assertEquals(0L, MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue().longValue());
        Assertions.assertEquals(0, WarehouseInFlightTracker.getInstance().getMaxRawSlots(WAREHOUSE_ID));
    }

    /**
     * If the pending loop throws (e.g., pending timeout), the {@code finally} block must still
     * clear the tracker entry.
     */
    @Test
    public void testTrackerClearedOnPendingTimeout() throws Exception {
        final int concurrencyLimit = 1;
        Config.enable_query_queue_v2 = true;
        GlobalVariable.setEnableQueryQueueSelect(true);
        GlobalVariable.setQueryQueueConcurrencyLimit(concurrencyLimit);
        GlobalVariable.setQueryQueueMaxQueuedQueries(8);
        // Very short pending timeout so the second query throws quickly.
        GlobalVariable.setQueryQueuePendingTimeoutSecond(1);

        BackendResourceStat.getInstance().setNumCoresOfBe(WAREHOUSE_ID, 1, 8);
        BackendResourceStat.getInstance().setMemLimitBytesOfBe(WAREHOUSE_ID, 1, 64L * 1024 * 1024 * 1024);

        // Fill the queue.
        List<DefaultCoordinator> runningCoords = new ArrayList<>();
        for (int i = 0; i < concurrencyLimit; i++) {
            DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
            manager.maybeWait(connectContext, coord);
            runningCoords.add(coord);
        }

        DefaultCoordinator timingOutCoord = getSchedulerWithQueryId("select count(1) from lineitem");
        Assertions.assertThrows(StarRocksException.class,
                () -> manager.maybeWait(connectContext, timingOutCoord));

        // Even though the call threw, the tracker entry must be cleared by the finally block.
        Assertions.assertEquals(0, WarehouseInFlightTracker.getInstance().getMaxRawSlots(WAREHOUSE_ID));
        Assertions.assertNull(WarehouseInFlightTracker.getInstance().getEntry(
                WAREHOUSE_ID, timingOutCoord.getSlot().getSlotId()));

        runningCoords.forEach(DefaultCoordinator::onFinished);
    }

    /**
     * When the configured threshold is low enough that the query qualifies as "big", the
     * per-warehouse {@code query_queue_big_query_total} counter must be incremented at
     * pending-enter (B4). Verifies the GlobalSlotProvider path actually wires the bump.
     */
    @Test
    public void testBigQueryCounterBumpedOnPendingEnter() throws Exception {
        Config.enable_query_queue_v2 = true;
        GlobalVariable.setEnableQueryQueueSelect(true);
        GlobalVariable.setQueryQueueConcurrencyLimit(8);
        GlobalVariable.setQueryQueueMaxQueuedQueries(8);
        // Force every tracked query to be flagged as big.
        Config.query_queue_big_query_slot_threshold_ratio = 0.0;

        BackendResourceStat.getInstance().setNumCoresOfBe(WAREHOUSE_ID, 1, 8);
        BackendResourceStat.getInstance().setMemLimitBytesOfBe(WAREHOUSE_ID, 1, 64L * 1024 * 1024 * 1024);

        long baseline = WarehouseSlotMetricMgr.getBigQueryCounter(WAREHOUSE_ID).getValue();

        DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
        manager.maybeWait(connectContext, coord);
        Assertions.assertEquals(LogicalSlot.State.ALLOCATED, coord.getSlot().getState());

        assertThat(WarehouseSlotMetricMgr.getBigQueryCounter(WAREHOUSE_ID).getValue())
                .isEqualTo(baseline + 1);

        coord.onFinished();
    }

    /**
     * When a big query actually pends (isPending == true), the per-warehouse wait histogram
     * must record a sample in the finally block (B4). Fills the queue so the test query has
     * to wait, then cancels it to drain and observes the histogram's count incremented.
     */
    @Test
    public void testBigQueryWaitHistogramUpdatedAfterPending() throws Exception {
        final int concurrencyLimit = 1;
        Config.enable_query_queue_v2 = true;
        GlobalVariable.setEnableQueryQueueSelect(true);
        GlobalVariable.setQueryQueueConcurrencyLimit(concurrencyLimit);
        GlobalVariable.setQueryQueueMaxQueuedQueries(8);
        // Force every tracked query to be flagged as big.
        Config.query_queue_big_query_slot_threshold_ratio = 0.0;

        BackendResourceStat.getInstance().setNumCoresOfBe(WAREHOUSE_ID, 1, 8);
        BackendResourceStat.getInstance().setMemLimitBytesOfBe(WAREHOUSE_ID, 1, 64L * 1024 * 1024 * 1024);

        long baseline = WarehouseSlotMetricMgr.getBigQueryWaitHistogram(WAREHOUSE_ID).getCount();

        // Fill the queue so the next query becomes pending.
        List<DefaultCoordinator> runningCoords = new ArrayList<>();
        for (int i = 0; i < concurrencyLimit; i++) {
            DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
            manager.maybeWait(connectContext, coord);
            Assertions.assertEquals(LogicalSlot.State.ALLOCATED, coord.getSlot().getState());
            runningCoords.add(coord);
        }

        // Submit a query that will pend.
        DefaultCoordinator pendingCoord = getSchedulerWithQueryId("select count(1) from lineitem");
        Thread thread = new Thread(() -> {
            try {
                manager.maybeWait(connectContext, pendingCoord);
            } catch (StarRocksException | InterruptedException ignored) {
                // Expected on cancellation below.
            }
        });
        thread.start();

        // Wait until the pending query is observable.
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(
                () -> MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue() > 0);

        // Cancel and drain so the finally block in maybeWait runs and records the histogram sample.
        pendingCoord.cancel("Cancel by test");
        runningCoords.forEach(DefaultCoordinator::onFinished);
        thread.join(5_000);

        // The histogram count must have advanced by 1 from the baseline.
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(
                () -> WarehouseSlotMetricMgr.getBigQueryWaitHistogram(WAREHOUSE_ID).getCount() > baseline);
        assertThat(WarehouseSlotMetricMgr.getBigQueryWaitHistogram(WAREHOUSE_ID).getCount())
                .isGreaterThan(baseline);
    }

    /**
     * Pre-scale wait (C3) triggers when configured + big query + GlobalSlotProvider + QQv2.
     * With {@code pre_scale_max_wait_ms = 1000} and threshold ratio = 0.0 (every query qualifies),
     * BackendResourceStat doesn't change during the test, so awaitCapacity never satisfies the gate
     * and exhausts the full wait window. The {@code maybeWait} call should therefore take ~1s longer
     * than an immediate admission.
     */
    @Test
    public void testPreScaleWaitTriggeredForBigQuery() throws Exception {
        Config.enable_query_queue_v2 = true;
        GlobalVariable.setEnableQueryQueueSelect(true);
        GlobalVariable.setQueryQueueConcurrencyLimit(8);
        GlobalVariable.setQueryQueueMaxQueuedQueries(8);

        BackendResourceStat.getInstance().setNumCoresOfBe(WAREHOUSE_ID, 1, 8);
        BackendResourceStat.getInstance().setMemLimitBytesOfBe(WAREHOUSE_ID, 1, 64L * 1024 * 1024 * 1024);

        // Enable pre-scale wait. Threshold ratio = 0.0 forces the gate to fire for any non-zero rawSlots.
        Config.query_queue_pre_scale_max_wait_ms = 1000L;
        Config.query_queue_pre_scale_slot_threshold_ratio = 0.0;

        DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
        long start = System.currentTimeMillis();
        manager.maybeWait(connectContext, coord);
        long elapsed = System.currentTimeMillis() - start;

        Assertions.assertEquals(LogicalSlot.State.ALLOCATED, coord.getSlot().getState());
        // Allow margin for jitter; pre-scale wait should have consumed roughly the configured window.
        assertThat(elapsed).isGreaterThanOrEqualTo(800L);

        coord.onFinished();
    }

    /**
     * With {@code pre_scale_max_wait_ms = 0} (default), the pre-scale gate is skipped entirely.
     * A query that would otherwise qualify must admit immediately.
     */
    @Test
    public void testPreScaleWaitDisabledWhenConfigZero() throws Exception {
        Config.enable_query_queue_v2 = true;
        GlobalVariable.setEnableQueryQueueSelect(true);
        GlobalVariable.setQueryQueueConcurrencyLimit(8);
        GlobalVariable.setQueryQueueMaxQueuedQueries(8);

        BackendResourceStat.getInstance().setNumCoresOfBe(WAREHOUSE_ID, 1, 8);
        BackendResourceStat.getInstance().setMemLimitBytesOfBe(WAREHOUSE_ID, 1, 64L * 1024 * 1024 * 1024);

        // Pre-scale disabled (default). Even with a ratio that would otherwise trigger, no wait.
        Config.query_queue_pre_scale_max_wait_ms = 0L;
        Config.query_queue_pre_scale_slot_threshold_ratio = 0.0;

        DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
        long start = System.currentTimeMillis();
        manager.maybeWait(connectContext, coord);
        long elapsed = System.currentTimeMillis() - start;

        Assertions.assertEquals(LogicalSlot.State.ALLOCATED, coord.getSlot().getState());
        // Should return promptly; allow generous margin to absorb scheduling overhead in CI.
        assertThat(elapsed).isLessThan(800L);

        coord.onFinished();
    }

    /**
     * When the pre-scale wait gate fires (C4), both the counter and the histogram metrics
     * must be bumped. The counter increments by 1 per gate firing; the histogram records
     * one sample (waitedMs) per firing, regardless of whether capacity was satisfied.
     */
    @Test
    public void testPreScaleWaitMetricsBumpedOnGateFiring() throws Exception {
        Config.enable_query_queue_v2 = true;
        GlobalVariable.setEnableQueryQueueSelect(true);
        GlobalVariable.setQueryQueueConcurrencyLimit(8);
        GlobalVariable.setQueryQueueMaxQueuedQueries(8);

        BackendResourceStat.getInstance().setNumCoresOfBe(WAREHOUSE_ID, 1, 8);
        BackendResourceStat.getInstance().setMemLimitBytesOfBe(WAREHOUSE_ID, 1, 64L * 1024 * 1024 * 1024);

        // Enable pre-scale wait with a short window; threshold ratio = 0.0 forces the gate to fire.
        Config.query_queue_pre_scale_max_wait_ms = 300L;
        Config.query_queue_pre_scale_slot_threshold_ratio = 0.0;

        long counterBaseline = WarehouseSlotMetricMgr.getPreScaleWaitCounter(WAREHOUSE_ID).getValue();
        long histogramBaseline = WarehouseSlotMetricMgr.getPreScaleWaitHistogram(WAREHOUSE_ID).getCount();

        DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
        manager.maybeWait(connectContext, coord);
        Assertions.assertEquals(LogicalSlot.State.ALLOCATED, coord.getSlot().getState());

        assertThat(WarehouseSlotMetricMgr.getPreScaleWaitCounter(WAREHOUSE_ID).getValue())
                .isEqualTo(counterBaseline + 1);
        assertThat(WarehouseSlotMetricMgr.getPreScaleWaitHistogram(WAREHOUSE_ID).getCount())
                .isEqualTo(histogramBaseline + 1);

        coord.onFinished();
    }

    /**
     * The tracker is a process-wide singleton; tests in this class operate on the default
     * warehouse and can leak entries across runs. Reach in via reflection to reset the map so
     * each test starts from a clean baseline.
     */
    @SuppressWarnings("unchecked")
    private static void clearTracker() {
        try {
            WarehouseInFlightTracker tracker = WarehouseInFlightTracker.getInstance();
            Field byWarehouseField = WarehouseInFlightTracker.class.getDeclaredField("byWarehouse");
            byWarehouseField.setAccessible(true);
            ((Map<Long, ?>) byWarehouseField.get(tracker)).clear();
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("Failed to clear WarehouseInFlightTracker singleton in tests", e);
        }
    }
}
