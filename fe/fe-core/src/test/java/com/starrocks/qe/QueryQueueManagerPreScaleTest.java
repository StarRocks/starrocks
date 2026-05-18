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
