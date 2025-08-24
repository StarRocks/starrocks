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

package com.starrocks.qe.scheduler.warehouse;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.epack.warehouse.LocalWarehouse;
import com.starrocks.epack.warehouse.WarehouseProperty;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.metric.MetricRepo;
import com.starrocks.qe.scheduler.SchedulerTestBase;
import com.starrocks.qe.scheduler.SchedulerTestNoneDBBase;
import com.starrocks.qe.scheduler.slot.BaseSlotManager;
import com.starrocks.qe.scheduler.slot.LogicalSlot;
import com.starrocks.qe.scheduler.slot.QueryQueueOptions;
import com.starrocks.qe.scheduler.slot.SlotSelectionStrategyV2;
import com.starrocks.qe.scheduler.slot.SlotTracker;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.BackendResourceStat;
import com.starrocks.system.ComputeNode;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class WarehouseSlotSelectionStrategyV2Test extends SchedulerTestBase {

    private static int MAX_QUEUE_PENDING_LENGTH = 100;
    private static int DEFAULT_QUEUE_WAITING_TIMEOUT_SECOND = 300;
    private static int CPU_CORE_PER_BACKEND = 32;

    private static LocalWarehouse warehouse;
    private static BaseSlotManager baseSlotManager;

    @BeforeClass
    public static void beforeClass() throws Exception {
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();

        FeConstants.runningUnitTest = true;
        Config.enable_statistic_collect = false;

        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase(DB_NAME).useDatabase(DB_NAME);

        backend2 = UtFrameUtils.addMockBackend(10002, "127.0.0.2", 9060);
        backend2.setAlive(true);
        backend2.setCpuCores(16);
        backend2.setMemLimitBytes(16 * 1024 * 1024 * 1024L);
        backend2.setWarehouseId(WarehouseManager.DEFAULT_WAREHOUSE_ID);

        backend3 = UtFrameUtils.addMockBackend(10003, "127.0.0.3", 9060);
        backend3.setAlive(true);
        backend3.setCpuCores(16);
        backend3.setMemLimitBytes(16 * 1024 * 1024 * 1024L);

        backend2.setWarehouseId(WarehouseManager.DEFAULT_WAREHOUSE_ID);

        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().addBackend(backend2);
        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().addBackend(backend3);

        new MockUp<StarOSAgent>() {
            @Mock
            public List<Long> getWorkersByWorkerGroup(long workerGroupId) throws StarRocksException {
                return Lists.newArrayList(backend2.getId(), backend3.getId());
            }
        };
        new MockUp<WarehouseManager>() {
            @Mock
            public List<ComputeNode> getAliveComputeNodes(long warehouseId) {
                return new ArrayList<>(Arrays.asList(backend2, backend3));
            }
        };
        new MockUp<BackendResourceStat>() {
            @Mock
            public static int getAvgNumHardwareCoresOfBe(Map<Long, Integer> numHardwareCoresPerBe) {
                return CPU_CORE_PER_BACKEND;
            }
        };
        SchedulerTestBase.prepareTables(connectContext);
        warehouse = (LocalWarehouse) GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouse(
                WarehouseManager.DEFAULT_WAREHOUSE_ID);
        WarehouseProperty property = warehouse.getProperty();
        property.setEnableQueryQueue(true);
        property.setQueryQueueMaxQueuedQueries(MAX_QUEUE_PENDING_LENGTH);
        property.setQueryQueuePendingTimeoutSecond(DEFAULT_QUEUE_WAITING_TIMEOUT_SECOND);
        baseSlotManager = GlobalStateMgr.getCurrentState().getSlotManager();
    }

    @AfterClass
    public static void afterClass() {
        SchedulerTestNoneDBBase.afterClass();
    }

    @Before
    public void before() throws Exception {
        mockFrontends(FRONTENDS);
        mockFrontendService(new MockFrontendServiceClient());
        MetricRepo.COUNTER_QUERY_QUEUE_PENDING.increase(-MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue());
        connectContext.setStartTime();
    }

    @After
    public void after() {
        Awaitility.await().atMost(DEFAULT_QUEUE_WAITING_TIMEOUT_SECOND, TimeUnit.SECONDS)
                .until(() -> GlobalStateMgr.getCurrentState().getSlotManager().getSlots().isEmpty());
    }

    private static LogicalSlot generateSlot(int numSlots) {
        return new LogicalSlot(UUIDUtil.genTUniqueId(), "fe", WarehouseManager.DEFAULT_WAREHOUSE_ID,
                LogicalSlot.ABSENT_GROUP_ID, numSlots, 0, 0, 0, 0, 0);
    }

    @Test
    public void testHeadLineBlocking1() {
        QueryQueueOptions opts = QueryQueueOptions.createFromEnv(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        SlotSelectionStrategyV2 strategy = new SlotSelectionStrategyV2(baseSlotManager, WarehouseManager.DEFAULT_WAREHOUSE_ID);
        SlotTracker slotTracker = new SlotTracker(baseSlotManager, ImmutableList.of(strategy));

        LogicalSlot slot1 = generateSlot(opts.v2().getTotalSlots() / 2 + 1);
        LogicalSlot slot2 = generateSlot(opts.v2().getTotalSlots() / 2);
        LogicalSlot slot3 = generateSlot(2);

        // 1. Require and allocate slot1.
        slotTracker.requireSlot(slot1);
        Assertions.assertThat(strategy.peakSlotsToAllocate(slotTracker)).containsExactly(slot1);
        slotTracker.allocateSlot(slot1);

        // 2. Require slot2.
        slotTracker.requireSlot(slot2);
        Assertions.assertThat(strategy.peakSlotsToAllocate(slotTracker)).isEmpty();

        // 3. Require enough small slots to make its priority lower.
        {
            List<LogicalSlot> smallSlots = IntStream.range(0, 10)
                    .mapToObj(i -> generateSlot(2))
                    .collect(Collectors.toList());
            smallSlots.forEach(slotTracker::requireSlot);
            for (int numPeakedSmallSlots = 0; numPeakedSmallSlots < 10; ) {
                List<LogicalSlot> peakSlots = strategy.peakSlotsToAllocate(slotTracker);
                numPeakedSmallSlots += peakSlots.size();
                peakSlots.forEach(slotTracker::allocateSlot);
                peakSlots.forEach(slot -> Assertions.assertThat(slotTracker.releaseSlot(slot.getSlotId())).isSameAs(slot));
            }
        }

        // Try peak the only rest slot2, but it is blocked by slot1.
        Assertions.assertThat(strategy.peakSlotsToAllocate(slotTracker)).isEmpty();

        // 4. slot3 cannot be peaked because it is blocked by slot2.
        slotTracker.requireSlot(slot3);
        Assertions.assertThat(strategy.peakSlotsToAllocate(slotTracker)).isEmpty();

        // 5. slot3 can be peaked after releasing the pending slot2.
        Assertions.assertThat(slotTracker.releaseSlot(slot2.getSlotId())).isSameAs(slot2);
        Assertions.assertThat(strategy.peakSlotsToAllocate(slotTracker)).containsExactly(slot3);
        slotTracker.allocateSlot(slot3);
        Assertions.assertThat(slotTracker.releaseSlot(slot3.getSlotId())).isSameAs(slot3);
    }

    @Test
    public void testHeadLineBlocking2() {
        QueryQueueOptions opts = QueryQueueOptions.createFromEnv(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        SlotSelectionStrategyV2 strategy = new SlotSelectionStrategyV2(baseSlotManager, WarehouseManager.DEFAULT_WAREHOUSE_ID);
        SlotTracker slotTracker = new SlotTracker(baseSlotManager, ImmutableList.of(strategy));

        LogicalSlot slot1 = generateSlot(opts.v2().getTotalSlots() / 2 + 1);
        LogicalSlot slot2 = generateSlot(opts.v2().getTotalSlots() / 2);
        LogicalSlot slot3 = generateSlot(2);

        // 1. Require and allocate slot1.
        slotTracker.requireSlot(slot1);
        Assertions.assertThat(strategy.peakSlotsToAllocate(slotTracker)).containsExactly(slot1);
        slotTracker.allocateSlot(slot1);

        // 2. Require slot2.
        slotTracker.requireSlot(slot2);
        Assertions.assertThat(strategy.peakSlotsToAllocate(slotTracker)).isEmpty();

        // 3. Require enough small slots to make its priority lower.
        {
            List<LogicalSlot> smallSlots = IntStream.range(0, 10)
                    .mapToObj(i -> generateSlot(2))
                    .collect(Collectors.toList());
            smallSlots.forEach(slotTracker::requireSlot);
            for (int numPeakedSmallSlots = 0; numPeakedSmallSlots < 10; ) {
                List<LogicalSlot> peakSlots = strategy.peakSlotsToAllocate(slotTracker);
                numPeakedSmallSlots += peakSlots.size();
                peakSlots.forEach(slotTracker::allocateSlot);
                peakSlots.forEach(slot -> Assertions.assertThat(slotTracker.releaseSlot(slot.getSlotId())).isSameAs(slot));
            }
        }

        // Try peak the only rest slot2, but it is blocked by slot1.
        Assertions.assertThat(strategy.peakSlotsToAllocate(slotTracker)).isEmpty();

        // 4. slot3 cannot be peaked because it is blocked by slot2.
        for (int i = 0; i < 10; i++) {
            slotTracker.requireSlot(slot3);
            // if current concurrency is zero, always peak one
            Assertions.assertThat(strategy.peakSlotsToAllocate(slotTracker)).isEmpty();
            Assertions.assertThat(slotTracker.releaseSlot(slot3.getSlotId())).isSameAs(slot3);
        }
        slotTracker.requireSlot(slot3);
        Assertions.assertThat(strategy.peakSlotsToAllocate(slotTracker)).isEmpty();

        // 5. slot2 and slot3 can be peaked after releasing slot1.
        slotTracker.releaseSlot(slot1.getSlotId());
        Assertions.assertThat(strategy.peakSlotsToAllocate(slotTracker)).containsExactly(slot2, slot3);
    }

    @Test
    public void testConcurrencyLimit1() {
        QueryQueueOptions opts = QueryQueueOptions.createFromEnv(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        SlotSelectionStrategyV2 strategy = new SlotSelectionStrategyV2(baseSlotManager, WarehouseManager.DEFAULT_WAREHOUSE_ID);
        SlotTracker slotTracker = new SlotTracker(baseSlotManager, ImmutableList.of(strategy));

        WarehouseProperty warehouseProperty = warehouse.getProperty();
        int oldVal = warehouseProperty.getQueryQueueConcurrencyLimit();
        warehouseProperty.setQueryQueueConcurrencyLimit(10);

        LogicalSlot slot1 = generateSlot(opts.v2().getTotalSlots() / 2 + 1);
        LogicalSlot slot2 = generateSlot(opts.v2().getTotalSlots() / 2);
        LogicalSlot slot3 = generateSlot(2);

        // 1. Require and allocate slot1.
        slotTracker.requireSlot(slot1);
        Assertions.assertThat(strategy.peakSlotsToAllocate(slotTracker)).containsExactly(slot1);
        slotTracker.allocateSlot(slot1);

        // 2. Require slot2.
        slotTracker.requireSlot(slot2);
        Assertions.assertThat(strategy.peakSlotsToAllocate(slotTracker)).isEmpty();

        // 3. Require enough small slots to make its priority lower.
        Assertions.assertThat(slotTracker.getCurrentCurrency()).isEqualTo(1);
        {
            List<LogicalSlot> smallSlots = IntStream.range(0, 10)
                    .mapToObj(i -> generateSlot(2))
                    .collect(Collectors.toList());
            smallSlots.forEach(slotTracker::requireSlot);

            for (int numPeakedSmallSlots = 0; numPeakedSmallSlots < 10;) {
                List<LogicalSlot> peakSlots = strategy.peakSlotsToAllocate(slotTracker);
                numPeakedSmallSlots += peakSlots.size();
                peakSlots.forEach(slotTracker::allocateSlot);
                peakSlots.forEach(slot -> Assertions.assertThat(slotTracker.releaseSlot(slot.getSlotId())).isSameAs(slot));
            }
        }
        Assertions.assertThat(slotTracker.getCurrentCurrency()).isEqualTo(1);

        // Try peak the only rest slot2, but it is blocked by slot1.
        Assertions.assertThat(strategy.peakSlotsToAllocate(slotTracker)).isEmpty();

        // 4. slot3 cannot be peaked because it is blocked by slot2.
        for (int i = 0; i < 10; i++) {
            slotTracker.requireSlot(slot3);
            // if current concurrency is zero, always peak one
            Assertions.assertThat(strategy.peakSlotsToAllocate(slotTracker)).isEmpty();
            Assertions.assertThat(slotTracker.releaseSlot(slot3.getSlotId())).isSameAs(slot3);
        }
        slotTracker.requireSlot(slot3);
        Assertions.assertThat(strategy.peakSlotsToAllocate(slotTracker)).isEmpty();

        // 5. slot2 and slot3 can be peaked after releasing slot1.
        slotTracker.releaseSlot(slot1.getSlotId());
        Assertions.assertThat(strategy.peakSlotsToAllocate(slotTracker)).containsExactly(slot2, slot3);

        // reset concurrency limit
        warehouseProperty.setQueryQueueConcurrencyLimit(oldVal);
    }

    @Test
    public void testConcurrencyLimit2() {
        QueryQueueOptions opts = QueryQueueOptions.createFromEnv(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        SlotSelectionStrategyV2 strategy = new SlotSelectionStrategyV2(baseSlotManager, WarehouseManager.DEFAULT_WAREHOUSE_ID);
        SlotTracker slotTracker = new SlotTracker(baseSlotManager, ImmutableList.of(strategy));

        WarehouseProperty warehouseProperty = warehouse.getProperty();
        int oldVal = warehouseProperty.getQueryQueueConcurrencyLimit();
        warehouseProperty.setQueryQueueConcurrencyLimit(10);

        LogicalSlot slot1 = generateSlot(opts.v2().getTotalSlots() / 2 + 1);
        LogicalSlot slot2 = generateSlot(opts.v2().getTotalSlots() / 2);
        LogicalSlot slot3 = generateSlot(2);

        // 1. Require and allocate slot1.
        slotTracker.requireSlot(slot1);
        Assertions.assertThat(strategy.peakSlotsToAllocate(slotTracker)).containsExactly(slot1);
        slotTracker.allocateSlot(slot1);

        // 2. Require slot2.
        slotTracker.requireSlot(slot2);
        Assertions.assertThat(strategy.peakSlotsToAllocate(slotTracker)).isEmpty();

        // 3. Require enough small slots to make its priority lower.
        Assertions.assertThat(slotTracker.getCurrentCurrency()).isEqualTo(1);
        {
            List<LogicalSlot> smallSlots = IntStream.range(0, 10)
                    .mapToObj(i -> generateSlot(2))
                    .collect(Collectors.toList());
            smallSlots.forEach(slotTracker::requireSlot);

            int concurrency = slotTracker.getCurrentCurrency();
            int numPeakedSmallSlots = 0;
            List<LogicalSlot> runningSmallSlots = Lists.newArrayList();
            while (concurrency < 9) {
                List<LogicalSlot> peakSlots = strategy.peakSlotsToAllocate(slotTracker);
                Assertions.assertThat(peakSlots.isEmpty()).isFalse();
                numPeakedSmallSlots += peakSlots.size();
                peakSlots.forEach(slotTracker::allocateSlot);
                concurrency = slotTracker.getCurrentCurrency();
                runningSmallSlots.addAll(peakSlots);
            }
            Assertions.assertThat(numPeakedSmallSlots == 10);
            // since concurrency is 10, all small slots are blocked.
            Assertions.assertThat(strategy.peakSlotsToAllocate(slotTracker)).isEmpty();
            // release all running slots
            runningSmallSlots.forEach(slot -> Assertions.assertThat(slotTracker.releaseSlot(slot.getSlotId())).isSameAs(slot));
        }
        Assertions.assertThat(slotTracker.getCurrentCurrency()).isEqualTo(1);

        // Try peak the only rest slot2, but it is blocked by slot1.
        Assertions.assertThat(strategy.peakSlotsToAllocate(slotTracker)).isEmpty();

        // 4. slot3 cannot be peaked because it is blocked by slot2.
        for (int i = 0; i < 10; i++) {
            slotTracker.requireSlot(slot3);
            // if current concurrency is zero, always peak one
            Assertions.assertThat(strategy.peakSlotsToAllocate(slotTracker)).isEmpty();
            Assertions.assertThat(slotTracker.releaseSlot(slot3.getSlotId())).isSameAs(slot3);
        }
        slotTracker.requireSlot(slot3);
        Assertions.assertThat(strategy.peakSlotsToAllocate(slotTracker)).isEmpty();

        // 5. slot2 and slot3 can be peaked after releasing slot1.
        slotTracker.releaseSlot(slot1.getSlotId());
        Assertions.assertThat(strategy.peakSlotsToAllocate(slotTracker)).containsExactly(slot2, slot3);

        // reset concurrency limit
        warehouseProperty.setQueryQueueConcurrencyLimit(oldVal);
    }
}
