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

import com.google.api.client.util.Lists;
import com.google.common.collect.ImmutableList;
import com.starrocks.common.Config;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.metric.MetricRepo;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.BackendResourceStat;
import com.starrocks.system.ComputeNode;
import mockit.Mock;
import mockit.MockUp;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.starrocks.server.WarehouseManager.DEFAULT_WAREHOUSE_ID;
import static org.assertj.core.api.Assertions.assertThat;

public class SlotSelectionStrategyV2Test {
    private static final int NUM_CORES = 16;

    private boolean prevEnableQueryQueueV2 = false;
    private int prevQueryQueueV2ConcurrencyLevel = 0;
    private String prevQueryQueueV2ScheduleStrategy;
    private int prevMaxQueryQueueHistorySlotsNumber = 0;
    private int prevQueryQueueConcurrencyLimit = -1;

    private SlotManager slotManager;

    @BeforeAll
    public static void beforeClass() {
        MetricRepo.init();
    }

    @BeforeEach
    public void before() {
        prevEnableQueryQueueV2 = Config.enable_query_queue_v2;
        prevQueryQueueV2ConcurrencyLevel = Config.query_queue_v2_concurrency_level;
        prevQueryQueueV2ScheduleStrategy = Config.query_queue_v2_schedule_strategy;
        prevMaxQueryQueueHistorySlotsNumber = Config.max_query_queue_history_slots_number;
        prevQueryQueueConcurrencyLimit = GlobalVariable.getQueryQueueConcurrencyLimit();
        Config.enable_query_queue_v2 = true;
        Config.query_queue_v2_concurrency_level = 0;
        Config.query_queue_v2_schedule_strategy = QueryQueueOptions.SchedulePolicy.SWRR.name();
        Config.max_query_queue_history_slots_number = 0;
        GlobalVariable.setQueryQueueConcurrencyLimit(-1);

        ResourceUsageMonitor resourceUsageMonitor = new ResourceUsageMonitor();
        slotManager = new SlotManager(resourceUsageMonitor);

        BackendResourceStat.getInstance().setNumCoresOfBe(DEFAULT_WAREHOUSE_ID, 1, NUM_CORES);
    }

    @AfterEach
    public void after() {
        Config.enable_query_queue_v2 = prevEnableQueryQueueV2;
        Config.query_queue_v2_concurrency_level = prevQueryQueueV2ConcurrencyLevel;
        Config.query_queue_v2_schedule_strategy = prevQueryQueueV2ScheduleStrategy;
        Config.max_query_queue_history_slots_number = prevMaxQueryQueueHistorySlotsNumber;
        GlobalVariable.setQueryQueueConcurrencyLimit(prevQueryQueueConcurrencyLimit);

        BackendResourceStat.getInstance().reset();
    }

    private static void useWideFbeCapacityForSchedulingTests() {
        Config.query_queue_v2_concurrency_level = NUM_CORES;
    }

    private SlotSelectionStrategyV2 createSlotSelectionStrategy() {
        return new SlotSelectionStrategyV2(slotManager, WarehouseManager.DEFAULT_WAREHOUSE_ID);
    }

    @Test
    public void testSmallSlotDoesNotUseExtraCapacityUnderFbe() {
        QueryQueueOptions opts = QueryQueueOptions.createFromEnv(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        SlotSelectionStrategyV2 strategy = new SlotSelectionStrategyV2(slotManager, WarehouseManager.DEFAULT_WAREHOUSE_ID);
        SlotTracker slotTracker = new SlotTracker(slotManager, ImmutableList.of(strategy));

        assertThat(opts.v2().getTotalSmallSlots()).isZero();

        LogicalSlot largeSlot = generateSlot(opts.v2().getTotalSlots() - 1);
        LogicalSlot smallSlot1 = generateSlot(1);
        LogicalSlot smallSlot2 = generateSlot(1);

        slotTracker.requireSlot(largeSlot);
        assertThat(strategy.peakSlotsToAllocate(slotTracker)).containsExactly(largeSlot);
        slotTracker.allocateSlot(largeSlot);

        slotTracker.requireSlot(smallSlot1);
        slotTracker.requireSlot(smallSlot2);

        List<LogicalSlot> peakSmallSlots = strategy.peakSlotsToAllocate(slotTracker);
        assertThat(peakSmallSlots).containsExactly(smallSlot1);
        slotTracker.allocateSlot(smallSlot1);

        assertThat(strategy.peakSlotsToAllocate(slotTracker)).isEmpty();

        slotTracker.releaseSlot(largeSlot.getSlotId());
        assertThat(strategy.peakSlotsToAllocate(slotTracker)).containsExactly(smallSlot2);
    }

    @Test
    public void testPointQueriesAreCappedByFbeTotalSlots() {
        int oldConcurrencyLevel = Config.query_queue_v2_concurrency_level;
        try {
            Config.query_queue_v2_concurrency_level = 2;
            BackendResourceStat.getInstance().setNumCoresOfBe(DEFAULT_WAREHOUSE_ID, 2, NUM_CORES);
            BackendResourceStat.getInstance().setNumCoresOfBe(DEFAULT_WAREHOUSE_ID, 3, NUM_CORES);

            QueryQueueOptions opts = QueryQueueOptions.createFromEnv(WarehouseManager.DEFAULT_WAREHOUSE_ID);
            SlotSelectionStrategyV2 strategy = new SlotSelectionStrategyV2(slotManager, WarehouseManager.DEFAULT_WAREHOUSE_ID);
            SlotTracker slotTracker = new SlotTracker(slotManager, ImmutableList.of(strategy));

            assertThat(opts.v2().getNumWorkers()).isEqualTo(3);
            assertThat(opts.v2().getTotalSlots()).isEqualTo(24);
            assertThat(opts.v2().getTotalSmallSlots()).isZero();

            List<LogicalSlot> pointSlots = IntStream.range(0, 25)
                    .mapToObj(i -> generateSlot(1))
                    .collect(Collectors.toList());
            pointSlots.forEach(slotTracker::requireSlot);

            List<LogicalSlot> peaked = strategy.peakSlotsToAllocate(slotTracker);
            assertThat(peaked).hasSize(24);
            peaked.forEach(slotTracker::allocateSlot);
            assertThat(strategy.peakSlotsToAllocate(slotTracker)).isEmpty();
        } finally {
            Config.query_queue_v2_concurrency_level = oldConcurrencyLevel;
        }
    }

    @Test
    public void testHeadLineBlocking1() {
        useWideFbeCapacityForSchedulingTests();
        QueryQueueOptions opts = QueryQueueOptions.createFromEnv(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        SlotSelectionStrategyV2 strategy = new SlotSelectionStrategyV2(slotManager, WarehouseManager.DEFAULT_WAREHOUSE_ID);
        SlotTracker slotTracker = new SlotTracker(slotManager, ImmutableList.of(strategy));

        LogicalSlot slot1 = generateSlot(opts.v2().getTotalSlots() / 2 + 1);
        LogicalSlot slot2 = generateSlot(opts.v2().getTotalSlots() / 2);
        LogicalSlot slot3 = generateSlot(2);

        // 1. Require and allocate slot1.
        slotTracker.requireSlot(slot1);
        assertThat(strategy.peakSlotsToAllocate(slotTracker)).containsExactly(slot1);
        slotTracker.allocateSlot(slot1);

        // 2. Require slot2.
        slotTracker.requireSlot(slot2);
        assertThat(strategy.peakSlotsToAllocate(slotTracker)).isEmpty();

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
                peakSlots.forEach(slot -> assertThat(slotTracker.releaseSlot(slot.getSlotId())).isSameAs(slot));
            }
        }

        // Try peak the only rest slot2, but it is blocked by slot1.
        assertThat(strategy.peakSlotsToAllocate(slotTracker)).isEmpty();

        // 4. slot3 cannot be peaked because it is blocked by slot2.
        slotTracker.requireSlot(slot3);
        assertThat(strategy.peakSlotsToAllocate(slotTracker)).isEmpty();

        // 5. slot3 can be peaked after releasing the pending slot2.
        assertThat(slotTracker.releaseSlot(slot2.getSlotId())).isSameAs(slot2);
        assertThat(strategy.peakSlotsToAllocate(slotTracker)).containsExactly(slot3);
        slotTracker.allocateSlot(slot3);
        assertThat(slotTracker.releaseSlot(slot3.getSlotId())).isSameAs(slot3);
    }

    @Test
    public void testHeadLineBlocking2() {
        useWideFbeCapacityForSchedulingTests();
        QueryQueueOptions opts = QueryQueueOptions.createFromEnv(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        SlotSelectionStrategyV2 strategy = new SlotSelectionStrategyV2(slotManager, WarehouseManager.DEFAULT_WAREHOUSE_ID);
        SlotTracker slotTracker = new SlotTracker(slotManager, ImmutableList.of(strategy));

        LogicalSlot slot1 = generateSlot(opts.v2().getTotalSlots() / 2 + 1);
        LogicalSlot slot2 = generateSlot(opts.v2().getTotalSlots() / 2);
        LogicalSlot slot3 = generateSlot(2);

        // 1. Require and allocate slot1.
        slotTracker.requireSlot(slot1);
        assertThat(strategy.peakSlotsToAllocate(slotTracker)).containsExactly(slot1);
        slotTracker.allocateSlot(slot1);

        // 2. Require slot2.
        slotTracker.requireSlot(slot2);
        assertThat(strategy.peakSlotsToAllocate(slotTracker)).isEmpty();

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
                peakSlots.forEach(slot -> assertThat(slotTracker.releaseSlot(slot.getSlotId())).isSameAs(slot));
            }
        }

        // Try peak the only rest slot2, but it is blocked by slot1.
        assertThat(strategy.peakSlotsToAllocate(slotTracker)).isEmpty();

        // 4. slot3 cannot be peaked because it is blocked by slot2.
        for (int i = 0; i < 10; i++) {
            slotTracker.requireSlot(slot3);
            // if current concurrency is zero, always peak one
            assertThat(strategy.peakSlotsToAllocate(slotTracker)).isEmpty();
            assertThat(slotTracker.releaseSlot(slot3.getSlotId())).isSameAs(slot3);
        }
        slotTracker.requireSlot(slot3);
        assertThat(strategy.peakSlotsToAllocate(slotTracker)).isEmpty();

        // 5. slot2 and slot3 can be peaked after releasing slot1.
        slotTracker.releaseSlot(slot1.getSlotId());
        assertThat(strategy.peakSlotsToAllocate(slotTracker)).containsExactly(slot2, slot3);
    }

    @Test
    public void testUpdateOptionsPeriodicallyAtAllocating() throws InterruptedException {
        QueryQueueOptions opts = QueryQueueOptions.createFromEnv(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        SlotSelectionStrategyV2 strategy = new SlotSelectionStrategyV2(slotManager, WarehouseManager.DEFAULT_WAREHOUSE_ID);
        SlotTracker slotTracker = new SlotTracker(slotManager, ImmutableList.of(strategy));

        LogicalSlot slot1 = generateSlot(opts.v2().getTotalSlots() / 2 + 1);
        LogicalSlot slot2 = generateSlot(opts.v2().getTotalSlots() / 2 - 1);
        LogicalSlot slot3 = generateSlot(2);

        // 1. Require slot1, slot2, slot3.
        assertThat(slotTracker.requireSlot(slot1)).isTrue();
        assertThat(slotTracker.requireSlot(slot2)).isTrue();
        assertThat(slotTracker.requireSlot(slot3)).isTrue();

        // 2. Peak slot2 and slot3.
        assertThat(strategy.peakSlotsToAllocate(slotTracker)).containsExactly(slot3, slot2);

        // Make options changed.
        BackendResourceStat.getInstance().setNumCoresOfBe(DEFAULT_WAREHOUSE_ID, 1, NUM_CORES * 2);
        Thread.sleep(1200);
        // 3. Allocate slot2 and slot3.
        slotTracker.allocateSlot(slot2);
        slotTracker.allocateSlot(slot3);
        assertThat(slot2.getState()).isEqualTo(LogicalSlot.State.ALLOCATED);
        assertThat(slot3.getState()).isEqualTo(LogicalSlot.State.ALLOCATED);
        assertThat(slotTracker.getNumAllocatedSlots()).isEqualTo(slot2.getNumPhysicalSlots() + slot3.getNumPhysicalSlots());

        // 4. Peak slot1.
        assertThat(strategy.peakSlotsToAllocate(slotTracker)).containsExactly(slot1);
        slotTracker.allocateSlot(slot1);
        assertThat(slot1.getState()).isEqualTo(LogicalSlot.State.ALLOCATED);
        assertThat(slotTracker.getNumAllocatedSlots()).isEqualTo(
                slot2.getNumPhysicalSlots() + slot3.getNumPhysicalSlots() + slot1.getNumPhysicalSlots());
    }

    @Test
    public void testUpdateOptionsPeriodicallyAtReleasing() throws InterruptedException {
        QueryQueueOptions opts = QueryQueueOptions.createFromEnv(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        SlotSelectionStrategyV2 strategy = new SlotSelectionStrategyV2(slotManager, WarehouseManager.DEFAULT_WAREHOUSE_ID);
        SlotTracker slotTracker = new SlotTracker(slotManager, ImmutableList.of(strategy));

        LogicalSlot slot1 = generateSlot(opts.v2().getTotalSlots() / 2 + 1);
        LogicalSlot slot2 = generateSlot(opts.v2().getTotalSlots() / 2 - 1);
        LogicalSlot slot3 = generateSlot(2);

        // 1. Require slot1, slot2, slot3.
        assertThat(slotTracker.requireSlot(slot1)).isTrue();
        assertThat(slotTracker.requireSlot(slot2)).isTrue();
        assertThat(slotTracker.requireSlot(slot3)).isTrue();

        // 2. Peak, allocate slot2 and slot3.
        assertThat(strategy.peakSlotsToAllocate(slotTracker)).containsExactly(slot3, slot2);
        slotTracker.allocateSlot(slot2);
        slotTracker.allocateSlot(slot3);
        assertThat(slot2.getState()).isEqualTo(LogicalSlot.State.ALLOCATED);
        assertThat(slot3.getState()).isEqualTo(LogicalSlot.State.ALLOCATED);
        assertThat(slotTracker.getNumAllocatedSlots()).isEqualTo(slot2.getNumPhysicalSlots() + slot3.getNumPhysicalSlots());

        // Make options changed.
        BackendResourceStat.getInstance().setNumCoresOfBe(DEFAULT_WAREHOUSE_ID, 1, NUM_CORES * 2);
        Thread.sleep(1200);
        // 3. Release slot2 and slot3.
        assertThat(slotTracker.releaseSlot(slot2.getSlotId())).isSameAs(slot2);
        assertThat(slotTracker.releaseSlot(slot3.getSlotId())).isSameAs(slot3);

        // 4. Peak slot1.
        assertThat(strategy.peakSlotsToAllocate(slotTracker)).containsExactly(slot1);
        slotTracker.allocateSlot(slot1);
        assertThat(slot1.getState()).isEqualTo(LogicalSlot.State.ALLOCATED);
        assertThat(slotTracker.getNumAllocatedSlots()).isEqualTo(slot1.getNumPhysicalSlots());
    }

    @Test
    public void testUpdateOptionsChangeQueueStrategyOnline() throws InterruptedException {
        QueryQueueOptions opts = QueryQueueOptions.createFromEnv(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        SlotSelectionStrategyV2 strategy = new SlotSelectionStrategyV2(slotManager, WarehouseManager.DEFAULT_WAREHOUSE_ID);
        SlotTracker slotTracker = new SlotTracker(slotManager, ImmutableList.of(strategy));

        LogicalSlot slot1 = generateSlot(opts.v2().getTotalSlots() / 2 + 1);
        LogicalSlot slot2 = generateSlot(opts.v2().getTotalSlots() / 2 - 1);
        LogicalSlot slot3 = generateSlot(2);

        // 1. Require slot1, slot2, slot3.
        assertThat(slotTracker.requireSlot(slot1)).isTrue();
        assertThat(slotTracker.requireSlot(slot2)).isTrue();
        assertThat(slotTracker.requireSlot(slot3)).isTrue();
        assertThat(strategy.peakSlotsToAllocate(slotTracker)).containsExactly(slot3, slot2);

        // update query queue strategy
        Config.query_queue_v2_schedule_strategy = QueryQueueOptions.SchedulePolicy.SJF.name();
        Thread.sleep(1200);
        assertThat(strategy.peakSlotsToAllocate(slotTracker)).containsExactly(slot3, slot2);
        assertThat(strategy.getCurrentOptions()).isNotEqualTo(opts);
        assertThat(strategy.getCurrentOptions().hashCode()).isNotEqualTo(opts.hashCode());
        assertThat(strategy.getCurrentOptions().getPolicy()).isEqualTo(QueryQueueOptions.SchedulePolicy.SJF);
        opts = strategy.getCurrentOptions();
        slotTracker.allocateSlot(slot2);
        slotTracker.allocateSlot(slot3);
        assertThat(slotTracker.releaseSlot(slot2.getSlotId())).isSameAs(slot2);
        assertThat(slotTracker.releaseSlot(slot3.getSlotId())).isSameAs(slot3);

        // change it back
        Config.query_queue_v2_schedule_strategy = QueryQueueOptions.SchedulePolicy.SWRR.name();
        Thread.sleep(1200);
        assertThat(strategy.peakSlotsToAllocate(slotTracker)).containsExactly(slot1);
        assertThat(strategy.getCurrentOptions()).isNotEqualTo(opts);
        assertThat(strategy.getCurrentOptions().hashCode()).isNotEqualTo(opts.hashCode());
        assertThat(strategy.getCurrentOptions().getPolicy()).isEqualTo(QueryQueueOptions.SchedulePolicy.SWRR);
    }

    private static LogicalSlot generateSlot(int numSlots) {
        return new LogicalSlot(UUIDUtil.genTUniqueId(), "fe", WarehouseManager.DEFAULT_WAREHOUSE_ID,
                LogicalSlot.ABSENT_GROUP_ID, numSlots, 0, 0, 0, 0, 0);
    }

    @Test
    public void testHistorySlotsQueue() {
        useWideFbeCapacityForSchedulingTests();
        Config.max_query_queue_history_slots_number = 10;
        QueryQueueOptions opts = QueryQueueOptions.createFromEnv(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        SlotSelectionStrategyV2 strategy = new SlotSelectionStrategyV2(slotManager, WarehouseManager.DEFAULT_WAREHOUSE_ID);
        SlotTracker slotTracker = new SlotTracker(slotManager, ImmutableList.of(strategy));

        LogicalSlot slot1 = generateSlot(opts.v2().getTotalSlots() / 2 + 1);
        LogicalSlot slot2 = generateSlot(opts.v2().getTotalSlots() / 2);
        LogicalSlot slot3 = generateSlot(2);

        assertThat(slotTracker.getSlots().isEmpty());
        assertThat(slotTracker.getHistorySlots().isEmpty());

        // 1. Require and allocate slot1.
        slotTracker.requireSlot(slot1);
        assertThat(strategy.peakSlotsToAllocate(slotTracker)).containsExactly(slot1);
        slotTracker.allocateSlot(slot1);

        // 2. Require slot2.
        slotTracker.requireSlot(slot2);
        assertThat(strategy.peakSlotsToAllocate(slotTracker)).isEmpty();

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
                peakSlots.forEach(slot -> assertThat(slotTracker.releaseSlot(slot.getSlotId())).isSameAs(slot));
            }
        }
        assertThat(!slotTracker.getHistorySlots().isEmpty());
        List<LogicalSlot> historySlots = Lists.newArrayList(slotTracker.getHistorySlots());
        assertThat(historySlots.size() == 10);
        assertThat(historySlots.stream().allMatch(slot -> slot.getExtraMessage().isEmpty()));

        Collection<LogicalSlot> slots = slotTracker.getSlots();
        assertThat(slots.size() == historySlots.size() + slotTracker.getSlots().size());

        // Try peak the only rest slot2, but it is blocked by slot1.
        assertThat(strategy.peakSlotsToAllocate(slotTracker)).isEmpty();

        // 4. slot3 cannot be peaked because it is blocked by slot2.
        slotTracker.requireSlot(slot3);
        assertThat(strategy.peakSlotsToAllocate(slotTracker)).isEmpty();

        // 5. slot3 can be peaked after releasing the pending slot2.
        assertThat(slotTracker.releaseSlot(slot2.getSlotId())).isSameAs(slot2);
        assertThat(strategy.peakSlotsToAllocate(slotTracker)).containsExactly(slot3);
        slotTracker.allocateSlot(slot3);
        assertThat(slotTracker.releaseSlot(slot3.getSlotId())).isSameAs(slot3);

        historySlots = Lists.newArrayList(slotTracker.getHistorySlots());
        assertThat(historySlots.size() == 10);
        slots = slotTracker.getSlots();
        assertThat(slots.size() == historySlots.size() + slotTracker.getSlots().size());

        Config.max_query_queue_history_slots_number = 0;
    }

    @Test
    public void testConcurrencyLimit1() {
        useWideFbeCapacityForSchedulingTests();
        QueryQueueOptions opts = QueryQueueOptions.createFromEnv(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        SlotSelectionStrategyV2 strategy = new SlotSelectionStrategyV2(slotManager, WarehouseManager.DEFAULT_WAREHOUSE_ID);
        SlotTracker slotTracker = new SlotTracker(slotManager, ImmutableList.of(strategy));

        int oldVal = GlobalVariable.getQueryQueueConcurrencyLimit();
        GlobalVariable.setQueryQueueConcurrencyLimit(10);

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
            List<LogicalSlot> runningSmallSlots = com.google.common.collect.Lists.newArrayList();
            while (concurrency < 9) {
                List<LogicalSlot> peakSlots = strategy.peakSlotsToAllocate(slotTracker);
                Assertions.assertThat(peakSlots.isEmpty()).isFalse();
                numPeakedSmallSlots += peakSlots.size();
                peakSlots.forEach(slotTracker::allocateSlot);
                concurrency = slotTracker.getCurrentCurrency();
                runningSmallSlots.addAll(peakSlots);
            }
            // The concurrency limit is 10 and slot1 already holds one, so a single batch can admit at most 9
            // of the 10 small slots. Before the batch counter was added this loop admitted all 10 and drove
            // the running count to 11, i.e. past the limit it was supposed to enforce.
            Assertions.assertThat(numPeakedSmallSlots).isEqualTo(9);
            Assertions.assertThat(slotTracker.getCurrentCurrency()).isEqualTo(10);
            // since concurrency is at the limit, all small slots are blocked.
            Assertions.assertThat(strategy.peakSlotsToAllocate(slotTracker)).isEmpty();
            // release every small slot, including the one that stayed pending because the batch stopped at
            // the limit; otherwise it would still be queued when slot2 is examined below.
            smallSlots.forEach(slot -> slotTracker.releaseSlot(slot.getSlotId()));
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
        GlobalVariable.setQueryQueueConcurrencyLimit(oldVal);
    }

    @Test
    public void testConcurrencyLimitWithLargeSlots() {
        QueryQueueOptions opts = QueryQueueOptions.createFromEnv(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        SlotSelectionStrategyV2 strategy = new SlotSelectionStrategyV2(slotManager, WarehouseManager.DEFAULT_WAREHOUSE_ID);
        SlotTracker slotTracker = new SlotTracker(slotManager, ImmutableList.of(strategy));

        GlobalVariable.setQueryQueueConcurrencyLimit(1);
        LogicalSlot largeSlot1 = generateSlot(opts.v2().getTotalSlots() - 1);
        LogicalSlot largeSlot2 = generateSlot(opts.v2().getTotalSlots() - 1);

        slotTracker.requireSlot(largeSlot1);
        List<LogicalSlot> peakSlots = strategy.peakSlotsToAllocate(slotTracker);
        assertThat(peakSlots).containsExactly(largeSlot1);
        slotTracker.allocateSlot(largeSlot1);

        slotTracker.requireSlot(largeSlot2);
        peakSlots = strategy.peakSlotsToAllocate(slotTracker);
        assertThat(peakSlots).isEmpty();

        slotTracker.releaseSlot(largeSlot1.getSlotId());

        peakSlots = strategy.peakSlotsToAllocate(slotTracker);
        assertThat(peakSlots).hasSize(1);
        assertThat(peakSlots).containsExactly(largeSlot2);
        slotTracker.releaseSlot(largeSlot2.getSlotId());
        GlobalVariable.setQueryQueueConcurrencyLimit(-1);
    }

    /**
     * The concurrency limit is an upper bound on a single batch, not only across batches. The tracker's
     * own counter cannot move while {@code peakSlotsToAllocate} is running, so a batch that does not count
     * what it has already picked compares every candidate against the same constant and runs past the
     * limit.
     */
    @Test
    public void testConcurrencyLimitCapsOneBatch() {
        useWideFbeCapacityForSchedulingTests();
        QueryQueueOptions opts = QueryQueueOptions.createFromEnv(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        SlotSelectionStrategyV2 strategy = createSlotSelectionStrategy();
        SlotTracker slotTracker = new SlotTracker(slotManager, ImmutableList.of(strategy));

        final int concurrencyLimit = 3;
        final int numPendingQueries = 20;
        final int slotsPerQuery = 2;
        GlobalVariable.setQueryQueueConcurrencyLimit(concurrencyLimit);

        // The slot pool must have room for every pending query, otherwise the batch could be capped by the
        // pool and the assertion below would hold even without the concurrency limit bounding anything.
        assertThat(numPendingQueries * slotsPerQuery).isLessThan(opts.v2().getTotalSlots());

        List<LogicalSlot> pendingSlots = IntStream.range(0, numPendingQueries)
                .mapToObj(i -> generateSlot(slotsPerQuery))
                .collect(Collectors.toList());
        pendingSlots.forEach(slotTracker::requireSlot);

        assertThat(slotTracker.getCurrentCurrency()).isZero();
        assertThat(strategy.peakSlotsToAllocate(slotTracker)).hasSize(concurrencyLimit);
    }

    /**
     * The queries already running count towards the same limit, so a batch may only top the concurrency
     * up to the limit.
     */
    @Test
    public void testConcurrencyLimitCountsAlreadyAllocatedQueries() {
        useWideFbeCapacityForSchedulingTests();
        QueryQueueOptions opts = QueryQueueOptions.createFromEnv(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        SlotSelectionStrategyV2 strategy = createSlotSelectionStrategy();
        SlotTracker slotTracker = new SlotTracker(slotManager, ImmutableList.of(strategy));

        final int concurrencyLimit = 3;
        final int numPendingQueries = 10;
        final int slotsPerQuery = 2;
        GlobalVariable.setQueryQueueConcurrencyLimit(concurrencyLimit);
        assertThat((numPendingQueries + 1) * slotsPerQuery).isLessThan(opts.v2().getTotalSlots());

        LogicalSlot running = generateSlot(slotsPerQuery);
        slotTracker.requireSlot(running);
        assertThat(strategy.peakSlotsToAllocate(slotTracker)).containsExactly(running);
        slotTracker.allocateSlot(running);
        assertThat(slotTracker.getCurrentCurrency()).isEqualTo(1);

        List<LogicalSlot> pendingSlots = IntStream.range(0, numPendingQueries)
                .mapToObj(i -> generateSlot(slotsPerQuery))
                .collect(Collectors.toList());
        pendingSlots.forEach(slotTracker::requireSlot);

        List<LogicalSlot> peaked = strategy.peakSlotsToAllocate(slotTracker);
        assertThat(peaked).hasSize(concurrencyLimit - 1);
        peaked.forEach(slotTracker::allocateSlot);

        assertThat(slotTracker.getCurrentCurrency()).isEqualTo(concurrencyLimit);
        assertThat(strategy.peakSlotsToAllocate(slotTracker)).isEmpty();
    }

    @Test
    public void testConcurrencyLimitWithSmallSlots() {
        SlotSelectionStrategyV2 strategy = new SlotSelectionStrategyV2(slotManager, WarehouseManager.DEFAULT_WAREHOUSE_ID);
        SlotTracker slotTracker = new SlotTracker(slotManager, ImmutableList.of(strategy));

        GlobalVariable.setQueryQueueConcurrencyLimit(1);
        LogicalSlot smallSlot1 = generateSlot(1);
        LogicalSlot smallSlot2 = generateSlot(1);

        slotTracker.requireSlot(smallSlot1);
        List<LogicalSlot> peakSlots = strategy.peakSlotsToAllocate(slotTracker);
        assertThat(peakSlots).containsExactly(smallSlot1);
        slotTracker.allocateSlot(smallSlot1);

        slotTracker.requireSlot(smallSlot2);
        peakSlots = strategy.peakSlotsToAllocate(slotTracker);
        assertThat(peakSlots).isEmpty();

        slotTracker.releaseSlot(smallSlot1.getSlotId());

        peakSlots = strategy.peakSlotsToAllocate(slotTracker);
        assertThat(peakSlots).hasSize(1);
        assertThat(peakSlots).containsExactly(smallSlot2);
        slotTracker.releaseSlot(smallSlot2.getSlotId());
        GlobalVariable.setQueryQueueConcurrencyLimit(-1);
    }

    private static final long GB = 1024L * 1024 * 1024;

    /**
     * Point the memory gate at one node of 20 GB whose memory usage the test controls, and set the memory usage
     * limit, a fraction of the memory limit of a node.
     */
    private static ComputeNode useMemoryGate(double limit, long memUsedBytes) {
        GlobalVariable.setQueryQueueMemUsedPctLimit(limit);
        ComputeNode node = new ComputeNode(1, "127.0.0.1", 9050);
        node.setAlive(true);
        node.setMemLimitBytes(20 * GB);
        node.updateResourceUsage(0, memUsedBytes, 0);
        new MockUp<MemoryAdmissionGate>() {
            @Mock
            List<ComputeNode> getWarehouseNodes(long warehouseId) {
                return List.of(node);
            }
        };
        return node;
    }

    /**
     * A new resource usage report of the node, which also keeps the report fresh.
     */
    private static void report(ComputeNode node, long memUsedBytes) {
        node.updateResourceUsage(0, memUsedBytes, 0);
    }

    @Test
    public void testMemoryGateHoldsNewQueriesUntilUsageDrops() {
        useWideFbeCapacityForSchedulingTests();
        double prevLimit = GlobalVariable.getQueryQueueMemUsedPctLimit();
        try {
            ComputeNode node = useMemoryGate(0.5, 4 * GB);
            SlotSelectionStrategyV2 strategy = createSlotSelectionStrategy();
            SlotTracker slotTracker = new SlotTracker(slotManager, ImmutableList.of(strategy));

            LogicalSlot running = generateSlot(2);
            slotTracker.requireSlot(running);
            assertThat(strategy.peakSlotsToAllocate(slotTracker)).containsExactly(running);
            slotTracker.allocateSlot(running);

            // The node goes over the limit: new queries queue, the running one is untouched.
            report(node, 12 * GB);
            List<LogicalSlot> pending = IntStream.range(0, 3).mapToObj(i -> generateSlot(2)).collect(Collectors.toList());
            pending.forEach(slotTracker::requireSlot);
            for (int i = 0; i < 3; i++) {
                assertThat(strategy.peakSlotsToAllocate(slotTracker)).isEmpty();
            }
            assertThat(slotTracker.getCurrentCurrency()).isEqualTo(1);

            // Usage drops back under the limit: the queued queries run.
            report(node, 5 * GB);
            assertThat(strategy.peakSlotsToAllocate(slotTracker)).containsExactlyInAnyOrderElementsOf(pending);
        } finally {
            GlobalVariable.setQueryQueueMemUsedPctLimit(prevLimit);
        }
    }

    @Test
    public void testMemoryGateAdmitsOneQueryWhenNothingRuns() {
        useWideFbeCapacityForSchedulingTests();
        double prevLimit = GlobalVariable.getQueryQueueMemUsedPctLimit();
        try {
            // Over the limit with nothing running in the warehouse: loads, caches and queries that bypass the queue
            // hold memory that queueing cannot release, so the gate still lets one query through.
            useMemoryGate(0.5, 15 * GB);
            SlotSelectionStrategyV2 strategy = createSlotSelectionStrategy();
            SlotTracker slotTracker = new SlotTracker(slotManager, ImmutableList.of(strategy));

            List<LogicalSlot> pending = IntStream.range(0, 3).mapToObj(i -> generateSlot(2)).collect(Collectors.toList());
            pending.forEach(slotTracker::requireSlot);

            List<LogicalSlot> peaked = strategy.peakSlotsToAllocate(slotTracker);
            assertThat(peaked).hasSize(1);
            peaked.forEach(slotTracker::allocateSlot);
            assertThat(strategy.peakSlotsToAllocate(slotTracker)).isEmpty();
        } finally {
            GlobalVariable.setQueryQueueMemUsedPctLimit(prevLimit);
        }
    }

    @Test
    public void testMemoryGateDecisionHoldsForTheWholeBatch() {
        useWideFbeCapacityForSchedulingTests();
        double prevLimit = GlobalVariable.getQueryQueueMemUsedPctLimit();
        try {
            // Under the limit when the batch is selected, so every candidate that fits the slot pool is admitted in
            // the same batch; the next report is what the following batch will see.
            useMemoryGate(0.5, 9 * GB);
            SlotSelectionStrategyV2 strategy = createSlotSelectionStrategy();
            SlotTracker slotTracker = new SlotTracker(slotManager, ImmutableList.of(strategy));

            List<LogicalSlot> pending = IntStream.range(0, 5).mapToObj(i -> generateSlot(2)).collect(Collectors.toList());
            pending.forEach(slotTracker::requireSlot);
            assertThat(strategy.peakSlotsToAllocate(slotTracker)).containsExactlyInAnyOrderElementsOf(pending);
        } finally {
            GlobalVariable.setQueryQueueMemUsedPctLimit(prevLimit);
        }
    }

    @Test
    public void testMemoryGateIsEvaluatedAtMostOncePerBatch() {
        useWideFbeCapacityForSchedulingTests();
        double prevLimit = GlobalVariable.getQueryQueueMemUsedPctLimit();
        try {
            GlobalVariable.setQueryQueueMemUsedPctLimit(0.5);
            ComputeNode node = new ComputeNode(1, "127.0.0.1", 9050);
            node.setAlive(true);
            node.setMemLimitBytes(20 * GB);
            report(node, 4 * GB);
            AtomicInteger numEvaluations = new AtomicInteger();
            new MockUp<MemoryAdmissionGate>() {
                @Mock
                List<ComputeNode> getWarehouseNodes(long warehouseId) {
                    numEvaluations.incrementAndGet();
                    return List.of(node);
                }
            };
            SlotSelectionStrategyV2 strategy = createSlotSelectionStrategy();
            SlotTracker slotTracker = new SlotTracker(slotManager, ImmutableList.of(strategy));

            // Nothing is pending, so there is no candidate to decide on.
            assertThat(strategy.peakSlotsToAllocate(slotTracker)).isEmpty();
            assertThat(numEvaluations.get()).isZero();

            List<LogicalSlot> pending = IntStream.range(0, 5).mapToObj(i -> generateSlot(2)).collect(Collectors.toList());
            pending.forEach(slotTracker::requireSlot);
            assertThat(strategy.peakSlotsToAllocate(slotTracker)).containsExactlyInAnyOrderElementsOf(pending);
            assertThat(numEvaluations.get()).isOne();
        } finally {
            GlobalVariable.setQueryQueueMemUsedPctLimit(prevLimit);
        }
    }

    @Test
    public void testMemoryGateIgnoresUsageWhenNotConfigured() {
        useWideFbeCapacityForSchedulingTests();
        double prevLimit = GlobalVariable.getQueryQueueMemUsedPctLimit();
        try {
            useMemoryGate(0, 19 * GB);
            SlotSelectionStrategyV2 strategy = createSlotSelectionStrategy();
            SlotTracker slotTracker = new SlotTracker(slotManager, ImmutableList.of(strategy));

            LogicalSlot running = generateSlot(2);
            slotTracker.requireSlot(running);
            assertThat(strategy.peakSlotsToAllocate(slotTracker)).containsExactly(running);
            slotTracker.allocateSlot(running);

            List<LogicalSlot> pending = IntStream.range(0, 3).mapToObj(i -> generateSlot(2)).collect(Collectors.toList());
            pending.forEach(slotTracker::requireSlot);
            assertThat(strategy.peakSlotsToAllocate(slotTracker)).containsExactlyInAnyOrderElementsOf(pending);
        } finally {
            GlobalVariable.setQueryQueueMemUsedPctLimit(prevLimit);
        }
    }

    @Test
    public void testReportWakesAdmissionOnceTheMemoryGateReleases() {
        useWideFbeCapacityForSchedulingTests();
        double prevLimit = GlobalVariable.getQueryQueueMemUsedPctLimit();
        try {
            ComputeNode node = useMemoryGate(0.5, 4 * GB);
            SlotSelectionStrategyV2 strategy = createSlotSelectionStrategy();
            SlotTracker slotTracker = new SlotTracker(slotManager, ImmutableList.of(strategy));

            LogicalSlot running = generateSlot(2);
            slotTracker.requireSlot(running);
            assertThat(strategy.peakSlotsToAllocate(slotTracker)).containsExactly(running);
            slotTracker.allocateSlot(running);

            report(node, 12 * GB);
            slotTracker.requireSlot(generateSlot(2));
            assertThat(strategy.peakSlotsToAllocate(slotTracker)).isEmpty();

            // A report that keeps the node over the limit does not wake admission up.
            int numRequests = slotManager.requests.size();
            slotManager.getResourceUsageMonitor().notifyResourceUsageUpdate();
            assertThat(slotManager.requests).hasSize(numRequests);

            // The first report under the limit wakes it up once; later reports have nothing left to release.
            report(node, 5 * GB);
            slotManager.getResourceUsageMonitor().notifyResourceUsageUpdate();
            assertThat(slotManager.requests).hasSize(numRequests + 1);
            slotManager.getResourceUsageMonitor().notifyResourceUsageUpdate();
            assertThat(slotManager.requests).hasSize(numRequests + 1);
        } finally {
            GlobalVariable.setQueryQueueMemUsedPctLimit(prevLimit);
        }
    }

    @Test
    public void testDrainedQueueIsNoLongerHeldByTheMemoryGate() {
        useWideFbeCapacityForSchedulingTests();
        double prevLimit = GlobalVariable.getQueryQueueMemUsedPctLimit();
        try {
            ComputeNode node = useMemoryGate(0.5, 4 * GB);
            SlotSelectionStrategyV2 strategy = createSlotSelectionStrategy();
            SlotTracker slotTracker = new SlotTracker(slotManager, ImmutableList.of(strategy));

            LogicalSlot running = generateSlot(2);
            slotTracker.requireSlot(running);
            assertThat(strategy.peakSlotsToAllocate(slotTracker)).containsExactly(running);
            slotTracker.allocateSlot(running);

            // The gate holds a query, which is then canceled or times out while the node stays over the limit.
            report(node, 12 * GB);
            LogicalSlot held = generateSlot(2);
            slotTracker.requireSlot(held);
            assertThat(strategy.peakSlotsToAllocate(slotTracker)).isEmpty();
            slotTracker.releaseSlot(held.getSlotId());

            // Nothing waits any more, so the report under the limit has no admission to wake up.
            int numRequests = slotManager.requests.size();
            report(node, 5 * GB);
            slotManager.getResourceUsageMonitor().notifyResourceUsageUpdate();
            assertThat(slotManager.requests).hasSize(numRequests);

            // The same when the held query is admitted instead, as the one query of an idle warehouse.
            report(node, 12 * GB);
            held = generateSlot(2);
            slotTracker.requireSlot(held);
            assertThat(strategy.peakSlotsToAllocate(slotTracker)).isEmpty();
            slotTracker.releaseSlot(running.getSlotId());
            assertThat(strategy.peakSlotsToAllocate(slotTracker)).containsExactly(held);
            slotTracker.allocateSlot(held);
            report(node, 5 * GB);
            slotManager.getResourceUsageMonitor().notifyResourceUsageUpdate();
            assertThat(slotManager.requests).hasSize(numRequests);
        } finally {
            GlobalVariable.setQueryQueueMemUsedPctLimit(prevLimit);
        }
    }
}
