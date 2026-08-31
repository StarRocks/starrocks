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

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.ResourceGroup;
import com.starrocks.catalog.ResourceGroupMgr;
import com.starrocks.common.Config;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.metric.MetricRepo;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.BackendResourceStat;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.server.WarehouseManager.DEFAULT_WAREHOUSE_ID;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Group-level queueing in {@link SlotSelectionStrategyV2}: when {@code enable_group_level_query_queue} is on,
 * queries beyond their resource group's {@code concurrency_limit} stay pending instead of being allocated,
 * without blocking the queries of other groups behind them in the queue.
 */
public class SlotSelectionStrategyV2GroupQueueTest {
    private static final int NUM_CORES = 16;
    private static final long GROUP_LIMITED = 10L;
    private static final long GROUP_UNLIMITED = 20L;

    private boolean prevEnableQueryQueueV2 = false;
    private int prevQueryQueueV2ConcurrencyLevel = 0;
    private String prevQueryQueueV2ScheduleStrategy;
    private int prevQueryQueueConcurrencyLimit = -1;
    private boolean prevEnableGroupLevelQueryQueue = false;

    private SlotManager slotManager;
    private final Map<Long, ResourceGroup> groups = new HashMap<>();

    @BeforeAll
    public static void beforeClass() {
        MetricRepo.init();
    }

    @BeforeEach
    public void before() {
        prevEnableQueryQueueV2 = Config.enable_query_queue_v2;
        prevQueryQueueV2ConcurrencyLevel = Config.query_queue_v2_concurrency_level;
        prevQueryQueueV2ScheduleStrategy = Config.query_queue_v2_schedule_strategy;
        prevQueryQueueConcurrencyLimit = GlobalVariable.getQueryQueueConcurrencyLimit();
        prevEnableGroupLevelQueryQueue = GlobalVariable.isEnableGroupLevelQueryQueue();

        Config.enable_query_queue_v2 = true;
        Config.query_queue_v2_concurrency_level = 0;
        Config.query_queue_v2_schedule_strategy = QueryQueueOptions.SchedulePolicy.SWRR.name();
        GlobalVariable.setQueryQueueConcurrencyLimit(-1);
        GlobalVariable.setEnableGroupLevelQueryQueue(true);

        slotManager = new SlotManager(new ResourceUsageMonitor());
        BackendResourceStat.getInstance().setNumCoresOfBe(DEFAULT_WAREHOUSE_ID, 1, NUM_CORES);

        groups.clear();
        ResourceGroup limitedGroup = new ResourceGroup();
        limitedGroup.setConcurrencyLimit(2);
        groups.put(GROUP_LIMITED, limitedGroup);
        groups.put(GROUP_UNLIMITED, new ResourceGroup());

        Map<Long, ResourceGroup> localGroups = groups;
        new MockUp<ResourceGroupMgr>() {
            @Mock
            public ResourceGroup getResourceGroup(long id) {
                return localGroups.get(id);
            }
        };
    }

    @AfterEach
    public void after() {
        Config.enable_query_queue_v2 = prevEnableQueryQueueV2;
        Config.query_queue_v2_concurrency_level = prevQueryQueueV2ConcurrencyLevel;
        Config.query_queue_v2_schedule_strategy = prevQueryQueueV2ScheduleStrategy;
        GlobalVariable.setQueryQueueConcurrencyLimit(prevQueryQueueConcurrencyLimit);
        GlobalVariable.setEnableGroupLevelQueryQueue(prevEnableGroupLevelQueryQueue);

        BackendResourceStat.getInstance().reset();
    }

    @Test
    public void testOverLimitQueriesWaitAndResumeOnRelease() {
        SlotSelectionStrategyV2 strategy = new SlotSelectionStrategyV2(slotManager, DEFAULT_WAREHOUSE_ID);
        SlotTracker slotTracker = new SlotTracker(slotManager, ImmutableList.of(strategy));

        LogicalSlot slot1 = generateSlot(1, GROUP_LIMITED);
        LogicalSlot slot2 = generateSlot(1, GROUP_LIMITED);
        LogicalSlot slot3 = generateSlot(1, GROUP_LIMITED);
        slotTracker.requireSlot(slot1);
        slotTracker.requireSlot(slot2);
        slotTracker.requireSlot(slot3);

        List<LogicalSlot> peaked = strategy.peakSlotsToAllocate(slotTracker);
        assertThat(peaked).containsExactly(slot1, slot2);
        peaked.forEach(slotTracker::allocateSlot);

        assertThat(strategy.peakSlotsToAllocate(slotTracker)).isEmpty();

        slotTracker.releaseSlot(slot1.getSlotId());
        assertThat(strategy.peakSlotsToAllocate(slotTracker)).containsExactly(slot3);
    }

    @Test
    public void testBlockedGroupDoesNotBlockOtherGroups() {
        SlotSelectionStrategyV2 strategy = new SlotSelectionStrategyV2(slotManager, DEFAULT_WAREHOUSE_ID);
        SlotTracker slotTracker = new SlotTracker(slotManager, ImmutableList.of(strategy));

        LogicalSlot limited1 = generateSlot(1, GROUP_LIMITED);
        LogicalSlot limited2 = generateSlot(1, GROUP_LIMITED);
        LogicalSlot limited3 = generateSlot(1, GROUP_LIMITED);
        LogicalSlot unlimited1 = generateSlot(1, GROUP_UNLIMITED);
        LogicalSlot noGroup1 = generateSlot(1, 0);
        slotTracker.requireSlot(limited1);
        slotTracker.requireSlot(limited2);
        slotTracker.requireSlot(limited3);
        slotTracker.requireSlot(unlimited1);
        slotTracker.requireSlot(noGroup1);

        List<LogicalSlot> peaked = strategy.peakSlotsToAllocate(slotTracker);
        assertThat(peaked).containsExactly(limited1, limited2, unlimited1, noGroup1);
        peaked.forEach(slotTracker::allocateSlot);

        assertThat(strategy.peakSlotsToAllocate(slotTracker)).isEmpty();

        slotTracker.releaseSlot(limited2.getSlotId());
        assertThat(strategy.peakSlotsToAllocate(slotTracker)).containsExactly(limited3);
    }

    @Test
    public void testDisabledVariableKeepsCurrentBehavior() {
        GlobalVariable.setEnableGroupLevelQueryQueue(false);

        SlotSelectionStrategyV2 strategy = new SlotSelectionStrategyV2(slotManager, DEFAULT_WAREHOUSE_ID);
        SlotTracker slotTracker = new SlotTracker(slotManager, ImmutableList.of(strategy));

        LogicalSlot slot1 = generateSlot(1, GROUP_LIMITED);
        LogicalSlot slot2 = generateSlot(1, GROUP_LIMITED);
        LogicalSlot slot3 = generateSlot(1, GROUP_LIMITED);
        slotTracker.requireSlot(slot1);
        slotTracker.requireSlot(slot2);
        slotTracker.requireSlot(slot3);

        assertThat(strategy.peakSlotsToAllocate(slotTracker)).containsExactly(slot1, slot2, slot3);
    }

    @Test
    public void testCancelPendingSlotKeepsGroupCountsIntact() {
        SlotSelectionStrategyV2 strategy = new SlotSelectionStrategyV2(slotManager, DEFAULT_WAREHOUSE_ID);
        SlotTracker slotTracker = new SlotTracker(slotManager, ImmutableList.of(strategy));

        LogicalSlot slot1 = generateSlot(1, GROUP_LIMITED);
        LogicalSlot slot2 = generateSlot(1, GROUP_LIMITED);
        LogicalSlot slot3 = generateSlot(1, GROUP_LIMITED);
        slotTracker.requireSlot(slot1);
        slotTracker.requireSlot(slot2);
        slotTracker.requireSlot(slot3);

        List<LogicalSlot> peaked = strategy.peakSlotsToAllocate(slotTracker);
        assertThat(peaked).containsExactly(slot1, slot2);
        peaked.forEach(slotTracker::allocateSlot);

        // Cancel the pending query: releasing it must not decrement the group's running count.
        slotTracker.releaseSlot(slot3.getSlotId());
        assertThat(strategy.peakSlotsToAllocate(slotTracker)).isEmpty();

        LogicalSlot slot4 = generateSlot(1, GROUP_LIMITED);
        slotTracker.requireSlot(slot4);
        assertThat(strategy.peakSlotsToAllocate(slotTracker)).isEmpty();

        slotTracker.releaseSlot(slot1.getSlotId());
        assertThat(strategy.peakSlotsToAllocate(slotTracker)).containsExactly(slot4);
    }

    /**
     * The count of a group whose queries have all finished must go back to zero, otherwise the group would
     * permanently lose part of its concurrency_limit.
     */
    @Test
    public void testGroupCountReturnsToZeroWhenAllQueriesFinish() {
        SlotSelectionStrategyV2 strategy = new SlotSelectionStrategyV2(slotManager, DEFAULT_WAREHOUSE_ID);
        SlotTracker slotTracker = new SlotTracker(slotManager, ImmutableList.of(strategy));

        LogicalSlot slot1 = generateSlot(1, GROUP_LIMITED);
        slotTracker.requireSlot(slot1);
        assertThat(strategy.peakSlotsToAllocate(slotTracker)).containsExactly(slot1);
        slotTracker.allocateSlot(slot1);
        slotTracker.releaseSlot(slot1.getSlotId());

        // The whole limit of 2 is available again.
        LogicalSlot slot2 = generateSlot(1, GROUP_LIMITED);
        LogicalSlot slot3 = generateSlot(1, GROUP_LIMITED);
        slotTracker.requireSlot(slot2);
        slotTracker.requireSlot(slot3);
        assertThat(strategy.peakSlotsToAllocate(slotTracker)).containsExactly(slot2, slot3);
    }

    /**
     * Group-level queueing has to work the same under the short-job-first policy, whose queue orders slots by
     * score instead of by insertion.
     */
    @Test
    public void testOverLimitQueriesWaitUnderShortJobFirst() {
        Config.query_queue_v2_schedule_strategy = QueryQueueOptions.SchedulePolicy.SJF.name();
        SlotSelectionStrategyV2 strategy = new SlotSelectionStrategyV2(slotManager, DEFAULT_WAREHOUSE_ID);
        SlotTracker slotTracker = new SlotTracker(slotManager, ImmutableList.of(strategy));

        LogicalSlot slot1 = generateSlot(1, GROUP_LIMITED);
        LogicalSlot slot2 = generateSlot(1, GROUP_LIMITED);
        LogicalSlot slot3 = generateSlot(1, GROUP_LIMITED);
        slotTracker.requireSlot(slot1);
        slotTracker.requireSlot(slot2);
        slotTracker.requireSlot(slot3);

        List<LogicalSlot> peaked = strategy.peakSlotsToAllocate(slotTracker);
        assertThat(peaked).hasSize(2);
        peaked.forEach(slotTracker::allocateSlot);

        assertThat(strategy.peakSlotsToAllocate(slotTracker)).isEmpty();

        slotTracker.releaseSlot(peaked.get(0).getSlotId());
        assertThat(strategy.peakSlotsToAllocate(slotTracker)).hasSize(1);
    }

    /**
     * Every options change moves the pending slots into a new queue and then supplements them from the known
     * contexts. A slot must not end up in the queue more than once, otherwise it would be allocated twice.
     */
    @Test
    public void testOptionsChangesDoNotDuplicateSlotsUnderShortJobFirst() throws InterruptedException {
        Config.query_queue_v2_schedule_strategy = QueryQueueOptions.SchedulePolicy.SJF.name();
        SlotSelectionStrategyV2 strategy = new SlotSelectionStrategyV2(slotManager, DEFAULT_WAREHOUSE_ID);
        SlotTracker slotTracker = new SlotTracker(slotManager, ImmutableList.of(strategy));

        LogicalSlot slot = generateSlot(1, GROUP_LIMITED);
        slotTracker.requireSlot(slot);

        for (int concurrencyLevel : new int[] {8, 16}) {
            // The options are refreshed at most once per UPDATE_OPTIONS_INTERVAL_MS.
            Thread.sleep(1100);
            Config.query_queue_v2_concurrency_level = concurrencyLevel;
            strategy.updateOptionsPeriodically();
        }

        assertThat(strategy.peakSlotsToAllocate(slotTracker)).containsExactly(slot);
    }

    /**
     * A slot held back by its group keeps its place when the round ends early because the warehouse ran out of
     * capacity, so it is scheduled before the slots which were still waiting behind it.
     */
    @Test
    public void testBlockedSlotKeepsItsPlaceWhenCapacityEndsTheRound() {
        // 1 worker of 16 cores at concurrency level 2 gives 16 * 2 / 4 = 8 total slots.
        Config.query_queue_v2_concurrency_level = 2;
        SlotSelectionStrategyV2 strategy = new SlotSelectionStrategyV2(slotManager, DEFAULT_WAREHOUSE_ID);
        SlotTracker slotTracker = new SlotTracker(slotManager, ImmutableList.of(strategy));

        // Fill the group up to its limit of 2, which takes 4 of the 8 total slots.
        LogicalSlot running1 = generateSlot(2, GROUP_LIMITED);
        LogicalSlot running2 = generateSlot(2, GROUP_LIMITED);
        slotTracker.requireSlot(running1);
        slotTracker.requireSlot(running2);
        strategy.peakSlotsToAllocate(slotTracker).forEach(slotTracker::allocateSlot);

        LogicalSlot blocked = generateSlot(2, GROUP_LIMITED);
        LogicalSlot pass1 = generateSlot(2, GROUP_UNLIMITED);
        LogicalSlot pass2 = generateSlot(2, GROUP_UNLIMITED);
        LogicalSlot behind = generateSlot(2, GROUP_UNLIMITED);
        slotTracker.requireSlot(blocked);
        slotTracker.requireSlot(pass1);
        slotTracker.requireSlot(pass2);
        slotTracker.requireSlot(behind);

        // `blocked` is held back by its group, `pass1` and `pass2` exhaust the total slots, so `behind` is never
        // reached in this round.
        List<LogicalSlot> peaked = strategy.peakSlotsToAllocate(slotTracker);
        assertThat(peaked).containsExactly(pass1, pass2);
        peaked.forEach(slotTracker::allocateSlot);

        // Free the group and the capacity taken by the two running queries.
        slotTracker.releaseSlot(running1.getSlotId());
        slotTracker.releaseSlot(running2.getSlotId());

        assertThat(strategy.peakSlotsToAllocate(slotTracker)).containsExactly(blocked, behind);
    }

    /**
     * A slot which was peaked but never allocated has taken no seat of its group, so releasing it must not give
     * back a seat which another query is still holding.
     */
    @Test
    public void testReleasingASlotWhichWasNeverAllocatedKeepsTheGroupCount() {
        SlotSelectionStrategyV2 strategy = new SlotSelectionStrategyV2(slotManager, DEFAULT_WAREHOUSE_ID);
        SlotTracker slotTracker = new SlotTracker(slotManager, ImmutableList.of(strategy));

        LogicalSlot running = generateSlot(1, GROUP_LIMITED);
        slotTracker.requireSlot(running);
        assertThat(strategy.peakSlotsToAllocate(slotTracker)).containsExactly(running);
        slotTracker.allocateSlot(running);

        LogicalSlot peakedOnly = generateSlot(1, GROUP_LIMITED);
        slotTracker.requireSlot(peakedOnly);
        assertThat(strategy.peakSlotsToAllocate(slotTracker)).containsExactly(peakedOnly);
        slotTracker.releaseSlot(peakedOnly.getSlotId());

        // `running` still holds one of the two seats of the group.
        LogicalSlot slot3 = generateSlot(1, GROUP_LIMITED);
        LogicalSlot slot4 = generateSlot(1, GROUP_LIMITED);
        slotTracker.requireSlot(slot3);
        slotTracker.requireSlot(slot4);
        assertThat(strategy.peakSlotsToAllocate(slotTracker)).hasSize(1);
    }

    private static LogicalSlot generateSlot(int numSlots, long groupId) {
        return new LogicalSlot(UUIDUtil.genTUniqueId(), "fe", WarehouseManager.DEFAULT_WAREHOUSE_ID,
                groupId, numSlots, 0, 0, 0, 0, 0);
    }
}
