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
import com.google.common.util.concurrent.Uninterruptibles;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.metric.MetricRepo;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.server.WarehouseManager;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class SlotTrackerTest {
    private static SlotManager slotManager;

    @BeforeAll
    public static void beforeClass() {
        MetricRepo.init();
        ResourceUsageMonitor resourceUsageMonitor = new ResourceUsageMonitor();
        slotManager = new SlotManager(resourceUsageMonitor);
    }

    @Test
    public void testRequireSlot() {
        SlotTracker slotTracker = new SlotTracker(slotManager, ImmutableList.of());

        LogicalSlot slot1 = generateSlot(1);
        assertThat(slotTracker.requireSlot(slot1)).isTrue();
        assertThat(slotTracker.getSlots()).hasSize(1);
        // Re-require the same slot has no effect.
        assertThat(slotTracker.requireSlot(slot1)).isTrue();
        assertThat(slotTracker.getSlots()).hasSize(1);
    }

    @Test
    public void tesAllocateSlot() {
        SlotTracker slotTracker = new SlotTracker(slotManager, ImmutableList.of());

        LogicalSlot slot1 = generateSlot(1);

        // Allocation of a slot that has not been required has no effect.
        slotTracker.allocateSlot(slot1);
        assertThat(slotTracker.getSlots()).isEmpty();

        // Allocation of a required slot has effect.
        assertThat(slotTracker.requireSlot(slot1)).isTrue();
        slotTracker.allocateSlot(slot1);
        assertThat(slotTracker.getNumAllocatedSlots()).isOne();

        // Re-allocation of the same slot has no effect.
        slotTracker.allocateSlot(slot1);
        assertThat(slotTracker.getNumAllocatedSlots()).isOne();
    }

    @Test
    public void tesReleaseSlot() {
        SlotTracker slotTracker = new SlotTracker(slotManager, ImmutableList.of());

        LogicalSlot slot1 = generateSlot(1);

        // 1. Release of a slot that has not been required has no effect.
        assertThat(slotTracker.releaseSlot(slot1.getSlotId())).isNull();

        // 2.1 Release a required slot.
        assertThat(slotTracker.requireSlot(slot1)).isTrue();
        assertThat(slotTracker.releaseSlot(slot1.getSlotId())).isSameAs(slot1);
        assertThat(slotTracker.getNumAllocatedSlots()).isZero();
        assertThat(slotTracker.getSlots()).isEmpty();

        // 2.2 Re-release of the same slot has no effect.
        slotTracker.allocateSlot(slot1);
        assertThat(slotTracker.releaseSlot(slot1.getSlotId())).isNull();

        // 3.1 Release a required slot.
        assertThat(slotTracker.requireSlot(slot1)).isTrue();

        slotTracker.allocateSlot(slot1);
        assertThat(slotTracker.getNumAllocatedSlots()).isOne();

        assertThat(slotTracker.releaseSlot(slot1.getSlotId())).isSameAs(slot1);
        assertThat(slotTracker.getNumAllocatedSlots()).isZero();
        assertThat(slotTracker.getSlots()).isEmpty();

        // 3.2 Re-release of the same slot has no effect.
        slotTracker.allocateSlot(slot1);
        assertThat(slotTracker.releaseSlot(slot1.getSlotId())).isNull();
    }

    private static LogicalSlot generateSlot(int numSlots) {
        return new LogicalSlot(UUIDUtil.genTUniqueId(), "fe", WarehouseManager.DEFAULT_WAREHOUSE_ID,
                LogicalSlot.ABSENT_GROUP_ID, numSlots, 0, 0, 0,
                0, 0);
    }

    private static LogicalSlot generateSlot(int numSlots, long expiredPendingTimeMs, long expiredAllocatedTimeMs) {
        return new LogicalSlot(UUIDUtil.genTUniqueId(), "fe", WarehouseManager.DEFAULT_WAREHOUSE_ID,
                LogicalSlot.ABSENT_GROUP_ID, numSlots, expiredPendingTimeMs, expiredAllocatedTimeMs, 0,
                0, 0);
    }

    @Test
    public void testSlotTrackerMetrics() {
        SlotTracker slotTracker = new SlotTracker(slotManager, ImmutableList.of());
        assertThat(slotTracker.getWarehouseId()).isEqualTo(WarehouseManager.DEFAULT_WAREHOUSE_ID);
        assertThat(slotTracker.getWarehouseName().equals(""));
        assertThat(slotTracker.getQueuePendingLength()).isEqualTo(0);
        assertThat(slotTracker.getNumAllocatedSlots()).isEqualTo(0);
        assertThat(slotTracker.getMaxRequiredSlots()).isEmpty();
        assertThat(slotTracker.getSumRequiredSlots()).isEmpty();
        assertThat(slotTracker.getMaxSlots()).isEmpty();
        assertThat(slotTracker.getRemainSlots()).isEqualTo(slotTracker.getMaxSlots());
        assertThat(slotTracker.getMaxQueueQueueLength()).isEqualTo(GlobalVariable.getQueryQueueMaxQueuedQueries());
        assertThat(slotTracker.getMaxQueuePendingTimeSecond()).isEqualTo(GlobalVariable.getQueryQueuePendingTimeoutSecond());

        LogicalSlot slot1 = generateSlot(1);
        assertThat(slotTracker.requireSlot(slot1)).isTrue();
        assertThat(slotTracker.getQueuePendingLength()).isEqualTo(1);
        assertThat(slotTracker.getNumAllocatedSlots()).isEqualTo(0);
        assertThat(slotTracker.getMaxRequiredSlots()).isEqualTo(Optional.of(1));
        assertThat(slotTracker.getSumRequiredSlots()).isEqualTo(Optional.of(1));
        assertThat(slotTracker.getMaxSlots()).isEmpty();
        assertThat(slotTracker.getRemainSlots()).isEmpty();
        assertThat(slotTracker.getMaxQueueQueueLength()).isEqualTo(GlobalVariable.getQueryQueueMaxQueuedQueries());
        assertThat(slotTracker.getMaxQueuePendingTimeSecond()).isEqualTo(GlobalVariable.getQueryQueuePendingTimeoutSecond());

        // re-release of the same slot has no effect.
        assertThat(slotTracker.requireSlot(slot1)).isTrue();
        assertThat(slotTracker.getQueuePendingLength()).isEqualTo(1);
        assertThat(slotTracker.getNumAllocatedSlots()).isEqualTo(0);
        assertThat(slotTracker.getMaxRequiredSlots()).isEqualTo(Optional.of(1));
        assertThat(slotTracker.getSumRequiredSlots()).isEqualTo(Optional.of(1));
        assertThat(slotTracker.getMaxSlots()).isEmpty();
        assertThat(slotTracker.getRemainSlots()).isEmpty();
        assertThat(slotTracker.getMaxQueueQueueLength()).isEqualTo(GlobalVariable.getQueryQueueMaxQueuedQueries());
        assertThat(slotTracker.getMaxQueuePendingTimeSecond()).isEqualTo(GlobalVariable.getQueryQueuePendingTimeoutSecond());

        // allocate slot
        slotTracker.allocateSlot(slot1);
        assertThat(slotTracker.requireSlot(slot1)).isTrue();
        assertThat(slotTracker.getQueuePendingLength()).isEqualTo(0);
        assertThat(slotTracker.getNumAllocatedSlots()).isEqualTo(1);
        assertThat(slotTracker.getMaxRequiredSlots()).isEmpty();
        assertThat(slotTracker.getSumRequiredSlots()).isEmpty();
        assertThat(slotTracker.getMaxSlots()).isEmpty();
        assertThat(slotTracker.getRemainSlots()).isEmpty();
        assertThat(slotTracker.getMaxQueueQueueLength()).isEqualTo(GlobalVariable.getQueryQueueMaxQueuedQueries());
        assertThat(slotTracker.getMaxQueuePendingTimeSecond()).isEqualTo(GlobalVariable.getQueryQueuePendingTimeoutSecond());

        // release slot tracker
        assertThat(slotTracker.releaseSlot(slot1.getSlotId())).isSameAs(slot1);
        assertThat(slotTracker.getQueuePendingLength()).isEqualTo(0);
        assertThat(slotTracker.getNumAllocatedSlots()).isEqualTo(0);
        assertThat(slotTracker.getMaxRequiredSlots()).isEmpty();
        assertThat(slotTracker.getSumRequiredSlots()).isEmpty();
        assertThat(slotTracker.getMaxSlots()).isEmpty();
        assertThat(slotTracker.getRemainSlots()).isEqualTo(slotTracker.getMaxSlots());
        assertThat(slotTracker.getMaxQueueQueueLength()).isEqualTo(GlobalVariable.getQueryQueueMaxQueuedQueries());
        assertThat(slotTracker.getMaxQueuePendingTimeSecond()).isEqualTo(GlobalVariable.getQueryQueuePendingTimeoutSecond());
    }

    @Test
    public void testExtraMessage() {
        QueryQueueOptions.V2 v2 = new QueryQueueOptions.V2(
                GlobalVariable.getQueryQueueMaxQueuedQueries(),
                GlobalVariable.getQueryQueuePendingTimeoutSecond(),
                1, 1, 1, 1
        );
        BaseSlotTracker.ExtraMessage extraMessage = new BaseSlotTracker.ExtraMessage(1, v2);
        assertThat(extraMessage.getConcurrency()).isEqualTo(1);
        assertThat(extraMessage.getV2()).isEqualTo(v2);
        String json = GsonUtils.GSON.toJson(extraMessage);
        assertThat(json).isEqualTo("{\"Concurrency\":1,\"QueryQueueOption\":"
                + GsonUtils.GSON.toJson(v2) + "}");
    }

    @Test
    public void testGetEarliestQueryWaitTimeSecond() {
        SlotTracker slotTracker = new SlotTracker(slotManager, ImmutableList.of());
        LogicalSlot slot1 = generateSlot(1);
        slotTracker.requireSlot(slot1);
        Uninterruptibles.sleepUninterruptibly(1000, java.util.concurrent.TimeUnit.MILLISECONDS);

        // wait time is greater than 1 second before slot allocation
        double waitTime = slotTracker.getEarliestQueryWaitTimeSecond();
        assertThat(waitTime).isGreaterThanOrEqualTo(1.0);

        // wait time is zero after slot allocation
        slotTracker.allocateSlot(slot1);
        waitTime = slotTracker.getEarliestQueryWaitTimeSecond();
        assertThat(waitTime).isEqualTo(0.0);
    }

    @Test
    public void testPeakExpiredSlotsWithDivergentDeadlines() {
        SlotTracker slotTracker = new SlotTracker(slotManager, ImmutableList.of());

        long nowMs = System.currentTimeMillis();
        // Pending deadline long past but allocated deadline far in the future, like a DML whose
        // query timeout is much longer than its pending timeout. Its pending deadline is the
        // earliest of the two slots, so ordering by that field puts it first and its unexpired
        // allocated deadline ends the scan before the expired slot is reached.
        LogicalSlot longDeadlineSlot = generateSlot(1, nowMs - 3_600_000, nowMs + 3_600_000);
        // Both deadlines already past.
        LogicalSlot expiredSlot = generateSlot(1, nowMs - 700_000, nowMs - 400_000);

        assertThat(slotTracker.requireSlot(longDeadlineSlot)).isTrue();
        assertThat(slotTracker.requireSlot(expiredSlot)).isTrue();
        slotTracker.allocateSlot(longDeadlineSlot);
        slotTracker.allocateSlot(expiredSlot);

        // The expired slot must be reclaimable even though the long-deadline slot
        // precedes it in pending-deadline order.
        assertThat(slotTracker.peakExpiredSlots()).containsExactly(expiredSlot);

        assertThat(slotTracker.releaseSlot(expiredSlot.getSlotId())).isSameAs(expiredSlot);
        assertThat(slotTracker.peakExpiredSlots()).isEmpty();
        assertThat(slotTracker.getMinExpiredTimeMs()).isEqualTo(longDeadlineSlot.getExpiredAllocatedTimeMs());
    }

    @Test
    public void testGetMinExpiredTimeMs() {
        SlotTracker slotTracker = new SlotTracker(slotManager, ImmutableList.of());
        assertThat(slotTracker.getMinExpiredTimeMs()).isZero();

        long nowMs = System.currentTimeMillis();
        LogicalSlot longDeadlineSlot = generateSlot(1, nowMs - 3_600_000, nowMs + 3_600_000);
        LogicalSlot expiredSlot = generateSlot(1, nowMs - 700_000, nowMs - 400_000);
        assertThat(slotTracker.requireSlot(longDeadlineSlot)).isTrue();
        assertThat(slotTracker.requireSlot(expiredSlot)).isTrue();

        // The wake-up deadline must be the earliest allocated deadline, not the earliest
        // pending deadline, so the worker never wakes with nothing to reclaim.
        assertThat(slotTracker.getMinExpiredTimeMs()).isEqualTo(expiredSlot.getExpiredAllocatedTimeMs());
    }

    @Test
    public void testMinExpiredTimeMsAfterPendingDeadlinePassed() {
        SlotTracker slotTracker = new SlotTracker(slotManager, ImmutableList.of());

        long nowMs = System.currentTimeMillis();
        LogicalSlot slot = generateSlot(1, nowMs - 60_000, nowMs + 3_600_000);
        assertThat(slotTracker.requireSlot(slot)).isTrue();
        slotTracker.allocateSlot(slot);

        // Nothing is reclaimable, so the worker's only blocking deadline must be in
        // the future; a past value here makes RequestWorker spin without waiting.
        assertThat(slotTracker.peakExpiredSlots()).isEmpty();
        assertThat(slotTracker.getMinExpiredTimeMs()).isGreaterThan(nowMs);
    }
}
