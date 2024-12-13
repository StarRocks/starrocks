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
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.metric.MetricRepo;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SlotTrackerTest {
    @BeforeClass
    public static void beforeClass() {
        MetricRepo.init();
    }

    @Test
    public void testRequireSlot() {
        SlotTracker slotTracker = new SlotTracker(ImmutableList.of());

        LogicalSlot slot1 = generateSlot(1);
        assertThat(slotTracker.requireSlot(slot1)).isTrue();
        assertThat(slotTracker.getSlots()).hasSize(1);
        // Re-require the same slot has no effect.
        assertThat(slotTracker.requireSlot(slot1)).isTrue();
        assertThat(slotTracker.getSlots()).hasSize(1);
    }

    @Test
    public void tesAllocateSlot() {
        SlotTracker slotTracker = new SlotTracker(ImmutableList.of());

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
        SlotTracker slotTracker = new SlotTracker(ImmutableList.of());

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
        return new LogicalSlot(UUIDUtil.genTUniqueId(), "fe", LogicalSlot.ABSENT_GROUP_ID, numSlots, 0, 0, 0, 0, 0);
    }
}
