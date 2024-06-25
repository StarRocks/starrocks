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

public class DefaultSlotSelectionStrategyTest {
    @BeforeClass
    public static void beforeClass() {
        MetricRepo.init();
    }

    @Test
    public void testReleaseSlot() throws InterruptedException {
        DefaultSlotSelectionStrategy strategy =
                new DefaultSlotSelectionStrategy(() -> false, (groupId) -> false);
        SlotTracker slotTracker = new SlotTracker(ImmutableList.of(strategy));

        LogicalSlot slot1 = generateSlot(1, 0);
        LogicalSlot slot2 = generateSlot(1, 0);

        // 1.1 require slot1.
        assertThat(slotTracker.requireSlot(slot1)).isTrue();
        assertThat(slotTracker.getSlots()).hasSize(1);

        // 1.2 release slot1.
        slotTracker.releaseSlot(slot1.getSlotId());
        assertThat(slotTracker.getNumAllocatedSlots()).isZero();
        assertThat(strategy.getNumAllocatedSlotsOfGroup(0)).isZero();

        // 2.1 require and allocate slot2.
        assertThat(slotTracker.requireSlot(slot2)).isTrue();
        assertThat(slotTracker.getSlots()).hasSize(1);
        slotTracker.allocateSlot(slot2);
        assertThat(slotTracker.getNumAllocatedSlots()).isOne();
        assertThat(strategy.getNumAllocatedSlotsOfGroup(0)).isOne();

        // 2.2 require slot1.
        assertThat(slotTracker.requireSlot(slot1)).isTrue();
        assertThat(slotTracker.getSlots()).hasSize(2);

        // 2.3 release slot1.
        slotTracker.releaseSlot(slot1.getSlotId());
        assertThat(slotTracker.getNumAllocatedSlots()).isOne();
        assertThat(strategy.getNumAllocatedSlotsOfGroup(0)).isOne();

        // 2.4 release slot2
        Thread.sleep(1200); // give enough interval time to trigger sweepEmptyGroups.
        assertThat(slotTracker.releaseSlot(slot2.getSlotId())).isSameAs(slot2);
        assertThat(strategy.getNumAllocatedSlotsOfGroup(0)).isZero();
    }

    private static LogicalSlot generateSlot(int numSlots, long groupId) {
        return new LogicalSlot(UUIDUtil.genTUniqueId(), "fe", groupId, numSlots, 0, 0, 0, 0, 0);
    }
}
