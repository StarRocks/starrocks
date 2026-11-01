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
import com.starrocks.ha.LeaderInfo;
import com.starrocks.metric.MetricRepo;
import com.starrocks.server.WarehouseManager;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class SlotManagerTest {
    private static final String LEADER_IP = "127.0.0.1";
    private static final String NEW_LEADER_IP = "127.0.0.2";
    private static final int HTTP_PORT = 8030;
    private static final int RPC_PORT = 9020;

    @BeforeAll
    public static void beforeClass() {
        MetricRepo.init();
    }

    /**
     * Only the allocated slots are the concern of the leader FE change, because the pending requirements can still be
     * allocated and notified by the new leader FE.
     */
    @Test
    public void testPeakSlotsOfPreviousLeader() {
        SlotManager slotManager = new SlotManager(new ResourceUsageMonitor());
        SlotTracker slotTracker = new SlotTracker(slotManager, ImmutableList.of());

        LogicalSlot allocatedSlot = generateSlot(3);
        assertThat(slotTracker.requireSlot(allocatedSlot)).isTrue();
        slotTracker.allocateSlot(allocatedSlot);

        LogicalSlot pendingSlot = generateSlot(1);
        assertThat(slotTracker.requireSlot(pendingSlot)).isTrue();

        assertThat(slotTracker.peakSlotsOfPreviousLeader()).containsExactly(allocatedSlot);

        // Peaking does not remove the slots from the tracker.
        assertThat(slotTracker.getNumAllocatedSlots()).isEqualTo(3);
        assertThat(slotTracker.getSlots()).containsExactlyInAnyOrder(allocatedSlot, pendingSlot);

        // The peeked slots are released explicitly.
        assertThat(slotTracker.releaseSlot(allocatedSlot.getSlotId())).isSameAs(allocatedSlot);
        assertThat(slotTracker.getNumAllocatedSlots()).isZero();
        assertThat(slotTracker.peakSlotsOfPreviousLeader()).isEmpty();

        // The pending requirement is kept.
        assertThat(slotTracker.getSlot(pendingSlot.getSlotId())).isSameAs(pendingSlot);
        assertThat(pendingSlot.getState()).isEqualTo(LogicalSlot.State.REQUIRING);
    }

    /**
     * The slots allocated around a leader FE change can never be released by their requesters, which only send the
     * release-slot RPC to the current leader FE. They should be released when the leader FE is changed, otherwise they
     * stay ALLOCATED (RUNNING in `SHOW RUNNING QUERIES` without any alive coordinator) until they expire.
     */
    @Test
    public void testReleaseSlotsOfPreviousLeaderOnLeaderChange() throws Exception {
        SlotManager slotManager = new SlotManager(new ResourceUsageMonitor());
        SlotTracker slotTracker = slotManager.getSlotTracker(WarehouseManager.DEFAULT_WAREHOUSE_ID);

        LogicalSlot slot = generateSlot(4);
        assertThat(slotTracker.requireSlot(slot)).isTrue();
        slotTracker.allocateSlot(slot);
        assertThat(slotTracker.getNumAllocatedSlots()).isEqualTo(4);

        slotManager.start();

        // The first known leader FE after this FE starts up.
        slotManager.onLeaderChange(new LeaderInfo(LEADER_IP, HTTP_PORT, RPC_PORT));
        assertThat(slotTracker.getNumAllocatedSlots()).isEqualTo(4);

        // The leader FE is not changed, so the allocated slots are still running.
        slotManager.onLeaderChange(new LeaderInfo(LEADER_IP, HTTP_PORT, RPC_PORT));
        assertThat(slotTracker.getNumAllocatedSlots()).isEqualTo(4);

        // The leader FE is changed, so the slots allocated by the previous leader FE are released.
        slotManager.onLeaderChange(new LeaderInfo(NEW_LEADER_IP, HTTP_PORT, RPC_PORT));
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> {
            assertThat(slotTracker.getNumAllocatedSlots()).isZero();
            assertThat(slotTracker.getSlot(slot.getSlotId())).isNull();
            return slot.getState() == LogicalSlot.State.RELEASED;
        });
    }

    private static LogicalSlot generateSlot(int numSlots) {
        final long nowMs = System.currentTimeMillis();
        return new LogicalSlot(UUIDUtil.genTUniqueId(), "fe-1", WarehouseManager.DEFAULT_WAREHOUSE_ID,
                LogicalSlot.ABSENT_GROUP_ID, numSlots,
                // The slots are far away from the expiration, so that only the leader FE change can release them.
                nowMs + TimeUnit.MINUTES.toMillis(10), nowMs + TimeUnit.MINUTES.toMillis(60), nowMs,
                1, 1);
    }
}