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

import com.starrocks.catalog.ResourceGroup;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.thrift.TResourceLogicalSlot;
import com.starrocks.thrift.TUniqueId;

import java.util.Map;

/**
 * A logical slot represents resources which is required by a query from the {@link SlotManager}.
 * <p> It contains multiple physical slots. A physical slot represents a part of resource from the cluster. There are a total of
 * {@link GlobalVariable#getQueryQueueConcurrencyLimit()} slots and {@link ResourceGroup#getConcurrencyLimit()} slots for a group.
 * <p> When the query queue is enabled by {@link GlobalVariable#isEnableQueryQueueSelect()},
 * {@link GlobalVariable#isEnableQueryQueueStatistic()} or {@link GlobalVariable#isEnableQueryQueueLoad()}, a query must be
 * required enough physical slots from the {@link SlotManager}.
 */
public class LogicalSlot {
    public static final long ABSENT_GROUP_ID = -1;

    private final TUniqueId slotId;
    private final String requestFeName;

    private final long groupId;

    private final int numPhysicalSlots;

    private final long expiredPendingTimeMs;

    private final long expiredAllocatedTimeMs;

    private final long feStartTimeMs;

    /**
     * Set when creating this slot. It is just used in {@code show running queries} and not accurate enough.
     */
    private final long startTimeMs;
    private final int numFragments;
    private int pipelineDop;

    private State state = State.CREATED;

    public LogicalSlot(TUniqueId slotId, String requestFeName, long groupId, int numPhysicalSlots,
                       long expiredPendingTimeMs, long expiredAllocatedTimeMs, long feStartTimeMs,
                       int numFragments, int pipelineDop) {
        this.slotId = slotId;
        this.requestFeName = requestFeName;
        this.groupId = groupId;
        this.numPhysicalSlots = numPhysicalSlots;
        this.expiredPendingTimeMs = expiredPendingTimeMs;
        this.expiredAllocatedTimeMs = expiredAllocatedTimeMs;
        this.feStartTimeMs = feStartTimeMs;
        this.startTimeMs = System.currentTimeMillis();
        this.numFragments = numFragments;
        this.pipelineDop = pipelineDop;
    }

    public State getState() {
        return state;
    }

    public void onRequire() {
        transitionState(State.CREATED, State.REQUIRING);
    }

    public void onAllocate() {
        transitionState(State.REQUIRING, State.ALLOCATED);
    }

    public void onCancel() {
        transitionState(state, State.CANCELLED);
    }

    public void onRetry() {
        transitionState(state, State.CREATED);
    }

    public void onRelease() {
        transitionState(State.ALLOCATED, State.RELEASED);
    }

    public TResourceLogicalSlot toThrift() {
        TResourceLogicalSlot tslot = new TResourceLogicalSlot();
        tslot.setSlot_id(slotId)
                .setRequest_fe_name(requestFeName)
                .setGroup_id(groupId)
                .setNum_slots(numPhysicalSlots)
                .setExpired_pending_time_ms(expiredPendingTimeMs)
                .setExpired_allocated_time_ms(expiredAllocatedTimeMs)
                .setFe_start_time_ms(feStartTimeMs)
                .setNum_fragments(numFragments)
                .setPipeline_dop(pipelineDop);

        return tslot;
    }

    public static LogicalSlot fromThrift(TResourceLogicalSlot tslot) {
        return new LogicalSlot(tslot.getSlot_id(), tslot.getRequest_fe_name(), tslot.getGroup_id(), tslot.getNum_slots(),
                tslot.getExpired_pending_time_ms(), tslot.getExpired_allocated_time_ms(), tslot.getFe_start_time_ms(),
                tslot.getNum_fragments(), tslot.getPipeline_dop());
    }

    public TUniqueId getSlotId() {
        return slotId;
    }

    public String getRequestFeName() {
        return requestFeName;
    }

    public long getGroupId() {
        return groupId;
    }

    public int getNumPhysicalSlots() {
        return numPhysicalSlots;
    }

    public long getExpiredPendingTimeMs() {
        return expiredPendingTimeMs;
    }

    public long getExpiredAllocatedTimeMs() {
        return expiredAllocatedTimeMs;
    }

    public boolean isPendingTimeout() {
        return System.currentTimeMillis() >= expiredPendingTimeMs;
    }

    public boolean isAllocatedExpired(long nowMs) {
        return nowMs >= expiredAllocatedTimeMs;
    }

    public long getFeStartTimeMs() {
        return feStartTimeMs;
    }

    public long getStartTimeMs() {
        return startTimeMs;
    }

    public int getNumDrivers() {
        return numFragments * pipelineDop;
    }

    public int getNumFragments() {
        return numFragments;
    }

    public boolean isAdaptiveDop() {
        return pipelineDop == 0;
    }

    public int getPipelineDop() {
        return pipelineDop;
    }

    public void setPipelineDop(int pipelineDop) {
        this.pipelineDop = pipelineDop;
    }

    @Override
    public String toString() {
        return "LogicalSlot{" +
                "slotId=" + DebugUtil.printId(slotId) +
                ", requestFeName='" + requestFeName + '\'' +
                ", groupId=" + groupId +
                ", numPhysicalSlots=" + numPhysicalSlots +
                ", expiredPendingTimeMs=" + TimeUtils.longToTimeString(expiredPendingTimeMs) +
                ", expiredAllocatedTimeMs=" + TimeUtils.longToTimeString(expiredAllocatedTimeMs) +
                ", feStartTimeMs=" + TimeUtils.longToTimeString(feStartTimeMs) +
                ", startTimeMs=" + TimeUtils.longToTimeString(startTimeMs) +
                ", state=" + state +
                '}';
    }

    private void transitionState(State from, State to) {
        if (state == from) {
            state = to;
        }
    }

    public enum State {
        CREATED,
        REQUIRING,
        ALLOCATED,
        RELEASED,
        CANCELLED;

        boolean isTerminal() {
            return this == RELEASED || this == CANCELLED;
        }

        // Put RUNNING State at first
        private static final Map<State, Integer> SORT_ORDER = Map.of(
                ALLOCATED, 1,
                REQUIRING, 2,
                CREATED, 3,
                CANCELLED, 4,
                RELEASED, 5
        );

        public int getSortOrder() {
            return SORT_ORDER.getOrDefault(this, SORT_ORDER.size());
        }

        public String toQueryStateString() {
            switch (this) {
                case CREATED:
                    return "CREATED";
                case REQUIRING:
                    return "PENDING";
                case ALLOCATED:
                    return "RUNNING";
                case RELEASED:
                case CANCELLED:
                    return "FINISHED";
                default:
                    return "UNKNOWN";
            }
        }
    }
}
