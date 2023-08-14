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

import com.starrocks.common.util.DebugUtil;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TResourceSlot;
import com.starrocks.thrift.TUniqueId;

public class Slot {
    public static final long ABSENT_GROUP_ID = -1;

    private final TUniqueId slotId;
    private final TNetworkAddress requestEndpoint;

    private final long groupId;

    private final int numSlots;

    private final long expiredPendingTimeMs;

    private final long expiredAllocatedTimeMs;

    private State state = State.CREATED;

    public Slot(TUniqueId slotId, TNetworkAddress requestEndpoint, long groupId, int numSlots,
                long expiredPendingTimeMs, long expiredAllocatedTimeMs) {
        this.slotId = slotId;
        this.requestEndpoint = requestEndpoint;
        this.groupId = groupId;
        this.numSlots = numSlots;
        this.expiredPendingTimeMs = expiredPendingTimeMs;
        this.expiredAllocatedTimeMs = expiredAllocatedTimeMs;
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

    public TResourceSlot toThrift() {
        TResourceSlot tslot = new TResourceSlot();
        tslot.setSlot_id(slotId)
                .setRequest_endpoint(requestEndpoint)
                .setGroup_id(groupId)
                .setNum_slots(numSlots)
                .setExpired_pending_time_ms(expiredPendingTimeMs)
                .setExpired_allocated_time_ms(expiredAllocatedTimeMs);

        return tslot;
    }

    public static Slot fromThrift(TResourceSlot tslot) {
        return new Slot(tslot.getSlot_id(), tslot.getRequest_endpoint(), tslot.getGroup_id(), tslot.getNum_slots(),
                tslot.getExpired_pending_time_ms(), tslot.getExpired_allocated_time_ms());
    }

    public TUniqueId getSlotId() {
        return slotId;
    }

    public TNetworkAddress getRequestEndpoint() {
        return requestEndpoint;
    }

    public long getGroupId() {
        return groupId;
    }

    public int getNumSlots() {
        return numSlots;
    }

    public long getExpiredPendingTimeMs() {
        return expiredPendingTimeMs;
    }

    public long getExpiredAllocatedTimeMs() {
        return expiredAllocatedTimeMs;
    }

    public boolean isPendingExpired(long nowMs) {
        return nowMs >= expiredPendingTimeMs;
    }

    public boolean isAllocatedExpired(long nowMs) {
        return nowMs >= expiredAllocatedTimeMs;
    }

    @Override
    public String toString() {
        return "Slot{" +
                "slotId=" + DebugUtil.printId(slotId) +
                ", requestEndpoint=" + requestEndpoint +
                ", groupId=" + groupId +
                ", numSlots=" + numSlots +
                ", expiredPendingTimeMs=" + expiredPendingTimeMs +
                ", expiredAllocatedTimeMs=" + expiredAllocatedTimeMs +
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
    }
}
