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

/** Keeps an admitted query's slot until both it and its metadata children finish execution. */
public final class SharedQueryQueueSlot {
    private final LogicalSlot slot;
    private final SlotProvider provider;
    private int users = 1;
    private boolean admitted;

    public SharedQueryQueueSlot(LogicalSlot slot, SlotProvider provider) {
        this.slot = slot;
        this.provider = provider;
    }

    public synchronized void markAdmitted() {
        admitted = slot.getState() == LogicalSlot.State.ALLOCATED && !(provider instanceof LocalSlotProvider);
    }

    public synchronized boolean retain() {
        if (users == 0 || !admitted || slot.getExpiredAllocatedTimeMs() <= System.currentTimeMillis()) {
            return false;
        }
        users++;
        return true;
    }

    public LogicalSlot getSlot() {
        return slot;
    }

    public void release() {
        synchronized (this) {
            if (--users != 0) {
                return;
            }
        }
        provider.cancelSlotRequirement(slot);
        provider.releaseSlot(slot);
    }
}
