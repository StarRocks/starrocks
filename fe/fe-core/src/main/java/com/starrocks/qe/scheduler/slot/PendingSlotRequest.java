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

import com.starrocks.qe.scheduler.RecoverableException;
import com.starrocks.thrift.TNetworkAddress;

import java.util.concurrent.CompletableFuture;

public class PendingSlotRequest {
    private final LogicalSlot slot;
    private final TNetworkAddress leaderEndpoint;
    private final CompletableFuture<LogicalSlot> slotFuture = new CompletableFuture<>();

    public PendingSlotRequest(LogicalSlot slot, TNetworkAddress leaderEndpoint) {
        this.slot = slot;
        this.leaderEndpoint = leaderEndpoint;
    }

    LogicalSlot getSlot() {
        return slot;
    }

    public TNetworkAddress getLeaderEndpoint() {
        return leaderEndpoint;
    }

    public CompletableFuture<LogicalSlot> getSlotFuture() {
        return slotFuture;
    }

    public void onRequire() {
        slot.onRequire();
    }

    public void onFailed(Exception cause) {
        slot.onCancel();
        slotFuture.completeExceptionally(cause);
    }

    public void onRetry(RecoverableException cause) {
        slot.onRetry();
        slotFuture.completeExceptionally(cause);
    }

    public void onFinished(int pipelineDop) {
        slot.setPipelineDop(pipelineDop);
        slot.onAllocate();
        slotFuture.complete(slot);
    }

    public void onCancel() {
        slot.onCancel();
        slotFuture.cancel(true);
    }

    @Override
    public String toString() {
        return "PendingSlotRequest{" +
                "slot=" + slot +
                ", leaderEndpoint=" + leaderEndpoint +
                ", slotFuture=" + slotFuture +
                '}';
    }
}
