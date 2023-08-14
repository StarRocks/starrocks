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

import java.util.concurrent.CompletableFuture;

public class PendingSlotRequest {
    private final Slot slot;
    private final CompletableFuture<Slot> slotFuture = new CompletableFuture<>();

    public PendingSlotRequest(Slot slot) {
        this.slot = slot;
    }

    public CompletableFuture<Slot> getSlotFuture() {
        return slotFuture;
    }

    public void onRequire() {
        slot.onRequire();
    }

    public void onFailed(Exception cause) {
        slot.onCancel();
        slotFuture.completeExceptionally(cause);
    }

    public void onFinished() {
        slot.onAllocate();
        slotFuture.complete(slot);
    }

    public void onCancel() {
        slot.onCancel();
        slotFuture.cancel(true);
    }

}
