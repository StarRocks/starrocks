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

import com.starrocks.common.Config;
import com.starrocks.common.Status;
import com.starrocks.common.UserException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.rpc.FrontendServiceProxy;
import com.starrocks.thrift.TReleaseSlotRequest;
import com.starrocks.thrift.TReleaseSlotResponse;
import com.starrocks.thrift.TRequireSlotRequest;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TUniqueId;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The slot manager view in the follower FEs. It receives the slot operations from {@link com.starrocks.qe.scheduler.Coordinator}
 * and sends it to {@link SlotManager} via RPC.
 *
 * @see SlotManager
 */
public class SlotProvider {
    private static final Logger LOG = LogManager.getLogger(SlotProvider.class);

    private final ConcurrentMap<TUniqueId, PendingSlotRequest> pendingSlots = new ConcurrentHashMap<>();

    public CompletableFuture<Slot> requireSlot(Slot slot) {
        PendingSlotRequest slotRequest = new PendingSlotRequest(slot);
        slotRequest.onRequire();

        pendingSlots.put(slot.getSlotId(), slotRequest);

        try {
            requireSlotFromSlotManager(slot);
        } catch (Exception e) {
            LOG.warn("[Slot] failed to require slot [slot={}]", slot, e);
            pendingSlots.remove(slot.getSlotId());
            slotRequest.onFailed(e);
        }

        return slotRequest.getSlotFuture();
    }

    public Status finishSlotRequirement(TUniqueId slotId, Status status) {
        PendingSlotRequest slotRequest = pendingSlots.remove(slotId);
        if (slotRequest == null) {
            LOG.warn("[Slot] finishSlotRequirement receives a response with non-exist slotId [slotId={}] [status={}]",
                    DebugUtil.printId(slotId), status);
            return Status.internalError("the slotId does not exist");
        }

        if (status.ok()) {
            slotRequest.onFinished();
        } else {
            LOG.warn("[Slot] finishSlotRequirement receives a failed response [slot={}] [status={}]", slotRequest, status);
            slotRequest.onFailed(new UserException(status.getErrorMsg()));
        }

        return new Status();
    }

    public void cancelSlotRequirement(Slot slot) {
        if (slot == null) {
            return;
        }

        PendingSlotRequest slotRequest = pendingSlots.remove(slot.getSlotId());
        if (slotRequest == null) {
            return;
        }

        slotRequest.onCancel();
        releaseSlotToSlotManager(slot);
    }

    public void releaseSlot(Slot slot) {
        if (slot == null || slot.getState() != Slot.State.ALLOCATED) {
            return;
        }

        slot.onRelease();
        releaseSlotToSlotManager(slot);
    }

    private void requireSlotFromSlotManager(Slot slot) throws Exception {
        TRequireSlotRequest request = new TRequireSlotRequest();
        request.setSlot(slot.toThrift());

        FrontendServiceProxy.call(slot.getLeaderEndpoint(),
                Config.thrift_rpc_timeout_ms,
                Config.thrift_rpc_retry_times,
                client -> client.requireSlotAsync(request));
    }

    private void releaseSlotToSlotManager(Slot slot) {
        TReleaseSlotRequest slotRequest = new TReleaseSlotRequest();
        slotRequest.setSlot_id(slot.getSlotId());

        try {
            TReleaseSlotResponse res = FrontendServiceProxy.call(
                    slot.getLeaderEndpoint(),
                    Config.thrift_rpc_timeout_ms,
                    Config.thrift_rpc_retry_times,
                    client -> client.releaseSlot(slotRequest));
            if (res.getStatus().getStatus_code() != TStatusCode.OK) {
                String errMsg = "";
                if (!CollectionUtils.isEmpty(res.getStatus().getError_msgs())) {
                    errMsg = res.getStatus().getError_msgs().get(0);
                }
                LOG.warn("[Slot] failed to release slot [slot={}] [errMsg={}]", slot, errMsg);
            }
        } catch (Exception e) {
            LOG.warn("[Slot] failed to release slot [slot={}]", slot, e);
        }
    }
}
