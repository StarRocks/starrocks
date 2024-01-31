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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.system.BackendCoreStat;
import com.starrocks.thrift.TUniqueId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Calculate proper pipeline DOP for adaptive DOP query according to the fragments and DOP of running queries.
 *
 * <p> It is effective only when {@link GlobalVariable#isQueryQueueDriverHighWaterEffective()} returns true
 * and pipeline_dop is 0 for query.
 */
public class PipelineDriverAllocator {
    private static final Logger LOG = LogManager.getLogger(PipelineDriverAllocator.class);

    private final Set<TUniqueId> allocatedSlotIds = Sets.newConcurrentHashSet();
    private final AtomicInteger numAllocatedDrivers = new AtomicInteger();

    private final Queue<AllocationRequest> allocationRequests = Queues.newConcurrentLinkedQueue();

    @VisibleForTesting
    static final int NUM_BATCH_SLOTS = 1024;

    private static class AllocationRequest {
        private final LogicalSlot slot;
        private final CompletableFuture<Void> doneFuture = new CompletableFuture<>();

        public AllocationRequest(LogicalSlot slot) {
            this.slot = slot;
        }

        public void waitFinish() {
            try {
                doneFuture.get();
            } catch (InterruptedException | ExecutionException e) {
                LOG.warn("[Slot] wait allocation finish failed [slot={}]", slot, e);
                Thread.currentThread().interrupt();
            }
        }

        public void finish() {
            doneFuture.complete(null);
        }
    }

    public void allocate(LogicalSlot slot) {
        if (!GlobalVariable.isQueryQueueDriverHighWaterEffective()) {
            return;
        }

        boolean hasAllocated = !allocatedSlotIds.add(slot.getSlotId());
        if (hasAllocated) {
            return;
        }

        if (!slot.isAdaptiveDop()) {
            numAllocatedDrivers.getAndAdd(slot.getNumDrivers());
            return;
        }

        // If there are too many running drivers, set DOP to 1.
        if (numAllocatedDrivers.get() + slot.getNumFragments() >= GlobalVariable.getQueryQueueDriverHighWater()) {
            slot.setPipelineDop(1);
            numAllocatedDrivers.getAndAdd(slot.getNumDrivers());
            return;
        }

        AllocationRequest allocationRequest = new AllocationRequest(slot);
        allocationRequests.add(allocationRequest);
        allocateInBatch();
        allocationRequest.waitFinish();
    }

    public void release(LogicalSlot slot) {
        boolean hasAllocated = slot != null && allocatedSlotIds.remove(slot.getSlotId());
        if (hasAllocated) {
            numAllocatedDrivers.getAndAdd(-slot.getNumDrivers());
        }
    }

    @VisibleForTesting
    int getNumAllocatedDrivers() {
        return numAllocatedDrivers.get();
    }

    private void allocateInBatch() {
        List<AllocationRequest> requests = new ArrayList<>();
        // Use synchronized to guarantee that only one requester is handling all current allocation requests at the same time.
        synchronized (allocationRequests) {
            while (!allocationRequests.isEmpty() && requests.size() < NUM_BATCH_SLOTS) {
                requests.add(allocationRequests.poll());
            }
        }

        if (requests.isEmpty()) {
            return;
        }

        int numFragments = requests.stream().mapToInt(req -> req.slot.getNumFragments()).sum();
        int dop = calculateDop(numAllocatedDrivers.get(), numFragments, requests.size());

        for (AllocationRequest req : requests) {
            req.slot.setPipelineDop(dop);
            numAllocatedDrivers.getAndAdd(req.slot.getNumDrivers());
            req.finish();
        }
    }

    private int calculateDop(int curNumAllocatedDrivers, int numFragments, int numSlots) {
        if (numFragments <= 0) {
            return 1;
        }

        // Calculate DOP by driverHighWater.
        final int hardLimit = GlobalVariable.getQueryQueueDriverHighWater();
        int dop = calculateDopByLimit(curNumAllocatedDrivers, numFragments, hardLimit);

        if (dop <= 1) {
            return dop;
        }

        // If there is only one running query, do not use driverLowWater to punish DOP.
        if (curNumAllocatedDrivers <= 0 && numSlots == 1) {
            return dop;
        }

        if (!GlobalVariable.isQueryQueueDriverLowWaterEffective()) {
            return dop;
        }

        // Punish DOP by driverLowWater.
        final int softLimit = GlobalVariable.getQueryQueueDriverLowWater();
        int exceedSoftLimit = curNumAllocatedDrivers + numFragments * dop - softLimit;
        if (exceedSoftLimit > 0) {
            int delta = hardLimit - softLimit;
            dop = dop * (delta - exceedSoftLimit) / delta;

            int minDop = calculateDopByLimit(curNumAllocatedDrivers, numFragments, softLimit);
            dop = Math.max(dop, minDop);
        }

        return dop;
    }

    private int calculateDopByLimit(int curNumAllocatedDrivers, int numFragments, int limit) {
        if (curNumAllocatedDrivers + numFragments >= limit) {
            return 1;
        }

        int dop = (limit - curNumAllocatedDrivers) / numFragments;
        dop = Math.min(dop, BackendCoreStat.getDefaultDOP());
        dop = Math.max(1, dop);

        return dop;
    }

}
