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

import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.ComputeNode;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

/**
 * It monitors the resource usages from backends and invokes listeners when the global resource or any group resource becomes
 * available from non-available.
 */
public class ResourceUsageMonitor {
    private final AtomicBoolean isGlobalResourceOverloaded = new AtomicBoolean();
    private final ConcurrentHashMap<Long, AtomicBoolean> isGroupResourceOverloaded = new ConcurrentHashMap<>();

    private final AtomicInteger numListeners = new AtomicInteger();
    private final Map<Integer, Runnable> resourceAvailableListeners = new ConcurrentHashMap<>();

    public void registerResourceAvailableListener(Runnable listener) {
        Integer id = numListeners.getAndIncrement();
        resourceAvailableListeners.put(id, listener);
    }

    public void notifyResourceUsageUpdate() {
        rejudgeResourceOverloaded();
    }

    public void notifyBackendDead() {
        rejudgeResourceOverloaded();
    }

    public boolean isGlobalResourceOverloaded() {
        return isGlobalResourceOverloaded.get();
    }

    public boolean isGroupResourceOverloaded(long groupId) {
        AtomicBoolean value = isGroupResourceOverloaded.get(groupId);
        return value != null && value.get();
    }

    private void rejudgeResourceOverloaded() {
        // Use | not || to make sure all the methods invoked.
        if (rejudgeGlobalResourceOverloaded() | rejudgeGroupResourceOverloaded()) { // NOSONAR
            resourceAvailableListeners.values().forEach(Runnable::run);
        }
    }

    /**
     * Rejudge whether the global resource is overloaded.
     *
     * @return whether the global resource becomes available from non-available.
     */
    private boolean rejudgeGlobalResourceOverloaded() {
        return doRejudgeResourceOverloaded(isGlobalResourceOverloaded, this::judgeResourceOverloaded);
    }

    /**
     * Rejudge whether each group resource is overloaded.
     *
     * @return whether any group resource becomes available from non-available.
     */
    private boolean rejudgeGroupResourceOverloaded() {
        boolean needNotify = false;
        for (Long groupId : GlobalStateMgr.getCurrentState().getResourceGroupMgr().getResourceGroupIds()) {
            AtomicBoolean value = isGroupResourceOverloaded.computeIfAbsent(groupId, k -> new AtomicBoolean());
            // Use | not || to make sure all the methods invoked.
            needNotify |= doRejudgeResourceOverloaded(value, () -> judgeGroupResourceOverloaded(groupId));
        }
        return needNotify;
    }

    /**
     * Rejudge whether a resource is overloaded.
     *
     * @return whether the resource becomes available from non-available.
     */
    private boolean doRejudgeResourceOverloaded(AtomicBoolean value, BooleanSupplier newValueSupplier) {
        boolean prev;
        boolean now;
        boolean updated = false;
        while (!updated) {
            prev = value.get();
            now = newValueSupplier.getAsBoolean();
            updated = value.compareAndSet(prev, now);
            if (updated && prev && !now) {
                return true;
            }
        }

        return false;
    }

    /**
     * Judge whether the global resource is overloaded.
     *
     * @return whether the resource is overloaded.
     */
    private boolean judgeResourceOverloaded() {
        return GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().backendAndComputeNodeStream()
                .anyMatch(ComputeNode::isResourceOverloaded);
    }

    /**
     * Judge whether the group resource is overloaded.
     *
     * @return whether the group resource is overloaded.
     */
    private boolean judgeGroupResourceOverloaded(long groupId) {
        return GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().backendAndComputeNodeStream()
                .anyMatch(worker -> worker.isResourceGroupOverloaded(groupId));
    }

}
