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

/**
 *
 */
public class ResourceUsageMonitor {
    private final AtomicBoolean isResourceOverloaded = new AtomicBoolean();
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

    public boolean isResourceOverloaded() {
        return isResourceOverloaded.get();
    }

    private void rejudgeResourceOverloaded() {
        boolean prev;
        boolean now;
        boolean updated = false;
        while (!updated) {
            prev = isResourceOverloaded.get();
            now = judgeResourceOverloaded();
            updated = isResourceOverloaded.compareAndSet(prev, now);
            if (updated && prev && !now) {
                resourceAvailableListeners.values().forEach(Runnable::run);
            }
        }
    }

    private boolean judgeResourceOverloaded() {
        return GlobalStateMgr.getCurrentSystemInfo().backendAndComputeNodeStream()
                .anyMatch(ComputeNode::isResourceOverloaded);
    }

}
