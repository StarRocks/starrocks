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

package com.starrocks.qe.scheduler;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.starrocks.qe.SessionVariableConstants.ComputationFragmentSchedulingPolicy;
import com.starrocks.qe.SimpleScheduler;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Supplier;

public class SkipBlacklistWorkerProviderTest {
    
    private static <C extends ComputeNode> ImmutableMap<Long, C> genWorkers(long startId, long endId,
                                                                            Supplier<C> factory, boolean halfDead) {
        Map<Long, C> res = new TreeMap<>();
        for (long i = startId; i < endId; i++) {
            C worker = factory.get();
            worker.setId(i);
            if (halfDead && i % 2 == 0) {
                worker.setAlive(false);
            } else {
                worker.setAlive(true);
            }
            worker.setHost("host#" + i);
            worker.setBePort(80);
            res.put(i, worker);
        }
        return ImmutableMap.copyOf(res);
    }

    @Test
    public void testSkipBlacklistBehavior() {
        ImmutableMap<Long, ComputeNode> id2Backend = genWorkers(0, 10, Backend::new, false);
        ImmutableMap<Long, ComputeNode> id2ComputeNode = genWorkers(10, 15, ComputeNode::new, false);

        long inBlacklistBEId = 3L;
        long inBlacklistCNId = 13L;
        Set<Long> blacklistWorkerIds = ImmutableSet.of(inBlacklistBEId, inBlacklistCNId);

        // Mock SimpleScheduler to mark some nodes as in blacklist
        new MockUp<SimpleScheduler>() {
            @Mock
            public boolean isInBlocklist(long backendId) {
                return blacklistWorkerIds.contains(backendId);
            }
        };

        new MockUp<SystemInfoService>() {
            @Mock
            public ImmutableMap<Long, ComputeNode> getIdToBackend() {
                return id2Backend;
            }

            @Mock
            public ImmutableMap<Long, ComputeNode> getIdComputeNode() {
                return id2ComputeNode;
            }
        };

        // Test SkipBlacklistWorkerProvider - should include blacklisted nodes
        SkipBlacklistWorkerProvider.Factory skipFactory = new SkipBlacklistWorkerProvider.Factory();
        SkipBlacklistWorkerProvider skipProvider =
                skipFactory.captureAvailableWorkers(
                        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo(),
                        false, -1, ComputationFragmentSchedulingPolicy.ALL_NODES,
                        WarehouseManager.DEFAULT_RESOURCE);

        // Nodes in blacklist SHOULD be available when using SkipBlacklistWorkerProvider
        Assertions.assertTrue(skipProvider.isDataNodeAvailable(inBlacklistBEId),
                "Backend in blacklist should be available when skip_black_list is enabled");
        Assertions.assertNotNull(skipProvider.getWorkerById(inBlacklistCNId),
                "ComputeNode in blacklist should be available when skip_black_list is enabled");

        // Test DefaultWorkerProvider - should exclude blacklisted nodes
        DefaultWorkerProvider.Factory defaultFactory = new DefaultWorkerProvider.Factory();
        DefaultWorkerProvider defaultProvider =
                defaultFactory.captureAvailableWorkers(
                        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo(),
                        false, -1, ComputationFragmentSchedulingPolicy.ALL_NODES,
                        WarehouseManager.DEFAULT_RESOURCE);

        // Nodes in blacklist should NOT be available with DefaultWorkerProvider
        Assertions.assertFalse(defaultProvider.isDataNodeAvailable(inBlacklistBEId),
                "Backend in blacklist should NOT be available with default provider");
        Assertions.assertNull(defaultProvider.getWorkerById(inBlacklistCNId),
                "ComputeNode in blacklist should NOT be available with default provider");
    }
}
