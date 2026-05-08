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

package com.starrocks.lake.qe.scheduler;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.starrocks.qe.SessionVariableConstants.BlacklistBackupRoutingPolicy;
import com.starrocks.qe.SessionVariableConstants.ComputationFragmentSchedulingPolicy;
import com.starrocks.qe.SimpleScheduler;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Supplier;

public class SkipBlacklistSharedDataWorkerProviderTest {

    /**
     * Asserts that {@code selected} is a valid backup for {@code workerId}: either {@code -1} when there is no
     * eligible buddy, or a node id in {@code expectedEligible} and not equal to {@code workerId}.
     */
    private static void assertSelectBackupWorkerRespectsEligible(long workerId, long selected,
            Set<Long> expectedEligible) {
        if (expectedEligible.isEmpty()) {
            Assertions.assertEquals(-1L, selected, "no eligible buddies => -1");
        } else {
            Assertions.assertTrue(expectedEligible.contains(selected),
                    () -> "backup must be in eligible set " + expectedEligible + " but was " + selected);
            Assertions.assertNotEquals(workerId, selected);
        }
    }

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

    /**
     * {@link com.starrocks.qe.SessionVariableConstants.BlacklistBackupRoutingPolicy#CIRCULAR}:
     * {@link SkipBlacklistSharedDataWorkerProvider} may use a blocklisted buddy; default provider skips it and
     * takes the next id on the sorted ring.
     */
    @Test
    public void testSelectBackupWorkerSkipsBlacklistCircularPolicy() {
        ImmutableMap<Long, ComputeNode> id2ComputeNode = genWorkers(10, 15, ComputeNode::new, false);

        long inBlacklistCNId = 11L;
        Set<Long> blacklistWorkerIds = ImmutableSet.of(inBlacklistCNId);

        // Mock SimpleScheduler to mark node 11 as in blacklist
        new MockUp<SimpleScheduler>() {
            @Mock
            public boolean isInBlocklist(long backendId) {
                return blacklistWorkerIds.contains(backendId);
            }
        };

        new MockUp<WarehouseManager>() {
            @Mock
            public List<Long> getAllComputeNodeIds(ComputeResource computeResource) {
                return Arrays.asList(10L, 11L, 12L, 13L, 14L);
            }
        };
        new MockUp<SystemInfoService>() {
            @Mock
            public ComputeNode getBackendOrComputeNode(long nodeId) {
                return id2ComputeNode.get(nodeId);
            }
        };

        BlacklistBackupRoutingPolicy backupRoutingPolicy = BlacklistBackupRoutingPolicy.CIRCULAR;

        // Test SkipBlacklistSharedDataWorkerProvider - should select blacklisted node as backup
        SkipBlacklistSharedDataWorkerProvider.Factory skipFactory =
                new SkipBlacklistSharedDataWorkerProvider.Factory(backupRoutingPolicy);
        SkipBlacklistSharedDataWorkerProvider skipProvider =
                skipFactory.captureAvailableWorkers(
                        null, // Not used in our mock
                        false, -1, ComputationFragmentSchedulingPolicy.ALL_NODES,
                        WarehouseManager.DEFAULT_RESOURCE);

        // Select node 10, then try to get backup worker
        // The backup should be node 11 (which is in blacklist) because skip_black_list is enabled
        long backupWorker = skipProvider.selectBackupWorker(10L);
        Assertions.assertEquals(11L, backupWorker,
                "Backup worker should be node 11 (in blacklist) when skip_black_list is enabled");

        // Test DefaultSharedDataWorkerProvider - should NOT select blacklisted node as backup
        DefaultSharedDataWorkerProvider.Factory defaultFactory =
                new DefaultSharedDataWorkerProvider.Factory(backupRoutingPolicy);
        DefaultSharedDataWorkerProvider defaultProvider =
                defaultFactory.captureAvailableWorkers(
                        null, // Not used in our mock
                        false, -1, ComputationFragmentSchedulingPolicy.ALL_NODES,
                        WarehouseManager.DEFAULT_RESOURCE);

        // Select node 10 again, then try to get backup worker
        // The backup should be node 12 (skip node 11 which is in blacklist)
        long defaultBackupWorker = defaultProvider.selectBackupWorker(10L);
        Assertions.assertEquals(12L, defaultBackupWorker,
                "Backup worker should be node 12 (skipping blacklisted node 11) with default provider");
    }

    /**
     * {@link com.starrocks.qe.SessionVariableConstants.BlacklistBackupRoutingPolicy#RANDOM}:
     * same eligibles as {@link #testSelectBackupWorkerSkipsBlacklistCircularPolicy}, but each call draws uniformly from the
     * eligible set (not necessarily the same id as the circular next-node choice).
     */
    @Test
    public void testSelectBackupWorkerSkipsBlacklistRandomPolicy() {
        ImmutableMap<Long, ComputeNode> id2ComputeNode = genWorkers(10, 15, ComputeNode::new, false);

        long inBlacklistCNId = 11L;
        Set<Long> blacklistWorkerIds = ImmutableSet.of(inBlacklistCNId);

        new MockUp<SimpleScheduler>() {
            @Mock
            public boolean isInBlocklist(long backendId) {
                return blacklistWorkerIds.contains(backendId);
            }
        };

        new MockUp<WarehouseManager>() {
            @Mock
            public List<Long> getAllComputeNodeIds(ComputeResource computeResource) {
                return Arrays.asList(10L, 11L, 12L, 13L, 14L);
            }
        };
        new MockUp<SystemInfoService>() {
            @Mock
            public ComputeNode getBackendOrComputeNode(long nodeId) {
                return id2ComputeNode.get(nodeId);
            }
        };

        BlacklistBackupRoutingPolicy backupRoutingPolicy = BlacklistBackupRoutingPolicy.RANDOM;

        // Test SkipBlacklistSharedDataWorkerProvider - should include blacklisted node as backup
        SkipBlacklistSharedDataWorkerProvider.Factory skipFactory =
                new SkipBlacklistSharedDataWorkerProvider.Factory(backupRoutingPolicy);
        SkipBlacklistSharedDataWorkerProvider skipProvider =
                skipFactory.captureAvailableWorkers(
                        null, // Not used in our mock
                        false, -1, ComputationFragmentSchedulingPolicy.ALL_NODES,
                        WarehouseManager.DEFAULT_RESOURCE);

        // Test DefaultSharedDataWorkerProvider - should NOT select blacklisted node as backup
        DefaultSharedDataWorkerProvider.Factory defaultFactory =
                new DefaultSharedDataWorkerProvider.Factory(backupRoutingPolicy);
        DefaultSharedDataWorkerProvider defaultProvider =
                defaultFactory.captureAvailableWorkers(
                        null, // Not used in our mock
                        false, -1, ComputationFragmentSchedulingPolicy.ALL_NODES,
                        WarehouseManager.DEFAULT_RESOURCE);

        long workerId = 10L;
        Set<Long> skipEligible = ImmutableSet.of(11L, 12L, 13L, 14L);
        Set<Long> defaultEligible = ImmutableSet.of(12L, 13L, 14L);

        for (int j = 0; j < 100; j++) {
            // Test SkipBlacklistSharedDataWorkerProvider - should include blacklisted node as backup
            assertSelectBackupWorkerRespectsEligible(workerId, skipProvider.selectBackupWorker(workerId),
                    skipEligible);
            // Test DefaultSharedDataWorkerProvider - should NOT select blacklisted node as backup
            assertSelectBackupWorkerRespectsEligible(workerId, defaultProvider.selectBackupWorker(workerId),
                    defaultEligible);
        }
    }
}
