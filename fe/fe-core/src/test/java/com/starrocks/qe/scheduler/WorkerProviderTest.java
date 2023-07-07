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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.starrocks.common.Config;
import com.starrocks.common.Reference;
import com.starrocks.common.UserException;
import com.starrocks.qe.SimpleScheduler;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TNetworkAddress;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class WorkerProviderTest {
    private final ImmutableMap<Long, ComputeNode> id2Backend = genWorkers(0, 10, Backend::new);
    private final ImmutableMap<Long, ComputeNode> id2ComputeNode = genWorkers(10, 15, ComputeNode::new);
    private final ImmutableMap<Long, ComputeNode> availableId2Backend = ImmutableMap.of(
            0L, id2Backend.get(0L),
            2L, id2Backend.get(2L),
            3L, id2Backend.get(3L),
            5L, id2Backend.get(5L),
            7L, id2Backend.get(7L));
    private final ImmutableMap<Long, ComputeNode> availableId2ComputeNode = ImmutableMap.of(
            10L, id2ComputeNode.get(10L),
            12L, id2ComputeNode.get(12L),
            14L, id2ComputeNode.get(14L));
    private final Map<Long, ComputeNode> availableId2Worker = Stream.of(availableId2Backend, availableId2ComputeNode)
            .map(Map::entrySet)
            .flatMap(Collection::stream)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    private static <C extends ComputeNode> ImmutableMap<Long, C> genWorkers(long startId, long endId,
                                                                            Supplier<C> factory) {
        Map<Long, C> res = new TreeMap<>();
        for (long i = startId; i < endId; i++) {
            C worker = factory.get();
            worker.setId(i);
            worker.setAlive(true);
            worker.setHost("host#" + i);
            worker.setBePort(80);
            res.put(i, worker);
        }
        return ImmutableMap.copyOf(res);
    }

    @Test
    public void testCaptureAvailableWorkers() {

        long deadBEId = 1L;
        long deadCNId = 11L;
        long inBlacklistBEId = 3L;
        long inBlacklistCNId = 13L;
        Set<Long> nonAvailableWorkerId = ImmutableSet.of(deadBEId, deadCNId, inBlacklistBEId, inBlacklistCNId);
        id2Backend.get(deadBEId).setAlive(false);
        id2ComputeNode.get(deadCNId).setAlive(false);
        new MockUp<SimpleScheduler>() {
            @Mock
            public boolean isInBlacklist(long backendId) {
                return backendId == inBlacklistBEId || backendId == inBlacklistCNId;
            }
        };

        Reference<Integer> nextComputeNodeIndex = new Reference<>(0);
        new MockUp<WorkerProvider>() {
            @Mock
            int getNextComputeNodeIndex() {
                int next = nextComputeNodeIndex.getRef();
                nextComputeNodeIndex.setRef(next + 1);
                return next;
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

        WorkerProvider workerProvider;
        List<Integer> numUsedComputeNodesList = ImmutableList.of(100, 0, -1, 1, 2, 3, 4, 5, 6);
        for (Integer numUsedComputeNodes : numUsedComputeNodesList) {
            // Reset nextComputeNodeIndex.
            nextComputeNodeIndex.setRef(0);

            workerProvider =
                    WorkerProvider.captureAvailableWorkers(GlobalStateMgr.getCurrentSystemInfo(), true,
                            numUsedComputeNodes);

            int numAvailableComputeNodes = 0;
            for (long id = 0; id < 15; id++) {
                ComputeNode worker = workerProvider.getWorker(id);
                if (nonAvailableWorkerId.contains(id)
                        // Exceed the limitation of numUsedComputeNodes.
                        || (numUsedComputeNodes > 0 && numAvailableComputeNodes >= numUsedComputeNodes)) {
                    Assert.assertNull(worker);
                } else {
                    Assert.assertNotNull("numUsedComputeNodes=" + numUsedComputeNodes + ",id=" + id, worker);
                    Assert.assertEquals(id, worker.getId());

                    if (id2ComputeNode.containsKey(id)) {
                        numAvailableComputeNodes++;
                    }
                }
            }
        }
    }

    @Test
    public void testCaptureAvailableWorkersForSharedData() {
        String prevRunMode = Config.run_mode;
        try {
            Config.run_mode = "shared_data";
            RunMode.detectRunMode();

            long deadBEId = 1L;
            long deadCNId = 11L;
            long inBlacklistBEId = 3L;
            long inBlacklistCNId = 13L;
            Set<Long> nonAvailableWorkerId = ImmutableSet.of(deadBEId, deadCNId, inBlacklistBEId, inBlacklistCNId);
            id2Backend.get(deadBEId).setAlive(false);
            id2ComputeNode.get(deadCNId).setAlive(false);
            new MockUp<SimpleScheduler>() {
                @Mock
                public boolean isInBlacklist(long backendId) {
                    return backendId == inBlacklistBEId || backendId == inBlacklistCNId;
                }
            };

            Reference<Integer> nextComputeNodeIndex = new Reference<>(0);
            new MockUp<WorkerProvider>() {
                @Mock
                int getNextComputeNodeIndex() {
                    int next = nextComputeNodeIndex.getRef();
                    nextComputeNodeIndex.setRef(next + 1);
                    return next;
                }
            };

            new MockUp<WarehouseManager>() {
                @Mock
                public ImmutableMap<Long, ComputeNode> getComputeNodesFromWarehouse() {
                    return id2ComputeNode;
                }
            };

            WorkerProvider workerProvider;
            List<Integer> numUsedComputeNodesList = ImmutableList.of(100, 0, -1, 1, 2, 3, 4, 5, 6);
            for (Integer numUsedComputeNodes : numUsedComputeNodesList) {
                // Reset nextComputeNodeIndex.
                nextComputeNodeIndex.setRef(0);

                workerProvider =
                        WorkerProvider.captureAvailableWorkers(GlobalStateMgr.getCurrentSystemInfo(), false,
                                numUsedComputeNodes);

                for (long id = 0; id < 15; id++) {
                    ComputeNode worker = workerProvider.getWorker(id);
                    ComputeNode backend = workerProvider.getBackend(id);
                    // SHARED_DATA MODE considers backends and compute nodes the same.
                    Assert.assertEquals(backend, worker);
                    if (nonAvailableWorkerId.contains(id) || !id2ComputeNode.containsKey(id)) {
                        Assert.assertNull(worker);
                    } else {
                        Assert.assertNotNull("id=" + id, worker);
                        Assert.assertEquals(id, worker.getId());
                    }
                }
            }
        } finally {
            Config.run_mode = prevRunMode;
            RunMode.detectRunMode();
        }
    }

    @Test
    public void testChooseBackend() throws UserException {
        WorkerProvider workerProvider =
                new WorkerProvider(id2Backend, id2ComputeNode, availableId2Backend, availableId2ComputeNode, true);
        for (long id = 0; id < 15; id++) {
            if (availableId2Backend.containsKey(id)) {
                ComputeNode backend = availableId2Backend.get(id);

                TNetworkAddress addr = workerProvider.chooseBackend(id);
                Assert.assertEquals(backend.getAddress(), addr);
                testUsingWorkerHelper(workerProvider, backend);
            } else {
                long finalId = id;
                Assert.assertThrows(UserException.class, () -> workerProvider.chooseBackend(finalId));
            }
        }
    }

    private static <C extends ComputeNode> void testChooseNextWorkerHelper(WorkerProvider workerProvider,
                                                                           Map<Long, C> id2Worker)
            throws UserException {

        Reference<Long> workerIdRef = new Reference<>();
        Set<Long> selectedWorkers = new HashSet<>(id2Worker.size());
        for (int i = 0; i < id2Worker.size(); i++) {
            TNetworkAddress addr = workerProvider.chooseNextWorker(workerIdRef);
            long id = workerIdRef.getRef();
            ComputeNode worker = id2Worker.get(id);

            Assert.assertEquals(worker.getAddress(), addr);

            Assert.assertFalse(selectedWorkers.contains(id));
            selectedWorkers.add(id);

            testUsingWorkerHelper(workerProvider, worker);
        }
    }

    @Test
    public void testChooseNextWorker() throws UserException {
        WorkerProvider workerProvider;
        Reference<Long> workerIdRef = new Reference<>();

        workerProvider =
                new WorkerProvider(id2Backend, id2ComputeNode, availableId2Backend, availableId2ComputeNode, true);
        testChooseNextWorkerHelper(workerProvider, availableId2ComputeNode);

        workerProvider = new WorkerProvider(id2Backend, id2ComputeNode, availableId2Backend, ImmutableMap.of(), true);
        testChooseNextWorkerHelper(workerProvider, availableId2Backend);

        workerProvider =
                new WorkerProvider(id2Backend, id2ComputeNode, availableId2Backend, availableId2ComputeNode, false);
        testChooseNextWorkerHelper(workerProvider, availableId2Backend);

        workerProvider = new WorkerProvider(id2Backend, id2ComputeNode, ImmutableMap.of(), ImmutableMap.of(), false);
        WorkerProvider finalWorkerProvider = workerProvider;
        Assert.assertThrows(UserException.class, () -> finalWorkerProvider.chooseNextWorker(workerIdRef));
    }

    @Test
    public void testChooseAllComputedNodes() {
        WorkerProvider workerProvider;
        List<TNetworkAddress> computeNodeAddrs;

        workerProvider =
                new WorkerProvider(id2Backend, id2ComputeNode, availableId2Backend, availableId2ComputeNode, false);
        computeNodeAddrs = workerProvider.chooseAllComputedNodes();
        Assert.assertTrue(computeNodeAddrs.isEmpty());

        workerProvider =
                new WorkerProvider(id2Backend, id2ComputeNode, availableId2Backend, availableId2ComputeNode, true);
        computeNodeAddrs = workerProvider.chooseAllComputedNodes();
        Assert.assertEquals(availableId2ComputeNode.size(), computeNodeAddrs.size());
        Set<TNetworkAddress> computeNodeAddrSet = new HashSet<>(computeNodeAddrs);
        for (ComputeNode computeNode : availableId2ComputeNode.values()) {
            Assert.assertTrue(computeNodeAddrSet.contains(computeNode.getAddress()));

            testUsingWorkerHelper(workerProvider, computeNode);
        }
    }

    private static <C extends ComputeNode> void testGetBackendHelper(WorkerProvider workerProvider,
                                                                     Map<Long, C> availableId2Worker) {
        for (long id = -1; id < 16; id++) {
            ComputeNode backend = workerProvider.getBackend(id);
            boolean isContained = workerProvider.isBackendAvailable(id);
            if (!availableId2Worker.containsKey(id)) {
                Assert.assertNull(backend);
                Assert.assertFalse(isContained);
            } else {
                Assert.assertNotNull("id=" + id, backend);
                Assert.assertEquals(availableId2Worker.get(id), backend);
                Assert.assertTrue(isContained);
            }
        }
    }

    @Test
    public void testGetBackend() {
        WorkerProvider workerProvider =
                new WorkerProvider(id2Backend, id2ComputeNode, availableId2Backend, availableId2ComputeNode, true);
        testGetBackendHelper(workerProvider, availableId2Backend);
    }

    @Test
    public void testGetWorkersPreferringComputeNode() {
        WorkerProvider workerProvider;

        workerProvider =
                new WorkerProvider(id2Backend, id2ComputeNode, availableId2Backend, availableId2ComputeNode, true);
        assertThat(workerProvider.getWorkers())
                .containsOnlyOnceElementsOf(availableId2ComputeNode.values());

        workerProvider = new WorkerProvider(id2Backend, id2ComputeNode, availableId2Backend, ImmutableMap.of(), true);
        assertThat(workerProvider.getWorkers())
                .containsOnlyOnceElementsOf(availableId2Backend.values());

        workerProvider =
                new WorkerProvider(id2Backend, id2ComputeNode, availableId2Backend, availableId2ComputeNode, false);
        assertThat(workerProvider.getWorkers())
                .containsOnlyOnceElementsOf(availableId2ComputeNode.values());
    }

    @Test
    public void testReportBackendNotFoundException() {
        WorkerProvider workerProvider =
                new WorkerProvider(id2Backend, id2ComputeNode, availableId2Backend, availableId2ComputeNode, true);
        Assert.assertThrows(SchedulerException.class, workerProvider::reportBackendNotFoundException);
    }

    public static void testUsingWorkerHelper(WorkerProvider workerProvider, ComputeNode worker) {
        Assert.assertTrue(workerProvider.isUsingWorker(worker.getId()));
        Assert.assertEquals(worker.getHttpAddress(), workerProvider.getUsedHttpAddrByBeAddr(worker.getAddress()));
        Assert.assertEquals(worker, workerProvider.getUsedWorkerByBeAddr(worker.getAddress()));
        assertThat(workerProvider.getUsedWorkerIDs()).contains(worker.getId());
        assertThat(workerProvider.getUsedWorkerBeAddrs()).contains(worker.getAddress());
    }
}
