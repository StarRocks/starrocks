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
import com.starrocks.qe.SessionVariableConstants.ComputationFragmentSchedulingPolicy;
import com.starrocks.qe.SimpleScheduler;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.HistoricalNodeMgr;
import com.starrocks.system.SystemInfoService;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Supplier;

public class CandidateWorkerProviderTest {
    private SystemInfoService systemInfoService = new SystemInfoService();
    private final ImmutableMap<Long, Backend> id2Backend = genWorkers(0, 10, Backend::new, false);
    private final ImmutableMap<Long, ComputeNode> id2ComputeNode = genWorkers(10, 15, ComputeNode::new, false);
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

    @Before
    public void setUp() throws IOException {
        for (Backend backend : id2Backend.values()) {
            systemInfoService.addBackend(backend);
        }
        for (ComputeNode computeNode : id2ComputeNode.values()) {
            systemInfoService.addComputeNode(computeNode);
        }

        WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        warehouseManager.initDefaultWarehouse();
        HistoricalNodeMgr historicalNodeMgr = GlobalStateMgr.getCurrentState().getHistoricalNodeMgr();

        String warehouse = WarehouseManager.DEFAULT_WAREHOUSE_NAME;
        //List<Long> computeNodeIds = Arrays.asList(201L, 202L);
        long updateTime = System.currentTimeMillis();
        historicalNodeMgr.updateHistoricalBackendIds(id2Backend.keySet().asList(), updateTime, warehouse);
        historicalNodeMgr.updateHistoricalComputeNodeIds(id2ComputeNode.keySet().asList(), updateTime, warehouse);
    }

    @Test
    public void testCaptureAvailableWorkers() {
        CandidateWorkerProvider.Factory workerProviderFactory = new CandidateWorkerProvider.Factory();

        CandidateWorkerProvider workerProvider = workerProviderFactory.captureAvailableWorkers(systemInfoService, true, 100,
                ComputationFragmentSchedulingPolicy.COMPUTE_NODES_ONLY, WarehouseManager.DEFAULT_WAREHOUSE_ID);
        Collection<ComputeNode> workers = workerProvider.getAllWorkers();
        Assert.assertEquals(workers.size(), id2ComputeNode.size());

        CandidateWorkerProvider workerProvider2 = workerProviderFactory.captureAvailableWorkers(systemInfoService, true, 100,
                ComputationFragmentSchedulingPolicy.ALL_NODES, WarehouseManager.DEFAULT_WAREHOUSE_ID);
        Collection<ComputeNode> workers2 = workerProvider2.getAllWorkers();
        Assert.assertEquals(workers2.size(), id2ComputeNode.size() + id2Backend.size());

        CandidateWorkerProvider workerProvider3 = workerProviderFactory.captureAvailableWorkers(systemInfoService, true, 12,
                ComputationFragmentSchedulingPolicy.ALL_NODES, WarehouseManager.DEFAULT_WAREHOUSE_ID);
        Collection<ComputeNode> workers3 = workerProvider3.getAllWorkers();
        Assert.assertEquals(workers3.size(), 12);

        CandidateWorkerProvider workerProvider4 = workerProviderFactory.captureAvailableWorkers(systemInfoService, true, 0,
                ComputationFragmentSchedulingPolicy.ALL_NODES, WarehouseManager.DEFAULT_WAREHOUSE_ID);
        Collection<ComputeNode> workers4 = workerProvider4.getAllWorkers();
        Assert.assertEquals(workers4.size(), id2ComputeNode.size() + id2Backend.size());
    }

    @Test
    public void testFilterWorkers() {
        long deadBEId = 1L;
        long deadCNId = 11L;
        long inBlacklistBEId = 3L;
        long inBlacklistCNId = 13L;
        id2Backend.get(deadBEId).setAlive(false);
        id2ComputeNode.get(deadCNId).setAlive(false);

        new MockUp<SimpleScheduler>() {
            @Mock
            public boolean isInBlocklist(long backendId) {
                return backendId == inBlacklistBEId || backendId == inBlacklistCNId;
            }
        };

        CandidateWorkerProvider.Factory workerProviderFactory = new CandidateWorkerProvider.Factory();

        CandidateWorkerProvider workerProvider = workerProviderFactory.captureAvailableWorkers(systemInfoService, true, 100,
                ComputationFragmentSchedulingPolicy.ALL_NODES, WarehouseManager.DEFAULT_WAREHOUSE_ID);
        Collection<ComputeNode> workers = workerProvider.getAllWorkers();
        Assert.assertEquals(workers.size(), id2ComputeNode.size() + id2Backend.size() - 4);
    }
}
