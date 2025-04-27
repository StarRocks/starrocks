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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.starrocks.qe.SessionVariableConstants;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.HistoricalNodeMgr;
import com.starrocks.system.SystemInfoService;
import com.starrocks.warehouse.Warehouse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.starrocks.qe.WorkerProviderHelper.getNextWorker;

/**
 * CandidateWorkerProvider manages the candidate ComputeNode/Backend.
 * Now only its `getAllWorkers` function is used in HDFSBackendSelector to acquire the candidate node
 * which may contain the target cache data.
 * Compared to the default provider, it only differs in worker set for constructor, so we implement it
 * by extending the DefaultWorkerProvider to reduce duplicate code.
 * Currently, we use historical nodes as candidates.
 */
public class CandidateWorkerProvider extends DefaultWorkerProvider implements WorkerProvider {
    private static final Logger LOG = LogManager.getLogger(CandidateWorkerProvider.class);

    private static final AtomicInteger NEXT_COMPUTE_NODE_INDEX = new AtomicInteger(0);
    private static final AtomicInteger NEXT_BACKEND_INDEX = new AtomicInteger(0);

    public static class Factory implements WorkerProvider.Factory {
        @Override
        public CandidateWorkerProvider captureAvailableWorkers(
                SystemInfoService systemInfoService,
                boolean preferComputeNode, int numUsedComputeNodes,
                SessionVariableConstants.ComputationFragmentSchedulingPolicy computationFragmentSchedulingPolicy,
                long warehouseId) {
            WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
            Warehouse warehouse = warehouseManager.getWarehouse(warehouseId);
            HistoricalNodeMgr historicalNodeMgr = GlobalStateMgr.getCurrentState().getHistoricalNodeMgr();

            ImmutableMap<Long, ComputeNode> idToBackend = getHistoricalBackends(systemInfoService, historicalNodeMgr,
                    warehouse.getName());
            ImmutableMap<Long, ComputeNode> idToComputeNode =
                    buildComputeNodeInfo(systemInfoService, historicalNodeMgr, idToBackend, numUsedComputeNodes,
                            computationFragmentSchedulingPolicy, warehouse.getName());

            if (LOG.isDebugEnabled()) {
                LOG.debug("idToBackend: {}", idToBackend);
                LOG.debug("idToComputeNode: {}", idToComputeNode);
            }

            return new CandidateWorkerProvider(idToBackend, idToComputeNode,
                    filterAvailableWorkers(idToBackend), filterAvailableWorkers(idToComputeNode),
                    preferComputeNode, warehouseId);
        }
    }

    @VisibleForTesting
    public CandidateWorkerProvider(ImmutableMap<Long, ComputeNode> id2Backend,
                                   ImmutableMap<Long, ComputeNode> id2ComputeNode,
                                   ImmutableMap<Long, ComputeNode> availableID2Backend,
                                   ImmutableMap<Long, ComputeNode> availableID2ComputeNode,
                                   boolean preferComputeNode, long warehouseId) {
        super(id2Backend, id2ComputeNode, availableID2Backend, availableID2ComputeNode, preferComputeNode, warehouseId);
    }

    private static ImmutableMap<Long, ComputeNode> buildComputeNodeInfo(
            SystemInfoService systemInfoService,
            HistoricalNodeMgr historicalNodeMgr,
            ImmutableMap<Long, ComputeNode> idToBackend,
            int numUsedComputeNodes,
            SessionVariableConstants.ComputationFragmentSchedulingPolicy computationFragmentSchedulingPolicy,
            String warehouse) {
        //get CN and BE from historicalNodeMgr
        ImmutableMap<Long, ComputeNode> idToComputeNode = getHistoricalComputeNodes(
                systemInfoService, historicalNodeMgr, warehouse);
        if (RunMode.isSharedDataMode()) {
            return idToComputeNode;
        }

        //define Node Pool
        Map<Long, ComputeNode> computeNodes = new HashMap<>();

        //add CN and BE to Node Pool
        if (numUsedComputeNodes <= 0) {
            computeNodes.putAll(idToComputeNode);
            if (computationFragmentSchedulingPolicy == SessionVariableConstants.ComputationFragmentSchedulingPolicy.ALL_NODES) {
                computeNodes.putAll(idToBackend);
            }
        } else {
            for (int i = 0; i < idToComputeNode.size() && computeNodes.size() < numUsedComputeNodes; i++) {
                ComputeNode computeNode =
                        getNextWorker(idToComputeNode, CandidateWorkerProvider::getNextComputeNodeIndex);
                Preconditions.checkNotNull(computeNode);
                if (!isWorkerAvailable(computeNode)) {
                    continue;
                }
                computeNodes.put(computeNode.getId(), computeNode);
            }
            if (computationFragmentSchedulingPolicy == SessionVariableConstants.ComputationFragmentSchedulingPolicy.ALL_NODES) {
                for (int i = 0; i < idToBackend.size() && computeNodes.size() < numUsedComputeNodes; i++) {
                    ComputeNode backend =
                            getNextWorker(idToBackend, CandidateWorkerProvider::getNextBackendIndex);
                    Preconditions.checkNotNull(backend);
                    if (!isWorkerAvailable(backend)) {
                        continue;
                    }
                    computeNodes.put(backend.getId(), backend);
                }

            }
        }

        //return Node Pool
        return ImmutableMap.copyOf(computeNodes);
    }

    private static ImmutableMap<Long, ComputeNode> getHistoricalBackends(
            SystemInfoService systemInfoService,
            HistoricalNodeMgr historicalNodeMgr,
            String warehouse) {
        ImmutableMap.Builder<Long, ComputeNode> builder = ImmutableMap.builder();
        ImmutableList<Long> backendIds = historicalNodeMgr.getHistoricalBackendIds(warehouse);
        for (long nodeId : backendIds) {
            ComputeNode backend = systemInfoService.getBackend(nodeId);
            if (backend != null) {
                builder.put(nodeId, backend);
            }
        }
        return builder.build();
    }

    private static ImmutableMap<Long, ComputeNode> getHistoricalComputeNodes(
            SystemInfoService systemInfoService,
            HistoricalNodeMgr historicalNodeMgr,
            String warehouse) {
        ImmutableMap.Builder<Long, ComputeNode> builder = ImmutableMap.builder();
        ImmutableList<Long> computeNodeIds = historicalNodeMgr.getHistoricalComputeNodeIds(warehouse);
        for (long nodeId : computeNodeIds) {
            ComputeNode computeNode = systemInfoService.getComputeNode(nodeId);
            if (computeNode != null) {
                builder.put(nodeId, computeNode);
            }
        }
        return builder.build();
    }

    @VisibleForTesting
    static int getNextComputeNodeIndex() {
        return NEXT_COMPUTE_NODE_INDEX.getAndIncrement();
    }

    @VisibleForTesting
    static int getNextBackendIndex() {
        return NEXT_BACKEND_INDEX.getAndIncrement();
    }

    private static <C extends ComputeNode> ImmutableMap<Long, C> filterAvailableWorkers(ImmutableMap<Long, C> workers) {
        return ImmutableMap.copyOf(
                workers.entrySet().stream()
                        .filter(entry -> isWorkerAvailable(entry.getValue()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
        );
    }
}
