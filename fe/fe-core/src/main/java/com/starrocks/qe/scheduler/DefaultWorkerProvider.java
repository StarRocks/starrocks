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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariableConstants.ComputationFragmentSchedulingPolicy;
import com.starrocks.qe.SimpleScheduler;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import org.apache.commons.collections4.MapUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.starrocks.qe.WorkerProviderHelper.getNextWorker;

/**
 * DefaultWorkerProvider handles ComputeNode/Backend selection in SHARED_NOTHING mode.
 * NOTE: remember to update DefaultSharedDataWorkerProvider if the change applies to both run modes.
 */
public class DefaultWorkerProvider implements WorkerProvider {
    private static final Logger LOG = LogManager.getLogger(DefaultWorkerProvider.class);

    private static final AtomicInteger NEXT_COMPUTE_NODE_INDEX = new AtomicInteger(0);
    private static final AtomicInteger NEXT_BACKEND_INDEX = new AtomicInteger(0);

    /**
     * All the backend nodes, including those that are not alive or in blacklist.
     */
    private final ImmutableMap<Long, ComputeNode> id2Backend;
    /**
     * All the compute nodes, including those that are not alive or in blacklist.
     */
    private final ImmutableMap<Long, ComputeNode> id2ComputeNode;
    /**
     * The available backend nodes, which are alive and not in the blacklist.
     */
    private final ImmutableMap<Long, ComputeNode> availableID2Backend;
    /**
     * The available compute nodes, which are alive and not in the blacklist.
     */
    private final ImmutableMap<Long, ComputeNode> availableID2ComputeNode;

    /**
     * These three members record the workers used by the related job.
     * They are updated when calling {@code chooseXXX} methods.
     */
    private final Set<Long> selectedWorkerIds;

    /**
     * Indicates whether there are available compute nodes.
     */
    private final boolean hasComputeNode;
    /**
     * Indicates whether compute nodes should be used for non-HDFS operators.
     * The HDFS operator always prefers to use compute nodes even if {@code usedComputeNode} is false.
     */
    private final boolean usedComputeNode;

    private final boolean preferComputeNode;

    public static class Factory implements WorkerProvider.Factory {
        @Override
        public DefaultWorkerProvider captureAvailableWorkers(SystemInfoService systemInfoService,
                                     boolean preferComputeNode, int numUsedComputeNodes,
                                     ComputationFragmentSchedulingPolicy computationFragmentSchedulingPolicy,
                                     long warehouseId) {

            ImmutableMap<Long, ComputeNode> idToComputeNode =
                    buildComputeNodeInfo(systemInfoService, numUsedComputeNodes, 
                                         computationFragmentSchedulingPolicy, warehouseId);

            ImmutableMap<Long, ComputeNode> idToBackend = ImmutableMap.copyOf(systemInfoService.getIdToBackend());

            if (LOG.isDebugEnabled()) {
                LOG.debug("idToBackend size={}", idToBackend.size());
                for (Map.Entry<Long, ComputeNode> entry : idToBackend.entrySet()) {
                    Long backendID = entry.getKey();
                    ComputeNode backend = entry.getValue();
                    LOG.debug("backend: {}-{}-{}", backendID, backend.getHost(), backend.getBePort());
                }

                LOG.debug("idToComputeNode: {}", idToComputeNode);
            }

            return new DefaultWorkerProvider(idToBackend, idToComputeNode,
                    filterAvailableWorkers(idToBackend), filterAvailableWorkers(idToComputeNode),
                    preferComputeNode);
        }
    }

    @VisibleForTesting
    public DefaultWorkerProvider(ImmutableMap<Long, ComputeNode> id2Backend,
                                 ImmutableMap<Long, ComputeNode> id2ComputeNode,
                                 ImmutableMap<Long, ComputeNode> availableID2Backend,
                                 ImmutableMap<Long, ComputeNode> availableID2ComputeNode,
                                 boolean preferComputeNode) {
        this.id2Backend = id2Backend;
        this.id2ComputeNode = id2ComputeNode;

        this.availableID2Backend = availableID2Backend;
        this.availableID2ComputeNode = availableID2ComputeNode;

        this.selectedWorkerIds = Sets.newConcurrentHashSet();

        this.hasComputeNode = MapUtils.isNotEmpty(availableID2ComputeNode);
        if (MapUtils.isEmpty(availableID2Backend) && hasComputeNode) {
            this.usedComputeNode = true;
        } else {
            this.usedComputeNode = hasComputeNode && preferComputeNode;
        }
        this.preferComputeNode = preferComputeNode;
    }

    @VisibleForTesting
    public DefaultWorkerProvider(
            ImmutableMap<Long, ComputeNode> id2ComputeNode,
            ImmutableMap<Long, ComputeNode> availableID2ComputeNode) {
        this.id2Backend = ImmutableMap.of();
        this.id2ComputeNode = id2ComputeNode;

        this.availableID2Backend = ImmutableMap.of();
        this.availableID2ComputeNode = availableID2ComputeNode;

        this.selectedWorkerIds = Sets.newConcurrentHashSet();

        this.hasComputeNode = true;
        this.preferComputeNode = true;
        this.usedComputeNode = true;
    }

    @Override
    public long selectNextWorker() throws NonRecoverableException {
        ComputeNode worker;
        if (usedComputeNode) {
            worker = getNextWorker(availableID2ComputeNode, DefaultWorkerProvider::getNextComputeNodeIndex);
        } else {
            worker = getNextWorker(availableID2Backend, DefaultWorkerProvider::getNextBackendIndex);
        }

        if (worker == null) {
            reportWorkerNotFoundException();
        }
        Preconditions.checkNotNull(worker);

        selectWorkerUnchecked(worker.getId());
        return worker.getId();
    }

    @Override
    public void selectWorker(long workerId) throws NonRecoverableException {
        if (getWorkerById(workerId) == null) {
            reportWorkerNotFoundException();
        }
        selectWorkerUnchecked(workerId);
    }

    @Override
    public List<Long> selectAllComputeNodes() {
        if (!usedComputeNode) {
            return Collections.emptyList();
        }

        List<Long> nodeIds = availableID2ComputeNode.values().stream()
                .map(ComputeNode::getId)
                .collect(Collectors.toList());
        nodeIds.forEach(this::selectWorkerUnchecked);

        return nodeIds;
    }

    @Override
    public Collection<ComputeNode> getAllWorkers() {
        if (hasComputeNode) {
            return availableID2ComputeNode.values();
        } else {
            return ImmutableList.copyOf(availableID2Backend.values());
        }
    }

    @Override
    public ComputeNode getWorkerById(long workerId) {
        ComputeNode worker = availableID2Backend.get(workerId);
        if (worker != null) {
            return worker;
        }
        return availableID2ComputeNode.get(workerId);
    }

    @Override
    public boolean isDataNodeAvailable(long dataNodeId) {
        return getBackend(dataNodeId) != null || getComputeNode(dataNodeId) != null;
    }

    @Override
    public void reportDataNodeNotFoundException() throws NonRecoverableException {
        reportWorkerNotFoundException(false);
    }

    @Override
    public void reportWorkerNotFoundException() throws NonRecoverableException {
        reportWorkerNotFoundException(usedComputeNode);
    }

    @Override
    public boolean isWorkerSelected(long workerId) {
        return selectedWorkerIds.contains(workerId);
    }

    @Override
    public List<Long> getSelectedWorkerIds() {
        return new ArrayList<>(selectedWorkerIds);
    }

    /**
     * if usedComputeNode turns on or no backend, we add all compute nodes to the result.
     * if perferComputeNode turns on, we just return computeNode set
     * else add backend set and return
     *
     * @return
     */
    @Override
    public List<Long> getAllAvailableNodes() {
        List<Long> nodeIds = Lists.newArrayList();
        if (usedComputeNode || availableID2Backend.isEmpty()) {
            nodeIds.addAll(availableID2ComputeNode.keySet());
        }

        if (preferComputeNode) {
            return nodeIds;
        }
        nodeIds.addAll(availableID2Backend.keySet());
        return nodeIds;
    }

    @Override
    public boolean isPreferComputeNode() {
        return preferComputeNode;
    }

    @Override
    public void selectWorkerUnchecked(long workerId) {
        selectedWorkerIds.add(workerId);
    }

    @Override
    public String toString() {
        return toString(usedComputeNode, true);
    }

    @VisibleForTesting
    ComputeNode getBackend(Long backendID) {
        return availableID2Backend.get(backendID);
    }

    @VisibleForTesting
    ComputeNode getComputeNode(Long computeNodeID) {
        return availableID2ComputeNode.get(computeNodeID);
    }

    @Override
    public long selectBackupWorker(long workerId) {
        // not allowed to have backup node
        return -1;
    }

    private String toString(boolean chooseComputeNode, boolean allowNormalNodes) {
        return chooseComputeNode ? computeNodesToString(allowNormalNodes) :
                backendsToString(allowNormalNodes);
    }

    private void reportWorkerNotFoundException(boolean chooseComputeNode) throws NonRecoverableException {
        throw new NonRecoverableException(
                FeConstants.getNodeNotFoundError(chooseComputeNode) + toString(chooseComputeNode, false));
    }

    private String computeNodesToString(boolean allowNormalNodes) {
        StringBuilder out = new StringBuilder("compute node: ");

        id2ComputeNode.forEach((backendID, backend) -> {
            if (shouldIncludeNode(backend, backendID, allowNormalNodes)) {
                out.append(
                        String.format("[%s alive: %b inBlacklist: %b] ", backend.getHost(),
                                backend.isAlive(), SimpleScheduler.isInBlocklist(backendID)));
            }
        });
        return out.toString();
    }

    private String backendsToString(boolean allowNormalNodes) {
        StringBuilder out = new StringBuilder("backend: ");
        id2Backend.forEach((backendID, backend) -> {
            if (shouldIncludeNode(backend, backendID, allowNormalNodes)) {
                out.append(
                        formatNodeInfo(backend.getHost(), backend.isAlive(), SimpleScheduler.isInBlocklist(backendID)));
            }
        });
        return out.toString();
    }

    private boolean shouldIncludeNode(ComputeNode node, Long nodeId, boolean allowNormalNodes) {
        return allowNormalNodes || !node.isAlive() || SimpleScheduler.isInBlocklist(nodeId);
    }

    private String formatNodeInfo(String host, boolean isAlive, boolean isInBlacklist) {
        return String.format("[%s alive: %b inBlacklist: %b] ",
                host, isAlive, isInBlacklist);
    }

    @VisibleForTesting
    static int getNextComputeNodeIndex() {
        if (RunMode.getCurrentRunMode() == RunMode.SHARED_DATA) {
            long currentWh = WarehouseManager.DEFAULT_WAREHOUSE_ID;
            if (ConnectContext.get() != null) {
                currentWh = ConnectContext.get().getCurrentWarehouseId();
            }
            return GlobalStateMgr.getCurrentState().getWarehouseMgr().
                    getNextComputeNodeIndexFromWarehouse(currentWh).getAndIncrement();
        }
        return NEXT_COMPUTE_NODE_INDEX.getAndIncrement();
    }

    @VisibleForTesting
    static int getNextBackendIndex() {
        return NEXT_BACKEND_INDEX.getAndIncrement();
    }

    private static ImmutableMap<Long, ComputeNode> buildComputeNodeInfo(SystemInfoService systemInfoService,
                                  int numUsedComputeNodes,
                                  ComputationFragmentSchedulingPolicy computationFragmentSchedulingPolicy,
                                  long warehouseId) {
        //define Node Pool
        Map<Long, ComputeNode> computeNodes = new HashMap<>();

        //get CN and BE from systemInfoService
        ImmutableMap<Long, ComputeNode> idToComputeNode
                = ImmutableMap.copyOf(systemInfoService.getIdComputeNode());
        ImmutableMap<Long, ComputeNode> idToBackend
                = ImmutableMap.copyOf(systemInfoService.getIdToBackend());

        //add CN and BE to Node Pool
        if (numUsedComputeNodes <= 0) {
            computeNodes.putAll(idToComputeNode);
            if (computationFragmentSchedulingPolicy == ComputationFragmentSchedulingPolicy.ALL_NODES) {
                computeNodes.putAll(idToBackend);
            }
        } else {
            for (int i = 0; i < idToComputeNode.size() && computeNodes.size() < numUsedComputeNodes; i++) {
                ComputeNode computeNode =
                        getNextWorker(idToComputeNode, DefaultWorkerProvider::getNextComputeNodeIndex);
                Preconditions.checkNotNull(computeNode);
                if (!isWorkerAvailable(computeNode)) {
                    continue;
                }
                computeNodes.put(computeNode.getId(), computeNode);
            }
            if (computationFragmentSchedulingPolicy == ComputationFragmentSchedulingPolicy.ALL_NODES) {
                for (int i = 0; i < idToBackend.size() && computeNodes.size() < numUsedComputeNodes; i++) {
                    ComputeNode backend =
                            getNextWorker(idToBackend, DefaultWorkerProvider::getNextBackendIndex);
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

    public static boolean isWorkerAvailable(ComputeNode worker) {
        return worker.isAlive() && !SimpleScheduler.isInBlocklist(worker.getId());
    }

    @VisibleForTesting
    static AtomicInteger getNextComputeNodeIndexer() {
        return NEXT_COMPUTE_NODE_INDEX;
    }

    private static <C extends ComputeNode> ImmutableMap<Long, C> filterAvailableWorkers(ImmutableMap<Long, C> workers) {
        return ImmutableMap.copyOf(
                workers.entrySet().stream()
                        .filter(entry -> isWorkerAvailable(entry.getValue()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
        );
    }
}
