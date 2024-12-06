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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.SessionVariableConstants.ComputationFragmentSchedulingPolicy;
import com.starrocks.qe.SimpleScheduler;
import com.starrocks.qe.scheduler.NonRecoverableException;
import com.starrocks.qe.scheduler.WorkerProvider;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.warehouse.Warehouse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.starrocks.qe.WorkerProviderHelper.getNextWorker;

/**
 * WorkerProvider for SHARED_DATA mode. Compared to its counterpart for SHARED_NOTHING mode:
 * 1. All Backends and ComputeNodes are treated the same as ComputeNodes.
 * 2. Allows using backup node, when any of the initial workers in scan location is not available.
 * Be noticed that,
 * - All the worker nodes and available worker nodes are captured at the time of this provider creation. It
 * is possible that the worker may not be available later when calling the interfaces of this provider.
 * - All the nodes will be considered as available after the snapshot nodes info are captured, even though it
 * may not be true all the time.
 * - Specifically, when calling `selectBackupWorker()`, the selected node will be checked again if it is in
 * `SimpleScheduler.isInBlocklist()` or not, to make sure that the backup node is in the available node list
 * and not in the block list.
 * Also in shared-data mode, all nodes will be treated as compute nodes. so the session variable @@prefer_compute_node
 * will be always true, and @@use_compute_nodes will be always -1 which means using all the available compute nodes.
 */
public class DefaultSharedDataWorkerProvider implements WorkerProvider {
    private static final Logger LOG = LogManager.getLogger(DefaultSharedDataWorkerProvider.class);
    private static final AtomicInteger NEXT_COMPUTE_NODE_INDEX = new AtomicInteger(0);

    public static class Factory implements WorkerProvider.Factory {
        @Override
        public DefaultSharedDataWorkerProvider captureAvailableWorkers(SystemInfoService systemInfoService,
                                               boolean preferComputeNode,
                                               int numUsedComputeNodes,
                                               ComputationFragmentSchedulingPolicy computationFragmentSchedulingPolicy,
                                               long warehouseId) {

            WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
            ImmutableMap.Builder<Long, ComputeNode> builder = ImmutableMap.builder();
            List<Long> computeNodeIds = warehouseManager.getAllComputeNodeIds(warehouseId);
            computeNodeIds.forEach(nodeId -> builder.put(nodeId,
                    GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendOrComputeNode(nodeId)));
            ImmutableMap<Long, ComputeNode> idToComputeNode = builder.build();
            if (LOG.isDebugEnabled()) {
                LOG.debug("idToComputeNode: {}", idToComputeNode);
            }

            ImmutableMap<Long, ComputeNode> availableComputeNodes = filterAvailableWorkers(idToComputeNode);
            if (availableComputeNodes.isEmpty()) {
                Warehouse warehouse = warehouseManager.getWarehouse(warehouseId);
                throw ErrorReportException.report(ErrorCode.ERR_NO_NODES_IN_WAREHOUSE, warehouse.getName());
            }

            return new DefaultSharedDataWorkerProvider(idToComputeNode, availableComputeNodes);
        }
    }

    /**
     * All the compute nodes (including backends), including those that are not alive or in block list.
     */
    private final ImmutableMap<Long, ComputeNode> id2ComputeNode;
    /**
     * The available compute nodes, which are alive and not in the block list when creating the snapshot. It is still
     * possible that the node becomes unavailable later, it will be checked again in some of the interfaces.
     */
    private final ImmutableMap<Long, ComputeNode> availableID2ComputeNode;

    /**
     * List of the compute node ids, used to select buddy node in case some of the nodes are not available.
     */
    private ImmutableList<Long> allComputeNodeIds;

    private final Set<Long> selectedWorkerIds;

    @VisibleForTesting
    public DefaultSharedDataWorkerProvider(ImmutableMap<Long, ComputeNode> id2ComputeNode,
                                           ImmutableMap<Long, ComputeNode> availableID2ComputeNode
    ) {
        this.id2ComputeNode = id2ComputeNode;
        this.availableID2ComputeNode = availableID2ComputeNode;
        this.selectedWorkerIds = Sets.newConcurrentHashSet();
        this.allComputeNodeIds = null;
    }

    @Override
    public long selectNextWorker() throws NonRecoverableException {
        ComputeNode worker;
        worker = getNextWorker(availableID2ComputeNode, DefaultSharedDataWorkerProvider::getNextComputeNodeIndex);

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
            reportWorkerNotFoundException(workerId);
        }
        selectWorkerUnchecked(workerId);
    }

    @Override
    public List<Long> selectAllComputeNodes() {
        List<Long> nodeIds = availableID2ComputeNode.values().stream()
                .map(ComputeNode::getId)
                .collect(Collectors.toList());
        nodeIds.forEach(this::selectWorkerUnchecked);
        return nodeIds;
    }

    @Override
    public Collection<ComputeNode> getAllWorkers() {
        return availableID2ComputeNode.values();
    }

    @Override
    public ComputeNode getWorkerById(long workerId) {
        return availableID2ComputeNode.get(workerId);
    }

    @Override
    public boolean isDataNodeAvailable(long dataNodeId) {
        // DataNode and ComputeNode is exchangeable in SHARED_DATA mode
        return availableID2ComputeNode.containsKey(dataNodeId);
    }

    @Override
    public void reportDataNodeNotFoundException() throws NonRecoverableException {
        reportWorkerNotFoundException();
    }

    @Override
    public boolean isWorkerSelected(long workerId) {
        return selectedWorkerIds.contains(workerId);
    }

    @Override
    public List<Long> getSelectedWorkerIds() {
        return new ArrayList<>(selectedWorkerIds);
    }

    @Override
    public List<Long> getAllAvailableNodes() {
        return Lists.newArrayList(availableID2ComputeNode.keySet());
    }

    @Override
    public boolean isPreferComputeNode() {
        return true;
    }

    @Override
    public void selectWorkerUnchecked(long workerId) {
        selectedWorkerIds.add(workerId);
    }

    @Override
    public void reportWorkerNotFoundException() throws NonRecoverableException {
        reportWorkerNotFoundException(-1);
    }

    private void reportWorkerNotFoundException(long workerId) throws NonRecoverableException {
        throw new NonRecoverableException(
                FeConstants.getNodeNotFoundError(true) + " nodeId: " + workerId + " " + computeNodesToString(false));
    }

    @Override
    public boolean allowUsingBackupNode() {
        return true;
    }

    /**
     * Try to select the next workerId in the sorted list just after the workerId, if the next one is not available,
     * e.g. also in BlockList, then try the next one in the list, until all nodes have benn tried.
     */
    @Override
    public long selectBackupWorker(long workerId) {
        if (availableID2ComputeNode.isEmpty() || !id2ComputeNode.containsKey(workerId)) {
            return -1;
        }
        if (allComputeNodeIds == null) {
            createAvailableIdList();
        }
        Preconditions.checkNotNull(allComputeNodeIds);
        Preconditions.checkState(allComputeNodeIds.contains(workerId));

        int startPos = allComputeNodeIds.indexOf(workerId);
        int attempts = allComputeNodeIds.size();
        while (attempts-- > 0) {
            // ensure the buddyId selection is stable, that is, giving the same input, the output is always the same.
            // TODO: call StarOSAgent interface, let starmgr to choose a buddy node or trigger scheduling as necessary.
            startPos = (startPos + 1) % allComputeNodeIds.size();
            long buddyId = allComputeNodeIds.get(startPos);
            if (buddyId != workerId && availableID2ComputeNode.containsKey(buddyId) &&
                    !SimpleScheduler.isInBlocklist(buddyId)) {
                return buddyId;
            }
        }
        return -1;
    }

    @Override
    public String toString() {
        return computeNodesToString(true);
    }

    private String computeNodesToString(boolean allowNormalNodes) {
        StringBuilder out = new StringBuilder("compute node: ");

        id2ComputeNode.forEach((backendID, backend) -> {
            if (allowNormalNodes || !backend.isAlive() || !availableID2ComputeNode.containsKey(backendID) ||
                    SimpleScheduler.isInBlocklist(backendID)) {
                out.append(
                        String.format("[%s alive: %b, available: %b, inBlacklist: %b] ", backend.getHost(),
                                backend.isAlive(), availableID2ComputeNode.containsKey(backendID),
                                SimpleScheduler.isInBlocklist(backendID)));
            }
        });
        return out.toString();
    }

    private void createAvailableIdList() {
        List<Long> ids = new ArrayList<>(id2ComputeNode.keySet());
        Collections.sort(ids);
        this.allComputeNodeIds = ImmutableList.copyOf(ids);
    }

    @VisibleForTesting
    static int getNextComputeNodeIndex() {
        return NEXT_COMPUTE_NODE_INDEX.getAndIncrement();
    }

    @VisibleForTesting
    static AtomicInteger getNextComputeNodeIndexer() {
        return NEXT_COMPUTE_NODE_INDEX;
    }

    private static ImmutableMap<Long, ComputeNode> filterAvailableWorkers(ImmutableMap<Long, ComputeNode> workers) {
        ImmutableMap.Builder<Long, ComputeNode> builder = new ImmutableMap.Builder<>();
        for (Map.Entry<Long, ComputeNode> entry : workers.entrySet()) {
            if (entry.getValue().isAlive() && !SimpleScheduler.isInBlocklist(entry.getKey())) {
                builder.put(entry);
            }
        }
        return builder.build();
    }
}
