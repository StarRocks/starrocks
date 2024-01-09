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
import com.google.common.collect.Sets;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.SimpleScheduler;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
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
import java.util.function.IntSupplier;
import java.util.stream.Collectors;

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
                                                             boolean preferComputeNode,
                                                             int numUsedComputeNodes) {

            ImmutableMap<Long, ComputeNode> idToComputeNode =
                    buildComputeNodeInfo(systemInfoService, numUsedComputeNodes);
            ImmutableMap<Long, ComputeNode> idToBackend;
            if (RunMode.isSharedDataMode()) {
                idToBackend = idToComputeNode;
            } else {
                idToBackend = ImmutableMap.copyOf(systemInfoService.getIdToBackend());
            }

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
        // Backends and compute nodes are identical in the SHARED_DATA mode.
        this.usedComputeNode = hasComputeNode && (preferComputeNode || RunMode.isSharedDataMode());
        this.preferComputeNode = preferComputeNode;
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
    public void selectWorker(Long workerId) throws NonRecoverableException {
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
    public ComputeNode getWorkerById(Long workerId) {
        ComputeNode worker = availableID2Backend.get(workerId);
        if (worker != null) {
            return worker;
        }
        return availableID2ComputeNode.get(workerId);
    }

    @Override
    public boolean isDataNodeAvailable(Long dataNodeId) {
        return getBackend(dataNodeId) != null;
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
    public boolean isWorkerSelected(Long workerId) {
        return selectedWorkerIds.contains(workerId);
    }

    @Override
    public List<Long> getSelectedWorkerIds() {
        return new ArrayList<>(selectedWorkerIds);
    }

    @Override
    public boolean isPreferComputeNode() {
        return preferComputeNode;
    }

    @Override
    public String toString() {
        return toString(usedComputeNode);
    }

    @VisibleForTesting
    ComputeNode getBackend(Long backendID) {
        return availableID2Backend.get(backendID);
    }

    private String toString(boolean chooseComputeNode) {
        return chooseComputeNode ? computeNodesToString() : backendsToString();
    }

    private void selectWorkerUnchecked(Long workerId) {
        selectedWorkerIds.add(workerId);
    }

    private void reportWorkerNotFoundException(boolean chooseComputeNode) throws NonRecoverableException {
        throw new NonRecoverableException(
                FeConstants.getNodeNotFoundError(chooseComputeNode) + toString(chooseComputeNode));
    }

    private String computeNodesToString() {
        StringBuilder out = new StringBuilder("compute node: ");
        id2ComputeNode.forEach((backendID, backend) -> out.append(
                String.format("[%s alive: %b inBlacklist: %b] ", backend.getHost(),
                        backend.isAlive(), SimpleScheduler.isInBlocklist(backendID))));
        return out.toString();
    }

    private String backendsToString() {
        StringBuilder out = new StringBuilder("backend: ");
        id2Backend.forEach((backendID, backend) -> out.append(
                String.format("[%s alive: %b inBlacklist: %b] ", backend.getHost(),
                        backend.isAlive(), SimpleScheduler.isInBlocklist(backendID))));
        return out.toString();
    }

    @VisibleForTesting
    static int getNextComputeNodeIndex() {
        return NEXT_COMPUTE_NODE_INDEX.getAndIncrement();
    }

    @VisibleForTesting
    static int getNextBackendIndex() {
        return NEXT_BACKEND_INDEX.getAndIncrement();
    }

    private static ImmutableMap<Long, ComputeNode> buildComputeNodeInfo(SystemInfoService systemInfoService,
                                                                        int numUsedComputeNodes) {
        if (RunMode.isSharedDataMode()) {
            return GlobalStateMgr.getCurrentWarehouseMgr().getComputeNodesFromWarehouse();
        }

        ImmutableMap<Long, ComputeNode> idToComputeNode
                = ImmutableMap.copyOf(systemInfoService.getIdComputeNode());
        if (numUsedComputeNodes <= 0 || numUsedComputeNodes >= idToComputeNode.size()) {
            return idToComputeNode;
        } else {
            Map<Long, ComputeNode> computeNodes = new HashMap<>(numUsedComputeNodes);
            for (int i = 0; i < idToComputeNode.size() && computeNodes.size() < numUsedComputeNodes; i++) {
                ComputeNode computeNode =
                        getNextWorker(idToComputeNode, DefaultWorkerProvider::getNextComputeNodeIndex);
                Preconditions.checkNotNull(computeNode);
                if (!isWorkerAvailable(computeNode)) {
                    continue;
                }
                computeNodes.put(computeNode.getId(), computeNode);
            }
            return ImmutableMap.copyOf(computeNodes);
        }
    }

    private static <C extends ComputeNode> C getNextWorker(ImmutableMap<Long, C> workers,
                                                           IntSupplier getNextWorkerNodeIndex) {
        if (workers.isEmpty()) {
            return null;
        }
        int index = getNextWorkerNodeIndex.getAsInt() % workers.size();
        return workers.values().asList().get(index);
    }

    private static boolean isWorkerAvailable(ComputeNode worker) {
        return worker.isAlive() && !SimpleScheduler.isInBlocklist(worker.getId());
    }

    private static <C extends ComputeNode> ImmutableMap<Long, C> filterAvailableWorkers(ImmutableMap<Long, C> workers) {
        return ImmutableMap.copyOf(
                workers.entrySet().stream()
                        .filter(entry -> isWorkerAvailable(entry.getValue()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
        );
    }
}
