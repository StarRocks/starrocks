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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Reference;
import com.starrocks.qe.SimpleScheduler;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TNetworkAddress;
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
    private final Set<Long> usedWorkerIDs;
    private final Map<TNetworkAddress, ComputeNode> beAddr2UsedWorker;
    /**
     * Used only by channel stream load, recording the mapping from BE port to BE's HTTP port.
     */
    private final Map<TNetworkAddress, TNetworkAddress> beAddr2HttpAddr;

    /**
     * Indicates whether there are available compute nodes.
     */
    private final boolean hasComputeNode;
    /**
     * Indicates whether compute nodes should be used for non-HDFS operators.
     * The HDFS operator always prefers to use compute nodes even if {@code usedComputeNode} is false.
     */
    private final boolean usedComputeNode;

    public static class Factory implements WorkerProvider.Factory {
        @Override
        public DefaultWorkerProvider captureAvailableWorkers(SystemInfoService systemInfoService,
                                                             boolean preferComputeNode,
                                                             int numUsedComputeNodes) {

            ImmutableMap<Long, ComputeNode> idToComputeNode =
                    buildComputeNodeInfo(systemInfoService, numUsedComputeNodes);
            ImmutableMap<Long, ComputeNode> idToBackend;
            if (RunMode.getCurrentRunMode() == RunMode.SHARED_DATA) {
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

            // Backends and compute nodes are identical in the SHARED_DATA mode.
            preferComputeNode = preferComputeNode || RunMode.getCurrentRunMode() == RunMode.SHARED_DATA;

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

        this.usedWorkerIDs = Sets.newConcurrentHashSet();
        this.beAddr2UsedWorker = Maps.newHashMap();
        this.beAddr2HttpAddr = Maps.newHashMap();

        this.hasComputeNode = MapUtils.isNotEmpty(availableID2ComputeNode);
        this.usedComputeNode = hasComputeNode && preferComputeNode;
    }

    @Override
    public TNetworkAddress chooseBackend(Long backendID) throws SchedulerException {
        ComputeNode backend = getBackend(backendID);

        if (backend == null) {
            reportBackendNotFoundException();
        }
        Preconditions.checkNotNull(backend);

        TNetworkAddress addr = backend.getAddress();
        recordUsedWorker(backend.getId(), addr);
        return addr;
    }

    @Override
    public TNetworkAddress chooseNextWorker(Reference<Long> workerIdRef) throws SchedulerException {
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

        TNetworkAddress addr = worker.getAddress();
        recordUsedWorker(worker.getId(), addr);
        workerIdRef.setRef(worker.getId());
        return addr;
    }

    @Override
    public List<TNetworkAddress> chooseAllComputedNodes() {
        if (!usedComputeNode) {
            return Collections.emptyList();
        }

        List<TNetworkAddress> addrs = new ArrayList<>(availableID2ComputeNode.size());
        for (ComputeNode computeNode : availableID2ComputeNode.values()) {
            TNetworkAddress addr = computeNode.getAddress();
            recordUsedWorker(computeNode.getId(), addr);
            addrs.add(addr);
        }

        return addrs;
    }

    @Override
    public Collection<ComputeNode> getWorkers() {
        if (hasComputeNode) {
            return availableID2ComputeNode.values();
        } else {
            return ImmutableList.copyOf(availableID2Backend.values());
        }
    }

    @Override
    public boolean isBackendAvailable(Long backendID) {
        return getBackend(backendID) != null;
    }

    @Override
    public void reportBackendNotFoundException() throws SchedulerException {
        reportWorkerNotFoundException(false);
    }

    @Override
    public void reportWorkerNotFoundException() throws SchedulerException {
        reportWorkerNotFoundException(usedComputeNode);
    }

    @Override
    public void recordUsedWorker(Long workerID, TNetworkAddress beAddr) {
        if (usedWorkerIDs.add(workerID)) {
            ComputeNode worker = getWorker(workerID);
            beAddr2UsedWorker.put(beAddr, worker);
            beAddr2HttpAddr.put(beAddr, worker.getHttpAddress());
        }
    }

    @Override
    public boolean isUsingWorker(Long workerID) {
        return usedWorkerIDs.contains(workerID);
    }

    @Override
    public ComputeNode getUsedWorkerByBeAddr(TNetworkAddress addr) {
        ComputeNode worker = beAddr2UsedWorker.get(addr);
        return Preconditions.checkNotNull(worker, "Expect backend address [{}] to being used", addr);
    }

    @Override
    public TNetworkAddress getUsedHttpAddrByBeAddr(TNetworkAddress addr) {
        TNetworkAddress httpAddr = beAddr2HttpAddr.get(addr);
        return Preconditions.checkNotNull(httpAddr, "Expect backend address [{}] to being used", addr);
    }

    @Override
    public List<Long> getUsedWorkerIDs() {
        return new ArrayList<>(usedWorkerIDs);
    }

    @Override
    public Collection<TNetworkAddress> getUsedWorkerBeAddrs() {
        return beAddr2UsedWorker.keySet();
    }

    @Override
    public String toString() {
        return toString(usedComputeNode);
    }

    @VisibleForTesting
    ComputeNode getBackend(Long backendID) {
        return availableID2Backend.get(backendID);
    }

    @VisibleForTesting
    ComputeNode getWorker(Long workerID) {
        ComputeNode worker = availableID2Backend.get(workerID);
        if (worker != null) {
            return worker;
        }
        return availableID2ComputeNode.get(workerID);
    }

    private String toString(boolean chooseComputeNode) {
        return chooseComputeNode ? computeNodesToString() : backendsToString();
    }

    private void reportWorkerNotFoundException(boolean chooseComputeNode) throws SchedulerException {
        throw new SchedulerException(
                FeConstants.getNodeNotFoundError(chooseComputeNode) + toString(chooseComputeNode));
    }

    private String computeNodesToString() {
        StringBuilder out = new StringBuilder("compute node: ");
        id2ComputeNode.forEach((backendID, backend) -> out.append(
                String.format("[%s alive: %b inBlacklist: %b] ", backend.getHost(),
                        backend.isAlive(), SimpleScheduler.isInBlacklist(backendID))));
        return out.toString();
    }

    private String backendsToString() {
        StringBuilder out = new StringBuilder("backend: ");
        id2Backend.forEach((backendID, backend) -> out.append(
                String.format("[%s alive: %b inBlacklist: %b] ", backend.getHost(),
                        backend.isAlive(), SimpleScheduler.isInBlacklist(backendID))));
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
        if (RunMode.getCurrentRunMode() == RunMode.SHARED_DATA) {
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
        return worker.isAlive() && !SimpleScheduler.isInBlacklist(worker.getId());
    }

    private static <C extends ComputeNode> ImmutableMap<Long, C> filterAvailableWorkers(ImmutableMap<Long, C> workers) {
        return ImmutableMap.copyOf(
                workers.entrySet().stream()
                        .filter(entry -> isWorkerAvailable(entry.getValue()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
        );
    }
}
