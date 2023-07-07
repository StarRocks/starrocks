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
import com.starrocks.common.UserException;
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
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class WorkerProvider {
    private static final Logger LOG = LogManager.getLogger(WorkerProvider.class);
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
    private ImmutableMap<Long, ComputeNode> availableID2Backend;
    /**
     * The available compute nodes, which are alive and not in the blacklist.
     */
    private ImmutableMap<Long, ComputeNode> availableID2ComputeNode;

    /**
     * These three members record the workers used by the related job.
     * They are updated when calling {@code chooseXXX} methods.
     */
    private final Set<Long> usingWorkerIDs;
    private final Map<TNetworkAddress, ComputeNode> addr2UsingWorker;
    // Used only by channel stream load, records the mapping from BE port to BE's webserver port.
    private final Map<TNetworkAddress, TNetworkAddress> addr2HttpAddr;

    /**
     * Indicates whether there are available compute nodes.
     */
    private boolean hasComputeNode;
    /**
     * Indicates whether compute nodes should be used for non-HDFS operators.
     * The HDFS operator always prefers to use compute nodes even if {@code usedComputeNode} is false.
     */
    private boolean usedComputeNode;

    @VisibleForTesting
    public WorkerProvider(ImmutableMap<Long, ComputeNode> id2Backend, ImmutableMap<Long, ComputeNode> id2ComputeNode,
                          ImmutableMap<Long, ComputeNode> availableID2Backend,
                          ImmutableMap<Long, ComputeNode> availableID2ComputeNode,
                          boolean preferComputeNode) {
        this.id2Backend = id2Backend;
        this.id2ComputeNode = id2ComputeNode;

        this.availableID2Backend = availableID2Backend;
        this.availableID2ComputeNode = availableID2ComputeNode;

        this.usingWorkerIDs = Sets.newConcurrentHashSet();
        this.addr2UsingWorker = Maps.newHashMap();
        this.addr2HttpAddr = Maps.newHashMap();

        this.hasComputeNode = MapUtils.isNotEmpty(availableID2ComputeNode);
        this.usedComputeNode = hasComputeNode && preferComputeNode;
    }

    /**
     * Capture the available workers from {@code systemInfoService}, which are alive and not in the blacklist.
     *
     * @param systemInfoService   The service which provides all the backend nodes and compute nodes.
     * @param preferComputeNode   Whether to prefer using compute nodes over backend nodes.
     * @param numUsedComputeNodes The maximum number of used compute nodes.
     */
    public static WorkerProvider captureAvailableWorkers(SystemInfoService systemInfoService,
                                                         boolean preferComputeNode,
                                                         int numUsedComputeNodes) {
        ImmutableMap<Long, ComputeNode> idToComputeNode = buildComputeNodeInfo(systemInfoService, numUsedComputeNodes);
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

        return new WorkerProvider(idToBackend, idToComputeNode,
                filterAvailableWorkers(idToBackend), filterAvailableWorkers(idToComputeNode),
                preferComputeNode || RunMode.getCurrentRunMode() == RunMode.SHARED_DATA);
    }

    /**
     * Choose the specific backend node.
     *
     * @param backendID The ID of the backend node to choose.
     * @return The address with the {@code ComputeNode#bePort} of the backend node.
     * @throws UserException if there is no available backend with {@code backendID}.
     */
    public TNetworkAddress chooseBackend(Long backendID) throws UserException {
        ComputeNode backend = getBackend(backendID);
        backend = checkNotNull(backend, false);
        TNetworkAddress addr = backend.getAddress();
        recordUsedWorker(backend.getId(), addr);
        return addr;
    }

    /**
     * Choose the next worker node.
     *
     * @param workerIdRef storing ID of the worker node to choose.
     * @return The address with the {@code ComputeNode#bePort} of the worker node.
     * @throws UserException if there is no available backend with {@code backendID}.
     */
    public TNetworkAddress chooseNextWorker(Reference<Long> workerIdRef) throws UserException {
        ComputeNode worker;
        if (usedComputeNode) {
            worker = getNextWorker(availableID2ComputeNode, WorkerProvider::getNextComputeNodeIndex);
        } else {
            worker = getNextWorker(availableID2Backend, WorkerProvider::getNextBackendIndex);
        }

        worker = checkNotNull(worker, usedComputeNode);

        TNetworkAddress addr = worker.getAddress();
        recordUsedWorker(worker.getId(), addr);
        workerIdRef.setRef(worker.getId());
        return addr;
    }

    /**
     * Choose all the available compute nodes.
     *
     * @return The address with the {@code ComputeNode#bePort} of the compute nodes to choose.
     */
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

    public boolean containsBackend(Long backendID) {
        return getBackend(backendID) != null;
    }

    public ComputeNode getBackend(Long backendID) {
        return availableID2Backend.get(backendID);
    }

    public ComputeNode getWorker(Long workerID) {
        ComputeNode worker = availableID2Backend.get(workerID);
        if (worker != null) {
            return worker;
        }
        return availableID2ComputeNode.get(workerID);
    }

    public Collection<ComputeNode> getWorkersPreferringComputeNode() {
        if (hasComputeNode) {
            return availableID2ComputeNode.values();
        } else {
            return ImmutableList.copyOf(availableID2Backend.values());
        }
    }

    public boolean isUsingWorker(Long workerID) {
        return usingWorkerIDs.contains(workerID);
    }

    public ComputeNode getUsingWorkerByAddr(TNetworkAddress addr) {
        ComputeNode worker = addr2UsingWorker.get(addr);
        return Preconditions.checkNotNull(worker, "Expect backend address [{}] to being used", addr);
    }

    public TNetworkAddress getUsingHttpAddrByAddr(TNetworkAddress addr) {
        TNetworkAddress httpAddr = addr2HttpAddr.get(addr);
        return Preconditions.checkNotNull(httpAddr, "Expect backend address [{}] to being used", addr);
    }

    public List<Long> getUsingWorkerIDs() {
        return new ArrayList<>(usingWorkerIDs);
    }

    public Collection<TNetworkAddress> getUsingWorkerAddrs() {
        return addr2UsingWorker.keySet();
    }

    public void reportBackendNotFoundException() throws SchedulerException {
        reportWorkerNotFoundException(false);
    }

    private void reportWorkerNotFoundException(boolean chooseComputeNode) throws SchedulerException {
        throw new SchedulerException(
                FeConstants.getNodeNotFoundError(chooseComputeNode) + toString(chooseComputeNode));
    }

    @Override
    public String toString() {
        return toString(usedComputeNode);
    }

    public String toString(boolean chooseComputeNode) {
        return chooseComputeNode ? computeNodesToString() : backendsToString();
    }

    public String computeNodesToString() {
        StringBuilder out = new StringBuilder("compute node: ");
        id2ComputeNode.forEach((backendID, backend) -> out.append(
                String.format("[%s alive: %b inBlacklist: %b] ", backend.getHost(),
                        backend.isAlive(), SimpleScheduler.isInBlacklist(backendID))));
        return out.toString();
    }

    public String backendsToString() {
        StringBuilder out = new StringBuilder("backend: ");
        id2Backend.forEach((backendID, backend) -> out.append(
                String.format("[%s alive: %b inBlacklist: %b] ", backend.getHost(),
                        backend.isAlive(), SimpleScheduler.isInBlacklist(backendID))));
        return out.toString();
    }

    public void recordUsedWorker(Long workerID, TNetworkAddress addr) {
        if (usingWorkerIDs.add(workerID)) {
            ComputeNode worker = getWorker(workerID);
            addr2UsingWorker.put(addr, worker);
            addr2HttpAddr.put(addr, worker.getHttpAddress());
        }
    }

    @VisibleForTesting
    static int getNextComputeNodeIndex() {
        return NEXT_COMPUTE_NODE_INDEX.getAndIncrement();
    }

    @VisibleForTesting
    static int getNextBackendIndex() {
        return NEXT_BACKEND_INDEX.getAndIncrement();
    }

    private <T> T checkNotNull(T obj, boolean chooseComputeNode) throws UserException {
        if (obj == null) {
            reportWorkerNotFoundException(chooseComputeNode);
        }
        return obj;
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
                ComputeNode computeNode = getNextWorker(idToComputeNode, WorkerProvider::getNextComputeNodeIndex);
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
                                                           Supplier<Integer> getNextWorkerNodeIndex) {
        if (workers.isEmpty()) {
            return null;
        }
        int index = getNextWorkerNodeIndex.get() % workers.size();
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
