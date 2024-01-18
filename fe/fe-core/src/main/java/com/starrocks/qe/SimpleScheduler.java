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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/qe/SimpleScheduler.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.qe;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.starrocks.common.Config;
import com.starrocks.common.Reference;
import com.starrocks.common.util.NetUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TScanRangeLocation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class SimpleScheduler {
    private static Logger LOG = LogManager.getLogger(SimpleScheduler.class);
    //count id for compute node get TNetworkAddress
    private static AtomicLong nextComputeNodeHostId = new AtomicLong(0);
    //count id for backend get TNetworkAddress
    private static AtomicLong nextBackendHostId = new AtomicLong(0);
    //count id for get ComputeNode
    private static Map<Long, Integer> blacklistNodes = Maps.newHashMap();
    private static Lock lock = new ReentrantLock();
    private static UpdateBlacklistThread updateBlacklistThread;
    private static AtomicBoolean enableUpdateBlacklistThread;

    static {
        enableUpdateBlacklistThread = new AtomicBoolean(true);
        updateBlacklistThread = new UpdateBlacklistThread();
        updateBlacklistThread.start();
    }

    @Nullable
    public static TNetworkAddress getHost(long nodeId,
                                          List<TScanRangeLocation> locations,
                                          ImmutableMap<Long, ComputeNode> computeNodes,
                                          Reference<Long> nodeIdRef) {

        if (locations == null || computeNodes == null) {
            return null;
        }
        LOG.debug("getHost nodeID={}, nodeSize={}", nodeId, computeNodes.size());

        ComputeNode node = computeNodes.get(nodeId);

        lock.lock();
        try {
            if (node != null && node.isAlive() && !blacklistNodes.containsKey(nodeId)) {
                nodeIdRef.setRef(nodeId);
                return new TNetworkAddress(node.getHost(), node.getBePort());
            } else {
                for (TScanRangeLocation location : locations) {
                    if (location.backend_id == nodeId) {
                        continue;
                    }
                    // choose the first alive backend(in analysis stage, the locations are random)
                    ComputeNode candidateNode = computeNodes.get(location.backend_id);
                    if (candidateNode != null && candidateNode.isAlive()
                            && !blacklistNodes.containsKey(location.backend_id)) {
                        nodeIdRef.setRef(location.backend_id);
                        return new TNetworkAddress(candidateNode.getHost(), candidateNode.getBePort());
                    }
                }

                // In shared data mode, we can select any alive node to replace the original dead node for query
                if (RunMode.getCurrentRunMode() == RunMode.SHARED_DATA) {
                    List<ComputeNode> allNodes = new ArrayList<>(computeNodes.size());
                    allNodes.addAll(computeNodes.values());
                    List<ComputeNode> candidateNodes = allNodes.stream()
                            .filter(x -> x.getId() != nodeId && x.isAlive() &&
                                    !blacklistNodes.containsKey(x.getId())).collect(Collectors.toList());
                    if (!candidateNodes.isEmpty()) {
                        // use modulo operation to ensure that the same node is selected for the dead node
                        ComputeNode candidateNode = candidateNodes.get((int) (nodeId % candidateNodes.size()));
                        nodeIdRef.setRef(candidateNode.getId());
                        return new TNetworkAddress(candidateNode.getHost(), candidateNode.getBePort());
                    }
                }
            }
        } finally {
            lock.unlock();
        }
        // no backend or compute node returned
        return null;
    }

    @Nullable
    public static TNetworkAddress getComputeNodeHost(ImmutableMap<Long, ComputeNode> computeNodes,
                                                     Reference<Long> computeNodeIdRef) {
        ComputeNode node = getComputeNode(computeNodes);
        if (node != null) {
            computeNodeIdRef.setRef(node.getId());
            return new TNetworkAddress(node.getHost(), node.getBePort());
        }
        return null;
    }

    @Nullable
    public static TNetworkAddress getBackendHost(ImmutableMap<Long, ComputeNode> backendMap,
                                                 Reference<Long> backendIdRef) {
        ComputeNode node = getBackend(backendMap);
        if (node != null) {
            backendIdRef.setRef(node.getId());
            return new TNetworkAddress(node.getHost(), node.getBePort());
        }
        return null;
    }

    @Nullable
    public static ComputeNode getBackend(ImmutableMap<Long, ComputeNode> nodeMap) {
        if (nodeMap == null || nodeMap.isEmpty()) {
            return null;
        }
        return chooseNode(nodeMap.values().asList(), nextBackendHostId);
    }

    @Nullable
    public static ComputeNode getComputeNode(ImmutableMap<Long, ComputeNode> nodeMap) {
        if (nodeMap == null || nodeMap.isEmpty()) {
            return null;
        }
        return chooseNode(nodeMap.values().asList(), nextComputeNodeHostId);
    }

    @Nullable
    private static <T extends ComputeNode> T chooseNode(ImmutableList<T> nodes, AtomicLong nextId) {
        long id = nextId.getAndIncrement();
        for (int i = 0; i < nodes.size(); i++) {
            T node = nodes.get((int) (id % nodes.size()));
            if (node != null && node.isAlive() && !blacklistNodes.containsKey(node.getId())) {
                nextId.addAndGet(i); // skip failed nodes
                return node;
            }
            id++;
        }
        return null;
    }

    public static void addToBlacklist(Long backendID) {
        if (backendID == null) {
            return;
        }
        lock.lock();
        try {
            int tryTime = Config.heartbeat_timeout_second + 1;
            blacklistNodes.put(backendID, tryTime);
            LOG.warn("add black list " + backendID);
        } finally {
            lock.unlock();
        }
    }

    public static boolean isInBlacklist(long backendId) {
        lock.lock();
        try {
            return blacklistNodes.containsKey(backendId);
        } finally {
            lock.unlock();
        }
    }

    // The function is used for unit test
    public static boolean removeFromBlacklist(Long backendID) {
        if (backendID == null) {
            return true;
        }
        lock.lock();
        try {
            return blacklistNodes.remove(backendID) != null;
        } finally {
            lock.unlock();
        }
    }

    public static void updateBlacklist() {
        SystemInfoService clusterInfoService = GlobalStateMgr.getCurrentSystemInfo();

        lock.lock();
        Map<Long, Integer> blackListBackendsCopy = new HashMap<>(blacklistNodes);
        lock.unlock();

        List<Long> removedNodes = new ArrayList<>();
        Map<Long, Integer> retryingNodes = new HashMap<>();

        Iterator<Map.Entry<Long, Integer>> iterator = blackListBackendsCopy.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, Integer> entry = iterator.next();
            Long nodeId = entry.getKey();

            // 1. If the node is null, means that the node has been removed.
            // 2. check the all ports of the node
            // 3. retry Config.heartbeat_timeout_second + 1 times
            // If both of the above conditions are met, the node is removed from the blacklist
            ComputeNode node = clusterInfoService.getBackendOrComputeNode(nodeId);
            if (node == null) {
                removedNodes.add(nodeId);
                LOG.warn("remove nodeID {} from blacklist", nodeId);
            } else if (clusterInfoService.checkNodeAvailable(node)) {
                String host = node.getHost();
                List<Integer> ports = new ArrayList<Integer>();
                Collections.addAll(ports, node.getBePort(), node.getBrpcPort(), node.getHttpPort());
                if (NetUtils.checkAccessibleForAllPorts(host, ports)) {
                    removedNodes.add(nodeId);
                    LOG.warn("remove nodeID {} from blacklist", nodeId);
                }
            } else {
                Integer retryTimes = entry.getValue();
                retryTimes = retryTimes - 1;
                if (retryTimes <= 0) {
                    removedNodes.add(nodeId);
                    LOG.warn("remove nodeID {} from blacklist", nodeId);
                } else {
                    retryingNodes.put(nodeId, retryTimes);
                }
            }
        }

        lock.lock();
        try {
            // remove nodes.
            for (Long backendId : removedNodes) {
                blacklistNodes.remove(backendId);
            }

            // update the retry times.
            for (Map.Entry<Long, Integer> entry : retryingNodes.entrySet()) {
                if (blacklistNodes.containsKey(entry.getKey())) {
                    blacklistNodes.put(entry.getKey(), entry.getValue());
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public static void disableUpdateBlacklistThread() {
        enableUpdateBlacklistThread.set(false);
    }

    private static class UpdateBlacklistThread implements Runnable {
        private static final Logger LOG = LogManager.getLogger(UpdateBlacklistThread.class);
        private static Thread thread;

        public UpdateBlacklistThread() {
            thread = new Thread(this, "UpdateBlacklistThread");
            thread.setDaemon(true);
        }

        public void start() {
            thread.start();
        }

        @Override
        public void run() {
            LOG.debug("UpdateBlacklistThread is start to run");
            while (enableUpdateBlacklistThread.get()) {
                try {
                    Thread.sleep(1000L);
                    LOG.debug("UpdateBlacklistThread retry begin");
                    updateBlacklist();
                    LOG.debug("UpdateBlacklistThread retry end");

                } catch (Throwable ex) {
                    LOG.warn("blacklist thread exception" + ex);
                }
            }
        }
    }
}
