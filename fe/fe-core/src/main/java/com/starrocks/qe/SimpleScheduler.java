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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.starrocks.common.Reference;
import com.starrocks.server.RunMode;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TScanRangeLocation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class SimpleScheduler {
    private static final Logger LOG = LogManager.getLogger(SimpleScheduler.class);
    //count id for compute node get TNetworkAddress
    private static final AtomicLong NEXT_COMPUTE_NODE_HOST_ID = new AtomicLong(0);
    //count id for backend get TNetworkAddress
    private static final AtomicLong NEXT_BACKEND_HOST_ID = new AtomicLong(0);

    private static final HostBlacklist HOST_BLACKLIST = new HostBlacklist();

    static {
        HOST_BLACKLIST.startAutoUpdate();
    }

    public static HostBlacklist getHostBlacklist() {
        return HOST_BLACKLIST;
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
        if (node != null && node.isAlive() && !HOST_BLACKLIST.contains(nodeId)) {
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
                        && !HOST_BLACKLIST.contains(location.backend_id)) {
                    nodeIdRef.setRef(location.backend_id);
                    return new TNetworkAddress(candidateNode.getHost(), candidateNode.getBePort());
                }
            }

            // In shared data mode, we can select any alive node to replace the original dead node for query
            if (RunMode.isSharedDataMode()) {
                List<ComputeNode> allNodes = new ArrayList<>(computeNodes.size());
                allNodes.addAll(computeNodes.values());
                List<ComputeNode> candidateNodes = allNodes.stream()
                        .filter(x -> x.getId() != nodeId && x.isAlive() &&
                                !HOST_BLACKLIST.contains(x.getId())).collect(Collectors.toList());
                if (!candidateNodes.isEmpty()) {
                    // use modulo operation to ensure that the same node is selected for the dead node
                    ComputeNode candidateNode = candidateNodes.get((int) (nodeId % candidateNodes.size()));
                    nodeIdRef.setRef(candidateNode.getId());
                    return new TNetworkAddress(candidateNode.getHost(), candidateNode.getBePort());
                }
            }
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
        return chooseNode(nodeMap.values().asList(), NEXT_BACKEND_HOST_ID);
    }

    @Nullable
    public static ComputeNode getComputeNode(ImmutableMap<Long, ComputeNode> nodeMap) {
        if (nodeMap == null || nodeMap.isEmpty()) {
            return null;
        }
        return chooseNode(nodeMap.values().asList(), NEXT_COMPUTE_NODE_HOST_ID);
    }

    @Nullable
    private static <T extends ComputeNode> T chooseNode(ImmutableList<T> nodes, AtomicLong nextId) {
        long id = nextId.getAndIncrement();
        for (int i = 0; i < nodes.size(); i++) {
            T node = nodes.get((int) (id % nodes.size()));
            if (node != null && node.isAlive() && !HOST_BLACKLIST.contains(node.getId())) {
                nextId.addAndGet(i); // skip failed nodes
                return node;
            }
            id++;
        }
        return null;
    }

    public static void addToBlocklist(Long backendID) {
        HOST_BLACKLIST.add(backendID);
    }

    public static boolean isInBlocklist(long backendId) {
        return HOST_BLACKLIST.contains(backendId);
    }

    // The function is used for unit test
    @VisibleForTesting
    public static boolean removeFromBlocklist(Long backendID) {
        return HOST_BLACKLIST.remove(backendID);
    }

    public static void disableUpdateBlocklistThread() {
        HOST_BLACKLIST.disableAutoUpdate();
    }
}
