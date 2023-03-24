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
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.DataNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TScanRangeLocation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nullable;

public class SimpleScheduler {
    private static Logger LOG = LogManager.getLogger(SimpleScheduler.class);
    //count id for compute node get TNetworkAddress
    private static AtomicLong nextComputeNodeHostId = new AtomicLong(0);
    //count id for backend get TNetworkAddress
    private static AtomicLong nextDataNodeHostId = new AtomicLong(0);
    //count id for get ComputeNode
    private static Map<Long, Integer> blacklistDataNodes = Maps.newHashMap();
    private static Lock lock = new ReentrantLock();
    private static UpdateBlacklistThread updateBlacklistThread;

    static {
        updateBlacklistThread = new UpdateBlacklistThread();
        updateBlacklistThread.start();
    }

    @Nullable
    public static TNetworkAddress getHost(long backendId,
                                          List<TScanRangeLocation> locations,
                                          ImmutableMap<Long, DataNode> backends,
                                          Reference<Long> backendIdRef) {
        if (locations == null || backends == null) {
            return null;
        }
        LOG.debug("getHost backendID={}, backendSize={}", backendId, backends.size());
        DataNode backend = backends.get(backendId);
        lock.lock();
        try {
            if (backend != null && backend.isAlive() && !blacklistDataNodes.containsKey(backendId)) {
                backendIdRef.setRef(backendId);
                return new TNetworkAddress(backend.getHost(), backend.getBePort());
            } else {
                for (TScanRangeLocation location : locations) {
                    if (location.datanode_id == backendId) {
                        continue;
                    }
                    // choose the first alive backend(in analysis stage, the locations are random)
                    DataNode candidateDataNode = backends.get(location.datanode_id);
                    if (candidateDataNode != null && candidateDataNode.isAlive()
                            && !blacklistDataNodes.containsKey(location.datanode_id)) {
                        backendIdRef.setRef(location.datanode_id);
                        return new TNetworkAddress(candidateDataNode.getHost(), candidateDataNode.getBePort());
                    }
                }
            }
        } finally {
            lock.unlock();
        }
        // no backend returned
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
    public static TNetworkAddress getDataNodeHost(ImmutableMap<Long, DataNode> backendMap,
                                                 Reference<Long> backendIdRef) {
        DataNode node = getDataNode(backendMap);
        if (node != null) {
            backendIdRef.setRef(node.getId());
            return new TNetworkAddress(node.getHost(), node.getBePort());
        }
        return null;
    }

    @Nullable
    public static DataNode getDataNode(ImmutableMap<Long, DataNode> nodeMap) {
        if (nodeMap == null || nodeMap.isEmpty()) {
            return null;
        }
        return chooseNode(nodeMap.values().asList(), nextDataNodeHostId);
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
            if (node != null && node.isAlive() && !blacklistDataNodes.containsKey(node.getId())) {
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
            blacklistDataNodes.put(backendID, tryTime);
            LOG.warn("add black list " + backendID);
        } finally {
            lock.unlock();
        }
    }

    public static boolean isInBlacklist(long backendId) {
        lock.lock();
        try {
            return blacklistDataNodes.containsKey(backendId);
        } finally {
            lock.unlock();
        }
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
            while (true) {
                try {
                    Thread.sleep(1000L);
                    SystemInfoService clusterInfoService = GlobalStateMgr.getCurrentSystemInfo();
                    LOG.debug("UpdateBlacklistThread retry begin");
                    lock.lock();
                    try {
                        Iterator<Map.Entry<Long, Integer>> iterator = blacklistDataNodes.entrySet().iterator();
                        while (iterator.hasNext()) {
                            Map.Entry<Long, Integer> entry = iterator.next();
                            Long backendId = entry.getKey();

                            // remove from blacklist if
                            // 1. backend does not exist anymore
                            // 2. backend is alive
                            if (clusterInfoService.getDataNode(backendId) == null
                                    || clusterInfoService.checkDataNodeAvailable(backendId)) {
                                iterator.remove();
                                LOG.debug("remove backendID {} which is alive", backendId);
                            } else {
                                // 3. max try time is reach
                                Integer retryTimes = entry.getValue();
                                retryTimes = retryTimes - 1;
                                if (retryTimes <= 0) {
                                    iterator.remove();
                                    LOG.warn("remove backendID {}. reach max try time", backendId);
                                } else {
                                    entry.setValue(retryTimes);
                                    LOG.debug("blacklistDataNodes backendID={} retryTimes={}", backendId, retryTimes);
                                }
                            }
                        }
                    } finally {
                        lock.unlock();
                        LOG.debug("UpdateBlacklistThread retry end");
                    }

                } catch (Throwable ex) {
                    LOG.warn("blacklist thread exception" + ex);
                }
            }
        }
    }
}
