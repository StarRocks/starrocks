// This file is made available under Elastic License 2.0.
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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.Config;
import com.starrocks.common.Reference;
import com.starrocks.common.util.NetUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TScanRangeLocation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SimpleScheduler {
    //count id for compute node get TNetworkAddress
    private static AtomicLong nextComputeNodeHostId = new AtomicLong(0);
    //count id for backend get TNetworkAddress
    private static AtomicLong nextBackendHostId = new AtomicLong(0);
    //count id for get ComputeNode
    private static AtomicLong nextComputeNodeId = new AtomicLong(0);
    private static final Logger LOG = LogManager.getLogger(SimpleScheduler.class);

    private static Map<Long, Integer> blacklistBackends = Maps.newHashMap();
    private static Lock lock = new ReentrantLock();
    private static UpdateBlacklistThread updateBlacklistThread;

    static {
        updateBlacklistThread = new UpdateBlacklistThread();
        updateBlacklistThread.start();
    }

    public static TNetworkAddress getHost(long backendId,
                                          List<TScanRangeLocation> locations,
                                          ImmutableMap<Long, Backend> backends,
                                          Reference<Long> backendIdRef) {
        if (locations == null || backends == null) {
            return null;
        }
        LOG.debug("getHost backendID={}, backendSize={}", backendId, backends.size());
        Backend backend = backends.get(backendId);
        lock.lock();
        try {
            if (backend != null && backend.isAlive() && !blacklistBackends.containsKey(backendId)) {
                backendIdRef.setRef(backendId);
                return new TNetworkAddress(backend.getHost(), backend.getBePort());
            } else {
                for (TScanRangeLocation location : locations) {
                    if (location.backend_id == backendId) {
                        continue;
                    }
                    // choose the first alive backend(in analysis stage, the locations are random)
                    Backend candidateBackend = backends.get(location.backend_id);
                    if (candidateBackend != null && candidateBackend.isAlive()
                            && !blacklistBackends.containsKey(location.backend_id)) {
                        backendIdRef.setRef(location.backend_id);
                        return new TNetworkAddress(candidateBackend.getHost(), candidateBackend.getBePort());
                    }
                }
            }
        } finally {
            lock.unlock();
        }
        // no backend returned
        return null;
    }

    public static TNetworkAddress getComputeNodeHost(ImmutableMap<Long, ComputeNode> computenodes,
                                                     Reference<Long> computeNodeIdRef) {
        if (computenodes == null) {
            return null;
        }
        int computeNodedSize = computenodes.size();
        if (computeNodedSize == 0) {
            return null;
        }
        long id = nextComputeNodeHostId.getAndIncrement() % computeNodedSize;

        List<Long> idToComputeNodeId = Lists.newArrayList();
        idToComputeNodeId.addAll(computenodes.keySet());
        Long computeNodeId = idToComputeNodeId.get((int) id);
        ComputeNode computeNode = computenodes.get(computeNodeId);

        if (computeNode != null && computeNode.isAlive() && !blacklistBackends.containsKey(computeNodeId)) {
            computeNodeIdRef.setRef(computeNodeId);
            return new TNetworkAddress(computeNode.getHost(), computeNode.getBePort());
        } else {
            long candidateId = id + 1;  // get next candidate id
            for (int i = 0; i < computeNodedSize; i++) {
                LOG.debug("i={} candidatedId={}", i, candidateId);
                if (candidateId >= computeNodedSize) {
                    candidateId = 0;
                }
                if (candidateId == id) {
                    continue;
                }
                Long candidateComputeNodeId = idToComputeNodeId.get((int) candidateId);
                LOG.debug("candidatebackendId={}", candidateComputeNodeId);
                ComputeNode candidateBackend = computenodes.get(candidateComputeNodeId);
                if (candidateBackend != null && candidateBackend.isAlive()
                        && !blacklistBackends.containsKey(candidateComputeNodeId)) {
                    computeNodeIdRef.setRef(candidateComputeNodeId);
                    return new TNetworkAddress(candidateBackend.getHost(), candidateBackend.getBePort());
                }
                candidateId = nextComputeNodeId.getAndIncrement() % computeNodedSize;
            }
        }
        // no compute node returned
        return null;
    }

    public static TNetworkAddress getBackendHost(ImmutableMap<Long, Backend> backends,
                                          Reference<Long> backendIdRef) {
        if (backends == null) {
            return null;
        }
        int backendSize = backends.size();
        if (backendSize == 0) {
            return null;
        }
        long id = nextBackendHostId.getAndIncrement() % backendSize;

        List<Long> idToBackendId = Lists.newArrayList();
        idToBackendId.addAll(backends.keySet());
        Long backendId = idToBackendId.get((int) id);
        Backend backend = backends.get(backendId);

        if (backend != null && backend.isAlive() && !blacklistBackends.containsKey(backendId)) {
            backendIdRef.setRef(backendId);
            return new TNetworkAddress(backend.getHost(), backend.getBePort());
        } else {
            long candidateId = id + 1;  // get next candidate id
            for (int i = 0; i < backendSize; i++) {
                LOG.debug("i={} candidatedId={}", i, candidateId);
                if (candidateId >= backendSize) {
                    candidateId = 0;
                }
                if (candidateId == id) {
                    continue;
                }
                Long candidatebackendId = idToBackendId.get((int) candidateId);
                LOG.debug("candidatebackendId={}", candidatebackendId);
                Backend candidateBackend = backends.get(candidatebackendId);
                if (candidateBackend != null && candidateBackend.isAlive()
                        && !blacklistBackends.containsKey(candidatebackendId)) {
                    backendIdRef.setRef(candidatebackendId);
                    return new TNetworkAddress(candidateBackend.getHost(), candidateBackend.getBePort());
                }
                candidateId = nextBackendHostId.getAndIncrement() % backendSize;
            }
        }
        // no backend returned
        return null;
    }

    public static ComputeNode getComputeNode(ImmutableMap<Long, ComputeNode> computeNodes) {
        if (computeNodes == null) {
            return null;
        }
        int computeNodeSize = computeNodes.size();
        if (computeNodeSize == 0) {
            return null;
        }
        long id = nextComputeNodeId.getAndIncrement() % computeNodeSize;

        List<Long> idToComputeNodeId = Lists.newArrayList();
        idToComputeNodeId.addAll(computeNodes.keySet());
        Long computeNodeId = idToComputeNodeId.get((int) id);
        ComputeNode computeNode = computeNodes.get(computeNodeId);

        if (computeNode != null && computeNode.isAlive() && !blacklistBackends.containsKey(computeNodeId)) {
            return computeNode;
        } else {
            long candidateId = nextComputeNodeId.getAndIncrement() % computeNodeSize;  // get next candidate id
            for (int i = 0; i < computeNodeSize; i++) {
                LOG.debug("i={} candidatedId={}", i, candidateId);
                if (candidateId == id) {
                    continue;
                }
                Long candidatebackendId = idToComputeNodeId.get((int) candidateId);
                LOG.debug("candidatebackendId={}", candidatebackendId);
                ComputeNode candidateBackend = computeNodes.get(candidatebackendId);
                if (candidateBackend != null && candidateBackend.isAlive()
                        && !blacklistBackends.containsKey(candidatebackendId)) {
                    return candidateBackend;
                }
                candidateId = nextComputeNodeId.getAndIncrement() % computeNodeSize;
            }
        }
        // no backend returned
        return null;
    }

    public static void addToBlacklist(Long backendID) {
        if (backendID == null) {
            return;
        }
        lock.lock();
        try {
            int tryTime = Config.heartbeat_timeout_second + 1;
            blacklistBackends.put(backendID, tryTime);
            LOG.warn("add black list " + backendID);
        } finally {
            lock.unlock();
        }
    }

    public static boolean isInBlacklist(long backendId) {
        lock.lock();
        try {
            return blacklistBackends.containsKey(backendId);
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
            return blacklistBackends.remove(backendID) != null;
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
                        Iterator<Map.Entry<Long, Integer>> iterator = blacklistBackends.entrySet().iterator();
                        while (iterator.hasNext()) {
                            Map.Entry<Long, Integer> entry = iterator.next();
                            Long backendId = entry.getKey();

                            // 1. If the backend is null, means that the backend has been removed.
                            // 2. check the all ports of the backend
                            // 3. retry three times
                            // If both of the above conditions are met, the backend is removed from the blacklist
                            Backend backend = clusterInfoService.getBackend(backendId);
                            if (backend == null) {
                                iterator.remove();
                                LOG.warn("remove backendID {} from blacklist", backendId);
                            } else if (clusterInfoService.checkBackendAvailable(backendId)) {
                                String host = backend.getHost();
                                List<Integer> ports = new ArrayList<Integer>();
                                Collections.addAll(ports, backend.getBePort(), backend.getBrpcPort(), backend.getHttpPort());
                                if (NetUtils.checkAccessibleForAllPorts(host, ports)) {
                                    iterator.remove();
                                    LOG.warn("remove backendID {} from blacklist", backendId);;
                                }
                            } else {
                                Integer retryTimes = entry.getValue();
                                retryTimes = retryTimes - 1;
                                if (retryTimes <= 0) {
                                    iterator.remove();
                                    LOG.warn("remove backendID {} from blacklist", backendId);
                                } else {
                                    entry.setValue(retryTimes);
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
