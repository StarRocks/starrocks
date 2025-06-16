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

package com.starrocks.qe;

import com.google.api.client.util.Sets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.Config;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.common.util.NetUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

// NOTE: unit tests are all in SimpleSchedulerTest
public class HostBlacklist {
    private static final Logger LOG = LogManager.getLogger(SimpleScheduler.class);
    private static final int HISTORY_SIZE = 1000;

    private final UpdateBlacklistThread updateBlacklistThread = new UpdateBlacklistThread();
    private final AtomicBoolean enableUpdateBlacklistThread = new AtomicBoolean(true);

    // hostId -> current DisconnectEvent
    private final Map<Long, DisconnectEvent> hostBlacklist = Maps.newConcurrentMap();

    // by timeline, thread unsafe
    private final LinkedList<DisconnectEvent> eventHistory = new LinkedList<>();
    // lock history
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    public void disableAutoUpdate() {
        enableUpdateBlacklistThread.set(false);
    }

    public void addByManual(Long hostId) {
        if (hostId == null) {
            return;
        }

        DisconnectEvent de = new DisconnectEvent(hostId, LocalDateTime.now(), DisconnectEvent.TYPE_MANUAL);
        // manual disconnect can overwrite auto disconnect
        hostBlacklist.put(hostId, de);
        LOG.warn("manual add black list: " + hostId + ", at: " + de.disconnectTime);
    }

    public void add(Long hostId) {
        if (hostId == null) {
            return;
        }

        DisconnectEvent de = new DisconnectEvent(hostId, LocalDateTime.now(), DisconnectEvent.TYPE_AUTO);

        // auto disconnect can't overwrite manual disconnect
        hostBlacklist.compute(hostId, (k, v) -> v != null && v.type == DisconnectEvent.TYPE_MANUAL ? v : de);

        rwLock.writeLock().lock();
        try {
            if (eventHistory.size() > HISTORY_SIZE) {
                eventHistory.removeFirst();
            }
            eventHistory.addLast(de);
        } finally {
            rwLock.writeLock().unlock();
        }
        LOG.warn("add black list: " + hostId + ", at: " + de.disconnectTime);
    }

    public boolean contains(long nodeId) {
        return hostBlacklist.containsKey(nodeId);
    }

    public boolean remove(Long hostId) {
        if (hostId == null) {
            return true;
        }

        if (hostBlacklist.remove(hostId) != null) {
            LOG.warn("remove black list: " + hostId + ", at: " + LocalDateTime.now());
            return true;
        }
        return false;
    }

    // Mostly for TEST purpose
    public void clear() {
        hostBlacklist.clear();
        eventHistory.clear();
    }

    public List<List<String>> getShowData() {
        List<DisconnectEvent> allEvents = Lists.newArrayList();
        rwLock.readLock().lock();
        try {
            // copy
            allEvents.addAll(eventHistory);
        } finally {
            rwLock.readLock().unlock();
        }

        List<List<String>> result = Lists.newArrayList();
        for (DisconnectEvent value : hostBlacklist.values()) {
            List<String> row = Lists.newArrayList();
            long count = allEvents.stream().filter(e -> e.hostId == value.hostId).count();

            row.add(String.valueOf(value.hostId));
            row.add(value.type == DisconnectEvent.TYPE_AUTO ? "AUTO" : "MANUAL");
            row.add(value.disconnectTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
            row.add(String.valueOf(count));
            row.add(String.valueOf(Config.black_host_connect_failures_within_time));
            result.add(row);
        }
        return result;
    }

    private void updateHistory() {
        LocalDateTime deadline = LocalDateTime.now().minusSeconds(Config.black_host_history_sec);
        LOG.debug("updateHistory, deadline: {}", deadline);
        rwLock.writeLock().lock();
        try {
            int size = eventHistory.size();
            Iterator<DisconnectEvent> iter = eventHistory.iterator();
            while (iter.hasNext()) {
                DisconnectEvent bh = iter.next();
                if (bh.disconnectTime.isBefore(deadline) || size > HISTORY_SIZE) {
                    iter.remove();
                    size--;
                } else {
                    break;
                }
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    private List<DisconnectEvent> getHistories(Set<Long> nodeIds) {
        if (nodeIds == null || nodeIds.isEmpty()) {
            return Collections.emptyList();
        }

        rwLock.readLock().lock();
        try {
            return eventHistory.stream().filter(f -> nodeIds.contains(f.hostId)).collect(Collectors.toList());
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public void refresh() {
        updateHistory();

        SystemInfoService clusterInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();

        List<Long> offlineNode = Lists.newArrayList();
        Set<Long> reconnectNode = Sets.newHashSet();

        for (Map.Entry<Long, DisconnectEvent> entry : hostBlacklist.entrySet()) {
            Long nodeId = entry.getKey();
            ComputeNode node = clusterInfoService.getBackendOrComputeNode(nodeId);
            if (node == null) {
                // Unknown node, the node must be removed from the system
                offlineNode.add(nodeId);
                continue;
            }

            LocalDateTime penaltyEndTime =
                    entry.getValue().disconnectTime.plusNanos(Config.black_host_penalty_min_ms * 1000_000);
            if (penaltyEndTime.isAfter(LocalDateTime.now())) {
                // penaltyEndTime > now()
                // It is not long enough to stay in the blocklist, keep it in the blocklist
                // Avoid the node enter and exit the blocklist too quick.
                continue;
            }

            // Check all the ports, determine if the BE node is recovered
            if (clusterInfoService.checkNodeAvailable(node) && entry.getValue().type == DisconnectEvent.TYPE_AUTO) {
                String host = node.getHost();
                List<Integer> ports = Lists.newArrayList();
                Collections.addAll(ports, node.getBePort(), node.getBrpcPort(), node.getHttpPort());
                if (NetUtils.checkAccessibleForAllPorts(host, ports)) {
                    reconnectNode.add(nodeId);
                }
            }
        }

        // remove nodes.
        for (Long nodeId : offlineNode) {
            remove(nodeId);
            LOG.warn("nodeID {} is offline, remove nodeID {} from blacklist", nodeId, nodeId);
        }

        // update the retry times.
        List<DisconnectEvent> histories = getHistories(reconnectNode);
        for (Long nodeId : reconnectNode) {
            long count = histories.stream().filter(f -> f.hostId == nodeId).count();
            if (count < Config.black_host_connect_failures_within_time) {
                remove(nodeId);
                LOG.warn("remove nodeID {} from blacklist", nodeId);
            } else {
                LOG.warn("nodeID {} more than {} disconnections with in the last {}s, will remain in the blacklist",
                        nodeId, Config.black_host_connect_failures_within_time, Config.black_host_history_sec);
            }
        }
    }

    public void startAutoUpdate() {
        updateBlacklistThread.start();
    }

    private class UpdateBlacklistThread extends FrontendDaemon {
        public UpdateBlacklistThread() {
            super("UpdateBlacklistThread", 1000);
        }

        @Override
        protected void runAfterCatalogReady() {
            if (!enableUpdateBlacklistThread.get()) {
                return;
            }
            LOG.debug("UpdateBlacklistThread retry begin");
            refresh();
            LOG.debug("UpdateBlacklistThread retry end");
        }
    }

    private static class DisconnectEvent {
        public static final int TYPE_AUTO = 0;
        public static final int TYPE_MANUAL = 1;

        public final long hostId;
        public final int type;
        public final LocalDateTime disconnectTime;

        public DisconnectEvent(long hostId, LocalDateTime lastDisconnectTime, int type) {
            this.hostId = hostId;
            this.disconnectTime = lastDisconnectTime;
            this.type = type;
        }

        @Override
        public String toString() {
            return "DisconnectEvent{" +
                    "hostId=" + hostId +
                    ", type=" + type +
                    ", disconnectTime=" + disconnectTime +
                    '}';
        }
    }
}
