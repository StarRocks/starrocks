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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/TabletInvertedIndex.java

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

package com.starrocks.catalog;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.common.Config;
import com.starrocks.common.util.concurrent.QueryableReentrantReadWriteLock;
import com.starrocks.lake.LakeTablet;
import com.starrocks.memory.MemoryTrackable;
import com.starrocks.memory.estimate.Estimator;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TStorageMedium;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/*
 * this class stores an inverted index
 * key is tablet id. value is the related ids of this tablet
 * Checkpoint thread is no need to modify this inverted index, because this inverted index will not be written
 * into image, all metadata are in globalStateMgr, and the inverted index will be rebuilt when FE restart.
 */
public class TabletInvertedIndex implements MemoryTrackable {
    private static final Logger LOG = LogManager.getLogger(TabletInvertedIndex.class);

    public static final int NOT_EXIST_VALUE = -1;

    public static final TabletMeta NOT_EXIST_TABLET_META = new TabletMeta(NOT_EXIST_VALUE, NOT_EXIST_VALUE,
            NOT_EXIST_VALUE, NOT_EXIST_VALUE, TStorageMedium.HDD);

    private final QueryableReentrantReadWriteLock lock = new QueryableReentrantReadWriteLock();

    // tablet id -> tablet meta
    private final Map<Long, TabletMeta> tabletMetaMap = new Long2ObjectOpenHashMap<>();

    // replica id -> tablet id
    private final Map<Long, Long> replicaToTabletMap = new Long2LongOpenHashMap();

    // tablet id -> backend set
    private final Map<Long, Set<Long>> forceDeleteTablets = new Long2ObjectOpenHashMap<>();

    // tablet id -> (backend id -> replica)
    private final Map<Long, Map<Long, Replica>> replicaMetaTable = new Long2ObjectOpenHashMap<>();
    // backing replica table, for visiting backend replicas faster.
    // backend id -> (tablet id -> replica)
    private final Map<Long, Map<Long, Replica>> backingReplicaMetaTable = new Long2ObjectOpenHashMap<>();

    public TabletInvertedIndex() {
    }

    public void readLock() {
        lock.sharedLockDetectingSlowLock(Config.slow_lock_threshold_ms, TimeUnit.MILLISECONDS);
    }

    public void readUnlock() {
        lock.sharedUnlock();
    }

    private void writeLock() {
        lock.exclusiveLockDetectingSlowLock(Config.slow_lock_threshold_ms, TimeUnit.MILLISECONDS);
    }

    private void writeUnlock() {
        lock.exclusiveUnlock();
    }

    public Long getTabletIdByReplica(long replicaId) {
        readLock();
        try {
            return replicaToTabletMap.get(replicaId);
        } finally {
            readUnlock();
        }
    }

    public TabletMeta getTabletMeta(long tabletId) {
        readLock();
        try {
            return tabletMetaMap.get(tabletId);
        } finally {
            readUnlock();
        }
    }

    public List<TabletMeta> getTabletMetaList(List<Long> tabletIdList) {
        List<TabletMeta> tabletMetaList = new ArrayList<>(tabletIdList.size());
        readLock();
        try {
            for (Long tabletId : tabletIdList) {
                tabletMetaList.add(tabletMetaMap.getOrDefault(tabletId, NOT_EXIST_TABLET_META));
            }
            return tabletMetaList;
        } finally {
            readUnlock();
        }
    }

    // always add tablet before adding replicas
    public void addTablet(long tabletId, TabletMeta tabletMeta) {
        if (GlobalStateMgr.isCheckpointThread()) {
            return;
        }
        writeLock();
        try {
            tabletMetaMap.putIfAbsent(tabletId, tabletMeta);
            LOG.debug("add tablet: {} tabletMeta: {}", tabletId, tabletMeta);
        } finally {
            writeUnlock();
        }
    }

    @VisibleForTesting
    public Map<Long, Set<Long>> getForceDeleteTablets() {
        readLock();
        try {
            return forceDeleteTablets;
        } finally {
            readUnlock();
        }
    }

    public boolean tabletForceDelete(long tabletId, long backendId) {
        readLock();
        try {
            if (forceDeleteTablets.containsKey(tabletId)) {
                return forceDeleteTablets.get(tabletId).contains(backendId);
            }
            return false;
        } finally {
            readUnlock();
        }
    }

    public void markTabletForceDelete(long tabletId, long backendId) {
        writeLock();
        try {
            if (forceDeleteTablets.containsKey(tabletId)) {
                forceDeleteTablets.get(tabletId).add(backendId);
            } else {
                forceDeleteTablets.put(tabletId, Sets.newHashSet(backendId));
            }
        } finally {
            writeUnlock();
        }
    }

    public void markTabletForceDelete(long tabletId, Set<Long> backendIds) {
        if (backendIds.isEmpty()) {
            return;
        }
        writeLock();
        try {
            forceDeleteTablets.put(tabletId, backendIds);
        } finally {
            writeUnlock();
        }
    }

    public void markTabletForceDelete(Tablet tablet) {
        // LakeTablet is managed by StarOS, no need to do this mark and clean up
        if (tablet instanceof LakeTablet) {
            return;
        }
        markTabletForceDelete(tablet.getId(), tablet.getBackendIds());
    }

    public void eraseTabletForceDelete(long tabletId, long backendId) {
        writeLock();
        try {
            if (forceDeleteTablets.containsKey(tabletId)) {
                forceDeleteTablets.get(tabletId).remove(backendId);
                if (forceDeleteTablets.get(tabletId).isEmpty()) {
                    forceDeleteTablets.remove(tabletId);
                }
            }
        } finally {
            writeUnlock();
        }
    }

    public void deleteTablet(long tabletId) {
        if (GlobalStateMgr.isCheckpointThread()) {
            return;
        }
        writeLock();
        try {
            Map<Long, Replica> replicas = replicaMetaTable.remove(tabletId);
            if (replicas != null) {
                for (Replica replica : replicas.values()) {
                    replicaToTabletMap.remove(replica.getId());
                }

                for (long backendId : replicas.keySet()) {
                    removeReplica(backingReplicaMetaTable, backendId, tabletId);
                }
            }
            tabletMetaMap.remove(tabletId);

            LOG.debug("delete tablet: {}", tabletId);
        } finally {
            writeUnlock();
        }
    }

    // Only for test
    public Map<Long, Map<Long, Replica>> getReplicaMetaTable() {
        return replicaMetaTable;
    }

    public void addReplica(long tabletId, Replica replica) {
        if (GlobalStateMgr.isCheckpointThread()) {
            return;
        }
        writeLock();
        try {
            Preconditions.checkState(tabletMetaMap.containsKey(tabletId));
            setReplica(replicaMetaTable, tabletId, replica.getBackendId(), replica);
            replicaToTabletMap.put(replica.getId(), tabletId);
            setReplica(backingReplicaMetaTable, replica.getBackendId(), tabletId, replica);
            LOG.debug("add replica {} of tablet {} in backend {}",
                    replica.getId(), tabletId, replica.getBackendId());
        } finally {
            writeUnlock();
        }
    }

    public void deleteReplica(long tabletId, long backendId) {
        if (GlobalStateMgr.isCheckpointThread()) {
            return;
        }
        writeLock();
        try {
            if (!tabletMetaMap.containsKey(tabletId)) {
                return;
            }
            if (replicaMetaTable.containsKey(tabletId)) {
                Replica replica = removeReplica(replicaMetaTable, tabletId, backendId);
                Preconditions.checkState(replica != null);
                replicaToTabletMap.remove(replica.getId());
                removeReplica(backingReplicaMetaTable, backendId, tabletId);
                LOG.debug("delete replica {} of tablet {} in backend {}",
                        replica.getId(), tabletId, backendId);
            } else {
                // this may happen when fe restart after tablet is empty(bug cause)
                // add log instead of assertion to observe
                LOG.error("tablet[{}] contains no replica in inverted index", tabletId);
            }
        } finally {
            writeUnlock();
        }
    }

    public Replica getReplica(long tabletId, long backendId) {
        readLock();
        try {
            return getReplica(replicaMetaTable, tabletId, backendId);
        } finally {
            readUnlock();
        }
    }

    public List<Replica> getReplicasByTabletId(long tabletId) {
        readLock();
        try {
            if (replicaMetaTable.containsKey(tabletId)) {
                return Lists.newArrayList(replicaMetaTable.get(tabletId).values());
            }
            return Lists.newArrayList();
        } finally {
            readUnlock();
        }
    }

    /**
     * For each tabletId in the tablet_id list, get the replica on specified backend or null, return as a list.
     *
     * @param tabletIds tablet_id list
     * @param backendId backendid
     * @return list of replica or null if backend not found
     */
    public List<Replica> getReplicasOnBackendByTabletIds(List<Long> tabletIds, long backendId) {
        readLock();
        try {
            Map<Long, Replica> replicaMetaWithBackend = row(backingReplicaMetaTable, backendId);
            if (!replicaMetaWithBackend.isEmpty()) {
                List<Replica> replicas = Lists.newArrayList();
                for (long tabletId : tabletIds) {
                    replicas.add(replicaMetaWithBackend.get(tabletId));
                }
                return replicas;
            }
            return null;
        } finally {
            readUnlock();
        }
    }

    public List<Long> getTabletIdsByBackendId(long backendId) {
        List<Long> tabletIds = Lists.newArrayList();
        readLock();
        try {
            Map<Long, Replica> replicaMetaWithBackend = row(backingReplicaMetaTable, backendId);
            tabletIds.addAll(replicaMetaWithBackend.keySet());
        } finally {
            readUnlock();
        }
        return tabletIds;
    }

    public List<Long> getTabletIdsByBackendIdAndStorageMedium(long backendId, TStorageMedium storageMedium) {
        readLock();
        try {
            Map<Long, Replica> replicaMetaWithBackend = row(backingReplicaMetaTable, backendId);
            return replicaMetaWithBackend.keySet().stream()
                    .filter(id -> tabletMetaMap.get(id).getStorageMedium() == storageMedium)
                    .collect(Collectors.toList());
        } finally {
            readUnlock();
        }
    }

    public long getTabletNumByBackendId(long backendId) {
        readLock();
        try {
            return row(backingReplicaMetaTable, backendId).size();
        } finally {
            readUnlock();
        }
    }

    /**
     * Get the number of tablets on the specified backend and pathHash
     * @param backendId the ID of the backend
     * @param pathHash the hash of the path
     * @return the number of tablets as a long value
     *
     * @implNote Linear scan, invoke this interface with caution if the number of replicas is large
     */
    public long getTabletNumByBackendIdAndPathHash(long backendId, long pathHash) {
        readLock();
        try {
            Map<Long, Replica> replicaMetaWithBackend = row(backingReplicaMetaTable, backendId);
            return replicaMetaWithBackend.values().stream().filter(r -> r.getPathHash() == pathHash).count();
        } finally {
            readUnlock();
        }
    }

    /**
     * Get the number of tablets on the specified backend, grouped by pathHash
     * @param backendId the ID of the backend
     * @return Map<pathHash, tabletNum> the number of tablets grouped by pathHash
     *
     * @implNote Linear scan, invoke this interface with caution if the number of replicas is large
     */
    public Map<Long, Long> getTabletNumByBackendIdGroupByPathHash(long backendId) {
        Map<Long, Long> pathHashToTabletNum = Maps.newHashMap();
        readLock();
        try {
            Map<Long, Replica> replicaMetaWithBackend = row(backingReplicaMetaTable, backendId);
            for (Replica r : replicaMetaWithBackend.values()) {
                pathHashToTabletNum.compute(r.getPathHash(), (k, v) -> v == null ? 1L : v + 1);
            }
        } finally {
            readUnlock();
        }
        return pathHashToTabletNum;
    }

    public Map<TStorageMedium, Long> getReplicaNumByBeIdAndStorageMedium(long backendId) {
        Map<TStorageMedium, Long> replicaNumMap = Maps.newHashMap();
        long hddNum = 0;
        long ssdNum = 0;
        readLock();
        try {
            Map<Long, Replica> replicaMetaWithBackend = row(backingReplicaMetaTable, backendId);
            for (long tabletId : replicaMetaWithBackend.keySet()) {
                if (tabletMetaMap.get(tabletId).getStorageMedium() == TStorageMedium.HDD) {
                    hddNum++;
                } else {
                    ssdNum++;
                }
            }
        } finally {
            readUnlock();
        }
        replicaNumMap.put(TStorageMedium.HDD, hddNum);
        replicaNumMap.put(TStorageMedium.SSD, ssdNum);
        return replicaNumMap;
    }

    public long getTabletCount() {
        readLock();
        try {
            return this.tabletMetaMap.size();
        } finally {
            readUnlock();
        }
    }

    public long getReplicaCount() {
        readLock();
        try {
            return this.replicaToTabletMap.size();
        } finally {
            readUnlock();
        }
    }

    public Map<Long, Replica> getReplicas(long tabletId) {
        readLock();
        try {
            return this.replicaMetaTable.get(tabletId);
        } finally {
            readUnlock();
        }
    }

    // The caller should hold readLock.
    public Map<Long, Replica> getReplicaMetaWithBackend(Long backendId) {
        return row(backingReplicaMetaTable, backendId);
    }

    // just for test
    public void clear() {
        writeLock();
        try {
            tabletMetaMap.clear();
            replicaToTabletMap.clear();
            replicaMetaTable.clear();
            backingReplicaMetaTable.clear();
        } finally {
            writeUnlock();
        }
    }

    @Override
    public Map<String, Long> estimateCount() {
        return ImmutableMap.of("TabletMeta", getTabletCount(),
                               "TabletCount", getTabletCount(),
                               "ReplicateCount", getReplicaCount());
    }

    @Override
    public long estimateSize() {
        readLock();
        try {
            return Estimator.estimate(tabletMetaMap) +
                   Estimator.estimate(replicaToTabletMap) +
                   Estimator.estimate(replicaMetaTable) +
                   Estimator.estimate(backingReplicaMetaTable);
        } finally {
            readUnlock();
        }
    }

    private static Replica getReplica(Map<Long, Map<Long, Replica>> table, long rowKey, long columnKey) {
        if (table.containsKey(rowKey)) {
            return table.get(rowKey).get(columnKey);
        }
        return null;
    }

    private static void setReplica(Map<Long, Map<Long, Replica>> table, long rowKey, long columnKey, Replica replica) {
        if (table.containsKey(rowKey)) {
            table.get(rowKey).put(columnKey, replica);
        } else {
            Map<Long, Replica> column = new Long2ObjectOpenHashMap<>();
            column.put(columnKey, replica);
            table.put(rowKey, column);
        }
    }

    private static Replica removeReplica(Map<Long, Map<Long, Replica>> table, long rowKey, long columnKey) {
        if (table.containsKey(rowKey)) {
            Map<Long, Replica> row = table.get(rowKey);
            Replica replica = row.remove(columnKey);
            if (row.isEmpty()) {
                table.remove(rowKey);
            }
            return replica;
        }
        return null;
    }

    private static Map<Long, Replica> row(Map<Long, Map<Long, Replica>> table, long rowKey) {
        Map<Long, Replica> row = table.get(rowKey);
        if (row == null) {
            row = new Long2ObjectOpenHashMap<>();
        }
        return row;
    }
}
