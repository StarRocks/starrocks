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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.util.concurrent.ConcurrentLong2ObjectHashMap;
import com.starrocks.lake.LakeTablet;
import com.starrocks.memory.MemoryTrackable;
import com.starrocks.memory.estimate.Estimator;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TStorageMedium;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;

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

    // tablet id -> tablet meta
    private final ConcurrentLong2ObjectHashMap<TabletMeta> tabletMetaMap = new ConcurrentLong2ObjectHashMap<>();

    // tablet id -> replicas
    private final ConcurrentLong2ObjectHashMap<CopyOnWriteArrayList<Replica>> tabletToReplicaList =
            new ConcurrentLong2ObjectHashMap<>();

    // backend id -> tablet id list
    private final ConcurrentLong2ObjectHashMap<Set<Long>> backendToTabletIdList =
            new ConcurrentLong2ObjectHashMap<>();

    // tablet id -> backend set
    private final ConcurrentLong2ObjectHashMap<Set<Long>> forceDeleteTablets = new ConcurrentLong2ObjectHashMap<>();

    // Protects multi-map mutations; readers rely on higher level synchronization.
    private final ReentrantLock mutationLock = new ReentrantLock();

    private void writeLock() {
        mutationLock.lock();
    }

    private void writeUnlock() {
        mutationLock.unlock();
    }

    public TabletInvertedIndex() {
    }

    public TabletMeta getTabletMeta(long tabletId) {
        return tabletMetaMap.get(tabletId);
    }

    public List<TabletMeta> getTabletMetaList(List<Long> tabletIdList) {
        List<TabletMeta> tabletMetaList = new ArrayList<>(tabletIdList.size());
        for (Long tabletId : tabletIdList) {
            tabletMetaList.add(tabletMetaMap.getOrDefault(tabletId, NOT_EXIST_TABLET_META));
        }
        return tabletMetaList;
    }

    // always add tablet before adding replicas
    public void addTablet(long tabletId, TabletMeta tabletMeta) {
        if (GlobalStateMgr.isCheckpointThread()) {
            return;
        }
        mutationLock.lock();
        try {
            tabletMetaMap.putIfAbsent(tabletId, tabletMeta);
            LOG.debug("add tablet: {} tabletMeta: {}", tabletId, tabletMeta);
        } finally {
            mutationLock.unlock();
        }
    }

    @VisibleForTesting
    public ConcurrentLong2ObjectHashMap<Set<Long>> getForceDeleteTablets() {
        return forceDeleteTablets;
    }

    public boolean tabletForceDelete(long tabletId, long backendId) {
        Set<Long> backendSets = forceDeleteTablets.get(tabletId);
        if (backendSets != null) {
            return backendSets.contains(backendId);
        } else {
            return false;
        }
    }

    public void markTabletForceDelete(long tabletId, long backendId) {
        forceDeleteTablets.computeIfAbsent(tabletId, k -> ConcurrentHashMap.newKeySet()).add(backendId);
    }

    public void markTabletForceDelete(long tabletId, Set<Long> backendIds) {
        if (backendIds.isEmpty()) {
            return;
        }

        forceDeleteTablets.computeIfAbsent(tabletId, k -> ConcurrentHashMap.newKeySet()).addAll(backendIds);
    }

    public void markTabletForceDelete(Tablet tablet) {
        // LakeTablet is managed by StarOS, no need to do this mark and clean up
        if (tablet instanceof LakeTablet) {
            return;
        }
        markTabletForceDelete(tablet.getId(), tablet.getBackendIds());
    }

    /**
     * Batch mark tablets for force delete. This method acquires the write lock only once
     * for all tablets, which is more efficient than calling markTabletForceDelete for each tablet.
     *
     * @param tablets collection of tablets to mark for force delete
     */
    public void markTabletsForceDelete(java.util.Collection<Tablet> tablets) {
        if (tablets == null || tablets.isEmpty()) {
            return;
        }
        writeLock();
        try {
            for (Tablet tablet : tablets) {
                // LakeTablet is managed by StarOS, no need to do this mark and clean up
                if (tablet instanceof LakeTablet) {
                    continue;
                }
                long tabletId = tablet.getId();
                Set<Long> backendIds = tablet.getBackendIds();
                if (backendIds != null && !backendIds.isEmpty()) {
                    forceDeleteTablets.put(tabletId, backendIds);
                }
            }
        } finally {
            writeUnlock();
        }
    }

    public void eraseTabletForceDelete(long tabletId, long backendId) {
        forceDeleteTablets.computeIfPresent(tabletId, (id, backendSets) -> {
            backendSets.remove(backendId);
            return backendSets.isEmpty() ? null : backendSets;
        });
    }

    public void deleteTablet(long tabletId) {
        if (GlobalStateMgr.isCheckpointThread()) {
            return;
        }
        mutationLock.lock();
        try {
            deleteTabletUnlocked(tabletId);
        } finally {
            mutationLock.unlock();
        }
    }

    /**
     * Batch delete tablets. This method acquires the write lock only once for all tablets,
     * which is more efficient than calling deleteTablet for each tablet individually.
     *
     * @param tabletIds collection of tablet IDs to delete
     */
    public void deleteTablets(java.util.Collection<Long> tabletIds) {
        if (tabletIds == null || tabletIds.isEmpty()) {
            return;
        }
        if (GlobalStateMgr.isCheckpointThread()) {
            return;
        }
        writeLock();
        try {
            for (long tabletId : tabletIds) {
                deleteTabletUnlocked(tabletId);
            }
        } finally {
            writeUnlock();
        }
    }

    /**
     * Internal method to delete tablet without acquiring lock.
     * Caller must hold the mutation lock.
     */
    private void deleteTabletUnlocked(long tabletId) {
        CopyOnWriteArrayList<Replica> replicas = tabletToReplicaList.remove(tabletId);
        if (replicas != null) {
            for (Replica replica : replicas) {
                Set<Long> tabletIds = backendToTabletIdList.get(replica.getBackendId());
                if (tabletIds != null) {
                    tabletIds.remove(tabletId);
                }
            }
        }
        tabletMetaMap.remove(tabletId);

        LOG.debug("delete tablet: {}", tabletId);
    }

    // Only for test
    public ConcurrentLong2ObjectHashMap<CopyOnWriteArrayList<Replica>> getReplicaMetaTable() {
        return tabletToReplicaList;
    }

    public void addReplica(long tabletId, Replica replica) {
        if (GlobalStateMgr.isCheckpointThread()) {
            return;
        }
        mutationLock.lock();
        try {
            CopyOnWriteArrayList<Replica> replicas = tabletToReplicaList.computeIfAbsent(tabletId,
                    k -> new CopyOnWriteArrayList<>());
            replicas.add(replica);

            Set<Long> tabletIds = backendToTabletIdList.computeIfAbsent(replica.getBackendId(),
                    k -> ConcurrentHashMap.newKeySet());
            tabletIds.add(tabletId);

            LOG.debug("add replica {} of tablet {} in backend {}", replica.getId(), tabletId, replica.getBackendId());
        } finally {
            mutationLock.unlock();
        }
    }

    public void deleteReplica(long tabletId, long backendId) {
        if (GlobalStateMgr.isCheckpointThread()) {
            return;
        }
        mutationLock.lock();
        try {
            if (tabletMetaMap.get(tabletId) == null) {
                return;
            }
            CopyOnWriteArrayList<Replica> replicaList = tabletToReplicaList.get(tabletId);
            if (replicaList != null) {
                replicaList.removeIf(replica -> replica.getBackendId() == backendId);
            }

            Set<Long> tabletIds = backendToTabletIdList.get(backendId);
            if (tabletIds != null) {
                tabletIds.remove(tabletId);
            }

            LOG.debug("remove tablet {} in backend {}", tabletId, backendId);
        } finally {
            mutationLock.unlock();
        }
    }

    public Replica getReplica(long tabletId, long backendId) {
        CopyOnWriteArrayList<Replica> replicas = tabletToReplicaList.get(tabletId);
        if (replicas == null) {
            return null;
        }
        for (Replica replica : replicas) {
            if (replica.getBackendId() == backendId) {
                return replica;
            }
        }
        return null;
    }

    public List<Replica> getReplicasByTabletId(long tabletId) {
        CopyOnWriteArrayList<Replica> replicas = tabletToReplicaList.get(tabletId);
        if (replicas == null) {
            return Lists.newArrayList();
        }
        return Lists.newArrayList(replicas);
    }

    /**
     * Returns replicas that belong to the specified backend among the given tablet ids. Any tablet that does not
     * have a replica on the backend is ignored. Callers treat {@code null} as "no replica found on this backend".
     *
     * @param tabletIds tablet id list to probe
     * @param backendId backend id whose replicas are required
     * @return replicas located on {@code backendId}, or {@code null} if the backend does not host any of them
     */
    public List<Replica> getReplicasOnBackendByTabletIds(List<Long> tabletIds, long backendId) {
        List<Replica> replicas = Lists.newArrayListWithCapacity(tabletIds.size());
        for (Long tabletId : tabletIds) {
            Replica replica = getReplica(tabletId, backendId);
            if (replica != null) {
                replicas.add(replica);
            }
        }
        if (replicas.isEmpty()) {
            return null;
        }
        return replicas;
    }

    public List<Long> getTabletIdsByBackendId(long backendId) {
        Set<Long> tabletIds = backendToTabletIdList.get(backendId);
        if (tabletIds == null) {
            return Lists.newArrayList();
        }
        return Lists.newArrayList(tabletIds);
    }

    public List<Long> getTabletIdsByBackendIdAndStorageMedium(long backendId, TStorageMedium storageMedium) {
        List<Long> result = Lists.newArrayList();

        Set<Long> tabletIds = backendToTabletIdList.get(backendId);
        if (tabletIds == null) {
            return result;
        }
        for (Long tabletId : tabletIds) {
            TabletMeta tabletMeta = tabletMetaMap.get(tabletId);
            if (tabletMeta == null) {
                continue;
            }
            if (tabletMeta.getStorageMedium() == storageMedium) {
                result.add(tabletId);
            }
        }
        return result;
    }

    public long getTabletNumByBackendId(long backendId) {
        Set<Long> tabletIds = backendToTabletIdList.get(backendId);
        return tabletIds == null ? 0 : tabletIds.size();
    }

    /**
     * Get the number of tablets on the specified backend, grouped by pathHash
     *
     * @param backendId the ID of the backend
     * @return Map<pathHash, tabletNum> the number of tablets grouped by pathHash
     * @implNote Linear scan, invoke this interface with caution if the number of replicas is large
     */
    public Map<Long, Long> getTabletNumByBackendIdGroupByPathHash(long backendId) {
        Map<Long, Long> pathHashToTabletNum = Maps.newHashMap();

        Set<Long> tabletIds = backendToTabletIdList.get(backendId);
        if (tabletIds == null) {
            return pathHashToTabletNum;
        }
        for (Long tabletId : tabletIds) {
            CopyOnWriteArrayList<Replica> replicas = tabletToReplicaList.get(tabletId);
            if (replicas == null) {
                continue;
            }
            for (Replica r : replicas) {
                if (r.getBackendId() == backendId) {
                    pathHashToTabletNum.compute(r.getPathHash(), (k, v) -> v == null ? 1L : v + 1);
                    break;
                }
            }
        }

        return pathHashToTabletNum;
    }

    public Map<TStorageMedium, Long> getReplicaNumByBeIdAndStorageMedium(long backendId) {
        Map<TStorageMedium, Long> replicaNumMap = Maps.newHashMap();
        long hddNum = 0;
        long ssdNum = 0;
        Set<Long> tabletIds = backendToTabletIdList.get(backendId);
        if (tabletIds == null) {
            replicaNumMap.put(TStorageMedium.HDD, hddNum);
            replicaNumMap.put(TStorageMedium.SSD, ssdNum);
            return replicaNumMap;
        }
        for (Long tabletId : tabletIds) {
            TabletMeta tabletMeta = tabletMetaMap.get(tabletId);
            if (tabletMeta == null) {
                continue;
            }
            if (tabletMeta.getStorageMedium() == TStorageMedium.HDD) {
                hddNum++;
            } else {
                ssdNum++;
            }
        }
        replicaNumMap.put(TStorageMedium.HDD, hddNum);
        replicaNumMap.put(TStorageMedium.SSD, ssdNum);
        return replicaNumMap;
    }

    public long getTabletCount() {
        return this.tabletMetaMap.size();
    }

    public long getReplicaCount() {
        long replicaCount = 0;
        for (CopyOnWriteArrayList<Replica> replicas : tabletToReplicaList.values()) {
            replicaCount += replicas.size();
        }
        return replicaCount;
    }

    // just for test
    public void clear() {
        mutationLock.lock();
        try {
            tabletMetaMap.clear();
            tabletToReplicaList.clear();
            backendToTabletIdList.clear();
            forceDeleteTablets.clear();
        } finally {
            mutationLock.unlock();
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
        return Estimator.estimate(tabletMetaMap) +
                Estimator.estimate(tabletToReplicaList) +
                Estimator.estimate(backendToTabletIdList) +
                Estimator.estimate(forceDeleteTablets);
    }
}
