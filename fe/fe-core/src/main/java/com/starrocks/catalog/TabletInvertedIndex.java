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
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.starrocks.common.Pair;
import com.starrocks.lake.LakeTablet;
import com.starrocks.memory.MemoryTrackable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TStorageMedium;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static com.starrocks.server.GlobalStateMgr.isCheckpointThread;

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
            NOT_EXIST_VALUE, NOT_EXIST_VALUE, NOT_EXIST_VALUE, TStorageMedium.HDD);

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    // tablet id -> tablet meta
    private final Map<Long, TabletMeta> tabletMetaMap = Maps.newConcurrentMap();

    // replica id -> tablet id
    private final Map<Long, Long> replicaToTabletMap = Maps.newHashMap();

    // tablet id -> backend set
    private final Map<Long, Set<Long>> forceDeleteTablets = Maps.newHashMap();

    // tablet id -> (backend id -> replica)
    private final Table<Long, Long, Replica> replicaMetaTable = HashBasedTable.create();
    // backing replica table, for visiting backend replicas faster.
    // backend id -> (tablet id -> replica)
    private final Table<Long, Long, Replica> backingReplicaMetaTable = HashBasedTable.create();

    public TabletInvertedIndex() {
    }

    public void readLock() {
        this.lock.readLock().lock();
    }

    public void readUnlock() {
        this.lock.readLock().unlock();
    }

    private void writeLock() {
        this.lock.writeLock().lock();
    }

    private void writeUnlock() {
        this.lock.writeLock().unlock();
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
        forceDeleteTablets.put(tabletId, backendIds);
        writeUnlock();
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
            Map<Long, Replica> replicas = replicaMetaTable.rowMap().remove(tabletId);
            if (replicas != null) {
                for (Replica replica : replicas.values()) {
                    replicaToTabletMap.remove(replica.getId());
                }

                for (long backendId : replicas.keySet()) {
                    backingReplicaMetaTable.remove(backendId, tabletId);
                }
            }
            tabletMetaMap.remove(tabletId);

            LOG.debug("delete tablet: {}", tabletId);
        } finally {
            writeUnlock();
        }
    }

    public Table<Long, Long, Replica> getReplicaMetaTable() {
        return replicaMetaTable;
    }

    public void addReplica(long tabletId, Replica replica) {
        if (GlobalStateMgr.isCheckpointThread()) {
            return;
        }
        writeLock();
        try {
            Preconditions.checkState(tabletMetaMap.containsKey(tabletId));
            replicaMetaTable.put(tabletId, replica.getBackendId(), replica);
            replicaToTabletMap.put(replica.getId(), tabletId);
            backingReplicaMetaTable.put(replica.getBackendId(), tabletId, replica);
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
            if (replicaMetaTable.containsRow(tabletId)) {
                Replica replica = replicaMetaTable.remove(tabletId, backendId);
                assert replica != null;
                replicaToTabletMap.remove(replica.getId());
                replicaMetaTable.remove(tabletId, backendId);
                backingReplicaMetaTable.remove(backendId, tabletId);
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
            return replicaMetaTable.get(tabletId, backendId);
        } finally {
            readUnlock();
        }
    }

    public List<Replica> getReplicasByTabletId(long tabletId) {
        readLock();
        try {
            if (replicaMetaTable.containsRow(tabletId)) {
                return Lists.newArrayList(replicaMetaTable.row(tabletId).values());
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
            Map<Long, Replica> replicaMetaWithBackend = backingReplicaMetaTable.row(backendId);
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
            Map<Long, Replica> replicaMetaWithBackend = backingReplicaMetaTable.row(backendId);
            tabletIds.addAll(replicaMetaWithBackend.keySet());
        } finally {
            readUnlock();
        }
        return tabletIds;
    }

    public List<Long> getTabletIdsByBackendIdAndStorageMedium(long backendId, TStorageMedium storageMedium) {
        List<Long> tabletIds;
        readLock();
        try {
            Map<Long, Replica> replicaMetaWithBackend = backingReplicaMetaTable.row(backendId);
            tabletIds = replicaMetaWithBackend.keySet().stream().filter(
                    id -> tabletMetaMap.get(id).getStorageMedium() == storageMedium).collect(Collectors.toList());
        } finally {
            readUnlock();
        }
        return tabletIds;
    }

    public long getTabletNumByBackendId(long backendId) {
        readLock();
        try {
            Map<Long, Replica> replicaMetaWithBackend = backingReplicaMetaTable.row(backendId);
            return replicaMetaWithBackend.size();
        } finally {
            readUnlock();
        }
    }

    public long getTabletNumByBackendIdAndPathHash(long backendId, long pathHash) {
        readLock();
        try {
            Map<Long, Replica> replicaMetaWithBackend = backingReplicaMetaTable.row(backendId);
            return replicaMetaWithBackend.values().stream().filter(r -> r.getPathHash() == pathHash).count();
        } finally {
            readUnlock();
        }
    }

    public Map<TStorageMedium, Long> getReplicaNumByBeIdAndStorageMedium(long backendId) {
        Map<TStorageMedium, Long> replicaNumMap = Maps.newHashMap();
        long hddNum = 0;
        long ssdNum = 0;
        readLock();
        try {
            Map<Long, Replica> replicaMetaWithBackend = backingReplicaMetaTable.row(backendId);
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
        return this.tabletMetaMap.size();
    }

    public long getReplicaCount() {
        readLock();
        try {
            return this.replicaMetaTable.size();
        } finally {
            readUnlock();
        }
    }

    public Map<Long, Replica> getReplicaMetaWithBackend(Long backendId) {
        return backingReplicaMetaTable.row(backendId);
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

    public void recreateTabletInvertIndex() {
        if (isCheckpointThread()) {
            return;
        }

        // create inverted index
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        for (Database db : GlobalStateMgr.getCurrentState().getLocalMetastore().getFullNameToDb().values()) {
            long dbId = db.getId();
            for (com.starrocks.catalog.Table table : db.getTables()) {
                if (!table.isNativeTableOrMaterializedView()) {
                    continue;
                }

                OlapTable olapTable = (OlapTable) table;
                long tableId = olapTable.getId();
                for (PhysicalPartition partition : olapTable.getAllPhysicalPartitions()) {
                    long physicalPartitionId = partition.getId();
                    TStorageMedium medium = olapTable.getPartitionInfo().getDataProperty(
                            partition.getParentId()).getStorageMedium();
                    for (MaterializedIndex index : partition
                            .getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                        long indexId = index.getId();
                        int schemaHash = olapTable.getSchemaHashByIndexId(indexId);
                        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partition.getParentId(), physicalPartitionId,
                                indexId, schemaHash, medium, table.isCloudNativeTableOrMaterializedView());
                        for (Tablet tablet : index.getTablets()) {
                            long tabletId = tablet.getId();
                            invertedIndex.addTablet(tabletId, tabletMeta);
                            if (table.isOlapTableOrMaterializedView()) {
                                for (Replica replica : ((LocalTablet) tablet).getImmutableReplicas()) {
                                    invertedIndex.addReplica(tabletId, replica);
                                }
                            }
                        }
                    } // end for indices
                } // end for partitions
            } // end for tables
        } // end for dbs
    }

    @Override
    public Map<String, Long> estimateCount() {
        return ImmutableMap.of("TabletMeta", (long) tabletMetaMap.size(),
                "TabletCount", getTabletCount(),
                "ReplicateCount", getReplicaCount());
    }

    @Override
    public List<Pair<List<Object>, Long>> getSamples() {
        readLock();
        try {
            List<Object> tabletMetaSamples = tabletMetaMap.values()
                    .stream()
                    .limit(1)
                    .collect(Collectors.toList());

            List<Object> longSamples = Lists.newArrayList(0L);
            long longSize = tabletMetaMap.size() + replicaToTabletMap.size() * 2L + forceDeleteTablets.size() * 4L
                    + replicaMetaTable.size() * 2L + backingReplicaMetaTable.size() * 2L;

            return Lists.newArrayList(Pair.create(tabletMetaSamples, (long) tabletMetaMap.size()),
                    Pair.create(longSamples, longSize));
        } finally {
            readUnlock();
        }
    }
}
