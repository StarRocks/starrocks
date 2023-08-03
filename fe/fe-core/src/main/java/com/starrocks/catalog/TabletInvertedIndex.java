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
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.starrocks.catalog.Replica.ReplicaState;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.lake.LakeTablet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.system.Backend;
import com.starrocks.thrift.TPartitionVersionInfo;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TTablet;
import com.starrocks.thrift.TTabletInfo;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.PartitionCommitInfo;
import com.starrocks.transaction.TableCommitInfo;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/*
 * this class stores a inverted index
 * key is tablet id. value is the related ids of this tablet
 * Checkpoint thread is no need to modify this inverted index, because this inverted index will not be wrote
 * into images, all meta data are in globalStateMgr, and the inverted index will be rebuild when FE restart.
 */
public class TabletInvertedIndex {
    private static final Logger LOG = LogManager.getLogger(TabletInvertedIndex.class);

    public static final int NOT_EXIST_VALUE = -1;

    public static final TabletMeta NOT_EXIST_TABLET_META = new TabletMeta(NOT_EXIST_VALUE, NOT_EXIST_VALUE,
            NOT_EXIST_VALUE, NOT_EXIST_VALUE, NOT_EXIST_VALUE, TStorageMedium.HDD);

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    // tablet id -> tablet meta
    private final Map<Long, TabletMeta> tabletMetaMap = Maps.newHashMap();

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

    private void readLock() {
        this.lock.readLock().lock();
    }

    private void readUnlock() {
        this.lock.readLock().unlock();
    }

    private void writeLock() {
        this.lock.writeLock().lock();
    }

    private void writeUnlock() {
        this.lock.writeLock().unlock();
    }

    public void tabletReport(long backendId, Map<Long, TTablet> backendTablets,
                             final HashMap<Long, TStorageMedium> storageMediumMap,
                             ListMultimap<Long, Long> tabletSyncMap,
                             ListMultimap<Long, Long> tabletDeleteFromMeta,
                             Set<Long> foundTabletsWithValidSchema,
                             Map<Long, TTabletInfo> foundTabletsWithInvalidSchema,
                             ListMultimap<TStorageMedium, Long> tabletMigrationMap,
                             Map<Long, ListMultimap<Long, TPartitionVersionInfo>> transactionsToPublish,
                             Map<Long, Long> transactionsToCommitTime,
                             ListMultimap<Long, Long> transactionsToClear,
                             ListMultimap<Long, Long> tabletRecoveryMap,
                             Set<Pair<Long, Integer>> tabletWithoutPartitionId) {

        for (TTablet backendTablet : backendTablets.values()) {
            for (TTabletInfo tabletInfo : backendTablet.tablet_infos) {
                if (!tabletInfo.isSetPartition_id() || tabletInfo.getPartition_id() < 1) {
                    tabletWithoutPartitionId.add(new Pair<>(tabletInfo.getTablet_id(), tabletInfo.getSchema_hash()));
                }
            }
        }

        int backendStorageTypeCnt = -1;
        Backend be = GlobalStateMgr.getCurrentSystemInfo().getBackend(backendId);
        if (be != null) {
            backendStorageTypeCnt = be.getAvailableBackendStorageTypeCnt();
        }

        readLock();
        long start = System.currentTimeMillis();
        try {
            LOG.debug("begin to do tablet diff with backend[{}]. num: {}", backendId, backendTablets.size());
            // backingReplicaMetaTable.row(backendId) won't return null
            Map<Long, Replica> replicaMetaWithBackend = backingReplicaMetaTable.row(backendId);
            // traverse replicas in meta with this backend
            for (Map.Entry<Long, Replica> entry : replicaMetaWithBackend.entrySet()) {
                long tabletId = entry.getKey();
                Preconditions.checkState(tabletMetaMap.containsKey(tabletId));
                TabletMeta tabletMeta = tabletMetaMap.get(tabletId);

                if (tabletMeta.isLakeTablet()) {
                    continue;
                }

                if (backendTablets.containsKey(tabletId)) {
                    TTablet backendTablet = backendTablets.get(tabletId);
                    Replica replica = entry.getValue();
                    for (TTabletInfo backendTabletInfo : backendTablet.getTablet_infos()) {
                        if (backendTabletInfo.isSetIs_error_state()) {
                            replica.setIsErrorState(backendTabletInfo.is_error_state);
                        }
                        if (tabletMeta.containsSchemaHash(backendTabletInfo.getSchema_hash())) {
                            foundTabletsWithValidSchema.add(tabletId);
                            // 1. (intersection)
                            if (needSync(replica, backendTabletInfo)) {
                                // need sync
                                tabletSyncMap.put(tabletMeta.getDbId(), tabletId);
                            }

                            // check and set path,
                            // path info of replica is only saved in Leader FE
                            if (backendTabletInfo.isSetPath_hash() &&
                                    replica.getPathHash() != backendTabletInfo.getPath_hash()) {
                                replica.setPathHash(backendTabletInfo.getPath_hash());
                            }

                            if (backendTabletInfo.isSetSchema_hash() && replica.getState() == ReplicaState.NORMAL
                                    && replica.getSchemaHash() != backendTabletInfo.getSchema_hash()) {
                                // update the schema hash only when replica is normal
                                replica.setSchemaHash(backendTabletInfo.getSchema_hash());
                            }

                            if (needRecover(replica, tabletMeta.getOldSchemaHash(), backendTabletInfo)) {
                                LOG.warn("replica {} of tablet {} on backend {} need recovery. "
                                                + "replica in FE: {}, report version {}, report schema hash: {},"
                                                + " is bad: {}",
                                        replica.getId(), tabletId, backendId,
                                        replica, backendTabletInfo.getVersion(), backendTabletInfo.getSchema_hash(),
                                        backendTabletInfo.isSetUsed() ? backendTabletInfo.isUsed() : "unknown");
                                tabletRecoveryMap.put(tabletMeta.getDbId(), tabletId);
                            }

                            replica.setLastReportVersion(backendTabletInfo.getVersion());

                            // check if tablet needs migration
                            long partitionId = tabletMeta.getPartitionId();
                            TStorageMedium storageMedium = storageMediumMap.get(partitionId);
                            if (storageMedium != null && backendTabletInfo.isSetStorage_medium()) {
                                // If storage medium is less than 1, there is no need to send migration tasks to BE.
                                // Because BE will ignore this request.
                                if (storageMedium != backendTabletInfo.getStorage_medium()) {
                                    if (backendStorageTypeCnt <= 1) {
                                        LOG.debug("available storage medium type count is less than 1, " +
                                                        "no need to send migrate task. tabletId={}, backendId={}.",
                                                tabletId, backendId);
                                    } else if (tabletMigrationMap.size() <=
                                            Config.tablet_sched_max_migration_task_sent_once) {
                                        tabletMigrationMap.put(storageMedium, tabletId);
                                    }
                                }
                                if (storageMedium != tabletMeta.getStorageMedium()) {
                                    tabletMeta.setStorageMedium(storageMedium);
                                }
                            }
                            // check if we should clear transactions
                            if (backendTabletInfo.isSetTransaction_ids()) {
                                List<Long> transactionIds = backendTabletInfo.getTransaction_ids();
                                GlobalTransactionMgr transactionMgr =
                                        GlobalStateMgr.getCurrentGlobalTransactionMgr();
                                for (Long transactionId : transactionIds) {
                                    TransactionState transactionState =
                                            transactionMgr.getTransactionState(tabletMeta.getDbId(), transactionId);
                                    if (transactionState == null ||
                                            transactionState.getTransactionStatus() == TransactionStatus.ABORTED) {
                                        transactionsToClear.put(transactionId, tabletMeta.getPartitionId());
                                        LOG.debug("transaction id [{}] is not valid any more, "
                                                + "clear it from backend [{}]", transactionId, backendId);
                                    } else if (transactionState.getTransactionStatus() ==
                                            TransactionStatus.VISIBLE) {
                                        TableCommitInfo tableCommitInfo =
                                                transactionState.getTableCommitInfo(tabletMeta.getTableId());
                                        PartitionCommitInfo partitionCommitInfo =
                                                tableCommitInfo.getPartitionCommitInfo(partitionId);
                                        if (partitionCommitInfo == null) {
                                            /*
                                             * This may happen as follows:
                                             * 1. txn is committed on BE, and report commit info to FE
                                             * 2. FE received report and begin to assemble partitionCommitInfos.
                                             * 3. At the same time, some partitions have been dropped, so
                                             *    partitionCommitInfos does not contain these partitions.
                                             * 4. So we will not able to get partitionCommitInfo here.
                                             *
                                             * Just print a log to observe
                                             */
                                            LOG.info(
                                                    "failed to find partition commit info. table: {}, " +
                                                            "partition: {}, tablet: {}, txn_id: {}",
                                                    tabletMeta.getTableId(), partitionId, tabletId,
                                                    transactionState.getTransactionId());
                                        } else {
                                            TPartitionVersionInfo versionInfo =
                                                    new TPartitionVersionInfo(tabletMeta.getPartitionId(),
                                                            partitionCommitInfo.getVersion(), 0);
                                            ListMultimap<Long, TPartitionVersionInfo> map =
                                                    transactionsToPublish.get(transactionState.getDbId());
                                            if (map == null) {
                                                map = ArrayListMultimap.create();
                                                transactionsToPublish.put(transactionState.getDbId(), map);
                                            }
                                            map.put(transactionId, versionInfo);
                                            transactionsToCommitTime.put(transactionId,
                                                    transactionState.getCommitTime());
                                        }
                                    }
                                }
                            } // end for txn id

                            // update replica's version count
                            // no need to write log, and no need to get db lock.
                            if (backendTabletInfo.isSetVersion_count()) {
                                replica.setVersionCount(backendTabletInfo.getVersion_count());
                            }
                        } else {
                            // tablet with invalid schema hash
                            foundTabletsWithInvalidSchema.put(tabletId, backendTabletInfo);
                        } // end for be tablet info
                    }
                } else {
                    // 2. (meta - be)
                    // may need delete from meta
                    LOG.debug("backend[{}] does not report tablet[{}-{}]", backendId, tabletId, tabletMeta);
                    tabletDeleteFromMeta.put(tabletMeta.getDbId(), tabletId);
                }
            } // end for replicaMetaWithBackend
        } finally {
            readUnlock();
        }

        long end = System.currentTimeMillis();
        LOG.info("finished to do tablet diff with backend[{}]. sync: {}. metaDel: {}. foundValid: {}. foundInvalid: {}."
                        + " migration: {}. found invalid transactions {}. found republish transactions {} "
                        + " cost: {} ms", backendId, tabletSyncMap.size(),
                tabletDeleteFromMeta.size(), foundTabletsWithValidSchema.size(), foundTabletsWithInvalidSchema.size(),
                tabletMigrationMap.size(), transactionsToClear.size(), transactionsToPublish.size(), (end - start));
    }

    private void deleteTabletByConsistencyChecker(long tabletId, long backendId,
                                                  String reason, Set<Long> invalidTablets) {
        LOG.info("delete tablet {} on backend {} from inverted index by consistency checker, because: {}",
                tabletId, backendId, reason);
        deleteTablet(tabletId);
        invalidTablets.add(tabletId);
    }

    /**
     * Check the consistency between {@link com.starrocks.catalog.TabletInvertedIndex} and
     * {@link com.starrocks.server.LocalMetastore}.
     * <p>
     * If we find an invalid tablet, i.e. it's neither in current catalog nor in recycle bin,
     * we will remove it from {@link com.starrocks.catalog.TabletInvertedIndex} directly.
     * And this process will also output a report in `fe.log`, including valid number of
     * tablet and number of tablet in recycle bin for each backend.
     */
    public void checkTabletMetaConsistency() {
        LocalMetastore localMetastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        CatalogRecycleBin recycleBin = GlobalStateMgr.getCurrentRecycleBin();

        Set<Long> invalidTablets = new HashSet<>();
        // backend id -> <num of currently existed tablet, num of tablet in recycle bin>
        Map<Long, Pair<Long, Long>> backendTabletNumReport = new HashMap<>();
        List<Long> backendIds = GlobalStateMgr.getCurrentSystemInfo().getBackendIds();

        long startTime = System.currentTimeMillis();
        long scannedTabletCount = 0;

        for (Long backendId : backendIds) {
            List<Long> tabletIds = getTabletIdsByBackendId(backendId);
            backendTabletNumReport.put(backendId, new Pair<>(0L, 0L));

            for (Long tabletId : tabletIds) {
                scannedTabletCount++;
                boolean isInRecycleBin = false;

                TabletMeta tabletMeta = getTabletMeta(tabletId);
                if (tabletMeta == null) {
                    deleteTabletByConsistencyChecker(tabletId, backendId, "tablet meta is null", invalidTablets);
                    continue;
                }

                // validate database
                long dbId = tabletMeta.getDbId();
                Database database = localMetastore.getDb(dbId);
                if (database == null) {
                    database = recycleBin.getDatabase(dbId);
                    if (database != null) {
                        isInRecycleBin = true;
                    } else {
                        deleteTabletByConsistencyChecker(tabletId, backendId,
                                "database " + dbId + " doesn't exist", invalidTablets);
                        continue;
                    }
                }

                try {
                    database.readLock();

                    // validate table
                    long tableId = tabletMeta.getTableId();
                    com.starrocks.catalog.Table table = database.getTable(tableId);
                    if (table == null) {
                        table = recycleBin.getTable(dbId, tableId);
                        if (table != null) {
                            isInRecycleBin = true;
                        } else {
                            deleteTabletByConsistencyChecker(tabletId, backendId,
                                    "table " + dbId + "." + tableId + " doesn't exist", invalidTablets);
                            continue;
                        }
                    }

                    // validate partition
                    long partitionId = tabletMeta.getPartitionId();
                    Partition partition = table.getPartition(partitionId);
                    if (partition == null) {
                        partition = recycleBin.getPartition(partitionId);
                        if (partition != null) {
                            isInRecycleBin = true;
                        } else {
                            deleteTabletByConsistencyChecker(tabletId, backendId,
                                    "partition " + dbId + "." + tableId + "." + partitionId + " doesn't exist",
                                    invalidTablets);
                            continue;
                        }
                    }

                    // validate index
                    long indexId = tabletMeta.getIndexId();
                    MaterializedIndex index = partition.getIndex(indexId);
                    if (index == null) {
                        deleteTabletByConsistencyChecker(tabletId, backendId,
                                "materialized index " + dbId + "." + tableId + "." +
                                        partitionId + "." + indexId + " doesn't exist",
                                invalidTablets);
                        continue;
                    }

                    if (!table.isCloudNativeTableOrMaterializedView()) {
                        // validate tablet
                        Tablet tablet = index.getTablet(tabletId);
                        if (tablet == null) {
                            deleteTabletByConsistencyChecker(tabletId, backendId,
                                    "tablet " + dbId + "." + tableId + "." +
                                            partitionId + "." + indexId + "." + tabletId + " doesn't exist",
                                    invalidTablets);
                            continue;
                        }
                    }

                    if (isInRecycleBin) {
                        backendTabletNumReport.get(backendId).second++;
                    } else {
                        backendTabletNumReport.get(backendId).first++;
                    }

                    tabletMeta.resetToBeCleanedTime();
                } finally {
                    database.readUnlock();
                }
            } // end for tabletIds
        } // end for backendIds

        // logging report
        LOG.info("TabletMetaChecker has cleaned {} invalid tablet(s), scanned {} tablet(s) on {} backend(s) in {}ms," +
                " backend tablet count info(format: backend_id=curr_tablet_count:recycle_tablet_count): {}",
                invalidTablets.size(), scannedTabletCount, backendIds.size(),
                System.currentTimeMillis() - startTime, backendTabletNumReport);
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

    private boolean needSync(Replica replicaInFe, TTabletInfo backendTabletInfo) {
        if (backendTabletInfo.isSetUsed() && !backendTabletInfo.isUsed()) {
            // tablet is bad, do not sync
            // it will be handled in needRecovery()
            return false;
        }

        if (replicaInFe.getState() == ReplicaState.ALTER) {
            // ignore the replica is ALTER state. its version will be taken care by load process and alter table process
            return false;
        }

        long versionInFe = replicaInFe.getVersion();

        // backend replica's version is equal to replica in FE, but replica in FE is bad, while backend replica is good, sync it
        if (backendTabletInfo.getVersion() > versionInFe) {
            // backend replica's version is larger or newer than replica in FE, sync it.
            return true;
        } else {
            return versionInFe == backendTabletInfo.getVersion() &&
                    replicaInFe.isBad();
        }
    }

    /**
     * Be will set `used' to false for bad replicas and `version_miss' to true for replicas with hole
     * in their version chain. In either case, those replicas need to be fixed by TabletScheduler.
     */
    private boolean needRecover(Replica replicaInFe, int schemaHashInFe, TTabletInfo backendTabletInfo) {
        if (replicaInFe.getState() != ReplicaState.NORMAL) {
            // only normal replica need recover
            // case:
            // the replica's state is CLONE, which means this a newly created replica in clone process.
            // and an old out-of-date replica reports here, and this report should not mark this replica as
            // 'need recovery'.
            // Other state such as ROLLUP/SCHEMA_CHANGE, the replica behavior is unknown, so for safety reason,
            // also not mark this replica as 'need recovery'.
            return false;
        }

        if (schemaHashInFe != backendTabletInfo.getSchema_hash()
                || backendTabletInfo.getVersion() == -1) {
            // no data file exist on BE, maybe this is a newly created schema change tablet. no need to recovery
            return false;
        }

        if (backendTabletInfo.isSetUsed() && !backendTabletInfo.isUsed()) {
            // tablet is bad
            return true;
        }

        // lastReportVersion should be increased monotonically.
        return backendTabletInfo.getVersion() < replicaInFe.getLastReportVersion();
    }

    // always add tablet before adding replicas
    public void addTablet(long tabletId, TabletMeta tabletMeta) {
        if (GlobalStateMgr.isCheckpointThread()) {
            return;
        }
        writeLock();
        try {
            tabletMetaMap.putIfAbsent(tabletId, tabletMeta);

            LOG.debug("add tablet: {}", tabletId);
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
            Preconditions.checkState(tabletMetaMap.containsKey(tabletId), tabletId);
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
        return this.replicaMetaTable.size();
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
}

