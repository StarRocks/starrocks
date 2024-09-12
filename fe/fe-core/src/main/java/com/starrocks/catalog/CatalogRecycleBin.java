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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/CatalogRecycleBin.java

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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.RecoverInfo;
import com.starrocks.persist.gson.IForwardCompatibleObject;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TStorageMedium;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import static com.starrocks.server.GlobalStateMgr.isCheckpointThread;
import static java.lang.Math.max;

public class CatalogRecycleBin extends FrontendDaemon implements Writable {
    private static final Logger LOG = LogManager.getLogger(CatalogRecycleBin.class);
    // erase meta at least after MIN_ERASE_LATENCY milliseconds
    // to avoid erase log ahead of drop log
    private static final long MIN_ERASE_LATENCY = 10L * 60L * 1000L;  // 10 min
    // Maximum value of a batch of operations for actually delete database(table/partition)
    // The erase operation will be locked, so one batch can not be too many.
    private static final int MAX_ERASE_OPERATIONS_PER_CYCLE = 500;
    private static final long FAIL_RETRY_INTERVAL = 60L * 1000L; // 1 min

    private final Map<Long, RecycleDatabaseInfo> idToDatabase;
    // The first Long type is DdId, the second Long is TableId
    private final com.google.common.collect.Table<Long, Long, RecycleTableInfo> idToTableInfo;
    // The first Long type is DdId, the second String is TableName
    private final com.google.common.collect.Table<Long, String, RecycleTableInfo> nameToTableInfo;
    private final Map<Long, RecyclePartitionInfo> idToPartition;

    protected Map<Long, Long> idToRecycleTime;

    // The real recycle time will extend by LATE_RECYCLE_INTERVAL_SECONDS when enable `eraseLater`.
    // It is only take effect on master when the tablet scheduler repairs a tablet that is about to expire.
    // Assume that the repair task will be done within LATE_RECYCLE_INTERVAL_SECONDS.
    // We should check DB/table/partition that was about to expire in LATE_RECYCLE_INTERVAL_SECONDS, and make sure
    // they stay longer until the asynchronous agent task finish.
    protected static int LATE_RECYCLE_INTERVAL_SECONDS = 60;
    protected Set<Long> enableEraseLater;

    public CatalogRecycleBin() {
        super("recycle bin");
        idToDatabase = Maps.newHashMap();
        idToTableInfo = HashBasedTable.create();
        nameToTableInfo = HashBasedTable.create();
        idToPartition = Maps.newHashMap();
        idToRecycleTime = Maps.newHashMap();
        enableEraseLater = new HashSet<>();
    }

    private void removeRecycleMarkers(Long id) {
        idToRecycleTime.remove(id);
        enableEraseLater.remove(id);
    }

    public synchronized void recycleDatabase(Database db, Set<String> tableNames, boolean isForce) {
        if (isForce) {
            onEraseDatabase(db.getId());
            return;
        }
        Preconditions.checkState(!idToDatabase.containsKey(db.getId()));

        // db should be empty. all tables are recycled before
        Preconditions.checkState(db.getTables().isEmpty());

        // erase db with same name
        eraseDatabaseWithSameName(db.getFullName());

        // recycle db
        RecycleDatabaseInfo databaseInfo = new RecycleDatabaseInfo(db, tableNames);
        idToDatabase.put(db.getId(), databaseInfo);
        idToRecycleTime.put(db.getId(), System.currentTimeMillis());
        LOG.info("recycle db[{}-{}]", db.getId(), db.getOriginName());
    }

    public void onEraseDatabase(long dbId) {
        // remove database transaction manager
        GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().removeDatabaseTransactionMgr(dbId);
        // unbind db to storage volume
        GlobalStateMgr.getCurrentState().getStorageVolumeMgr().unbindDbToStorageVolume(dbId);
    }

    public synchronized Database getDatabase(long dbId) {
        RecycleDatabaseInfo databaseInfo = idToDatabase.get(dbId);
        if (databaseInfo != null) {
            return databaseInfo.getDb();
        }
        return null;
    }

    public synchronized void recycleTable(long dbId, Table table, boolean recoverable) {
        if (idToTableInfo.row(dbId).containsKey(table.getId())) {
            LOG.error("table[{}-{}] already in recycle bin.", table.getId(), table.getName());
            return;
        }
        String tableName = table.getName();
        RecycleTableInfo newTableInfo = new RecycleTableInfo(dbId, table, recoverable);
        RecycleTableInfo oldTableInfo = nameToTableInfo.get(dbId, tableName);

        // There's already a table with the same name in the recycle bin.
        if (oldTableInfo != null) {
            // Our serialization rules require `idToTableInfo` and `nameToTableInfo` to be the same size, so
            // we need to make sure that tables with different ids have different names (in the recycle bin), so
            // here we rename the previous table's name.
            Table oldTable = oldTableInfo.getTable();
            oldTable.setName(uniqueIllegalTableName(tableName, oldTable.getId()));
            // Mark the renamed table as unrecoverable, as if it had been force dropped, to maintain a similar
            // behavior as older versions.
            oldTableInfo.setRecoverable(false);
            nameToTableInfo.remove(dbId, table.getName());
            nameToTableInfo.put(dbId, oldTable.getName(), oldTableInfo);
            // Speed up the deletion of this renamed table by modifying its recycle time to zero
            idToRecycleTime.put(oldTable.getId(), 0L);
        }

        // If the table was force dropped, set recycle time to zero so that this table will be deleted immediately
        // in the next cleanup round.
        idToRecycleTime.put(table.getId(), !recoverable ? 0 : System.currentTimeMillis());
        idToTableInfo.put(dbId, table.getId(), newTableInfo);
        nameToTableInfo.put(dbId, table.getName(), newTableInfo);

        LOG.info("Finished put table '{}' to recycle bin. tableId: {}", table.getName(), table.getId());
    }

    @Nullable
    public synchronized Table getTable(long dbId, long tableId) {
        RecycleTableInfo tableInfo = idToTableInfo.row(dbId).get(tableId);
        return tableInfo != null ? tableInfo.table : null;
    }

    boolean isTableRecoverable(long dbId, long tableId) {
        RecycleTableInfo tableInfo = idToTableInfo.row(dbId).get(tableId);
        return tableInfo != null  && tableInfo.isRecoverable();
    }

    /**
     * Returns a list of all tables in the recycle bin that belong to the database represented by dbId.
     *
     * @param dbId the id the database
     * @return a list of all recoverable tables in the recycle bin that belong to the database represented by dbId
     */
    @NotNull
    public synchronized List<Table> getTables(long dbId) {
        return idToTableInfo.row(dbId).values().stream()
                .map(RecycleTableInfo::getTable)
                .collect(Collectors.toList());
    }

    public synchronized void recyclePartition(RecyclePartitionInfo recyclePartitionInfo) {
        Preconditions.checkState(!idToPartition.containsKey(recyclePartitionInfo.getPartition().getId()));

        long dbId = recyclePartitionInfo.getDbId();
        long tableId = recyclePartitionInfo.getTableId();
        Partition partition = recyclePartitionInfo.getPartition();
        long partitionId = partition.getId();
        String partitionName = partition.getName();

        disableRecoverPartitionWithSameName(dbId, tableId, partitionName);

        long recycleTime = recyclePartitionInfo.isRecoverable() ? System.currentTimeMillis() : 0;
        idToRecycleTime.put(partitionId, recycleTime);
        idToPartition.put(partitionId, recyclePartitionInfo);
        LOG.info("Finished put partition '{}' to recycle bin. dbId: {} tableId: {} partitionId: {} recoverable: {}",
                partitionName, dbId, tableId, partitionId, recyclePartitionInfo.isRecoverable());
    }

    public synchronized Partition getPartition(long partitionId) {
        RecyclePartitionInfo partitionInfo = idToPartition.get(partitionId);
        if (partitionInfo != null) {
            return partitionInfo.getPartition();
        }
        return null;
    }

    public PhysicalPartition getPhysicalPartition(long physicalPartitionId) {
        for (Partition partition : idToPartition.values().stream()
                .map(RecyclePartitionInfo::getPartition)
                .collect(Collectors.toList())) {
            for (PhysicalPartition subPartition : partition.getSubPartitions()) {
                if (subPartition.getId() == physicalPartitionId) {
                    return subPartition;
                }
            }
        }
        return null;
    }

    public synchronized short getPartitionReplicationNum(long partitionId) {
        RecyclePartitionInfo partitionInfo = idToPartition.get(partitionId);
        if (partitionInfo != null) {
            return partitionInfo.getReplicationNum();
        }
        return (short) -1;
    }

    public synchronized Range<PartitionKey> getPartitionRange(long partitionId) {
        RecyclePartitionInfo partitionInfo = idToPartition.get(partitionId);
        return partitionInfo != null ? partitionInfo.getRange() : null;
    }

    public synchronized DataProperty getPartitionDataProperty(long partitionId) {
        RecyclePartitionInfo partitionInfo = idToPartition.get(partitionId);
        if (partitionInfo != null) {
            return partitionInfo.getDataProperty();
        }
        return null;
    }

    public synchronized boolean getPartitionIsInMemory(long partitionId) {
        RecyclePartitionInfo partitionInfo = idToPartition.get(partitionId);
        if (partitionInfo != null) {
            return partitionInfo.isInMemory();
        }
        return false;
    }

    public synchronized List<Partition> getPartitions(long tableId) {
        return idToPartition.values().stream()
                .filter(v -> (v.getTableId() == tableId))
                .map(RecyclePartitionInfo::getPartition)
                .collect(Collectors.toList());
    }

    /**
     * if we can erase this instance, we should check if anyone enable erase later.
     * Only used by main loop.
     */
    private synchronized boolean timeExpired(long id, long currentTimeMs) {
        long latencyMs = currentTimeMs - idToRecycleTime.get(id);
        long expireMs = max(Config.catalog_trash_expire_second * 1000L, MIN_ERASE_LATENCY);
        if (enableEraseLater.contains(id)) {
            // if enableEraseLater is set, extend the timeout by LATE_RECYCLE_INTERVAL_SECONDS
            expireMs += LATE_RECYCLE_INTERVAL_SECONDS * 1000L;
        }
        return latencyMs > expireMs;
    }

    private synchronized boolean canEraseTable(RecycleTableInfo tableInfo, long currentTimeMs) {
        if (timeExpired(tableInfo.getTable().getId(), currentTimeMs)) {
            return true;
        }

        // database is force dropped, the table can not be recovered, erase it.
        if (GlobalStateMgr.getCurrentState().getStarRocksMeta().getDbIncludeRecycleBin(tableInfo.getDbId()) == null) {
            return true;
        }
        return false;
    }

    private synchronized boolean canErasePartition(RecyclePartitionInfo partitionInfo, long currentTimeMs) {
        if (timeExpired(partitionInfo.getPartition().getId(), currentTimeMs)) {
            return true;
        }

        // database is force dropped, the partition can not be recovered, erase it.
        Database database = GlobalStateMgr.getCurrentState().getStarRocksMeta()
                .getDbIncludeRecycleBin(partitionInfo.getDbId());
        if (database == null) {
            return true;
        }

        // table is force dropped, the partition can not be recovered, erase it.
        if (GlobalStateMgr.getCurrentState().getStarRocksMeta()
                .getTableIncludeRecycleBin(database, partitionInfo.getTableId()) == null) {
            return true;
        }

        return false;
    }

    /**
     * make sure there are still some time before the subject is erased
     */
    public synchronized boolean ensureEraseLater(long id, long currentTimeMs) {
        // 1. not in idToRecycleTime, maybe already erased, sorry it's too late!
        if (!idToRecycleTime.containsKey(id)) {
            return false;
        }
        // 2. will expire after quite a long time, don't worry
        long latency = currentTimeMs - idToRecycleTime.get(id);
        if (latency < (Config.catalog_trash_expire_second - LATE_RECYCLE_INTERVAL_SECONDS) * 1000L) {
            return true;
        }
        // 3. already expired, sorry.
        if (latency > Config.catalog_trash_expire_second * 1000L) {
            return false;
        }
        enableEraseLater.add(id);
        return true;
    }

    protected synchronized void eraseDatabase(long currentTimeMs) {
        Iterator<Map.Entry<Long, RecycleDatabaseInfo>> dbIter = idToDatabase.entrySet().iterator();
        int currentEraseOpCnt = 0;
        while (dbIter.hasNext()) {
            Map.Entry<Long, RecycleDatabaseInfo> entry = dbIter.next();
            RecycleDatabaseInfo dbInfo = entry.getValue();
            Database db = dbInfo.getDb();
            if (timeExpired(db.getId(), currentTimeMs)) {
                // erase db
                dbIter.remove();
                removeRecycleMarkers(entry.getKey());

                onEraseDatabase(db.getId());
                GlobalStateMgr.getCurrentState().getEditLog().logEraseDb(db.getId());
                LOG.info("erase db[{}-{}] finished", db.getId(), db.getOriginName());
                currentEraseOpCnt++;
                if (currentEraseOpCnt >= MAX_ERASE_OPERATIONS_PER_CYCLE) {
                    break;
                }
            }
        }
    }

    private synchronized void eraseDatabaseWithSameName(String dbName) {
        Iterator<Map.Entry<Long, RecycleDatabaseInfo>> iterator = idToDatabase.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, RecycleDatabaseInfo> entry = iterator.next();
            RecycleDatabaseInfo dbInfo = entry.getValue();
            Database db = dbInfo.getDb();
            if (db.getFullName().equals(dbName)) {
                iterator.remove();
                removeRecycleMarkers(entry.getKey());

                onEraseDatabase(db.getId());
                LOG.info("erase database[{}-{}], because db with the same name db is recycled", db.getId(), dbName);
            }
        }
    }

    public synchronized void replayEraseDatabase(long dbId) {
        idToDatabase.remove(dbId);
        idToRecycleTime.remove(dbId);

        onEraseDatabase(dbId);
        LOG.info("replay erase db[{}] finished", dbId);
    }

    synchronized List<RecycleTableInfo> pickTablesToErase(long currentTimeMs) {
        List<RecycleTableInfo> l1 = Lists.newArrayList(); // Tables that support retry on delete failure
        List<RecycleTableInfo> l2 = Lists.newArrayList(); // Tables that don't support retry on delete failure
        outerLoop:
        for (Map<Long, RecycleTableInfo> tableEntry : idToTableInfo.rowMap().values()) {
            for (Map.Entry<Long, RecycleTableInfo> entry : tableEntry.entrySet()) {
                RecycleTableInfo tableInfo = entry.getValue();
                if (!canEraseTable(tableInfo, currentTimeMs)) {
                    continue;
                }
                if (tableInfo.table.isDeleteRetryable()) {
                    l1.add(tableInfo);
                } else {
                    l2.add(tableInfo);
                }
                if (l1.size()  + l2.size() >= MAX_ERASE_OPERATIONS_PER_CYCLE) {
                    break outerLoop;
                }
            }
        }
        if (!l1.isEmpty()) {
            List<Long> tableIds = l1.stream()
                    .filter(RecycleTableInfo::isRecoverable)
                    .peek(i -> i.setRecoverable(false))
                    .map(i -> i.getTable().getId())
                    .collect(Collectors.toList());
            logDisableTableRecovery(tableIds);
        }
        if (!l2.isEmpty()) {
            List<Long> tableIds = l2.stream().map(i -> i.getTable().getId()).collect(Collectors.toList());
            removeTableFromRecycleBin(tableIds);
            logEraseTables(tableIds);
        }
        return ListUtils.union(l1, l2);
    }

    private void disableTableRecovery(List<Long> tableIds) {
        for (Long tableId : tableIds) {
            RecycleTableInfo info = idToTableInfo.column(tableId).values().stream().findFirst().orElse(null);
            if (info != null) {
                info.setRecoverable(false);
            }
        }
    }

    private void disablePartitionRecovery(long partitionId) {
        RecyclePartitionInfo info = idToPartition.get(partitionId);
        if (info != null) {
            info.setRecoverable(false);
        }
    }

    static void logDisableTableRecovery(List<Long> tableIds) {
        if (tableIds.isEmpty()) {
            return;
        }
        GlobalStateMgr.getCurrentState().getEditLog().logDisableTableRecovery(tableIds);
        LOG.info("Finished log disable table recovery of tables: {}", StringUtils.join(tableIds, ","));
    }

    static void logEraseTables(List<Long> tableIds) {
        if (tableIds.isEmpty()) {
            return;
        }
        GlobalStateMgr.getCurrentState().getEditLog().logEraseMultiTables(tableIds);
        LOG.info("Finished log erase tables: {}", StringUtils.join(tableIds, ","));
    }

    private List<RecycleTableInfo> removeTableFromRecycleBin(List<Long> tableIds) {
        List<RecycleTableInfo> removedTableInfos = Lists.newArrayListWithCapacity(tableIds.size());
        for (Long tableId : tableIds) {
            Map<Long, RecycleTableInfo> column = idToTableInfo.column(tableId);
            // For different TableIds, there must be only a unique DbId
            Long dbId = column.keySet().stream().findFirst().orElse(null);
            if (dbId == null) {
                continue;
            }
            RecycleTableInfo tableInfo = idToTableInfo.remove(dbId, tableId);
            if (tableInfo != null) {
                removedTableInfos.add(tableInfo);
                nameToTableInfo.remove(dbId, tableInfo.table.getName());
            }
            idToRecycleTime.remove(tableId);
            enableEraseLater.remove(tableId);
        }
        return removedTableInfos;
    }

    private synchronized void setNextEraseMinTime(long id, long eraseTime) {
        long expireTime = max(Config.catalog_trash_expire_second * 1000L, MIN_ERASE_LATENCY);
        idToRecycleTime.replace(id, eraseTime - expireTime);
    }

    @VisibleForTesting
    void eraseTable(long currentTimeMs) {
        List<RecycleTableInfo> tableToErase = pickTablesToErase(currentTimeMs);
        if (tableToErase.isEmpty()) {
            return;
        }

        List<Long> finishedTables = Lists.newArrayList();
        for (RecycleTableInfo info : tableToErase) {
            boolean succ = info.table.deleteFromRecycleBin(info.dbId, false);
            if (!info.table.isDeleteRetryable()) {
                // Nothing to do
                continue;
            }
            Preconditions.checkState(!info.isRecoverable());
            if (succ) {
                finishedTables.add(info.table.getId());
            } else {
                setNextEraseMinTime(info.table.getId(), System.currentTimeMillis() + FAIL_RETRY_INTERVAL);
            }
        }

        removeTableFromRecycleBin(finishedTables);
        logEraseTables(finishedTables);
    }

    public synchronized void replayDisablePartitionRecovery(long partitionId) {
        disablePartitionRecovery(partitionId);
        LOG.info("Finished replay disable partition recovery. partitionId: {}", partitionId);
    }

    public synchronized void replayDisableTableRecovery(List<Long> tableIds) {
        disableTableRecovery(tableIds);
        LOG.info("Finished replay disable table recovery. table id list: {}", StringUtils.join(tableIds, ","));
    }

    public synchronized void replayEraseTable(List<Long> tableIds) {
        List<RecycleTableInfo> removedTableInfos = removeTableFromRecycleBin(tableIds);
        for (RecycleTableInfo info : removedTableInfos) {
            info.table.deleteFromRecycleBin(info.dbId, true);
        }
        LOG.info("Finished replay erase tables. table id list: {}", StringUtils.join(tableIds, ","));
    }

    protected synchronized void erasePartition(long currentTimeMs) {
        Iterator<Map.Entry<Long, RecyclePartitionInfo>> iterator = idToPartition.entrySet().iterator();
        int currentEraseOpCnt = 0;
        while (iterator.hasNext()) {
            Map.Entry<Long, RecyclePartitionInfo> entry = iterator.next();
            RecyclePartitionInfo partitionInfo = entry.getValue();
            Partition partition = partitionInfo.getPartition();

            long partitionId = entry.getKey();
            if (!canErasePartition(partitionInfo, currentTimeMs)) {
                continue;
            }
            if (partitionInfo.delete()) {
                iterator.remove();
                removeRecycleMarkers(partitionId);

                GlobalStateMgr.getCurrentState().getEditLog().logErasePartition(partitionId);

                LOG.info("Removed partition '{}' from recycle bin. dbId: {} tableId: {} partitionId: {}",
                        partition.getName(), partitionInfo.getDbId(), partitionInfo.getTableId(), partitionId);
                currentEraseOpCnt++;
                if (currentEraseOpCnt >= MAX_ERASE_OPERATIONS_PER_CYCLE) {
                    break;
                }
            } else {
                Preconditions.checkState(!partitionInfo.isRecoverable());
                setNextEraseMinTime(partitionId, System.currentTimeMillis() + FAIL_RETRY_INTERVAL);
            }
        } // end for partitions
    }

    private synchronized void disableRecoverPartitionWithSameName(long dbId, long tableId, String partitionName) {
        for (Map.Entry<Long, RecyclePartitionInfo> entry : idToPartition.entrySet()) {
            RecyclePartitionInfo partitionInfo = entry.getValue();
            if (partitionInfo.getDbId() != dbId || partitionInfo.getTableId() != tableId ||
                    !partitionInfo.getPartition().getName().equalsIgnoreCase(partitionName)) {
                continue;
            }
            partitionInfo.setRecoverable(false);
            idToRecycleTime.replace(partitionInfo.getPartition().getId(), 0L);
            break;
        }
    }

    public synchronized void replayErasePartition(long partitionId) {
        RecyclePartitionInfo partitionInfo = idToPartition.remove(partitionId);
        idToRecycleTime.remove(partitionId);

        Partition partition = partitionInfo.getPartition();
        if (!isCheckpointThread()) {
            GlobalStateMgr.getCurrentState().getStarRocksMeta().onErasePartition(partition);
        }

        LOG.info("replay erase partition[{}-{}] finished", partitionId, partition.getName());
    }

    public synchronized Database recoverDatabase(String dbName) throws DdlException {
        RecycleDatabaseInfo dbInfo = null;
        Iterator<Map.Entry<Long, RecycleDatabaseInfo>> iterator = idToDatabase.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, RecycleDatabaseInfo> entry = iterator.next();
            if (dbName.equals(entry.getValue().getDb().getFullName())) {
                dbInfo = entry.getValue();
                break;
            }
        }

        if (dbInfo == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        // 1. recover all tables in this db
        recoverAllTables(dbInfo);

        Database db = dbInfo.getDb();
        // 2. remove db from idToDatabase and idToRecycleTime
        idToDatabase.remove(db.getId());
        removeRecycleMarkers(db.getId());

        return db;
    }

    public synchronized Database replayRecoverDatabase(long dbId) {
        RecycleDatabaseInfo dbInfo = idToDatabase.get(dbId);

        try {
            recoverAllTables(dbInfo);
        } catch (DdlException e) {
            // should not happend
            LOG.error("failed replay recover database: {}", dbId, e);
        }

        idToDatabase.remove(dbId);
        idToRecycleTime.remove(dbId);

        return dbInfo.getDb();
    }

    private void recoverAllTables(RecycleDatabaseInfo dbInfo) throws DdlException {
        if (dbInfo == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_NO_DB_ERROR);
        }
        Database db = dbInfo.getDb();
        Set<String> tableNames = Sets.newHashSet(dbInfo.getTableNames());
        long dbId = db.getId();
        Iterator<Map.Entry<Long, RecycleTableInfo>> iterator = idToTableInfo.row(dbId).entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, RecycleTableInfo> entry = iterator.next();
            RecycleTableInfo tableInfo = entry.getValue();
            if (tableInfo.getDbId() != dbId || !tableNames.contains(tableInfo.getTable().getName())) {
                continue;
            }
            if (!tableInfo.isRecoverable()) {
                LOG.info("Table '{}' of id {} is not recoverable", tableInfo.getTable().getName(),
                        tableInfo.table.getId());
                continue;
            }

            Table table = tableInfo.getTable();
            db.registerTableUnlocked(table);
            LOG.info("Recovered table '{}' to database '{}'. tableId: {} dbId: {}", table.getName(),
                    db.getOriginName(), table.getId(), dbId);
            iterator.remove();
            nameToTableInfo.remove(dbId, table.getName());
            removeRecycleMarkers(table.getId());
            tableNames.remove(table.getName());
        }

        if (!tableNames.isEmpty()) {
            throw new DdlException(String.format(
                    "Unable to recover database '%s' because some tables have been deleted: %s",
                    db.getOriginName(), StringUtils.join(tableNames, ",")));
        }
    }

    public synchronized boolean recoverTable(Database db, String tableName) {
        // make sure to get db lock
        long dbId = db.getId();
        Map<String, RecycleTableInfo> nameToTableInfoDbLevel = nameToTableInfo.row(dbId);
        RecycleTableInfo recycleTableInfo = nameToTableInfoDbLevel.get(tableName);
        if (recycleTableInfo == null || !recycleTableInfo.isRecoverable()) {
            return false;
        }

        Table table = recycleTableInfo.getTable();
        db.registerTableUnlocked(table);
        nameToTableInfoDbLevel.remove(tableName);
        idToTableInfo.row(dbId).remove(table.getId());
        removeRecycleMarkers(table.getId());

        // log
        RecoverInfo recoverInfo = new RecoverInfo(dbId, table.getId(), -1L);
        GlobalStateMgr.getCurrentState().getEditLog().logRecoverTable(recoverInfo);
        LOG.info("recover db[{}] with table[{}]: {}", dbId, table.getId(), table.getName());
        return true;
    }

    public synchronized void replayRecoverTable(RecoverInfo recoverInfo) {
        long dbId = recoverInfo.getDbId();
        long tableId = recoverInfo.getTableId();

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        Locker locker = new Locker();
        locker.lockDatabase(dbId, LockType.WRITE);
        try {
            // make sure to get db write lock
            Map<Long, RecycleTableInfo> idToTableInfoDbLevel = idToTableInfo.row(dbId);
            RecycleTableInfo tableInfo = idToTableInfoDbLevel.get(tableId);
            Preconditions.checkState(tableInfo.getDbId() == db.getId());
            Table table = tableInfo.getTable();
            db.registerTableUnlocked(table);
            nameToTableInfo.row(dbId).remove(table.getName());
            idToTableInfoDbLevel.remove(tableId);
            idToRecycleTime.remove(tableInfo.getTable().getId());
            LOG.info("replay recover table[{}-{}] finished", tableId, tableInfo.getTable().getName());
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }

    }

    public synchronized void recoverPartition(long dbId, OlapTable table, String partitionName) throws DdlException {
        // make sure to get db write lock
        RecyclePartitionInfo recoverPartitionInfo = null;

        for (Map.Entry<Long, RecyclePartitionInfo> entry : idToPartition.entrySet()) {
            RecyclePartitionInfo partitionInfo = entry.getValue();

            if (partitionInfo.getTableId() != table.getId()) {
                continue;
            }

            if (!partitionInfo.getPartition().getName().equalsIgnoreCase(partitionName)) {
                continue;
            }

            if (!partitionInfo.isRecoverable()) {
                LOG.info("Found a partition named '{}', but it cannot be recovered", partitionName);
                continue;
            }

            recoverPartitionInfo = partitionInfo;
            break;
        }

        if (recoverPartitionInfo == null) {
            throw new DdlException(String.format("No partition named '%s' in recycle bin that belongs to table '%s'",
                    partitionName, table.getName()));
        }

        recoverPartitionInfo.recover(table);

        long partitionId = recoverPartitionInfo.getPartition().getId();

        // remove from recycle bin
        idToPartition.remove(partitionId);
        removeRecycleMarkers(partitionId);

        // log
        RecoverInfo recoverInfo = new RecoverInfo(dbId, table.getId(), partitionId);
        GlobalStateMgr.getCurrentState().getEditLog().logRecoverPartition(recoverInfo);
        LOG.info("Recovered partition '{}' of table '{}'. dbId={} tableId={} partitionId={}", partitionName,
                table.getName(), dbId, table.getId(), partitionId);
    }

    // The caller should keep db write lock
    public synchronized void replayRecoverPartition(OlapTable table, long partitionId) {
        Iterator<Map.Entry<Long, RecyclePartitionInfo>> iterator = idToPartition.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, RecyclePartitionInfo> entry = iterator.next();
            RecyclePartitionInfo partitionInfo = entry.getValue();
            if (partitionInfo.getPartition().getId() != partitionId) {
                continue;
            }

            Preconditions.checkState(partitionInfo.getTableId() == table.getId());

            table.addPartition(partitionInfo.getPartition());
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) table.getPartitionInfo();
            rangePartitionInfo.setRange(partitionId, false, partitionInfo.getRange());
            rangePartitionInfo.setDataProperty(partitionId, partitionInfo.getDataProperty());
            rangePartitionInfo.setReplicationNum(partitionId, partitionInfo.getReplicationNum());
            rangePartitionInfo.setIsInMemory(partitionId, partitionInfo.isInMemory());

            if (table.isCloudNativeTable()) {
                rangePartitionInfo.setDataCacheInfo(partitionId,
                        ((RecyclePartitionInfoV2) partitionInfo).getDataCacheInfo());
            }

            iterator.remove();
            idToRecycleTime.remove(partitionId);

            LOG.info("replay recover partition[{}-{}] finished", partitionId, partitionInfo.getPartition().getName());
            break;
        }
    }

    // no need to use synchronized.
    // only called when loading image
    public void addTabletToInvertedIndex() {
        // no need to handle idToDatabase. Database is already empty before being put here

        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();

        // idToTable
        for (RecycleTableInfo tableInfo : idToTableInfo.values()) {
            Table table = tableInfo.getTable();
            if (!table.isNativeTableOrMaterializedView()) {
                continue;
            }

            long dbId = tableInfo.getDbId();
            OlapTable olapTable = (OlapTable) table;
            long tableId = olapTable.getId();
            for (Partition partition : olapTable.getAllPartitions()) {
                long partitionId = partition.getId();
                DataProperty dataProperty = olapTable.getPartitionInfo().getDataProperty(partitionId);
                if (dataProperty == null) {
                    LOG.warn("can not find data property for table: {}, partitionId: {} ", table.getName(), partitionId);
                    continue;
                }
                TStorageMedium medium = dataProperty.getStorageMedium();
                for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                    long physicalPartitionId = physicalPartition.getId();
                    for (MaterializedIndex index : physicalPartition.getMaterializedIndices(IndexExtState.ALL)) {
                        long indexId = index.getId();
                        int schemaHash = olapTable.getSchemaHashByIndexId(indexId);
                        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, physicalPartitionId, indexId, schemaHash, medium,
                                table.isCloudNativeTable());

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
                }
            } // end for partitions
        }

        // idToPartition
        for (RecyclePartitionInfo partitionInfo : idToPartition.values()) {
            long dbId = partitionInfo.getDbId();
            long tableId = partitionInfo.getTableId();
            Partition partition = partitionInfo.getPartition();
            long partitionId = partition.getId();

            // we need to get olap table to get schema hash info
            // first find it in globalStateMgr. if not found, it should be in recycle bin
            OlapTable olapTable = null;
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
            if (db == null) {
                // just log. db should be in recycle bin
                if (!idToDatabase.containsKey(dbId)) {
                    LOG.error("db[{}] is neither in globalStateMgr nor in recycle bin"
                                    + " when rebuilding inverted index from recycle bin, partition[{}]",
                            dbId, partitionId);
                    continue;
                }
            } else {
                olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
            }

            if (olapTable == null) {
                if (!idToTableInfo.row(dbId).containsKey(tableId)) {
                    LOG.error("table[{}] is neither in globalStateMgr nor in recycle bin"
                                    + " when rebuilding inverted index from recycle bin, partition[{}]",
                            tableId, partitionId);
                    continue;
                }
                RecycleTableInfo tableInfo = idToTableInfo.row(dbId).get(tableId);
                olapTable = (OlapTable) tableInfo.getTable();
            }
            Preconditions.checkNotNull(olapTable);
            // storage medium should be got from RecyclePartitionInfo, not from olap table. because olap table
            // does not have this partition any more
            TStorageMedium medium = partitionInfo.getDataProperty().getStorageMedium();
            for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                long physicalPartitionId = physicalPartition.getId();
                for (MaterializedIndex index : physicalPartition.getMaterializedIndices(IndexExtState.ALL)) {
                    long indexId = index.getId();
                    int schemaHash = olapTable.getSchemaHashByIndexId(indexId);
                    TabletMeta tabletMeta = new TabletMeta(dbId, tableId, physicalPartitionId, indexId, schemaHash, medium,
                            olapTable.isCloudNativeTable());
                    for (Tablet tablet : index.getTablets()) {
                        long tabletId = tablet.getId();
                        invertedIndex.addTablet(tabletId, tabletMeta);
                        if (olapTable.isOlapTableOrMaterializedView()) {
                            for (Replica replica : ((LocalTablet) tablet).getImmutableReplicas()) {
                                invertedIndex.addReplica(tabletId, replica);
                            }
                        }
                    }
                } // end for indices
            } // end for partitions
        }
    }

    public void removeInvalidateReference() {
        // privilege object can be invalidated after gc
        GlobalStateMgr.getCurrentState().getAuthorizationMgr().removeInvalidObject();
    }

    @Override
    protected void runAfterCatalogReady() {
        long currentTimeMs = System.currentTimeMillis();
        // should follow the partition/table/db order
        // in case of partition(table) is still in recycle bin but table(db) is missing
        try {
            erasePartition(currentTimeMs);
            // synchronized is unfair lock, sleep here allows other high-priority operations to obtain a lock
            Thread.sleep(100);
            eraseTable(currentTimeMs);
            Thread.sleep(100);
            eraseDatabase(currentTimeMs);
            removeInvalidateReference();
        } catch (InterruptedException e) {
            LOG.warn("Failed to execute runAfterCatalogReady", e);
        }
    }

    @VisibleForTesting
    synchronized boolean isContainedInidToRecycleTime(long id) {
        return idToRecycleTime.get(id) != null;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        int count = idToDatabase.size();
        out.writeInt(count);
        for (Map.Entry<Long, RecycleDatabaseInfo> entry : idToDatabase.entrySet()) {
            out.writeLong(entry.getKey());
            entry.getValue().write(out);
        }

        count = idToTableInfo.size();
        out.writeInt(count);

        for (Map<Long, RecycleTableInfo> tableEntry : idToTableInfo.rowMap().values()) {
            for (Map.Entry<Long, RecycleTableInfo> entry : tableEntry.entrySet()) {
                out.writeLong(entry.getKey());
                entry.getValue().write(out);
            }
        }

        count = idToPartition.size();
        out.writeInt(count);
        for (Map.Entry<Long, RecyclePartitionInfo> entry : idToPartition.entrySet()) {
            out.writeLong(entry.getKey());
            entry.getValue().write(out);
        }

        count = idToRecycleTime.size();
        out.writeInt(count);
        for (Map.Entry<Long, Long> entry : idToRecycleTime.entrySet()) {
            out.writeLong(entry.getKey());
            out.writeLong(entry.getValue());
        }
    }

    public static class RecycleDatabaseInfo implements Writable {
        @SerializedName("d")
        private Database db;
        @SerializedName("t")
        private Set<String> tableNames;

        public RecycleDatabaseInfo() {
            tableNames = Sets.newHashSet();
        }

        public RecycleDatabaseInfo(Database db, Set<String> tableNames) {
            this.db = db;
            this.tableNames = tableNames;
        }

        public Database getDb() {
            return db;
        }

        public Set<String> getTableNames() {
            return tableNames;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            db.write(out);

            int count = tableNames.size();
            out.writeInt(count);
            for (String tableName : tableNames) {
                Text.writeString(out, tableName);
            }
        }
    }

    static class RecycleTableInfo implements Writable {
        @SerializedName(value = "i")
        private long dbId;
        @SerializedName(value = "t")
        private Table table;
        // Whether this table can be recovered later.
        @SerializedName(value = "r")
        private boolean recoverable;

        public RecycleTableInfo() {
            // The default constructor is called when deserializing from json to `RecycleTableInfo` object.
            // Because `recoverable` does not exist in the old versions, the value of this field may not be set
            // during json deserialization, so here it is set to true.
            this.recoverable = true;
        }

        public RecycleTableInfo(long dbId, Table table, boolean recoverable) {
            this.dbId = dbId;
            this.table = table;
            this.recoverable = recoverable;
        }

        public long getDbId() {
            return dbId;
        }

        public Table getTable() {
            return table;
        }

        public void setRecoverable(boolean recoverable) {
            this.recoverable = recoverable;
        }

        public boolean isRecoverable() {
            return recoverable;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(dbId);
            table.write(out);
        }
    }

    public synchronized List<Long> getAllDbIds() {
        return Lists.newArrayList(idToDatabase.keySet());
    }

    public void save(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
        int numJson = 1 + idToDatabase.size() + 1 + idToTableInfo.size()
                + 1 + idToPartition.size() + 1;
        SRMetaBlockWriter writer = imageWriter.getBlockWriter(SRMetaBlockID.CATALOG_RECYCLE_BIN, numJson);

        writer.writeInt(idToDatabase.size());
        for (RecycleDatabaseInfo recycleDatabaseInfo : idToDatabase.values()) {
            writer.writeJson(recycleDatabaseInfo);
        }

        writer.writeInt(idToTableInfo.size());
        for (Map<Long, RecycleTableInfo> tableEntry : idToTableInfo.rowMap().values()) {
            for (RecycleTableInfo recycleTableInfo : tableEntry.values()) {
                writer.writeJson(recycleTableInfo);
            }
        }

        writer.writeInt(idToPartition.size());
        for (RecyclePartitionInfo recyclePartitionInfo : idToPartition.values()) {
            if (recyclePartitionInfo instanceof RecyclePartitionInfoV1) {
                RecyclePartitionInfoV1 recyclePartitionInfoV1 = (RecyclePartitionInfoV1) recyclePartitionInfo;
                RecycleRangePartitionInfo recycleRangePartitionInfo = new RecycleRangePartitionInfo(
                        recyclePartitionInfoV1.dbId, recyclePartitionInfoV1.tableId, recyclePartitionInfoV1.partition,
                        recyclePartitionInfoV1.range, recyclePartitionInfoV1.dataProperty,
                        recyclePartitionInfoV1.replicationNum,
                        recyclePartitionInfoV1.isInMemory, null);
                writer.writeJson(recycleRangePartitionInfo);
            } else {
                writer.writeJson(recyclePartitionInfo);
            }
        }

        writer.writeJson(idToRecycleTime);

        writer.close();
    }

    public void load(SRMetaBlockReader reader) throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        int idToDatabaseSize = reader.readInt();
        for (int i = 0; i < idToDatabaseSize; ++i) {
            RecycleDatabaseInfo recycleDatabaseInfo = reader.readJson(RecycleDatabaseInfo.class);
            idToDatabase.put(recycleDatabaseInfo.db.getId(), recycleDatabaseInfo);
        }

        int idToTableInfoSize = reader.readInt();
        for (int i = 0; i < idToTableInfoSize; ++i) {
            RecycleTableInfo recycleTableInfo = reader.readJson(RecycleTableInfo.class);
            idToTableInfo.put(recycleTableInfo.dbId, recycleTableInfo.table.getId(), recycleTableInfo);
            nameToTableInfo.put(recycleTableInfo.getDbId(), recycleTableInfo.getTable().getName(), recycleTableInfo);
        }

        int idToPartitionSize = reader.readInt();
        for (int i = 0; i < idToPartitionSize; ++i) {
            RecyclePartitionInfo recyclePartitionInfo = reader.readJson(RecyclePartitionInfoV2.class);
            if (recyclePartitionInfo instanceof IForwardCompatibleObject) {
                // Ignore the future unknown subtype derived from RecyclePartitionInfoV2
                LOG.warn("Ignore unknown partition type(partitionId: {}) from the future version!",
                        recyclePartitionInfo.getPartition().getId());
                continue;
            }
            idToPartition.put(recyclePartitionInfo.partition.getId(), recyclePartitionInfo);
        }

        idToRecycleTime = (Map<Long, Long>) reader.readJson(new TypeToken<Map<Long, Long>>() {
        }.getType());

        if (!isCheckpointThread()) {
            // add tablet in Recycle bin to TabletInvertedIndex
            addTabletToInvertedIndex();
        }
        // create DatabaseTransactionMgr for db in recycle bin.
        // these dbs do not exist in `idToDb` of the globalStateMgr.
        for (Long dbId : getAllDbIds()) {
            GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().addDatabaseTransactionMgr(dbId);
        }
    }

    // Construct a table name that is illegal for the user and unique within the system.
    private static String uniqueIllegalTableName(String tableName, long tableId) {
        return String.format("%s@%s", tableName, tableId);
    }

    public static long getFailRetryInterval() {
        return FAIL_RETRY_INTERVAL;
    }

    public static long getMaxEraseOperationsPerCycle() {
        return MAX_ERASE_OPERATIONS_PER_CYCLE;
    }

    public static long getMinEraseLatency() {
        return MIN_ERASE_LATENCY;
    }
}
