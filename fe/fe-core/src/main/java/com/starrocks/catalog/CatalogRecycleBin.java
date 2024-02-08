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
import com.starrocks.common.util.RangeUtils;
import com.starrocks.lake.DataCacheInfo;
import com.starrocks.persist.RecoverInfo;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonPreProcessable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TStorageMedium;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

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

    public synchronized boolean recycleDatabase(Database db, Set<String> tableNames) {
        if (idToDatabase.containsKey(db.getId())) {
            LOG.error("db[{}-{}] already in recycle bin.", db.getId(), db.getOriginName());
            return false;
        }

        // db should be empty. all tables are recycled before
        Preconditions.checkState(db.getTables().isEmpty());

        // erase db with same name
        eraseDatabaseWithSameName(db.getFullName());

        // recycle db
        RecycleDatabaseInfo databaseInfo = new RecycleDatabaseInfo(db, tableNames);
        idToDatabase.put(db.getId(), databaseInfo);
        idToRecycleTime.put(db.getId(), System.currentTimeMillis());
        LOG.info("recycle db[{}-{}]", db.getId(), db.getOriginName());
        return true;
    }

    public synchronized Database getDatabase(long dbId) {
        RecycleDatabaseInfo databaseInfo = idToDatabase.get(dbId);
        if (databaseInfo != null) {
            return databaseInfo.getDb();
        }
        return null;
    }

    public synchronized Table recycleTable(long dbId, Table table) {
        if (idToTableInfo.row(dbId).containsKey(table.getId())) {
            LOG.error("table[{}-{}] already in recycle bin.", table.getId(), table.getName());
            return null;
        }

        // erase table with same name
        Table oldTable = eraseTableWithSameName(dbId, table.getName());

        // recycle table
        RecycleTableInfo tableInfo = new RecycleTableInfo(dbId, table);
        idToRecycleTime.put(table.getId(), System.currentTimeMillis());
        idToTableInfo.put(dbId, table.getId(), tableInfo);
        nameToTableInfo.put(dbId, table.getName(), tableInfo);
        LOG.info("recycle table[{}-{}]", table.getId(), table.getName());
        return oldTable;
    }

    public synchronized Table getTable(long dbId, long tableId) {
        RecycleTableInfo tableInfo = idToTableInfo.row(dbId).get(tableId);
        if (tableInfo != null) {
            return tableInfo.getTable();
        }
        return null;
    }

    public synchronized List<Table> getTables(long dbId) {
        return idToTableInfo.row(dbId).values().stream()
                .map(RecycleTableInfo::getTable)
                .collect(Collectors.toList());
    }

    public synchronized boolean recyclePartition(long dbId, long tableId,
                                                 Partition partition, Range<PartitionKey> range,
                                                 DataProperty dataProperty,
                                                 short replicationNum,
                                                 boolean isInMemory,
                                                 DataCacheInfo dataCacheInfo) {
        if (idToPartition.containsKey(partition.getId())) {
            LOG.error("partition[{}-{}] already in recycle bin.", partition.getId(), partition.getName());
            return false;
        }

        // erase partition with same name
        erasePartitionWithSameName(dbId, tableId, partition.getName());

        // recycle partition
        RecyclePartitionInfo partitionInfo = new RecycleRangePartitionInfo(dbId, tableId, partition,
                range, dataProperty, replicationNum, isInMemory, dataCacheInfo);

        idToRecycleTime.put(partition.getId(), System.currentTimeMillis());
        idToPartition.put(partition.getId(), partitionInfo);
        LOG.info("recycle partition[{}-{}]", partition.getId(), partition.getName());
        return true;
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
        if (partitionInfo != null) {
            if (partitionInfo instanceof RecyclePartitionInfoV1) {
                return ((RecyclePartitionInfoV1) partitionInfo).getRange();
            } else {
                return ((RecycleRangePartitionInfo) partitionInfo).getRange();
            }
        }
        return null;
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
        if (GlobalStateMgr.getCurrentState().getLocalMetastore().getDbIncludeRecycleBin(tableInfo.getDbId()) == null) {
            return true;
        }
        return false;
    }

    private synchronized boolean canErasePartition(RecyclePartitionInfo partitionInfo, long currentTimeMs) {
        if (timeExpired(partitionInfo.getPartition().getId(), currentTimeMs)) {
            return true;
        }

        // database is force dropped, the partition can not be recovered, erase it.
        Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDbIncludeRecycleBin(partitionInfo.getDbId());
        if (database == null) {
            return true;
        }

        // table is force dropped, the partition can not be recovered, erase it.
        if (GlobalStateMgr.getCurrentState().getLocalMetastore()
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

                GlobalStateMgr.getCurrentState().getLocalMetastore().onEraseDatabase(db.getId());
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

                GlobalStateMgr.getCurrentState().getLocalMetastore().onEraseDatabase(db.getId());
                LOG.info("erase database[{}-{}], because db with the same name db is recycled", db.getId(), dbName);
            }
        }
    }

    public synchronized void replayEraseDatabase(long dbId) {
        idToDatabase.remove(dbId);
        idToRecycleTime.remove(dbId);

        GlobalStateMgr.getCurrentState().getLocalMetastore().onEraseDatabase(dbId);
        LOG.info("replay erase db[{}] finished", dbId);
    }

    @VisibleForTesting
    public synchronized List<RecycleTableInfo> eraseTable(long currentTimeMs) {
        List<RecycleTableInfo> tableToRemove = Lists.newArrayList();
        int currentEraseOpCnt = 0;
        for (Map<Long, RecycleTableInfo> tableEntry : idToTableInfo.rowMap().values()) {
            for (Map.Entry<Long, RecycleTableInfo> entry : tableEntry.entrySet()) {
                RecycleTableInfo tableInfo = entry.getValue();

                if (canEraseTable(tableInfo, currentTimeMs)
                        || GlobalStateMgr.getCurrentState().getLocalMetastore().getDbIncludeRecycleBin(tableInfo.dbId) == null) {
                    tableToRemove.add(tableInfo);
                    currentEraseOpCnt++;
                    if (currentEraseOpCnt >= MAX_ERASE_OPERATIONS_PER_CYCLE) {
                        break;
                    }
                }
            } // end for tables
        }

        List<Long> tableIdList = Lists.newArrayList();
        if (!tableToRemove.isEmpty()) {
            for (RecycleTableInfo tableInfo : tableToRemove) {
                Table table = tableInfo.getTable();
                long tableId = table.getId();
                GlobalStateMgr.getCurrentState().getLocalMetastore().removeAutoIncrementIdByTableId(tableId, false);
                removeRecycleMarkers(tableId);
                nameToTableInfo.remove(tableInfo.dbId, table.getName());
                idToTableInfo.remove(tableInfo.dbId, tableId);
                tableIdList.add(tableId);
            }
            GlobalStateMgr.getCurrentState().getEditLog().logEraseMultiTables(tableIdList);
            LOG.info("multi erase write log finished. erased {} table(s)", currentEraseOpCnt);
        }
        return tableToRemove;
    }

    private synchronized Table eraseTableWithSameName(long dbId, String tableName) {
        Map<String, RecycleTableInfo> nameToTableInfoDBLevel = nameToTableInfo.row(dbId);
        RecycleTableInfo tableInfo = nameToTableInfoDBLevel.get(tableName);
        if (tableInfo == null) {
            return null;
        }
        Table table = tableInfo.getTable();
        nameToTableInfoDBLevel.remove(tableName);
        idToTableInfo.row(dbId).remove(table.getId());
        removeRecycleMarkers(table.getId());
        LOG.info("erase table[{}-{}], because table with the same name is recycled", table.getId(), tableName);
        return table;
    }

    public synchronized void replayEraseTable(long tableId) {
        Map<Long, RecycleTableInfo> column = idToTableInfo.column(tableId);
        // For different TableIds, there must be only a unique DbId
        Optional<Long> dbIdOpt = column.keySet().stream().findFirst();
        if (dbIdOpt.isPresent()) {
            Long dbId = dbIdOpt.get();
            RecycleTableInfo tableInfo = idToTableInfo.remove(dbId, tableId);
            if (tableInfo != null) {
                Runnable runnable = null;
                Table table = tableInfo.getTable();
                GlobalStateMgr.getCurrentState().getLocalMetastore().removeAutoIncrementIdByTableId(tableId, true);
                nameToTableInfo.remove(dbId, table.getName());
                runnable = table.delete(true);
                if (!isCheckpointThread() && runnable != null) {
                    runnable.run();
                }
            }
        }
        idToRecycleTime.remove(tableId);
        LOG.info("replay erase table[{}] finished", tableId);
    }

    protected synchronized void erasePartition(long currentTimeMs) {
        Iterator<Map.Entry<Long, RecyclePartitionInfo>> iterator = idToPartition.entrySet().iterator();
        int currentEraseOpCnt = 0;
        while (iterator.hasNext()) {
            Map.Entry<Long, RecyclePartitionInfo> entry = iterator.next();
            RecyclePartitionInfo partitionInfo = entry.getValue();
            Partition partition = partitionInfo.getPartition();

            long partitionId = entry.getKey();
            if (canErasePartition(partitionInfo, currentTimeMs)) {
                GlobalStateMgr.getCurrentState().getLocalMetastore().onErasePartition(partition);
                // erase partition
                iterator.remove();
                removeRecycleMarkers(partitionId);

                // log
                GlobalStateMgr.getCurrentState().getEditLog().logErasePartition(partitionId);
                LOG.info("erase partition[{}-{}] finished", partitionId, partition.getName());
                currentEraseOpCnt++;
                if (currentEraseOpCnt >= MAX_ERASE_OPERATIONS_PER_CYCLE) {
                    break;
                }
            }
        } // end for partitions
    }

    private synchronized void erasePartitionWithSameName(long dbId, long tableId, String partitionName) {
        Iterator<Map.Entry<Long, RecyclePartitionInfo>> iterator = idToPartition.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, RecyclePartitionInfo> entry = iterator.next();
            RecyclePartitionInfo partitionInfo = entry.getValue();
            if (partitionInfo.getDbId() != dbId || partitionInfo.getTableId() != tableId) {
                continue;
            }

            Partition partition = partitionInfo.getPartition();
            if (partition.getName().equals(partitionName)) {
                GlobalStateMgr.getCurrentState().getLocalMetastore().onErasePartition(partition);
                iterator.remove();
                removeRecycleMarkers(entry.getKey());
                LOG.info("erase partition[{}-{}] finished, because partition with the same name is recycled",
                        partition.getId(), partitionName);
            }
        }
    }

    public synchronized void replayErasePartition(long partitionId) {
        RecyclePartitionInfo partitionInfo = idToPartition.remove(partitionId);
        idToRecycleTime.remove(partitionId);

        Partition partition = partitionInfo.getPartition();
        if (!isCheckpointThread()) {
            GlobalStateMgr.getCurrentState().getLocalMetastore().onErasePartition(partition);
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

            Table table = tableInfo.getTable();
            db.registerTableUnlocked(table);
            LOG.info("recover db[{}] with table[{}]: {}", dbId, table.getId(), table.getName());
            iterator.remove();
            nameToTableInfo.remove(dbId, table.getName());
            removeRecycleMarkers(table.getId());
            tableNames.remove(table.getName());
        }

        if (!tableNames.isEmpty()) {
            throw new DdlException("Tables[" + tableNames + "] is missing. Can not recover db");
        }
    }

    public synchronized boolean recoverTable(Database db, String tableName) {
        // make sure to get db lock
        long dbId = db.getId();
        Map<String, RecycleTableInfo> nameToTableInfoDbLevel = nameToTableInfo.row(dbId);
        RecycleTableInfo recycleTableInfo = nameToTableInfoDbLevel.get(tableName);
        if (recycleTableInfo == null) {
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

    public synchronized void replayRecoverTable(Database db, long tableId) {
        // make sure to get db write lock
        long dbId = db.getId();
        Map<Long, RecycleTableInfo> idToTableInfoDbLevel = idToTableInfo.row(dbId);
        RecycleTableInfo tableInfo = idToTableInfoDbLevel.get(tableId);
        Preconditions.checkState(tableInfo.getDbId() == db.getId());
        Table table = tableInfo.getTable();
        db.registerTableUnlocked(table);
        nameToTableInfo.row(dbId).remove(table.getName());
        idToTableInfoDbLevel.remove(tableId);
        idToRecycleTime.remove(tableInfo.getTable().getId());
        LOG.info("replay recover table[{}-{}] finished", tableId, tableInfo.getTable().getName());
    }

    public synchronized void recoverPartition(long dbId, OlapTable table, String partitionName) throws DdlException {
        // make sure to get db write lock
        RecyclePartitionInfo recoverPartitionInfo = null;

        Iterator<Map.Entry<Long, RecyclePartitionInfo>> iterator = idToPartition.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, RecyclePartitionInfo> entry = iterator.next();
            RecyclePartitionInfo partitionInfo = entry.getValue();

            if (partitionInfo.getTableId() != table.getId()) {
                continue;
            }

            if (!partitionInfo.getPartition().getName().equalsIgnoreCase(partitionName)) {
                continue;
            }

            recoverPartitionInfo = partitionInfo;
            break;
        }

        if (recoverPartitionInfo == null) {
            throw new DdlException("No partition named " + partitionName + " in table " + table.getName());
        }

        // check if range is invalid
        Range<PartitionKey> recoverRange = null;
        if (recoverPartitionInfo instanceof RecyclePartitionInfoV1) {
            recoverRange = ((RecyclePartitionInfoV1) recoverPartitionInfo).getRange();
        } else {
            recoverRange = ((RecycleRangePartitionInfo) recoverPartitionInfo).getRange();
        }
        RangePartitionInfo partitionInfo = (RangePartitionInfo) table.getPartitionInfo();
        if (partitionInfo.getAnyIntersectRange(recoverRange, false) != null) {
            throw new DdlException("Can not recover partition[" + partitionName + "]. Range conflict.");
        }

        // recover partition
        Partition recoverPartition = recoverPartitionInfo.getPartition();
        Preconditions.checkState(recoverPartition.getName().equalsIgnoreCase(partitionName));
        table.addPartition(recoverPartition);

        // recover partition info
        long partitionId = recoverPartition.getId();
        partitionInfo.setRange(partitionId, false, recoverRange);
        partitionInfo.setDataProperty(partitionId, recoverPartitionInfo.getDataProperty());
        partitionInfo.setReplicationNum(partitionId, recoverPartitionInfo.getReplicationNum());
        partitionInfo.setIsInMemory(partitionId, recoverPartitionInfo.isInMemory());
        if (table.isCloudNativeTable()) {
            partitionInfo.setDataCacheInfo(partitionId,
                    ((RecyclePartitionInfoV2) recoverPartitionInfo).getDataCacheInfo());
        }

        // remove from recycle bin
        idToPartition.remove(partitionId);
        removeRecycleMarkers(partitionId);

        // log
        RecoverInfo recoverInfo = new RecoverInfo(dbId, table.getId(), partitionId);
        GlobalStateMgr.getCurrentState().getEditLog().logRecoverPartition(recoverInfo);
        LOG.info("recover partition[{}], name: {}", partitionId, partitionName);
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
            if (partitionInfo instanceof RecyclePartitionInfoV1) {
                rangePartitionInfo.setRange(partitionId, false, ((RecyclePartitionInfoV1) partitionInfo).getRange());
            } else {
                rangePartitionInfo.setRange(partitionId, false, ((RecycleRangePartitionInfo) partitionInfo).getRange());
            }
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
            Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
            if (db == null) {
                // just log. db should be in recycle bin
                if (!idToDatabase.containsKey(dbId)) {
                    LOG.error("db[{}] is neither in globalStateMgr nor in recycle bin"
                                    + " when rebuilding inverted index from recycle bin, partition[{}]",
                            dbId, partitionId);
                    continue;
                }
            } else {
                olapTable = (OlapTable) db.getTable(tableId);
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
            List<RecycleTableInfo> recycleTableInfos = eraseTable(currentTimeMs);
            postProcessEraseTable(recycleTableInfos);
            Thread.sleep(100);
            eraseDatabase(currentTimeMs);
            removeInvalidateReference();
        } catch (InterruptedException e) {
            LOG.warn(e);
        }
    }

    private void postProcessEraseTable(List<RecycleTableInfo> tableToRemove) {
        for (RecycleTableInfo tableInfo : tableToRemove) {
            Table table = tableInfo.getTable();
            Runnable runnable = table.delete(false);
            if (runnable != null) {
                runnable.run();
            }
            LOG.info("erased table [{}-{}].", table.getId(), table.getName());
        }
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

    public void readFields(DataInput in) throws IOException {
        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            long id = in.readLong();
            RecycleDatabaseInfo dbInfo = new RecycleDatabaseInfo();
            dbInfo.readFields(in);
            idToDatabase.put(id, dbInfo);
        }

        count = in.readInt();
        for (int i = 0; i < count; i++) {
            long id = in.readLong();
            RecycleTableInfo tableInfo = new RecycleTableInfo();
            tableInfo.readFields(in);
            idToTableInfo.put(tableInfo.getDbId(), id, tableInfo);
            nameToTableInfo.put(tableInfo.getDbId(), tableInfo.getTable().getName(), tableInfo);
        }

        count = in.readInt();
        for (int i = 0; i < count; i++) {
            long id = in.readLong();
            long dbId = in.readLong();
            RecyclePartitionInfo partitionInfo;
            // dbId is used to distinguish if it is lake table. We write a -1 in it.
            // If this dbId < 0, then the table is a lake table, so use RecyclePartitionInfoV2 to read it
            // else, it is an olap table, just read it as usual, but because dbId has been read, set it into partitionInfo
            // and read the remain part
            if (dbId < 0) {
                partitionInfo = RecyclePartitionInfoV2.read(in);
            } else {
                RecyclePartitionInfoV1 v1 = new RecyclePartitionInfoV1();
                v1.readFields(in);
                v1.setDbId(dbId);
                partitionInfo = v1;
            }
            idToPartition.put(id, partitionInfo);
        }

        count = in.readInt();
        for (int i = 0; i < count; i++) {
            long id = in.readLong();
            long time = in.readLong();
            idToRecycleTime.put(id, time);
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

        public void readFields(DataInput in) throws IOException {
            db = Database.read(in);

            int count = in.readInt();
            for (int i = 0; i < count; i++) {
                String tableName = Text.readString(in);
                tableNames.add(tableName);
            }
        }
    }

    private static class RecycleTableInfo implements Writable {
        @SerializedName(value = "i")
        private long dbId;
        @SerializedName(value = "t")
        private Table table;

        public RecycleTableInfo() {
            // for persist
        }

        public RecycleTableInfo(long dbId, Table table) {
            this.dbId = dbId;
            this.table = table;
        }

        public long getDbId() {
            return dbId;
        }

        public Table getTable() {
            return table;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(dbId);
            table.write(out);
        }

        public void readFields(DataInput in) throws IOException {
            dbId = in.readLong();
            table = Table.read(in);
        }
    }

    private abstract static class RecyclePartitionInfo implements Writable {
        @SerializedName(value = "dbId")
        protected long dbId;
        @SerializedName(value = "tableId")
        protected long tableId;
        @SerializedName(value = "partition")
        protected Partition partition;
        @SerializedName(value = "dataProperty")
        protected DataProperty dataProperty;
        @SerializedName(value = "replicationNum")
        protected short replicationNum;
        @SerializedName(value = "isInMemory")
        protected boolean isInMemory;

        protected RecyclePartitionInfo() {
        }

        protected RecyclePartitionInfo(long dbId, long tableId, Partition partition,
                                       DataProperty dataProperty, short replicationNum,
                                       boolean isInMemory) {
            this.dbId = dbId;
            this.tableId = tableId;
            this.partition = partition;
            this.dataProperty = dataProperty;
            this.replicationNum = replicationNum;
            this.isInMemory = isInMemory;
        }

        public long getDbId() {
            return dbId;
        }

        public long getTableId() {
            return tableId;
        }

        public Partition getPartition() {
            return partition;
        }

        public DataProperty getDataProperty() {
            return dataProperty;
        }

        public short getReplicationNum() {
            return replicationNum;
        }

        public boolean isInMemory() {
            return isInMemory;
        }

        public void setDbId(long dbId) {
            this.dbId = dbId;
        }
    }

    // only for RangePartition
    public static class RecyclePartitionInfoV1 extends RecyclePartitionInfo {
        private Range<PartitionKey> range;

        public RecyclePartitionInfoV1() {
            super();
        }

        public RecyclePartitionInfoV1(long dbId, long tableId, Partition partition, Range<PartitionKey> range,
                                      DataProperty dataProperty, short replicationNum, boolean isInMemory) {
            super(dbId, tableId, partition, dataProperty, replicationNum, isInMemory);
            this.range = range;
        }

        public Range<PartitionKey> getRange() {
            return range;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(dbId);
            out.writeLong(tableId);
            partition.write(out);
            RangeUtils.writeRange(out, range);
            dataProperty.write(out);
            out.writeShort(replicationNum);
            out.writeBoolean(isInMemory);
        }

        public void readFields(DataInput in) throws IOException {
            // dbId has been read in CatalogRecycleBin.readFields()
            tableId = in.readLong();
            partition = Partition.read(in);
            range = RangeUtils.readRange(in);
            dataProperty = DataProperty.read(in);
            replicationNum = in.readShort();
            isInMemory = in.readBoolean();
        }
    }

    public static class RecyclePartitionInfoV2 extends RecyclePartitionInfo {
        @SerializedName(value = "storageCacheInfo")
        private DataCacheInfo dataCacheInfo;

        public RecyclePartitionInfoV2(long dbId, long tableId, Partition partition,
                                      DataProperty dataProperty, short replicationNum, boolean isInMemory,
                                      DataCacheInfo dataCacheInfo) {
            super(dbId, tableId, partition, dataProperty, replicationNum, isInMemory);
            this.dataCacheInfo = dataCacheInfo;
        }

        public DataCacheInfo getDataCacheInfo() {
            return dataCacheInfo;
        }

        public static RecyclePartitionInfoV2 read(DataInput in) throws IOException {
            String json = Text.readString(in);
            return GsonUtils.GSON.fromJson(json, RecyclePartitionInfoV2.class);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(-1L);
            String json = GsonUtils.GSON.toJson(this);
            Text.writeString(out, json);
        }
    }

    public static class RecycleRangePartitionInfo extends RecyclePartitionInfoV2 implements GsonPreProcessable,
            GsonPostProcessable {

        private Range<PartitionKey> range;
        // because Range<PartitionKey> and PartitionKey can not be serialized by gson
        // ATTN: call preSerialize before serialization and postDeserialized after deserialization
        @SerializedName(value = "serializedRange")
        private byte[] serializedRange;

        public RecycleRangePartitionInfo(long dbId, long tableId, Partition partition, Range<PartitionKey> range,
                                         DataProperty dataProperty, short replicationNum, boolean isInMemory,
                                         DataCacheInfo dataCacheInfo) {
            super(dbId, tableId, partition, dataProperty, replicationNum,
                    isInMemory, dataCacheInfo);
            this.range = range;
        }

        public Range<PartitionKey> getRange() {
            return range;
        }

        private byte[] serializeRange(Range<PartitionKey> range) throws IOException {
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(stream);
            RangeUtils.writeRange(dos, range);
            return stream.toByteArray();
        }

        private Range<PartitionKey> deserializeRange(byte[] serializedRange) throws IOException {
            InputStream inputStream = new ByteArrayInputStream(serializedRange);
            DataInput dataInput = new DataInputStream(inputStream);
            return RangeUtils.readRange(dataInput);
        }

        @Override
        public void gsonPreProcess() throws IOException {
            serializedRange = serializeRange(range);
        }

        @Override
        public void gsonPostProcess() throws IOException {
            range = deserializeRange(serializedRange);
        }
    }

    public synchronized List<Long> getAllDbIds() {
        return Lists.newArrayList(idToDatabase.keySet());
    }

    public long loadRecycleBin(DataInputStream dis, long checksum) throws IOException {
        readFields(dis);
        if (!isCheckpointThread()) {
            // add tablet in Recycle bin to TabletInvertedIndex
            addTabletToInvertedIndex();
        }
        // create DatabaseTransactionMgr for db in recycle bin.
        // these dbs do not exist in `idToDb` of the globalStateMgr.
        for (Long dbId : getAllDbIds()) {
            GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().addDatabaseTransactionMgr(dbId);
        }
        LOG.info("finished replay recycleBin from image");
        return checksum;
    }

    public long saveRecycleBin(DataOutputStream dos, long checksum) throws IOException {
        write(dos);
        return checksum;
    }

    public void save(DataOutputStream dos) throws IOException, SRMetaBlockException {
        int numJson = 1 + idToDatabase.size() + 1 + idToTableInfo.size()
                + 1 + idToPartition.size() + 1;
        SRMetaBlockWriter writer = new SRMetaBlockWriter(dos, SRMetaBlockID.CATALOG_RECYCLE_BIN, numJson);

        writer.writeJson(idToDatabase.size());
        for (RecycleDatabaseInfo recycleDatabaseInfo : idToDatabase.values()) {
            writer.writeJson(recycleDatabaseInfo);
        }

        writer.writeJson(idToTableInfo.size());
        for (Map<Long, RecycleTableInfo> tableEntry : idToTableInfo.rowMap().values()) {
            for (RecycleTableInfo recycleTableInfo : tableEntry.values()) {
                writer.writeJson(recycleTableInfo);
            }
        }

        writer.writeJson(idToPartition.size());
        for (RecyclePartitionInfo recyclePartitionInfo : idToPartition.values()) {
            if (recyclePartitionInfo instanceof RecyclePartitionInfoV1) {
                RecyclePartitionInfoV1 recyclePartitionInfoV1 = (RecyclePartitionInfoV1) recyclePartitionInfo;
                RecycleRangePartitionInfo recycleRangePartitionInfo = new RecycleRangePartitionInfo(
                        recyclePartitionInfoV1.dbId, recyclePartitionInfoV1.tableId, recyclePartitionInfoV1.partition,
                        recyclePartitionInfoV1.range, recyclePartitionInfoV1.dataProperty, recyclePartitionInfoV1.replicationNum,
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
            RecycleRangePartitionInfo recycleRangePartitionInfo = reader.readJson(RecycleRangePartitionInfo.class);
            idToPartition.put(recycleRangePartitionInfo.partition.getId(), recycleRangePartitionInfo);
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
}