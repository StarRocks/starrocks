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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/qe/ConnectProcessor.java

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

package com.starrocks.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.catalog.CatalogRecycleBin;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.system.information.InfoSchemaDb;
import com.starrocks.catalog.system.sys.SysDb;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.MaterializedViewExceptions;
import com.starrocks.common.Pair;
import com.starrocks.common.util.DynamicPartitionUtil;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.memory.MemoryTrackable;
import com.starrocks.meta.LocalMetastoreInterface;
import com.starrocks.mv.MVMetaVersionRepairer;
import com.starrocks.mv.MVRepairHandler;
import com.starrocks.persist.AddPartitionsInfoV2;
import com.starrocks.persist.AddSubPartitionsInfoV2;
import com.starrocks.persist.AutoIncrementInfo;
import com.starrocks.persist.ColumnRenameInfo;
import com.starrocks.persist.CreateDbInfo;
import com.starrocks.persist.CreateTableInfo;
import com.starrocks.persist.DatabaseInfo;
import com.starrocks.persist.DropDbInfo;
import com.starrocks.persist.DropInfo;
import com.starrocks.persist.DropPartitionInfo;
import com.starrocks.persist.DropPartitionsInfo;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.ListPartitionPersistInfo;
import com.starrocks.persist.ModifyTableColumnOperationLog;
import com.starrocks.persist.ModifyTablePropertyOperationLog;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.PartitionPersistInfoV2;
import com.starrocks.persist.PartitionVersionRecoveryInfo;
import com.starrocks.persist.PhysicalPartitionPersistInfoV2;
import com.starrocks.persist.RangePartitionPersistInfo;
import com.starrocks.persist.RecoverInfo;
import com.starrocks.persist.ReplacePartitionOperationLog;
import com.starrocks.persist.TableInfo;
import com.starrocks.persist.TruncateTableInfo;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.sql.ast.AlterDatabaseQuotaStmt;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.SingleRangePartitionDesc;
import com.starrocks.sql.optimizer.statistics.IDictManager;
import com.starrocks.thrift.TStorageMedium;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;

import static com.starrocks.meta.StarRocksMeta.inactiveRelatedMaterializedView;
import static com.starrocks.server.GlobalStateMgr.NEXT_ID_INIT_VALUE;
import static com.starrocks.server.GlobalStateMgr.isCheckpointThread;

public class LocalMetastore implements MVRepairHandler, MemoryTrackable, LocalMetastoreInterface {
    private static final Logger LOG = LogManager.getLogger(LocalMetastore.class);

    private final ConcurrentHashMap<Long, Database> idToDb = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Database> fullNameToDb = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, Long> tableIdToIncrementId = new ConcurrentHashMap<>();

    private final GlobalStateMgr stateMgr;
    private final CatalogRecycleBin recycleBin;

    public LocalMetastore(GlobalStateMgr globalStateMgr, CatalogRecycleBin recycleBin) {
        this.stateMgr = globalStateMgr;
        this.recycleBin = recycleBin;

        // put built-in database into local metastore
        InfoSchemaDb infoSchemaDb = new InfoSchemaDb();
        Preconditions.checkState(infoSchemaDb.getId() < NEXT_ID_INIT_VALUE,
                "InfoSchemaDb id shouldn't larger than " + NEXT_ID_INIT_VALUE);
        idToDb.put(infoSchemaDb.getId(), infoSchemaDb);
        fullNameToDb.put(infoSchemaDb.getFullName(), infoSchemaDb);

        SysDb starRocksDb = new SysDb();
        Preconditions.checkState(starRocksDb.getId() < NEXT_ID_INIT_VALUE,
                "starrocks id shouldn't larger than " + NEXT_ID_INIT_VALUE);
        idToDb.put(starRocksDb.getId(), starRocksDb);
        fullNameToDb.put(starRocksDb.getFullName(), starRocksDb);
    }

    @Override
    public void createDb(Database db, String storageVolumeId) {
        unprotectCreateDb(db);
        GlobalStateMgr.getCurrentState().getEditLog().logCreateDb(db, storageVolumeId);
    }

    // For replay edit log, needn't lock metadata
    public void unprotectCreateDb(Database db) {
        idToDb.put(db.getId(), db);
        fullNameToDb.put(db.getFullName(), db);
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        db.setExist(true);
        locker.unLockDatabase(db.getId(), LockType.WRITE);
        stateMgr.getGlobalTransactionMgr().addDatabaseTransactionMgr(db.getId());
    }

    public void replayCreateDb(CreateDbInfo createDbInfo) {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        LocalMetastore localMetastore = globalStateMgr.getLocalMetastore();

        globalStateMgr.tryLock(true);
        try {
            Database db = new Database(createDbInfo.getId(), createDbInfo.getDbName());
            localMetastore.unprotectCreateDb(db);
            GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().addDatabaseTransactionMgr(db.getId());

            // If user upgrades from 3.0, the storage volume id will be null
            if (createDbInfo.getStorageVolumeId() != null) {
                globalStateMgr.getStorageVolumeMgr().replayBindDbToStorageVolume(createDbInfo.getStorageVolumeId(), db.getId());
            }
            LOG.info("finish replay create db, name: {}, id: {}", db.getOriginName(), db.getId());
        } finally {
            globalStateMgr.unlock();
        }
    }

    @Override
    public void dropDb(Database db, boolean isForceDrop) {
        // 3. remove db from globalStateMgr
        idToDb.remove(db.getId());
        fullNameToDb.remove(db.getFullName());
        unprotectDropDb(db, isForceDrop, false);

        DropDbInfo info = new DropDbInfo(db.getFullName(), isForceDrop);
        GlobalStateMgr.getCurrentState().getEditLog().logDropDb(info);
    }

    @NotNull
    public void unprotectDropDb(Database db, boolean isForeDrop, boolean isReplay) {
        for (Table table : db.getTables()) {
            db.unprotectDropTable(table.getId(), isForeDrop, isReplay);
            if (isForeDrop) {
                // Normally we should delete table after releasing the database lock, to
                // avoid occupying the database lock for too long. However, at this point
                // we are in the process of dropping the database, which means that the
                // database should no longer be in use, and occupying the database lock
                // should have no effect.
                table.delete(db.getId(), isReplay);
            }
        }
    }

    public void replayDropDb(String dbName, boolean isForceDrop) {
        GlobalStateMgr.getCurrentState().tryLock(true);
        try {
            Database db = fullNameToDb.get(dbName);
            Locker locker = new Locker();
            locker.lockDatabase(db.getId(), LockType.WRITE);
            try {
                Set<String> tableNames = new HashSet(db.getTableNamesViewWithLock());
                unprotectDropDb(db, isForceDrop, true);
                recycleBin.recycleDatabase(db, tableNames, isForceDrop);
                db.setExist(false);
            } finally {
                locker.unLockDatabase(db.getId(), LockType.WRITE);
            }

            fullNameToDb.remove(dbName);
            idToDb.remove(db.getId());

            LOG.info("finish replay drop db, name: {}, id: {}", dbName, db.getId());
        } finally {
            GlobalStateMgr.getCurrentState().unlock();
        }
    }

    @Override
    public void recoverDatabase(Database db) {
        unprotectCreateDb(db);
        // log
        RecoverInfo recoverInfo = new RecoverInfo(db.getId(), -1L, -1L);
        GlobalStateMgr.getCurrentState().getEditLog().logRecoverDb(recoverInfo);
    }

    public void replayRecoverDatabase(RecoverInfo info) {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();

        long dbId = info.getDbId();
        Database db = recycleBin.replayRecoverDatabase(dbId);
        globalStateMgr.tryLock(true);
        try {
            unprotectCreateDb(db);
            LOG.info("finish replay create db, name: {}, id: {}", db.getOriginName(), db.getId());
        } finally {
            globalStateMgr.unlock();
        }

        LOG.info("replay recover db[{}], name: {}", dbId, db.getOriginName());
    }

    @Override
    public void alterDatabaseQuota(DatabaseInfo dbInfo) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbInfo.getDbName());

        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            replayAlterDatabaseQuota(dbInfo);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }

        GlobalStateMgr.getCurrentState().getEditLog().logAlterDb(dbInfo);
    }

    public void replayAlterDatabaseQuota(DatabaseInfo dbInfo) {
        String dbName = dbInfo.getDbName();
        LOG.info("Begin to unprotect alter db info {}", dbName);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        AlterDatabaseQuotaStmt.QuotaType quotaType = dbInfo.getQuotaType();
        long quota = dbInfo.getQuota();

        Preconditions.checkNotNull(db);
        if (quotaType == AlterDatabaseQuotaStmt.QuotaType.DATA) {
            db.setDataQuota(quota);
        } else if (quotaType == AlterDatabaseQuotaStmt.QuotaType.REPLICA) {
            db.setReplicaQuota(quota);
        }
    }

    @Override
    public void renameDatabase(String dbName, String newDbName) {
        replayRenameDatabase(dbName, newDbName);
        DatabaseInfo dbInfo = new DatabaseInfo(dbName, newDbName, -1L, AlterDatabaseQuotaStmt.QuotaType.NONE);
        GlobalStateMgr.getCurrentState().getEditLog().logDatabaseRename(dbInfo);
    }

    public void replayRenameDatabase(String dbName, String newDbName) {
        GlobalStateMgr.getCurrentState().tryLock(true);
        try {
            Database db = fullNameToDb.get(dbName);
            db.setName(newDbName);
            fullNameToDb.remove(dbName);
            fullNameToDb.put(newDbName, db);

            LOG.info("replay rename database {} to {}, id: {}", dbName, newDbName, db.getId());
        } finally {
            GlobalStateMgr.getCurrentState().unlock();
        }
    }

    @Override
    public List<String> listDbNames() {
        return Lists.newArrayList(fullNameToDb.keySet());
    }

    @Override
    public ConcurrentHashMap<Long, Database> getIdToDb() {
        return idToDb;
    }

    @Override
    public List<Long> getDbIds() {
        return Lists.newArrayList(idToDb.keySet());
    }

    @Override
    public ConcurrentHashMap<String, Database> getFullNameToDb() {
        return fullNameToDb;
    }

    @Override
    public Database getDb(String name) {
        if (name == null) {
            return null;
        }
        if (fullNameToDb.containsKey(name)) {
            return fullNameToDb.get(name);
        } else {
            // This maybe an information_schema db request, and information_schema db name is case-insensitive.
            // So, we first extract db name to check if it is information_schema.
            // Then we reassemble the origin cluster name with lower case db name,
            // and finally get information_schema db from the name map.
            String dbName = ClusterNamespace.getNameFromFullName(name);
            if (dbName.equalsIgnoreCase(InfoSchemaDb.DATABASE_NAME) ||
                    dbName.equalsIgnoreCase(SysDb.DATABASE_NAME)) {
                return fullNameToDb.get(dbName.toLowerCase());
            }
        }
        return null;
    }

    @Override
    public Database getDb(long dbId) {
        return idToDb.get(dbId);
    }

    @Override
    public void createTable(CreateTableInfo createTableInfo) {
        GlobalStateMgr.getCurrentState().getEditLog().logCreateTable(createTableInfo);
    }

    public void replayCreateTable(CreateTableInfo info) {
        Table table = info.getTable();
        Database db = this.fullNameToDb.get(info.getDbName());
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            db.registerTableUnlocked(table);
            if (table.isTemporaryTable()) {
                TemporaryTableMgr temporaryTableMgr = GlobalStateMgr.getCurrentState().getTemporaryTableMgr();
                UUID sessionId = ((OlapTable) table).getSessionId();
                temporaryTableMgr.addTemporaryTable(sessionId, db.getId(), table.getName(), table.getId());
            }
            table.onReload();
        } catch (Throwable e) {
            LOG.error("replay create table failed: {}", table, e);
            // Rethrow, we should not eat the exception when replaying editlog.
            throw e;
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }

        if (!isCheckpointThread()) {
            // add to inverted index
            if (table.isOlapOrCloudNativeTable() || table.isMaterializedView()) {
                TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
                OlapTable olapTable = (OlapTable) table;
                long dbId = db.getId();
                long tableId = table.getId();
                for (PhysicalPartition partition : olapTable.getAllPhysicalPartitions()) {
                    long physicalPartitionId = partition.getId();
                    TStorageMedium medium = olapTable.getPartitionInfo().getDataProperty(
                            partition.getParentId()).getStorageMedium();
                    for (MaterializedIndex mIndex : partition
                            .getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                        long indexId = mIndex.getId();
                        int schemaHash = olapTable.getSchemaHashByIndexId(indexId);
                        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partition.getParentId(), physicalPartitionId,
                                indexId, schemaHash, medium, table.isCloudNativeTableOrMaterializedView());
                        for (Tablet tablet : mIndex.getTablets()) {
                            long tabletId = tablet.getId();
                            invertedIndex.addTablet(tabletId, tabletMeta);
                            if (tablet instanceof LocalTablet) {
                                for (Replica replica : ((LocalTablet) tablet).getImmutableReplicas()) {
                                    invertedIndex.addReplica(tabletId, replica);
                                }
                            }
                        }
                    }
                } // end for partitions

                DynamicPartitionUtil.registerOrRemovePartitionScheduleInfo(db.getId(), olapTable);
            }
        }

        // If user upgrades from 3.0, the storage volume id will be null
        if (table.isCloudNativeTableOrMaterializedView() && info.getStorageVolumeId() != null) {
            GlobalStateMgr.getCurrentState().getStorageVolumeMgr()
                    .replayBindTableToStorageVolume(info.getStorageVolumeId(), table.getId());
        }
    }

    @Override
    public void dropTable(DropInfo dropInfo) {
        GlobalStateMgr.getCurrentState().getEditLog().logDropTable(dropInfo);
    }

    public void replayDropTable(Database db, long tableId, boolean isForceDrop) {
        Table table;
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            table = getTable(db.getId(), tableId);
            if (table.isTemporaryTable()) {
                table = db.unprotectDropTemporaryTable(tableId, isForceDrop, false);
                UUID sessionId = ((OlapTable) table).getSessionId();
                TemporaryTableMgr temporaryTableMgr = GlobalStateMgr.getCurrentState().getTemporaryTableMgr();
                temporaryTableMgr.dropTemporaryTable(sessionId, db.getId(), table.getName());
            } else {
                table = db.unprotectDropTable(tableId, isForceDrop, true);
            }
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
        if (table != null && isForceDrop) {
            table.delete(db.getId(), true);
        }
    }

    @Override
    public void renameTable(TableInfo tableInfo) {
        GlobalStateMgr.getCurrentState().getEditLog().logTableRename(tableInfo);
    }

    public void replayRenameTable(TableInfo tableInfo) {
        long dbId = tableInfo.getDbId();
        long tableId = tableInfo.getTableId();
        String newTableName = tableInfo.getNewTableName();

        Database db = getDb(dbId);
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            OlapTable table = (OlapTable) getTable(db.getId(), tableId);
            String tableName = table.getName();
            db.dropTable(tableName);
            table.setName(newTableName);
            db.registerTableUnlocked(table);
            inactiveRelatedMaterializedView(db, table,
                    MaterializedViewExceptions.inactiveReasonForBaseTableRenamed(tableName));

            LOG.info("replay rename table[{}] to {}, tableId: {}", tableName, newTableName, table.getId());
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
    }

    @Override
    public void truncateTable(TruncateTableInfo info) {
        GlobalStateMgr.getCurrentState().getEditLog().logTruncateTable(info);
    }

    public void truncateTableInternal(OlapTable olapTable, List<Partition> newPartitions,
                                      boolean isEntireTable, boolean isReplay) {
        // use new partitions to replace the old ones.
        Set<Tablet> oldTablets = Sets.newHashSet();
        for (Partition newPartition : newPartitions) {
            Partition oldPartition = olapTable.replacePartition(newPartition);
            for (PhysicalPartition physicalPartition : oldPartition.getSubPartitions()) {
                // save old tablets to be removed
                for (MaterializedIndex index : physicalPartition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                    // let HashSet do the deduplicate work
                    oldTablets.addAll(index.getTablets());
                }
            }
        }

        if (isEntireTable) {
            // drop all temp partitions
            olapTable.dropAllTempPartitions();
        }

        // remove the tablets in old partitions
        for (Tablet tablet : oldTablets) {
            TabletInvertedIndex index = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
            index.deleteTablet(tablet.getId());
            // Ensure that only the leader records truncate information.
            // TODO(yangzaorang): the information will be lost when failover occurs. The probability of this case
            // happening is small, and the trash data will be deleted by BE anyway, but we need to find a better
            // solution.
            if (!isReplay) {
                index.markTabletForceDelete(tablet);
            }
        }
    }

    public void replayTruncateTable(TruncateTableInfo info) {
        Database db = getDb(info.getDbId());
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            OlapTable olapTable = (OlapTable) getTable(db.getId(), info.getTblId());
            truncateTableInternal(olapTable, info.getPartitions(), info.isEntireTable(), true);

            if (!GlobalStateMgr.isCheckpointThread()) {
                // add tablet to inverted index
                TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
                for (Partition partition : info.getPartitions()) {
                    long partitionId = partition.getId();
                    TStorageMedium medium = olapTable.getPartitionInfo().getDataProperty(
                            partitionId).getStorageMedium();
                    for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                        for (MaterializedIndex mIndex : physicalPartition.getMaterializedIndices(
                                MaterializedIndex.IndexExtState.ALL)) {
                            long indexId = mIndex.getId();
                            int schemaHash = olapTable.getSchemaHashByIndexId(indexId);
                            TabletMeta tabletMeta = new TabletMeta(db.getId(), olapTable.getId(),
                                    physicalPartition.getId(), indexId, schemaHash, medium,
                                    olapTable.isCloudNativeTableOrMaterializedView());
                            for (Tablet tablet : mIndex.getTablets()) {
                                long tabletId = tablet.getId();
                                invertedIndex.addTablet(tabletId, tabletMeta);
                                if (olapTable.isOlapTable()) {
                                    for (Replica replica : ((LocalTablet) tablet).getImmutableReplicas()) {
                                        invertedIndex.addReplica(tabletId, replica);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
    }

    @Override
    public void modifyTableProperty(Database db, OlapTable table, Map<String, String> properties, short operationType) {
        ModifyTablePropertyOperationLog info =
                new ModifyTablePropertyOperationLog(db.getId(), table.getId(), properties);

        switch (operationType) {
            case OperationType.OP_MODIFY_IN_MEMORY:
                GlobalStateMgr.getCurrentState().getEditLog().logModifyInMemory(info);
                break;
            case OperationType.OP_MODIFY_WRITE_QUORUM:
                GlobalStateMgr.getCurrentState().getEditLog().logModifyWriteQuorum(info);
                break;
        }
    }

    @Override
    public void alterTable(ModifyTablePropertyOperationLog log) {
        GlobalStateMgr.getCurrentState().getEditLog().logAlterTableProperties(log);
    }

    @Override
    public List<String> listTableNames(String dbName) {
        Database database = getDb(dbName);
        if (database != null) {
            return database.getTables().stream()
                    .map(Table::getName).collect(Collectors.toList());
        } else {
            throw new StarRocksConnectorException("Database " + dbName + " doesn't exist");
        }
    }

    @Override
    public List<Table> getTables(Long dbId) {
        Database database = getDb(dbId);
        if (database == null) {
            return Collections.emptyList();
        } else {
            return database.getTables();
        }
    }

    @Override
    public Table getTable(String dbName, String tblName) {
        Database database = getDb(dbName);
        if (database == null) {
            return null;
        }
        return database.getTable(tblName);
    }

    @Override
    public Table getTable(Long dbId, Long tableId) {
        Database database = getDb(dbId);
        if (database == null) {
            return null;
        }
        return database.getTable(tableId);
    }

    @Override
    public void addPartitionLog(Database db, OlapTable olapTable, List<PartitionDesc> partitionDescs,
                                boolean isTempPartition, PartitionInfo partitionInfo,
                                List<Partition> partitionList, Set<String> existPartitionNameSet)
            throws DdlException {
        PartitionType partitionType = partitionInfo.getType();
        if (partitionInfo.isRangePartition()) {
            addRangePartitionLog(db, olapTable, partitionDescs, isTempPartition, partitionInfo, partitionList,
                    existPartitionNameSet);
        } else if (partitionType == PartitionType.LIST) {
            addListPartitionLog(db, olapTable, partitionDescs, isTempPartition, partitionInfo, partitionList,
                    existPartitionNameSet);
        } else {
            throw new DdlException("Only support adding partition log to range/list partitioned table");
        }
    }

    private void addRangePartitionLog(Database db, OlapTable olapTable, List<PartitionDesc> partitionDescs,
                                      boolean isTempPartition, PartitionInfo partitionInfo,
                                      List<Partition> partitionList, Set<String> existPartitionNameSet) {
        int partitionLen = partitionList.size();
        List<PartitionPersistInfoV2> partitionInfoV2List = Lists.newArrayListWithCapacity(partitionLen);
        if (partitionLen == 1) {
            Partition partition = partitionList.get(0);
            if (existPartitionNameSet.contains(partition.getName())) {
                LOG.info("add partition[{}] which already exists", partition.getName());
                return;
            }
            PartitionPersistInfoV2 info = new RangePartitionPersistInfo(db.getId(), olapTable.getId(), partition,
                    partitionDescs.get(0).getPartitionDataProperty(),
                    partitionInfo.getReplicationNum(partition.getId()),
                    partitionInfo.getIsInMemory(partition.getId()), isTempPartition,
                    ((RangePartitionInfo) partitionInfo).getRange(partition.getId()),
                    ((SingleRangePartitionDesc) partitionDescs.get(0)).getDataCacheInfo());
            partitionInfoV2List.add(info);
            AddPartitionsInfoV2 infos = new AddPartitionsInfoV2(partitionInfoV2List);
            GlobalStateMgr.getCurrentState().getEditLog().logAddPartitions(infos);

            LOG.info("succeed in creating partition[{}], name: {}, temp: {}", partition.getId(),
                    partition.getName(), isTempPartition);
        } else {
            for (int i = 0; i < partitionLen; i++) {
                Partition partition = partitionList.get(i);
                if (!existPartitionNameSet.contains(partition.getName())) {
                    PartitionPersistInfoV2 info = new RangePartitionPersistInfo(db.getId(), olapTable.getId(),
                            partition, partitionDescs.get(i).getPartitionDataProperty(),
                            partitionInfo.getReplicationNum(partition.getId()),
                            partitionInfo.getIsInMemory(partition.getId()), isTempPartition,
                            ((RangePartitionInfo) partitionInfo).getRange(partition.getId()),
                            ((SingleRangePartitionDesc) partitionDescs.get(i)).getDataCacheInfo());

                    partitionInfoV2List.add(info);
                }
            }

            AddPartitionsInfoV2 infos = new AddPartitionsInfoV2(partitionInfoV2List);
            GlobalStateMgr.getCurrentState().getEditLog().logAddPartitions(infos);

            for (PartitionPersistInfoV2 infoV2 : partitionInfoV2List) {
                LOG.info("succeed in creating partition[{}], name: {}, temp: {}", infoV2.getPartition().getId(),
                        infoV2.getPartition().getName(), isTempPartition);
            }
        }
    }

    @VisibleForTesting
    public void addListPartitionLog(Database db, OlapTable olapTable, List<PartitionDesc> partitionDescs,
                                    boolean isTempPartition, PartitionInfo partitionInfo,
                                    List<Partition> partitionList, Set<String> existPartitionNameSet)
            throws DdlException {
        if (partitionList == null) {
            throw new DdlException("partitionList should not null");
        } else if (partitionList.size() == 0) {
            return;
        }

        // TODO: add only 1 log for multi list partition
        int i = 0;
        for (Partition partition : partitionList) {
            if (existPartitionNameSet.contains(partition.getName())) {
                LOG.info("add partition[{}] which already exists", partition.getName());
                continue;
            }
            long partitionId = partition.getId();
            PartitionPersistInfoV2 info = new ListPartitionPersistInfo(db.getId(), olapTable.getId(), partition,
                    partitionDescs.get(i).getPartitionDataProperty(),
                    partitionInfo.getReplicationNum(partitionId),
                    partitionInfo.getIsInMemory(partitionId),
                    isTempPartition,
                    ((ListPartitionInfo) partitionInfo).getIdToValues().get(partitionId),
                    ((ListPartitionInfo) partitionInfo).getIdToMultiValues().get(partitionId),
                    partitionDescs.get(i).getDataCacheInfo());
            GlobalStateMgr.getCurrentState().getEditLog().logAddPartition(info);
            LOG.info("succeed in creating list partition[{}], name: {}, temp: {}", partitionId,
                    partition.getName(), isTempPartition);
            i++;
        }
    }

    public void replayAddPartition(PartitionPersistInfoV2 info) throws DdlException {
        Database db = this.getDb(info.getDbId());
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            OlapTable olapTable = (OlapTable) getTable(db.getId(), info.getTableId());
            Partition partition = info.getPartition();

            PartitionInfo partitionInfo = olapTable.getPartitionInfo();
            if (info.isTempPartition()) {
                olapTable.addTempPartition(partition);
            } else {
                olapTable.addPartition(partition);
            }

            PartitionType partitionType = partitionInfo.getType();
            if (partitionType == PartitionType.LIST) {
                try {
                    ((ListPartitionInfo) partitionInfo).unprotectHandleNewPartitionDesc(
                            olapTable.getIdToColumn(), info.asListPartitionPersistInfo());
                } catch (AnalysisException e) {
                    throw new DdlException(e.getMessage());
                }
            } else if (partitionInfo.isRangePartition()) {
                ((RangePartitionInfo) partitionInfo).unprotectHandleNewSinglePartitionDesc(
                        info.asRangePartitionPersistInfo());
            } else if (partitionType == PartitionType.UNPARTITIONED) {
                // insert overwrite job will create temp partition and replace the single partition.
                partitionInfo.addPartition(partition.getId(), info.getDataProperty(), info.getReplicationNum(),
                        info.isInMemory(), info.getDataCacheInfo());
            } else {
                throw new DdlException("Unsupported partition type: " + partitionType.name());
            }

            if (!isCheckpointThread()) {
                // add to inverted index
                TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
                for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                    for (MaterializedIndex index :
                            physicalPartition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                        long indexId = index.getId();
                        int schemaHash = olapTable.getSchemaHashByIndexId(indexId);
                        TabletMeta tabletMeta = new TabletMeta(info.getDbId(), info.getTableId(), partition.getId(),
                                index.getId(), schemaHash, info.getDataProperty().getStorageMedium());
                        for (Tablet tablet : index.getTablets()) {
                            long tabletId = tablet.getId();
                            invertedIndex.addTablet(tabletId, tabletMeta);
                            // modify some logic
                            if (tablet instanceof LocalTablet) {
                                for (Replica replica : ((LocalTablet) tablet).getImmutableReplicas()) {
                                    invertedIndex.addReplica(tabletId, replica);
                                }
                            }
                        }
                    }
                }
            }
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
    }

    @Override
    public void dropPartition(DropPartitionInfo dropPartitionInfo) {
        GlobalStateMgr.getCurrentState().getEditLog().logDropPartition(dropPartitionInfo);
    }

    public void replayDropPartition(DropPartitionInfo info) {
        Database db = this.getDb(info.getDbId());
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            OlapTable olapTable = (OlapTable) getTable(db.getId(), info.getTableId());
            if (info.isTempPartition()) {
                olapTable.dropTempPartition(info.getPartitionName(), true);
            } else {
                olapTable.dropPartition(info.getDbId(), info.getPartitionName(), info.isForceDrop());
            }
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
    }

    @Override
    public void dropPartitions(DropPartitionsInfo dropPartitionInfo) {
        GlobalStateMgr.getCurrentState().getEditLog().logDropPartitions(dropPartitionInfo);
    }

    public void replayDropPartitions(DropPartitionsInfo info) {
        Database db = this.getDb(info.getDbId());
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            LOG.info("Begin to unprotect drop partitions. db = " + info.getDbId()
                    + " table = " + info.getTableId()
                    + " partitionNames = " + info.getPartitionNames());
            List<String> partitionNames = info.getPartitionNames();
            OlapTable olapTable = (OlapTable) getTable(db.getId(), info.getTableId());
            boolean isTempPartition = info.isTempPartition();
            long dbId = info.getDbId();
            boolean isForceDrop = info.isForceDrop();
            partitionNames.stream().forEach(partitionName -> {
                if (isTempPartition) {
                    olapTable.dropTempPartition(partitionName, true);
                } else {
                    olapTable.dropPartition(dbId, partitionName, isForceDrop);
                }
            });
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
    }

    @Override
    public void renamePartition(TableInfo tableInfo) {
        GlobalStateMgr.getCurrentState().getEditLog().logPartitionRename(tableInfo);
    }

    public void replayRenamePartition(TableInfo tableInfo) {
        long dbId = tableInfo.getDbId();
        long tableId = tableInfo.getTableId();
        long partitionId = tableInfo.getPartitionId();
        String newPartitionName = tableInfo.getNewPartitionName();

        Database db = getDb(dbId);
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            OlapTable table = (OlapTable) getTable(db.getId(), tableId);
            Partition partition = table.getPartition(partitionId);
            table.renamePartition(partition.getName(), newPartitionName);
            LOG.info("replay rename partition[{}] to {}", partition.getName(), newPartitionName);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
    }

    @Override
    public void replaceTempPartition(ReplacePartitionOperationLog info) {
        GlobalStateMgr.getCurrentState().getEditLog().logReplaceTempPartition(info);
    }

    public void replayReplaceTempPartition(ReplacePartitionOperationLog replaceTempPartitionLog) {
        Database db = getDb(replaceTempPartitionLog.getDbId());
        if (db == null) {
            return;
        }
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            OlapTable olapTable = (OlapTable) getTable(db.getId(), replaceTempPartitionLog.getTblId());
            if (olapTable == null) {
                return;
            }
            if (replaceTempPartitionLog.isUnPartitionedTable()) {
                olapTable.replacePartition(replaceTempPartitionLog.getPartitions().get(0),
                        replaceTempPartitionLog.getTempPartitions().get(0));
                return;
            }
            olapTable.replaceTempPartitions(replaceTempPartitionLog.getPartitions(),
                    replaceTempPartitionLog.getTempPartitions(),
                    replaceTempPartitionLog.isStrictRange(),
                    replaceTempPartitionLog.useTempPartitionName());
        } catch (DdlException e) {
            LOG.warn("should not happen.", e);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
    }

    public void replayRecoverPartition(RecoverInfo info) {
        long dbId = info.getDbId();
        Database db = getDb(dbId);
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            Table table = getTable(db.getId(), info.getTableId());
            recycleBin.replayRecoverPartition((OlapTable) table, info.getPartitionId());
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
    }

    @Override
    public void setPartitionVersion(PartitionVersionRecoveryInfo info) {
        GlobalStateMgr.getCurrentState().getEditLog().logRecoverPartitionVersion(info);
    }

    @Override
    public void addSubPartitionLog(Database db, OlapTable olapTable, Partition partition,
                                   List<PhysicalPartition> subPartitions) {
        List<PhysicalPartitionPersistInfoV2> partitionInfoV2List = Lists.newArrayList();
        for (PhysicalPartition subPartition : subPartitions) {
            PhysicalPartitionPersistInfoV2 info = new PhysicalPartitionPersistInfoV2(
                    db.getId(), olapTable.getId(), partition.getId(), subPartition);
            partitionInfoV2List.add(info);
        }

        AddSubPartitionsInfoV2 infos = new AddSubPartitionsInfoV2(partitionInfoV2List);
        GlobalStateMgr.getCurrentState().getEditLog().logAddSubPartitions(infos);

        for (PhysicalPartition subPartition : subPartitions) {
            LOG.info("succeed in creating sub partitions[{}]", subPartition);
        }
    }

    public void replayAddSubPartition(PhysicalPartitionPersistInfoV2 info) throws DdlException {
        Database db = this.getDb(info.getDbId());
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            OlapTable olapTable = (OlapTable) getTable(db.getId(), info.getTableId());
            Partition partition = olapTable.getPartition(info.getPartitionId());
            PhysicalPartition physicalPartition = info.getPhysicalPartition();
            partition.addSubPartition(physicalPartition);
            olapTable.addPhysicalPartition(physicalPartition);

            if (!isCheckpointThread()) {
                // add to inverted index
                TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
                for (MaterializedIndex index : physicalPartition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                    long indexId = index.getId();
                    int schemaHash = olapTable.getSchemaHashByIndexId(indexId);
                    TabletMeta tabletMeta = new TabletMeta(info.getDbId(), info.getTableId(), info.getPartitionId(),
                            physicalPartition.getId(), index.getId(), schemaHash, olapTable.getPartitionInfo().getDataProperty(
                            info.getPartitionId()).getStorageMedium(), false);
                    for (Tablet tablet : index.getTablets()) {
                        long tabletId = tablet.getId();
                        invertedIndex.addTablet(tabletId, tabletMeta);
                        // modify some logic
                        if (tablet instanceof LocalTablet) {
                            for (Replica replica : ((LocalTablet) tablet).getImmutableReplicas()) {
                                invertedIndex.addReplica(tabletId, replica);
                            }
                        }
                    }
                }
            }
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
    }

    public void replayRenameRollup(TableInfo tableInfo) {
        long dbId = tableInfo.getDbId();
        long tableId = tableInfo.getTableId();
        long indexId = tableInfo.getIndexId();
        String newRollupName = tableInfo.getNewRollupName();

        Database db = getDb(dbId);
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            OlapTable table = (OlapTable) getTable(db.getId(), tableId);
            String rollupName = table.getIndexNameById(indexId);
            Map<String, Long> indexNameToIdMap = table.getIndexNameToId();
            indexNameToIdMap.remove(rollupName);
            indexNameToIdMap.put(newRollupName, indexId);

            LOG.info("replay rename rollup[{}] to {}", rollupName, newRollupName);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
    }

    public void replayRenameColumn(ColumnRenameInfo columnRenameInfo) throws DdlException {
        long dbId = columnRenameInfo.getDbId();
        long tableId = columnRenameInfo.getTableId();
        String colName = columnRenameInfo.getColumnName();
        String newColName = columnRenameInfo.getNewColumnName();
        Database db = getDb(dbId);
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            OlapTable olapTable = (OlapTable) getTable(db.getId(), tableId);
            olapTable.renameColumn(colName, newColName);
            LOG.info("replay rename column[{}] to {}", colName, newColName);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
    }

    public void replayModifyHiveTableColumn(short opCode, ModifyTableColumnOperationLog info) {
        if (info.getDbName() == null) {
            return;
        }
        String hiveExternalDb = info.getDbName();
        String hiveExternalTable = info.getTableName();
        LOG.info("replayModifyTableColumn hiveDb:{},hiveTable:{}", hiveExternalDb, hiveExternalTable);
        List<Column> columns = info.getColumns();
        Database db = getDb(hiveExternalDb);
        HiveTable table;
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            Table tbl = getTable(db.getFullName(), hiveExternalTable);
            table = (HiveTable) tbl;
            table.setNewFullSchema(columns);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
    }

    public void replayModifyTableProperty(short opCode, ModifyTablePropertyOperationLog info) {
        long dbId = info.getDbId();
        long tableId = info.getTableId();
        Map<String, String> properties = info.getProperties();
        String comment = info.getComment();

        Database db = getDb(dbId);
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            OlapTable olapTable = (OlapTable) getTable(db.getId(), tableId);
            if (opCode == OperationType.OP_SET_FORBIDDEN_GLOBAL_DICT) {
                String enAble = properties.get(PropertyAnalyzer.ENABLE_LOW_CARD_DICT_TYPE);
                Preconditions.checkState(enAble != null);
                if (olapTable != null) {
                    if (enAble.equals(PropertyAnalyzer.DISABLE_LOW_CARD_DICT)) {
                        olapTable.setHasForbiddenGlobalDict(true);
                        IDictManager.getInstance().disableGlobalDict(olapTable.getId());
                    } else {
                        olapTable.setHasForbiddenGlobalDict(false);
                        IDictManager.getInstance().enableGlobalDict(olapTable.getId());
                    }
                }
            } else {
                TableProperty tableProperty = olapTable.getTableProperty();
                if (tableProperty == null) {
                    tableProperty = new TableProperty(properties);
                    olapTable.setTableProperty(tableProperty.buildProperty(opCode));
                } else {
                    tableProperty.modifyTableProperties(properties);
                    tableProperty.buildProperty(opCode);
                }

                if (StringUtils.isNotEmpty(comment)) {
                    olapTable.setComment(comment);
                }

                // need to replay partition info meta
                if (opCode == OperationType.OP_MODIFY_IN_MEMORY) {
                    for (Partition partition : olapTable.getPartitions()) {
                        olapTable.getPartitionInfo().setIsInMemory(partition.getId(), tableProperty.isInMemory());
                    }
                } else if (opCode == OperationType.OP_MODIFY_REPLICATION_NUM) {
                    // update partition replication num if this table is unpartitioned table
                    PartitionInfo partitionInfo = olapTable.getPartitionInfo();
                    if (partitionInfo.getType() == PartitionType.UNPARTITIONED) {
                        String partitionName = olapTable.getName();
                        Partition partition = olapTable.getPartition(partitionName);
                        if (partition != null) {
                            partitionInfo.setReplicationNum(partition.getId(), tableProperty.getReplicationNum());
                        }
                    }
                } else if (opCode == OperationType.OP_MODIFY_ENABLE_PERSISTENT_INDEX) {
                    olapTable.setEnablePersistentIndex(tableProperty.enablePersistentIndex());
                    if (olapTable.isCloudNativeTable()) {
                        olapTable.setPersistentIndexType(tableProperty.getPersistentIndexType());
                    }
                } else if (opCode == OperationType.OP_MODIFY_PRIMARY_INDEX_CACHE_EXPIRE_SEC) {
                    olapTable.setPrimaryIndexCacheExpireSec(tableProperty.primaryIndexCacheExpireSec());
                } else if (opCode == OperationType.OP_MODIFY_BINLOG_CONFIG) {
                    if (!olapTable.isBinlogEnabled()) {
                        olapTable.clearBinlogAvailableVersion();
                    }
                }
            }
        } catch (Exception ex) {
            LOG.warn("The replay log failed and this log was ignored.", ex);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
    }

    public void replayDeleteAutoIncrementId(AutoIncrementInfo info) throws IOException {
        for (Map.Entry<Long, Long> entry : info.tableIdToIncrementId().entrySet()) {
            Long tableId = entry.getKey();
            tableIdToIncrementId.remove(tableId);
        }
    }

    public void replayAutoIncrementId(AutoIncrementInfo info) throws IOException {
        for (Map.Entry<Long, Long> entry : info.tableIdToIncrementId().entrySet()) {
            Long tableId = entry.getKey();
            Long id = entry.getValue();

            Long oldId = tableIdToIncrementId.putIfAbsent(tableId, id);

            if (oldId != null && id > tableIdToIncrementId.get(tableId)) {
                tableIdToIncrementId.replace(tableId, id);
            }
        }
    }

    public Long allocateAutoIncrementId(Long tableId, Long rows) {
        Long oldId = tableIdToIncrementId.putIfAbsent(tableId, 1L);
        if (oldId == null) {
            oldId = tableIdToIncrementId.get(tableId);
        }

        Long newId = oldId + rows;
        // AUTO_INCREMENT counter overflow
        if (newId < oldId) {
            throw new RuntimeException("AUTO_INCREMENT counter overflow");
        }

        while (!tableIdToIncrementId.replace(tableId, oldId, newId)) {
            oldId = tableIdToIncrementId.get(tableId);
            newId = oldId + rows;

            // AUTO_INCREMENT counter overflow
            if (newId < oldId) {
                throw new RuntimeException("AUTO_INCREMENT counter overflow");
            }
        }

        return oldId;
    }

    public void removeAutoIncrementIdByTableId(Long tableId, boolean isReplay) {
        Long id = tableIdToIncrementId.remove(tableId);
        if (!isReplay && id != null) {
            ConcurrentHashMap<Long, Long> deltaMap = new ConcurrentHashMap<>();
            deltaMap.put(tableId, 0L);
            AutoIncrementInfo info = new AutoIncrementInfo(deltaMap);
            GlobalStateMgr.getCurrentState().getEditLog().logSaveDeleteAutoIncrementId(info);
        }
    }

    public ConcurrentHashMap<Long, Long> tableIdToIncrementId() {
        return tableIdToIncrementId;
    }

    public Long getCurrentAutoIncrementIdByTableId(Long tableId) {
        return tableIdToIncrementId.get(tableId);
    }

    public void addOrReplaceAutoIncrementIdByTableId(Long tableId, Long id) {
        Long oldId = tableIdToIncrementId.putIfAbsent(tableId, id);
        if (oldId != null) {
            tableIdToIncrementId.replace(tableId, id);
        }
    }

    public void save(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
        // Don't write system db meta
        Map<Long, Database> idToDbNormal =
                idToDb.entrySet().stream().filter(entry -> entry.getKey() > NEXT_ID_INIT_VALUE)
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        int totalTableNum = 0;
        for (Database database : idToDbNormal.values()) {
            totalTableNum += database.getTableNumber();
        }
        int cnt = 1 + idToDbNormal.size() + idToDbNormal.size() /* record database table size */ + totalTableNum + 1;
        SRMetaBlockWriter writer = imageWriter.getBlockWriter(SRMetaBlockID.LOCAL_META_STORE, cnt);

        writer.writeInt(idToDbNormal.size());
        for (Database database : idToDbNormal.values()) {
            writer.writeJson(database);
            int totalTableNumber = database.getTables().size();
            writer.writeInt(totalTableNumber);
            List<Table> tables = database.getTables();
            for (Table table : tables) {
                writer.writeJson(table);
            }
        }

        AutoIncrementInfo info = new AutoIncrementInfo(tableIdToIncrementId);
        writer.writeJson(info);

        writer.close();
    }

    public void load(SRMetaBlockReader reader) throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        int dbSize = reader.readInt();
        for (int i = 0; i < dbSize; ++i) {
            Database db = reader.readJson(Database.class);
            int tableSize = reader.readInt();
            for (int j = 0; j < tableSize; ++j) {
                Table table = reader.readJson(Table.class);
                db.registerTableUnlocked(table);
            }

            idToDb.put(db.getId(), db);
            fullNameToDb.put(db.getFullName(), db);
            stateMgr.getGlobalTransactionMgr().addDatabaseTransactionMgr(db.getId());
            db.getTables().forEach(tbl -> {
                try {
                    tbl.onReload();
                    if (tbl.isTemporaryTable()) {
                        TemporaryTableMgr temporaryTableMgr = GlobalStateMgr.getCurrentState().getTemporaryTableMgr();
                        temporaryTableMgr.addTemporaryTable(UUIDUtil.genUUID(), db.getId(), tbl.getName(), tbl.getId());
                    }
                } catch (Throwable e) {
                    LOG.error("reload table failed: {}", tbl, e);
                }
            });
        }

        AutoIncrementInfo autoIncrementInfo = reader.readJson(AutoIncrementInfo.class);
        for (Map.Entry<Long, Long> entry : autoIncrementInfo.tableIdToIncrementId().entrySet()) {
            Long tableId = entry.getKey();
            Long id = entry.getValue();

            tableIdToIncrementId.put(tableId, id);
        }

        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().recreateTabletInvertIndex();
        GlobalStateMgr.getCurrentState().getEsRepository().loadTableFromCatalog();
    }

    @Override
    public void handleMVRepair(Database db, Table table, List<MVRepairHandler.PartitionRepairInfo> partitionRepairInfos) {
        MVMetaVersionRepairer.repairBaseTableVersionChanges(db, table, partitionRepairInfos);
    }

    // for test only
    @VisibleForTesting
    public void clear() {
        if (idToDb != null) {
            idToDb.clear();
        }
        if (fullNameToDb != null) {
            fullNameToDb.clear();
        }
        System.gc();
    }

    @Override
    public List<Pair<List<Object>, Long>> getSamples() {
        long totalCount = idToDb.values()
                .stream()
                .mapToInt(database -> {
                    Locker locker = new Locker();
                    locker.lockDatabase(database.getId(), LockType.READ);
                    try {
                        return database.getOlapPartitionsCount();
                    } finally {
                        locker.unLockDatabase(database.getId(), LockType.READ);
                    }
                }).sum();
        List<Object> samples = new ArrayList<>();
        // get every olap table's first partition
        for (Database database : idToDb.values()) {
            Locker locker = new Locker();
            locker.lockDatabase(database.getId(), LockType.READ);
            try {
                samples.addAll(database.getPartitionSamples());
            } finally {
                locker.unLockDatabase(database.getId(), LockType.READ);
            }
        }
        return Lists.newArrayList(Pair.create(samples, totalCount));
    }

    @Override
    public Map<String, Long> estimateCount() {
        long totalCount = idToDb.values()
                .stream()
                .mapToInt(database -> {
                    Locker locker = new Locker();
                    locker.lockDatabase(database.getId(), LockType.READ);
                    try {
                        return database.getOlapPartitionsCount();
                    } finally {
                        locker.unLockDatabase(database.getId(), LockType.READ);
                    }
                }).sum();
        return ImmutableMap.of("Partition", totalCount);
    }
}
