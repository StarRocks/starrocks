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
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.sleepycat.je.Transaction;
import com.starrocks.alter.AlterJobException;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedView;
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
import com.starrocks.catalog.View;
import com.starrocks.catalog.system.information.InfoSchemaDb;
import com.starrocks.catalog.system.sys.SysDb;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.MaterializedViewExceptions;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.common.util.DynamicPartitionUtil;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.common.util.WriteQuorum;
import com.starrocks.common.util.concurrent.MarkedCountDownLatch;
import com.starrocks.common.util.concurrent.lock.AutoCloseableLock;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.memory.MemoryTrackable;
import com.starrocks.meta.MetaStore;
import com.starrocks.meta.MetadataHandler;
import com.starrocks.meta.StarRocksMetadata;
import com.starrocks.meta.kv.ByteCoder;
import com.starrocks.mv.MVMetaVersionRepairer;
import com.starrocks.mv.MVRepairHandler;
import com.starrocks.persist.AddPartitionsInfoV2;
import com.starrocks.persist.AddSubPartitionsInfoV2;
import com.starrocks.persist.AlterMaterializedViewBaseTableInfosLog;
import com.starrocks.persist.AlterMaterializedViewStatusLog;
import com.starrocks.persist.AlterViewInfo;
import com.starrocks.persist.AutoIncrementInfo;
import com.starrocks.persist.BackendTabletsInfo;
import com.starrocks.persist.BatchDeleteReplicaInfo;
import com.starrocks.persist.BatchDropInfo;
import com.starrocks.persist.ChangeMaterializedViewRefreshSchemeLog;
import com.starrocks.persist.ColumnRenameInfo;
import com.starrocks.persist.ConsistencyCheckInfo;
import com.starrocks.persist.CreateDbInfo;
import com.starrocks.persist.CreateTableInfo;
import com.starrocks.persist.DatabaseInfo;
import com.starrocks.persist.DropDbInfo;
import com.starrocks.persist.DropInfo;
import com.starrocks.persist.DropPartitionInfo;
import com.starrocks.persist.DropPartitionsInfo;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.ListPartitionPersistInfo;
import com.starrocks.persist.ModifyPartitionInfo;
import com.starrocks.persist.ModifyTableColumnOperationLog;
import com.starrocks.persist.ModifyTablePropertyOperationLog;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.PartitionPersistInfoV2;
import com.starrocks.persist.PartitionVersionRecoveryInfo;
import com.starrocks.persist.PhysicalPartitionPersistInfoV2;
import com.starrocks.persist.RangePartitionPersistInfo;
import com.starrocks.persist.RecoverInfo;
import com.starrocks.persist.RenameMaterializedViewLog;
import com.starrocks.persist.ReplacePartitionOperationLog;
import com.starrocks.persist.ReplicaPersistInfo;
import com.starrocks.persist.SetReplicaStatusOperationLog;
import com.starrocks.persist.SwapTableOperationLog;
import com.starrocks.persist.TableInfo;
import com.starrocks.persist.TruncateTableInfo;
import com.starrocks.persist.catalog.DatabaseLog;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.privilege.PrivilegeBuiltinConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.MaterializedViewAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AlterDatabaseQuotaStmt;
import com.starrocks.sql.ast.AlterMaterializedViewStatusClause;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SingleRangePartitionDesc;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.optimizer.statistics.IDictManager;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.TabletMetadataUpdateAgentTask;
import com.starrocks.task.TabletMetadataUpdateAgentTaskFactory;
import com.starrocks.thrift.TTabletMetaType;
import com.starrocks.thrift.TTaskType;
import com.starrocks.thrift.TWriteQuorumType;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;

import static com.starrocks.meta.StarRocksMetadata.inactiveRelatedMaterializedView;
import static com.starrocks.server.GlobalStateMgr.NEXT_ID_INIT_VALUE;
import static com.starrocks.server.GlobalStateMgr.isCheckpointThread;

public class LocalMetastore implements MVRepairHandler, MemoryTrackable, MetaStore {
    private static final Logger LOG = LogManager.getLogger(LocalMetastore.class);

    private final ConcurrentHashMap<Long, Database> idToDb = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Database> fullNameToDb = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, Long> tableIdToIncrementId = new ConcurrentHashMap<>();

    private final GlobalStateMgr stateMgr;

    public LocalMetastore(GlobalStateMgr globalStateMgr) {
        this.stateMgr = globalStateMgr;

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
    public void createDb(CreateDbInfo createDbInfo) {
        Database db = new Database(createDbInfo.getId(), createDbInfo.getDbName());
        unprotectCreateDb(db);
        GlobalStateMgr.getCurrentState().getEditLog().logCreateDb(db, createDbInfo.getStorageVolumeId());
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

        globalStateMgr.tryLock(true);
        try {
            Database db = new Database(createDbInfo.getId(), createDbInfo.getDbName());
            unprotectCreateDb(db);
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
    public void dropDb(DropDbInfo dropDbInfo) {
        // 3. remove db from globalStateMgr
        String dbName = dropDbInfo.getDbName();
        boolean isForceDrop = dropDbInfo.isForceDrop();

        Database db = fullNameToDb.get(dbName);
        idToDb.remove(db.getId());
        fullNameToDb.remove(db.getFullName());
        unprotectDropDb(db, isForceDrop, false);

        GlobalStateMgr.getCurrentState().getEditLog().logDropDb(dropDbInfo);
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
                GlobalStateMgr.getCurrentState().getRecycleBin().recycleDatabase(db, tableNames, isForceDrop);
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
        Database db = GlobalStateMgr.getCurrentState().getRecycleBin().replayRecoverDatabase(dbId);
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
        Database db = getDb(dbInfo.getDbName());

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
        Database db = getDb(dbName);
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
        Database db = this.fullNameToDb.get(createTableInfo.getDbName());
        Table table = createTableInfo.getTable();
        if (table.isOlapOrCloudNativeTable() || table.isMaterializedView()) {
            OlapTable olapTable = (OlapTable) table;
            GlobalStateMgr.getCurrentState().getTabletInvertedIndex()
                    .buildTabletInvertedIndex(db.getId(), olapTable, olapTable.getAllPhysicalPartitions());
        }
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
                OlapTable olapTable = (OlapTable) table;
                GlobalStateMgr.getCurrentState().getTabletInvertedIndex()
                        .buildTabletInvertedIndex(db.getId(), olapTable, olapTable.getAllPhysicalPartitions());
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
                List<PhysicalPartition> physicalPartitionList = new ArrayList<>();
                for (Partition partition : info.getPartitions()) {
                    physicalPartitionList.addAll(partition.getSubPartitions());
                }
                GlobalStateMgr.getCurrentState().getTabletInvertedIndex()
                        .buildTabletInvertedIndex(db.getId(), olapTable, physicalPartitionList);
            }
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
    }

    @Override
    public void swapTable(SwapTableOperationLog log) {
        swapTableInternal(log);
        GlobalStateMgr.getCurrentState().getEditLog().logSwapTable(log);
    }

    public void replaySwapTable(SwapTableOperationLog log) {
        swapTableInternal(log);
        long dbId = log.getDbId();
        long origTblId = log.getOrigTblId();
        long newTblId = log.getNewTblId();
        Database db = getDb(dbId);
        OlapTable origTable = (OlapTable) getTable(db.getId(), origTblId);
        OlapTable newTbl = (OlapTable) getTable(db.getId(), newTblId);
        LOG.debug("finish replay swap table {}-{} with table {}-{}", origTblId, origTable.getName(), newTblId,
                newTbl.getName());
    }

    /**
     * The swap table operation works as follow:
     * For example, SWAP TABLE A WITH TABLE B.
     * must pre check A can be renamed to B and B can be renamed to A
     */
    public void swapTableInternal(SwapTableOperationLog log) {
        long dbId = log.getDbId();
        long origTblId = log.getOrigTblId();
        long newTblId = log.getNewTblId();
        Database db = getDb(dbId);
        OlapTable origTable = (OlapTable) getTable(db.getId(), origTblId);
        OlapTable newTbl = (OlapTable) getTable(db.getId(), newTblId);

        String origTblName = origTable.getName();
        String newTblName = newTbl.getName();

        // drop origin table and new table
        db.dropTable(origTblName);
        db.dropTable(newTblName);

        // rename new table name to origin table name and add it to database
        newTbl.checkAndSetName(origTblName, false);
        db.registerTableUnlocked(newTbl);

        // rename origin table name to new table name and add it to database
        origTable.checkAndSetName(newTblName, false);
        db.registerTableUnlocked(origTable);

        // swap dependencies of base table
        if (origTable.isMaterializedView()) {
            MaterializedView oldMv = (MaterializedView) origTable;
            MaterializedView newMv = (MaterializedView) newTbl;
            updateTaskDefinition(oldMv);
            updateTaskDefinition(newMv);
        }
    }

    private void updateTaskDefinition(MaterializedView materializedView) {
        Task currentTask = GlobalStateMgr.getCurrentState().getTaskManager().getTask(
                TaskBuilder.getMvTaskName(materializedView.getId()));
        if (currentTask != null) {
            currentTask.setDefinition(materializedView.getTaskDefinition());
            currentTask.setPostRun(TaskBuilder.getAnalyzeMVStmt(materializedView.getName()));
        }
    }

    @Override
    public void updateTableMeta(Database db, String tableName, Map<String, String> properties, TTabletMetaType metaType)
            throws DdlException {
        List<Partition> partitions = Lists.newArrayList();
        OlapTable olapTable = (OlapTable) getTable(db.getFullName(), tableName);

        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(olapTable.getId()), LockType.READ);
        try {
            partitions.addAll(olapTable.getPartitions());
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(olapTable.getId()), LockType.READ);
        }

        boolean metaValue = false;
        switch (metaType) {
            case INMEMORY:
                metaValue = Boolean.parseBoolean(properties.get(PropertyAnalyzer.PROPERTIES_INMEMORY));
                if (metaValue == olapTable.isInMemory()) {
                    return;
                }
                break;
            case ENABLE_PERSISTENT_INDEX:
                metaValue = Boolean.parseBoolean(properties.get(PropertyAnalyzer.PROPERTIES_ENABLE_PERSISTENT_INDEX));
                if (metaValue == olapTable.enablePersistentIndex()) {
                    return;
                }
                break;
            case WRITE_QUORUM:
                TWriteQuorumType writeQuorum = WriteQuorum
                        .findTWriteQuorumByName(properties.get(PropertyAnalyzer.PROPERTIES_WRITE_QUORUM));
                if (writeQuorum == olapTable.writeQuorum()) {
                    return;
                }
                break;
            case REPLICATED_STORAGE:
                metaValue = Boolean.parseBoolean(properties.get(PropertyAnalyzer.PROPERTIES_REPLICATED_STORAGE));
                if (metaValue == olapTable.enableReplicatedStorage()) {
                    return;
                }
                break;
            case BUCKET_SIZE:
                long bucketSize = Long.parseLong(properties.get(PropertyAnalyzer.PROPERTIES_BUCKET_SIZE));
                if (bucketSize == olapTable.getAutomaticBucketSize()) {
                    return;
                }
                break;
            case MUTABLE_BUCKET_NUM:
                long mutableBucketNum = Long.parseLong(properties.get(PropertyAnalyzer.PROPERTIES_MUTABLE_BUCKET_NUM));
                if (mutableBucketNum == olapTable.getMutableBucketNum()) {
                    return;
                }
                break;
            case ENABLE_LOAD_PROFILE:
                boolean enableLoadProfile = Boolean.parseBoolean(properties.get(PropertyAnalyzer.PROPERTIES_ENABLE_LOAD_PROFILE));
                if (enableLoadProfile == olapTable.enableLoadProfile()) {
                    return;
                }
                break;
            case PRIMARY_INDEX_CACHE_EXPIRE_SEC:
                int primaryIndexCacheExpireSec = Integer.parseInt(properties.get(
                        PropertyAnalyzer.PROPERTIES_PRIMARY_INDEX_CACHE_EXPIRE_SEC));
                if (primaryIndexCacheExpireSec == olapTable.primaryIndexCacheExpireSec()) {
                    return;
                }
                break;
            default:
                LOG.warn("meta type: {} does not support", metaType);
                return;
        }

        if (metaType == TTabletMetaType.INMEMORY || metaType == TTabletMetaType.ENABLE_PERSISTENT_INDEX) {
            for (Partition partition : partitions) {
                updatePartitionTabletMeta(db, olapTable.getName(), partition.getName(), metaValue, metaType);
            }
        }

        try (AutoCloseableLock ignore =
                     new AutoCloseableLock(new Locker(), db.getId(), Lists.newArrayList(olapTable.getId()), LockType.WRITE)) {
            TableProperty tableProperty = olapTable.getTableProperty();
            if (tableProperty == null) {
                tableProperty = new TableProperty(properties);
                olapTable.setTableProperty(tableProperty);
            } else {
                tableProperty.modifyTableProperties(properties);
            }

            ModifyTablePropertyOperationLog info =
                    new ModifyTablePropertyOperationLog(db.getId(), olapTable.getId(), properties);
            switch (metaType) {
                case INMEMORY:
                    tableProperty.buildInMemory();
                    // need to update partition info meta
                    for (Partition partition : olapTable.getPartitions()) {
                        olapTable.getPartitionInfo().setIsInMemory(partition.getId(), tableProperty.isInMemory());
                    }
                    GlobalStateMgr.getCurrentState().getEditLog().logModifyInMemory(info);
                case ENABLE_PERSISTENT_INDEX:
                    tableProperty.buildEnablePersistentIndex();
                    if (olapTable.isCloudNativeTable()) {
                        // now default to LOCAL
                        tableProperty.buildPersistentIndexType();
                    }
                    GlobalStateMgr.getCurrentState().getEditLog().logModifyEnablePersistentIndex(info);
                case WRITE_QUORUM:
                    tableProperty.buildWriteQuorum();
                    GlobalStateMgr.getCurrentState().getEditLog().logModifyWriteQuorum(info);
                case REPLICATED_STORAGE:
                    tableProperty.buildReplicatedStorage();
                    GlobalStateMgr.getCurrentState().getEditLog().logModifyReplicatedStorage(info);
                case BUCKET_SIZE:
                    tableProperty.buildBucketSize();
                    GlobalStateMgr.getCurrentState().getEditLog().logModifyBucketSize(info);
                case MUTABLE_BUCKET_NUM:
                    tableProperty.buildMutableBucketNum();
                    GlobalStateMgr.getCurrentState().getEditLog().logModifyMutableBucketNum(info);
                case ENABLE_LOAD_PROFILE:
                    tableProperty.buildEnableLoadProfile();
                    GlobalStateMgr.getCurrentState().getEditLog().logModifyEnableLoadProfile(info);
                case PRIMARY_INDEX_CACHE_EXPIRE_SEC:
                    tableProperty.buildPrimaryIndexCacheExpireSec();
                    GlobalStateMgr.getCurrentState().getEditLog().logModifyPrimaryIndexCacheExpireSec(info);
                default:
                    LOG.warn("meta type: {} does not support", metaType);
                    return;
            }
        }
    }

    /**
     * Update one specified partition's in-memory property by partition name of table
     * This operation may return partial successfully, with a exception to inform user to retry
     */
    public void updatePartitionTabletMeta(Database db,
                                          String tableName,
                                          String partitionName,
                                          boolean metaValue,
                                          TTabletMetaType metaType) throws DdlException {
        // be id -> <tablet id>
        Map<Long, Set<Long>> beIdToTabletSet = Maps.newHashMap();
        OlapTable olapTable = (OlapTable) getTable(db.getFullName(), tableName);

        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(olapTable.getId()), LockType.READ);
        try {
            Partition partition = olapTable.getPartition(partitionName);
            if (partition == null) {
                throw new DdlException(
                        "Partition[" + partitionName + "] does not exist in table[" + olapTable.getName() + "]");
            }

            for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                for (MaterializedIndex index
                        : physicalPartition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE)) {
                    for (Tablet tablet : index.getTablets()) {
                        for (Replica replica : ((LocalTablet) tablet).getImmutableReplicas()) {
                            Set<Long> tabletSet = beIdToTabletSet.computeIfAbsent(replica.getBackendId(), k -> Sets.newHashSet());
                            tabletSet.add(tablet.getId());
                        }
                    }
                }
            }
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(olapTable.getId()), LockType.READ);
        }

        int totalTaskNum = beIdToTabletSet.keySet().size();
        MarkedCountDownLatch<Long, Set<Long>> countDownLatch = new MarkedCountDownLatch<>(totalTaskNum);
        AgentBatchTask batchTask = new AgentBatchTask();
        for (Map.Entry<Long, Set<Long>> kv : beIdToTabletSet.entrySet()) {
            countDownLatch.addMark(kv.getKey(), kv.getValue());
            long backendId = kv.getKey();
            Set<Long> tablets = kv.getValue();
            TabletMetadataUpdateAgentTask task = TabletMetadataUpdateAgentTaskFactory
                    .createGenericBooleanPropertyUpdateTask(backendId, tablets, metaValue, metaType);
            Preconditions.checkState(task != null, "task is null");
            task.setLatch(countDownLatch);
            batchTask.addTask(task);
        }
        if (!FeConstants.runningUnitTest) {
            // send all tasks and wait them finished
            AgentTaskQueue.addBatchTask(batchTask);
            AgentTaskExecutor.submit(batchTask);
            LOG.info("send update tablet meta task for table {}, partitions {}, number: {}",
                    tableName, partitionName, batchTask.getTaskNum());

            // estimate timeout
            long timeout = Config.tablet_create_timeout_second * 1000L * totalTaskNum;
            timeout = Math.min(timeout, Config.max_create_table_timeout_second * 1000L);
            boolean ok = false;
            try {
                ok = countDownLatch.await(timeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                LOG.warn("InterruptedException: ", e);
            }

            if (!ok || !countDownLatch.getStatus().ok()) {
                String errMsg = "Failed to update partition[" + partitionName + "]. tablet meta.";
                // clear tasks
                AgentTaskQueue.removeBatchTask(batchTask, TTaskType.UPDATE_TABLET_META_INFO);

                if (!countDownLatch.getStatus().ok()) {
                    errMsg += " Error: " + countDownLatch.getStatus().getErrorMsg();
                } else {
                    List<Map.Entry<Long, Set<Long>>> unfinishedMarks = countDownLatch.getLeftMarks();
                    // only show at most 3 results
                    List<Map.Entry<Long, Set<Long>>> subList =
                            unfinishedMarks.subList(0, Math.min(unfinishedMarks.size(), 3));
                    if (!subList.isEmpty()) {
                        errMsg += " Unfinished mark: " + Joiner.on(", ").join(subList);
                    }
                }
                errMsg += ". This operation maybe partial successfully, You should retry until success.";
                LOG.warn(errMsg);
                throw new DdlException(errMsg);
            }
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

    @Override
    public void alterTable(ModifyTablePropertyOperationLog log) {
        GlobalStateMgr.getCurrentState().getEditLog().logAlterTableProperties(log);
    }

    @Override
    public void renameColumn(ColumnRenameInfo columnRenameInfo) {
        GlobalStateMgr.getCurrentState().getEditLog().logColumnRename(columnRenameInfo);
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
            return new ArrayList<>(database.getTables());
        }
    }

    @Override
    public Table getTable(String dbName, String tblName) {
        DatabaseLog database = (DatabaseLog) getDb(dbName);
        if (database == null) {
            return null;
        }
        return database.getTable(tblName);
    }

    @Override
    public Table getTable(Long dbId, Long tableId) {
        DatabaseLog database = (DatabaseLog) getDb(dbId);
        if (database == null) {
            return null;
        }
        return database.getTable(tableId);
    }

    @Override
    public void modifyViewDef(AlterViewInfo alterViewInfo) {
        alterView(alterViewInfo);
        GlobalStateMgr.getCurrentState().getEditLog().logModifyViewDef(alterViewInfo);
    }

    public void alterView(AlterViewInfo alterViewInfo) {
        long dbId = alterViewInfo.getDbId();
        long tableId = alterViewInfo.getTableId();
        String inlineViewDef = alterViewInfo.getInlineViewDef();
        List<Column> newFullSchema = alterViewInfo.getNewFullSchema();
        String comment = alterViewInfo.getComment();

        Database db = getDb(dbId);
        View view = (View) getTable(db.getId(), tableId);

        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(view.getId()), LockType.WRITE);
        try {
            String viewName = view.getName();
            view.setInlineViewDefWithSqlMode(inlineViewDef, alterViewInfo.getSqlMode());
            try {
                view.init();
            } catch (UserException e) {
                throw new AlterJobException("failed to init view stmt", e);
            }
            view.setNewFullSchema(newFullSchema);
            view.setComment(comment);
            inactiveRelatedMaterializedView(db, view,
                    MaterializedViewExceptions.inactiveReasonForBaseViewChanged(viewName));
            db.dropTable(viewName);
            db.registerTableUnlocked(view);

            LOG.info("replay modify view[{}] definition to {}", viewName, inlineViewDef);
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(view.getId()), LockType.WRITE);
        }
    }

    @Override
    public void renameMaterializedView(RenameMaterializedViewLog renameMaterializedViewLog) {
        GlobalStateMgr.getCurrentState().getEditLog().logMvRename(renameMaterializedViewLog);
    }

    public void replayRenameMaterializedView(RenameMaterializedViewLog log) {
        long dbId = log.getDbId();
        long materializedViewId = log.getId();
        String newMaterializedViewName = log.getNewMaterializedViewName();
        Database db = getDb(dbId);
        MaterializedView oldMaterializedView = (MaterializedView) getTable(db.getId(), materializedViewId);
        if (oldMaterializedView != null) {
            try (AutoCloseableLock ignore = new AutoCloseableLock(new Locker(), db.getId(),
                    Lists.newArrayList(oldMaterializedView.getId()), LockType.WRITE)) {
                db.dropTable(oldMaterializedView.getName());
                oldMaterializedView.setName(newMaterializedViewName);
                db.registerTableUnlocked(oldMaterializedView);
                updateTaskDefinition(oldMaterializedView);
                LOG.info("Replay rename materialized view [{}] to {}, id: {}", oldMaterializedView.getName(),
                        newMaterializedViewName, oldMaterializedView.getId());
            } catch (Throwable e) {
                oldMaterializedView.setInactiveAndReason("replay rename failed: " + e.getMessage());
                LOG.warn("replay rename materialized-view failed: {}", oldMaterializedView.getName(), e);
            }
        }
    }

    @Override
    public void alterMvBaseTableInfos(AlterMaterializedViewBaseTableInfosLog alterMaterializedViewBaseTableInfos) {
        GlobalStateMgr.getCurrentState().getEditLog().logAlterMvBaseTableInfos(alterMaterializedViewBaseTableInfos);
    }

    public void replayAlterMaterializedViewBaseTableInfos(AlterMaterializedViewBaseTableInfosLog log) {
        long dbId = log.getDbId();
        long mvId = log.getMvId();
        Database db = getDb(dbId);
        MaterializedView mv = (MaterializedView) getTable(db.getId(), mvId);
        if (mv == null) {
            return;
        }

        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(mv.getId()), LockType.WRITE);
        try {
            mv.replayAlterMaterializedViewBaseTableInfos(log);
        } catch (Throwable e) {
            LOG.warn("replay alter materialized-view status failed: {}", mv.getName(), e);
            mv.setInactiveAndReason("replay alter status failed: " + e.getMessage());
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(mv.getId()), LockType.WRITE);
        }
    }

    @Override
    public void alterMvStatus(AlterMaterializedViewStatusLog log) {
        GlobalStateMgr.getCurrentState().getEditLog().logAlterMvStatus(log);
    }

    public void replayAlterMaterializedViewStatus(AlterMaterializedViewStatusLog log) {
        long dbId = log.getDbId();
        long tableId = log.getTableId();
        Database db = getDb(dbId);
        MaterializedView mv = (MaterializedView) getTable(db.getId(), tableId);
        if (mv == null) {
            return;
        }

        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(mv.getId()), LockType.WRITE);
        try {
            alterMaterializedViewStatus(mv, log.getStatus(), true);
        } catch (Throwable e) {
            LOG.warn("replay alter materialized-view status failed: {}", mv.getName(), e);
            mv.setInactiveAndReason("replay alter status failed: " + e.getMessage());
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(mv.getId()), LockType.WRITE);
        }
    }

    public void alterMaterializedViewStatus(MaterializedView materializedView, String status, boolean isReplay) {
        LOG.info("process change materialized view {} status to {}, isReplay: {}",
                materializedView.getName(), status, isReplay);
        if (AlterMaterializedViewStatusClause.ACTIVE.equalsIgnoreCase(status)) {
            ConnectContext context = new ConnectContext();
            context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
            context.setQualifiedUser(AuthenticationMgr.ROOT_USER);
            context.setCurrentUserIdentity(UserIdentity.ROOT);
            context.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));

            String createMvSql = materializedView.getMaterializedViewDdlStmt(false, isReplay);
            QueryStatement mvQueryStatement = null;
            try {
                mvQueryStatement = recreateMVQuery(materializedView, context, createMvSql);
            } catch (SemanticException e) {
                throw new SemanticException("Can not active materialized view [%s]" +
                        " because analyze materialized view define sql: \n\n%s" +
                        "\n\nCause an error: %s", materializedView.getName(), createMvSql, e.getDetailMsg());
            }

            // Skip checks to maintain eventual consistency when replay
            List<BaseTableInfo> baseTableInfos =
                    Lists.newArrayList(MaterializedViewAnalyzer.getBaseTableInfos(mvQueryStatement, !isReplay));
            materializedView.setBaseTableInfos(baseTableInfos);
            materializedView.getRefreshScheme().getAsyncRefreshContext().clearVisibleVersionMap();
            materializedView.onReload();
            materializedView.setActive();
        } else if (AlterMaterializedViewStatusClause.INACTIVE.equalsIgnoreCase(status)) {
            materializedView.setInactiveAndReason("user use alter materialized view set status to inactive");
        }
    }

    /*
     * Recreate the MV query and validate the correctness of syntax and schema
     */
    public static QueryStatement recreateMVQuery(MaterializedView materializedView,
                                                 ConnectContext context,
                                                 String createMvSql) {
        // If we could parse the MV sql successfully, and the schema of mv does not change,
        // we could reuse the existing MV
        Optional<Database> mayDb = GlobalStateMgr.getCurrentState().getStarRocksMetadata().mayGetDb(materializedView.getDbId());

        // check database existing
        String dbName = mayDb.orElseThrow(() ->
                new SemanticException("database " + materializedView.getDbId() + " not exists")).getFullName();
        context.setDatabase(dbName);

        // Try to parse and analyze the creation sql
        List<StatementBase> statementBaseList = SqlParser.parse(createMvSql, context.getSessionVariable());
        CreateMaterializedViewStatement createStmt = (CreateMaterializedViewStatement) statementBaseList.get(0);
        Analyzer.analyze(createStmt, context);

        // validate the schema
        List<Column> newColumns = createStmt.getMvColumnItems().stream()
                .sorted(Comparator.comparing(Column::getName))
                .collect(Collectors.toList());
        List<Column> existedColumns = materializedView.getColumns().stream()
                .sorted(Comparator.comparing(Column::getName))
                .collect(Collectors.toList());
        if (newColumns.size() != existedColumns.size()) {
            throw new SemanticException(String.format("number of columns changed: %d != %d",
                    existedColumns.size(), newColumns.size()));
        }
        for (int i = 0; i < existedColumns.size(); i++) {
            Column existed = existedColumns.get(i);
            Column created = newColumns.get(i);
            if (!existed.isSchemaCompatible(created)) {
                String message = MaterializedViewExceptions.inactiveReasonForColumnNotCompatible(
                        existed.toString(), created.toString());
                materializedView.setInactiveAndReason(message);
                throw new SemanticException(message);
            }
        }

        return createStmt.getQueryStatement();
    }

    @Override
    public void alterMaterializedViewProperties(ModifyTablePropertyOperationLog log) {
        GlobalStateMgr.getCurrentState().getEditLog().logAlterMaterializedViewProperties(log);
    }

    public void replayAlterMaterializedViewProperties(short opCode, ModifyTablePropertyOperationLog log) {
        long dbId = log.getDbId();
        long tableId = log.getTableId();
        Map<String, String> properties = log.getProperties();

        Database db = getDb(dbId);
        MaterializedView mv = (MaterializedView) getTable(db.getId(), tableId);
        if (mv == null) {
            LOG.warn("Ignore change materialized view properties og because table:" + tableId + "is null");
            return;
        }

        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(mv.getId()), LockType.WRITE);
        try {
            TableProperty tableProperty = mv.getTableProperty();
            if (tableProperty == null) {
                tableProperty = new TableProperty(properties);
                mv.setTableProperty(tableProperty.buildProperty(opCode));
            } else {
                tableProperty.modifyTableProperties(properties);
                tableProperty.buildProperty(opCode);
            }
        } catch (Throwable e) {
            mv.setInactiveAndReason("replay failed: " + e.getMessage());
            LOG.warn("replay alter materialized-view properties failed: {}", mv.getName(), e);
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(mv.getId()), LockType.WRITE);
        }
    }

    @Override
    public void changeMaterializedRefreshScheme(ChangeMaterializedViewRefreshSchemeLog log) {
        GlobalStateMgr.getCurrentState().getEditLog().logMvChangeRefreshScheme(log);
    }

    public void replayChangeMaterializedViewRefreshScheme(ChangeMaterializedViewRefreshSchemeLog log) {
        long dbId = log.getDbId();
        long id = log.getId();
        Database db = getDb(dbId);
        if (db == null) {
            return;
        }

        MaterializedView oldMaterializedView = (MaterializedView) getTable(db.getId(), id);
        if (oldMaterializedView == null) {
            LOG.warn("Ignore change materialized view refresh scheme log because table:" + id + "is null");
            return;
        }

        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(oldMaterializedView.getId()), LockType.WRITE);
        try {
            final MaterializedView.MvRefreshScheme newMvRefreshScheme = new MaterializedView.MvRefreshScheme();
            final MaterializedView.MvRefreshScheme oldRefreshScheme = oldMaterializedView.getRefreshScheme();
            newMvRefreshScheme.setAsyncRefreshContext(oldRefreshScheme.getAsyncRefreshContext());
            newMvRefreshScheme.setLastRefreshTime(oldRefreshScheme.getLastRefreshTime());
            final MaterializedView.RefreshType refreshType = log.getRefreshType();
            final MaterializedView.AsyncRefreshContext asyncRefreshContext = log.getAsyncRefreshContext();
            newMvRefreshScheme.setType(refreshType);
            newMvRefreshScheme.setAsyncRefreshContext(asyncRefreshContext);

            long maxChangedTableRefreshTime =
                    MvUtils.getMaxTablePartitionInfoRefreshTime(
                            log.getAsyncRefreshContext().getBaseTableVisibleVersionMap().values());
            newMvRefreshScheme.setLastRefreshTime(maxChangedTableRefreshTime);

            oldMaterializedView.setRefreshScheme(newMvRefreshScheme);
            LOG.info(
                    "Replay materialized view [{}]'s refresh type to {}, start time to {}, " +
                            "interval step to {}, timeunit to {}, id: {}, maxChangedTableRefreshTime:{}",
                    oldMaterializedView.getName(), refreshType.name(), asyncRefreshContext.getStartTime(),
                    asyncRefreshContext.getStep(),
                    asyncRefreshContext.getTimeUnit(), oldMaterializedView.getId(), maxChangedTableRefreshTime);
        } catch (Throwable e) {
            oldMaterializedView.setInactiveAndReason("replay failed: " + e.getMessage());
            LOG.warn("replay change materialized-view refresh scheme failed: {}",
                    oldMaterializedView.getName(), e);
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(oldMaterializedView.getId()), LockType.WRITE);
        }
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

    @Override
    public void addPartition(AddPartitionsInfoV2 addPartitionsInfo) {
        GlobalStateMgr.getCurrentState().getEditLog().logAddPartitions(addPartitionsInfo);
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
                GlobalStateMgr.getCurrentState().getTabletInvertedIndex()
                        .buildTabletInvertedIndex(info.getDbId(), olapTable, partition.getSubPartitions());
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
            GlobalStateMgr.getCurrentState().getRecycleBin().replayRecoverPartition((OlapTable) table, info.getPartitionId());
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
    }

    @Override
    public void modifyPartition(ModifyPartitionInfo info) {
        GlobalStateMgr.getCurrentState().getEditLog().logModifyPartition(info);
    }

    public void replayModifyPartition(ModifyPartitionInfo info) {
        Database db = getDb(info.getDbId());
        OlapTable olapTable = (OlapTable) getTable(db.getId(), info.getTableId());

        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(olapTable.getId()), LockType.WRITE);
        try {
            PartitionInfo partitionInfo = olapTable.getPartitionInfo();
            if (info.getDataProperty() != null) {
                partitionInfo.setDataProperty(info.getPartitionId(), info.getDataProperty());
            }
            if (info.getReplicationNum() != (short) -1) {
                short replicationNum = info.getReplicationNum();
                partitionInfo.setReplicationNum(info.getPartitionId(), replicationNum);
                // update default replication num if this table is unpartitioned table
                if (partitionInfo.getType() == PartitionType.UNPARTITIONED) {
                    olapTable.setReplicationNum(replicationNum);
                }
            }
            partitionInfo.setIsInMemory(info.getPartitionId(), info.isInMemory());
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(olapTable.getId()), LockType.WRITE);
        }
    }

    @Override
    public void setPartitionVersion(PartitionVersionRecoveryInfo info) {
        GlobalStateMgr.getCurrentState().getEditLog().logRecoverPartitionVersion(info);
    }

    @Override
    public void addSubPartitionLog(AddSubPartitionsInfoV2 addSubPartitionsInfo) {
        GlobalStateMgr.getCurrentState().getEditLog().logAddSubPartitions(addSubPartitionsInfo);
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
                GlobalStateMgr.getCurrentState().getTabletInvertedIndex()
                        .buildTabletInvertedIndex(db.getId(), olapTable, Lists.newArrayList(physicalPartition));
            }
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
    }

    @Override
    public List<PhysicalPartition> getAllPhysicalPartition(OlapTable olapTable) {
        return new ArrayList<>(olapTable.getAllPhysicalPartitions());
    }

    @Override
    public List<PhysicalPartition> getAllPhysicalPartition(Partition partition) {
        return new ArrayList<>(partition.getSubPartitions());
    }

    @Override
    public PhysicalPartition getPhysicalPartition(Partition partition, Long physicalPartitionId) {
        Partition partitionInMemory = (Partition) partition;
        return partitionInMemory.getSubPartition(physicalPartitionId);
    }

    @Override
    public void addPhysicalPartition(Partition partition, PhysicalPartition physicalPartition) {
        partition.addSubPartition(physicalPartition);
    }

    @Override
    public void dropPhysicalPartition(Partition partition, Long physicalPartitionId) {
        partition.removeSubPartition(physicalPartitionId);
    }

    @Override
    public List<MaterializedIndex> getMaterializedIndices(PhysicalPartition physicalPartition,
                                                          MaterializedIndex.IndexExtState indexExtState) {
        return physicalPartition.getMaterializedIndices(indexExtState);
    }

    @Override
    public MaterializedIndex getMaterializedIndex(PhysicalPartition physicalPartition, Long mIndexId) {
        return physicalPartition.getIndex(mIndexId);
    }

    @Override
    public void addMaterializedIndex(PhysicalPartition physicalPartition, MaterializedIndex materializedIndex) {
        physicalPartition.createRollupIndex(materializedIndex);
    }

    @Override
    public void dropMaterializedIndex(PhysicalPartition physicalPartition, Long mIndexId) {
        physicalPartition.deleteRollupIndex(mIndexId);
    }

    @Override
    public void dropRollup(DropInfo dropInfo) {
        GlobalStateMgr.getCurrentState().getEditLog().logDropRollup(dropInfo);
    }

    public void replayDropRollup(DropInfo dropInfo, GlobalStateMgr globalStateMgr) {
        Database db = getDb(dropInfo.getDbId());
        long tableId = dropInfo.getTableId();
        long rollupIndexId = dropInfo.getIndexId();
        OlapTable olapTable = (OlapTable) getTable(db.getId(), tableId);

        try (AutoCloseableLock ignore =
                     new AutoCloseableLock(new Locker(), db.getId(), Lists.newArrayList(tableId), LockType.WRITE)) {
            TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();

            for (PhysicalPartition partition : olapTable.getPhysicalPartitions()) {
                MaterializedIndex rollupIndex = partition.deleteRollupIndex(rollupIndexId);

                if (!GlobalStateMgr.isCheckpointThread()) {
                    // remove from inverted index
                    for (Tablet tablet : rollupIndex.getTablets()) {
                        invertedIndex.deleteTablet(tablet.getId());
                    }
                }
            }

            String rollupIndexName = olapTable.getIndexNameById(rollupIndexId);
            olapTable.deleteIndexInfo(rollupIndexName);
        }
        LOG.info("replay drop rollup {}", dropInfo.getIndexId());
    }

    @Override
    public void batchDropRollup(BatchDropInfo batchDropInfo) {
        GlobalStateMgr.getCurrentState().getEditLog().logBatchDropRollup(batchDropInfo);
    }

    @Override
    public void renameRollup(TableInfo tableInfo) {
        replayRenameRollup(tableInfo);
        GlobalStateMgr.getCurrentState().getEditLog().logRollupRename(tableInfo);
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

    @Override
    public List<Tablet> getAllTablets(MaterializedIndex materializedIndex) {
        return materializedIndex.getTablets();
    }

    @Override
    public List<Long> getAllTabletIDs(MaterializedIndex materializedIndex) {
        return materializedIndex.getTabletIdsInOrder();
    }

    @Override
    public Tablet getTablet(MaterializedIndex materializedIndex, Long tabletId) {
        return materializedIndex.getTablet(tabletId);
    }

    @Override
    public List<Replica> getAllReplicas(Tablet tablet) {
        return tablet.getAllReplicas();
    }

    @Override
    public Replica getReplica(LocalTablet tablet, Long replicaId) {
        return tablet.getReplicaById(replicaId);
    }

    @Override
    public void addReplica(ReplicaPersistInfo info) {
        replayAddReplica(info);
        GlobalStateMgr.getCurrentState().getEditLog().logAddReplica(info);
    }

    public void replayAddReplica(ReplicaPersistInfo info) {
        StarRocksMetadata starRocksMetadata = GlobalStateMgr.getServingState().getStarRocksMetadata();

        Database db = starRocksMetadata.getDbIncludeRecycleBin(info.getDbId());
        if (db == null) {
            LOG.warn("replay add replica failed, db is null, info: {}", info);
            return;
        }
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            OlapTable olapTable = (OlapTable) starRocksMetadata.getTableIncludeRecycleBin(db, info.getTableId());
            if (olapTable == null) {
                LOG.warn("replay add replica failed, table is null, info: {}", info);
                return;
            }
            PhysicalPartition partition = starRocksMetadata
                    .getPhysicalPartitionIncludeRecycleBin(olapTable, info.getPartitionId());
            if (partition == null) {
                LOG.warn("replay add replica failed, partition is null, info: {}", info);
                return;
            }
            MaterializedIndex materializedIndex = partition.getIndex(info.getIndexId());
            if (materializedIndex == null) {
                LOG.warn("replay add replica failed, materializedIndex is null, info: {}", info);
                return;
            }
            LocalTablet tablet = (LocalTablet) materializedIndex.getTablet(info.getTabletId());
            if (tablet == null) {
                LOG.warn("replay add replica failed, tablet is null, info: {}", info);
                return;
            }

            // for compatibility
            int schemaHash = info.getSchemaHash();
            if (schemaHash == -1) {
                schemaHash = olapTable.getSchemaHashByIndexId(info.getIndexId());
            }

            Replica replica = new Replica(info.getReplicaId(), info.getBackendId(), info.getVersion(),
                    schemaHash, info.getDataSize(), info.getRowCount(),
                    Replica.ReplicaState.NORMAL,
                    info.getLastFailedVersion(),
                    info.getLastSuccessVersion(),
                    info.getMinReadableVersion());
            tablet.addReplica(replica);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
    }

    @Override
    public void deleteReplica(ReplicaPersistInfo replicaPersistInfo) {
        replayDeleteReplica(replicaPersistInfo);
        GlobalStateMgr.getCurrentState().getEditLog().logDeleteReplica(replicaPersistInfo);
    }

    public void replayDeleteReplica(ReplicaPersistInfo info) {
        StarRocksMetadata starRocksMetadata = GlobalStateMgr.getServingState().getStarRocksMetadata();

        Database db = starRocksMetadata.getDbIncludeRecycleBin(info.getDbId());
        if (db == null) {
            LOG.warn("replay delete replica failed, db is null, info: {}", info);
            return;
        }
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            OlapTable olapTable = (OlapTable) starRocksMetadata.getTableIncludeRecycleBin(db, info.getTableId());
            if (olapTable == null) {
                LOG.warn("replay delete replica failed, table is null, info: {}", info);
                return;
            }
            PhysicalPartition partition = starRocksMetadata
                    .getPhysicalPartitionIncludeRecycleBin(olapTable, info.getPartitionId());
            if (partition == null) {
                LOG.warn("replay delete replica failed, partition is null, info: {}", info);
                return;
            }
            MaterializedIndex materializedIndex = partition.getIndex(info.getIndexId());
            if (materializedIndex == null) {
                LOG.warn("replay delete replica failed, materializedIndex is null, info: {}", info);
                return;
            }
            LocalTablet tablet = (LocalTablet) materializedIndex.getTablet(info.getTabletId());
            if (tablet == null) {
                LOG.warn("replay delete replica failed, tablet is null, info: {}", info);
                return;
            }
            tablet.deleteReplicaByBackendId(info.getBackendId());
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
    }

    @Override
    public void batchDeleteReplicaInfo(BatchDeleteReplicaInfo replicaPersistInfo) {
        GlobalStateMgr.getCurrentState().getEditLog().logBatchDeleteReplica(replicaPersistInfo);
    }

    public void replayBatchDeleteReplica(BatchDeleteReplicaInfo info) {
        if (info.getReplicaInfoList() != null) {
            for (ReplicaPersistInfo persistInfo : info.getReplicaInfoList()) {
                replayDeleteReplica(persistInfo);
            }
        } else {
            LOG.warn("invalid BatchDeleteReplicaInfo, replicaInfoList is null");
        }
    }

    @Override
    public void updateReplica(ReplicaPersistInfo replicaPersistInfo) {
        replayUpdateReplica(replicaPersistInfo);
        GlobalStateMgr.getCurrentState().getEditLog().logUpdateReplica(replicaPersistInfo);
    }

    public void replayUpdateReplica(ReplicaPersistInfo info) {
        StarRocksMetadata starRocksMetadata = GlobalStateMgr.getServingState().getStarRocksMetadata();

        Database db = starRocksMetadata.getDbIncludeRecycleBin(info.getDbId());
        if (db == null) {
            LOG.warn("replay update replica failed, db is null, info: {}", info);
            return;
        }
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            OlapTable olapTable = (OlapTable) starRocksMetadata.getTableIncludeRecycleBin(db, info.getTableId());
            if (olapTable == null) {
                LOG.warn("replay update replica failed, table is null, info: {}", info);
                return;
            }
            PhysicalPartition partition = starRocksMetadata
                    .getPhysicalPartitionIncludeRecycleBin(olapTable, info.getPartitionId());
            if (partition == null) {
                LOG.warn("replay update replica failed, partition is null, info: {}", info);
                return;
            }
            MaterializedIndex materializedIndex = partition.getIndex(info.getIndexId());
            if (materializedIndex == null) {
                LOG.warn("replay update replica failed, materializedIndex is null, info: {}", info);
                return;
            }
            LocalTablet tablet = (LocalTablet) materializedIndex.getTablet(info.getTabletId());
            if (tablet == null) {
                LOG.warn("replay update replica failed, tablet is null, info: {}", info);
                return;
            }
            Replica replica = tablet.getReplicaByBackendId(info.getBackendId());
            if (replica == null) {
                LOG.warn("replay update replica failed, replica is null, info: {}", info);
                return;
            }
            replica.updateRowCount(info.getVersion(), info.getMinReadableVersion(), info.getDataSize(), info.getRowCount());
            replica.setBad(false);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
    }

    @Override
    public void setReplicaStatus(SetReplicaStatusOperationLog log) {
        GlobalStateMgr.getCurrentState().getEditLog().logSetReplicaStatus(log);
    }

    public void replaySetReplicaStatus(SetReplicaStatusOperationLog log) {
        long tabletId = log.getTabletId();
        long backendId = log.getBackendId();
        Replica.ReplicaStatus status = log.getReplicaStatus();

        TabletInvertedIndex tabletInvertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        TabletMeta meta = tabletInvertedIndex.getTabletMeta(tabletId);
        if (meta == null) {
            LOG.info("tablet {} does not exist", tabletId);
            return;
        }
        long dbId = meta.getDbId();
        Database db = getDb(dbId);
        if (db == null) {
            LOG.info("database {} of tablet {} does not exist", dbId, tabletId);
            return;
        }
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            Replica replica = tabletInvertedIndex.getReplica(tabletId, backendId);
            if (replica == null) {
                LOG.info("replica of tablet {} does not exist", tabletId);
                return;
            }
            if (status == Replica.ReplicaStatus.BAD || status == Replica.ReplicaStatus.OK) {
                replica.setBadForce(status == Replica.ReplicaStatus.BAD);
            }
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
    }

    @Override
    public void backendTabletsInfo(BackendTabletsInfo backendTabletsInfo) {
        replayBackendTabletsInfo(backendTabletsInfo);
        GlobalStateMgr.getCurrentState().getEditLog().logBackendTabletsInfo(backendTabletsInfo);
    }

    public void replayBackendTabletsInfo(BackendTabletsInfo backendTabletsInfo) {
        List<Pair<Long, Integer>> tabletsWithSchemaHash = backendTabletsInfo.getTabletSchemaHash();
        if (!tabletsWithSchemaHash.isEmpty()) {
            // In previous version, we save replica info in `tabletsWithSchemaHash`,
            // but it is wrong because we can not get replica from `tabletInvertedIndex` when doing checkpoint,
            // because when doing checkpoint, the tabletInvertedIndex is not initialized at all.
            //
            // So we can only discard this information, in this case, it is equivalent to losing the record of these operations.
            // But it doesn't matter, these records are currently only used to record whether a replica is in a bad state.
            // This state has little effect on the system, and it can be restored after the system has processed the bad state replica.
            for (Pair<Long, Integer> tabletInfo : tabletsWithSchemaHash) {
                LOG.warn("find an old backendTabletsInfo for tablet {}, ignore it", tabletInfo.first);
            }
            return;
        }

        // in new version, replica info is saved here.
        // but we need to get replica from db->tbl->partition->...
        List<ReplicaPersistInfo> replicaPersistInfos = backendTabletsInfo.getReplicaPersistInfos();
        for (ReplicaPersistInfo info : replicaPersistInfos) {
            long dbId = info.getDbId();
            Database db = getDb(dbId);
            if (db == null) {
                continue;
            }
            Locker locker = new Locker();
            locker.lockDatabase(db.getId(), LockType.WRITE);
            try {
                OlapTable tbl = (OlapTable) getTable(db.getId(), info.getTableId());
                if (tbl == null) {
                    continue;
                }
                Partition partition = tbl.getPartition(info.getPartitionId());
                if (partition == null) {
                    continue;
                }
                MaterializedIndex mindex = partition.getDefaultPhysicalPartition().getIndex(info.getIndexId());
                if (mindex == null) {
                    continue;
                }
                LocalTablet tablet = (LocalTablet) mindex.getTablet(info.getTabletId());
                if (tablet == null) {
                    continue;
                }
                Replica replica = tablet.getReplicaById(info.getReplicaId());
                if (replica != null) {
                    replica.setBad(true);
                    LOG.debug("get replica {} of tablet {} on backend {} to bad when replaying",
                            info.getReplicaId(), info.getTabletId(), info.getBackendId());
                }
            } finally {
                locker.unLockDatabase(db.getId(), LockType.WRITE);
            }
        }
    }

    @Override
    public void finishConsistencyCheck(ConsistencyCheckInfo info) {
        replayFinishConsistencyCheck(info);
        GlobalStateMgr.getCurrentState().getEditLog().logFinishConsistencyCheckNoWait(info);
    }

    public void replayFinishConsistencyCheck(ConsistencyCheckInfo info) {
        Database db = getDb(info.getDbId());
        if (db == null) {
            LOG.warn("replay finish consistency check failed, db is null, info: {}", info);
            return;
        }
        OlapTable table = (OlapTable) getTable(db.getId(), info.getTableId());
        if (table == null) {
            LOG.warn("replay finish consistency check failed, table is null, info: {}", info);
            return;
        }

        try (AutoCloseableLock ignore
                     = new AutoCloseableLock(new Locker(), db.getId(), Lists.newArrayList(table.getId()), LockType.WRITE)) {
            Partition partition = table.getPartition(info.getPartitionId());
            if (partition == null) {
                LOG.warn("replay finish consistency check failed, partition is null, info: {}", info);
                return;
            }
            MaterializedIndex index = partition.getDefaultPhysicalPartition().getIndex(info.getIndexId());
            if (index == null) {
                LOG.warn("replay finish consistency check failed, index is null, info: {}", info);
                return;
            }
            LocalTablet tablet = (LocalTablet) getTablet(index, info.getTabletId());
            if (tablet == null) {
                LOG.warn("replay finish consistency check failed, tablet is null, info: {}", info);
                return;
            }

            long lastCheckTime = info.getLastCheckTime();
            db.setLastCheckTime(lastCheckTime);
            table.setLastCheckTime(lastCheckTime);
            partition.setLastCheckTime(lastCheckTime);
            index.setLastCheckTime(lastCheckTime);
            tablet.setLastCheckTime(lastCheckTime);
            tablet.setCheckedVersion(info.getCheckedVersion());

            tablet.setIsConsistent(info.isConsistent());
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
        /*
        if (!isCheckpointThread() && GlobalStateMgr.getCurrentState().usingBDBMetastore) {
            return;
        }
         */

        int dbSize = reader.readInt();
        for (int i = 0; i < dbSize; ++i) {
            DatabaseLog db = reader.readJson(DatabaseLog.class);
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

    public void saveKV() {
        //TODO: clear all first
        MetadataHandler metadataHandler = GlobalStateMgr.getServingState().getMetadataHandler();
        Transaction transaction = metadataHandler.starTransaction();

        for (Map.Entry<Long, Database> entry : idToDb.entrySet()) {
            Database db = entry.getValue();

            metadataHandler.put(transaction,
                    ByteCoder.encode("meta_object", "db", db.getId()),
                    db, Database.class);
            metadataHandler.put(transaction,
                    ByteCoder.encode("meta_name", "db", db.getFullName()),
                    db.getId(), Long.class);
            metadataHandler.put(transaction,
                    ByteCoder.encode("meta_id", "instance", String.valueOf(db.getId())),
                    "", String.class);
        }
        transaction.commit();
    }

    public void buildTabletIndex() {
        for (Database db : GlobalStateMgr.getCurrentState().getMetastore().getFullNameToDb().values()) {
            long dbId = db.getId();
            for (Table table : db.getTables()) {
                if (!table.isNativeTableOrMaterializedView()) {
                    continue;
                }

                OlapTable olapTable = (OlapTable) table;
                try (AutoCloseableLock ignore =
                             new AutoCloseableLock(new Locker(), dbId, Lists.newArrayList(olapTable.getId()), LockType.READ)) {
                    GlobalStateMgr.getCurrentState().getTabletInvertedIndex().buildTabletInvertedIndex(
                            dbId, olapTable, olapTable.getAllPhysicalPartitions());
                } // end for tables
            }
        } // end for dbs
    }
}
