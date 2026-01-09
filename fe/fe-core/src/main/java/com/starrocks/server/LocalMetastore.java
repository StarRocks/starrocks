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
import com.google.common.base.Strings;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.staros.proto.FilePathInfo;
import com.starrocks.alter.AlterJobExecutor;
import com.starrocks.alter.AlterMVJobExecutor;
import com.starrocks.alter.MaterializedViewHandler;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.ObjectType;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.binlog.BinlogConfig;
import com.starrocks.catalog.CatalogRecycleBin;
import com.starrocks.catalog.CatalogUtils;
import com.starrocks.catalog.ColocateGroupSchema;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.DistributionInfoBuilder;
import com.starrocks.catalog.FlatJsonConfig;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MaterializedViewRefreshType;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionInfoBuilder;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.View;
import com.starrocks.catalog.system.information.InfoSchemaDb;
import com.starrocks.catalog.system.sys.SysDb;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.InvalidOlapTableStateException;
import com.starrocks.common.MaterializedViewExceptions;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.DynamicPartitionUtil;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.Util;
import com.starrocks.common.util.concurrent.CountingLatch;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.journal.SerializeException;
import com.starrocks.lake.DataCacheInfo;
import com.starrocks.lake.LakeMaterializedView;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.StorageInfo;
import com.starrocks.load.pipe.PipeManager;
import com.starrocks.memory.MemoryTrackable;
import com.starrocks.mv.MVMetaVersionRepairer;
import com.starrocks.mv.MVRepairHandler;
import com.starrocks.mv.analyzer.MVPartitionExprResolver;
import com.starrocks.persist.AddPartitionsInfoV2;
import com.starrocks.persist.AddSubPartitionsInfoV2;
import com.starrocks.persist.AutoIncrementInfo;
import com.starrocks.persist.BackendIdsUpdateInfo;
import com.starrocks.persist.BackendTabletsInfo;
import com.starrocks.persist.BatchDeleteReplicaInfo;
import com.starrocks.persist.ColocatePersistInfo;
import com.starrocks.persist.ColumnRenameInfo;
import com.starrocks.persist.CreateDbInfo;
import com.starrocks.persist.CreateTableInfo;
import com.starrocks.persist.DatabaseInfo;
import com.starrocks.persist.DisablePartitionRecoveryInfo;
import com.starrocks.persist.DisableTableRecoveryInfo;
import com.starrocks.persist.DropDbInfo;
import com.starrocks.persist.DropPartitionInfo;
import com.starrocks.persist.DropPartitionsInfo;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.ListPartitionPersistInfo;
import com.starrocks.persist.ModifyColumnCommentLog;
import com.starrocks.persist.ModifyPartitionInfo;
import com.starrocks.persist.ModifyTableColumnOperationLog;
import com.starrocks.persist.ModifyTablePropertyOperationLog;
import com.starrocks.persist.MultiEraseTableInfo;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.PartitionPersistInfoV2;
import com.starrocks.persist.PartitionVersionRecoveryInfo;
import com.starrocks.persist.PartitionVersionRecoveryInfo.PartitionVersion;
import com.starrocks.persist.PhysicalPartitionPersistInfoV2;
import com.starrocks.persist.RangePartitionPersistInfo;
import com.starrocks.persist.RecoverInfo;
import com.starrocks.persist.ReplacePartitionOperationLog;
import com.starrocks.persist.ReplicaPersistInfo;
import com.starrocks.persist.SetReplicaStatusOperationLog;
import com.starrocks.persist.TableInfo;
import com.starrocks.persist.TruncateTableInfo;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.ExecuteOption;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.AdminCheckTabletsStmt;
import com.starrocks.sql.ast.AdminSetPartitionVersionStmt;
import com.starrocks.sql.ast.AdminSetReplicaStatusStmt;
import com.starrocks.sql.ast.AlterDatabaseQuotaStmt;
import com.starrocks.sql.ast.AlterDatabaseQuotaStmt.QuotaType;
import com.starrocks.sql.ast.AlterDatabaseRenameStatement;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.sql.ast.AlterTableCommentClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.AlterViewStmt;
import com.starrocks.sql.ast.AsyncRefreshSchemeDesc;
import com.starrocks.sql.ast.CancelAlterTableStmt;
import com.starrocks.sql.ast.CancelRefreshMaterializedViewStmt;
import com.starrocks.sql.ast.ColumnRenameClause;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateMaterializedViewStmt;
import com.starrocks.sql.ast.CreateTableLikeStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.CreateTemporaryTableStmt;
import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.DropMaterializedViewStmt;
import com.starrocks.sql.ast.DropPartitionClause;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.ExpressionPartitionDesc;
import com.starrocks.sql.ast.HintNode;
import com.starrocks.sql.ast.IncrementalRefreshSchemeDesc;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.ManualRefreshSchemeDesc;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.PartitionRangeDesc;
import com.starrocks.sql.ast.PartitionRenameClause;
import com.starrocks.sql.ast.RecoverDbStmt;
import com.starrocks.sql.ast.RecoverPartitionStmt;
import com.starrocks.sql.ast.RecoverTableStmt;
import com.starrocks.sql.ast.RefreshMaterializedViewStatement;
import com.starrocks.sql.ast.RefreshSchemeClause;
import com.starrocks.sql.ast.ReplacePartitionClause;
import com.starrocks.sql.ast.ReplicaStatus;
import com.starrocks.sql.ast.RollupRenameClause;
import com.starrocks.sql.ast.ShowAlterStmt;
import com.starrocks.sql.ast.SingleRangePartitionDesc;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SyncRefreshSchemeDesc;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.ast.TableRenameClause;
import com.starrocks.sql.ast.TruncateTableStmt;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprToSql;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.IntervalLiteral;
import com.starrocks.sql.ast.expression.SetVarHint;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.ast.expression.UserVariableHint;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.common.PListCell;
import com.starrocks.sql.common.SyncPartitionUtils;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.optimizer.statistics.IDictManager;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.util.EitherOr;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.task.TabletTaskExecutor;
import com.starrocks.thrift.TGetTasksParams;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.thrift.TTabletMetaType;
import com.starrocks.warehouse.Warehouse;
import com.starrocks.warehouse.cngroup.CRAcquireContext;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.threeten.extra.PeriodDuration;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;

import static com.starrocks.server.GlobalStateMgr.NEXT_ID_INIT_VALUE;
import static com.starrocks.server.GlobalStateMgr.isCheckpointThread;

public class LocalMetastore implements ConnectorMetadata, MVRepairHandler, MemoryTrackable {
    private static final Logger LOG = LogManager.getLogger(LocalMetastore.class);

    private final ConcurrentHashMap<Long, Database> idToDb = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Database> fullNameToDb = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, Long> tableIdToIncrementId = new ConcurrentHashMap<>();

    private final GlobalStateMgr stateMgr;
    private final CatalogRecycleBin recycleBin;
    private ColocateTableIndex colocateTableIndex;
    /**
     * Concurrent colocate table creation process have dependency on each other
     * (even in different databases), but we do not want to affect the performance
     * of non-colocate table creation, so here we use a separate latch to
     * synchronize only the creation of colocate tables.
     */
    private final CountingLatch colocateTableCreateSyncer = new CountingLatch(0);

    public LocalMetastore(GlobalStateMgr globalStateMgr, CatalogRecycleBin recycleBin,
                          ColocateTableIndex colocateTableIndex) {
        this.stateMgr = globalStateMgr;
        this.recycleBin = recycleBin;
        this.colocateTableIndex = colocateTableIndex;

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

    boolean tryLock(boolean mustLock) {
        return stateMgr.tryLock(mustLock);
    }

    void unlock() {
        stateMgr.unlock();
    }

    long getNextId() {
        return stateMgr.getNextId();
    }

    GlobalStateMgr getStateMgr() {
        return stateMgr;
    }

    public void recreateTabletInvertIndex() {
        if (isCheckpointThread()) {
            return;
        }

        // create inverted index
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        for (Database db : this.fullNameToDb.values()) {
            long dbId = db.getId();
            for (Table table : db.getTables()) {
                if (!table.isNativeTableOrMaterializedView()) {
                    continue;
                }

                OlapTable olapTable = (OlapTable) table;
                if (olapTable.isMaterializedView()) {
                    // For mv, this may throw exception in mv backup/restore case, we should catch it and set mv to inactive
                    MaterializedView mv = (MaterializedView) olapTable;
                    try {
                        recreateOlapTableTabletInvertIndex(dbId, mv, invertedIndex);
                    } catch (Exception e) {
                        LOG.error("recreate inverted index for metadata table {} failed", mv.getName(), e);
                        mv.setInactiveAndReason(MaterializedViewExceptions.inactiveReasonForMetadataTableRestoreCorrupted(
                                mv.getName()));
                    }
                } else {
                    recreateOlapTableTabletInvertIndex(dbId, olapTable, invertedIndex);
                }
            } // end for tables
        } // end for dbs
    }

    private void recreateOlapTableTabletInvertIndex(long dbId,
                                                    OlapTable olapTable,
                                                    TabletInvertedIndex invertedIndex) {
        long tableId = olapTable.getId();
        for (PhysicalPartition partition : olapTable.getAllPhysicalPartitions()) {
            long partitionId = partition.getParentId();
            TStorageMedium medium = olapTable.getPartitionInfo().getDataProperty(
                    partitionId).getStorageMedium();
            long physicalPartitionId = partition.getId();
            for (MaterializedIndex index : partition
                    .getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                long indexId = index.getId();
                TabletMeta tabletMeta = new TabletMeta(dbId, tableId, physicalPartitionId,
                        indexId, medium, olapTable.isCloudNativeTableOrMaterializedView());
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
        }
    }

    public void createDb(String dbName) throws DdlException, AlreadyExistsException {
        createDb(dbName, new HashMap<>());
    }

    @Override
    public void createDb(ConnectContext context, String dbName, Map<String, String> properties)
            throws DdlException, AlreadyExistsException {
        createDb(dbName, properties);
    }

    public void createDb(String dbName, Map<String, String> properties) throws DdlException, AlreadyExistsException {
        long id = 0L;
        if (!tryLock(false)) {
            throw new DdlException("Failed to acquire globalStateMgr lock. Try again");
        }
        try {
            if (fullNameToDb.containsKey(dbName)) {
                throw new AlreadyExistsException("Database Already Exists");
            } else {
                id = getNextId();
                Database db = new Database(id, dbName);
                String volume = StorageVolumeMgr.DEFAULT;
                if (properties != null && properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_VOLUME)) {
                    volume = properties.remove(PropertyAnalyzer.PROPERTIES_STORAGE_VOLUME);
                }
                if (!GlobalStateMgr.getCurrentState().getStorageVolumeMgr().bindDbToStorageVolume(volume, id)) {
                    throw new DdlException(String.format("Storage volume %s not exists", volume));
                }
                unprotectCreateDb(db);
                String storageVolumeId = GlobalStateMgr.getCurrentState().getStorageVolumeMgr().getStorageVolumeIdOfDb(id);
                GlobalStateMgr.getCurrentState().getEditLog().logCreateDb(db, storageVolumeId);
            }
        } finally {
            unlock();
        }
        LOG.info("createDb dbName = " + dbName + ", id = " + id);
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

    public ConcurrentHashMap<Long, Database> getIdToDb() {
        return idToDb;
    }

    public void replayCreateDb(Database db) {
        tryLock(true);
        try {
            unprotectCreateDb(db);
            LOG.info("finish replay create db, name: {}, id: {}", db.getOriginName(), db.getId());
        } finally {
            unlock();
        }
    }

    public void replayCreateDb(CreateDbInfo createDbInfo) {
        tryLock(true);
        try {
            Database db = new Database(createDbInfo.getId(), createDbInfo.getDbName());
            unprotectCreateDb(db);
            // If user upgrades from 3.0, the storage volume id will be null
            if (createDbInfo.getStorageVolumeId() != null) {
                stateMgr.getStorageVolumeMgr().replayBindDbToStorageVolume(createDbInfo.getStorageVolumeId(), db.getId());
            }
            LOG.info("finish replay create db, name: {}, id: {}", db.getOriginName(), db.getId());
        } finally {
            unlock();
        }
    }

    @Override
    public void dropDb(ConnectContext context, String dbName, boolean isForceDrop) throws DdlException, MetaNotFoundException {
        // 1. check if database exists
        Database db;
        if (!tryLock(false)) {
            throw new DdlException("Failed to acquire globalStateMgr lock. Try again");
        }
        try {
            if (!fullNameToDb.containsKey(dbName)) {
                throw new MetaNotFoundException("Database not found");
            }
            db = this.fullNameToDb.get(dbName);
            if (!isForceDrop && !db.getTemporaryTables().isEmpty()) {
                throw new DdlException("The database [" + dbName + "] " +
                        "cannot be dropped because there are still some temporary tables in it. " +
                        "If you want to forcibly drop, please use \"DROP DATABASE <database> FORCE.\"");
            }
        } finally {
            unlock();
        }

        // 2. drop tables in db
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            if (!db.isExist()) {
                throw new MetaNotFoundException("Database '" + dbName + "' not found");
            }
            if (!isForceDrop && stateMgr.getGlobalTransactionMgr().existCommittedTxns(db.getId(), null, null)) {
                throw new DdlException(
                        "There are still some transactions in the COMMITTED state waiting to be completed. " +
                                "The database [" + dbName +
                                "] cannot be dropped. If you want to forcibly drop(cannot be recovered)," +
                                " please use \"DROP DATABASE <database> FORCE\".");
            }

            // save table names for recycling
            Set<String> tableNames = new HashSet<>(db.getTableNamesViewWithLock());
            unprotectDropDb(db, isForceDrop, false);
            recycleBin.recycleDatabase(db, tableNames, !isForceDrop);
            db.setExist(false);

            // 3. remove db from globalStateMgr
            idToDb.remove(db.getId());
            fullNameToDb.remove(db.getFullName());

            // 4. drop mv task
            TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
            TGetTasksParams tasksParams = new TGetTasksParams();
            tasksParams.setDb(dbName);
            List<Long> dropTaskIdList = taskManager.filterTasks(tasksParams)
                    .stream().map(Task::getId).collect(Collectors.toList());
            taskManager.dropTasks(dropTaskIdList);

            DropDbInfo info = new DropDbInfo(db.getFullName(), isForceDrop);
            GlobalStateMgr.getCurrentState().getEditLog().logDropDb(info);

            // 5. Drop Pipes
            PipeManager pipeManager = GlobalStateMgr.getCurrentState().getPipeManager();
            pipeManager.dropPipesOfDb(dbName, db.getId());

            LOG.info("finish drop database[{}], id: {}, is force : {}", dbName, db.getId(), isForceDrop);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
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

    public void replayDropDb(String dbName, boolean isForceDrop) throws DdlException {
        tryLock(true);
        try {
            Database db = fullNameToDb.get(dbName);
            Locker locker = new Locker();
            locker.lockDatabase(db.getId(), LockType.WRITE);
            try {
                Set<String> tableNames = new HashSet(db.getTableNamesViewWithLock());
                unprotectDropDb(db, isForceDrop, true);
                recycleBin.recycleDatabase(db, tableNames, !isForceDrop);
                db.setExist(false);
            } finally {
                locker.unLockDatabase(db.getId(), LockType.WRITE);
            }

            fullNameToDb.remove(dbName);
            idToDb.remove(db.getId());

            LOG.info("finish replay drop db, name: {}, id: {}", dbName, db.getId());
        } finally {
            unlock();
        }
    }

    public void recoverDatabase(RecoverDbStmt recoverStmt) throws DdlException {
        // check is new db with same name already exist
        if (getDb(recoverStmt.getDbName()) != null) {
            throw new DdlException("Database[" + recoverStmt.getDbName() + "] already exist.");
        }

        Database db = recycleBin.recoverDatabase(recoverStmt.getDbName());

        // add db to globalStateMgr
        if (!tryLock(false)) {
            throw new DdlException("Failed to acquire globalStateMgr lock. Try again");
        }
        try {
            if (fullNameToDb.containsKey(db.getFullName())) {
                throw new DdlException("Database[" + db.getOriginName() + "] already exist.");
                // it's ok that we do not put db back to CatalogRecycleBin
                // cause this db cannot recover anymore
            }

            fullNameToDb.put(db.getFullName(), db);
            idToDb.put(db.getId(), db);
            Locker locker = new Locker();
            locker.lockDatabase(db.getId(), LockType.WRITE);
            db.setExist(true);
            locker.unLockDatabase(db.getId(), LockType.WRITE);

            List<MaterializedView> materializedViews = db.getMaterializedViews();
            TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
            for (MaterializedView materializedView : materializedViews) {
                MaterializedViewRefreshType refreshType = materializedView.getRefreshScheme().getType();
                if (refreshType != MaterializedViewRefreshType.SYNC) {
                    Task task = TaskBuilder.buildMvTask(materializedView, db.getFullName());
                    TaskBuilder.updateTaskInfo(task, materializedView);
                    taskManager.createTask(task);
                }
            }

            // log
            RecoverInfo recoverInfo = new RecoverInfo(db.getId(), -1L, -1L);
            GlobalStateMgr.getCurrentState().getEditLog().logRecoverDb(recoverInfo);
        } finally {
            unlock();
        }

        LOG.info("finish recover database, name: {}, id: {}", recoverStmt.getDbName(), db.getId());
    }

    public void recoverTable(RecoverTableStmt recoverStmt) throws DdlException {
        String dbName = recoverStmt.getDbName();

        Database db = null;
        if ((db = getDb(dbName)) == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        String tableName = recoverStmt.getTableName();
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            Table table = getTable(db.getFullName(), tableName);
            if (table != null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
            }

            if (!recycleBin.recoverTable(db, tableName)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName);
            }

            Table recoverTable = getTable(db.getFullName(), tableName);
            if (recoverTable instanceof OlapTable) {
                DynamicPartitionUtil.registerOrRemovePartitionScheduleInfo(db.getId(), (OlapTable) recoverTable);
            }

        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
    }

    public void recoverPartition(RecoverPartitionStmt recoverStmt) throws DdlException {
        String dbName = recoverStmt.getDbName();

        Database db = null;
        if ((db = getDb(dbName)) == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        String tableName = recoverStmt.getTableName();
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            Table table = getTable(db.getFullName(), tableName);
            if (table == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName);
            }

            if (!table.isOlapOrCloudNativeTable()) {
                throw new DdlException("table[" + tableName + "] is not OLAP table or LAKE table");
            }
            OlapTable olapTable = (OlapTable) table;

            String partitionName = recoverStmt.getPartitionName();
            if (olapTable.getPartition(partitionName) != null) {
                throw new DdlException("partition[" + partitionName + "] already exist in table[" + tableName + "]");
            }

            recycleBin.recoverPartition(db.getId(), olapTable, partitionName);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
    }

    public void replayEraseDatabase(long dbId) {
        recycleBin.replayEraseDatabase(dbId);
    }

    public void replayRecoverDatabase(RecoverInfo info) {
        long dbId = info.getDbId();
        Database db = recycleBin.replayRecoverDatabase(dbId);

        // add db to globalStateMgr
        replayCreateDb(db);

        LOG.info("replay recover db[{}], name: {}", dbId, db.getOriginName());
    }

    public void alterDatabaseQuota(AlterDatabaseQuotaStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();
        Database db = getDb(dbName);
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        Preconditions.checkArgument(stmt.getQuota() >= 0, "Quota must be non-negative");

        DatabaseInfo dbInfo = new DatabaseInfo(db.getFullName(), "", stmt.getQuota(), stmt.getQuotaType());
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            GlobalStateMgr.getCurrentState().getEditLog().logAlterDb(dbInfo, wal -> {
                if (stmt.getQuotaType() == QuotaType.DATA) {
                    db.setDataQuota(stmt.getQuota());
                } else if (stmt.getQuotaType() == QuotaType.REPLICA) {
                    db.setReplicaQuota(stmt.getQuota());
                }
            });
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
    }

    public void replayAlterDatabaseQuota(DatabaseInfo dbInfo) {
        String dbName = dbInfo.getDbName();
        LOG.info("Begin to unprotect alter db info {}", dbName);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        QuotaType quotaType = dbInfo.getQuotaType();
        long quota = dbInfo.getQuota();

        Preconditions.checkNotNull(db);
        if (quotaType == QuotaType.DATA) {
            db.setDataQuota(quota);
        } else if (quotaType == QuotaType.REPLICA) {
            db.setReplicaQuota(quota);
        }
    }

    public void renameDatabase(AlterDatabaseRenameStatement stmt) throws DdlException {
        String fullDbName = stmt.getDbName();
        String newFullDbName = stmt.getNewDbName();

        if (fullDbName.equals(newFullDbName)) {
            throw new DdlException("Same database name");
        }

        Database db;
        if (!tryLock(false)) {
            throw new DdlException("Failed to acquire globalStateMgr lock. Try again");
        }
        try {
            // check if db exists
            db = fullNameToDb.get(fullDbName);
            if (db == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, fullDbName);
            }

            // check if name is already used
            if (fullNameToDb.get(newFullDbName) != null) {
                throw new DdlException("Database name[" + newFullDbName + "] is already used");
            }

            DatabaseInfo dbInfo =
                    new DatabaseInfo(fullDbName, newFullDbName, -1L, AlterDatabaseQuotaStmt.QuotaType.NONE);
            Locker locker = new Locker();
            locker.lockDatabase(db.getId(), LockType.WRITE);
            try {
                GlobalStateMgr.getCurrentState().getEditLog().logDatabaseRename(dbInfo, wal -> {
                    renameDbInternal(db, newFullDbName);
                });
            } finally {
                locker.unLockDatabase(db.getId(), LockType.WRITE);
            }
        } finally {
            unlock();
        }

        LOG.info("rename database[{}] to [{}], id: {}", fullDbName, newFullDbName, db.getId());
    }

    public void replayRenameDatabase(String dbName, String newDbName) {
        tryLock(true);
        try {
            Database db = fullNameToDb.get(dbName);
            renameDbInternal(db, newDbName);
            LOG.info("replay rename database {} to {}, id: {}", dbName, newDbName, db.getId());
        } finally {
            unlock();
        }
    }

    private void renameDbInternal(Database db, String newDbName) {
        String oldDbName = db.getFullName();
        db.setName(newDbName);
        fullNameToDb.remove(oldDbName);
        fullNameToDb.put(newDbName, db);
    }

    @Override
    public boolean createTable(ConnectContext context, CreateTableStmt stmt) throws DdlException {
        return createTable(stmt);
    }

    /**
     * Following is the step to create an olap table:
     * 1. create columns
     * 2. create partition info
     * 3. create distribution info
     * 4. set table id and base index id
     * 5. set bloom filter columns
     * 6. set and build TableProperty includes:
     * 6.1. dynamicProperty
     * 6.2. replicationNum
     * 6.3. inMemory
     * 7. set index meta
     * 8. check colocation properties
     * 9. create tablet in BE
     * 10. add this table to FE's meta
     * 11. add this table to ColocateGroup if necessary
     *
     * @return whether the table is created
     */
    public boolean createTable(CreateTableStmt stmt) throws DdlException {
        // check if db exists
        Database db = getDb(stmt.getDbName());
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, stmt.getDbName());
        }

        boolean isTemporaryTable = (stmt instanceof CreateTemporaryTableStmt);
        // perform the existence check which is cheap before any further heavy operations.
        // NOTE: don't even check the quota if already exists.
        String tableName = stmt.getTableName();
        if (!isTemporaryTable && getTable(db.getFullName(), tableName) != null) {
            if (!stmt.isSetIfNotExists()) {
                throw new DdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
            } else {
                LOG.info("create table[{}] which already exists", tableName);
                return false;
            }
        }

        if (!stmt.isExternal()) {
            // check cluster capacity
            try {
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().checkClusterCapacity();
                checkDataSizeQuota(db);
                checkReplicaQuota(db);
            } catch (StarRocksException e) {
                throw new DdlException(e.getMessage());
            }
        }

        AbstractTableFactory tableFactory = TableFactoryProvider.getFactory(stmt.getEngineName());
        if (tableFactory == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_STORAGE_ENGINE, stmt.getEngineName());
        }

        Table table = tableFactory.createTable(this, db, stmt);
        String storageVolumeId = GlobalStateMgr.getCurrentState().getStorageVolumeMgr()
                .getStorageVolumeIdOfTable(table.getId());

        try {
            onCreate(db, table, storageVolumeId, stmt.isSetIfNotExists());
        } catch (DdlException e) {
            if (table.isCloudNativeTable()) {
                GlobalStateMgr.getCurrentState().getStorageVolumeMgr().unbindTableToStorageVolume(table.getId());
            }
            throw e;
        }
        return true;
    }

    @Override
    public void createTableLike(CreateTableLikeStmt stmt) throws DdlException {
        createTable(stmt.getCreateTableStmt());
    }

    @Override
    public void addPartitions(ConnectContext ctx, Database db, String tableName, AddPartitionClause addPartitionClause)
            throws DdlException {
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.READ);
        try {
            Table table = getTable(db.getFullName(), tableName);
            CatalogUtils.checkTableExist(db, tableName);
            CatalogUtils.checkNativeTable(db, table);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
        }
        addPartitions(ctx, db, tableName,
                addPartitionClause.getResolvedPartitionDescList(),
                addPartitionClause.isTempPartition(),
                addPartitionClause.getDistributionDesc());
    }

    private OlapTable checkTable(Database db, String tableName) throws DdlException {
        CatalogUtils.checkTableExist(db, tableName);
        Table table = getTable(db.getFullName(), tableName);
        CatalogUtils.checkNativeTable(db, table);
        OlapTable olapTable = (OlapTable) table;
        CatalogUtils.checkTableState(olapTable, tableName);
        return olapTable;
    }

    private OlapTable checkTable(Database db, Long tableId) throws DdlException {
        Table table = getTable(db.getId(), tableId);
        if (table == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tableId);
        }
        CatalogUtils.checkNativeTable(db, table);
        OlapTable olapTable = (OlapTable) table;
        CatalogUtils.checkTableState(olapTable, table.getName());
        return olapTable;
    }

    private void checkPartitionType(PartitionInfo partitionInfo) throws DdlException {
        PartitionType partitionType = partitionInfo.getType();
        if (!partitionInfo.isRangePartition() && partitionType != PartitionType.LIST) {
            throw new DdlException("Only support adding partition to range/list partitioned table");
        }
    }

    private DistributionInfo getDistributionInfo(OlapTable olapTable, DistributionDesc distributionDesc)
            throws DdlException {
        DistributionInfo distributionInfo;
        List<Column> baseSchema = olapTable.getBaseSchema();
        DistributionInfo defaultDistributionInfo = olapTable.getDefaultDistributionInfo();
        if (distributionDesc != null) {
            distributionInfo = DistributionInfoBuilder.build(distributionDesc, baseSchema);
            // for now. we only support modify distribution's bucket num
            if (distributionInfo.getType() != defaultDistributionInfo.getType()) {
                throw new DdlException("Cannot assign different distribution type. default is: "
                        + defaultDistributionInfo.getType());
            }

            if (distributionInfo.getType() == DistributionInfo.DistributionInfoType.HASH) {
                HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) distributionInfo;
                List<Column> newDistriCols = MetaUtils.getColumnsByColumnIds(olapTable,
                        hashDistributionInfo.getDistributionColumns());
                List<Column> defaultDistriCols = MetaUtils.getColumnsByColumnIds(olapTable,
                        defaultDistributionInfo.getDistributionColumns());
                if (!newDistriCols.equals(defaultDistriCols)) {
                    throw new DdlException("Cannot assign hash distribution with different distribution cols. "
                            + "default is: " + defaultDistriCols);
                }
                if (hashDistributionInfo.getBucketNum() < 0) {
                    throw new DdlException("Cannot assign hash distribution buckets less than 0");
                }
            }
            if (distributionInfo.getType() == DistributionInfo.DistributionInfoType.RANDOM) {
                RandomDistributionInfo randomDistributionInfo = (RandomDistributionInfo) distributionInfo;
                if (randomDistributionInfo.getBucketNum() < 0) {
                    throw new DdlException("Cannot assign random distribution buckets less than 0");
                }
            }
        } else {
            distributionInfo = defaultDistributionInfo;
        }
        return distributionInfo;
    }

    private void checkColocation(Database db, OlapTable olapTable, DistributionInfo distributionInfo,
                                 List<PartitionDesc> partitionDescs)
            throws DdlException {
        if (colocateTableIndex.isColocateTable(olapTable.getId())) {
            String fullGroupName = db.getId() + "_" + olapTable.getColocateGroup();
            ColocateGroupSchema groupSchema = colocateTableIndex.getGroupSchema(fullGroupName);
            Preconditions.checkNotNull(groupSchema);
            groupSchema.checkDistribution(olapTable.getIdToColumn(), distributionInfo);
            for (PartitionDesc partitionDesc : partitionDescs) {
                groupSchema.checkReplicationNum(partitionDesc.getReplicationNum());
            }
        }
    }

    private void checkDataProperty(List<PartitionDesc> partitionDescs) {
        for (PartitionDesc partitionDesc : partitionDescs) {
            DataProperty dataProperty = partitionDesc.getPartitionDataProperty();
            Preconditions.checkNotNull(dataProperty);
        }
    }

    private List<Pair<Partition, PartitionDesc>> createPartitionMap(Database db, OlapTable copiedTable,
                                                                    List<PartitionDesc> partitionDescs,
                                                                    HashMap<String, Set<Long>> partitionNameToTabletSet,
                                                                    Set<Long> tabletIdSetForAll,
                                                                    Set<String> existPartitionNameSet,
                                                                    ComputeResource computeResource)
            throws DdlException {
        List<Pair<Partition, PartitionDesc>> partitionList = Lists.newArrayList();
        for (PartitionDesc partitionDesc : partitionDescs) {
            long partitionId = getNextId();
            DataProperty dataProperty = partitionDesc.getPartitionDataProperty();
            String partitionName = partitionDesc.getPartitionName();
            if (existPartitionNameSet.contains(partitionName)) {
                continue;
            }
            Long version = partitionDesc.getVersionInfo();
            Set<Long> tabletIdSet = Sets.newHashSet();

            copiedTable.getPartitionInfo().setDataProperty(partitionId, dataProperty);
            copiedTable.getPartitionInfo().setReplicationNum(partitionId, partitionDesc.getReplicationNum());
            copiedTable.getPartitionInfo().setDataCacheInfo(partitionId, partitionDesc.getDataCacheInfo());

            Partition partition =
                    createPartition(db, copiedTable, partitionId, partitionName, version, tabletIdSet, computeResource);

            partitionList.add(Pair.create(partition, partitionDesc));
            tabletIdSetForAll.addAll(tabletIdSet);
            partitionNameToTabletSet.put(partitionName, tabletIdSet);
        }
        return partitionList;
    }

    private void checkIfMetaChange(OlapTable olapTable, OlapTable copiedTable, String tableName) throws DdlException {
        // rollup index may be added or dropped during add partition operation.
        // schema may be changed during add partition operation.
        boolean metaChanged = false;
        if (olapTable.getIndexNameToMetaId().size() != copiedTable.getIndexNameToMetaId().size()) {
            metaChanged = true;
        } else {
            // compare schemaHash
            for (Map.Entry<Long, MaterializedIndexMeta> entry : olapTable.getIndexMetaIdToMeta().entrySet()) {
                long indexMetaId = entry.getKey();
                if (!copiedTable.getIndexMetaIdToMeta().containsKey(indexMetaId)) {
                    metaChanged = true;
                    break;
                }
                if (copiedTable.getIndexMetaIdToMeta().get(indexMetaId).getSchemaHash() !=
                        entry.getValue().getSchemaHash()) {
                    metaChanged = true;
                    break;
                }
            }
        }

        if (olapTable.getDefaultDistributionInfo().getType() !=
                copiedTable.getDefaultDistributionInfo().getType()) {
            metaChanged = true;
        }

        if (metaChanged) {
            throw new DdlException("Table[" + tableName + "]'s meta has been changed. try again.");
        }
    }

    private void updatePartitionInfo(PartitionInfo partitionInfo, List<Pair<Partition, PartitionDesc>> partitionList,
                                     Set<String> existPartitionNameSet, boolean isTempPartition,
                                     OlapTable olapTable)
            throws DdlException {
        if (partitionInfo instanceof RangePartitionInfo) {
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
            rangePartitionInfo.handleNewRangePartitionDescs(olapTable.getIdToColumn(),
                    partitionList, existPartitionNameSet, isTempPartition);
        } else if (partitionInfo instanceof ListPartitionInfo) {
            ListPartitionInfo listPartitionInfo = (ListPartitionInfo) partitionInfo;
            listPartitionInfo.handleNewListPartitionDescs(olapTable.getIdToColumn(),
                    partitionList, existPartitionNameSet, isTempPartition);
        } else {
            throw new DdlException("Only support adding partition to range/list partitioned table");
        }

        if (isTempPartition) {
            for (Pair<Partition, PartitionDesc> entry : partitionList) {
                Partition partition = entry.first;
                if (!existPartitionNameSet.contains(partition.getName())) {
                    olapTable.addTempPartition(partition);
                }
            }
        } else {
            for (Pair<Partition, PartitionDesc> entry : partitionList) {
                Partition partition = entry.first;
                if (!existPartitionNameSet.contains(partition.getName())) {
                    olapTable.addPartition(partition);
                }
            }
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
                    isTempPartition,
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
                            isTempPartition,
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

    private void addPartitionLog(Database db, OlapTable olapTable, List<PartitionDesc> partitionDescs,
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

    private void addSubPartitionLog(Database db, OlapTable olapTable, Partition partition,
                                    List<PhysicalPartition> subPartitioins) throws DdlException {
        List<PhysicalPartitionPersistInfoV2> partitionInfoV2List = Lists.newArrayList();
        for (PhysicalPartition subPartition : subPartitioins) {
            PhysicalPartitionPersistInfoV2 info =
                    new PhysicalPartitionPersistInfoV2(db.getId(), olapTable.getId(), partition.getId(), subPartition);
            partitionInfoV2List.add(info);
        }

        AddSubPartitionsInfoV2 infos = new AddSubPartitionsInfoV2(partitionInfoV2List);
        GlobalStateMgr.getCurrentState().getEditLog().logAddSubPartitions(infos);

        for (PhysicalPartition subPartition : subPartitioins) {
            LOG.info("succeed in creating sub partitions[{}]", subPartition);
        }

    }

    private void cleanExistPartitionNameSet(Set<String> existPartitionNameSet,
                                            HashMap<String, Set<Long>> partitionNameToTabletSet) {
        for (String partitionName : existPartitionNameSet) {
            Set<Long> existPartitionTabletSet = partitionNameToTabletSet.get(partitionName);
            if (existPartitionTabletSet == null) {
                // should not happen
                continue;
            }
            for (Long tabletId : existPartitionTabletSet) {
                // createPartitionWithIndices create duplicate tablet that if not exists scenario
                // so here need to clean up those created tablets which partition already exists from invert index
                GlobalStateMgr.getCurrentState().getTabletInvertedIndex().deleteTablet(tabletId);
            }
        }
    }

    private void cleanTabletIdSetForAll(Set<Long> tabletIdSetForAll) {
        // Cleanup of shards for LakeTable is taken care by ShardDeleter
        for (Long tabletId : tabletIdSetForAll) {
            GlobalStateMgr.getCurrentState().getTabletInvertedIndex().deleteTablet(tabletId);
        }
    }

    private void checkPartitionNum(OlapTable olapTable) throws DdlException {
        if (olapTable.getNumberOfPartitions() > Config.max_partition_number_per_table) {
            throw new DdlException("Table " + olapTable.getName() + " created partitions exceeded the maximum limit: " +
                    Config.max_partition_number_per_table + ". You can modify this restriction on by setting" +
                    " max_partition_number_per_table larger.");
        }
    }

    private void addPartitions(ConnectContext ctx, Database db, String tableName, List<PartitionDesc> partitionDescs,
                               boolean isTempPartition, DistributionDesc distributionDesc) throws DdlException {
        DistributionInfo distributionInfo;
        OlapTable olapTable = checkTable(db, tableName);
        OlapTable copiedTable;

        Locker locker = new Locker();
        locker.lockTableWithIntensiveDbLock(db.getId(), olapTable.getId(), LockType.READ);
        Set<String> checkExistPartitionName = Sets.newConcurrentHashSet();
        try {

            // get partition info
            PartitionInfo partitionInfo = olapTable.getPartitionInfo();

            // check partition type
            checkPartitionType(partitionInfo);

            // check partition num
            checkPartitionNum(olapTable);

            // get distributionInfo
            distributionInfo = getDistributionInfo(olapTable, distributionDesc).copy();
            olapTable.inferDistribution(distributionInfo);

            // check colocation
            checkColocation(db, olapTable, distributionInfo, partitionDescs);
            copiedTable = AnalyzerUtils.getShadowCopyTable(olapTable);
            copiedTable.setDefaultDistributionInfo(distributionInfo);
            checkExistPartitionName = CatalogUtils.checkPartitionNameExistForAddPartitions(olapTable, partitionDescs);
        } finally {
            locker.unLockTableWithIntensiveDbLock(db.getId(), olapTable.getId(), LockType.READ);
        }

        Preconditions.checkNotNull(distributionInfo);
        Preconditions.checkNotNull(olapTable);
        Preconditions.checkNotNull(copiedTable);

        // create partition outside db lock
        checkDataProperty(partitionDescs);

        Set<Long> tabletIdSetForAll = Sets.newHashSet();
        HashMap<String, Set<Long>> partitionNameToTabletSet = Maps.newHashMap();
        try {
            // create partition list
            List<Pair<Partition, PartitionDesc>> newPartitions =
                    createPartitionMap(db, copiedTable, partitionDescs, partitionNameToTabletSet, tabletIdSetForAll,
                            checkExistPartitionName, ctx.getCurrentComputeResource());

            // build partitions
            List<Partition> partitionList = newPartitions.stream().map(x -> x.first).collect(Collectors.toList());
            buildPartitions(db, copiedTable, partitionList.stream().map(Partition::getSubPartitions)
                    .flatMap(p -> p.stream()).collect(Collectors.toList()), ctx.getCurrentComputeResource());

            // check again
            if (!locker.lockTableAndCheckDbExist(db, olapTable.getId(), LockType.WRITE)) {
                throw new DdlException("db " + db.getFullName()
                        + "(" + db.getId() + ") has been dropped");
            }
            Set<String> existPartitionNameSet = Sets.newHashSet();
            try {
                olapTable = checkTable(db, tableName);
                existPartitionNameSet = CatalogUtils.checkPartitionNameExistForAddPartitions(olapTable,
                        partitionDescs);
                if (existPartitionNameSet.size() > 0) {
                    for (String partitionName : existPartitionNameSet) {
                        LOG.info("add partition[{}] which already exists", partitionName);
                    }
                }

                // check if meta changed
                checkIfMetaChange(olapTable, copiedTable, tableName);

                // get partition info
                PartitionInfo partitionInfo = olapTable.getPartitionInfo();

                // check partition type
                checkPartitionType(partitionInfo);

                // update partition info
                updatePartitionInfo(partitionInfo, newPartitions, existPartitionNameSet, isTempPartition, olapTable);

                try {
                    colocateTableIndex.updateLakeTableColocationInfo(olapTable, true /* isJoin */,
                            null /* expectGroupId */);
                } catch (DdlException e) {
                    LOG.info("table {} update colocation info failed when add partition, {}", olapTable.getId(), e.getMessage());
                }

                // add partition log
                addPartitionLog(db, olapTable, partitionDescs, isTempPartition, partitionInfo, partitionList,
                        existPartitionNameSet);
            } finally {
                cleanExistPartitionNameSet(existPartitionNameSet, partitionNameToTabletSet);
                locker.unLockTableWithIntensiveDbLock(db.getId(), olapTable.getId(), LockType.WRITE);
            }
        } catch (DdlException e) {
            cleanTabletIdSetForAll(tabletIdSetForAll);
            throw e;
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
                        info.getDataCacheInfo());
            } else {
                throw new DdlException("Unsupported partition type: " + partitionType.name());
            }

            if (!isCheckpointThread()) {
                // add to inverted index
                TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
                PhysicalPartition physicalPartition = partition.getDefaultPhysicalPartition();
                for (MaterializedIndex index : physicalPartition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                    TabletMeta tabletMeta = new TabletMeta(info.getDbId(), info.getTableId(), physicalPartition.getId(),
                            index.getId(), info.getDataProperty().getStorageMedium());
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

    @Override
    public void dropPartition(Database db, Table table, DropPartitionClause clause) throws DdlException {
        CatalogUtils.checkTableExist(db, table.getName());
        Locker locker = new Locker();
        OlapTable olapTable = (OlapTable) table;
        Preconditions.checkArgument(locker.isDbWriteLockHeldByCurrentThread(db));
        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        boolean isTempPartition = clause.isTempPartition();
        boolean isDropAll = clause.isDropAll();
        long dbId = db.getId();
        long tableId = olapTable.getId();
        EditLog editLog = GlobalStateMgr.getCurrentState().getEditLog();

        if (olapTable.getState() != OlapTable.OlapTableState.NORMAL) {
            throw InvalidOlapTableStateException.of(olapTable.getState(), olapTable.getName());
        }

        if (isDropAll && isTempPartition) {
            DropPartitionsInfo info =
                    new DropPartitionsInfo(dbId, tableId, true, clause.isForceDrop(), null, true);
            editLog.logDropPartitions(info, wal -> olapTable.dropAllTempPartitions());
            LOG.info("succeed in dropping all partitions, db: {}, table: {}, is temp: {}, is force: {}",
                    db.getFullName(), olapTable.getName(), true, clause.isForceDrop());
            return;
        }

        if (!partitionInfo.isRangePartition() && partitionInfo.getType() != PartitionType.LIST) {
            throw new DdlException("Alter table [" + olapTable.getName() + "] failed. Not a partitioned table");
        }

        List<String> existPartitions = Lists.newArrayList();
        List<String> notExistPartitions = Lists.newArrayList();
        for (String partitionName : clause.getResolvedPartitionNames()) {
            if (olapTable.checkPartitionNameExist(partitionName, isTempPartition)) {
                existPartitions.add(partitionName);
            } else {
                notExistPartitions.add(partitionName);
            }
        }
        if (CollectionUtils.isNotEmpty(notExistPartitions)) {
            if (clause.isSetIfExists()) {
                LOG.info("drop partition[{}] which does not exist", notExistPartitions);
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_DROP_PARTITION_NON_EXISTENT, notExistPartitions);
            }
        }
        if (CollectionUtils.isEmpty(existPartitions)) {
            return;
        }

        // check committed txns
        for (String partitionName : existPartitions) {
            if (!isTempPartition) {
                Partition partition = olapTable.getPartition(partitionName);
                if (!clause.isForceDrop()) {
                    if (partition != null) {
                        if (stateMgr.getGlobalTransactionMgr()
                                .existCommittedTxns(db.getId(), olapTable.getId(), partition.getId())) {
                            throw new DdlException(
                                    "There are still some transactions in the COMMITTED state waiting to be completed." +
                                            " The partition [" + partitionName +
                                            "] cannot be dropped. If you want to forcibly drop(cannot be recovered)," +
                                            " please use \"DROP PARTITION <partition> FORCE\".");
                        }
                    }
                }
            }
        }

        DropPartitionsInfo info =
                new DropPartitionsInfo(dbId, tableId, isTempPartition, clause.isForceDrop(), existPartitions);
        editLog.logDropPartitions(info, wal -> {
            dropPartitionInternal(olapTable, db, existPartitions, isTempPartition, clause.isForceDrop());
        });

        if (!isTempPartition) {
            try {
                for (MvId mvId : olapTable.getRelatedMaterializedViews()) {
                    MaterializedView materializedView = (MaterializedView) getTable(mvId.getDbId(), mvId.getId());
                    if (materializedView != null && materializedView.isLoadTriggeredRefresh()) {
                        Database mvDb = getDb(mvId.getDbId());
                        GlobalStateMgr.getCurrentState().getLocalMetastore().refreshMaterializedView(
                                mvDb.getFullName(), materializedView.getName(), false, null,
                                Constants.TaskRunPriority.NORMAL.value(), true, false);
                    }
                }
            } catch (MetaNotFoundException e) {
                throw new DdlException("fail to refresh materialized views when dropping partition", e);
            }
        }
        LOG.info("succeed in dropping partitions[{}], db: {}, table: {}, is temp : {}, is force : {}", existPartitions,
                db.getFullName(), olapTable.getName(),
                isTempPartition,
                clause.isForceDrop());
    }

    protected void dropPartitionInternal(OlapTable olapTable,
                                       Database db,
                                       List<String> partitionNames,
                                       boolean isTempPartition,
                                       boolean isForceDrop) {
        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        for (String partitionName : partitionNames) {
            // drop
            if (isTempPartition) {
                olapTable.dropTempPartition(partitionName, true);
            } else {
                Range<PartitionKey> partitionRange = null;
                Partition partition = olapTable.getPartition(partitionName);
                if (partition != null) {
                    GlobalStateMgr.getCurrentState().getAnalyzeMgr().recordDropPartition(partition.getId());
                    if (partitionInfo instanceof RangePartitionInfo) {
                        partitionRange = ((RangePartitionInfo) partitionInfo).getRange(partition.getId());
                    }
                }
                olapTable.dropPartition(db.getId(), partitionName, isForceDrop);
                if (olapTable instanceof MaterializedView mv) {
                    try {
                        SyncPartitionUtils.dropBaseVersionMeta(mv, partitionName, partitionRange);
                    } catch (Exception e) {
                        LOG.warn("failed to drop base version meta for mv {}, partition {}",
                                mv.getName(), partitionName, e);
                    }
                }
            }
        }
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

    public void replayDropPartitions(DropPartitionsInfo info) {
        Database db = this.getDb(info.getDbId());
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            LOG.info("Begin to unprotect drop partitions. db = " + info.getDbId()
                    + " table = " + info.getTableId()
                    + " partitionNames = " + info.getPartitionNames()
                    + " isTempPartition = " + info.isTempPartition()
                    + " isForceDrop = " + info.isForceDrop()
                    + " isDropAll = " + info.isDropAll());
            List<String> partitionNames = info.getPartitionNames();
            OlapTable olapTable = (OlapTable) getTable(db.getId(), info.getTableId());
            boolean isTempPartition = info.isTempPartition();
            long dbId = info.getDbId();
            boolean isForceDrop = info.isForceDrop();
            boolean isDropAll = info.isDropAll();
            if (isDropAll && isTempPartition) {
                olapTable.dropAllTempPartitions();
                return;
            }
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

    public void replayErasePartition(long partitionId) throws DdlException {
        recycleBin.replayErasePartition(partitionId);
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

    private PhysicalPartition createPhysicalPartition(Database db, OlapTable olapTable,
                                                      Partition partition, ComputeResource computeResource) throws DdlException {
        long partitionId = partition.getId();
        DistributionInfo distributionInfo = olapTable.getDefaultDistributionInfo().copy();
        olapTable.inferDistribution(distributionInfo);
        // create sub partition
        Map<Long, MaterializedIndex> indexMap = new HashMap<>();
        // physical partitions in the same logical partition use the same shard_group_id,
        // so that the shards of this logical partition are more evenly distributed.
        long shardGroupId = partition.getDefaultPhysicalPartition().getBaseIndex().getShardGroupId();
        for (long indexMetaId : olapTable.getIndexMetaIdToMeta().keySet()) {
            // initially, index id and index meta id are the same
            MaterializedIndex rollup = new MaterializedIndex(indexMetaId, MaterializedIndex.IndexState.NORMAL, shardGroupId);
            indexMap.put(indexMetaId, rollup);
        }

        long id = GlobalStateMgr.getCurrentState().getNextId();
        PhysicalPartition physicalPartition = new PhysicalPartition(
                id, partition.generatePhysicalPartitionName(id),
                partition.getId(), indexMap.get(olapTable.getBaseIndexMetaId()));
        // set ShardGroupId to partition for rollback to old version
        physicalPartition.setShardGroupId(shardGroupId);
        physicalPartition.setBucketNum(distributionInfo.getBucketNum());

        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        short replicationNum = partitionInfo.getReplicationNum(partitionId);
        TStorageMedium storageMedium = partitionInfo.getDataProperty(partitionId).getStorageMedium();
        for (Map.Entry<Long, MaterializedIndex> entry : indexMap.entrySet()) {
            MaterializedIndex index = entry.getValue();
            Set<Long> tabletIdSet = new HashSet<>();

            // create tablets
            TabletMeta tabletMeta =
                    new TabletMeta(db.getId(), olapTable.getId(), id, index.getId(),
                            storageMedium, olapTable.isCloudNativeTableOrMaterializedView());

            if (olapTable.isCloudNativeTableOrMaterializedView()) {
                createLakeTablets(olapTable, id, index.getShardGroupId(), index, distributionInfo,
                        tabletMeta, tabletIdSet, computeResource);
            } else {
                createOlapTablets(olapTable, index, Replica.ReplicaState.NORMAL, distributionInfo,
                        physicalPartition.getVisibleVersion(), replicationNum, tabletMeta, tabletIdSet);
            }
            if (index.getMetaId() != olapTable.getBaseIndexMetaId()) {
                // add rollup index to partition
                physicalPartition.createRollupIndex(index);
            }
        }

        return physicalPartition;
    }

    public void addSubPartitions(Database db, OlapTable table, Partition partition,
                                 int numSubPartition, ComputeResource computeResource) throws DdlException {
        try {
            table.setAutomaticBucketing(true);

            OlapTable olapTable = checkTable(db, table.getId());
            OlapTable copiedTable;

            Locker locker = new Locker();
            locker.lockTableWithIntensiveDbLock(db.getId(), olapTable.getId(), LockType.READ);
            try {

                if (partition.getDistributionInfo().getType() != DistributionInfo.DistributionInfoType.RANDOM) {
                    throw new DdlException("Only support adding physical partition to random distributed table");
                }

                copiedTable = AnalyzerUtils.getShadowCopyTable(olapTable);
            } finally {
                locker.unLockTableWithIntensiveDbLock(db.getId(), olapTable.getId(), LockType.READ);
            }

            Preconditions.checkNotNull(olapTable);
            Preconditions.checkNotNull(copiedTable);

            List<PhysicalPartition> subPartitions = new ArrayList<>();
            // create physical partition
            for (int i = 0; i < numSubPartition; i++) {
                PhysicalPartition subPartition = createPhysicalPartition(db, copiedTable, partition, computeResource);
                subPartitions.add(subPartition);
            }

            // build partitions
            buildPartitions(db, copiedTable, subPartitions, computeResource);

            // check again
            if (!locker.lockTableAndCheckDbExist(db, olapTable.getId(), LockType.WRITE)) {
                throw new DdlException("db " + db.getFullName()
                        + "(" + db.getId() + ") has been dropped");
            }
            try {
                olapTable = checkTable(db, table.getId());
                // check if meta changed
                checkIfMetaChange(olapTable, copiedTable, table.getName());

                if (olapTable.getPartition(partition.getId()) == null) {
                    throw new DdlException("Partition[" + partition.getName() + "]' has been dropped.");
                }

                for (PhysicalPartition subPartition : subPartitions) {
                    // add sub partition
                    partition.addSubPartition(subPartition);
                    olapTable.addPhysicalPartition(subPartition);
                }

                olapTable.setShardGroupChanged(true);

                // add partition log
                addSubPartitionLog(db, olapTable, partition, subPartitions);
            } finally {
                locker.unLockTableWithIntensiveDbLock(db.getId(), olapTable.getId(), LockType.WRITE);
            }
        } finally {
            table.setAutomaticBucketing(false);
        }
    }

    public void replayAddSubPartition(PhysicalPartitionPersistInfoV2 info) throws DdlException {
        Database db = this.getDb(info.getDbId());
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            OlapTable olapTable = (OlapTable) getTable(db.getId(), info.getTableId());
            if (olapTable == null) {
                LOG.warn("replay add sub partition failed, table is null, info: {}", info);
                return;
            }
            Partition partition = olapTable.getPartition(info.getPartitionId());
            if (partition == null) {
                LOG.warn("replay add sub partition failed, partition is null, info: {}", info);
                return;
            }
            PhysicalPartition physicalPartition = info.getPhysicalPartition();
            partition.addSubPartition(physicalPartition);
            // the shardGrouId may invalid when upgrade from old version
            if (olapTable.isCloudNativeTable() &&
                    physicalPartition.getBaseIndex().getShardGroupId() ==
                            PhysicalPartition.INVALID_SHARD_GROUP_ID) {
                physicalPartition.getBaseIndex().setShardGroupId(physicalPartition.getShardGroupId());
            }
            olapTable.addPhysicalPartition(physicalPartition);

            if (!isCheckpointThread()) {
                // add to inverted index
                TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
                for (MaterializedIndex index : physicalPartition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                    TabletMeta tabletMeta = new TabletMeta(info.getDbId(), info.getTableId(),
                            physicalPartition.getId(), index.getId(), olapTable.getPartitionInfo().getDataProperty(
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

    Partition createPartition(Database db, OlapTable table, long partitionId, String partitionName,
                              Long version, Set<Long> tabletIdSet, ComputeResource computeResource) throws DdlException {
        DistributionInfo distributionInfo = table.getDefaultDistributionInfo().copy();
        table.inferDistribution(distributionInfo);

        return createPartition(db, table, partitionId, partitionName, version, tabletIdSet, distributionInfo, computeResource);
    }

    Partition createPartition(Database db, OlapTable table, long partitionId, String partitionName,
                              Long version, Set<Long> tabletIdSet, DistributionInfo distributionInfo,
                              ComputeResource computeResource) throws DdlException {
        PartitionInfo partitionInfo = table.getPartitionInfo();
        Map<Long, MaterializedIndex> indexMap = new HashMap<>();
        for (long indexMetaId : table.getIndexMetaIdToMeta().keySet()) {
            long shardGroupId = PhysicalPartition.INVALID_SHARD_GROUP_ID;
            if (table.isCloudNativeTableOrMaterializedView()) {
                // create shard group
                shardGroupId = GlobalStateMgr.getCurrentState().getStarOSAgent().
                        createShardGroup(db.getId(), table.getId(), partitionId, indexMetaId);
            }
            // initially, index id and index meta id are the same
            MaterializedIndex rollup = new MaterializedIndex(indexMetaId, MaterializedIndex.IndexState.NORMAL, shardGroupId);
            indexMap.put(indexMetaId, rollup);
        }

        Partition logicalPartition = new Partition(
                partitionId,
                partitionName,
                distributionInfo);

        long physicalPartitionId = GlobalStateMgr.getCurrentState().getNextId();
        PhysicalPartition physicalPartition = new PhysicalPartition(
                physicalPartitionId,
                logicalPartition.generatePhysicalPartitionName(physicalPartitionId),
                partitionId,
                indexMap.get(table.getBaseIndexMetaId()));
        physicalPartition.setBucketNum(distributionInfo.getBucketNum());

        logicalPartition.addSubPartition(physicalPartition);

        // version
        if (version != null) {
            physicalPartition.updateVisibleVersion(version);
        }

        short replicationNum = partitionInfo.getReplicationNum(partitionId);
        TStorageMedium storageMedium = partitionInfo.getDataProperty(partitionId).getStorageMedium();
        for (Map.Entry<Long, MaterializedIndex> entry : indexMap.entrySet()) {
            MaterializedIndex index = entry.getValue();

            // create tablets
            TabletMeta tabletMeta =
                    new TabletMeta(db.getId(), table.getId(), physicalPartitionId, index.getId(),
                            storageMedium, table.isCloudNativeTableOrMaterializedView());

            if (table.isCloudNativeTableOrMaterializedView()) {
                createLakeTablets(table, physicalPartitionId, index.getShardGroupId(), index, distributionInfo,
                        tabletMeta, tabletIdSet, computeResource);
            } else {
                createOlapTablets(table, index, Replica.ReplicaState.NORMAL, distributionInfo,
                        physicalPartition.getVisibleVersion(), replicationNum, tabletMeta, tabletIdSet);
            }
            if (index.getMetaId() != table.getBaseIndexMetaId()) {
                // add rollup index to partition
                physicalPartition.createRollupIndex(index);
            } else {
                // base index set ShardGroupId for rollback to old version
                physicalPartition.setShardGroupId(index.getShardGroupId());
            }
        }

        return logicalPartition;
    }

    void buildPartitions(Database db, OlapTable table, List<PhysicalPartition> partitions,
                         ComputeResource computeResource) throws DdlException {
        if (partitions.isEmpty()) {
            return;
        }
        int numAliveNodes = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getAliveBackendNumber();

        if (RunMode.isSharedDataMode()) {
            numAliveNodes = 0;
            final WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
            final List<Long> computeNodeIds = warehouseManager.getAllComputeNodeIds(computeResource);
            for (long nodeId : computeNodeIds) {
                if (GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendOrComputeNode(nodeId).isAlive()) {
                    ++numAliveNodes;
                }
            }
        }
        if (numAliveNodes == 0) {
            if (RunMode.isSharedDataMode()) {
                throw new DdlException("no alive compute nodes");
            } else {
                throw new DdlException("no alive backends");
            }
        }

        int numReplicas = 0;
        for (PhysicalPartition partition : partitions) {
            numReplicas += partition.storageReplicaCount();
        }

        TabletTaskExecutor.CreateTabletOption option = new TabletTaskExecutor.CreateTabletOption();
        // Enable `tablet_creation_optimization` creates only one shared tablet metadata for all tablets under a partition. 
        // Enable `file_bundling` reuses the optimization logic.
        // These two configure only use in shared-data mode
        option.setEnableTabletCreationOptimization(table.isCloudNativeTableOrMaterializedView()
                && (Config.lake_enable_tablet_creation_optimization || table.isFileBundling()));
        option.setGtid(GlobalStateMgr.getCurrentState().getGtidGenerator().nextGtid());

        try {
            GlobalStateMgr.getCurrentState().getConsistencyChecker().addCreatingTableId(table.getId());
            if (numReplicas > Config.create_table_max_serial_replicas) {
                LOG.info("start to build {} partitions concurrently for table {}.{} with {} replicas, resource:{}",
                        partitions.size(), db.getFullName(), table.getName(), numReplicas, computeResource);
                TabletTaskExecutor.buildPartitionsConcurrently(
                        db.getId(), table, partitions, numReplicas, numAliveNodes, computeResource, option);
            } else {
                LOG.info("start to build {} partitions sequentially for table {}.{} with {} replicas, resource:{}",
                        partitions.size(), db.getFullName(), table.getName(), numReplicas, computeResource);
                TabletTaskExecutor.buildPartitionsSequentially(
                        db.getId(), table, partitions, numReplicas, numAliveNodes, computeResource, option);
            }
        } finally {
            GlobalStateMgr.getCurrentState().getConsistencyChecker().deleteCreatingTableId(table.getId());
        }
    }

    /*
     * generate and check columns' order and key's existence
     */
    void validateColumns(List<Column> columns) throws DdlException {
        if (columns.isEmpty()) {
            ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_MUST_HAVE_COLUMNS);
        }

        boolean encounterValue = false;
        boolean hasKey = false;
        for (Column column : columns) {
            if (column.isKey()) {
                if (encounterValue) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_OLAP_KEY_MUST_BEFORE_VALUE);
                }
                hasKey = true;
            } else {
                encounterValue = true;
            }
        }

        if (!hasKey) {
            ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_MUST_HAVE_KEYS);
        }
    }

    // only for test
    public void setColocateTableIndex(ColocateTableIndex colocateTableIndex) {
        this.colocateTableIndex = colocateTableIndex;
    }

    public ColocateTableIndex getColocateTableIndex() {
        return colocateTableIndex;
    }

    public void setLakeStorageInfo(Database db, OlapTable table, String storageVolumeId, Map<String, String> properties)
            throws DdlException {
        DataCacheInfo dataCacheInfo = null;
        try {
            dataCacheInfo = PropertyAnalyzer.analyzeDataCacheInfo(properties);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }

        // get service shard storage info from StarMgr
        FilePathInfo pathInfo = !storageVolumeId.isEmpty() ?
                stateMgr.getStarOSAgent().allocateFilePath(storageVolumeId, db.getId(), table.getId()) :
                stateMgr.getStarOSAgent().allocateFilePath(db.getId(), table.getId());
        table.setStorageInfo(pathInfo, dataCacheInfo);
    }

    public void onCreate(Database db, Table table, String storageVolumeId, boolean isSetIfNotExists)
            throws DdlException {
        // check database exists again, because database can be dropped when creating table
        if (!tryLock(false)) {
            throw new DdlException("Failed to acquire globalStateMgr lock. " +
                    "Try again or increasing value of `catalog_try_lock_timeout_ms` configuration.");
        }

        try {
            /*
             * When creating table or mv, we need to create the tablets and prepare some of the
             * metadata first before putting this new table or mv in the database. So after the
             * first step, we need to acquire the global lock and double check whether the db still
             * exists because it maybe dropped by other concurrent client. And if we don't use the lock
             * protection and handle the concurrency properly, the replay of table/mv creation may fail
             * on restart or on follower.
             *
             * After acquire the db lock, we also need to acquire the db lock and write edit log. Since the
             * db lock maybe under high contention and IO is busy, current thread can hold the global lock
             * for quite a long time and make the other operation waiting for the global lock fail.
             *
             * So here after the confirmation of existence of modifying database, we release the global lock
             * When dropping database, we will set the `exist` field of db object to false. And in the following
             * creation process, we will double-check the `exist` field.
             */
            if (getDb(db.getId()) == null) {
                throw new DdlException("Database has been dropped when creating table/mv/view");
            }
        } finally {
            unlock();
        }

        if (db.isSystemDatabase()) {
            ErrorReport.reportDdlException(ErrorCode.ERR_CANT_CREATE_TABLE, table.getName(),
                    "cannot create table in system database");
        }
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            if (!db.isExist()) {
                throw new DdlException("Database has been dropped when creating table/mv/view");
            }

            if (!db.registerTableUnlocked(table)) {
                if (!isSetIfNotExists) {
                    table.delete(db.getId(), false);
                    ErrorReport.reportDdlException(ErrorCode.ERR_CANT_CREATE_TABLE, table.getName(),
                            "table already exists");
                } else {
                    LOG.info("Create table[{}] which already exists", table.getName());
                    return;
                }
            }

            // NOTE: The table has been added to the database, and the following procedure cannot throw exception.
            LOG.info("Successfully create table: {}-{}, in database: {}-{}",
                    table.getName(), table.getId(), db.getFullName(), db.getId());

            CreateTableInfo createTableInfo = new CreateTableInfo(db.getFullName(), table, storageVolumeId);
            GlobalStateMgr.getCurrentState().getEditLog().logCreateTable(createTableInfo);
            table.onCreate(db);
        } catch (SerializeException e) {
            db.unRegisterTableUnlocked(table);
            LOG.warn("create table failed", e);
            ErrorReport.reportDdlException(ErrorCode.ERR_CANT_CREATE_TABLE, table.getName(), e.getMessage());
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
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
                        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, physicalPartitionId,
                                mIndex.getId(), medium, table.isCloudNativeTableOrMaterializedView());
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

    private void createLakeTablets(OlapTable table, long physicalPartitionId, long shardGroupId, MaterializedIndex index,
                                   DistributionInfo distributionInfo, TabletMeta tabletMeta,
                                   Set<Long> tabletIdSet, ComputeResource computeResource)
            throws DdlException {
        Preconditions.checkArgument(table.isCloudNativeTableOrMaterializedView());

        DistributionInfo.DistributionInfoType distributionInfoType = distributionInfo.getType();
        if (distributionInfoType != DistributionInfo.DistributionInfoType.HASH
                && distributionInfoType != DistributionInfo.DistributionInfoType.RANDOM
                && distributionInfoType != DistributionInfo.DistributionInfoType.RANGE) {
            throw new DdlException("Unknown distribution type: " + distributionInfoType);
        }

        Map<String, String> properties = new HashMap<>();
        properties.put(LakeTablet.PROPERTY_KEY_TABLE_ID, Long.toString(table.getId()));
        properties.put(LakeTablet.PROPERTY_KEY_PARTITION_ID, Long.toString(physicalPartitionId));
        properties.put(LakeTablet.PROPERTY_KEY_INDEX_ID, Long.toString(index.getId()));
        int bucketNum = distributionInfo.getBucketNum();
        final long warehouseId = computeResource.getWarehouseId();
        final WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        if (!warehouseManager.isResourceAvailable(computeResource)) {
            Warehouse warehouse = warehouseManager.getWarehouse(warehouseId);
            throw ErrorReportException.report(ErrorCode.ERR_NO_NODES_IN_WAREHOUSE, warehouse.getName());
        }
        List<Long> shardIds = stateMgr.getStarOSAgent().createShards(bucketNum,
                table.getPartitionFilePathInfo(physicalPartitionId),
                table.getPartitionFileCacheInfo(physicalPartitionId),
                shardGroupId,
                null, properties, computeResource);
        for (long shardId : shardIds) {
            Tablet tablet = new LakeTablet(shardId);
            index.addTablet(tablet, tabletMeta);
            tabletIdSet.add(tablet.getId());
        }
    }

    private void createOlapTablets(OlapTable table, MaterializedIndex index, Replica.ReplicaState replicaState,
                                   DistributionInfo distributionInfo, long version, short replicationNum,
                                   TabletMeta tabletMeta, Set<Long> tabletIdSet) throws DdlException {
        Preconditions.checkArgument(replicationNum > 0);

        DistributionInfo.DistributionInfoType distributionInfoType = distributionInfo.getType();
        if (distributionInfoType != DistributionInfo.DistributionInfoType.HASH
                && distributionInfoType != DistributionInfo.DistributionInfoType.RANDOM
                && distributionInfoType != DistributionInfo.DistributionInfoType.RANGE) {
            throw new DdlException("Unknown distribution type: " + distributionInfoType);
        }

        List<List<Long>> backendsPerBucketSeq = null;
        ColocateTableIndex.GroupId groupId = null;
        boolean initBucketSeqWithSameOrigNameGroup = false;
        boolean isColocateTable = colocateTableIndex.isColocateTable(tabletMeta.getTableId());
        // chooseBackendsArbitrary is true, means this may be the first table of colocation group,
        // or this is just a normal table, and we can choose backends arbitrary.
        // otherwise, backends should be chosen from backendsPerBucketSeq;
        boolean chooseBackendsArbitrary;

        // We should synchronize the creation of colocate tables, otherwise it can have concurrent issues.
        // Considering the following situation,
        // T1: P1 issues `create colocate table` and finds that there isn't a bucket sequence associated
        //     with the colocate group, so it will initialize the bucket sequence for the first time
        // T2: P2 do the same thing as P1
        // T3: P1 set the bucket sequence for colocate group stored in `ColocateTableIndex`
        // T4: P2 also set the bucket sequence, hence overwrite what P1 just wrote
        // T5: After P1 creates the colocate table, the actual tablet distribution won't match the bucket sequence
        //     of the colocate group, and balancer will create a lot of COLOCATE_MISMATCH tasks which shouldn't exist.
        if (isColocateTable) {
            try {
                // Optimization: wait first, before global lock
                colocateTableCreateSyncer.awaitZero();
                // Since we have supported colocate tables in different databases,
                // we should use global lock, not db lock.
                tryLock(false);
                try {
                    // Wait again, for safety
                    // We are in global lock, we should have timeout in case holding lock for too long
                    colocateTableCreateSyncer.awaitZero(Config.catalog_try_lock_timeout_ms, TimeUnit.MILLISECONDS);
                    // if this is a colocate table, try to get backend seqs from colocation index.
                    groupId = colocateTableIndex.getGroup(tabletMeta.getTableId());
                    backendsPerBucketSeq = colocateTableIndex.getBackendsPerBucketSeq(groupId);
                    if (backendsPerBucketSeq.isEmpty()) {
                        List<ColocateTableIndex.GroupId> colocateWithGroupsInOtherDb =
                                colocateTableIndex.getColocateWithGroupsInOtherDb(groupId);
                        if (!colocateWithGroupsInOtherDb.isEmpty()) {
                            backendsPerBucketSeq =
                                    colocateTableIndex.getBackendsPerBucketSeq(colocateWithGroupsInOtherDb.get(0));
                            initBucketSeqWithSameOrigNameGroup = true;
                        }
                    }
                    chooseBackendsArbitrary = backendsPerBucketSeq == null || backendsPerBucketSeq.isEmpty();
                    if (chooseBackendsArbitrary) {
                        colocateTableCreateSyncer.increment();
                    }
                } finally {
                    unlock();
                }
            } catch (InterruptedException e) {
                LOG.warn("wait for concurrent colocate table creation finish failed, msg: {}",
                        e.getMessage(), e);
                Thread.currentThread().interrupt();
                throw new DdlException("wait for concurrent colocate table creation finish failed", e);
            }
        } else {
            chooseBackendsArbitrary = true;
        }

        try {
            if (chooseBackendsArbitrary) {
                backendsPerBucketSeq = Lists.newArrayList();
            }
            for (int i = 0; i < distributionInfo.getBucketNum(); ++i) {
                // create a new tablet with random chosen backends
                LocalTablet tablet = new LocalTablet(getNextId());

                // add tablet to inverted index first
                index.addTablet(tablet, tabletMeta);
                tabletIdSet.add(tablet.getId());

                // get BackendIds
                List<Long> chosenBackendIds;
                if (chooseBackendsArbitrary) {
                    // This is the first colocate table in the group, or just a normal table,
                    // randomly choose backends
                    if (Config.enable_strict_storage_medium_check) {
                        chosenBackendIds =
                                chosenBackendIdBySeq(replicationNum, table.getLocation(), tabletMeta.getStorageMedium());
                    } else {
                        try {
                            chosenBackendIds = chosenBackendIdBySeq(replicationNum, table.getLocation());
                        } catch (DdlException ex) {
                            throw new DdlException(String.format(
                                    "%s, table=%s, replication_num=%d, default_replication_num=%d",
                                    ex.getMessage(), table.getName(), replicationNum, Config.default_replication_num));
                        }
                    }
                    backendsPerBucketSeq.add(chosenBackendIds);
                } else {
                    // get backends from existing backend sequence
                    chosenBackendIds = backendsPerBucketSeq.get(i);
                }

                // create replicas
                int schemaHash = table.getSchemaHashByIndexMetaId(index.getMetaId());
                for (long backendId : chosenBackendIds) {
                    long replicaId = getNextId();
                    Replica replica = new Replica(replicaId, backendId, replicaState, version, schemaHash);
                    tablet.addReplica(replica);
                }
                Preconditions.checkState(chosenBackendIds.size() == replicationNum,
                        chosenBackendIds.size() + " vs. " + replicationNum);
            }

            // In the following two situations, we should set the bucket seq for colocate group and persist the info,
            //   1. This is the first time we add a table to colocate group, and it doesn't have the same original name
            //      with colocate group in other database.
            //   2. It's indeed the first time, but it should colocate with group in other db
            //      (because of having the same original name), we should use the bucket
            //      seq of other group to initialize our own.
            if ((groupId != null && chooseBackendsArbitrary) || initBucketSeqWithSameOrigNameGroup) {
                colocateTableIndex.addBackendsPerBucketSeq(groupId, backendsPerBucketSeq);
                ColocatePersistInfo info =
                        ColocatePersistInfo.createForBackendsPerBucketSeq(groupId, backendsPerBucketSeq);
                GlobalStateMgr.getCurrentState().getEditLog().logColocateBackendsPerBucketSeq(info);
            }
        } finally {
            if (isColocateTable && chooseBackendsArbitrary) {
                colocateTableCreateSyncer.decrement();
            }
        }
    }

    // create replicas for tablet with random chosen backends
    private List<Long> chosenBackendIdBySeq(int replicationNum, Multimap<String, String> locReq,
                                            TStorageMedium storageMedium)
            throws DdlException {
        List<Long> chosenBackendIds =
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getNodeSelector()
                        .seqChooseBackendIdsByStorageMedium(replicationNum,
                                true, true, locReq, storageMedium);
        if (CollectionUtils.isEmpty(chosenBackendIds)) {
            throw new DdlException(
                    "Failed to find enough hosts with storage medium " + storageMedium +
                            " at all backends, number of replicas needed: " +
                            replicationNum + ". Storage medium check failure can be forcefully ignored by executing " +
                            "'ADMIN SET FRONTEND CONFIG (\"enable_strict_storage_medium_check\" = \"false\");', " +
                            "but incompatible medium type can cause balance problem, so we strongly recommend" +
                            " creating table with compatible 'storage_medium' property set.");
        }
        return chosenBackendIds;
    }

    private List<Long> chosenBackendIdBySeq(int replicationNum, Multimap<String, String> locReq) throws DdlException {
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        List<Long> chosenBackendIds = systemInfoService.getNodeSelector()
                .seqChooseBackendIds(replicationNum, true, true, locReq);
        if (CollectionUtils.isEmpty(chosenBackendIds)) {
            StringBuffer sb = new StringBuffer();
            List<Backend> availableBes = systemInfoService.getAvailableBackends();
            List<Long> availableBeIds = availableBes.stream().filter(b -> !b.checkDiskExceedLimitForCreate()).map(Backend::getId)
                    .collect(Collectors.toList());
            sb.append(String.format("Table replication num should be less than or equal to the number of available backends. "
                    + "You can change this default by setting the replication_num table properties. "
                    + "Current available backends: [%s]", Joiner.on(",").join(availableBeIds)));

            List<Long> decommissionedBeIds = systemInfoService.getDecommissionedBackendIds();
            if (!decommissionedBeIds.isEmpty()) {
                sb.append(String.format(", decommissioned backends: [%s]", Joiner.on(",").join(decommissionedBeIds)));
            }

            List<Long> noDiskSpaceBeIds = availableBes.stream().filter(b -> b.checkDiskExceedLimitForCreate()).map(Backend::getId)
                    .collect(Collectors.toList());
            if (!noDiskSpaceBeIds.isEmpty()) {
                sb.append(String.format(", backends without enough disk space: [%s]", Joiner.on(",").join(noDiskSpaceBeIds)));
            }

            throw new DdlException(sb.toString());
        }
        return chosenBackendIds;
    }

    @Override
    public void dropTable(ConnectContext context, DropTableStmt stmt) throws DdlException {
        dropTable(stmt);
    }

    // Drop table
    public void dropTable(DropTableStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();
        String tableName = stmt.getTableName();

        // check database
        Database db = getDb(dbName);
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }
        if (db.isSystemDatabase()) {
            ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                    "cannot drop table in system database: " + db.getOriginName());
        }
        db.dropTable(tableName, stmt.isSetIfExists(), stmt.isForceDrop());
    }

    public void dropTemporaryTable(String dbName, long tableId, String tableName,
                                   boolean isSetIfExsists, boolean isForce) throws DdlException {
        Database db = getDb(dbName);
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }
        db.dropTemporaryTable(tableId, tableName, isSetIfExsists, isForce);
    }

    public void replayDropTable(Database db, long tableId, boolean isForceDrop) {
        Table table;
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            table = getTable(db.getId(), tableId);
            if (table.isTemporaryTable()) {
                table = db.unprotectDropTemporaryTable(tableId, isForceDrop, true);
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

    public void replayEraseTable(long tableId) {
        recycleBin.replayEraseTable(Collections.singletonList(tableId));
    }

    public void replayEraseMultiTables(MultiEraseTableInfo multiEraseTableInfo) {
        List<Long> tableIds = multiEraseTableInfo.getTableIds();
        recycleBin.replayEraseTable(tableIds);
    }

    public void replayDisableTableRecovery(DisableTableRecoveryInfo disableTableRecoveryInfo) {
        recycleBin.replayDisableTableRecovery(disableTableRecoveryInfo.getTableIds());
    }

    public void replayDisablePartitionRecovery(DisablePartitionRecoveryInfo disablePartitionRecoveryInfo) {
        recycleBin.replayDisablePartitionRecovery(disablePartitionRecoveryInfo.getPartitionId());
    }

    public void replayRecoverTable(RecoverInfo info) {
        long dbId = info.getDbId();
        Database db = getDb(dbId);
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            recycleBin.replayRecoverTable(db, info.getTableId());
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
    }

    public void replayAddReplica(ReplicaPersistInfo info) {
        Database db = getDbIncludeRecycleBin(info.getDbId());
        if (db == null) {
            LOG.warn("replay add replica failed, db is null, info: {}", info);
            return;
        }
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            OlapTable olapTable = (OlapTable) getTableIncludeRecycleBin(db, info.getTableId());
            if (olapTable == null) {
                LOG.warn("replay add replica failed, table is null, info: {}", info);
                return;
            }
            PhysicalPartition partition = getPhysicalPartitionIncludeRecycleBin(olapTable, info.getPartitionId());
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
                schemaHash = olapTable.getSchemaHashByIndexMetaId(materializedIndex.getMetaId());
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

    public void replayUpdateReplica(ReplicaPersistInfo info) {
        Database db = getDbIncludeRecycleBin(info.getDbId());
        if (db == null) {
            LOG.warn("replay update replica failed, db is null, info: {}", info);
            return;
        }
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            OlapTable olapTable = (OlapTable) getTableIncludeRecycleBin(db, info.getTableId());
            if (olapTable == null) {
                LOG.warn("replay update replica failed, table is null, info: {}", info);
                return;
            }
            PhysicalPartition partition = getPhysicalPartitionIncludeRecycleBin(olapTable, info.getPartitionId());
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

    public void replayDeleteReplica(ReplicaPersistInfo info) {
        Database db = getDbIncludeRecycleBin(info.getDbId());
        if (db == null) {
            LOG.warn("replay delete replica failed, db is null, info: {}", info);
            return;
        }
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            OlapTable olapTable = (OlapTable) getTableIncludeRecycleBin(db, info.getTableId());
            if (olapTable == null) {
                LOG.warn("replay delete replica failed, table is null, info: {}", info);
                return;
            }
            PhysicalPartition partition = getPhysicalPartitionIncludeRecycleBin(olapTable, info.getPartitionId());
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
    public Database getDb(ConnectContext context, String name) {
        return getDb(name);
    }

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

    public List<Database> getAllDbs() {
        return Lists.newArrayList(idToDb.values());
    }

    public Optional<Database> mayGetDb(String name) {
        return Optional.ofNullable(getDb(name));
    }

    public Optional<Database> mayGetDb(long dbId) {
        return Optional.ofNullable(getDb(dbId));
    }

    public Optional<Table> mayGetTable(long dbId, long tableId) {
        return mayGetDb(dbId).flatMap(db -> Optional.ofNullable(db.getTable(tableId)));
    }

    public Optional<Table> mayGetTable(String dbName, String tableName) {
        return mayGetDb(dbName).flatMap(db -> Optional.ofNullable(db.getTable(tableName)));
    }

    public ConcurrentHashMap<String, Database> getFullNameToDb() {
        return fullNameToDb;
    }

    public Database getDbIncludeRecycleBin(long dbId) {
        Database db = idToDb.get(dbId);
        if (db == null) {
            db = recycleBin.getDatabase(dbId);
        }
        return db;
    }

    @Override
    public boolean tableExists(ConnectContext context, String dbName, String tblName) {
        Database database = getDb(context, dbName);
        if (database == null) {
            return false;
        }
        return database.getTable(tblName) != null;
    }

    @Override
    public Table getTable(ConnectContext context, String dbName, String tblName) {
        return getTable(dbName, tblName);
    }

    public Table getTable(String dbName, String tblName) {
        Database database = getDb(dbName);
        if (database == null) {
            return null;
        }
        return database.getTable(tblName);
    }

    /**
     * @param mvId mv's mvid
     * @return a checked mv by input mvid.
     */
    public MaterializedView getMaterializedView(MvId mvId) {
        long dbId = mvId.getDbId();
        long tableId = mvId.getId();
        Table table = getTable(dbId, tableId);
        if (table == null || !(table instanceof MaterializedView)) {
            return null;
        }
        return (MaterializedView) table;
    }

    public Table getTable(Long dbId, Long tableId) {
        Database database = getDb(dbId);
        if (database == null) {
            return null;
        }
        return database.getTable(tableId);
    }

    public List<Table> getTables(Long dbId) {
        Database database = getDb(dbId);
        if (database == null) {
            return Collections.emptyList();
        } else {
            return database.getTables();
        }
    }

    /**
     * Get Table descriptor and materialized index for the materialized view index specific by `dbName`.`tblName`
     *
     * @param dbName  - the string represents the database name
     * @param tblName - the string represents the table name
     * @return a Table instance
     */
    public Pair<Table, MaterializedIndexMeta> getMaterializedViewIndex(String dbName, String tblName) {
        Database database = getDb(dbName);
        if (database == null) {
            return null;
        }
        return database.getMaterializedViewIndex(tblName);
    }

    public Table getTableIncludeRecycleBin(Database db, long tableId) {
        Table table = getTable(db.getId(), tableId);
        if (table == null) {
            table = recycleBin.getTable(db.getId(), tableId);
        }
        return table;
    }

    public List<Table> getTablesIncludeRecycleBin(Database db) {
        List<Table> tables = db.getTables();
        tables.addAll(recycleBin.getTables(db.getId()));
        return tables;
    }

    public Partition getPartitionIncludeRecycleBin(OlapTable table, long partitionId) {
        Partition partition = table.getPartition(partitionId);
        if (partition == null) {
            partition = recycleBin.getPartition(partitionId);
        }
        return partition;
    }

    public PhysicalPartition getPhysicalPartitionIncludeRecycleBin(OlapTable table, long physicalPartitionId) {
        PhysicalPartition partition = table.getPhysicalPartition(physicalPartitionId);
        if (partition == null) {
            partition = recycleBin.getPhysicalPartition(physicalPartitionId);
        }
        return partition;
    }

    public Collection<Partition> getPartitionsIncludeRecycleBin(OlapTable table) {
        Collection<Partition> partitions = new ArrayList<>(table.getPartitions());
        partitions.addAll(recycleBin.getPartitions(table.getId()));
        return partitions;
    }

    public Collection<Partition> getAllPartitionsIncludeRecycleBin(OlapTable table) {
        Collection<Partition> partitions = table.getAllPartitions();
        partitions.addAll(recycleBin.getPartitions(table.getId()));
        return partitions;
    }

    public Collection<PhysicalPartition> getAllPhysicalPartitionsIncludeRecycleBin(OlapTable table) {
        Collection<PhysicalPartition> partitions = table.getAllPhysicalPartitions();
        partitions.addAll(recycleBin.getPhysicalPartitions(table.getId()));
        return partitions;
    }

    // NOTE: result can be null, cause partition erase is not in db lock
    public DataProperty getDataPropertyIncludeRecycleBin(PartitionInfo info, long partitionId) {
        DataProperty dataProperty = info.getDataProperty(partitionId);
        if (dataProperty == null) {
            dataProperty = recycleBin.getPartitionDataProperty(partitionId);
        }
        return dataProperty;
    }

    // NOTE: result can be -1, cause partition erase is not in db lock
    public short getReplicationNumIncludeRecycleBin(PartitionInfo info, long partitionId) {
        short replicaNum = info.getReplicationNum(partitionId);
        if (replicaNum == (short) -1) {
            replicaNum = recycleBin.getPartitionReplicationNum(partitionId);
        }
        return replicaNum;
    }

    @Override
    public List<String> listDbNames(ConnectContext context) {
        return Lists.newArrayList(fullNameToDb.keySet());
    }

    @Override
    public List<String> listTableNames(ConnectContext context, String dbName) {
        Database database = getDb(dbName);
        if (database != null) {
            return database.getTables().stream()
                    .map(Table::getName).collect(Collectors.toList());
        } else {
            throw new StarRocksConnectorException("Database " + dbName + " doesn't exist");
        }
    }

    @Override
    public List<Long> getDbIds() {
        return Lists.newArrayList(idToDb.keySet());
    }

    public List<Long> getDbIdsIncludeRecycleBin() {
        List<Long> dbIds = getDbIds();
        dbIds.addAll(recycleBin.getAllDbIds());
        return dbIds;
    }

    public HashMap<Long, TStorageMedium> getPartitionIdToStorageMediumMap() {
        HashMap<Long, TStorageMedium> storageMediumMap = new HashMap<>();

        // record partition which need to change storage medium
        // dbId -> (tableId -> partitionId)
        HashMap<Long, Multimap<Long, Long>> changedPartitionsMap = new HashMap<>();
        long currentTimeMs = System.currentTimeMillis();
        List<Long> dbIds = getDbIds();

        for (long dbId : dbIds) {
            Database db = getDb(dbId);
            if (db == null) {
                LOG.warn("db {} does not exist while doing backend report", dbId);
                continue;
            }

            Locker locker = new Locker();
            locker.lockDatabase(db.getId(), LockType.READ);
            try {
                for (Table table : db.getTables()) {
                    if (!table.isOlapTableOrMaterializedView()) {
                        continue;
                    }

                    long tableId = table.getId();
                    OlapTable olapTable = (OlapTable) table;
                    PartitionInfo partitionInfo = olapTable.getPartitionInfo();
                    for (Partition partition : olapTable.getAllPartitions()) {
                        long partitionId = partition.getId();
                        DataProperty dataProperty = partitionInfo.getDataProperty(partition.getId());
                        Preconditions.checkNotNull(dataProperty,
                                partition.getName() + ", pId:" + partitionId + ", db: " + dbId + ", tbl: " + tableId);
                        // only normal state table can migrate.
                        // PRIMARY_KEYS table does not support local migration.
                        if (dataProperty.getStorageMedium() == TStorageMedium.SSD
                                && dataProperty.getCooldownTimeMs() < currentTimeMs
                                && olapTable.getState() == OlapTable.OlapTableState.NORMAL) {
                            // expire. change to HDD.
                            // record and change when holding write lock
                            Multimap<Long, Long> multimap = changedPartitionsMap.get(dbId);
                            if (multimap == null) {
                                multimap = HashMultimap.create();
                                changedPartitionsMap.put(dbId, multimap);
                            }
                            multimap.put(tableId, partitionId);
                        } else {
                            storageMediumMap.put(partitionId, dataProperty.getStorageMedium());
                        }
                    } // end for partitions
                } // end for tables
            } finally {
                locker.unLockDatabase(db.getId(), LockType.READ);
            }
        } // end for dbs

        // handle data property changed
        for (Long dbId : changedPartitionsMap.keySet()) {
            Database db = getDb(dbId);
            if (db == null) {
                LOG.warn("db {} does not exist while checking backend storage medium", dbId);
                continue;
            }
            Multimap<Long, Long> tableIdToPartitionIds = changedPartitionsMap.get(dbId);

            // use try lock to avoid blocking a long time.
            // if block too long, backend report rpc will timeout.
            Locker locker = new Locker();
            if (!locker.tryLockDatabase(db.getId(), LockType.WRITE, Database.TRY_LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                LOG.warn("try get db {}-{} write lock but failed when checking backend storage medium",
                        db.getFullName(), dbId);
                continue;
            }
            Preconditions.checkState(locker.isDbWriteLockHeldByCurrentThread(db));
            try {
                for (Long tableId : tableIdToPartitionIds.keySet()) {
                    Table table = getTable(db.getId(), tableId);
                    if (table == null) {
                        continue;
                    }
                    OlapTable olapTable = (OlapTable) table;
                    PartitionInfo partitionInfo = olapTable.getPartitionInfo();

                    Collection<Long> partitionIds = tableIdToPartitionIds.get(tableId);
                    for (Long partitionId : partitionIds) {
                        Partition partition = olapTable.getPartition(partitionId);
                        if (partition == null) {
                            continue;
                        }
                        DataProperty dataProperty = partitionInfo.getDataProperty(partition.getId());
                        if (dataProperty.getStorageMedium() == TStorageMedium.SSD
                                && dataProperty.getCooldownTimeMs() < currentTimeMs) {
                            // expire. change to HDD.
                            DataProperty hdd = new DataProperty(TStorageMedium.HDD);
                            // log
                            ModifyPartitionInfo info =
                                    new ModifyPartitionInfo(db.getId(), olapTable.getId(),
                                            partition.getId(),
                                            hdd,
                                            (short) -1);
                            GlobalStateMgr.getCurrentState().getEditLog().logModifyPartition(info, wal -> {
                                partitionInfo.setDataProperty(partition.getId(), hdd);
                            });
                            LOG.debug("partition[{}-{}-{}] storage medium changed from SSD to HDD",
                                    dbId, tableId, partitionId);
                            storageMediumMap.put(partitionId, TStorageMedium.HDD);
                        }
                    } // end for partitions
                } // end for tables
            } finally {
                locker.unLockDatabase(db.getId(), LockType.WRITE);
            }
        } // end for dbs
        return storageMediumMap;
    }

    /*
     * used for handling AlterTableStmt (for client is the ALTER TABLE command).
     * including SchemaChangeHandler and RollupHandler
     */
    @Override
    public void alterTable(ConnectContext context, AlterTableStmt stmt) throws StarRocksException {
        AlterJobExecutor alterJobExecutor = new AlterJobExecutor();
        alterJobExecutor.process(stmt, context);
    }

    /**
     * used for handling AlterViewStmt (the ALTER VIEW command).
     */
    @Override
    public void alterView(ConnectContext context, AlterViewStmt stmt) throws StarRocksException {
        new AlterJobExecutor().process(stmt, context);
    }

    @Override
    public void createMaterializedView(CreateMaterializedViewStmt stmt)
            throws AnalysisException, DdlException {
        MaterializedViewHandler materializedViewHandler =
                GlobalStateMgr.getCurrentState().getAlterJobMgr().getMaterializedViewHandler();
        String tableName = stmt.getBaseIndexName();
        // check db
        String dbName = stmt.getDBName();
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }
        // check cluster capacity
        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().checkClusterCapacity();
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.READ);
        try {
            // check db quota
            checkDataSizeQuota(db);
            checkReplicaQuota(db);
        } catch (StarRocksException e) {
            throw new DdlException(e.getMessage());
        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
        }

        Table table = getTable(db.getFullName(), tableName);
        if (table == null) {
            throw new DdlException("create materialized failed. table:" + tableName + " not exist");
        }
        if (!table.isOlapOrCloudNativeTable()) {
            throw new DdlException("Do not support create synchronous materialized view(rollup) on " +
                    table.getType().name() + " table[" + tableName + "]");
        }
        OlapTable olapTable = (OlapTable) table;

        if (!locker.lockTableAndCheckDbExist(db, olapTable.getId(), LockType.WRITE)) {
            throw new DdlException("create materialized failed. database:" + db.getFullName() + " not exist");
        }
        try {
            if (olapTable.getKeysType() == KeysType.PRIMARY_KEYS) {
                throw new DdlException(
                        "Do not support create materialized view on primary key table[" + tableName + "]");
            }
            if (GlobalStateMgr.getCurrentState().getInsertOverwriteJobMgr().hasRunningOverwriteJob(olapTable.getId())) {
                throw new DdlException("Table[" + olapTable.getName() + "] is doing insert overwrite job, " +
                        "please start to create materialized view after insert overwrite");
            }
            olapTable.checkStableAndNormal();

            materializedViewHandler.processCreateMaterializedView(stmt, db, olapTable);
        } finally {
            locker.unLockTableWithIntensiveDbLock(db.getId(), olapTable.getId(), LockType.WRITE);
        }
    }

    // TODO(murphy) refactor it into MVManager
    @Override
    public void createMaterializedView(CreateMaterializedViewStatement stmt)
            throws DdlException {
        // check mv exists,name must be different from view/mv/table which exists in metadata
        String mvName = stmt.getTblName();
        String dbName = stmt.getDbName();
        LOG.debug("Begin create materialized view: {}", mvName);
        // check if db exists
        Database db = this.getDb(dbName);
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        // check if table exists in db
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.READ);
        try {
            if (getTable(db.getFullName(), mvName) != null) {
                if (stmt.isIfNotExists()) {
                    LOG.info("Create materialized view [{}] which already exists", mvName);
                    return;
                } else {
                    ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, mvName);
                }
            }
        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
        }
        // create columns
        List<Column> baseSchema = stmt.getMvColumnItems();
        validateColumns(baseSchema);

        Map<String, String> properties = stmt.getProperties();
        if (properties == null) {
            properties = Maps.newHashMap();
        }

        // create partition info
        // add generate columns into base schema
        PartitionInfo partitionInfo = buildPartitionInfo(stmt, baseSchema);

        // create distribution info
        DistributionDesc distributionDesc = stmt.getDistributionDesc();
        Preconditions.checkNotNull(distributionDesc);
        DistributionInfo baseDistribution = DistributionInfoBuilder.build(distributionDesc, baseSchema);
        // create refresh scheme
        MaterializedView.MvRefreshScheme mvRefreshScheme;
        RefreshSchemeClause refreshSchemeDesc = stmt.getRefreshSchemeDesc();
        if (refreshSchemeDesc instanceof AsyncRefreshSchemeDesc) {
            mvRefreshScheme = new MaterializedView.MvRefreshScheme();
            AsyncRefreshSchemeDesc asyncRefreshSchemeDesc = (AsyncRefreshSchemeDesc) refreshSchemeDesc;
            MaterializedView.AsyncRefreshContext asyncRefreshContext = mvRefreshScheme.getAsyncRefreshContext();
            asyncRefreshContext.setDefineStartTime(asyncRefreshSchemeDesc.isDefineStartTime());
            int randomizeStart = 0;
            if (properties.containsKey(PropertyAnalyzer.PROPERTY_MV_RANDOMIZE_START)) {
                try {
                    randomizeStart = Integer.parseInt(properties.get((PropertyAnalyzer.PROPERTY_MV_RANDOMIZE_START)));
                } catch (NumberFormatException e) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_INVALID_PARAMETER,
                            PropertyAnalyzer.PROPERTY_MV_RANDOMIZE_START + " only accept integer as parameter");
                }
                // remove this transient variable
                properties.remove(PropertyAnalyzer.PROPERTY_MV_RANDOMIZE_START);
            }

            long random = getRandomStart(asyncRefreshSchemeDesc.getIntervalLiteral(), randomizeStart);
            if (asyncRefreshSchemeDesc.isDefineStartTime() || randomizeStart == -1) {
                long definedStartTime = Utils.getLongFromDateTime(asyncRefreshSchemeDesc.getStartTime());
                // Add random set only if mv_randomize_start > 0 when user has already set the start time
                if (randomizeStart > 0) {
                    definedStartTime += random;
                }
                asyncRefreshContext.setStartTime(definedStartTime);
            } else if (asyncRefreshSchemeDesc.getIntervalLiteral() != null) {
                long currentTimeSecond = Utils.getLongFromDateTime(LocalDateTime.now());
                long randomizedStart = currentTimeSecond + random;
                asyncRefreshContext.setStartTime(randomizedStart);
            }
            if (asyncRefreshSchemeDesc.getIntervalLiteral() != null) {
                long intervalStep = ((IntLiteral) asyncRefreshSchemeDesc.getIntervalLiteral().getValue()).getValue();
                String refreshTimeUnit = asyncRefreshSchemeDesc.getIntervalLiteral().getUnitIdentifier().getDescription();
                asyncRefreshContext.setStep(intervalStep);
                asyncRefreshContext.setTimeUnit(refreshTimeUnit);

                // Check the interval time should not be less than the min allowed config time.
                if (Config.materialized_view_min_refresh_interval > 0) {
                    TimeUnit intervalTimeUnit = TimeUtils.convertUnitIdentifierToTimeUnit(refreshTimeUnit);
                    long periodSeconds = TimeUtils.convertTimeUnitValueToSecond(intervalStep, intervalTimeUnit);
                    if (periodSeconds < Config.materialized_view_min_refresh_interval) {
                        throw new DdlException(String.format("Refresh schedule interval %s is too small which may cost " +
                                        "a lot of memory/cpu resources to refresh the asynchronous materialized view, " +
                                        "please config an interval larger than " +
                                        "Config.materialized_view_min_refresh_interval(%ss).",
                                periodSeconds,
                                Config.materialized_view_min_refresh_interval));
                    }
                }
            }

            // task which type is EVENT_TRIGGERED can not use external table as base table now.
            if (asyncRefreshContext.getTimeUnit() == null) {
                // asyncRefreshContext's timeUnit is null means this task's type is EVENT_TRIGGERED
                Map<TableName, Table> tableNameTableMap = AnalyzerUtils.collectAllTable(stmt.getQueryStatement());
                if (tableNameTableMap.values().stream().anyMatch(table -> !table.isNativeTableOrMaterializedView())) {
                    throw new DdlException(
                            "Materialized view which type is ASYNC need to specify refresh interval for " +
                                    "external table");
                }
            }
        } else if (refreshSchemeDesc instanceof SyncRefreshSchemeDesc) {
            mvRefreshScheme = new MaterializedView.MvRefreshScheme();
            mvRefreshScheme.setType(MaterializedViewRefreshType.SYNC);
        } else if (refreshSchemeDesc instanceof ManualRefreshSchemeDesc) {
            mvRefreshScheme = new MaterializedView.MvRefreshScheme();
            mvRefreshScheme.setType(MaterializedViewRefreshType.MANUAL);
        } else {
            mvRefreshScheme = new MaterializedView.MvRefreshScheme();
            mvRefreshScheme.setType(MaterializedViewRefreshType.INCREMENTAL);
        }

        if (refreshSchemeDesc.getMoment() == RefreshSchemeClause.RefreshMoment.IMMEDIATE) {
            mvRefreshScheme.setMoment(MaterializedView.RefreshMoment.IMMEDIATE);
        } else if (refreshSchemeDesc.getMoment() == RefreshSchemeClause.RefreshMoment.DEFERRED) {
            mvRefreshScheme.setMoment(MaterializedView.RefreshMoment.DEFERRED);
        } else {
            throw new DdlException("Unknown refresh moment " + refreshSchemeDesc.getMoment() + " for materialized view");
        }

        // create mv
        long mvId = GlobalStateMgr.getCurrentState().getNextId();
        MaterializedView materializedView;
        if (RunMode.isSharedNothingMode()) {
            if (refreshSchemeDesc instanceof IncrementalRefreshSchemeDesc) {
                materializedView = GlobalStateMgr.getCurrentState().getMaterializedViewMgr()
                        .createSinkTable(stmt, partitionInfo, mvId, db.getId());
                materializedView.setMaintenancePlan(stmt.getMaintenancePlan());
            } else {
                materializedView =
                        new MaterializedView(mvId, db.getId(), mvName, baseSchema, stmt.getKeysType(), partitionInfo,
                                baseDistribution, mvRefreshScheme);
            }
        } else {
            Preconditions.checkState(RunMode.isSharedDataMode());
            if (refreshSchemeDesc instanceof IncrementalRefreshSchemeDesc) {
                throw new DdlException("Incremental materialized view in shared_data mode is not supported");
            }

            materializedView =
                    new LakeMaterializedView(mvId, db.getId(), mvName, baseSchema, stmt.getKeysType(), partitionInfo,
                            baseDistribution, mvRefreshScheme);
        }

        //bitmap indexes
        List<Index> mvIndexes = stmt.getMvIndexes();
        materializedView.setIndexes(mvIndexes);

        // sort keys
        if (CollectionUtils.isNotEmpty(stmt.getSortKeys())) {
            materializedView.setTableProperty(new TableProperty());
            materializedView.getTableProperty().setMvSortKeys(stmt.getSortKeys());
        }
        // set comment
        materializedView.setComment(stmt.getComment());
        // set baseTableIds
        materializedView.setBaseTableInfos(stmt.getBaseTableInfos());
        // set viewDefineSql
        materializedView.setViewDefineSql(stmt.getInlineViewDef());
        materializedView.setSimpleDefineSql(stmt.getSimpleViewDef());
        materializedView.setOriginalViewDefineSql(stmt.getOriginalViewDefineSql());
        materializedView.setIvmDefineSql(stmt.getIvmViewDef());
        materializedView.setOriginalDBName(stmt.getOriginalDBName());
        // current refresh mode
        materializedView.setCurrentRefreshMode(stmt.getCurrentRefreshMode());
        // set encode row id version
        materializedView.setEncodeRowIdVersion(stmt.getEncodeRowIdVersion());
        // set partitionRefTableExprs
        if (stmt.getPartitionRefTableExpr() != null) {
            //avoid to get a list of null inside
            materializedView.setPartitionRefTableExprs(Lists.newArrayList(stmt.getPartitionRefTableExpr()));
        }
        // set base index id
        long baseIndexMetaId = getNextId();
        materializedView.setBaseIndexMetaId(baseIndexMetaId);
        // set query output indexes
        materializedView.setQueryOutputIndices(stmt.getQueryOutputIndices());
        // set base index meta
        int schemaVersion = 0;
        int schemaHash = Util.schemaHash(schemaVersion, baseSchema, null, 0d);
        short shortKeyColumnCount = GlobalStateMgr.calcShortKeyColumnCount(baseSchema, null);
        TStorageType baseIndexStorageType = TStorageType.COLUMN;
        materializedView.setIndexMeta(baseIndexMetaId, mvName, baseSchema, schemaVersion, schemaHash,
                shortKeyColumnCount, baseIndexStorageType, stmt.getKeysType());

        // validate hint
        Map<String, String> optHints = Maps.newHashMap();
        if (stmt.isExistQueryScopeHint()) {
            SessionVariable sessionVariable = GlobalStateMgr.getCurrentState().getVariableMgr().newSessionVariable();
            for (HintNode hintNode : stmt.getAllQueryScopeHints()) {
                if (hintNode instanceof SetVarHint) {
                    for (Map.Entry<String, String> entry : hintNode.getValue().entrySet()) {
                        GlobalStateMgr.getCurrentState().getVariableMgr().setSystemVariable(sessionVariable,
                                new SystemVariable(entry.getKey(), new StringLiteral(entry.getValue())), true);
                        optHints.put(entry.getKey(), entry.getValue());
                    }
                } else if (hintNode instanceof UserVariableHint) {
                    throw new DdlException("unsupported user variable hint in Materialized view for now.");
                }
            }
        }

        boolean isNonPartitioned = partitionInfo.isUnPartitioned();
        DataProperty dataProperty = PropertyAnalyzer.analyzeMVDataProperty(materializedView, properties);
        String colocateGroup = properties.get(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH);
        PropertyAnalyzer.analyzeMVProperties(db, materializedView, properties, isNonPartitioned,
                stmt.getPartitionByExprToAdjustExprMap());
        final long warehouseId = materializedView.getWarehouseId();
        try {
            // process single partition info
            if (isNonPartitioned) {
                final WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
                final CRAcquireContext acquireContext = CRAcquireContext.of(warehouseId);
                final ComputeResource computeResource = warehouseManager.acquireComputeResource(acquireContext);
                if (!warehouseManager.isResourceAvailable(computeResource)) {
                    throw new DdlException("No available resource for warehouse " + warehouseId);
                }
                buildNonPartitionOlapTable(db, materializedView, partitionInfo, dataProperty, computeResource);
            } else {
                List<Expr> mvPartitionExprs = stmt.getPartitionByExprs();
                LinkedHashMap<Expr, SlotRef> partitionExprMaps = MVPartitionExprResolver.getMVPartitionExprsChecked(
                        mvPartitionExprs, stmt.getQueryStatement(), stmt.getBaseTableInfos());
                LOG.info("Generate mv {} partition exprs: {}", mvName, partitionExprMaps);
                materializedView.setPartitionExprMaps(partitionExprMaps);
            }

            // shared-data mv's colocation info must be updated after tablet creation
            if (StringUtils.isNotEmpty(colocateGroup)) {
                colocateTableIndex.addTableToGroup(db, materializedView, colocateGroup, true /* expectLakeTable */);
            }

            GlobalStateMgr.getCurrentState().getMaterializedViewMgr().prepareMaintenanceWork(stmt, materializedView);

            String storageVolumeId = "";
            if (materializedView.isCloudNativeMaterializedView()) {
                storageVolumeId = GlobalStateMgr.getCurrentState().getStorageVolumeMgr()
                        .getStorageVolumeIdOfTable(materializedView.getId());
            }
            onCreate(db, materializedView, storageVolumeId, stmt.isIfNotExists());
        } catch (DdlException e) {
            if (materializedView.isCloudNativeMaterializedView()) {
                GlobalStateMgr.getCurrentState().getStorageVolumeMgr().unbindTableToStorageVolume(materializedView.getId());
            }
            throw e;
        }
        LOG.info("Successfully create materialized view [{}:{}]", mvName, materializedView.getMvId());

        // NOTE: The materialized view has been added to the database, and the following procedure cannot throw exception.
        createTaskForMaterializedView(dbName, materializedView, optHints);
        DynamicPartitionUtil.registerOrRemovePartitionTTLTable(db.getId(), materializedView);
    }

    /**
     * Initialize a non-partitioned table with one partition.
     */
    public void buildNonPartitionOlapTable(Database db,
                                           OlapTable olapTable,
                                           PartitionInfo partitionInfo,
                                           DataProperty dataProperty,
                                           ComputeResource computeResource) throws DdlException {
        if (olapTable.isPartitionedTable()) {
            throw new DdlException("Table " + olapTable.getName() + " is a partitioned table, not a non-partitioned table");
        }

        long partitionId = GlobalStateMgr.getCurrentState().getNextId();
        Preconditions.checkNotNull(dataProperty);
        partitionInfo.setDataProperty(partitionId, dataProperty);
        partitionInfo.setReplicationNum(partitionId, olapTable.getDefaultReplicationNum());
        StorageInfo storageInfo = olapTable.getTableProperty().getStorageInfo();
        partitionInfo.setDataCacheInfo(partitionId,
                storageInfo == null ? null : storageInfo.getDataCacheInfo());
        Set<Long> tabletIdSet = new HashSet<>();
        Long version = Partition.PARTITION_INIT_VERSION;
        Partition partition = createPartition(db, olapTable, partitionId, olapTable.getName(), version, tabletIdSet,
                computeResource);
        buildPartitions(db, olapTable, new ArrayList<>(partition.getSubPartitions()), computeResource);
        olapTable.addPartition(partition);
    }

    private long getRandomStart(IntervalLiteral interval, long randomizeStart) throws DdlException {
        if (interval == null || randomizeStart == -1) {
            return 0;
        }
        // randomize the start time if not specified manually, to avoid refresh conflicts
        // default random interval is min(300s, INTERVAL/2)
        // user could specify it through mv_randomize_start
        long period = ((IntLiteral) interval.getValue()).getLongValue();
        TimeUnit timeUnit =
                TimeUtils.convertUnitIdentifierToTimeUnit(interval.getUnitIdentifier().getDescription());
        long intervalSeconds = TimeUtils.convertTimeUnitValueToSecond(period, timeUnit);
        long randomInterval = randomizeStart == 0 ? Math.min(300, intervalSeconds / 2) : randomizeStart;
        return randomInterval > 0 ? ThreadLocalRandom.current().nextLong(randomInterval) : randomInterval;
    }

    public static PartitionInfo buildPartitionInfo(CreateMaterializedViewStatement stmt,
                                                   List<Column> baseSchema) throws DdlException {
        List<Expr> partitionByExprs = stmt.getPartitionByExprs();
        PartitionType partitionType = stmt.getPartitionType();
        List<Column> mvPartitionColumns = stmt.getPartitionColumns();
        if (CollectionUtils.isNotEmpty(partitionByExprs)) {
            if (partitionByExprs.size() != mvPartitionColumns.size()) {
                throw new DdlException(String.format("Partition by exprs size %s is not equal to partition columns size %s",
                        partitionByExprs.size(), mvPartitionColumns.size()));
            }

            if (partitionType == PartitionType.LIST) {
                Map<Integer, Column> generatedPartitionCols = stmt.getGeneratedPartitionCols();
                List<Column> newPartitionColumns = new ArrayList<>();
                Preconditions.checkNotNull(baseSchema);
                for (int i = 0; i < partitionByExprs.size(); i++) {
                    Expr partitionByExpr = partitionByExprs.get(i);
                    Column mvPartitionColumn = mvPartitionColumns.get(i);
                    // generate column can be any partition expression.
                    if (generatedPartitionCols.containsKey(i)) {
                        Column generatedCol = generatedPartitionCols.get(i);
                        if (generatedCol == null) {
                            throw new DdlException("Partition expression for list must be a generated column: "
                                    + ExprToSql.toSql(partitionByExpr));
                        }
                        baseSchema.add(generatedCol);
                        newPartitionColumns.add(generatedCol);
                    } else {
                        if (!(partitionByExpr instanceof SlotRef || MvUtils.isFuncCallExpr(partitionByExpr,
                                FunctionSet.DATE_TRUNC))) {
                            throw new DdlException("List partition expression can only be ref-base-table's partition " +
                                    "expression but contains: " + ExprToSql.toSql(partitionByExpr));
                        }
                        newPartitionColumns.add(mvPartitionColumn);
                    }
                }
                return new ListPartitionInfo(PartitionType.LIST, newPartitionColumns);
            } else {
                if (partitionByExprs.size() > 1) {
                    throw new DdlException("Only support one partition column for range partition");
                }

                Expr partitionByExpr = partitionByExprs.get(0);
                ExpressionPartitionDesc expressionPartitionDesc = new ExpressionPartitionDesc(partitionByExpr);
                return PartitionInfoBuilder.build(expressionPartitionDesc, mvPartitionColumns, Maps.newHashMap(), false);
            }
        } else {
            if (partitionType != PartitionType.UNPARTITIONED && partitionType != null) {
                throw new DdlException("Partition type is " + stmt.getPartitionType() + ", but partition by expr is null");
            }
            return new SinglePartitionInfo();
        }
    }

    private void createTaskForMaterializedView(String dbName, MaterializedView materializedView,
                                               Map<String, String> optHints) throws DdlException {
        MaterializedViewRefreshType refreshType = materializedView.getRefreshScheme().getType();
        MaterializedView.RefreshMoment refreshMoment = materializedView.getRefreshScheme().getMoment();

        if (refreshType.equals(MaterializedViewRefreshType.INCREMENTAL)) {
            GlobalStateMgr.getCurrentState().getMaterializedViewMgr().startMaintainMV(materializedView);
            return;
        }

        if (refreshType != MaterializedViewRefreshType.SYNC) {

            Task task = TaskBuilder.buildMvTask(materializedView, dbName);
            TaskBuilder.updateTaskInfo(task, materializedView);

            if (optHints != null) {
                Map<String, String> taskProperties = task.getProperties();
                taskProperties.putAll(optHints);
            }

            TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
            taskManager.createTask(task);
            if (refreshMoment.equals(MaterializedView.RefreshMoment.IMMEDIATE)) {
                taskManager.executeTask(task.getName());
            }
        }
    }

    /**
     * Leave some clean up work to {@link MaterializedView#onDrop}
     */
    @Override
    public void dropMaterializedView(DropMaterializedViewStmt stmt) throws DdlException, MetaNotFoundException {
        Database db = getDb(stmt.getDbName());
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, stmt.getDbName());
        }
        Table table;
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.READ);
        try {
            table = getTable(db.getFullName(), stmt.getMvName());
        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
        }
        if (table instanceof MaterializedView) {
            try {
                TableRef tableRef = stmt.getTableRef();
                NodePosition pos = (tableRef != null) ? tableRef.getPos() : NodePosition.ZERO;
                TableName tableName = new TableName(stmt.getCatalogName(), stmt.getDbName(),
                        stmt.getMvName(), pos);
                Authorizer.checkMaterializedViewAction(ConnectContext.get(), tableName, PrivilegeType.DROP);
            } catch (AccessDeniedException e) {
                AccessDeniedException.reportAccessDenied(
                        stmt.getCatalogName(),
                        ConnectContext.get().getCurrentUserIdentity(),
                        ConnectContext.get().getCurrentRoleIds(), PrivilegeType.DROP.name(), ObjectType.MATERIALIZED_VIEW.name(),
                        stmt.getMvName());
            }

            db.dropTable(table.getName(), stmt.isSetIfExists(), true);
        } else {
            stateMgr.getAlterJobMgr().processDropMaterializedView(stmt);
        }
    }

    @Override
    public void alterMaterializedView(AlterMaterializedViewStmt stmt) throws DdlException, MetaNotFoundException {
        new AlterMVJobExecutor().process(stmt, ConnectContext.get());
    }

    private String executeRefreshMvTask(String dbName, MaterializedView materializedView,
                                        ExecuteOption executeOption)
            throws DdlException {
        MaterializedViewRefreshType refreshType = materializedView.getRefreshScheme().getType();
        LOG.info("Start to execute refresh materialized view task, mv: {}, refreshType: {}, executionOption:{}",
                materializedView.getName(), refreshType, executeOption);

        if (refreshType.equals(MaterializedViewRefreshType.INCREMENTAL)) {
            GlobalStateMgr.getCurrentState().getMaterializedViewMgr().onTxnPublish(materializedView);
        } else if (refreshType != MaterializedViewRefreshType.SYNC) {
            TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
            final String mvTaskName = TaskBuilder.getMvTaskName(materializedView.getId());
            if (!taskManager.containTask(mvTaskName)) {
                Task task = TaskBuilder.buildMvTask(materializedView, dbName);
                TaskBuilder.updateTaskInfo(task, materializedView);
                taskManager.createTask(task);
            }
            return taskManager.executeTask(mvTaskName, executeOption).getQueryId();
        }
        return null;
    }

    private MaterializedView getMaterializedViewToRefresh(String dbName, String mvName)
            throws DdlException, MetaNotFoundException {
        Database db = this.getDb(dbName);
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }
        final Table table = getTable(db.getFullName(), mvName);
        MaterializedView materializedView = null;
        if (table instanceof MaterializedView) {
            materializedView = (MaterializedView) table;
        }
        if (materializedView == null) {
            throw new MetaNotFoundException(mvName + " is not a materialized view");
        }
        return materializedView;
    }

    public String refreshMaterializedView(String dbName, String mvName, boolean force,
                                          EitherOr<PartitionRangeDesc, Set<PListCell>> partitionDesc,
                                          int priority, boolean mergeRedundant, boolean isManual)
            throws DdlException, MetaNotFoundException {
        return refreshMaterializedView(dbName, mvName, force, partitionDesc, priority, mergeRedundant, isManual, false,
                null);
    }

    public String refreshMaterializedView(String dbName, String mvName, boolean force,
                                          EitherOr<PartitionRangeDesc, Set<PListCell>> partitionDesc,
                                          int priority, boolean mergeRedundant, boolean isManual, boolean isSync,
                                          StatementBase statement) throws DdlException, MetaNotFoundException {
        MaterializedView materializedView = getMaterializedViewToRefresh(dbName, mvName);
        String mvTaskName = TaskBuilder.getMvTaskName(materializedView.getId());
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        Task task = taskManager.getTask(mvTaskName);

        HashMap<String, String> taskRunProperties = new HashMap<>(task.getProperties());
        if (partitionDesc != null) {
            if (!partitionDesc.getFirst().isEmpty()) {
                PartitionRangeDesc range = partitionDesc.left();
                taskRunProperties.put(TaskRun.PARTITION_START, range == null ? null : range.getPartitionStart());
                taskRunProperties.put(TaskRun.PARTITION_END, range == null ? null : range.getPartitionEnd());
            } else if (!partitionDesc.getSecond().isEmpty()) {
                Set<PListCell> list = partitionDesc.right();
                if (!CollectionUtils.isEmpty(list)) {
                    taskRunProperties.put(TaskRun.PARTITION_VALUES, PListCell.batchSerialize(list));
                }
            }
        }
        taskRunProperties.put(TaskRun.FORCE, Boolean.toString(force));

        ExecuteOption executeOption = new ExecuteOption(priority, mergeRedundant, taskRunProperties);
        executeOption.setManual(isManual);
        executeOption.setSync(isSync);
        if (statement != null && statement.isExplain()) {
            return taskManager.getMVRefreshExplain(task, executeOption, statement);
        } else {
            return executeRefreshMvTask(dbName, materializedView, executeOption);
        }
    }

    @Override
    public String refreshMaterializedView(RefreshMaterializedViewStatement refreshMaterializedViewStatement)
            throws DdlException, MetaNotFoundException {
        String dbName = refreshMaterializedViewStatement.getDbName();
        String mvName = refreshMaterializedViewStatement.getMvName();
        boolean force = refreshMaterializedViewStatement.isForceRefresh();
        EitherOr<PartitionRangeDesc, Set<PListCell>> partitionDesc = refreshMaterializedViewStatement.getPartitionDesc();
        int priority = refreshMaterializedViewStatement.getPriority() != null ?
                refreshMaterializedViewStatement.getPriority() : Constants.TaskRunPriority.HIGH.value();
        return refreshMaterializedView(dbName, mvName, force, partitionDesc, priority,
                Config.enable_mv_refresh_sync_refresh_mergeable, true, refreshMaterializedViewStatement.isSync(),
                refreshMaterializedViewStatement);
    }

    @Override
    public void cancelRefreshMaterializedView(
            CancelRefreshMaterializedViewStmt stmt) throws DdlException, MetaNotFoundException {
        String dbName = stmt.getDbName();
        String mvName = stmt.getMvName();
        MaterializedView materializedView = getMaterializedViewToRefresh(dbName, mvName);
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        Task refreshTask = taskManager.getTask(TaskBuilder.getMvTaskName(materializedView.getId()));
        boolean isForce = stmt.isForce();
        if (refreshTask != null) {
            taskManager.killTask(refreshTask.getName(), isForce);
        }
    }

    /*
     * used for handling CacnelAlterStmt (for client is the CANCEL ALTER
     * command). including SchemaChangeHandler and RollupHandler
     */
    public void cancelAlter(CancelAlterTableStmt stmt, String reason) throws DdlException {
        if (stmt.getAlterType() == ShowAlterStmt.AlterType.ROLLUP) {
            stateMgr.getRollupHandler().cancel(stmt, reason);
        } else if (stmt.getAlterType() == ShowAlterStmt.AlterType.COLUMN
                || stmt.getAlterType() == ShowAlterStmt.AlterType.OPTIMIZE) {
            stateMgr.getSchemaChangeHandler().cancel(stmt, reason);
        } else if (stmt.getAlterType() == ShowAlterStmt.AlterType.MATERIALIZED_VIEW) {
            stateMgr.getRollupHandler().cancelMV(stmt);
        } else {
            throw new DdlException("Cancel " + stmt.getAlterType() + " does not implement yet");
        }
    }

    public void cancelAlter(CancelAlterTableStmt stmt) throws DdlException {
        cancelAlter(stmt, "user cancelled");
    }

    // entry of rename table operation
    @Override
    public void renameTable(Database db, Table table, TableRenameClause tableRenameClause) throws DdlException {
        OlapTable olapTable = (OlapTable) table;
        if (olapTable.getState() != OlapTable.OlapTableState.NORMAL) {
            throw new DdlException("Table[" + olapTable.getName() + "] is under " + olapTable.getState());
        }

        String oldTableName = olapTable.getName();
        String newTableName = tableRenameClause.getNewTableName();
        if (oldTableName.equals(newTableName)) {
            throw new DdlException("Same table name");
        }

        // check if name is already used
        if (getTable(db.getFullName(), newTableName) != null) {
            throw new DdlException("Table name[" + newTableName + "] is already used");
        }
        olapTable.checkNameConflict(newTableName);

        TableInfo tableInfo = TableInfo.createForTableRename(db.getId(), olapTable.getId(), newTableName);
        GlobalStateMgr.getCurrentState().getEditLog().logTableRename(tableInfo, wal -> {
            olapTable.setName(newTableName);
            db.dropTable(oldTableName);
            db.registerTableUnlocked(olapTable);
        });
        AlterMVJobExecutor.inactiveRelatedMaterializedViewsRecursive(olapTable,
                MaterializedViewExceptions.inactiveReasonForBaseTableRenamed(oldTableName), false);
        LOG.info("rename table[{}] to {}, tableId: {}", oldTableName, newTableName, olapTable.getId());
    }

    @Override
    public void alterTableComment(Database db, Table table, AlterTableCommentClause clause) {
        ModifyTablePropertyOperationLog log = new ModifyTablePropertyOperationLog(db.getId(), table.getId());
        log.setComment(clause.getNewComment());
        GlobalStateMgr.getCurrentState().getEditLog().logAlterTableProperties(log, wal -> {
            table.setComment(clause.getNewComment());
        });
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
            LOG.info("replay rename table[{}] to {}, tableId: {}", tableName, newTableName, table.getId());
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
    }

    @Override
    public void renamePartition(Database db, Table table, PartitionRenameClause renameClause) throws DdlException {
        OlapTable olapTable = (OlapTable) table;
        if (olapTable.getState() != OlapTable.OlapTableState.NORMAL) {
            throw new DdlException("Table[" + olapTable.getName() + "] is under " + olapTable.getState());
        }

        if (!olapTable.getPartitionInfo().isRangePartition()) {
            throw new DdlException("Table[" + olapTable.getName() + "] is single partitioned. "
                    + "no need to rename partition name.");
        }

        // for automatic partitioned table, not support to rename partition name
        // since partition name is generated by system and depends on partition value
        if (olapTable.getPartitionInfo().isAutomaticPartition()) {
            throw new DdlException("Table[" + olapTable.getName() + "] is automatic partitioned. "
                    + "not support to rename partition name.");
        }

        String partitionName = renameClause.getPartitionName();
        String newPartitionName = renameClause.getNewPartitionName();
        if (partitionName.equalsIgnoreCase(newPartitionName)) {
            throw new DdlException("Same partition name");
        }

        Partition partition = olapTable.getPartition(partitionName);
        if (partition == null) {
            throw new DdlException("Partition[" + partitionName + "] does not exists");
        }

        // check if name is already used
        if (olapTable.checkPartitionNameExist(newPartitionName)) {
            throw new DdlException("Partition name[" + newPartitionName + "] is already used");
        }

        // log
        TableInfo tableInfo = TableInfo.createForPartitionRename(db.getId(), olapTable.getId(), partition.getId(),
                newPartitionName);
        GlobalStateMgr.getCurrentState().getEditLog().logPartitionRename(tableInfo, wal -> {
            olapTable.renamePartition(partitionName, newPartitionName);
        });
        LOG.info("rename partition[{}] to {}", partitionName, newPartitionName);
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

    public void renameRollup(Database db, OlapTable table, RollupRenameClause renameClause) throws DdlException {
        if (table.getState() != OlapTable.OlapTableState.NORMAL) {
            throw new DdlException("Table[" + table.getName() + "] is under " + table.getState());
        }

        String rollupName = renameClause.getRollupName();
        // check if it is base table name
        if (rollupName.equals(table.getName())) {
            throw new DdlException("Using ALTER TABLE RENAME to change table name");
        }

        String newRollupName = renameClause.getNewRollupName();
        if (rollupName.equals(newRollupName)) {
            throw new DdlException("Same rollup name");
        }

        Map<String, Long> indexNameToMetaId = table.getIndexNameToMetaId();
        if (indexNameToMetaId.get(rollupName) == null) {
            throw new DdlException("Rollup index[" + rollupName + "] does not exists");
        }

        // check if name is already used
        if (indexNameToMetaId.get(newRollupName) != null) {
            throw new DdlException("Rollup name[" + newRollupName + "] is already used");
        }

        long indexMetaId = indexNameToMetaId.get(rollupName);
        // log
        TableInfo tableInfo = TableInfo.createForRollupRename(db.getId(), table.getId(), indexMetaId, newRollupName);
        GlobalStateMgr.getCurrentState().getEditLog().logRollupRename(tableInfo, wal -> {
            indexNameToMetaId.remove(rollupName);
            indexNameToMetaId.put(newRollupName, indexMetaId);
        });
        LOG.info("rename rollup[{}] to {}", rollupName, newRollupName);
    }

    public void replayRenameRollup(TableInfo tableInfo) {
        long dbId = tableInfo.getDbId();
        long tableId = tableInfo.getTableId();
        long indexMetaId = tableInfo.getIndexMetaId();
        String newRollupName = tableInfo.getNewRollupName();

        Database db = getDb(dbId);
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            OlapTable table = (OlapTable) getTable(db.getId(), tableId);
            String rollupName = table.getIndexNameByMetaId(indexMetaId);
            Map<String, Long> indexNameToIdMap = table.getIndexNameToMetaId();
            indexNameToIdMap.remove(rollupName);
            indexNameToIdMap.put(newRollupName, indexMetaId);

            LOG.info("replay rename rollup[{}] to {}", rollupName, newRollupName);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
    }

    public void renameColumn(Database db, Table table, ColumnRenameClause renameClause) {
        if (!(table instanceof OlapTable olapTable)) {
            throw ErrorReportException.report(ErrorCode.ERR_COLUMN_RENAME_ONLY_FOR_OLAP_TABLE);
        }
        if (db.isSystemDatabase() || db.isStatisticsDatabase()) {
            throw ErrorReportException.report(ErrorCode.ERR_CANNOT_RENAME_COLUMN_IN_INTERNAL_DB, db.getFullName());
        }
        if (olapTable.getState() != OlapTable.OlapTableState.NORMAL) {
            throw ErrorReportException.report(ErrorCode.ERR_CANNOT_RENAME_COLUMN_OF_NOT_NORMAL_TABLE, olapTable.getState());
        }

        String colName = renameClause.getColName();
        String newColName = renameClause.getNewColName();

        Column column = olapTable.getColumn(colName);
        if (column == null) {
            throw ErrorReportException.report(ErrorCode.ERR_BAD_FIELD_ERROR, colName, table.getName());
        }
        Column currentColumn = olapTable.getColumn(newColName);
        if (currentColumn != null) {
            throw ErrorReportException.report(ErrorCode.ERR_DUP_FIELDNAME, newColName);
        }

        ColumnRenameInfo columnRenameInfo = new ColumnRenameInfo(db.getId(), table.getId(), colName, newColName);
        GlobalStateMgr.getCurrentState().getEditLog().logColumnRename(columnRenameInfo, wal -> {
            olapTable.renameColumn(colName, newColName);
        });
        LOG.info("rename column {} to {}", colName, newColName);
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

    // properties must be checked before this call
    public void modifyTableDynamicPartition(Database db, OlapTable table, Map<String, String> properties)
            throws DdlException {
        Map<String, String> logProperties = new HashMap<>(properties);
        TableProperty tableProperty = table.getTableProperty();
        Map<String, String> analyzedDynamicPartition = DynamicPartitionUtil.analyzeDynamicPartition(properties);

        ModifyTablePropertyOperationLog info =
                new ModifyTablePropertyOperationLog(db.getId(), table.getId(), logProperties);
        GlobalStateMgr.getCurrentState().getEditLog().logDynamicPartition(info, wal -> {
            tableProperty.modifyTableProperties(analyzedDynamicPartition);
            tableProperty.buildDynamicProperty();
            DynamicPartitionUtil.registerOrRemovePartitionScheduleInfo(db.getId(), table);
        });
    }

    private void alterPartitionLiveNumber(Database db,
                                          OlapTable table,
                                          Map<String, String> properties,
                                          List<Runnable> appliers) throws DdlException {
        if (!table.getPartitionInfo().isRangePartition()) {
            throw new DdlException("Table[" + table.getName() + "] is not range partitioned. "
                    + "no need to set partition live number.");
        }
        int partitionLiveNumber = PropertyAnalyzer.analyzePartitionLiveNumber(properties, true);
        TableProperty tableProperty = table.getTableProperty();
        appliers.add(() -> {
            tableProperty.modifyTableProperties(
                    PropertyAnalyzer.PROPERTIES_PARTITION_LIVE_NUMBER, String.valueOf(partitionLiveNumber));
            tableProperty.setPartitionTTLNumber(partitionLiveNumber);
            if (partitionLiveNumber == TableProperty.INVALID) {
                GlobalStateMgr.getCurrentState().getDynamicPartitionScheduler()
                        .removeTtlPartitionTable(db.getId(), table.getId());
            } else {
                GlobalStateMgr.getCurrentState().getDynamicPartitionScheduler()
                        .registerTtlPartitionTable(db.getId(), table.getId());
            }
        });
    }

    private void alterStorageMedium(OlapTable table,
                                    Map<String, String> properties,
                                    List<Runnable> appliers) throws DdlException {
        try {
            DataProperty dataProperty = PropertyAnalyzer.analyzeDataProperty(
                    properties, DataProperty.getInferredDefaultDataProperty(), false);
            TStorageMedium storageMedium = dataProperty.getStorageMedium();
            appliers.add(() -> {
                table.setStorageMedium(storageMedium);
                table.getTableProperty().modifyTableProperties(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TIME,
                        String.valueOf(dataProperty.getCooldownTimeMs()));
            });
        } catch (AnalysisException ex) {
            throw new DdlException(ex.getMessage());
        }
    }

    private void alterStorageCooldownTTL(OlapTable table,
                                       Map<String, String> properties,
                                       List<Runnable> appliers) throws DdlException {
        try {
            String storageCoolDownTTL = properties.get(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TTL);
            PeriodDuration periodDuration = PropertyAnalyzer.analyzeStorageCoolDownTTL(properties, true);
            TableProperty tableProperty = table.getTableProperty();
            appliers.add(() -> {
                tableProperty.modifyTableProperties(
                        PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TTL, storageCoolDownTTL);
                tableProperty.setStorageCoolDownTTL(periodDuration);
            });
        } catch (AnalysisException ex) {
            throw new DdlException(ex.getMessage());
        }
    }

    private void alterDataCachePartitionDuration(OlapTable table,
                                                 Map<String, String> properties,
                                                 List<Runnable> appliers) throws DdlException {
        try {
            String partitionDuration = properties.get(PropertyAnalyzer.PROPERTIES_DATACACHE_PARTITION_DURATION);
            PeriodDuration periodDuration = PropertyAnalyzer.analyzeDataCachePartitionDuration(properties);
            TableProperty tableProperty = table.getTableProperty();
            appliers.add(() -> {
                tableProperty.modifyTableProperties(
                        PropertyAnalyzer.PROPERTIES_DATACACHE_PARTITION_DURATION, partitionDuration);
                tableProperty.setDataCachePartitionDuration(periodDuration);
            });
        } catch (AnalysisException ex) {
            throw new DdlException(ex.getMessage());
        }
    }

    private void alterLabelsLocation(OlapTable table,
                                    Map<String, String> properties,
                                    List<Runnable> appliers) throws DdlException {
        if (table.getColocateGroup() != null) {
            throw new DdlException("Cannot set location for colocate table");
        }
        String location = properties.get(PropertyAnalyzer.PROPERTIES_LABELS_LOCATION);
        PropertyAnalyzer.analyzeLocation(properties, true);
        appliers.add(() -> {
            table.setLocation(location);
        });
    }

    private void alterPartitionTTL(OlapTable table,
                                   Map<String, String> properties,
                                   List<Runnable> appliers) throws DdlException {
        if (!table.getPartitionInfo().isRangePartition()) {
            throw new DdlException("Table[" + table.getName() + "] is not range partitioned. "
                    + "no need to set partition ttl.");
        }
        Pair<String, PeriodDuration> ttlDuration = PropertyAnalyzer.analyzePartitionTTL(properties, true);
        TableProperty tableProperty = table.getTableProperty();
        appliers.add(() -> {
            tableProperty.modifyTableProperties(
                    PropertyAnalyzer.PROPERTIES_PARTITION_TTL, ttlDuration.first);
            tableProperty.setPartitionTTL(ttlDuration.second);
        });
    }

    private void alterPartitionRetentionCondition(Database db,
                                                  OlapTable table,
                                                  Map<String, String> properties,
                                                  List<Runnable> appliers) throws DdlException {
        if (!table.getPartitionInfo().isPartitioned()) {
            throw new DdlException("Table[" + table.getName() + "] is not partitioned. "
                    + "no need to set partition retention condition.");
        }
        String ttlRetentionCondition = PropertyAnalyzer
                .analyzePartitionRetentionCondition(db, table, properties, true, null);
        TableProperty tableProperty = table.getTableProperty();
        appliers.add(() -> {
            tableProperty.modifyTableProperties(
                    PropertyAnalyzer.PROPERTIES_PARTITION_RETENTION_CONDITION, ttlRetentionCondition);
            tableProperty.setPartitionRetentionCondition(ttlRetentionCondition);
            // register or remove ttl partition table
            if (Strings.isNullOrEmpty(ttlRetentionCondition)) {
                GlobalStateMgr.getCurrentState().getDynamicPartitionScheduler().removeTtlPartitionTable(db.getId(),
                        table.getId());
            } else {
                GlobalStateMgr.getCurrentState().getDynamicPartitionScheduler().registerTtlPartitionTable(db.getId(),
                        table.getId());
            }
        });
    }

    private void alterTimeDriftConstraint(OlapTable table,
                                        Map<String, String> properties,
                                        List<Runnable> appliers) {
        String spec = properties.get(PropertyAnalyzer.PROPERTIES_TIME_DRIFT_CONSTRAINT);
        PropertyAnalyzer.analyzeTimeDriftConstraint(spec, table, properties);
        TableProperty tableProperty = table.getTableProperty();
        appliers.add(() -> {
            tableProperty.modifyTableProperties(PropertyAnalyzer.PROPERTIES_TIME_DRIFT_CONSTRAINT, spec);
            tableProperty.setTimeDriftConstraintSpec(spec);
        });
    }

    private void alterEnableStatisticCollectOnFirstLoad(OlapTable table,
                                                        Map<String, String> properties,
                                                        List<Runnable> appliers) {
        boolean enable = PropertyAnalyzer.analyzeEnableStatisticCollectOnFirstLoad(properties);
        properties.remove(PropertyAnalyzer.PROPERTIES_ENABLE_STATISTIC_COLLECT_ON_FIRST_LOAD);
        appliers.add(() -> {
            table.setEnableStatisticCollectOnFirstLoad(enable);
        });
    }

    private void alterCloudNativeFastSchemaEvolutionV2(OlapTable table,
                                                       Map<String, String> properties,
                                                       List<Runnable> appliers) {
        boolean value = PropertyAnalyzer.analyzeCloudNativeFastSchemaEvolutionV2(table.getType(), properties, true);
        appliers.add(() -> {
            ((LakeTable) table).setFastSchemaEvolutionV2(value);
        });
    }

    private void alterLakeCompactionMaxParallel(OlapTable table,
                                                Map<String, String> properties,
                                                List<Runnable> appliers) throws DdlException {
        if (!table.isCloudNativeTableOrMaterializedView()) {
            throw new DdlException("Property " + PropertyAnalyzer.PROPERTIES_LAKE_COMPACTION_MAX_PARALLEL +
                    " can only be set for cloud native tables");
        }
        String value = properties.remove(PropertyAnalyzer.PROPERTIES_LAKE_COMPACTION_MAX_PARALLEL);
        try {
            int maxParallel = Integer.parseInt(value);
            if (maxParallel < 0) {
                throw new DdlException("Invalid lake_compaction_max_parallel value: " + value +
                        ". Value must be non-negative.");
            }
            appliers.add(() -> {
                table.setLakeCompactionMaxParallel(maxParallel);
            });
        } catch (NumberFormatException e) {
            throw new DdlException("Invalid lake_compaction_max_parallel value: " + value +
                    ". Value must be an integer.");
        }
    }

    public void alterTableProperties(Database db, OlapTable table, Map<String, String> properties)
            throws DdlException {
        Map<String, String> propertiesToPersist = new HashMap<>(properties);
        List<Runnable> appliers = new ArrayList<>();
        if (propertiesToPersist.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_LIVE_NUMBER)) {
            alterPartitionLiveNumber(db, table, properties, appliers);
        }
        if (propertiesToPersist.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM)) {
            alterStorageMedium(table, properties,  appliers);
        }
        if (propertiesToPersist.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TTL)) {
            alterStorageCooldownTTL(table, properties, appliers);
        }
        if (propertiesToPersist.containsKey(PropertyAnalyzer.PROPERTIES_DATACACHE_PARTITION_DURATION)) {
            alterDataCachePartitionDuration(table, properties, appliers);
        }
        if (propertiesToPersist.containsKey(PropertyAnalyzer.PROPERTIES_LABELS_LOCATION)) {
            alterLabelsLocation(table, properties, appliers);
        }
        if (propertiesToPersist.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_TTL)) {
            alterPartitionTTL(table, properties, appliers);
        }
        if (propertiesToPersist.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_RETENTION_CONDITION)) {
            alterPartitionRetentionCondition(db, table, properties, appliers);
        }
        if (propertiesToPersist.containsKey(PropertyAnalyzer.PROPERTIES_TIME_DRIFT_CONSTRAINT)) {
            alterTimeDriftConstraint(table, properties, appliers);
        }
        if (propertiesToPersist.containsKey(PropertyAnalyzer.PROPERTIES_ENABLE_STATISTIC_COLLECT_ON_FIRST_LOAD)) {
            alterEnableStatisticCollectOnFirstLoad(table, properties, appliers);
        }
        if (propertiesToPersist.containsKey(PropertyAnalyzer.PROPERTIES_CLOUD_NATIVE_FAST_SCHEMA_EVOLUTION_V2)) {
            alterCloudNativeFastSchemaEvolutionV2(table, properties, appliers);
        }
        if (propertiesToPersist.containsKey(PropertyAnalyzer.PROPERTIES_LAKE_COMPACTION_MAX_PARALLEL)) {
            alterLakeCompactionMaxParallel(table, properties, appliers);
        }
        if (!properties.isEmpty()) {
            throw new DdlException("Modify failed because unknown properties: " + properties);
        }

        if (!appliers.isEmpty()) {
            ModifyTablePropertyOperationLog info =
                    new ModifyTablePropertyOperationLog(db.getId(), table.getId(), propertiesToPersist);
            GlobalStateMgr.getCurrentState().getEditLog().logAlterTableProperties(info, wal -> {
                for (Runnable applier : appliers) {
                    applier.run();
                }
            });
        }
    }

    /**
     * Set replication number for unpartitioned table.
     * ATTN: only for unpartitioned table now.
     *
     * @param db
     * @param table
     * @param properties
     * @throws DdlException
     */
    // The caller need to hold the db write lock
    public void modifyTableReplicationNum(Database db, OlapTable table, Map<String, String> properties)
            throws DdlException {
        if (colocateTableIndex.isColocateTable(table.getId())) {
            throw new DdlException("table " + table.getName() + " is colocate table, cannot change replicationNum");
        }

        String defaultReplicationNumName = "default." + PropertyAnalyzer.PROPERTIES_REPLICATION_NUM;
        PartitionInfo partitionInfo = table.getPartitionInfo();
        if (partitionInfo.isRangePartition()) {
            throw new DdlException(
                    "This is a range partitioned table, you should specify partitions with MODIFY PARTITION clause." +
                            " If you want to set default replication number, please use '" + defaultReplicationNumName +
                            "' instead of '" + PropertyAnalyzer.PROPERTIES_REPLICATION_NUM + "' to escape misleading.");
        }

        // unpartitioned table
        // update partition replication num
        String partitionName = table.getName();
        Partition partition = table.getPartition(partitionName);
        if (partition == null) {
            throw new DdlException("Partition does not exist. name: " + partitionName);
        }

        short replicationNum = Short.parseShort(properties.get(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM));
        DataProperty newDataProperty = partitionInfo.getDataProperty(partition.getId());

        // log
        ModifyPartitionInfo info = new ModifyPartitionInfo(db.getId(), table.getId(), partition.getId(),
                newDataProperty, replicationNum);
        GlobalStateMgr.getCurrentState().getEditLog().logModifyPartition(info, wal -> {
            partitionInfo.setReplicationNum(partition.getId(), replicationNum);
            // update table default replication num
            table.setReplicationNum(replicationNum);
        });
        LOG.info("modify partition[{}-{}-{}] replication num to {}", db.getOriginName(), table.getName(),
                partition.getName(), replicationNum);
    }

    /**
     * Set default replication number for a specified table.
     * You can see the default replication number by Show Create Table stmt.
     *
     * @param db
     * @param table
     * @param properties
     */
    // The caller need to hold the db write lock
    public void modifyTableDefaultReplicationNum(Database db, OlapTable table, Map<String, String> properties)
            throws DdlException {
        Locker locker = new Locker();
        Preconditions.checkArgument(locker.isDbWriteLockHeldByCurrentThread(db));
        if (colocateTableIndex.isColocateTable(table.getId())) {
            throw new DdlException("table " + table.getName() + " is colocate table, cannot change replicationNum");
        }

        // check unpartitioned table
        PartitionInfo partitionInfo = table.getPartitionInfo();
        Partition partition = null;
        boolean isUnpartitionedTable = (partitionInfo.getType() == PartitionType.UNPARTITIONED);
        if (isUnpartitionedTable) {
            String partitionName = table.getName();
            partition = table.getPartition(partitionName);
            if (partition == null) {
                throw new DdlException("Partition does not exist. name: " + partitionName);
            }
        }

        short replicationNum = Short.parseShort(properties.getOrDefault(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM,
                String.valueOf(RunMode.defaultReplicationNum())));
        TableProperty tableProperty = table.getTableProperty();

        // log
        ModifyTablePropertyOperationLog info =
                new ModifyTablePropertyOperationLog(db.getId(), table.getId(), properties);
        final Partition finalPartition = partition;
        GlobalStateMgr.getCurrentState().getEditLog().logModifyReplicationNum(info, wal -> {
            tableProperty.modifyTableProperties(properties);
            tableProperty.setReplicationNum(replicationNum);
            // update partition replication num if this table is unpartitioned table
            if (isUnpartitionedTable) {
                partitionInfo.setReplicationNum(finalPartition.getId(), replicationNum);
            }
        });
        LOG.info("modify table[{}] replication num to {}", table.getName(),
                properties.get(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM));
    }

    // The property must be checked before this call
    public void modifyTableEnablePersistentIndexMeta(Database db, OlapTable table, Map<String, String> properties) {
        Locker locker = new Locker();
        Preconditions.checkArgument(locker.isDbWriteLockHeldByCurrentThread(db));
        TableProperty tableProperty = table.getTableProperty();

        ModifyTablePropertyOperationLog info =
                new ModifyTablePropertyOperationLog(db.getId(), table.getId(), properties);
        GlobalStateMgr.getCurrentState().getEditLog().logModifyEnablePersistentIndex(info, wal -> {
            tableProperty.modifyTableProperties(properties);
            tableProperty.buildEnablePersistentIndex();
            if (table.isCloudNativeTable()) {
                // now default to CLOUD_NATIVE
                tableProperty.buildPersistentIndexType();
            }
        });
    }

    // The property must be checked before this call
    public void modifyFlatJsonMeta(Database db, OlapTable table, FlatJsonConfig flatJsonConfig) {
        Locker locker = new Locker();
        Preconditions.checkArgument(locker.isDbWriteLockHeldByCurrentThread(db));

        ModifyTablePropertyOperationLog info = new ModifyTablePropertyOperationLog(
                db.getId(),
                table.getId(),
                flatJsonConfig.toProperties()
        );
        GlobalStateMgr.getCurrentState().getEditLog().logModifyFlatJsonConfig(info, wal -> {
            table.setFlatJsonConfig(flatJsonConfig);
        });
    }

    public void modifyBinlogMeta(Database db, OlapTable table, BinlogConfig binlogConfig) {
        Locker locker = new Locker();
        Preconditions.checkArgument(locker.isDbWriteLockHeldByCurrentThread(db));
        ModifyTablePropertyOperationLog log = new ModifyTablePropertyOperationLog(
                db.getId(),
                table.getId(),
                binlogConfig.toProperties());
        GlobalStateMgr.getCurrentState().getEditLog().logModifyBinlogConfig(log, wal -> {
            if (!binlogConfig.getBinlogEnable()) {
                table.clearBinlogAvailableVersion();
                table.setBinlogTxnId(BinlogConfig.INVALID);
            }
            table.setCurBinlogConfig(binlogConfig);
        });
    }

    public void modifyTableConstraint(Database db,
                                      OlapTable olapTable,
                                      Map<String, String> properties) {
        TableProperty tableProperty = olapTable.getTableProperty();
        ModifyTablePropertyOperationLog info =
                new ModifyTablePropertyOperationLog(db.getId(), olapTable.getId(), properties);
        GlobalStateMgr.getCurrentState().getEditLog().logModifyConstraint(info, wal -> {
            tableProperty.modifyTableProperties(properties);
            tableProperty.buildConstraint();
        });
    }

    // The caller need to hold the db write lock
    public void modifyTableWriteQuorum(Database db, OlapTable table, Map<String, String> properties) {
        Locker locker = new Locker();
        Preconditions.checkArgument(locker.isDbWriteLockHeldByCurrentThread(db));
        TableProperty tableProperty = table.getTableProperty();

        ModifyTablePropertyOperationLog info =
                new ModifyTablePropertyOperationLog(db.getId(), table.getId(), properties);
        GlobalStateMgr.getCurrentState().getEditLog().logModifyWriteQuorum(info, wal -> {
            tableProperty.modifyTableProperties(properties);
            tableProperty.buildWriteQuorum();
        });
    }

    // The caller need to hold the db write lock
    public void modifyTableReplicatedStorage(Database db, OlapTable table, Map<String, String> properties) {
        Locker locker = new Locker();
        Preconditions.checkArgument(locker.isDbWriteLockHeldByCurrentThread(db));
        TableProperty tableProperty = table.getTableProperty();

        ModifyTablePropertyOperationLog info =
                new ModifyTablePropertyOperationLog(db.getId(), table.getId(), properties);
        GlobalStateMgr.getCurrentState().getEditLog().logModifyReplicatedStorage(info, wal -> {
            tableProperty.modifyTableProperties(properties);
            tableProperty.buildReplicatedStorage();
        });
    }

    // The caller need to hold the db write lock
    public void modifyTableAutomaticBucketSize(Database db, OlapTable table, Map<String, String> properties) {
        Locker locker = new Locker();
        Preconditions.checkArgument(locker.isDbWriteLockHeldByCurrentThread(db));
        TableProperty tableProperty = table.getTableProperty();

        ModifyTablePropertyOperationLog info =
                new ModifyTablePropertyOperationLog(db.getId(), table.getId(), properties);
        GlobalStateMgr.getCurrentState().getEditLog().logModifyBucketSize(info, wal -> {
            tableProperty.modifyTableProperties(properties);
            tableProperty.buildBucketSize();
        });
    }

    // The property must be checked before this call
    public void modifyTableMutableBucketNum(Database db, OlapTable table, Map<String, String> properties) {
        Locker locker = new Locker();
        Preconditions.checkArgument(locker.isDbWriteLockHeldByCurrentThread(db));
        TableProperty tableProperty = table.getTableProperty();

        ModifyTablePropertyOperationLog info = new ModifyTablePropertyOperationLog(db.getId(), table.getId(),
                properties);
        GlobalStateMgr.getCurrentState().getEditLog().logModifyMutableBucketNum(info, wal -> {
            tableProperty.modifyTableProperties(properties);
            tableProperty.buildMutableBucketNum();
        });
    }

    // The property must be checked before this call
    public void modifyTableEnableLoadProfile(Database db, OlapTable table, Map<String, String> properties) {
        Locker locker = new Locker();
        Preconditions.checkArgument(locker.isDbWriteLockHeldByCurrentThread(db));
        TableProperty tableProperty = table.getTableProperty();
        ModifyTablePropertyOperationLog info =
                new ModifyTablePropertyOperationLog(db.getId(), table.getId(), properties);
        GlobalStateMgr.getCurrentState().getEditLog().logModifyEnableLoadProfile(info, wal -> {
            tableProperty.modifyTableProperties(properties);
            tableProperty.buildEnableLoadProfile();
        });
    }

    // The property must be checked before this call
    public void modifyTableBaseCompactionForbiddenTimeRanges(Database db, OlapTable table, Map<String, String> properties) {
        Locker locker = new Locker();
        Preconditions.checkArgument(locker.isDbWriteLockHeldByCurrentThread(db));
        TableProperty tableProperty = table.getTableProperty();
        ModifyTablePropertyOperationLog info = new ModifyTablePropertyOperationLog(db.getId(), table.getId(),
                properties);
        GlobalStateMgr.getCurrentState().getEditLog().logModifyBaseCompactionForbiddenTimeRanges(info, wal -> {
            tableProperty.modifyTableProperties(properties);
            tableProperty.buildBaseCompactionForbiddenTimeRanges();
        });
    }

    public void modifyTablePrimaryIndexCacheExpireSec(Database db, OlapTable table, Map<String, String> properties) {
        Locker locker = new Locker();
        Preconditions.checkArgument(locker.isDbWriteLockHeldByCurrentThread(db));
        TableProperty tableProperty = table.getTableProperty();
        ModifyTablePropertyOperationLog info = new ModifyTablePropertyOperationLog(db.getId(), table.getId(),
                properties);
        GlobalStateMgr.getCurrentState().getEditLog().logModifyPrimaryIndexCacheExpireSec(info, wal -> {
            tableProperty.modifyTableProperties(properties);
            tableProperty.buildPrimaryIndexCacheExpireSec();
        });
    }

    public void modifyTableMeta(Database db, OlapTable table, Map<String, String> properties,
                                TTabletMetaType metaType) {
        if (metaType == TTabletMetaType.ENABLE_PERSISTENT_INDEX) {
            modifyTableEnablePersistentIndexMeta(db, table, properties);
        } else if (metaType == TTabletMetaType.WRITE_QUORUM) {
            modifyTableWriteQuorum(db, table, properties);
        } else if (metaType == TTabletMetaType.REPLICATED_STORAGE) {
            modifyTableReplicatedStorage(db, table, properties);
        } else if (metaType == TTabletMetaType.BUCKET_SIZE) {
            modifyTableAutomaticBucketSize(db, table, properties);
        } else if (metaType == TTabletMetaType.MUTABLE_BUCKET_NUM) {
            modifyTableMutableBucketNum(db, table, properties);
        } else if (metaType == TTabletMetaType.BASE_COMPACTION_FORBIDDEN_TIME_RANGES) {
            modifyTableBaseCompactionForbiddenTimeRanges(db, table, properties);
        } else if (metaType == TTabletMetaType.PRIMARY_INDEX_CACHE_EXPIRE_SEC) {
            modifyTablePrimaryIndexCacheExpireSec(db, table, properties);
        } else if (metaType == TTabletMetaType.ENABLE_LOAD_PROFILE) {
            modifyTableEnableLoadProfile(db, table, properties);
        }
    }

    public void setHasForbiddenGlobalDict(String dbName, String tableName, boolean isForbit) throws DdlException {
        Map<String, String> property = new HashMap<>();
        Database db = getDb(dbName);
        if (db == null) {
            throw new DdlException("the DB " + dbName + " is not exist");
        }
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.READ);
        try {
            Table table = getTable(db.getFullName(), tableName);
            if (table == null) {
                throw new DdlException("the DB " + dbName + " table: " + tableName + "isn't  exist");
            }

            if (table instanceof OlapTable olapTable) {
                if (isForbit) {
                    property.put(PropertyAnalyzer.ENABLE_LOW_CARD_DICT_TYPE, PropertyAnalyzer.DISABLE_LOW_CARD_DICT);
                } else {
                    property.put(PropertyAnalyzer.ENABLE_LOW_CARD_DICT_TYPE, PropertyAnalyzer.ABLE_LOW_CARD_DICT);
                }
                ModifyTablePropertyOperationLog info =
                        new ModifyTablePropertyOperationLog(db.getId(), table.getId(), property);
                GlobalStateMgr.getCurrentState().getEditLog().logSetHasForbiddenGlobalDict(info, wal -> {
                    olapTable.setHasForbiddenGlobalDict(isForbit);
                    if (isForbit) {
                        IDictManager.getInstance().disableGlobalDict(olapTable.getId());
                    } else {
                        IDictManager.getInstance().enableGlobalDict(olapTable.getId());
                    }
                });
            }
        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
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

    public void replayModifyColumnComment(short opCode, ModifyColumnCommentLog info) {
        OlapTable table = (OlapTable) getTable(info.getDbId(), info.getTableId());
        if (table == null) {
            return;
        }
        Locker locker = new Locker();
        locker.lockTableWithIntensiveDbLock(info.getDbId(), info.getTableId(), LockType.WRITE);
        try {
            Column olapColumn = table.getColumn(info.getColumnName());
            if (olapColumn != null) {
                olapColumn.setComment(info.getComment());
            }
        } finally {
            locker.unLockTableWithIntensiveDbLock(info.getDbId(), info.getTableId(), LockType.WRITE);
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
            if (opCode == OperationType.OP_MODIFY_DEFAULT_BUCKET_NUM) {
                if (olapTable != null &&
                        olapTable.getDefaultDistributionInfo() instanceof com.starrocks.catalog.HashDistributionInfo) {
                    String bucketNumStr = properties.get("default_bucket_num");
                    if (bucketNumStr != null) {
                        int bucketNum = Integer.parseInt(bucketNumStr);
                        ((com.starrocks.catalog.HashDistributionInfo) olapTable.getDefaultDistributionInfo())
                                .setBucketNum(bucketNum);
                        LOG.info("Replay OP_MODIFY_DEFAULT_BUCKET_NUM: set table {} default bucket num to {}",
                                tableId, bucketNum);
                    }
                }
            } else if (opCode == OperationType.OP_SET_FORBIDDEN_GLOBAL_DICT) {
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
            } else if (opCode == OperationType.OP_SET_HAS_DELETE) {
                olapTable.setHasDelete();
            } else {
                TableProperty tableProperty = olapTable.getTableProperty();
                tableProperty.modifyTableProperties(properties);
                tableProperty.buildProperty(opCode);

                if (StringUtils.isNotEmpty(comment)) {
                    olapTable.setComment(comment);
                }

                // need to replay partition info meta
                if (opCode == OperationType.OP_MODIFY_REPLICATION_NUM) {
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
                } else if (opCode == OperationType.OP_MODIFY_FLAT_JSON_CONFIG) {
                    // buildFlatJsonConfig is already called in buildProperty(opCode) above,
                    // and it will build the config from properties. But we need to ensure
                    // the config is set on the table after building.
                    FlatJsonConfig config = tableProperty.getFlatJsonConfig();
                    if (config != null) {
                        olapTable.setFlatJsonConfig(config);
                    }
                } else if (opCode == OperationType.OP_MODIFY_BASE_COMPACTION_FORBIDDEN_TIME_RANGES) {
                    GlobalStateMgr.getCurrentState().getCompactionControlScheduler().updateTableForbiddenTimeRanges(
                            tableId, tableProperty.getBaseCompactionForbiddenTimeRanges());
                }
            }
        } catch (Exception ex) {
            LOG.warn("The replay log failed and this log was ignored.", ex);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
    }

    public void replayAlterMaterializedViewProperties(ModifyTablePropertyOperationLog log) {
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
            tableProperty.modifyTableProperties(properties);
            tableProperty.buildMvProperties();

            if (properties.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_RETENTION_CONDITION)) {
                mv.analyzeAndSetMVRetentionCondition(new ConnectContext());
            }
            if (properties.containsKey(PropertyAnalyzer.PROPERTIES_MV_REFRESH_MODE)) {
                mv.setCurrentRefreshMode(MaterializedView.RefreshMode.valueOf(
                        tableProperty.getMvRefreshMode().toUpperCase(Locale.ROOT)));
            }
            if (properties.containsKey(PropertyAnalyzer.PROPERTIES_MV_REWRITE_STALENESS_SECOND)) {
                mv.setMaxMVRewriteStaleness(
                        Integer.parseInt(properties.get(PropertyAnalyzer.PROPERTIES_MV_REWRITE_STALENESS_SECOND)));
            }
            if (properties.containsKey(PropertyAnalyzer.PROPERTIES_WAREHOUSE)) {
                String warehouseName = properties.get(PropertyAnalyzer.PROPERTIES_WAREHOUSE);
                Warehouse warehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouse(warehouseName);
                mv.setWarehouseId(warehouse.getId());
            }
        } catch (Throwable e) {
            mv.setInactiveAndReason("replay failed: " + e.getMessage());
            LOG.warn("replay alter materialized-view properties failed: {}", mv.getName(), e);
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(mv.getId()), LockType.WRITE);
        }
    }

    @Override
    public void createView(ConnectContext context, CreateViewStmt stmt) throws DdlException {
        createView(stmt);
    }

    public void createView(CreateViewStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();
        String tableName = stmt.getTable();

        // check if db exists
        Database db = this.getDb(stmt.getDbName());
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }
        // check if table exists in db
        boolean existed = false;
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.READ);
        try {
            if (getTable(db.getFullName(), tableName) != null) {
                existed = true;
                if (stmt.isSetIfNotExists()) {
                    LOG.info("create view[{}] which already exists", tableName);
                    return;
                } else if (stmt.isReplace()) {
                    LOG.info("view {} already exists, need to replace it", tableName);
                } else {
                    ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
                }
            }
        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
        }

        if (existed) {
            // already existed, need to alter the view
            AlterViewStmt alterViewStmt = AlterViewStmt.fromReplaceStmt(stmt);
            new AlterJobExecutor().process(alterViewStmt, ConnectContext.get());
            LOG.info("replace view {} successfully", tableName);
        } else {
            List<Column> columns = stmt.getColumns();
            long tableId = getNextId();
            View view = new View(tableId, tableName, columns);
            try {
                view.checkInlineViewDef(stmt.getInlineViewDef(), ConnectContext.get().getSessionVariable().getSqlMode());
            } catch (StarRocksException e) {
                throw new DdlException("failed to check inline view def", e);
            }

            view.setComment(stmt.getComment());
            view.setInlineViewDefWithSqlMode(stmt.getInlineViewDef(),
                    ConnectContext.get().getSessionVariable().getSqlMode());

            if (stmt.isSecurity()) {
                view.setSecurity(stmt.isSecurity());
            }
            onCreate(db, view, "", stmt.isSetIfNotExists());
            LOG.info("successfully create view[" + tableName + "-" + view.getId() + "]");
        }
    }

    public void replayUpdateClusterAndBackends(BackendIdsUpdateInfo info) {
        for (long id : info.getBackendList()) {
            final Backend backend = stateMgr.getNodeMgr().getClusterInfo().getBackend(id);
            backend.setDecommissioned(false);
            backend.setBackendState(Backend.BackendState.free);
        }
    }

    /**
     * Validates that a table exists (by ID), is an OLAP/Cloud Native table, and is in NORMAL state.
     * This method should be called while holding a lock on the table.
     * Supports both permanent tables and session-specific temporary tables.
     *
     * @param context the connection context (used for session-aware table lookup)
     * @param db the database containing the table
     * @param tableId the ID of the table to validate
     * @param dbTbl the table name (used for lookup and error messages)
     * @return the validated OlapTable
     * @throws DdlException if validation fails (table not found, wrong type, wrong state, or table replaced)
     */
    private OlapTable validateTableForTruncate(ConnectContext context, Database db, long tableId, TableName dbTbl)
            throws DdlException {
        Table table;
        try {
            table = MetaUtils.getSessionAwareTable(context, db, dbTbl);
        } catch (SemanticException exception) {
            throw new DdlException(exception.getMessage());
        }
        // Check if the table has been replaced (dropped and recreated) during truncation
        if (table.getId() != tableId) {
            throw new DdlException("Table [" + dbTbl.getTbl() + "] has been modified during truncation, please retry");
        }
        if (!table.isOlapOrCloudNativeTable()) {
            throw new DdlException("Only support truncate OLAP table or LAKE table");
        }
        OlapTable olapTable = (OlapTable) table;
        if (olapTable.getState() != OlapTable.OlapTableState.NORMAL) {
            throw InvalidOlapTableStateException.of(olapTable.getState(), olapTable.getName());
        }
        return olapTable;
    }

    /*
     * Truncate specified table or partitions.
     * The main idea is:
     *
     * 1. using the same schema to create new table(partitions)
     * 2. use the new created table(partitions) to replace the old ones.
     *
     * if no partition specified, it will truncate all partitions of this table, including all temp partitions,
     * otherwise, it will only truncate those specified partitions.
     *
     */
    @Override
    public void truncateTable(TruncateTableStmt truncateTableStmt, ConnectContext context) throws DdlException {
        String dbName = truncateTableStmt.getDbName();
        if (dbName == null) {
            dbName = context.getDatabase();
        }
        String tableName = truncateTableStmt.getTblName();

        // check, and save some info which need to be checked again later
        Map<String, Partition> origPartitions = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        OlapTable copiedTbl;
        Database db = getDb(dbName);
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        TableRef tblRef = truncateTableStmt.getTblRef();
        TableName dbTbl = new TableName(dbName, tableName);
        boolean truncateEntireTable = tblRef.getPartitionDef() == null;

        // Get the table outside the lock to obtain the tableId for fine-grained locking.
        // Use session-aware lookup to support temporary tables.
        long tableId = MetaUtils.getSessionAwareTable(context, db, dbTbl).getId();
        Locker locker = new Locker();
        if (!locker.lockTableAndCheckDbExist(db, tableId, LockType.READ)) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }
        try {
            // Retrieve the table again under the lock, make sure the table still exists.
            OlapTable olapTable = validateTableForTruncate(context, db, tableId, dbTbl);
            if (!truncateEntireTable) {
                for (String partName : tblRef.getPartitionDef().getPartitionNames()) {
                    Partition partition = olapTable.getPartition(partName);
                    if (partition == null) {
                        throw new DdlException("Partition " + partName + " does not exist");
                    }

                    origPartitions.put(partName, partition);
                    GlobalStateMgr.getCurrentState().getAnalyzeMgr().recordDropPartition(partition.getId());
                }
            } else {
                for (Partition partition : olapTable.getPartitions()) {
                    origPartitions.put(partition.getName(), partition);
                    GlobalStateMgr.getCurrentState().getAnalyzeMgr().recordDropPartition(partition.getId());
                }
            }

            copiedTbl = AnalyzerUtils.getShadowCopyTable(olapTable);
        } finally {
            locker.unLockTableWithIntensiveDbLock(db.getId(), tableId, LockType.READ);
        }

        // 2. use the copied table to create partitions outside the lock
        List<Partition> newPartitions = Lists.newArrayListWithCapacity(origPartitions.size());
        // tabletIdSet to save all newly created tablet ids.
        Set<Long> tabletIdSet = Sets.newHashSet();
        try {
            for (Map.Entry<String, Partition> entry : origPartitions.entrySet()) {
                long oldPartitionId = entry.getValue().getId();
                long newPartitionId = getNextId();
                String newPartitionName = entry.getKey();

                PartitionInfo partitionInfo = copiedTbl.getPartitionInfo();
                partitionInfo.setReplicationNum(newPartitionId, partitionInfo.getReplicationNum(oldPartitionId));
                partitionInfo.setDataProperty(newPartitionId, partitionInfo.getDataProperty(oldPartitionId));

                if (copiedTbl.isCloudNativeTable()) {
                    partitionInfo.setDataCacheInfo(newPartitionId,
                            partitionInfo.getDataCacheInfo(oldPartitionId));
                }

                copiedTbl.setDefaultDistributionInfo(entry.getValue().getDistributionInfo());

                Partition newPartition =
                        createPartition(db, copiedTbl, newPartitionId, newPartitionName, null, tabletIdSet,
                                ConnectContext.get().getCurrentComputeResource());
                newPartitions.add(newPartition);
            }
            final ConnectContext connectContext = ConnectContext.get();
            final ComputeResource computeResource = connectContext.getCurrentComputeResource();
            buildPartitions(db, copiedTbl, newPartitions.stream().map(Partition::getSubPartitions)
                    .flatMap(p -> p.stream()).collect(Collectors.toList()), computeResource);
        } catch (DdlException e) {
            deleteUselessTablets(tabletIdSet);
            throw e;
        }
        Preconditions.checkState(origPartitions.size() == newPartitions.size());

        // all partitions are created successfully, try to replace the old partitions.
        // before replacing, we need to check again.
        if (!locker.lockTableAndCheckDbExist(db, tableId, LockType.WRITE)) {
            // The db could be dropped during the unlock-READ and lock-WRITE.
            deleteUselessTablets(tabletIdSet);
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }
        try {
            // The table could also be changed during the unlock-READ and lock-WRITE.
            OlapTable olapTable = validateTableForTruncate(context, db, tableId, dbTbl);
            // check partitions
            for (Map.Entry<String, Partition> entry : origPartitions.entrySet()) {
                Partition partition = olapTable.getPartition(entry.getValue().getId());
                if (partition == null || !partition.getName().equalsIgnoreCase(entry.getKey())) {
                    throw new DdlException("Partition [" + entry.getKey() + "] is changed during truncating table, " +
                            "please retry");
                }
            }

            // check if meta changed
            // rollup index may be added or dropped, and schema may be changed during creating partition operation.
            boolean metaChanged = false;
            if (olapTable.getIndexNameToMetaId().size() != copiedTbl.getIndexNameToMetaId().size()) {
                metaChanged = true;
            } else {
                // compare schemaHash
                Map<Long, Integer> copiedIndexMetaIdToSchemaHash = copiedTbl.getIndexMetaIdToSchemaHash();
                for (Map.Entry<Long, Integer> entry : olapTable.getIndexMetaIdToSchemaHash().entrySet()) {
                    long indexMetaId = entry.getKey();
                    if (!copiedIndexMetaIdToSchemaHash.containsKey(indexMetaId)) {
                        metaChanged = true;
                        break;
                    }
                    if (!copiedIndexMetaIdToSchemaHash.get(indexMetaId).equals(entry.getValue())) {
                        metaChanged = true;
                        break;
                    }
                }
            }

            if (olapTable.getDefaultDistributionInfo().getType() != copiedTbl.getDefaultDistributionInfo().getType()) {
                metaChanged = true;
            }

            if (metaChanged) {
                throw new DdlException("Table[" + copiedTbl.getName() + "]'s meta has been changed. try again.");
            }

            // write edit log
            TruncateTableInfo info = new TruncateTableInfo(db.getId(), olapTable.getId(), newPartitions,
                    truncateEntireTable);
            GlobalStateMgr.getCurrentState().getEditLog().logTruncateTable(info, wal -> {
                // replace
                truncateTableInternal(db.getId(), olapTable, newPartitions, truncateEntireTable, false);
                try {
                    colocateTableIndex.updateLakeTableColocationInfo(olapTable, true /* isJoin */,
                            null /* expectGroupId */);
                } catch (DdlException e) {
                    LOG.info("table {} update colocation info failed when truncate table, {}", olapTable.getId(), e.getMessage());
                }
            });

            // refresh mv
            Set<MvId> relatedMvs = olapTable.getRelatedMaterializedViews();
            for (MvId mvId : relatedMvs) {
                MaterializedView materializedView = (MaterializedView) getTable(mvId.getDbId(), mvId.getId());
                if (materializedView == null) {
                    LOG.warn("Table related materialized view {}.{} can not be found", mvId.getDbId(), mvId.getId());
                    continue;
                }
                if (materializedView.isLoadTriggeredRefresh()) {
                    Database mvDb = getDb(mvId.getDbId());
                    refreshMaterializedView(mvDb.getFullName(), getTable(mvDb.getId(), mvId.getId()).getName(), false, null,
                            Constants.TaskRunPriority.NORMAL.value(), true, false);
                }
            }
        } catch (DdlException e) {
            deleteUselessTablets(tabletIdSet);
            throw e;
        } catch (MetaNotFoundException e) {
            LOG.warn("Table related materialized view can not be found", e);
        } finally {
            locker.unLockTableWithIntensiveDbLock(db.getId(), tableId, LockType.WRITE);
        }

        LOG.info("finished to truncate table {}, partitions: {}",
                tblRef.getTableName(), tblRef.getPartitionDef() != null ? tblRef.getPartitionDef().getPartitionNames() : null);
    }

    private void deleteUselessTablets(Set<Long> tabletIdSet) {
        // create partition failed, remove all newly created tablets.
        // For lakeTable, shards cleanup is taken care in ShardDeleter.
        for (Long tabletId : tabletIdSet) {
            GlobalStateMgr.getCurrentState().getTabletInvertedIndex().deleteTablet(tabletId);
        }
    }

    protected void truncateTableInternal(long dbId, OlapTable olapTable, List<Partition> newPartitions,
                                       boolean isEntireTable, boolean isReplay) {
        // use new partitions to replace the old ones.
        Set<Tablet> oldTablets = Sets.newHashSet();
        for (Partition newPartition : newPartitions) {
            Partition oldPartition = olapTable.replacePartition(dbId, newPartition);
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
            truncateTableInternal(info.getDbId(), olapTable, info.getPartitions(), info.isEntireTable(), true);

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
                            TabletMeta tabletMeta = new TabletMeta(db.getId(), olapTable.getId(),
                                    physicalPartition.getId(), mIndex.getId(), medium,
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

    public void replayBackendTabletsInfo(BackendTabletsInfo backendTabletsInfo) {
        List<Pair<Long, Integer>> tabletsWithSchemaHash = backendTabletsInfo.getTabletSchemaHash();
        if (!tabletsWithSchemaHash.isEmpty()) {
            // In previous version, we save replica info in `tabletsWithSchemaHash`,
            // but it is wrong because we can not get replica from `tabletInvertedIndex` when doing checkpoint,
            // because when doing checkpoint, the tabletInvertedIndex is not initialized at all.
            //
            // So we can only discard this information, in this case, it is equivalent to losing the record of these operations.
            // But it doesn't matter, these records are currently only used to record whether a replica is in a bad state.
            // This state has little effect on the system, and it can be restored after the system has processed the bad state
            // replica.
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

    public void replayConvertDistributionType(TableInfo tableInfo) {
        // nothing to do, for compatibility
    }

    /*
     * The entry of replacing partitions with temp partitions.
     */
    public void replaceTempPartition(Database db, String tableName, ReplacePartitionClause clause) throws DdlException {
        List<String> partitionNames = clause.getPartitionNames();
        // duplicate temp partition will cause Incomplete transaction
        List<String> tempPartitionNames =
                clause.getTempPartitionNames().stream().distinct().collect(Collectors.toList());

        boolean isStrictRange = clause.isStrictRange();
        boolean useTempPartitionName = clause.useTempPartitionName();
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            Table table = getTable(db.getFullName(), tableName);
            if (table == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName);
            }

            if (!table.isOlapOrCloudNativeTable()) {
                throw new DdlException("Table[" + tableName + "] is not OLAP table or LAKE table");
            }

            OlapTable olapTable = (OlapTable) table;
            // check partition exist
            for (String partName : partitionNames) {
                if (!olapTable.checkPartitionNameExist(partName, false)) {
                    throw new DdlException("Partition[" + partName + "] does not exist");
                }
            }
            for (String partName : tempPartitionNames) {
                if (!olapTable.checkPartitionNameExist(partName, true)) {
                    throw new DdlException("Temp partition[" + partName + "] does not exist");
                }
            }

            partitionNames.stream().forEach(e ->
                    GlobalStateMgr.getCurrentState().getAnalyzeMgr().recordDropPartition(olapTable.getPartition(e).getId()));
            olapTable.replaceTempPartitions(db.getId(), partitionNames, tempPartitionNames, isStrictRange, useTempPartitionName);

            // write log
            ReplacePartitionOperationLog info = new ReplacePartitionOperationLog(db.getId(), olapTable.getId(),
                    partitionNames, tempPartitionNames, isStrictRange, useTempPartitionName);
            GlobalStateMgr.getCurrentState().getEditLog().logReplaceTempPartition(info);
            LOG.info("finished to replace partitions {} with temp partitions {} from table: {}",
                    clause.getPartitionNames(), clause.getTempPartitionNames(), tableName);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
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
                olapTable.replacePartition(db.getId(), replaceTempPartitionLog.getPartitions().get(0),
                        replaceTempPartitionLog.getTempPartitions().get(0));
                return;
            }
            olapTable.replaceTempPartitions(db.getId(), replaceTempPartitionLog.getPartitions(),
                    replaceTempPartitionLog.getTempPartitions(),
                    replaceTempPartitionLog.isStrictRange(),
                    replaceTempPartitionLog.useTempPartitionName());
        } catch (DdlException e) {
            LOG.warn("should not happen.", e);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
        }
    }

    // entry of checking tablets operation
    public void checkTablets(AdminCheckTabletsStmt stmt) {
        AdminCheckTabletsStmt.CheckType type = stmt.getType();
        if (type == AdminCheckTabletsStmt.CheckType.CONSISTENCY) {
            stateMgr.getConsistencyChecker().addTabletsToCheck(stmt.getTabletIds());
        }
    }

    // Set specified replica's status. If replica does not exist, just ignore it.
    public void setReplicaStatus(AdminSetReplicaStatusStmt stmt) {
        long tabletId = stmt.getTabletId();
        long backendId = stmt.getBackendId();
        ReplicaStatus status = stmt.getStatus();
        setReplicaStatusInternal(tabletId, backendId, status);
    }

    private Pair<Replica, TabletMeta> getReplicaAndMeta(long tabletId, long backendId) {
        TabletMeta meta = stateMgr.getTabletInvertedIndex().getTabletMeta(tabletId);
        if (meta == null) {
            LOG.info("tablet {} does not exist", tabletId);
            return null;
        }
        Replica replica = stateMgr.getTabletInvertedIndex().getReplica(tabletId, backendId);
        if (replica == null) {
            LOG.info("replica of tablet {} does not exist", tabletId);
            return null;
        }
        return Pair.create(replica, meta);
    }

    private void setReplicaBadStatus(Replica replica, long tabletId, boolean isToSetBad) {
        replica.setBadForce(isToSetBad);
        LOG.info("set replica {} of tablet {} on backend {} as {}.",
                replica.getId(), tabletId, replica.getBackendId(),
                isToSetBad ? ReplicaStatus.BAD : ReplicaStatus.OK);
    }

    public void replaySetReplicaStatus(SetReplicaStatusOperationLog log) {
        long tabletId = log.getTabletId();
        long backendId = log.getBackendId();
        Pair<Replica, TabletMeta> pair = getReplicaAndMeta(tabletId, backendId);
        if (pair == null) {
            return;
        }
        Replica replica = pair.first;
        ReplicaStatus status = log.getReplicaStatus();
        if (status == ReplicaStatus.BAD || status == ReplicaStatus.OK) {
            boolean isToSetBad = (status == ReplicaStatus.BAD);
            if (replica.isBad() != isToSetBad) {
                setReplicaBadStatus(replica, tabletId, isToSetBad);
            }
        }
    }

    protected void setReplicaStatusInternal(long tabletId, long backendId, ReplicaStatus status) {
        Pair<Replica, TabletMeta> pair = getReplicaAndMeta(tabletId, backendId);
        if (pair == null) {
            return;
        }
        Replica replica = pair.first;
        TabletMeta meta = pair.second;
        if (status == ReplicaStatus.BAD || status == ReplicaStatus.OK) {
            boolean isToSetBad = (status == ReplicaStatus.BAD);
            if (replica.isBad() != isToSetBad) {
                SetReplicaStatusOperationLog log =
                        new SetReplicaStatusOperationLog(backendId, tabletId, status);
                GlobalStateMgr.getCurrentState().getEditLog().logSetReplicaStatus(log, wal -> {
                    setReplicaBadStatus(replica, tabletId, isToSetBad);
                });
                // Put this tablet into urgent table so that it can be repaired ASAP.
                stateMgr.getTabletChecker().setTabletForUrgentRepair(
                        meta.getDbId(), meta.getTableId(), meta.getPhysicalPartitionId());
            }
        }
    }

    public void setPartitionVersion(AdminSetPartitionVersionStmt stmt) {
        TableRef tableRef = stmt.getTableRef();
        Database database = getDb(tableRef.getDbName());
        if (database == null) {
            throw ErrorReportException.report(ErrorCode.ERR_BAD_DB_ERROR, tableRef.getDbName());
        }
        Locker locker = new Locker();
        locker.lockDatabase(database.getId(), LockType.WRITE);
        try {
            Table table = getTable(database.getFullName(), tableRef.getTableName());
            if (table == null) {
                throw ErrorReportException.report(ErrorCode.ERR_BAD_TABLE_ERROR, tableRef.getTableName());
            }
            if (!table.isOlapTableOrMaterializedView()) {
                throw ErrorReportException.report(ErrorCode.ERR_NOT_OLAP_TABLE, tableRef.getTableName());
            }

            PhysicalPartition physicalPartition;
            OlapTable olapTable = (OlapTable) table;
            if (stmt.getPartitionId() != -1) {
                physicalPartition = olapTable.getPhysicalPartition(stmt.getPartitionId());
                if (physicalPartition == null) {
                    throw ErrorReportException.report(ErrorCode.ERR_NO_SUCH_PARTITION, stmt.getPartitionName());
                }
            } else {
                Partition partition = olapTable.getPartition(stmt.getPartitionName());
                if (partition == null) {
                    throw ErrorReportException.report(ErrorCode.ERR_NO_SUCH_PARTITION, stmt.getPartitionName());
                }
                if (partition.getSubPartitions().size() >= 2) {
                    throw ErrorReportException.report(ErrorCode.ERR_MULTI_SUB_PARTITION, stmt.getPartitionName());
                }
                physicalPartition = partition.getDefaultPhysicalPartition();
            }

            long visibleVersionTime = System.currentTimeMillis();
            PartitionVersion partitionVersion = new PartitionVersion(
                    database.getId(), table.getId(), physicalPartition.getId(), stmt.getVersion());
            GlobalStateMgr.getCurrentState().getEditLog().logRecoverPartitionVersion(
                    new PartitionVersionRecoveryInfo(Lists.newArrayList(partitionVersion), visibleVersionTime), wal -> {
                        physicalPartition.setVisibleVersion(stmt.getVersion(), visibleVersionTime);
                        physicalPartition.setNextVersion(stmt.getVersion() + 1);
                        setBadForHighVersionReplica(physicalPartition, stmt.getVersion());
                    });
            LOG.info("Successfully set partition: {} version to {}, table: {}, db: {}",
                    stmt.getPartitionName(), stmt.getVersion(), table.getName(), database.getFullName());
        } finally {
            locker.unLockDatabase(database.getId(), LockType.WRITE);
        }
    }

    private void setBadForHighVersionReplica(PhysicalPartition physicalPartition, long version) {
        for (MaterializedIndex index : physicalPartition.getMaterializedIndices(IndexExtState.VISIBLE)) {
            for (Tablet tablet : index.getTablets()) {
                if (!(tablet instanceof LocalTablet localTablet)) {
                    continue;
                }

                for (Replica replica : localTablet.getAllReplicas()) {
                    if (replica.getVersion() > version && localTablet.getAllReplicas().size() > 1) {
                        replica.setBad(true);
                        LOG.warn("set tablet: {} on backend: {} to bad, " +
                                        "because its version: {} is higher than partition visible version: {}",
                                tablet.getId(), replica.getBackendId(), replica.getVersion(), version);
                    }
                }
            }
        }
    }

    public void onEraseDatabase(long dbId) {
        // remove database transaction manager
        stateMgr.getGlobalTransactionMgr().removeDatabaseTransactionMgr(dbId);
        // remove load jobs belonging to this database
        stateMgr.getLoadMgr().removeLoadJobsByDb(dbId);
        // unbind db to storage volume
        stateMgr.getStorageVolumeMgr().unbindDbToStorageVolume(dbId);
    }

    public void onErasePartition(Partition partition) {
        // remove tablet in inverted index
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        for (PhysicalPartition subPartition : partition.getSubPartitions()) {
            for (MaterializedIndex index : subPartition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                for (Tablet tablet : index.getTablets()) {
                    long tabletId = tablet.getId();
                    invertedIndex.deleteTablet(tabletId);
                }
            }
        }
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

    @VisibleForTesting
    public OlapTable getCopiedTable(Database db, OlapTable olapTable, List<Long> sourcePartitionIds,
                                    Map<Long, String> origPartitions, boolean isOptimize) {
        OlapTable copiedTbl;
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.READ);
        try {
            if (olapTable.getState() != OlapTable.OlapTableState.NORMAL
                    && olapTable.getState() != OlapTable.OlapTableState.OPTIMIZE) {
                throw new RuntimeException("Table' state is not NORMAL: " + olapTable.getState()
                        + ", tableId:" + olapTable.getId() + ", tabletName:" + olapTable.getName());
            }
            for (Long id : sourcePartitionIds) {
                origPartitions.put(id, olapTable.getPartition(id).getName());
            }
            copiedTbl = AnalyzerUtils.getShadowCopyTable(olapTable);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
        }
        return copiedTbl;
    }

    @VisibleForTesting
    public OlapTable getCopiedTable(Database db, OlapTable olapTable, List<Long> sourcePartitionIds,
                                    Map<Long, String> origPartitions) {
        return getCopiedTable(db, olapTable, sourcePartitionIds, origPartitions, false);
    }

    @VisibleForTesting
    public List<Partition> getNewPartitionsFromPartitions(Database db, OlapTable olapTable,
                                                          List<Long> sourcePartitionIds,
                                                          Map<Long, String> origPartitions, OlapTable copiedTbl,
                                                          String namePostfix, Set<Long> tabletIdSet,
                                                          List<Long> tmpPartitionIds, DistributionDesc distributionDesc,
                                                          ComputeResource computeResource)
            throws DdlException {
        List<Partition> newPartitions = Lists.newArrayListWithCapacity(sourcePartitionIds.size());
        for (int i = 0; i < sourcePartitionIds.size(); ++i) {
            long newPartitionId = tmpPartitionIds.get(i);
            long sourcePartitionId = sourcePartitionIds.get(i);
            String newPartitionName = origPartitions.get(sourcePartitionId) + namePostfix;
            if (olapTable.checkPartitionNameExist(newPartitionName, true)) {
                // to prevent creating the same partitions when failover
                // this will happen when OverwriteJob crashed after created temp partitions,
                // but before changing to PREPARED state
                LOG.warn("partition:{} already exists in table:{}", newPartitionName, olapTable.getName());
                continue;
            }
            PartitionInfo partitionInfo = copiedTbl.getPartitionInfo();
            partitionInfo.setReplicationNum(newPartitionId, partitionInfo.getReplicationNum(sourcePartitionId));
            partitionInfo.setDataProperty(newPartitionId, partitionInfo.getDataProperty(sourcePartitionId));

            if (copiedTbl.isCloudNativeTableOrMaterializedView()) {
                partitionInfo.setDataCacheInfo(newPartitionId, partitionInfo.getDataCacheInfo(sourcePartitionId));
            }

            Partition newPartition = null;
            if (distributionDesc != null) {
                DistributionInfo distributionInfo = DistributionInfoBuilder.build(distributionDesc, olapTable.getColumns());
                if (distributionInfo.getBucketNum() == 0) {
                    Partition sourcePartition = olapTable.getPartition(sourcePartitionId);
                    olapTable.optimizeDistribution(distributionInfo, sourcePartition);
                }
                newPartition = createPartition(
                        db, copiedTbl, newPartitionId, newPartitionName, null, tabletIdSet, distributionInfo, computeResource);
            } else {
                newPartition = createPartition(db, copiedTbl, newPartitionId, newPartitionName, null, tabletIdSet,
                        computeResource);
            }

            newPartitions.add(newPartition);
        }
        return newPartitions;
    }

    // create new partitions from source partitions.
    // new partitions have the same indexes as source partitions.
    public List<Partition> createTempPartitionsFromPartitions(Database db, Table table,
                                                              String namePostfix, List<Long> sourcePartitionIds,
                                                              List<Long> tmpPartitionIds, DistributionDesc distributionDesc,
                                                              ComputeResource computeResource) {
        Preconditions.checkState(table instanceof OlapTable);
        OlapTable olapTable = (OlapTable) table;
        Map<Long, String> origPartitions = Maps.newHashMap();
        OlapTable copiedTbl = getCopiedTable(db, olapTable, sourcePartitionIds, origPartitions, distributionDesc != null);
        copiedTbl.setDefaultDistributionInfo(olapTable.getDefaultDistributionInfo());

        // 2. use the copied table to create partitions
        List<Partition> newPartitions = null;
        // tabletIdSet to save all newly created tablet ids.
        Set<Long> tabletIdSet = Sets.newHashSet();
        try {
            newPartitions = getNewPartitionsFromPartitions(db, olapTable, sourcePartitionIds, origPartitions,
                    copiedTbl, namePostfix, tabletIdSet, tmpPartitionIds, distributionDesc, computeResource);
            buildPartitions(db, copiedTbl, newPartitions.stream().map(Partition::getSubPartitions)
                    .flatMap(p -> p.stream()).collect(Collectors.toList()), computeResource);
        } catch (Exception e) {
            // create partition failed, remove all newly created tablets
            for (Long tabletId : tabletIdSet) {
                GlobalStateMgr.getCurrentState().getTabletInvertedIndex().deleteTablet(tabletId);
            }
            LOG.warn("create partitions from partitions failed.", e);
            throw new RuntimeException("create partitions failed: " + e.getMessage(), e);
        }
        return newPartitions;
    }

    public void replayDeleteAutoIncrementId(AutoIncrementInfo info) throws IOException {
        for (Map.Entry<Long, Long> entry : info.tableIdToIncrementId().entrySet()) {
            Long tableId = entry.getKey();
            tableIdToIncrementId.remove(tableId);
        }
    }

    public void deleteAutoIncrementIdForTable(Long tableId) {
        tableIdToIncrementId.remove(tableId);
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
        Long endId = tableIdToIncrementId.compute(tableId, (k, val) -> {
            Long currentVal = val;
            if (currentVal == null) {
                currentVal = 1L;
            }

            Long newVal = currentVal + rows;
            if (newVal < currentVal) {
                throw new RuntimeException("AUTO_INCREMENT counter overflow");
            }

            // log the delta result.
            ConcurrentHashMap<Long, Long> deltaMap = new ConcurrentHashMap<>();
            deltaMap.put(tableId, newVal);
            AutoIncrementInfo info = new AutoIncrementInfo(deltaMap);
            GlobalStateMgr.getCurrentState().getEditLog().logSaveAutoIncrementId(info, wal -> {
                // noting to do, tableIdToIncrementId will be updated after the compute end.
            });
            return newVal;
        });

        return endId - rows;
    }

    public void removeAutoIncrementIdByTableId(Long tableId) {
        Long id = tableIdToIncrementId.get(tableId);

        if (id != null) {
            ConcurrentHashMap<Long, Long> deltaMap = new ConcurrentHashMap<>();
            deltaMap.put(tableId, 0L);
            AutoIncrementInfo info = new AutoIncrementInfo(deltaMap);
            GlobalStateMgr.getCurrentState().getEditLog()
                    .logSaveDeleteAutoIncrementId(info, wal -> tableIdToIncrementId.remove(tableId));
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

    public void checkDataSizeQuota(Database database) throws StarRocksException {
        long usedQuotaDataBytes = database.usedDataQuotaBytes.get();
        long dataQuotaBytes = database.getDataQuota();
        if (usedQuotaDataBytes >= dataQuotaBytes) {
            Pair<Double, String> quotaUnitPair = DebugUtil.getByteUint(dataQuotaBytes);
            String readableQuota = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(quotaUnitPair.first) + " " + quotaUnitPair.second;
            throw new StarRocksException("Database[" + database.getFullName() + "] data size exceeds quota["
                    + readableQuota + "]");
        }
    }

    public void checkReplicaQuota(Database database) throws StarRocksException {
        long usedReplicaQuotaBytes = database.usedReplicaQuotaBytes.get();
        long replicaQuotaSize = database.getReplicaQuota();
        if (usedReplicaQuotaBytes >= replicaQuotaSize) {
            throw new StarRocksException("Database[" + database.getFullName() + "] replica number exceeds quota[" +
                    replicaQuotaSize + "]");
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

    /**
     * When loading the LocalMetastore, the DB/Catalog that the materialized view depends
     * on may not have been loaded yet, so you cannot reload the materialized view.
     * The reload operation of a materialized view should be called by {@link GlobalStateMgr#postLoadImage()}.
     */
    public void load(SRMetaBlockReader reader) throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        reader.readCollection(Database.class, db -> {
            reader.readCollection(Table.class, db::registerTableUnlocked);

            idToDb.put(db.getId(), db);
            fullNameToDb.put(db.getFullName(), db);
            stateMgr.getGlobalTransactionMgr().addDatabaseTransactionMgr(db.getId());
        });

        AutoIncrementInfo autoIncrementInfo = reader.readJson(AutoIncrementInfo.class);
        for (Map.Entry<Long, Long> entry : autoIncrementInfo.tableIdToIncrementId().entrySet()) {
            Long tableId = entry.getKey();
            Long id = entry.getValue();

            tableIdToIncrementId.put(tableId, id);
        }

        recreateTabletInvertIndex();
        GlobalStateMgr.getCurrentState().getEsRepository().loadTableFromCatalog();
    }

    @Override
    public void handleMVRepair(Database db, Table table, List<MVRepairHandler.PartitionRepairInfo> partitionRepairInfos) {
        MVMetaVersionRepairer.repairBaseTableVersionChanges(table, partitionRepairInfos);
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

    public void alterTableAutoIncrement(String dbName, String tableName, long newAutoIncrementValue) throws DdlException {
        Table table = getTable(dbName, tableName);
        Long tableId = table.getId();
        Long currentValue = getCurrentAutoIncrementIdByTableId(tableId);

        if (currentValue != null && newAutoIncrementValue <= currentValue) {
            throw new DdlException("New auto_increment value must be greater than current value: " + currentValue);
        }

        if (!((OlapTable) table).sendDropAutoIncrementMapTask()) {
            throw new DdlException("Failed to drop auto increment cache in CN/BE");
        }

        ConcurrentHashMap<Long, Long> idMap = new ConcurrentHashMap<>();
        idMap.put(tableId, newAutoIncrementValue);
        AutoIncrementInfo info = new AutoIncrementInfo(idMap);
        stateMgr.getEditLog().logSaveAutoIncrementId(
                info, wal -> addOrReplaceAutoIncrementIdByTableId(tableId, newAutoIncrementValue));

        LOG.info("Set auto_increment value for table {}.{} to {}", dbName, tableName, newAutoIncrementValue);
    }
}
