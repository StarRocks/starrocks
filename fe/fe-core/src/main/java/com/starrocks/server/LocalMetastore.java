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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.staros.proto.FilePathInfo;
import com.starrocks.alter.AlterJobMgr;
import com.starrocks.analysis.ColumnDef;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.KeysDesc;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TableRef;
import com.starrocks.analysis.TypeDef;
import com.starrocks.binlog.BinlogConfig;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.CatalogRecycleBin;
import com.starrocks.catalog.CatalogUtils;
import com.starrocks.catalog.ColocateGroupSchema;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.DynamicPartitionProperty;
import com.starrocks.catalog.EsTable;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.ExternalOlapTable;
import com.starrocks.catalog.ForeignKeyConstraint;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.MysqlTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableIndexes;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.UniqueConstraint;
import com.starrocks.catalog.View;
import com.starrocks.catalog.system.information.InfoSchemaDb;
import com.starrocks.catalog.system.sys.SysDb;
import com.starrocks.cluster.Cluster;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeConstants;
import com.starrocks.common.FeMetaVersion;
import com.starrocks.common.MarkedCountDownLatch;
import com.starrocks.common.MaterializedViewExceptions;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.NotImplementedException;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.common.UserException;
import com.starrocks.common.util.DynamicPartitionUtil;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.Util;
import com.starrocks.common.util.concurrent.CountingLatch;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorTableInfo;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.lake.LakeMaterializedView;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.StorageCacheInfo;
import com.starrocks.lake.StorageInfo;
import com.starrocks.meta.MetaContext;
import com.starrocks.persist.AddPartitionsInfo;
import com.starrocks.persist.AddPartitionsInfoV2;
import com.starrocks.persist.AutoIncrementInfo;
import com.starrocks.persist.BackendIdsUpdateInfo;
import com.starrocks.persist.BackendTabletsInfo;
import com.starrocks.persist.ColocatePersistInfo;
import com.starrocks.persist.CreateDbInfo;
import com.starrocks.persist.DatabaseInfo;
import com.starrocks.persist.DropDbInfo;
import com.starrocks.persist.DropPartitionInfo;
import com.starrocks.persist.ListPartitionPersistInfo;
import com.starrocks.persist.ModifyPartitionInfo;
import com.starrocks.persist.ModifyTableColumnOperationLog;
import com.starrocks.persist.ModifyTablePropertyOperationLog;
import com.starrocks.persist.MultiEraseTableInfo;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.PartitionPersistInfo;
import com.starrocks.persist.PartitionPersistInfoV2;
import com.starrocks.persist.RangePartitionPersistInfo;
import com.starrocks.persist.RecoverInfo;
import com.starrocks.persist.ReplacePartitionOperationLog;
import com.starrocks.persist.ReplicaPersistInfo;
import com.starrocks.persist.SetReplicaStatusOperationLog;
import com.starrocks.persist.TableInfo;
import com.starrocks.persist.TruncateTableInfo;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.privilege.PrivilegeActions;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.VariableMgr;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.ExecuteOption;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.mv.MaterializedViewMgr;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.SetStmtAnalyzer;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.AddRollupClause;
import com.starrocks.sql.ast.AdminCheckTabletsStmt;
import com.starrocks.sql.ast.AdminSetReplicaStatusStmt;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AlterDatabaseQuotaStmt;
import com.starrocks.sql.ast.AlterDatabaseRenameStatement;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.AlterViewStmt;
import com.starrocks.sql.ast.AsyncRefreshSchemeDesc;
import com.starrocks.sql.ast.CancelAlterTableStmt;
import com.starrocks.sql.ast.ColumnRenameClause;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateMaterializedViewStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.DropMaterializedViewStmt;
import com.starrocks.sql.ast.DropPartitionClause;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.ExpressionPartitionDesc;
import com.starrocks.sql.ast.ListPartitionDesc;
import com.starrocks.sql.ast.MultiItemListPartitionDesc;
import com.starrocks.sql.ast.MultiRangePartitionDesc;
import com.starrocks.sql.ast.PartitionConvertContext;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.PartitionRangeDesc;
import com.starrocks.sql.ast.PartitionRenameClause;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.RangePartitionDesc;
import com.starrocks.sql.ast.RecoverDbStmt;
import com.starrocks.sql.ast.RecoverPartitionStmt;
import com.starrocks.sql.ast.RecoverTableStmt;
import com.starrocks.sql.ast.RefreshMaterializedViewStatement;
import com.starrocks.sql.ast.RefreshSchemeDesc;
import com.starrocks.sql.ast.ReplacePartitionClause;
import com.starrocks.sql.ast.RollupRenameClause;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetListItem;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.ShowAlterStmt;
import com.starrocks.sql.ast.SingleItemListPartitionDesc;
import com.starrocks.sql.ast.SingleRangePartitionDesc;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.ast.TableRenameClause;
import com.starrocks.sql.ast.TruncateTableStmt;
import com.starrocks.sql.common.SyncPartitionUtils;
import com.starrocks.sql.optimizer.CachingMvPlanContextBuilder;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.statistics.IDictManager;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.CreateReplicaTask;
import com.starrocks.thrift.TCompressionType;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TStorageFormat;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.thrift.TTabletMetaType;
import com.starrocks.thrift.TTabletType;
import com.starrocks.thrift.TTaskType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.hadoop.util.ThreadUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.threeten.extra.PeriodDuration;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;

import static com.starrocks.server.GlobalStateMgr.NEXT_ID_INIT_VALUE;
import static com.starrocks.server.GlobalStateMgr.isCheckpointThread;

public class LocalMetastore implements ConnectorMetadata {
    private static final Logger LOG = LogManager.getLogger(LocalMetastore.class);

    private final ConcurrentHashMap<Long, Database> idToDb = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Database> fullNameToDb = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, Long> tableIdToIncrementId = new ConcurrentHashMap<>();

    private Cluster defaultCluster;

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
    }

    private boolean tryLock(boolean mustLock) {
        return stateMgr.tryLock(mustLock);
    }

    private void unlock() {
        stateMgr.unlock();
    }

    private long getNextId() {
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
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentInvertedIndex();
        for (Database db : this.fullNameToDb.values()) {
            long dbId = db.getId();
            for (Table table : db.getTables()) {
                if (!table.isNativeTableOrMaterializedView()) {
                    continue;
                }

                OlapTable olapTable = (OlapTable) table;
                long tableId = olapTable.getId();
                Collection<Partition> allPartitions = olapTable.getAllPartitions();
                for (Partition partition : allPartitions) {
                    long partitionId = partition.getId();
                    TStorageMedium medium = olapTable.getPartitionInfo().getDataProperty(
                            partitionId).getStorageMedium();
                    for (MaterializedIndex index : partition
                            .getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                        long indexId = index.getId();
                        int schemaHash = olapTable.getSchemaHashByIndexId(indexId);
                        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, schemaHash, medium,
                                table.isCloudNativeTableOrMaterializedView());
                        for (Tablet tablet : index.getTablets()) {
                            long tabletId = tablet.getId();
                            invertedIndex.addTablet(tabletId, tabletMeta);
                            if (table.isOlapTableOrMaterializedView()) {
                                for (Replica replica : ((LocalTablet) tablet).getImmutableReplicas()) {
                                    invertedIndex.addReplica(tabletId, replica);
                                    if (MetaContext.get().getMetaVersion() < FeMetaVersion.VERSION_48) {
                                        // set replica's schema hash
                                        replica.setSchemaHash(schemaHash);
                                    }
                                }
                            }
                        }
                    } // end for indices
                } // end for partitions
            } // end for tables
        } // end for dbs
    }

    public long loadDb(DataInputStream dis, long checksum) throws IOException {
        int dbCount = dis.readInt();
        long newChecksum = checksum ^ dbCount;
        for (long i = 0; i < dbCount; ++i) {
            Database db = new Database();
            db.readFields(dis);
            newChecksum ^= db.getId();
            idToDb.put(db.getId(), db);
            fullNameToDb.put(db.getFullName(), db);
            stateMgr.getGlobalTransactionMgr().addDatabaseTransactionMgr(db.getId());
            for (Table tbl : ListUtils.union(db.getTables(), db.getHiveTables())) {
                try {
                    tbl.onCreate();
                } catch (Throwable e) {
                    LOG.error("reload table failed: {}", tbl, e);
                }
            }
        }
        LOG.info("finished replay databases from image");
        return newChecksum;
    }

    public long saveDb(DataOutputStream dos, long checksum) throws IOException {
        // Don't write system db meta
        Map<Long, Database> idToDbNormal = idToDb.entrySet().stream()
                .filter(entry -> entry.getKey() > NEXT_ID_INIT_VALUE)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        int dbCount = idToDbNormal.size();

        checksum ^= dbCount;
        dos.writeInt(dbCount);
        for (Map.Entry<Long, Database> entry : idToDbNormal.entrySet()) {
            Database db = entry.getValue();
            checksum ^= entry.getKey();
            db.readLock();
            try {
                db.write(dos);
            } finally {
                db.readUnlock();
            }
        }
        return checksum;
    }

    @Override
    public void createDb(String dbName) throws DdlException, AlreadyExistsException {
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
                unprotectCreateDb(db);
                GlobalStateMgr.getCurrentState().getEditLog().logCreateDb(db);
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
        db.writeLock();
        db.setExist(true);
        db.writeUnlock();
        final Cluster cluster = defaultCluster;
        cluster.addDb(db.getFullName(), db.getId());
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
            LOG.info("finish replay create db, name: {}, id: {}", db.getOriginName(), db.getId());
        } finally {
            unlock();
        }
    }

    @Override
    public void dropDb(String dbName, boolean isForceDrop) throws DdlException, MetaNotFoundException {
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
        } finally {
            unlock();
        }

        List<Runnable> runnableList;
        // 2. drop tables in db
        db.writeLock();
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
            runnableList = unprotectDropDb(db, isForceDrop, false);
            if (!isForceDrop) {
                recycleBin.recycleDatabase(db, tableNames);
            } else {
                stateMgr.onEraseDatabase(db.getId());
            }
            db.setExist(false);
            if (!fullNameToDb.containsKey(dbName)) {
                throw new MetaNotFoundException("Database not found");
            }

            // 3. remove db from globalStateMgr
            idToDb.remove(db.getId());
            fullNameToDb.remove(db.getFullName());
            final Cluster cluster = defaultCluster;
            cluster.removeDb(dbName, db.getId());

            // 4. drop mv task
            TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
            List<Long> dropTaskIdList = taskManager.showTasks(dbName)
                    .stream().map(Task::getId).collect(Collectors.toList());
            taskManager.dropTasks(dropTaskIdList, false);

            DropDbInfo info = new DropDbInfo(db.getFullName(), isForceDrop);
            GlobalStateMgr.getCurrentState().getEditLog().logDropDb(info);

            LOG.info("finish drop database[{}], id: {}, is force : {}", dbName, db.getId(), isForceDrop);
        } finally {
            db.writeUnlock();
        }

        for (Runnable runnable : runnableList) {
            runnable.run();
        }
    }

    @NotNull
    public List<Runnable> unprotectDropDb(Database db, boolean isForeDrop, boolean isReplay) {
        List<Runnable> runnableList = new ArrayList<>();
        for (Table table : db.getTables()) {
            Runnable runnable = db.unprotectDropTable(table.getId(), isForeDrop, isReplay);
            if (runnable != null) {
                runnableList.add(runnable);
            }
        }
        return runnableList;
    }

    public void replayDropDb(String dbName, boolean isForceDrop) throws DdlException {
        List<Runnable> runnableList;
        tryLock(true);
        try {
            Database db = fullNameToDb.get(dbName);
            db.writeLock();
            try {
                Set<String> tableNames = new HashSet(db.getTableNamesViewWithLock());
                runnableList = unprotectDropDb(db, isForceDrop, true);
                if (!isForceDrop) {
                    recycleBin.recycleDatabase(db, tableNames);
                } else {
                    stateMgr.onEraseDatabase(db.getId());
                }
                db.setExist(false);
            } finally {
                db.writeUnlock();
            }

            fullNameToDb.remove(dbName);
            idToDb.remove(db.getId());
            final Cluster cluster = defaultCluster;
            cluster.removeDb(dbName, db.getId());

            LOG.info("finish replay drop db, name: {}, id: {}", dbName, db.getId());
        } finally {
            unlock();
        }

        for (Runnable runnable : runnableList) {
            runnable.run();
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
            db.writeLock();
            db.setExist(true);
            db.writeUnlock();
            final Cluster cluster = defaultCluster;
            cluster.addDb(db.getFullName(), db.getId());

            List<MaterializedView> materializedViews = db.getMaterializedViews();
            TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
            for (MaterializedView materializedView : materializedViews) {
                MaterializedView.RefreshType refreshType = materializedView.getRefreshScheme().getType();
                if (refreshType != MaterializedView.RefreshType.SYNC) {
                    Task task = TaskBuilder.buildMvTask(materializedView, db.getFullName());
                    TaskBuilder.updateTaskInfo(task, materializedView);
                    taskManager.createTask(task, false);
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
        db.writeLock();
        try {
            Table table = db.getTable(tableName);
            if (table != null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
            }

            if (!recycleBin.recoverTable(db, tableName)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName);
            }

            Table recoverTable = db.getTable(tableName);
            if (recoverTable instanceof OlapTable) {
                DynamicPartitionUtil.registerOrRemovePartitionScheduleInfo(db.getId(), (OlapTable) recoverTable);
            }

        } finally {
            db.writeUnlock();
        }
    }

    public void recoverPartition(RecoverPartitionStmt recoverStmt) throws DdlException {
        String dbName = recoverStmt.getDbName();

        Database db = null;
        if ((db = getDb(dbName)) == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        String tableName = recoverStmt.getTableName();
        db.writeLock();
        try {
            Table table = db.getTable(tableName);
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
            db.writeUnlock();
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

        AlterDatabaseQuotaStmt.QuotaType quotaType = stmt.getQuotaType();
        if (quotaType == AlterDatabaseQuotaStmt.QuotaType.DATA) {
            db.setDataQuotaWithLock(stmt.getQuota());
        } else if (quotaType == AlterDatabaseQuotaStmt.QuotaType.REPLICA) {
            db.setReplicaQuotaWithLock(stmt.getQuota());
        }
        long quota = stmt.getQuota();
        DatabaseInfo dbInfo = new DatabaseInfo(db.getFullName(), "", quota, quotaType);
        GlobalStateMgr.getCurrentState().getEditLog().logAlterDb(dbInfo);
    }

    public void replayAlterDatabaseQuota(String dbName, long quota, AlterDatabaseQuotaStmt.QuotaType quotaType) {
        Database db = getDb(dbName);
        Preconditions.checkNotNull(db);
        if (quotaType == AlterDatabaseQuotaStmt.QuotaType.DATA) {
            db.setDataQuotaWithLock(quota);
        } else if (quotaType == AlterDatabaseQuotaStmt.QuotaType.REPLICA) {
            db.setReplicaQuotaWithLock(quota);
        }
    }

    public void renameDatabase(AlterDatabaseRenameStatement stmt) throws DdlException {
        String fullDbName = stmt.getDbName();
        String newFullDbName = stmt.getNewDbName();

        if (fullDbName.equals(newFullDbName)) {
            throw new DdlException("Same database name");
        }

        Database db;
        Cluster cluster;
        if (!tryLock(false)) {
            throw new DdlException("Failed to acquire globalStateMgr lock. Try again");
        }
        try {
            cluster = defaultCluster;
            if (cluster == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_NO_EXISTS, SystemInfoService.DEFAULT_CLUSTER);
            }
            // check if db exists
            db = fullNameToDb.get(fullDbName);
            if (db == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, fullDbName);
            }

            // check if name is already used
            if (fullNameToDb.get(newFullDbName) != null) {
                throw new DdlException("Database name[" + newFullDbName + "] is already used");
            }
            cluster.removeDb(db.getFullName(), db.getId());
            cluster.addDb(newFullDbName, db.getId());
            // 1. rename db
            db.setNameWithLock(newFullDbName);

            // 2. add to meta. check again
            fullNameToDb.remove(fullDbName);
            fullNameToDb.put(newFullDbName, db);

            DatabaseInfo dbInfo =
                    new DatabaseInfo(fullDbName, newFullDbName, -1L, AlterDatabaseQuotaStmt.QuotaType.NONE);
            GlobalStateMgr.getCurrentState().getEditLog().logDatabaseRename(dbInfo);
        } finally {
            unlock();
        }

        LOG.info("rename database[{}] to [{}], id: {}", fullDbName, newFullDbName, db.getId());
    }

    public void replayRenameDatabase(String dbName, String newDbName) {
        tryLock(true);
        try {
            Database db = fullNameToDb.get(dbName);
            Cluster cluster = defaultCluster;
            cluster.removeDb(db.getFullName(), db.getId());
            db.setName(newDbName);
            cluster.addDb(newDbName, db.getId());
            fullNameToDb.remove(dbName);
            fullNameToDb.put(newDbName, db);

            LOG.info("replay rename database {} to {}, id: {}", dbName, newDbName, db.getId());
        } finally {
            unlock();
        }
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
     * 6.4. storageFormat
     * 7. set index meta
     * 8. check colocation properties
     * 9. create tablet in BE
     * 10. add this table to FE's meta
     * 11. add this table to ColocateGroup if necessary
     *
     * @return whether the table is created
     */
    @Override
    public boolean createTable(CreateTableStmt stmt) throws DdlException {
        String engineName = stmt.getEngineName();
        String dbName = stmt.getDbName();
        String tableName = stmt.getTableName();

        // check if db exists
        Database db = getDb(stmt.getDbName());
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        // only internal table should check quota and cluster capacity
        if (!stmt.isExternal()) {
            // check cluster capacity
            GlobalStateMgr.getCurrentSystemInfo().checkClusterCapacity();
            // check db quota
            db.checkQuota();
        }

        // check if table exists in db
        db.readLock();
        try {
            if (db.getTable(tableName) != null) {
                if (stmt.isSetIfNotExists()) {
                    LOG.info("create table[{}] which already exists", tableName);
                } else {
                    ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
                }
                return false;
            }
        } finally {
            db.readUnlock();
        }

        if (stmt.isOlapEngine()) {
            createOlapOrLakeTable(db, stmt);
        } else if (engineName.equalsIgnoreCase("mysql")) {
            createMysqlTable(db, stmt);
        } else if (engineName.equalsIgnoreCase("elasticsearch") || engineName.equalsIgnoreCase("es")) {
            createEsTable(db, stmt);
        } else if (engineName.equalsIgnoreCase("hive")) {
            createHiveTable(db, stmt);
        } else if (engineName.equalsIgnoreCase("file")) {
            createFileTable(db, stmt);
        } else if (engineName.equalsIgnoreCase("iceberg")) {
            createIcebergTable(db, stmt);
        } else if (engineName.equalsIgnoreCase("hudi")) {
            createHudiTable(db, stmt);
        } else if (engineName.equalsIgnoreCase("jdbc")) {
            createJDBCTable(db, stmt);
        } else {
            ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_STORAGE_ENGINE, engineName);
        }
        return true;
    }

    @Override
    public void addPartitions(Database db, String tableName, AddPartitionClause addPartitionClause)
            throws DdlException, AnalysisException {
        db.readLock();
        Map<String, String> tableProperties;
        OlapTable olapTable;
        PartitionInfo partitionInfo;
        try {
            Table table = db.getTable(tableName);
            CatalogUtils.checkTableExist(db, tableName);
            CatalogUtils.checkNativeTable(db, table);
            olapTable = (OlapTable) table;
            tableProperties = olapTable.getTableProperty().getProperties();
            partitionInfo = olapTable.getPartitionInfo();
        } finally {
            db.readUnlock();
        }
        PartitionDesc partitionDesc = addPartitionClause.getPartitionDesc();
        if (partitionDesc instanceof SingleItemListPartitionDesc
                || partitionDesc instanceof MultiItemListPartitionDesc
                || partitionDesc instanceof SingleRangePartitionDesc) {
            checkNotSystemTableForAutoPartition(partitionInfo, partitionDesc);
            addPartitions(db, tableName, ImmutableList.of(partitionDesc), addPartitionClause);
        } else if (partitionDesc instanceof RangePartitionDesc) {
            checkNotSystemTableForAutoPartition(partitionInfo, partitionDesc);
            addPartitions(db, tableName,
                    Lists.newArrayList(((RangePartitionDesc) partitionDesc).getSingleRangePartitionDescs()),
                    addPartitionClause);
        } else if (partitionDesc instanceof ListPartitionDesc) {
            checkNotSystemTableForAutoPartition(partitionInfo, partitionDesc);
            addPartitions(db, tableName,
                    Lists.newArrayList(((ListPartitionDesc) partitionDesc).getPartitionDescs()),
                    addPartitionClause);
        } else if (partitionDesc instanceof MultiRangePartitionDesc) {

            if (!(partitionInfo instanceof RangePartitionInfo)) {
                throw new DdlException("Batch creation of partitions only support range partition tables.");
            }
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;

            List<Column> partitionColumns = rangePartitionInfo.getPartitionColumns();
            if (partitionColumns.size() != 1) {
                throw new DdlException("Alter batch build partition only support single range column.");
            }

            Column firstPartitionColumn = partitionColumns.get(0);
            MultiRangePartitionDesc multiRangePartitionDesc = (MultiRangePartitionDesc) partitionDesc;
            boolean isAutoPartitionTable = false;
            if (partitionInfo.getType() == PartitionType.EXPR_RANGE) {
                isAutoPartitionTable = true;
                ExpressionRangePartitionInfo exprRangePartitionInfo = (ExpressionRangePartitionInfo) partitionInfo;
                Expr expr = exprRangePartitionInfo.getPartitionExprs().get(0);
                if (expr instanceof FunctionCallExpr) {
                    FunctionCallExpr functionCallExpr = (FunctionCallExpr) expr;
                    checkAutoPartitionTableLimit(functionCallExpr, multiRangePartitionDesc);
                }
            }
            Map<String, String> properties = addPartitionClause.getProperties();
            if (properties == null) {
                properties = Maps.newHashMap();
            }
            if (tableProperties != null && tableProperties.containsKey(DynamicPartitionProperty.START_DAY_OF_WEEK)) {
                properties.put(DynamicPartitionProperty.START_DAY_OF_WEEK,
                        tableProperties.get(DynamicPartitionProperty.START_DAY_OF_WEEK));
            }
            PartitionConvertContext context = new PartitionConvertContext();
            context.setAutoPartitionTable(isAutoPartitionTable);
            context.setFirstPartitionColumnType(firstPartitionColumn.getType());
            context.setProperties(properties);
            context.setTempPartition(addPartitionClause.isTempPartition());

            List<SingleRangePartitionDesc> singleRangePartitionDescs = multiRangePartitionDesc.convertToSingle(context);
            List<PartitionDesc> partitionDescs = singleRangePartitionDescs.stream()
                    .map(item -> (PartitionDesc) item).collect(Collectors.toList());
            addPartitions(db, tableName, partitionDescs, addPartitionClause);
        }
    }

    private void checkNotSystemTableForAutoPartition(PartitionInfo partitionInfo, PartitionDesc partitionDesc)
            throws DdlException {

        if (partitionDesc.isSystem()) {
            return;
        }

        if (!partitionInfo.isAutomaticPartition()) {
            return;
        }

        if (partitionInfo.isRangePartition()) {
            throw new DdlException("Automatically partitioned tables only support the syntax " +
                    "for adding partitions in batches.");
        }

        if (partitionInfo.getType() == PartitionType.LIST) {
            if (partitionDesc instanceof SingleItemListPartitionDesc) {
                SingleItemListPartitionDesc singleItemListPartitionDesc = (SingleItemListPartitionDesc) partitionDesc;
                if (singleItemListPartitionDesc.getValues().size() > 1) {
                    throw new DdlException("Automatically partitioned tables does not support " +
                            "multiple values in the same partition");
                }
            } else if (partitionDesc instanceof MultiItemListPartitionDesc) {
                MultiItemListPartitionDesc multiItemListPartitionDesc = (MultiItemListPartitionDesc) partitionDesc;
                if (multiItemListPartitionDesc.getMultiValues().size() > 1) {
                    throw new DdlException("Automatically partitioned tables does not support " +
                            "multiple values in the same partition");
                }
            }
        }
    }

    private void checkAutoPartitionTableLimit(FunctionCallExpr functionCallExpr,
                                              MultiRangePartitionDesc multiRangePartitionDesc) throws DdlException {
        String descGranularity = multiRangePartitionDesc.getTimeUnit();
        String functionName = functionCallExpr.getFnName().getFunction();
        if (FunctionSet.DATE_TRUNC.equalsIgnoreCase(functionName)) {
            Expr expr = functionCallExpr.getParams().exprs().get(0);
            String functionGranularity = ((StringLiteral) expr).getStringValue();
            if (!descGranularity.equalsIgnoreCase(functionGranularity)) {
                throw new DdlException("The granularity of the auto-partitioned table granularity(" +
                        functionGranularity + ") should be consistent with the increased partition granularity(" +
                        descGranularity + ").");
            }
        } else if (FunctionSet.TIME_SLICE.equalsIgnoreCase(functionName)) {
            throw new DdlException("time_slice does not support pre-created partitions");
        }
        if (multiRangePartitionDesc.getStep() > 1) {
            throw new DdlException("The step of the auto-partitioned table should be 1");
        }
    }

    private OlapTable checkTable(Database db, String tableName) throws DdlException {
        CatalogUtils.checkTableExist(db, tableName);
        Table table = db.getTable(tableName);
        CatalogUtils.checkNativeTable(db, table);
        OlapTable olapTable = (OlapTable) table;
        CatalogUtils.checkTableState(olapTable, tableName);
        return olapTable;
    }

    private void checkPartitionType(PartitionInfo partitionInfo) throws DdlException {
        PartitionType partitionType = partitionInfo.getType();
        if (!partitionInfo.isRangePartition() && partitionType != PartitionType.LIST) {
            throw new DdlException("Only support adding partition to range/list partitioned table");
        }
    }

    private void analyzeAddPartition(OlapTable olapTable, List<PartitionDesc> partitionDescs,
                                     AddPartitionClause addPartitionClause, PartitionInfo partitionInfo)
            throws DdlException, AnalysisException, NotImplementedException {

        Set<String> existPartitionNameSet =
                CatalogUtils.checkPartitionNameExistForAddPartitions(olapTable, partitionDescs);
        // partition properties is prior to clause properties
        // clause properties is prior to table properties
        // partition properties should inherit table properties
        Map<String, String> properties = olapTable.getProperties();
        Map<String, String> clauseProperties = addPartitionClause.getProperties();
        if (clauseProperties != null && !clauseProperties.isEmpty()) {
            properties.putAll(clauseProperties);
        }

        for (PartitionDesc partitionDesc : partitionDescs) {
            Map<String, String> cloneProperties = Maps.newHashMap(properties);
            Map<String, String> sourceProperties = partitionDesc.getProperties();
            if (sourceProperties != null && !sourceProperties.isEmpty()) {
                cloneProperties.putAll(sourceProperties);
            }

            String storageCoolDownTTL = olapTable.getTableProperty()
                    .getProperties().get(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TTL);
            if (storageCoolDownTTL != null) {
                cloneProperties.putIfAbsent(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TTL, storageCoolDownTTL);
            }

            if (partitionDesc instanceof SingleRangePartitionDesc) {
                RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
                SingleRangePartitionDesc singleRangePartitionDesc = ((SingleRangePartitionDesc) partitionDesc);
                singleRangePartitionDesc.analyze(rangePartitionInfo.getPartitionColumns().size(), cloneProperties);
                if (!existPartitionNameSet.contains(singleRangePartitionDesc.getPartitionName())) {
                    rangePartitionInfo.checkAndCreateRange(singleRangePartitionDesc,
                            addPartitionClause.isTempPartition());
                }
            } else if (partitionDesc instanceof SingleItemListPartitionDesc
                    || partitionDesc instanceof MultiItemListPartitionDesc) {
                List<ColumnDef> columnDefList = partitionInfo.getPartitionColumns().stream()
                        .map(item -> new ColumnDef(item.getName(), new TypeDef(item.getType())))
                        .collect(Collectors.toList());
                partitionDesc.analyze(columnDefList, cloneProperties);
                if (!existPartitionNameSet.contains(partitionDesc.getPartitionName())) {
                    CatalogUtils.checkPartitionValuesExistForAddListPartition(olapTable, partitionDesc,
                            addPartitionClause.isTempPartition());
                }
            } else {
                throw new DdlException("Only support adding partition to range/list partitioned table");
            }
        }
    }

    private DistributionInfo getDistributionInfo(OlapTable olapTable, AddPartitionClause addPartitionClause)
            throws DdlException {
        DistributionInfo distributionInfo;
        List<Column> baseSchema = olapTable.getBaseSchema();
        DistributionInfo defaultDistributionInfo = olapTable.getDefaultDistributionInfo();
        DistributionDesc distributionDesc = addPartitionClause.getDistributionDesc();
        if (distributionDesc != null) {
            distributionInfo = distributionDesc.toDistributionInfo(baseSchema);
            // for now. we only support modify distribution's bucket num
            if (distributionInfo.getType() != defaultDistributionInfo.getType()) {
                throw new DdlException("Cannot assign different distribution type. default is: "
                        + defaultDistributionInfo.getType());
            }

            if (distributionInfo.getType() == DistributionInfo.DistributionInfoType.HASH) {
                HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) distributionInfo;
                List<Column> newDistriCols = hashDistributionInfo.getDistributionColumns();
                List<Column> defaultDistriCols = ((HashDistributionInfo) defaultDistributionInfo)
                        .getDistributionColumns();
                if (!newDistriCols.equals(defaultDistriCols)) {
                    throw new DdlException("Cannot assign hash distribution with different distribution cols. "
                            + "default is: " + defaultDistriCols);
                }
                if (hashDistributionInfo.getBucketNum() < 0) {
                    throw new DdlException("Cannot assign hash distribution buckets less than 0");
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
            groupSchema.checkDistribution(distributionInfo);
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

    private Map<Partition, PartitionDesc> createPartitionMap(Database db, OlapTable copiedTable,
                                                             List<PartitionDesc> partitionDescs,
                                                             HashMap<String, Set<Long>> partitionNameToTabletSet,
                                                             Set<Long> tabletIdSetForAll,
                                                             Set<String> existPartitionNameSet)
            throws DdlException {
        Map<Partition, PartitionDesc> partitionMap = Maps.newHashMap();
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
            copiedTable.getPartitionInfo().setTabletType(partitionId, partitionDesc.getTabletType());
            copiedTable.getPartitionInfo().setReplicationNum(partitionId, partitionDesc.getReplicationNum());
            copiedTable.getPartitionInfo().setIsInMemory(partitionId, partitionDesc.isInMemory());
            copiedTable.getPartitionInfo().setStorageCacheInfo(partitionId, partitionDesc.getStorageCacheInfo());

            Partition partition =
                    createPartition(db, copiedTable, partitionId, partitionName, version, tabletIdSet);

            partitionMap.put(partition, partitionDesc);
            tabletIdSetForAll.addAll(tabletIdSet);
            partitionNameToTabletSet.put(partitionName, tabletIdSet);
        }
        return partitionMap;
    }

    private void checkIfMetaChange(OlapTable olapTable, OlapTable copiedTable, String tableName) throws DdlException {
        // rollup index may be added or dropped during add partition operation.
        // schema may be changed during add partition operation.
        boolean metaChanged = false;
        if (olapTable.getIndexNameToId().size() != copiedTable.getIndexNameToId().size()) {
            metaChanged = true;
        } else {
            // compare schemaHash
            for (Map.Entry<Long, MaterializedIndexMeta> entry : olapTable.getIndexIdToMeta().entrySet()) {
                long indexId = entry.getKey();
                if (!copiedTable.getIndexIdToMeta().containsKey(indexId)) {
                    metaChanged = true;
                    break;
                }
                if (copiedTable.getIndexIdToMeta().get(indexId).getSchemaHash() !=
                        entry.getValue().getSchemaHash()) {
                    metaChanged = true;
                    break;
                }
            }
        }

        if (metaChanged) {
            throw new DdlException("Table[" + tableName + "]'s meta has been changed. try again.");
        }
    }

    private void updatePartitionInfo(PartitionInfo partitionInfo, Map<Partition, PartitionDesc> partitionMap,
                                     Set<String> existPartitionNameSet, AddPartitionClause addPartitionClause,
                                     OlapTable olapTable)
            throws DdlException {
        boolean isTempPartition = addPartitionClause.isTempPartition();
        if (partitionInfo instanceof RangePartitionInfo) {
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
            rangePartitionInfo.handleNewRangePartitionDescs(partitionMap, existPartitionNameSet, isTempPartition);
        } else if (partitionInfo instanceof ListPartitionInfo) {
            ListPartitionInfo listPartitionInfo = (ListPartitionInfo) partitionInfo;
            listPartitionInfo.handleNewListPartitionDescs(partitionMap, existPartitionNameSet, isTempPartition);
        } else {
            throw new DdlException("Only support adding partition to range/list partitioned table");
        }

        if (isTempPartition) {
            for (Partition partition : partitionMap.keySet()) {
                if (!existPartitionNameSet.contains(partition.getName())) {
                    olapTable.addTempPartition(partition);
                }
            }
        } else {
            for (Partition partition : partitionMap.keySet()) {
                if (!existPartitionNameSet.contains(partition.getName())) {
                    olapTable.addPartition(partition);
                }
            }
        }
    }

    private void addRangePartitionLog(Database db, OlapTable olapTable, List<PartitionDesc> partitionDescs,
                                      AddPartitionClause addPartitionClause, PartitionInfo partitionInfo,
                                      List<Partition> partitionList, Set<String> existPartitionNameSet) {
        boolean isTempPartition = addPartitionClause.isTempPartition();
        int partitionLen = partitionList.size();
        // Forward compatible with previous log formats
        // Version 1.15 is compatible if users only use single-partition syntax.
        // Otherwise, the followers will be crash when reading the new log
        List<PartitionPersistInfoV2> partitionInfoV2List = Lists.newArrayListWithCapacity(partitionLen);
        if (partitionLen == 1) {
            Partition partition = partitionList.get(0);
            if (existPartitionNameSet.contains(partition.getName())) {
                LOG.info("add partition[{}] which already exists", partition.getName());
                return;
            }
            long partitionId = partition.getId();
            if (olapTable.isCloudNativeTableOrMaterializedView()) {
                PartitionPersistInfoV2 info = new RangePartitionPersistInfo(db.getId(), olapTable.getId(), partition,
                        partitionDescs.get(0).getPartitionDataProperty(),
                        partitionInfo.getReplicationNum(partition.getId()),
                        partitionInfo.getIsInMemory(partition.getId()), isTempPartition,
                        ((RangePartitionInfo) partitionInfo).getRange(partition.getId()),
                        ((SingleRangePartitionDesc) partitionDescs.get(0)).getStorageCacheInfo());
                partitionInfoV2List.add(info);
                AddPartitionsInfoV2 infos = new AddPartitionsInfoV2(partitionInfoV2List);
                GlobalStateMgr.getCurrentState().getEditLog().logAddPartitions(infos);
            } else {
                PartitionPersistInfo info = new PartitionPersistInfo(db.getId(), olapTable.getId(), partition,
                        ((RangePartitionInfo) partitionInfo).getRange(partitionId),
                        partitionDescs.get(0).getPartitionDataProperty(),
                        partitionInfo.getReplicationNum(partitionId),
                        partitionInfo.getIsInMemory(partitionId),
                        isTempPartition);
                GlobalStateMgr.getCurrentState().getEditLog().logAddPartition(info);
            }

            LOG.info("succeed in creating partition[{}], name: {}, temp: {}", partitionId,
                    partition.getName(), isTempPartition);
        } else {
            List<PartitionPersistInfo> partitionInfoList = Lists.newArrayListWithCapacity(partitionLen);
            for (int i = 0; i < partitionLen; i++) {
                Partition partition = partitionList.get(i);
                if (!existPartitionNameSet.contains(partition.getName())) {
                    if (olapTable.isCloudNativeTableOrMaterializedView()) {
                        PartitionPersistInfoV2 info = new RangePartitionPersistInfo(db.getId(), olapTable.getId(),
                                partition, partitionDescs.get(i).getPartitionDataProperty(),
                                partitionInfo.getReplicationNum(partition.getId()),
                                partitionInfo.getIsInMemory(partition.getId()), isTempPartition,
                                ((RangePartitionInfo) partitionInfo).getRange(partition.getId()),
                                ((SingleRangePartitionDesc) partitionDescs.get(i)).getStorageCacheInfo());

                        partitionInfoV2List.add(info);
                    } else {
                        PartitionPersistInfo info =
                                new PartitionPersistInfo(db.getId(), olapTable.getId(), partition,
                                        ((RangePartitionInfo) partitionInfo).getRange(partition.getId()),
                                        partitionDescs.get(i).getPartitionDataProperty(),
                                        partitionInfo.getReplicationNum(partition.getId()),
                                        partitionInfo.getIsInMemory(partition.getId()),
                                        isTempPartition);
                        partitionInfoList.add(info);
                    }
                }
            }

            if (olapTable.isCloudNativeTableOrMaterializedView()) {
                AddPartitionsInfoV2 infos = new AddPartitionsInfoV2(partitionInfoV2List);
                GlobalStateMgr.getCurrentState().getEditLog().logAddPartitions(infos);
            } else {
                AddPartitionsInfo infos = new AddPartitionsInfo(partitionInfoList);
                GlobalStateMgr.getCurrentState().getEditLog().logAddPartitions(infos);
            }

            for (PartitionPersistInfoV2 infoV2 : partitionInfoV2List) {
                LOG.info("succeed in creating partition[{}], name: {}, temp: {}", infoV2.getPartition().getId(),
                        infoV2.getPartition().getName(), isTempPartition);
            }
        }
    }

    private void addListPartitionLog(Database db, OlapTable olapTable, List<PartitionDesc> partitionDescs,
                                     AddPartitionClause addPartitionClause, PartitionInfo partitionInfo,
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
            boolean isTempPartition = addPartitionClause.isTempPartition();
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
                    ((ListPartitionInfo) partitionInfo).getIdToMultiValues().get(partitionId));
            GlobalStateMgr.getCurrentState().getEditLog().logAddPartition(info);
            LOG.info("succeed in creating list partition[{}], name: {}, temp: {}", partitionId,
                    partition.getName(), isTempPartition);
            i++;
        }
    }

    private void addPartitionLog(Database db, OlapTable olapTable, List<PartitionDesc> partitionDescs,
                                 AddPartitionClause addPartitionClause, PartitionInfo partitionInfo,
                                 List<Partition> partitionList, Set<String> existPartitionNameSet)
            throws DdlException {
        PartitionType partitionType = partitionInfo.getType();
        if (partitionInfo.isRangePartition()) {
            addRangePartitionLog(db, olapTable, partitionDescs, addPartitionClause, partitionInfo, partitionList,
                    existPartitionNameSet);
        } else if (partitionType == PartitionType.LIST) {
            addListPartitionLog(db, olapTable, partitionDescs, addPartitionClause, partitionInfo, partitionList,
                    existPartitionNameSet);
        } else {
            throw new DdlException("Only support adding partition log to range/list partitioned table");
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
                GlobalStateMgr.getCurrentInvertedIndex().deleteTablet(tabletId);
            }
        }
    }

    private void cleanTabletIdSetForAll(Set<Long> tabletIdSetForAll) {
        // Cleanup of shards for LakeTable is taken care by ShardDeleter
        for (Long tabletId : tabletIdSetForAll) {
            GlobalStateMgr.getCurrentInvertedIndex().deleteTablet(tabletId);
        }
    }

    private void addPartitions(Database db, String tableName, List<PartitionDesc> partitionDescs,
                               AddPartitionClause addPartitionClause) throws DdlException {
        DistributionInfo distributionInfo;
        OlapTable olapTable;
        OlapTable copiedTable;

        db.readLock();
        Set<String> checkExistPartitionName = Sets.newConcurrentHashSet();
        try {
            olapTable = checkTable(db, tableName);

            // get partition info
            PartitionInfo partitionInfo = olapTable.getPartitionInfo();

            // check partition type
            checkPartitionType(partitionInfo);

            // analyze add partition
            analyzeAddPartition(olapTable, partitionDescs, addPartitionClause, partitionInfo);

            // get distributionInfo
            distributionInfo = getDistributionInfo(olapTable, addPartitionClause).copy();
            if (distributionInfo.getBucketNum() == 0) {
                int numBucket = CatalogUtils.calAvgBucketNumOfRecentPartitions(olapTable, 5,
                        Config.enable_auto_tablet_distribution);
                distributionInfo.setBucketNum(numBucket);
            }

            // check colocation
            checkColocation(db, olapTable, distributionInfo, partitionDescs);

            copiedTable = olapTable.selectiveCopy(null, false, MaterializedIndex.IndexExtState.VISIBLE);
            copiedTable.setDefaultDistributionInfo(distributionInfo);
            checkExistPartitionName = CatalogUtils.checkPartitionNameExistForAddPartitions(olapTable, partitionDescs);
        } catch (AnalysisException | NotImplementedException e) {
            throw new DdlException(e.getMessage(), e);
        } finally {
            db.readUnlock();
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
            Map<Partition, PartitionDesc> partitionMap = createPartitionMap(db, copiedTable, partitionDescs,
                    partitionNameToTabletSet, tabletIdSetForAll, checkExistPartitionName);

            // build partitions
            ArrayList<Partition> partitionList = new ArrayList<>(partitionMap.keySet());
            buildPartitions(db, copiedTable, partitionList);

            // check again
            if (!db.writeLockAndCheckExist()) {
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
                updatePartitionInfo(partitionInfo, partitionMap, existPartitionNameSet, addPartitionClause, olapTable);

                colocateTableIndex.updateLakeTableColocationInfo(olapTable, true /* isJoin */, null /* expectGroupId */);

                // add partition log
                addPartitionLog(db, olapTable, partitionDescs, addPartitionClause, partitionInfo, partitionList,
                        existPartitionNameSet);
            } finally {
                cleanExistPartitionNameSet(existPartitionNameSet, partitionNameToTabletSet);
                db.writeUnlock();
            }
        } catch (DdlException e) {
            cleanTabletIdSetForAll(tabletIdSetForAll);
            throw e;
        }
    }

    public void replayAddPartition(PartitionPersistInfoV2 info) throws DdlException {
        Database db = this.getDb(info.getDbId());
        db.writeLock();
        try {
            OlapTable olapTable = (OlapTable) db.getTable(info.getTableId());
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
                            info.asListPartitionPersistInfo());
                } catch (AnalysisException e) {
                    throw new DdlException(e.getMessage());
                }
            } else if (partitionInfo.isRangePartition()) {
                ((RangePartitionInfo) partitionInfo).unprotectHandleNewSinglePartitionDesc(
                        info.asRangePartitionPersistInfo());
            } else if (partitionType == PartitionType.UNPARTITIONED) {
                // insert overwrite job will create temp partition and replace the single partition.
                partitionInfo.addPartition(partition.getId(), info.getDataProperty(), info.getReplicationNum(),
                        info.isInMemory(), info.getStorageCacheInfo());
            } else {
                throw new DdlException("Unsupported partition type: " + partitionType.name());
            }

            if (!isCheckpointThread()) {
                // add to inverted index
                TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentInvertedIndex();
                for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
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
        } finally {
            db.writeUnlock();
        }
    }

    public void replayAddPartition(PartitionPersistInfo info) throws DdlException {
        Database db = this.getDb(info.getDbId());
        db.writeLock();
        try {
            OlapTable olapTable = (OlapTable) db.getTable(info.getTableId());
            Partition partition = info.getPartition();

            PartitionInfo partitionInfo = olapTable.getPartitionInfo();
            if (info.isTempPartition()) {
                olapTable.addTempPartition(partition);
            } else {
                olapTable.addPartition(partition);
            }

            if (partitionInfo.isRangePartition()) {
                ((RangePartitionInfo) partitionInfo).unprotectHandleNewSinglePartitionDesc(partition.getId(),
                        info.isTempPartition(), info.getRange(), info.getDataProperty(), info.getReplicationNum(),
                        info.isInMemory());
            } else {
                partitionInfo.addPartition(
                        partition.getId(), info.getDataProperty(), info.getReplicationNum(), info.isInMemory());
            }
            if (!isCheckpointThread()) {
                // add to inverted index
                TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentInvertedIndex();
                for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                    long indexId = index.getId();
                    int schemaHash = olapTable.getSchemaHashByIndexId(indexId);
                    TabletMeta tabletMeta = new TabletMeta(info.getDbId(), info.getTableId(), partition.getId(),
                            index.getId(), schemaHash, info.getDataProperty().getStorageMedium());
                    for (Tablet tablet : index.getTablets()) {
                        long tabletId = tablet.getId();
                        invertedIndex.addTablet(tabletId, tabletMeta);
                        for (Replica replica : ((LocalTablet) tablet).getImmutableReplicas()) {
                            invertedIndex.addReplica(tabletId, replica);
                        }
                    }
                }
            }
        } finally {
            db.writeUnlock();
        }
    }

    public void dropPartition(Database db, Table table, DropPartitionClause clause) throws DdlException {
        CatalogUtils.checkTableExist(db, table.getName());
        OlapTable olapTable = (OlapTable) table;
        Preconditions.checkArgument(db.isWriteLockHeldByCurrentThread());

        String partitionName = clause.getPartitionName();
        boolean isTempPartition = clause.isTempPartition();

        if (olapTable.getState() != OlapTable.OlapTableState.NORMAL) {
            throw new DdlException("Table[" + olapTable.getName() + "]'s state is not NORMAL");
        }

        if (!olapTable.checkPartitionNameExist(partitionName, isTempPartition)) {
            if (clause.isSetIfExists()) {
                LOG.info("drop partition[{}] which does not exist", partitionName);
                return;
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_DROP_PARTITION_NON_EXISTENT, partitionName);
            }
        }

        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        if (!partitionInfo.isRangePartition() && partitionInfo.getType() != PartitionType.LIST) {
            throw new DdlException("Alter table [" + olapTable.getName() + "] failed. Not a partitioned table");
        }

        // drop
        if (isTempPartition) {
            olapTable.dropTempPartition(partitionName, true);
        } else {
            if (!clause.isForceDrop()) {
                Partition partition = olapTable.getPartition(partitionName);
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
            olapTable.dropPartition(db.getId(), partitionName, clause.isForceDrop());
            if (olapTable instanceof MaterializedView) {
                MaterializedView mv = (MaterializedView) olapTable;
                SyncPartitionUtils.dropBaseVersionMeta(mv, partitionName);
            }
            try {
                for (MvId mvId : olapTable.getRelatedMaterializedViews()) {
                    MaterializedView materializedView = (MaterializedView) db.getTable(mvId.getId());
                    if (materializedView != null && materializedView.isLoadTriggeredRefresh()) {
                        GlobalStateMgr.getCurrentState().getLocalMetastore().refreshMaterializedView(
                                db.getFullName(), materializedView.getName(), false, null,
                                Constants.TaskRunPriority.NORMAL.value(), true, false);
                    }
                }
            } catch (MetaNotFoundException e) {
                throw new DdlException("fail to refresh materialized views when dropping partition", e);
            }
        }

        // log
        DropPartitionInfo info = new DropPartitionInfo(db.getId(), olapTable.getId(), partitionName, isTempPartition,
                clause.isForceDrop());
        GlobalStateMgr.getCurrentState().getEditLog().logDropPartition(info);

        LOG.info("succeed in droping partition[{}], is temp : {}, is force : {}", partitionName, isTempPartition,
                clause.isForceDrop());
    }

    public void replayDropPartition(DropPartitionInfo info) {
        Database db = this.getDb(info.getDbId());
        db.writeLock();
        try {
            OlapTable olapTable = (OlapTable) db.getTable(info.getTableId());
            if (info.isTempPartition()) {
                olapTable.dropTempPartition(info.getPartitionName(), true);
            } else {
                olapTable.dropPartition(info.getDbId(), info.getPartitionName(), info.isForceDrop());
            }
        } finally {
            db.writeUnlock();
        }
    }

    public void replayErasePartition(long partitionId) throws DdlException {
        recycleBin.replayErasePartition(partitionId);
    }

    public void replayRecoverPartition(RecoverInfo info) {
        long dbId = info.getDbId();
        Database db = getDb(dbId);
        db.writeLock();
        try {
            Table table = db.getTable(info.getTableId());
            recycleBin.replayRecoverPartition((OlapTable) table, info.getPartitionId());
        } finally {
            db.writeUnlock();
        }
    }

    private Partition createPartition(Database db, OlapTable table, long partitionId, String partitionName,
                                      Long version, Set<Long> tabletIdSet) throws DdlException {
        return createPartitionCommon(db, table, partitionId, partitionName, table.getPartitionInfo(), version,
                tabletIdSet);
    }

    private Partition createPartitionCommon(Database db, OlapTable table, long partitionId, String partitionName,
                                            PartitionInfo partitionInfo, Long version, Set<Long> tabletIdSet)
            throws DdlException {
        Map<Long, MaterializedIndex> indexMap = new HashMap<>();
        for (long indexId : table.getIndexIdToMeta().keySet()) {
            MaterializedIndex rollup = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);
            indexMap.put(indexId, rollup);
        }
        DistributionInfo distributionInfo = table.getDefaultDistributionInfo().copy();
        if (distributionInfo.getBucketNum() == 0) {
            int numBucket = CatalogUtils.calAvgBucketNumOfRecentPartitions(table, 5, Config.enable_auto_tablet_distribution);
            distributionInfo.setBucketNum(numBucket);
        }

        // create shard group
        long shardGroupId = 0;
        if (table.isCloudNativeTableOrMaterializedView()) {
            shardGroupId = GlobalStateMgr.getCurrentState().getStarOSAgent().
                    createShardGroup(db.getId(), table.getId(), partitionId);
        }

        Partition partition =
                new Partition(partitionId, partitionName, indexMap.get(table.getBaseIndexId()),
                        distributionInfo, shardGroupId);
        // version
        if (version != null) {
            partition.updateVisibleVersion(version);
        }

        short replicationNum = partitionInfo.getReplicationNum(partitionId);
        TStorageMedium storageMedium = partitionInfo.getDataProperty(partitionId).getStorageMedium();
        for (Map.Entry<Long, MaterializedIndex> entry : indexMap.entrySet()) {
            long indexId = entry.getKey();
            MaterializedIndex index = entry.getValue();
            MaterializedIndexMeta indexMeta = table.getIndexIdToMeta().get(indexId);

            // create tablets
            TabletMeta tabletMeta =
                    new TabletMeta(db.getId(), table.getId(), partitionId, indexId, indexMeta.getSchemaHash(),
                            storageMedium, table.isCloudNativeTableOrMaterializedView());

            if (table.isCloudNativeTableOrMaterializedView()) {
                createLakeTablets(table, partitionId, shardGroupId, index, distributionInfo,
                        tabletMeta, tabletIdSet);
            } else {
                createOlapTablets(table, index, Replica.ReplicaState.NORMAL, distributionInfo,
                        partition.getVisibleVersion(), replicationNum, tabletMeta, tabletIdSet);
            }
            if (index.getId() != table.getBaseIndexId()) {
                // add rollup index to partition
                partition.createRollupIndex(index);
            }
        }
        return partition;
    }

    private void buildPartitions(Database db, OlapTable table, List<Partition> partitions) throws DdlException {
        if (partitions.isEmpty()) {
            return;
        }
        int numAliveBackends = GlobalStateMgr.getCurrentSystemInfo().getAliveBackendNumber();
        if (numAliveBackends == 0) {
            throw new DdlException("no alive backends");
        }

        int numReplicas = 0;
        for (Partition partition : partitions) {
            numReplicas += partition.getReplicaCount();
        }

        if (numReplicas > Config.create_table_max_serial_replicas) {
            LOG.info("start to build {} partitions concurrently for table {}.{} with {} replicas",
                    partitions.size(), db.getFullName(), table.getName(), numReplicas);
            buildPartitionsConcurrently(db.getId(), table, partitions, numReplicas, numAliveBackends);
        } else {
            LOG.info("start to build {} partitions sequentially for table {}.{} with {} replicas",
                    partitions.size(), db.getFullName(), table.getName(), numReplicas);
            buildPartitionsSequentially(db.getId(), table, partitions, numReplicas, numAliveBackends);
        }
    }

    private int countMaxTasksPerBackend(List<CreateReplicaTask> tasks) {
        Map<Long, Integer> tasksPerBackend = new HashMap<>();
        for (CreateReplicaTask task : tasks) {
            tasksPerBackend.compute(task.getBackendId(), (k, v) -> (v == null) ? 1 : v + 1);
        }
        return Collections.max(tasksPerBackend.values());
    }

    private void buildPartitionsSequentially(long dbId, OlapTable table, List<Partition> partitions, int numReplicas,
                                             int numBackends) throws DdlException {
        // Try to bundle at least 200 CreateReplicaTask's in a single AgentBatchTask.
        // The number 200 is just an experiment value that seems to work without obvious problems, feel free to
        // change it if you have a better choice.
        long start = System.currentTimeMillis();
        int avgReplicasPerPartition = numReplicas / partitions.size();
        int partitionGroupSize = Math.max(1, numBackends * 200 / Math.max(1, avgReplicasPerPartition));
        for (int i = 0; i < partitions.size(); i += partitionGroupSize) {
            int endIndex = Math.min(partitions.size(), i + partitionGroupSize);
            List<CreateReplicaTask> tasks = buildCreateReplicaTasks(dbId, table, partitions.subList(i, endIndex));
            int partitionCount = endIndex - i;
            int indexCountPerPartition = partitions.get(i).getVisibleMaterializedIndicesCount();
            int timeout = Config.tablet_create_timeout_second * countMaxTasksPerBackend(tasks);
            // Compatible with older versions, `Config.max_create_table_timeout_second` is the timeout time for a single index.
            // Here we assume that all partitions have the same number of indexes.
            int maxTimeout = partitionCount * indexCountPerPartition * Config.max_create_table_timeout_second;
            try {
                LOG.info("build partitions sequentially, send task one by one, all tasks timeout {}s",
                        Math.min(timeout, maxTimeout));
                sendCreateReplicaTasksAndWaitForFinished(tasks, Math.min(timeout, maxTimeout));
                LOG.info("build partitions sequentially, all tasks finished, took {}ms",
                        System.currentTimeMillis() - start);
                tasks.clear();
            } finally {
                for (CreateReplicaTask task : tasks) {
                    AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.CREATE, task.getSignature());
                }
            }
        }
    }

    private void buildPartitionsConcurrently(long dbId, OlapTable table, List<Partition> partitions, int numReplicas,
                                             int numBackends) throws DdlException {
        long start = System.currentTimeMillis();
        int timeout = Math.max(1, numReplicas / numBackends) * Config.tablet_create_timeout_second;
        int numIndexes = partitions.stream().mapToInt(Partition::getVisibleMaterializedIndicesCount).sum();
        int maxTimeout = numIndexes * Config.max_create_table_timeout_second;
        MarkedCountDownLatch<Long, Long> countDownLatch = new MarkedCountDownLatch<>(numReplicas);
        Thread t = new Thread(() -> {
            Map<Long, List<Long>> taskSignatures = new HashMap<>();
            try {
                int numFinishedTasks;
                int numSendedTasks = 0;
                for (Partition partition : partitions) {
                    if (!countDownLatch.getStatus().ok()) {
                        break;
                    }
                    List<CreateReplicaTask> tasks = buildCreateReplicaTasks(dbId, table, partition);
                    for (CreateReplicaTask task : tasks) {
                        List<Long> signatures =
                                taskSignatures.computeIfAbsent(task.getBackendId(), k -> new ArrayList<>());
                        signatures.add(task.getSignature());
                    }
                    sendCreateReplicaTasks(tasks, countDownLatch);
                    numSendedTasks += tasks.size();
                    numFinishedTasks = numReplicas - (int) countDownLatch.getCount();
                    // Since there is no mechanism to cancel tasks, if we send a lot of tasks at once and some error or timeout
                    // occurs in the middle of the process, it will create a lot of useless replicas that will be deleted soon and
                    // waste machine resources. Sending a lot of tasks at once may also block other users' tasks for a long time.
                    // To avoid these situations, new tasks are sent only when the average number of tasks on each node is less
                    // than 200.
                    // (numSendedTasks - numFinishedTasks) is number of tasks that have been sent but not yet finished.
                    while (numSendedTasks - numFinishedTasks > 200 * numBackends) {
                        ThreadUtil.sleepAtLeastIgnoreInterrupts(100);
                        numFinishedTasks = numReplicas - (int) countDownLatch.getCount();
                    }
                }
                countDownLatch.await();
                if (countDownLatch.getStatus().ok()) {
                    taskSignatures.clear();
                }
            } catch (Exception e) {
                LOG.warn(e);
                countDownLatch.countDownToZero(new Status(TStatusCode.UNKNOWN, e.toString()));
            } finally {
                for (Map.Entry<Long, List<Long>> entry : taskSignatures.entrySet()) {
                    for (Long signature : entry.getValue()) {
                        AgentTaskQueue.removeTask(entry.getKey(), TTaskType.CREATE, signature);
                    }
                }
            }
        }, "partition-build");
        t.start();
        try {
            LOG.info("build partitions concurrently for {}, waiting for all tasks finish with timeout {}s",
                    table.getName(), Math.min(timeout, maxTimeout));
            waitForFinished(countDownLatch, Math.min(timeout, maxTimeout));
            LOG.info("build partitions concurrently for {}, all tasks finished, took {}ms",
                    table.getName(), System.currentTimeMillis() - start);
        } catch (Exception e) {
            countDownLatch.countDownToZero(new Status(TStatusCode.UNKNOWN, e.getMessage()));
            throw e;
        }
    }

    private List<CreateReplicaTask> buildCreateReplicaTasks(long dbId, OlapTable table, List<Partition> partitions)
            throws DdlException {
        List<CreateReplicaTask> tasks = new ArrayList<>();
        for (Partition partition : partitions) {
            tasks.addAll(buildCreateReplicaTasks(dbId, table, partition));
        }
        return tasks;
    }

    private List<CreateReplicaTask> buildCreateReplicaTasks(long dbId, OlapTable table, Partition partition) throws DdlException {
        ArrayList<CreateReplicaTask> tasks = new ArrayList<>((int) partition.getReplicaCount());
        for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE)) {
            tasks.addAll(buildCreateReplicaTasks(dbId, table, partition, index));
        }
        return tasks;
    }

    private List<CreateReplicaTask> buildCreateReplicaTasks(long dbId, OlapTable table, Partition partition,
                                                            MaterializedIndex index) throws DdlException {
        List<CreateReplicaTask> tasks = new ArrayList<>((int) index.getReplicaCount());
        MaterializedIndexMeta indexMeta = table.getIndexMetaByIndexId(index.getId());
        for (Tablet tablet : index.getTablets()) {
            if (table.isCloudNativeTableOrMaterializedView()) {
                long primaryBackendId = -1;
                try {
                    primaryBackendId = ((LakeTablet) tablet).getPrimaryBackendId();
                } catch (UserException e) {
                    throw new DdlException(e.getMessage());
                }

                CreateReplicaTask task = new CreateReplicaTask(
                        primaryBackendId,
                        dbId,
                        table.getId(),
                        partition.getId(),
                        index.getId(),
                        tablet.getId(),
                        indexMeta.getShortKeyColumnCount(),
                        indexMeta.getSchemaHash(),
                        partition.getVisibleVersion(),
                        indexMeta.getKeysType(),
                        indexMeta.getStorageType(),
                        table.getPartitionInfo().getDataProperty(partition.getId()).getStorageMedium(),
                        indexMeta.getSchema(),
                        table.getBfColumns(),
                        table.getBfFpp(),
                        null,
                        table.getIndexes(),
                        table.getPartitionInfo().getIsInMemory(partition.getId()),
                        table.enablePersistentIndex(),
                        TTabletType.TABLET_TYPE_LAKE,
                        table.getCompressionType(), indexMeta.getSortKeyIdxes());
                tasks.add(task);
            } else {
                for (Replica replica : ((LocalTablet) tablet).getImmutableReplicas()) {
                    CreateReplicaTask task = new CreateReplicaTask(
                            replica.getBackendId(),
                            dbId,
                            table.getId(),
                            partition.getId(),
                            index.getId(),
                            tablet.getId(),
                            indexMeta.getShortKeyColumnCount(),
                            indexMeta.getSchemaHash(),
                            partition.getVisibleVersion(),
                            indexMeta.getKeysType(),
                            indexMeta.getStorageType(),
                            table.getPartitionInfo().getDataProperty(partition.getId()).getStorageMedium(),
                            indexMeta.getSchema(),
                            table.getBfColumns(),
                            table.getBfFpp(),
                            null,
                            table.getIndexes(),
                            table.getPartitionInfo().getIsInMemory(partition.getId()),
                            table.enablePersistentIndex(),
                            table.getCurBinlogConfig(),
                            table.getPartitionInfo().getTabletType(partition.getId()),
                            table.getCompressionType(), indexMeta.getSortKeyIdxes());
                    tasks.add(task);
                }
            }
        }
        return tasks;
    }

    // NOTE: Unfinished tasks will NOT be removed from the AgentTaskQueue.
    private void sendCreateReplicaTasksAndWaitForFinished(List<CreateReplicaTask> tasks, long timeout)
            throws DdlException {
        MarkedCountDownLatch<Long, Long> countDownLatch = new MarkedCountDownLatch<>(tasks.size());
        sendCreateReplicaTasks(tasks, countDownLatch);
        waitForFinished(countDownLatch, timeout);
    }

    private void sendCreateReplicaTasks(List<CreateReplicaTask> tasks,
                                        MarkedCountDownLatch<Long, Long> countDownLatch) {
        HashMap<Long, AgentBatchTask> batchTaskMap = new HashMap<>();
        for (CreateReplicaTask task : tasks) {
            task.setLatch(countDownLatch);
            countDownLatch.addMark(task.getBackendId(), task.getTabletId());
            AgentBatchTask batchTask = batchTaskMap.get(task.getBackendId());
            if (batchTask == null) {
                batchTask = new AgentBatchTask();
                batchTaskMap.put(task.getBackendId(), batchTask);
            }
            batchTask.addTask(task);
        }
        for (Map.Entry<Long, AgentBatchTask> entry : batchTaskMap.entrySet()) {
            AgentTaskQueue.addBatchTask(entry.getValue());
            AgentTaskExecutor.submit(entry.getValue());
        }
    }

    // REQUIRE: must set countDownLatch to error stat before throw an exception.
    private void waitForFinished(MarkedCountDownLatch<Long, Long> countDownLatch, long timeout) throws DdlException {
        try {
            if (countDownLatch.await(timeout, TimeUnit.SECONDS)) {
                if (!countDownLatch.getStatus().ok()) {
                    String errMsg = "fail to create tablet: " + countDownLatch.getStatus().getErrorMsg();
                    LOG.warn(errMsg);
                    throw new DdlException(errMsg);
                }
            } else { // timed out
                List<Map.Entry<Long, Long>> unfinishedMarks = countDownLatch.getLeftMarks();
                List<Map.Entry<Long, Long>> firstThree =
                        unfinishedMarks.subList(0, Math.min(unfinishedMarks.size(), 3));
                StringBuilder sb = new StringBuilder("Table creation timed out. unfinished replicas");
                sb.append("(").append(firstThree.size()).append("/").append(unfinishedMarks.size()).append("): ");
                // Show details of the first 3 unfinished tablets.
                for (Map.Entry<Long, Long> mark : firstThree) {
                    sb.append(mark.getValue()); // TabletId
                    sb.append('(');
                    Backend backend = stateMgr.getClusterInfo().getBackend(mark.getKey());
                    sb.append(backend != null ? backend.getHost() : "N/A");
                    sb.append(") ");
                }
                sb.append(" timeout=").append(timeout).append('s');
                String errMsg = sb.toString();
                LOG.warn(errMsg);

                String userErrorMsg = String.format(
                        "Table creation timed out.\n You can increase the timeout by increasing the " +
                                "config \"tablet_create_timeout_second\" and try again.\n" +
                                "To increase the config \"tablet_create_timeout_second\" (currently %d), " +
                                "run the following command:\n" +
                                "```\nadmin set frontend config(\"tablet_create_timeout_second\"=\"%d\")\n```\n" +
                                "or add the following configuration to the fe.conf file and restart the process:\n" +
                                "```\ntablet_create_timeout_second=%d\n```",
                        Config.tablet_create_timeout_second,
                        Config.tablet_create_timeout_second * 2,
                        Config.tablet_create_timeout_second * 2
                );
                countDownLatch.countDownToZero(new Status(TStatusCode.TIMEOUT, "timed out"));
                throw new DdlException(userErrorMsg);
            }
        } catch (InterruptedException e) {
            LOG.warn(e);
            countDownLatch.countDownToZero(new Status(TStatusCode.CANCELLED, "cancelled"));
        }
    }

    /*
     * generate and check columns' order and key's existence
     */
    private void validateColumns(List<Column> columns) throws DdlException {
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

    private boolean processColocationProperties(String colocateGroup, Database db, OlapTable olapTable, boolean expectLakeTable)
            throws DdlException {
        if (Strings.isNullOrEmpty(colocateGroup)) {
            return false;
        }

        if (olapTable.isCloudNativeTable() != expectLakeTable) {
            return false;
        }

        String fullGroupName = db.getId() + "_" + colocateGroup;
        ColocateGroupSchema groupSchema = colocateTableIndex.getGroupSchema(fullGroupName);
        ColocateTableIndex.GroupId colocateGrpIdInOtherDb = null; /* to use GroupId.grpId */
        if (groupSchema != null) {
            // group already exist, check if this table can be added to this group
            groupSchema.checkColocateSchema(olapTable);
        } else {
            // we also need to check the schema consistency with colocate group in other database
            colocateGrpIdInOtherDb = colocateTableIndex.checkColocateSchemaWithGroupInOtherDb(
                    colocateGroup, db.getId(), olapTable);
        }
        // Add table to this group, if group does not exist, create a new one.
        // If the to create colocate group should colocate with groups in other databases,
        // i.e. `colocateGrpIdInOtherDb` is not null, we reuse `GroupId.grpId` from those
        // groups, so that we can have a mechanism to precisely find all the groups that colocate with
        // each other in different databases.
        colocateTableIndex.addTableToGroup(db.getId(), olapTable, colocateGroup,
                colocateGrpIdInOtherDb == null ? null :
                        new ColocateTableIndex.GroupId(db.getId(), colocateGrpIdInOtherDb.grpId),
                false /* isReplay */);
        olapTable.setColocateGroup(colocateGroup);
        return true;
    }

    private void setLakeStorageInfo(OlapTable table, Map<String, String> properties) throws DdlException {
        // storage cache property
        boolean enableStorageCache =
                PropertyAnalyzer.analyzeBooleanProp(properties,
                        PropertyAnalyzer.PROPERTIES_ENABLE_STORAGE_CACHE, true);
        long storageCacheTtlS = 0;
        try {
            storageCacheTtlS = PropertyAnalyzer.analyzeLongProp(properties,
                    PropertyAnalyzer.PROPERTIES_STORAGE_CACHE_TTL,
                    Config.lake_default_storage_cache_ttl_seconds);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        if (storageCacheTtlS < -1) {
            throw new DdlException("Storage cache ttl should not be less than -1");
        }
        if (!enableStorageCache && storageCacheTtlS != 0 &&
                storageCacheTtlS != Config.lake_default_storage_cache_ttl_seconds) {
            throw new DdlException("Storage cache ttl should be 0 when cache is disabled");
        }
        if (enableStorageCache && storageCacheTtlS == 0) {
            throw new DdlException("Storage cache ttl should not be 0 when cache is enabled");
        }

        // set to false if absent
        boolean enableAsyncWriteBack = PropertyAnalyzer.analyzeBooleanProp(
                properties, PropertyAnalyzer.PROPERTIES_ENABLE_ASYNC_WRITE_BACK, false);

        if (!enableStorageCache && enableAsyncWriteBack) {
            throw new DdlException("enable_async_write_back can't be turned on when cache is disabled");
        }

        // get service shard storage info from StarMgr
        FilePathInfo pathInfo = stateMgr.getStarOSAgent().allocateFilePath(table.getId());
        table.setStorageInfo(pathInfo, enableStorageCache, storageCacheTtlS, enableAsyncWriteBack);
    }

    // Create olap table and related base index synchronously.
    // when run on shared-data mode, olap table needs to get storage group from StarMgr.
    private void createOlapOrLakeTable(Database db, CreateTableStmt stmt) throws DdlException {
        String tableName = stmt.getTableName();
        LOG.debug("begin create olap table: {}", tableName);

        // create columns
        List<Column> baseSchema = stmt.getColumns();
        validateColumns(baseSchema);

        // create partition info
        PartitionDesc partitionDesc = stmt.getPartitionDesc();
        PartitionInfo partitionInfo;
        Map<String, Long> partitionNameToId = Maps.newHashMap();
        if (partitionDesc != null) {
            // gen partition id first
            if (partitionDesc instanceof RangePartitionDesc) {
                RangePartitionDesc rangePartitionDesc = (RangePartitionDesc) partitionDesc;
                for (SingleRangePartitionDesc desc : rangePartitionDesc.getSingleRangePartitionDescs()) {
                    long partitionId = getNextId();
                    partitionNameToId.put(desc.getPartitionName(), partitionId);
                }
            } else if (partitionDesc instanceof ListPartitionDesc) {
                ListPartitionDesc listPartitionDesc = (ListPartitionDesc) partitionDesc;
                listPartitionDesc.findAllPartitionNames()
                        .forEach(partitionName -> partitionNameToId.put(partitionName, getNextId()));
            } else if (partitionDesc instanceof ExpressionPartitionDesc) {
                ExpressionPartitionDesc expressionPartitionDesc = (ExpressionPartitionDesc) partitionDesc;
                for (SingleRangePartitionDesc desc : expressionPartitionDesc.getRangePartitionDesc()
                        .getSingleRangePartitionDescs()) {
                    long partitionId = getNextId();
                    partitionNameToId.put(desc.getPartitionName(), partitionId);
                }

                DynamicPartitionUtil.checkIfAutomaticPartitionAllowed(stmt.getProperties());

            } else {
                throw new DdlException("Currently only support range or list partition with engine type olap");
            }
            partitionInfo = partitionDesc.toPartitionInfo(baseSchema, partitionNameToId, false);

            // Automatic partitioning needs to ensure that at least one tablet is opened.
            if (partitionInfo.isAutomaticPartition()) {
                long partitionId = getNextId();
                String replicateNum = String.valueOf(RunMode.defaultReplicationNum());
                if (stmt.getProperties() != null) {
                    replicateNum = stmt.getProperties().getOrDefault("replication_num",
                            String.valueOf(RunMode.defaultReplicationNum()));
                }
                partitionInfo.createAutomaticShadowPartition(partitionId, replicateNum);
                partitionNameToId.put(ExpressionRangePartitionInfo.AUTOMATIC_SHADOW_PARTITION_NAME, partitionId);
            }

        } else {
            if (DynamicPartitionUtil.checkDynamicPartitionPropertiesExist(stmt.getProperties())) {
                throw new DdlException("Only support dynamic partition properties on range partition table");
            }
            long partitionId = getNextId();
            // use table name as single partition name
            partitionNameToId.put(tableName, partitionId);
            partitionInfo = new SinglePartitionInfo();
        }

        // get keys type
        KeysDesc keysDesc = stmt.getKeysDesc();
        Preconditions.checkNotNull(keysDesc);
        KeysType keysType = keysDesc.getKeysType();

        // create distribution info
        DistributionDesc distributionDesc = stmt.getDistributionDesc();
        Preconditions.checkNotNull(distributionDesc);
        DistributionInfo distributionInfo = distributionDesc.toDistributionInfo(baseSchema);

        short shortKeyColumnCount = 0;
        List<Integer> sortKeyIdxes = new ArrayList<>();
        if (stmt.getSortKeys() != null) {
            List<String> baseSchemaNames = baseSchema.stream().map(Column::getName).collect(Collectors.toList());
            for (String column : stmt.getSortKeys()) {
                int idx = baseSchemaNames.indexOf(column);
                if (idx == -1) {
                    throw new DdlException("Invalid column '" + column + "': not exists in all columns.");
                }
                sortKeyIdxes.add(idx);
            }
            shortKeyColumnCount =
                    GlobalStateMgr.calcShortKeyColumnCount(baseSchema, stmt.getProperties(), sortKeyIdxes);
        } else {
            shortKeyColumnCount = GlobalStateMgr.calcShortKeyColumnCount(baseSchema, stmt.getProperties());
        }
        LOG.debug("create table[{}] short key column count: {}", tableName, shortKeyColumnCount);
        // indexes
        TableIndexes indexes = new TableIndexes(stmt.getIndexes());

        // set base index info to table
        // this should be done before create partition.
        Map<String, String> properties = stmt.getProperties();

        // create table
        long tableId = GlobalStateMgr.getCurrentState().getNextId();
        OlapTable table;
        if (stmt.isExternal()) {
            table = new ExternalOlapTable(db.getId(), tableId, tableName, baseSchema, keysType, partitionInfo,
                    distributionInfo, indexes, properties);
            if (GlobalStateMgr.getCurrentState().getNodeMgr()
                    .checkFeExistByRPCPort(((ExternalOlapTable) table).getSourceTableHost(),
                            ((ExternalOlapTable) table).getSourceTablePort())) {
                throw new DdlException("can not create OLAP external table of self cluster");
            }
        } else if (stmt.isOlapEngine()) {
            RunMode runMode = RunMode.getCurrentRunMode();
            String volume = (properties != null) ? properties.remove(PropertyAnalyzer.PROPERTIES_STORAGE_VOLUME) : null;

            if ("local".equalsIgnoreCase(volume)) {
                table = new OlapTable(tableId, tableName, baseSchema, keysType, partitionInfo, distributionInfo, indexes);
            } else if ("default".equalsIgnoreCase(volume)) {
                table = new LakeTable(tableId, tableName, baseSchema, keysType, partitionInfo, distributionInfo, indexes);
                setLakeStorageInfo(table, properties);
            } else if (!Strings.isNullOrEmpty(volume)) {
                throw new DdlException("Unknown storage volume \"" + volume + "\"");
            } else if (runMode == RunMode.SHARED_DATA) {
                table = new LakeTable(tableId, tableName, baseSchema, keysType, partitionInfo, distributionInfo, indexes);
                setLakeStorageInfo(table, properties);
            } else {
                table = new OlapTable(tableId, tableName, baseSchema, keysType, partitionInfo, distributionInfo, indexes);
            }

            if (table.isCloudNativeTable() && !runMode.isAllowCreateLakeTable()) {
                throw new DdlException("Cannot create table with persistent volume in current run mode \"" + runMode + "\"");
            }
            if (table.isOlapTable() && !runMode.isAllowCreateOlapTable()) {
                throw new DdlException("Cannot create table without persistent volume in current run mode \"" + runMode + "\"");
            }

            if (table.isCloudNativeTable() && table.getKeysType() == KeysType.PRIMARY_KEYS) {
                throw new DdlException("Does not support primary key in current version with run mode \"" + runMode + "\"");
            }
        } else {
            throw new DdlException("Unrecognized engine \"" + stmt.getEngineName() + "\"");
        }

        table.setComment(stmt.getComment());

        // set base index id
        long baseIndexId = getNextId();
        table.setBaseIndexId(baseIndexId);

        // analyze bloom filter columns
        Set<String> bfColumns = null;
        double bfFpp = 0;
        try {
            bfColumns = PropertyAnalyzer.analyzeBloomFilterColumns(properties, baseSchema,
                    table.getKeysType() == KeysType.PRIMARY_KEYS);
            if (bfColumns != null && bfColumns.isEmpty()) {
                bfColumns = null;
            }

            bfFpp = PropertyAnalyzer.analyzeBloomFilterFpp(properties);
            if (bfColumns != null && bfFpp == 0) {
                bfFpp = FeConstants.DEFAULT_BLOOM_FILTER_FPP;
            } else if (bfColumns == null) {
                bfFpp = 0;
            }

            table.setBloomFilterInfo(bfColumns, bfFpp);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }

        // analyze replication_num
        short replicationNum = RunMode.defaultReplicationNum();
        String logReplicationNum = "";
        try {
            boolean isReplicationNumSet =
                    properties != null && properties.containsKey(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM);
            if (properties != null) {
                logReplicationNum = properties.get(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM);
            }
            replicationNum = PropertyAnalyzer.analyzeReplicationNum(properties, replicationNum);
            if (isReplicationNumSet) {
                table.setReplicationNum(replicationNum);
            }
        } catch (AnalysisException ex) {
            throw new DdlException(String.format("%s table=%s, properties.replication_num=%s",
                    ex.getMessage(), table.getName(), logReplicationNum));
        }

        // set in memory
        boolean isInMemory =
                PropertyAnalyzer.analyzeBooleanProp(properties, PropertyAnalyzer.PROPERTIES_INMEMORY, false);
        table.setIsInMemory(isInMemory);

        boolean enablePersistentIndex =
                PropertyAnalyzer.analyzeBooleanProp(properties, PropertyAnalyzer.PROPERTIES_ENABLE_PERSISTENT_INDEX,
                        false);
        table.setEnablePersistentIndex(enablePersistentIndex);

        if (properties != null && (properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_ENABLE) ||
                properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_MAX_SIZE) ||
                properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_TTL))) {
            try {
                boolean enableBinlog = PropertyAnalyzer.analyzeBooleanProp(properties,
                        PropertyAnalyzer.PROPERTIES_BINLOG_ENABLE, false);
                long binlogTtl = PropertyAnalyzer.analyzeLongProp(properties,
                        PropertyAnalyzer.PROPERTIES_BINLOG_TTL, Config.binlog_ttl_second);
                long binlogMaxSize = PropertyAnalyzer.analyzeLongProp(properties,
                        PropertyAnalyzer.PROPERTIES_BINLOG_MAX_SIZE, Config.binlog_max_size);
                BinlogConfig binlogConfig = new BinlogConfig(0, enableBinlog,
                        binlogTtl, binlogMaxSize);
                table.setCurBinlogConfig(binlogConfig);
                LOG.info("create table {} set binlog config, enable_binlog = {}, binlogTtl = {}, binlog_max_size = {}",
                        tableName, enableBinlog, binlogTtl, binlogMaxSize);
            } catch (AnalysisException e) {
                throw new DdlException(e.getMessage());
            }
        }

        // write quorum
        try {
            table.setWriteQuorum(PropertyAnalyzer.analyzeWriteQuorum(properties));
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }

        if (table.hasAutoIncrementColumn() && stmt.isOlapEngine()
                && RunMode.allowCreateLakeTable()) {
            throw new DdlException("Table with AUTO_INCREMENT column can not be lake table");
        }

        // replicated storage
        table.setEnableReplicatedStorage(
                PropertyAnalyzer.analyzeBooleanProp(
                        properties, PropertyAnalyzer.PROPERTIES_REPLICATED_STORAGE,
                        Config.enable_replicated_storage_as_default_engine));

        if (table.enableReplicatedStorage().equals(false)) {
            for (Column col : baseSchema) {
                if (col.isAutoIncrement()) {
                    throw new DdlException("Table with AUTO_INCREMENT column must use Replicated Storage");
                }
            }
        }

        TTabletType tabletType = TTabletType.TABLET_TYPE_DISK;
        try {
            tabletType = PropertyAnalyzer.analyzeTabletType(properties);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }

        if (properties != null) {
            if (properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TTL) ||
                    properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TIME)) {
                if (table.getKeysType() == KeysType.PRIMARY_KEYS) {
                    throw new DdlException("Primary key table does not support storage medium cool down currently.");
                }
                if (partitionInfo instanceof ListPartitionInfo) {
                    throw new DdlException("List partition table does not support storage medium cool down currently.");
                }
                if (partitionInfo instanceof RangePartitionInfo) {
                    RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
                    List<Column> partitionColumns = rangePartitionInfo.getPartitionColumns();
                    if (partitionColumns.size() > 1) {
                        throw new DdlException("Multi-column range partition table " +
                                "does not support storage medium cool down currently.");
                    }
                    Column column = partitionColumns.get(0);
                    if (!column.getType().getPrimitiveType().isDateType()) {
                        throw new DdlException("Only support partition is date type for" +
                                " storage medium cool down currently.");
                    }
                }
            }
            try {
                PeriodDuration duration = PropertyAnalyzer.analyzeStorageCoolDownTTL(properties, false);
                if (duration != null) {
                    table.setStorageCoolDownTTL(duration);
                }
            } catch (AnalysisException e) {
                throw new DdlException(e.getMessage());
            }
        }

        if (partitionInfo.getType() == PartitionType.UNPARTITIONED) {
            // if this is an unpartitioned table, we should analyze data property and replication num here.
            // if this is a partitioned table, there properties are already analyzed in RangePartitionDesc analyze phase.

            // use table name as this single partition name
            long partitionId = partitionNameToId.get(tableName);
            DataProperty dataProperty = null;
            try {
                boolean hasMedium = false;
                if (properties != null) {
                    hasMedium = properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM);
                }
                dataProperty = PropertyAnalyzer.analyzeDataProperty(properties,
                        DataProperty.getInferredDefaultDataProperty(), false);
                if (hasMedium) {
                    table.setStorageMedium(dataProperty.getStorageMedium());
                }
            } catch (AnalysisException e) {
                throw new DdlException(e.getMessage());
            }
            Preconditions.checkNotNull(dataProperty);
            partitionInfo.setDataProperty(partitionId, dataProperty);
            partitionInfo.setReplicationNum(partitionId, replicationNum);
            partitionInfo.setIsInMemory(partitionId, isInMemory);
            partitionInfo.setTabletType(partitionId, tabletType);
            StorageInfo storageInfo = table.getTableProperty().getStorageInfo();
            StorageCacheInfo storageCacheInfo = storageInfo == null ? null : storageInfo.getStorageCacheInfo();
            partitionInfo.setStorageCacheInfo(partitionId, storageCacheInfo);
        }

        // check colocation properties
        String colocateGroup = null;
        try {
            colocateGroup = PropertyAnalyzer.analyzeColocate(properties);
            boolean addedToColocateGroup = processColocationProperties(colocateGroup, db,
                    table, false /* expectLakeTable */);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }

        // get base index storage type. default is COLUMN
        TStorageType baseIndexStorageType = null;
        try {
            baseIndexStorageType = PropertyAnalyzer.analyzeStorageType(properties);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        Preconditions.checkNotNull(baseIndexStorageType);
        // set base index meta
        int schemaVersion = 0;
        try {
            schemaVersion = PropertyAnalyzer.analyzeSchemaVersion(properties);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        int schemaHash = Util.schemaHash(schemaVersion, baseSchema, bfColumns, bfFpp);

        if (stmt.getSortKeys() != null) {
            table.setIndexMeta(baseIndexId, tableName, baseSchema, schemaVersion, schemaHash,
                    shortKeyColumnCount, baseIndexStorageType, keysType, null, sortKeyIdxes);
        } else {
            table.setIndexMeta(baseIndexId, tableName, baseSchema, schemaVersion, schemaHash,
                    shortKeyColumnCount, baseIndexStorageType, keysType, null);
        }

        for (AlterClause alterClause : stmt.getRollupAlterClauseList()) {
            AddRollupClause addRollupClause = (AddRollupClause) alterClause;

            Long baseRollupIndex = table.getIndexIdByName(tableName);

            // get storage type for rollup index
            TStorageType rollupIndexStorageType = null;
            try {
                rollupIndexStorageType = PropertyAnalyzer.analyzeStorageType(addRollupClause.getProperties());
            } catch (AnalysisException e) {
                throw new DdlException(e.getMessage());
            }
            Preconditions.checkNotNull(rollupIndexStorageType);
            // set rollup index meta to olap table
            List<Column> rollupColumns = stateMgr.getRollupHandler().checkAndPrepareMaterializedView(addRollupClause,
                    table, baseRollupIndex, false);
            short rollupShortKeyColumnCount =
                    GlobalStateMgr.calcShortKeyColumnCount(rollupColumns, alterClause.getProperties());
            int rollupSchemaHash = Util.schemaHash(schemaVersion, rollupColumns, bfColumns, bfFpp);
            long rollupIndexId = getNextId();
            table.setIndexMeta(rollupIndexId, addRollupClause.getRollupName(), rollupColumns, schemaVersion,
                    rollupSchemaHash, rollupShortKeyColumnCount, rollupIndexStorageType, keysType);
        }

        // analyze version info
        Long version = null;
        try {
            version = PropertyAnalyzer.analyzeVersionInfo(properties);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        Preconditions.checkNotNull(version);

        // get storage format
        TStorageFormat storageFormat = TStorageFormat.DEFAULT; // default means it's up to BE's config
        try {
            storageFormat = PropertyAnalyzer.analyzeStorageFormat(properties);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        table.setStorageFormat(storageFormat);

        // get storage volume
        String storageVolume = RunMode.allowCreateLakeTable() ? "default" : "local";
        table.setStorageVolume(storageVolume);

        // get compression type
        TCompressionType compressionType = TCompressionType.LZ4_FRAME;
        try {
            compressionType = PropertyAnalyzer.analyzeCompressionType(properties);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        table.setCompressionType(compressionType);

        // partition live number
        int partitionLiveNumber;
        if (properties != null && properties.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_LIVE_NUMBER)) {
            try {
                partitionLiveNumber = PropertyAnalyzer.analyzePartitionLiveNumber(properties, true);
            } catch (AnalysisException e) {
                throw new DdlException(e.getMessage());
            }
            table.setPartitionLiveNumber(partitionLiveNumber);
        }

        try {
            processConstraint(db, table, properties);
        } catch (AnalysisException e) {
            throw new DdlException(
                    String.format("processing constraint failed when creating table:%s. exception msg:%s",
                            table.getName(), e.getMessage()), e);
        }

        if (table.isCloudNativeTable() && partitionInfo.getType() == PartitionType.LIST) {
            throw new DdlException("Do not support create list partition Cloud Native table");
        }

        // a set to record every new tablet created when create table
        // if failed in any step, use this set to do clear things
        Set<Long> tabletIdSet = new HashSet<Long>();

        boolean createTblSuccess = false;
        boolean addToColocateGroupSuccess = false;
        // create partition
        try {
            // do not create partition for external table
            if (table.isOlapOrCloudNativeTable()) {
                if (partitionInfo.getType() == PartitionType.UNPARTITIONED) {
                    if (properties != null && !properties.isEmpty()) {
                        // here, all properties should be checked
                        throw new DdlException("Unknown properties: " + properties);
                    }
                    // this is a 1-level partitioned table, use table name as partition name
                    long partitionId = partitionNameToId.get(tableName);
                    Partition partition = createPartition(db, table, partitionId, tableName, version, tabletIdSet);
                    buildPartitions(db, table, Collections.singletonList(partition));
                    table.addPartition(partition);
                } else if (partitionInfo.isRangePartition() || partitionInfo.getType() == PartitionType.LIST) {
                    try {
                        // just for remove entries in stmt.getProperties(),
                        // and then check if there still has unknown properties
                        boolean hasMedium = false;
                        if (properties != null) {
                            hasMedium = properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM);
                        }
                        DataProperty dataProperty = PropertyAnalyzer.analyzeDataProperty(properties,
                                DataProperty.getInferredDefaultDataProperty(), false);
                        DynamicPartitionUtil.checkAndSetDynamicPartitionProperty(table, properties);
                        if (table.dynamicPartitionExists() && table.getColocateGroup() != null) {
                            HashDistributionInfo info = (HashDistributionInfo) distributionInfo;
                            if (info.getBucketNum() !=
                                    table.getTableProperty().getDynamicPartitionProperty().getBuckets()) {
                                throw new DdlException("dynamic_partition.buckets should equal the distribution buckets"
                                        + " if creating a colocate table");
                            }
                        }
                        if (hasMedium) {
                            table.setStorageMedium(dataProperty.getStorageMedium());
                        }
                        if (properties != null && !properties.isEmpty()) {
                            // here, all properties should be checked
                            throw new DdlException("Unknown properties: " + properties);
                        }
                    } catch (AnalysisException e) {
                        throw new DdlException(e.getMessage());
                    }

                    // this is a 2-level partitioned tables
                    List<Partition> partitions = new ArrayList<>(partitionNameToId.size());
                    for (Map.Entry<String, Long> entry : partitionNameToId.entrySet()) {
                        Partition partition = createPartition(db, table, entry.getValue(), entry.getKey(), version,
                                tabletIdSet);
                        partitions.add(partition);
                    }
                    // It's ok if partitions is empty.
                    buildPartitions(db, table, partitions);
                    for (Partition partition : partitions) {
                        table.addPartition(partition);
                    }
                } else {
                    throw new DdlException("Unsupported partition method: " + partitionInfo.getType().name());
                }
                // if binlog_enable is true when creating table,
                // then set binlogAvailableVersion without statistics through reportHandler
                if (table.isBinlogEnabled()) {
                    Map<String, String> binlogAvailableVersion = table.buildBinlogAvailableVersion();
                    table.setBinlogAvailableVersion(binlogAvailableVersion);
                    LOG.info("set binlog available version when create table, tableName : {}, partitions : {}",
                            tableName, binlogAvailableVersion.toString());
                }
            }

            // process lake table colocation properties, after partition and tablet creation
            processColocationProperties(colocateGroup, db, table, true /* expectLakeTable */);

            // check database exists again, because database can be dropped when creating table
            if (!tryLock(false)) {
                throw new DdlException("Failed to acquire globalStateMgr lock. Try again");
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
                    throw new DdlException("database has been dropped when creating table");
                }
            } finally {
                unlock();
            }
            createTblSuccess = db.createTableWithLock(table, false);
            if (!createTblSuccess) {
                if (!stmt.isSetIfNotExists()) {
                    ErrorReport
                            .reportDdlException(ErrorCode.ERR_CANT_CREATE_TABLE, tableName, "table already exists");
                } else {
                    LOG.info("Create table[{}] which already exists", tableName);
                    return;
                }
            }

            // NOTE: The table has been added to the database, and the following procedure cannot throw exception.

            // we have added these index to memory, only need to persist here
            if (colocateTableIndex.isColocateTable(tableId)) {
                ColocateTableIndex.GroupId groupId = colocateTableIndex.getGroup(tableId);
                List<List<Long>> backendsPerBucketSeq = colocateTableIndex.getBackendsPerBucketSeq(groupId);
                ColocatePersistInfo info =
                        ColocatePersistInfo.createForAddTable(groupId, tableId, backendsPerBucketSeq);
                GlobalStateMgr.getCurrentState().getEditLog().logColocateAddTable(info);
                addToColocateGroupSuccess = true;
            }
            LOG.info("Successfully create table[{};{}]", tableName, tableId);
            DynamicPartitionUtil.registerOrRemovePartitionScheduleInfo(db.getId(), table);
        } finally {
            if (!createTblSuccess) {
                for (Long tabletId : tabletIdSet) {
                    stateMgr.getCurrentInvertedIndex().deleteTablet(tabletId);
                }
            }
            // only remove from memory, because we have not persist it
            if (colocateTableIndex.isColocateTable(tableId) && !addToColocateGroupSuccess) {
                colocateTableIndex.removeTable(tableId, table, false /* isReplay */);
            }
        }
        if (Config.dynamic_partition_enable && table.getTableProperty().getDynamicPartitionProperty().getEnable()) {
            new Thread(() -> {
                try {
                    GlobalStateMgr.getCurrentState().getDynamicPartitionScheduler()
                            .executeDynamicPartitionForTable(db.getId(), tableId);
                } catch (Exception ex) {
                    LOG.warn("Some problems were encountered in the process of triggering " +
                            "the execution of dynamic partitioning", ex);
                }
            }, "BackgroundDynamicPartitionThread").start();
        }
    }

    private void processConstraint(
            Database db, OlapTable olapTable, Map<String, String> properties) throws AnalysisException {
        List<UniqueConstraint> uniqueConstraints = PropertyAnalyzer.analyzeUniqueConstraint(properties, db, olapTable);
        if (uniqueConstraints != null) {
            olapTable.setUniqueConstraints(uniqueConstraints);
        }

        List<ForeignKeyConstraint> foreignKeyConstraints =
                PropertyAnalyzer.analyzeForeignKeyConstraint(properties, db, olapTable);
        if (foreignKeyConstraints != null) {
            olapTable.setForeignKeyConstraints(foreignKeyConstraints);
        }
    }

    private void createMysqlTable(Database db, CreateTableStmt stmt) throws DdlException {
        String tableName = stmt.getTableName();

        List<Column> columns = stmt.getColumns();

        long tableId = GlobalStateMgr.getCurrentState().getNextId();
        MysqlTable mysqlTable = new MysqlTable(tableId, tableName, columns, stmt.getProperties());
        mysqlTable.setComment(stmt.getComment());

        // check database exists again, because database can be dropped when creating table
        if (!tryLock(false)) {
            throw new DdlException("Failed to acquire globalStateMgr lock. " +
                    "Try again or increasing value of `catalog_try_lock_timeout_ms` configuration.");
        }
        try {
            if (getDb(db.getId()) == null) {
                throw new DdlException("database has been dropped when creating table");
            }
            if (!db.createTableWithLock(mysqlTable, false)) {
                if (!stmt.isSetIfNotExists()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_CANT_CREATE_TABLE, tableName, "table already exists");
                } else {
                    LOG.info("Create table[{}] which already exists", tableName);
                    return;
                }
            }
        } finally {
            unlock();
        }

        LOG.info("Successfully create table[{}-{}]", tableName, tableId);
    }

    private void createEsTable(Database db, CreateTableStmt stmt) throws DdlException {
        String tableName = stmt.getTableName();

        // create columns
        List<Column> baseSchema = stmt.getColumns();
        validateColumns(baseSchema);

        // create partition info
        PartitionDesc partitionDesc = stmt.getPartitionDesc();
        PartitionInfo partitionInfo = null;
        Map<String, Long> partitionNameToId = Maps.newHashMap();
        if (partitionDesc != null) {
            partitionInfo = partitionDesc.toPartitionInfo(baseSchema, partitionNameToId, false);
        } else {
            long partitionId = getNextId();
            // use table name as single partition name
            partitionNameToId.put(tableName, partitionId);
            partitionInfo = new SinglePartitionInfo();
        }

        long tableId = GlobalStateMgr.getCurrentState().getNextId();
        EsTable esTable = new EsTable(tableId, tableName, baseSchema, stmt.getProperties(), partitionInfo);
        esTable.setComment(stmt.getComment());

        // check database exists again, because database can be dropped when creating table
        if (!tryLock(false)) {
            throw new DdlException("Failed to acquire globalStateMgr lock. Try again");
        }
        try {
            if (getDb(db.getId()) == null) {
                throw new DdlException("database has been dropped when creating table");
            }
            if (!db.createTableWithLock(esTable, false)) {
                if (!stmt.isSetIfNotExists()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_CANT_CREATE_TABLE, tableName, "table already exists");
                } else {
                    LOG.info("create table[{}] which already exists", tableName);
                    return;
                }
            }
        } finally {
            unlock();
        }

        LOG.info("successfully create table {} with id {}", tableName, tableId);
    }

    private void createHiveTable(Database db, CreateTableStmt stmt) throws DdlException {
        Table hiveTable = TableFactory.createTable(stmt, Table.TableType.HIVE);
        registerTable(db, hiveTable, stmt);
        LOG.info("successfully create table[{}-{}]", stmt.getTableName(), hiveTable.getId());
    }

    private void createIcebergTable(Database db, CreateTableStmt stmt) throws DdlException {
        Table icebergTable = TableFactory.createTable(stmt, Table.TableType.ICEBERG);
        registerTable(db, icebergTable, stmt);
        LOG.info("successfully create table[{}-{}]", stmt.getTableName(), icebergTable.getId());
    }

    private void createHudiTable(Database db, CreateTableStmt stmt) throws DdlException {
        Table hudiTable = TableFactory.createTable(stmt, Table.TableType.HUDI);
        registerTable(db, hudiTable, stmt);
        LOG.info("successfully create table[{}-{}]", stmt.getTableName(), hudiTable.getId());
    }

    private void createFileTable(Database db, CreateTableStmt stmt) throws DdlException {
        Table fileTable = TableFactory.createTable(stmt, Table.TableType.FILE);
        registerTable(db, fileTable, stmt);
        LOG.info("successfully create table[{}-{}]", stmt.getTableName(), fileTable.getId());
    }

    private void createJDBCTable(Database db, CreateTableStmt stmt) throws DdlException {
        String tableName = stmt.getTableName();
        List<Column> columns = stmt.getColumns();
        Map<String, String> properties = stmt.getProperties();
        long tableId = getNextId();
        JDBCTable jdbcTable = new JDBCTable(tableId, tableName, columns, properties);

        if (!tryLock(false)) {
            throw new DdlException("Failed to acquire globalStateMgr lock. Try again");
        }

        try {
            if (getDb(db.getId()) == null) {
                throw new DdlException("database has been dropped when creating table");
            }
            if (!db.createTableWithLock(jdbcTable, false)) {
                if (!stmt.isSetIfNotExists()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_CANT_CREATE_TABLE, tableName, "table already exists");
                } else {
                    LOG.info("create table [{}] which already exists", tableName);
                    return;
                }
            }
        } finally {
            unlock();
        }

        LOG.info("successfully create jdbc table[{}-{}]", tableName, tableId);
    }

    private void registerTable(Database db, Table table, CreateTableStmt stmt) throws DdlException {
        // check database exists again, because database can be dropped when creating table
        if (!tryLock(false)) {
            throw new DdlException("Failed to acquire globalStateMgr lock. Try again");
        }
        try {
            if (getDb(db.getId()) == null) {
                throw new DdlException("Database has been dropped when creating table");
            }
            if (!db.createTableWithLock(table, false)) {
                if (!stmt.isSetIfNotExists()) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_CANT_CREATE_TABLE, table.getName(),
                            "table already exists");
                } else {
                    LOG.info("Create table[{}] which already exists", table.getName());
                }
            }
        } finally {
            unlock();
        }
    }

    public void replayCreateTable(String dbName, Table table) throws DdlException {
        Database db = this.fullNameToDb.get(dbName);
        db.createTableWithLock(table, true);
        if (!isCheckpointThread()) {
            // add to inverted index
            if (table.isOlapOrCloudNativeTable()) {
                TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentInvertedIndex();
                OlapTable olapTable = (OlapTable) table;
                long dbId = db.getId();
                long tableId = table.getId();
                for (Partition partition : olapTable.getAllPartitions()) {
                    long partitionId = partition.getId();
                    TStorageMedium medium = olapTable.getPartitionInfo().getDataProperty(
                            partitionId).getStorageMedium();
                    for (MaterializedIndex mIndex : partition
                            .getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                        long indexId = mIndex.getId();
                        int schemaHash = olapTable.getSchemaHashByIndexId(indexId);
                        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, schemaHash, medium,
                                table.isCloudNativeTableOrMaterializedView());
                        for (Tablet tablet : mIndex.getTablets()) {
                            long tabletId = tablet.getId();
                            invertedIndex.addTablet(tabletId, tabletMeta);
                            if (table.isOlapTable()) {
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
    }

    public void replayCreateMaterializedView(String dbName, MaterializedView materializedView) {
        Database db = this.fullNameToDb.get(dbName);
        db.createMaterializedWithLock(materializedView, true);

        if (!isCheckpointThread()) {
            // add to inverted index
            TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentInvertedIndex();
            long dbId = db.getId();
            long mvId = materializedView.getId();
            for (Partition partition : materializedView.getAllPartitions()) {
                long partitionId = partition.getId();
                TStorageMedium medium = materializedView.getPartitionInfo().getDataProperty(
                        partitionId).getStorageMedium();
                for (MaterializedIndex mIndex : partition.getMaterializedIndices(
                        MaterializedIndex.IndexExtState.ALL)) {
                    long indexId = mIndex.getId();
                    int schemaHash = materializedView.getSchemaHashByIndexId(indexId);
                    TabletMeta tabletMeta = new TabletMeta(dbId, mvId, partitionId, indexId, schemaHash, medium);
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
            DynamicPartitionUtil.registerOrRemovePartitionTTLTable(dbId, materializedView);
        }
    }

    private void createLakeTablets(OlapTable table, long partitionId, long shardGroupId, MaterializedIndex index,
                                   DistributionInfo distributionInfo, TabletMeta tabletMeta,
                                   Set<Long> tabletIdSet)
            throws DdlException {
        Preconditions.checkArgument(table.isCloudNativeTableOrMaterializedView());

        DistributionInfo.DistributionInfoType distributionInfoType = distributionInfo.getType();
        if (distributionInfoType != DistributionInfo.DistributionInfoType.HASH) {
            throw new DdlException("Unknown distribution type: " + distributionInfoType);
        }

        Map<String, String> properties = new HashMap<>();
        properties.put(LakeTablet.PROPERTY_KEY_TABLE_ID, Long.toString(table.getId()));
        properties.put(LakeTablet.PROPERTY_KEY_PARTITION_ID, Long.toString(partitionId));
        properties.put(LakeTablet.PROPERTY_KEY_INDEX_ID, Long.toString(index.getId()));
        int bucketNum = distributionInfo.getBucketNum();
        List<Long> shardIds = stateMgr.getStarOSAgent().createShards(bucketNum,
                table.getPartitionFilePathInfo(), table.getPartitionFileCacheInfo(partitionId), shardGroupId, properties);
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
        if (distributionInfoType != DistributionInfo.DistributionInfoType.HASH) {
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
                // Optimization: wait first time, before global lock
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
                                chosenBackendIdBySeq(replicationNum, tabletMeta.getStorageMedium());
                    } else {
                        try {
                            chosenBackendIds = chosenBackendIdBySeq(replicationNum);
                        } catch (DdlException ex) {
                            throw new DdlException(String.format("%s, table=%s, default_replication_num=%d",
                                    ex.getMessage(), table.getName(), Config.default_replication_num));
                        }
                    }
                    backendsPerBucketSeq.add(chosenBackendIds);
                } else {
                    // get backends from existing backend sequence
                    chosenBackendIds = backendsPerBucketSeq.get(i);
                }

                // create replicas
                for (long backendId : chosenBackendIds) {
                    long replicaId = getNextId();
                    Replica replica = new Replica(replicaId, backendId, replicaState, version,
                            tabletMeta.getOldSchemaHash());
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
    private List<Long> chosenBackendIdBySeq(int replicationNum, TStorageMedium storageMedium)
            throws DdlException {
        List<Long> chosenBackendIds = GlobalStateMgr.getCurrentSystemInfo().seqChooseBackendIdsByStorageMedium(replicationNum,
                true, true, storageMedium);
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

    private List<Long> chosenBackendIdBySeq(int replicationNum) throws DdlException {
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentSystemInfo();
        List<Long> chosenBackendIds = systemInfoService.seqChooseBackendIds(replicationNum, true, true);
        if (!CollectionUtils.isEmpty(chosenBackendIds)) {
            return chosenBackendIds;
        } else if (replicationNum > 1) {
            List<Long> backendIds = systemInfoService.getBackendIds(true);
            throw new DdlException(
                    String.format("Table replication num should be less than of equal to the number of available BE nodes. "
                    + "You can change this default by setting the replication_num table properties. "
                    + "Current alive backend is [%s]. ", Joiner.on(",").join(backendIds)));
        } else {
            throw new DdlException("No alive nodes");
        }
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
        db.dropTable(tableName, stmt.isSetIfExists(), stmt.isForceDrop());
    }

    public void sendDropTabletTasks(HashMap<Long, AgentBatchTask> batchTaskMap) {
        int numDropTaskPerBe = Config.max_agent_tasks_send_per_be;
        for (Map.Entry<Long, AgentBatchTask> entry : batchTaskMap.entrySet()) {
            AgentBatchTask originTasks = entry.getValue();
            if (originTasks.getTaskNum() > numDropTaskPerBe) {
                AgentBatchTask partTask = new AgentBatchTask();
                List<AgentTask> allTasks = originTasks.getAllTasks();
                int curTask = 1;
                for (AgentTask task : allTasks) {
                    partTask.addTask(task);
                    if (curTask++ > numDropTaskPerBe) {
                        AgentTaskExecutor.submit(partTask);
                        curTask = 1;
                        partTask = new AgentBatchTask();
                        ThreadUtil.sleepAtLeastIgnoreInterrupts(1000);
                    }
                }
                if (partTask.getAllTasks().size() > 0) {
                    AgentTaskExecutor.submit(partTask);
                }
            } else {
                AgentTaskExecutor.submit(originTasks);
            }
        }
    }

    public void replayDropTable(Database db, long tableId, boolean isForceDrop) {
        Runnable runnable;
        db.writeLock();
        try {
            runnable = db.unprotectDropTable(tableId, isForceDrop, true);
        } finally {
            db.writeUnlock();
        }
        if (runnable != null) {
            runnable.run();
        }
    }

    public void replayEraseTable(long tableId) {
        recycleBin.replayEraseTable(tableId);
    }

    public void replayEraseMultiTables(MultiEraseTableInfo multiEraseTableInfo) {
        List<Long> tableIds = multiEraseTableInfo.getTableIds();
        for (Long tableId : tableIds) {
            recycleBin.replayEraseTable(tableId);
        }
    }

    public void replayRecoverTable(RecoverInfo info) {
        long dbId = info.getDbId();
        Database db = getDb(dbId);
        db.writeLock();
        try {
            recycleBin.replayRecoverTable(db, info.getTableId());
        } finally {
            db.writeUnlock();
        }
    }

    public void replayAddReplica(ReplicaPersistInfo info) {
        Database db = getDbIncludeRecycleBin(info.getDbId());
        if (db == null) {
            LOG.warn("replay add replica failed, db is null, info: {}", info);
            return;
        }
        db.writeLock();
        try {
            OlapTable olapTable = (OlapTable) getTableIncludeRecycleBin(db, info.getTableId());
            if (olapTable == null) {
                LOG.warn("replay add replica failed, table is null, info: {}", info);
                return;
            }
            Partition partition = getPartitionIncludeRecycleBin(olapTable, info.getPartitionId());
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
            db.writeUnlock();
        }
    }

    public void replayUpdateReplica(ReplicaPersistInfo info) {
        Database db = getDbIncludeRecycleBin(info.getDbId());
        if (db == null) {
            LOG.warn("replay update replica failed, db is null, info: {}", info);
            return;
        }
        db.writeLock();
        try {
            OlapTable olapTable = (OlapTable) getTableIncludeRecycleBin(db, info.getTableId());
            if (olapTable == null) {
                LOG.warn("replay update replica failed, table is null, info: {}", info);
                return;
            }
            Partition partition = getPartitionIncludeRecycleBin(olapTable, info.getPartitionId());
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
            db.writeUnlock();
        }
    }

    public void replayDeleteReplica(ReplicaPersistInfo info) {
        Database db = getDbIncludeRecycleBin(info.getDbId());
        if (db == null) {
            LOG.warn("replay delete replica failed, db is null, info: {}", info);
            return;
        }
        db.writeLock();
        try {
            OlapTable olapTable = (OlapTable) getTableIncludeRecycleBin(db, info.getTableId());
            if (olapTable == null) {
                LOG.warn("replay delete replica failed, table is null, info: {}", info);
                return;
            }
            Partition partition = getPartitionIncludeRecycleBin(olapTable, info.getPartitionId());
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
            db.writeUnlock();
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

    public Table getTableIncludeRecycleBin(Database db, long tableId) {
        Table table = db.getTable(tableId);
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
    public List<String> listDbNames() {
        return Lists.newArrayList(fullNameToDb.keySet());
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

            db.readLock();
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
                                && olapTable.getState() == OlapTable.OlapTableState.NORMAL
                                && olapTable.getKeysType() != KeysType.PRIMARY_KEYS) {
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
                db.readUnlock();
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
            if (!db.tryWriteLock(Database.TRY_LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                LOG.warn("try get db {} writelock but failed when hecking backend storage medium", dbId);
                continue;
            }
            Preconditions.checkState(db.isWriteLockHeldByCurrentThread());
            try {
                for (Long tableId : tableIdToPartitionIds.keySet()) {
                    Table table = db.getTable(tableId);
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
                            partitionInfo.setDataProperty(partition.getId(), hdd);
                            storageMediumMap.put(partitionId, TStorageMedium.HDD);
                            LOG.debug("partition[{}-{}-{}] storage medium changed from SSD to HDD",
                                    dbId, tableId, partitionId);

                            // log
                            ModifyPartitionInfo info =
                                    new ModifyPartitionInfo(db.getId(), olapTable.getId(),
                                            partition.getId(),
                                            hdd,
                                            (short) -1,
                                            partitionInfo.getIsInMemory(partition.getId()));
                            GlobalStateMgr.getCurrentState().getEditLog().logModifyPartition(info);
                        }
                    } // end for partitions
                } // end for tables
            } finally {
                db.writeUnlock();
            }
        } // end for dbs
        return storageMediumMap;
    }

    /*
     * used for handling AlterTableStmt (for client is the ALTER TABLE command).
     * including SchemaChangeHandler and RollupHandler
     */
    @Override
    public void alterTable(AlterTableStmt stmt) throws UserException {
        stateMgr.getAlterJobMgr().processAlterTable(stmt);
    }

    /**
     * used for handling AlterViewStmt (the ALTER VIEW command).
     */
    @Override
    public void alterView(AlterViewStmt stmt) throws UserException {
        stateMgr.getAlterJobMgr().processAlterView(stmt, ConnectContext.get());
    }

    @Override
    public void createMaterializedView(CreateMaterializedViewStmt stmt)
            throws AnalysisException, DdlException {
        stateMgr.getAlterJobMgr().processCreateSynchronousMaterializedView(stmt);
    }

    // TODO(murphy) refactor it into MVManager
    @Override
    public void createMaterializedView(CreateMaterializedViewStatement stmt)
            throws DdlException {
        // check mv exists,name must be different from view/mv/table which exists in metadata
        String mvName = stmt.getTableName().getTbl();
        String dbName = stmt.getTableName().getDb();
        LOG.debug("Begin create materialized view: {}", mvName);
        // check if db exists
        Database db = this.getDb(dbName);
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }
        // check if table exists in db
        db.readLock();
        try {
            if (db.getTable(mvName) != null) {
                if (stmt.isIfNotExists()) {
                    LOG.info("Create materialized view [{}] which already exists", mvName);
                    return;
                } else {
                    ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, mvName);
                }
            }
        } finally {
            db.readUnlock();
        }
        // create columns
        List<Column> baseSchema = stmt.getMvColumnItems();
        validateColumns(baseSchema);
        // create partition info
        PartitionInfo partitionInfo = buildPartitionInfo(stmt);
        // create distribution info
        DistributionDesc distributionDesc = stmt.getDistributionDesc();
        Preconditions.checkNotNull(distributionDesc);
        DistributionInfo distributionInfo = distributionDesc.toDistributionInfo(baseSchema);
        // create refresh scheme
        MaterializedView.MvRefreshScheme mvRefreshScheme;
        RefreshSchemeDesc refreshSchemeDesc = stmt.getRefreshSchemeDesc();
        if (refreshSchemeDesc.getType() == MaterializedView.RefreshType.ASYNC) {
            mvRefreshScheme = new MaterializedView.MvRefreshScheme();
            AsyncRefreshSchemeDesc asyncRefreshSchemeDesc = (AsyncRefreshSchemeDesc) refreshSchemeDesc;
            MaterializedView.AsyncRefreshContext asyncRefreshContext = mvRefreshScheme.getAsyncRefreshContext();
            asyncRefreshContext.setStartTime(Utils.getLongFromDateTime(asyncRefreshSchemeDesc.getStartTime()));
            asyncRefreshContext.setDefineStartTime(asyncRefreshSchemeDesc.isDefineStartTime());
            if (asyncRefreshSchemeDesc.getIntervalLiteral() != null) {
                asyncRefreshContext.setStep(
                        ((IntLiteral) asyncRefreshSchemeDesc.getIntervalLiteral().getValue()).getValue());
                asyncRefreshContext.setTimeUnit(
                        asyncRefreshSchemeDesc.getIntervalLiteral().getUnitIdentifier().getDescription());
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
        } else if (refreshSchemeDesc.getType() == MaterializedView.RefreshType.SYNC) {
            mvRefreshScheme = new MaterializedView.MvRefreshScheme();
            mvRefreshScheme.setType(MaterializedView.RefreshType.SYNC);
        } else if (refreshSchemeDesc.getType().equals(MaterializedView.RefreshType.MANUAL)) {
            mvRefreshScheme = new MaterializedView.MvRefreshScheme();
            mvRefreshScheme.setType(MaterializedView.RefreshType.MANUAL);
        } else {
            mvRefreshScheme = new MaterializedView.MvRefreshScheme();
            mvRefreshScheme.setType(MaterializedView.RefreshType.INCREMENTAL);
        }
        mvRefreshScheme.setMoment(refreshSchemeDesc.getMoment());
        // create mv
        long mvId = GlobalStateMgr.getCurrentState().getNextId();
        MaterializedView materializedView;
        if (RunMode.getCurrentRunMode().isAllowCreateOlapTable()) {
            if (refreshSchemeDesc.getType().equals(MaterializedView.RefreshType.INCREMENTAL)) {
                materializedView = MaterializedViewMgr.getInstance().createSinkTable(stmt, partitionInfo, mvId, db.getId());
                materializedView.setMaintenancePlan(stmt.getMaintenancePlan());
            } else {
                materializedView =
                        new MaterializedView(mvId, db.getId(), mvName, baseSchema, stmt.getKeysType(), partitionInfo,
                                distributionInfo, mvRefreshScheme);
            }
        } else {
            Preconditions.checkState(RunMode.getCurrentRunMode().isAllowCreateLakeTable());
            if (refreshSchemeDesc.getType().equals(MaterializedView.RefreshType.INCREMENTAL)) {
                throw new DdlException("Incremental materialized view in shared_data mode is not supported");
            }

            materializedView =
                    new LakeMaterializedView(mvId, db.getId(), mvName, baseSchema, stmt.getKeysType(), partitionInfo,
                            distributionInfo, mvRefreshScheme);
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
        // set partitionRefTableExprs
        materializedView.setPartitionRefTableExprs(Lists.newArrayList(stmt.getPartitionRefTableExpr()));
        // set base index id
        long baseIndexId = getNextId();
        materializedView.setBaseIndexId(baseIndexId);
        // set base index meta
        int schemaVersion = 0;
        int schemaHash = Util.schemaHash(schemaVersion, baseSchema, null, 0d);
        short shortKeyColumnCount = GlobalStateMgr.calcShortKeyColumnCount(baseSchema, null);
        TStorageType baseIndexStorageType = TStorageType.COLUMN;
        materializedView.setIndexMeta(baseIndexId, mvName, baseSchema, schemaVersion, schemaHash,
                shortKeyColumnCount, baseIndexStorageType, stmt.getKeysType());

        // validate optHints
        Map<String, String> optHints = null;
        QueryRelation queryRelation = stmt.getQueryStatement().getQueryRelation();
        if (queryRelation instanceof SelectRelation) {
            SelectRelation selectRelation = (SelectRelation) queryRelation;
            optHints = selectRelation.getSelectList().getOptHints();
            if (optHints != null && !optHints.isEmpty()) {
                SessionVariable sessionVariable = VariableMgr.newSessionVariable();
                for (String key : optHints.keySet()) {
                    VariableMgr.setSystemVariable(sessionVariable,
                            new SystemVariable(key, new StringLiteral(optHints.get(key))), true);
                }
            }
        }

        // process properties
        Map<String, String> properties = stmt.getProperties();
        if (properties == null) {
            properties = Maps.newHashMap();
        }
        // set replication_num
        short replicationNum = RunMode.defaultReplicationNum();
        try {
            if (properties.containsKey(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM)) {
                replicationNum = PropertyAnalyzer.analyzeReplicationNum(properties, replicationNum);
                materializedView.setReplicationNum(replicationNum);
            }
            if (properties.containsKey(PropertyAnalyzer.PROPERTIES_MV_REWRITE_STALENESS_SECOND)) {
                Integer maxMVRewriteStaleness = PropertyAnalyzer.analyzeMVRewriteStaleness(properties);
                materializedView.setMaxMVRewriteStaleness(maxMVRewriteStaleness);
            }
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage(), e);
        }
        // replicated storage
        materializedView.setEnableReplicatedStorage(
                PropertyAnalyzer.analyzeBooleanProp(
                        properties, PropertyAnalyzer.PROPERTIES_REPLICATED_STORAGE,
                        Config.enable_replicated_storage_as_default_engine));
        boolean isNonPartitioned = partitionInfo.getType() == PartitionType.UNPARTITIONED;
        DataProperty dataProperty = analyzeMVDataProperties(db, materializedView, properties, isNonPartitioned);

        boolean createMvSuccess;
        Set<Long> tabletIdSet = new HashSet<>();
        // process single partition info
        if (isNonPartitioned) {
            long partitionId = GlobalStateMgr.getCurrentState().getNextId();
            Preconditions.checkNotNull(dataProperty);
            partitionInfo.setDataProperty(partitionId, dataProperty);
            partitionInfo.setReplicationNum(partitionId, replicationNum);
            partitionInfo.setIsInMemory(partitionId, false);
            partitionInfo.setTabletType(partitionId, TTabletType.TABLET_TYPE_DISK);
            StorageInfo storageInfo = materializedView.getTableProperty().getStorageInfo();
            partitionInfo.setStorageCacheInfo(partitionId,
                    storageInfo == null ? null : storageInfo.getStorageCacheInfo());
            Long version = Partition.PARTITION_INIT_VERSION;
            Partition partition = createPartition(db, materializedView, partitionId, mvName, version, tabletIdSet);
            buildPartitions(db, materializedView, Collections.singletonList(partition));
            materializedView.addPartition(partition);
        }

        MaterializedViewMgr.getInstance().prepareMaintenanceWork(stmt, materializedView);

        // check database exists again, because database can be dropped when creating table
        if (!tryLock(false)) {
            throw new DdlException("Failed to acquire globalStateMgr lock. Try again");
        }
        try {
            if (getDb(db.getId()) == null) {
                throw new DdlException("database has been dropped when creating materialized view");
            }
        } finally {
            unlock();
        }
        db.writeLock();
        try {
            if (!db.isExist()) {
                throw new DdlException("Database has been dropped when creating materialized view");
            }
            createMvSuccess = db.createMaterializedWithLock(materializedView, false);
            if (!createMvSuccess) {
                for (Long tabletId : tabletIdSet) {
                    GlobalStateMgr.getCurrentInvertedIndex().deleteTablet(tabletId);
                }
                if (!stmt.isIfNotExists()) {
                    ErrorReport
                            .reportDdlException(ErrorCode.ERR_CANT_CREATE_TABLE, materializedView,
                                    "Materialized view already exists");
                } else {
                    LOG.info("Create materialized view[{}] which already exists", materializedView);
                    return;
                }
            }
        } finally {
            db.writeUnlock();
        }
        LOG.info("Successfully create materialized view [{}:{}]", mvName, materializedView.getMvId());

        // NOTE: The materialized view has been added to the database, and the following procedure cannot throw exception.
        createTaskForMaterializedView(dbName, materializedView, optHints);
        DynamicPartitionUtil.registerOrRemovePartitionTTLTable(db.getId(), materializedView);
    }

    private DataProperty analyzeMVDataProperties(Database db,
                                                 MaterializedView materializedView,
                                                 Map<String, String> properties,
                                                 boolean isNonPartitioned) throws DdlException {
        DataProperty dataProperty;
        try {

            // bloom_filter_columns
            if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BF_COLUMNS)) {
                List<Column> baseSchema = materializedView.getColumns();
                Set<String> bfColumns = PropertyAnalyzer.analyzeBloomFilterColumns(properties, baseSchema,
                        materializedView.getKeysType() == KeysType.PRIMARY_KEYS);
                if (bfColumns != null && bfColumns.isEmpty()) {
                    bfColumns = null;
                }
                double bfFpp = PropertyAnalyzer.analyzeBloomFilterFpp(properties);
                if (bfColumns != null && bfFpp == 0) {
                    bfFpp = FeConstants.DEFAULT_BLOOM_FILTER_FPP;
                } else if (bfColumns == null) {
                    bfFpp = 0;
                }
                materializedView.setBloomFilterInfo(bfColumns, bfFpp);
            }
            // set storage medium
            boolean hasMedium = properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM);
            dataProperty = PropertyAnalyzer.analyzeDataProperty(properties,
                    DataProperty.getInferredDefaultDataProperty(), false);
            if (hasMedium && dataProperty.getStorageMedium() == TStorageMedium.SSD) {
                materializedView.setStorageMedium(dataProperty.getStorageMedium());
                // set storage cooldown time into table property,
                // because we don't have property in MaterializedView
                materializedView.getTableProperty().getProperties()
                        .put(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TIME,
                                String.valueOf(dataProperty.getCooldownTimeMs()));
            }
            if (properties.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_TTL_NUMBER)) {
                int number = PropertyAnalyzer.analyzePartitionTimeToLive(properties);
                materializedView.getTableProperty().getProperties()
                        .put(PropertyAnalyzer.PROPERTIES_PARTITION_TTL_NUMBER, String.valueOf(number));
                materializedView.getTableProperty().setPartitionTTLNumber(number);
                if (isNonPartitioned) {
                    throw new AnalysisException(PropertyAnalyzer.PROPERTIES_PARTITION_TTL_NUMBER
                            + " does not support non-partitioned materialized view.");
                }
            }
            if (properties.containsKey(PropertyAnalyzer.PROPERTIES_AUTO_REFRESH_PARTITIONS_LIMIT)) {
                int limit = PropertyAnalyzer.analyzeAutoRefreshPartitionsLimit(properties, materializedView);
                materializedView.getTableProperty().getProperties()
                        .put(PropertyAnalyzer.PROPERTIES_AUTO_REFRESH_PARTITIONS_LIMIT, String.valueOf(limit));
                materializedView.getTableProperty().setAutoRefreshPartitionsLimit(limit);
                if (isNonPartitioned) {
                    throw new AnalysisException(PropertyAnalyzer.PROPERTIES_AUTO_REFRESH_PARTITIONS_LIMIT
                            + " does not support non-partitioned materialized view.");
                }
            }
            if (properties.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_REFRESH_NUMBER)) {
                int number = PropertyAnalyzer.analyzePartitionRefreshNumber(properties);
                materializedView.getTableProperty().getProperties()
                        .put(PropertyAnalyzer.PROPERTIES_PARTITION_REFRESH_NUMBER, String.valueOf(number));
                materializedView.getTableProperty().setPartitionRefreshNumber(number);
                if (isNonPartitioned) {
                    throw new AnalysisException(PropertyAnalyzer.PROPERTIES_PARTITION_REFRESH_NUMBER
                            + " does not support non-partitioned materialized view.");
                }
            }
            if (properties.containsKey(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES)) {
                List<TableName> tables = PropertyAnalyzer.analyzeExcludedTriggerTables(properties, materializedView);
                StringBuilder tableSb = new StringBuilder();
                for (int i = 1; i <= tables.size(); i++) {
                    TableName tableName = tables.get(i - 1);
                    if (tableName.getDb() == null) {
                        tableSb.append(tableName.getTbl());
                    } else {
                        tableSb.append(tableName.getDb()).append(".").append(tableName.getTbl());
                    }
                    if (i != tables.size()) {
                        tableSb.append(",");
                    }
                }
                materializedView.getTableProperty().getProperties()
                        .put(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES, tableSb.toString());
                materializedView.getTableProperty().setExcludedTriggerTables(tables);
            }
            if (properties.containsKey(PropertyAnalyzer.PROPERTIES_FORCE_EXTERNAL_TABLE_QUERY_REWRITE)) {
                String propertyValue = properties.get(PropertyAnalyzer.PROPERTIES_FORCE_EXTERNAL_TABLE_QUERY_REWRITE);
                TableProperty.QueryRewriteConsistencyMode value = TableProperty.analyzeQueryRewriteMode(propertyValue);
                properties.remove(PropertyAnalyzer.PROPERTIES_FORCE_EXTERNAL_TABLE_QUERY_REWRITE);
                materializedView.getTableProperty().getProperties().
                        put(PropertyAnalyzer.PROPERTIES_FORCE_EXTERNAL_TABLE_QUERY_REWRITE, String.valueOf(value));
                materializedView.getTableProperty().setForceExternalTableQueryRewrite(value);
            }
            if (properties.containsKey(PropertyAnalyzer.PROPERTIES_OLAP_TABLE_QUERY_REWRITE)) {
                String propertyValue = properties.get(PropertyAnalyzer.PROPERTIES_OLAP_TABLE_QUERY_REWRITE);
                TableProperty.QueryRewriteConsistencyMode value = TableProperty.analyzeQueryRewriteMode(propertyValue);
                properties.remove(PropertyAnalyzer.PROPERTIES_OLAP_TABLE_QUERY_REWRITE);
                materializedView.getTableProperty().getProperties().
                        put(PropertyAnalyzer.PROPERTIES_OLAP_TABLE_QUERY_REWRITE, String.valueOf(value));
                materializedView.getTableProperty().setOlapTableQueryRewrite(value);
            }
            if (properties.containsKey(PropertyAnalyzer.PROPERTIES_UNIQUE_CONSTRAINT)) {
                List<UniqueConstraint> uniqueConstraints = PropertyAnalyzer.analyzeUniqueConstraint(properties, db,
                        materializedView);
                materializedView.setUniqueConstraints(uniqueConstraints);
            }
            if (properties.containsKey(PropertyAnalyzer.PROPERTIES_FOREIGN_KEY_CONSTRAINT)) {
                List<ForeignKeyConstraint> foreignKeyConstraints = PropertyAnalyzer.analyzeForeignKeyConstraint(
                        properties, db, materializedView);
                materializedView.setForeignKeyConstraints(foreignKeyConstraints);
            }

            if (materializedView.isCloudNativeMaterializedView()) {
                setLakeStorageInfo(materializedView, properties);
            }

            // session properties
            if (!properties.isEmpty()) {
                // analyze properties
                List<SetListItem> setListItems = Lists.newArrayList();
                for (Map.Entry<String, String> entry : properties.entrySet()) {
                    if (!entry.getKey().startsWith(PropertyAnalyzer.PROPERTIES_MATERIALIZED_VIEW_SESSION_PREFIX)) {
                        throw new AnalysisException("Analyze materialized properties failed " +
                                "because unknown properties: " + properties +
                                ", please add `session.` prefix if you want add session variables for mv(" +
                                "eg, \"session.query_timeout\"=\"30000000\").");
                    }
                    String varKey = entry.getKey().substring(
                            PropertyAnalyzer.PROPERTIES_MATERIALIZED_VIEW_SESSION_PREFIX.length());
                    SystemVariable variable = new SystemVariable(varKey, new StringLiteral(entry.getValue()));
                    setListItems.add(variable);
                }
                SetStmtAnalyzer.analyze(new SetStmt(setListItems), null);

                // set properties if there are no exceptions
                materializedView.getTableProperty().getProperties().putAll(properties);
            }
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage(), e);
        }
        return dataProperty;
    }

    public static PartitionInfo buildPartitionInfo(CreateMaterializedViewStatement stmt) throws DdlException {
        PartitionDesc partitionDesc = stmt.getPartitionExpDesc();
        PartitionInfo partitionInfo;
        if (partitionDesc != null) {
            partitionInfo = partitionDesc.toPartitionInfo(
                    Collections.singletonList(stmt.getPartitionColumn()),
                    Maps.newHashMap(), false);
        } else {
            partitionInfo = new SinglePartitionInfo();
        }
        return partitionInfo;
    }

    private void createTaskForMaterializedView(String dbName, MaterializedView materializedView,
                                               Map<String, String> optHints) throws DdlException {
        MaterializedView.RefreshType refreshType = materializedView.getRefreshScheme().getType();
        MaterializedView.RefreshMoment refreshMoment = materializedView.getRefreshScheme().getMoment();

        if (refreshType.equals(MaterializedView.RefreshType.INCREMENTAL)) {
            MaterializedViewMgr.getInstance().startMaintainMV(materializedView);
            return;
        }

        if (refreshType != MaterializedView.RefreshType.SYNC) {

            Task task = TaskBuilder.buildMvTask(materializedView, dbName);
            TaskBuilder.updateTaskInfo(task, materializedView);

            if (optHints != null) {
                Map<String, String> taskProperties = task.getProperties();
                taskProperties.putAll(optHints);
            }

            TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
            taskManager.createTask(task, false);
            // for event triggered type, run task
            if (refreshMoment.equals(MaterializedView.RefreshMoment.IMMEDIATE)) {
                taskManager.executeTask(task.getName());
            }
        }
    }

    @Override
    public void dropMaterializedView(DropMaterializedViewStmt stmt) throws DdlException, MetaNotFoundException {
        Database db = getDb(stmt.getDbName());
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, stmt.getDbName());
        }
        Table table;
        db.readLock();
        try {
            table = db.getTable(stmt.getMvName());
        } finally {
            db.readUnlock();
        }
        if (table instanceof MaterializedView) {
            if (GlobalStateMgr.getCurrentState().isUsingNewPrivilege()) {
                if (!PrivilegeActions.checkMaterializedViewAction(ConnectContext.get(),
                        stmt.getDbName(),
                        stmt.getMvName(),
                        PrivilegeType.DROP)) {
                    ErrorReport.reportSemanticException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                            "DROP MATERIALIZED VIEW");
                }
            }

            MaterializedView mv = (MaterializedView) table;
            MaterializedViewMgr.getInstance().stopMaintainMV(mv);

            MvId mvId = new MvId(db.getId(), table.getId());
            db.dropTable(table.getName(), stmt.isSetIfExists(), true);
            CachingMvPlanContextBuilder.getInstance().invalidateFromCache(mv);
            List<BaseTableInfo> baseTableInfos = ((MaterializedView) table).getBaseTableInfos();
            if (baseTableInfos != null) {
                for (BaseTableInfo baseTableInfo : baseTableInfos) {
                    Table baseTable = baseTableInfo.getTable();
                    if (baseTable != null) {
                        baseTable.removeRelatedMaterializedView(mvId);
                        if (!baseTable.isNativeTableOrMaterializedView()) {
                            // remove relatedMaterializedViews for connector table
                            GlobalStateMgr.getCurrentState().getConnectorTblMetaInfoMgr().
                                    removeConnectorTableInfo(baseTableInfo.getCatalogName(),
                                            baseTableInfo.getDbName(),
                                            baseTableInfo.getTableIdentifier(),
                                            ConnectorTableInfo.builder().setRelatedMaterializedViews(
                                                    Sets.newHashSet(mvId)).build());
                        }
                    }
                }
            }
            TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
            Task refreshTask = taskManager.getTask(TaskBuilder.getMvTaskName(table.getId()));
            if (refreshTask != null) {
                taskManager.dropTasks(Lists.newArrayList(refreshTask.getId()), false);
            }
        } else {
            stateMgr.getAlterJobMgr().processDropMaterializedView(stmt);
        }
    }

    @Override
    public void alterMaterializedView(AlterMaterializedViewStmt stmt) throws DdlException, MetaNotFoundException {
        stateMgr.getAlterJobMgr().processAlterMaterializedView(stmt);
    }

    private String executeRefreshMvTask(String dbName, MaterializedView materializedView, ExecuteOption executeOption)
            throws DdlException {
        MaterializedView.RefreshType refreshType = materializedView.getRefreshScheme().getType();
        if (refreshType.equals(MaterializedView.RefreshType.INCREMENTAL)) {
            MaterializedViewMgr.getInstance().onTxnPublish(materializedView);
        } else if (refreshType != MaterializedView.RefreshType.SYNC) {
            TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
            final String mvTaskName = TaskBuilder.getMvTaskName(materializedView.getId());
            if (!taskManager.containTask(mvTaskName)) {
                Task task = TaskBuilder.buildMvTask(materializedView, dbName);
                TaskBuilder.updateTaskInfo(task, materializedView);
                taskManager.createTask(task, false);
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
        MaterializedView materializedView = null;
        db.readLock();
        try {
            final Table table = db.getTable(mvName);
            if (table instanceof MaterializedView) {
                materializedView = (MaterializedView) table;
            }
        } finally {
            db.readUnlock();
        }
        if (materializedView == null) {
            throw new MetaNotFoundException(mvName + " is not a materialized view");
        }
        return materializedView;
    }

    public String refreshMaterializedView(String dbName, String mvName, boolean force, PartitionRangeDesc range,
                                          int priority, boolean mergeRedundant, boolean isManual)
            throws DdlException, MetaNotFoundException {
        return refreshMaterializedView(dbName, mvName, force, range, priority, mergeRedundant, isManual, false);
    }

    public String refreshMaterializedView(String dbName, String mvName, boolean force, PartitionRangeDesc range,
                                          int priority, boolean mergeRedundant, boolean isManual, boolean isSync)
            throws DdlException, MetaNotFoundException {
        MaterializedView materializedView = getMaterializedViewToRefresh(dbName, mvName);

        HashMap<String, String> taskRunProperties = new HashMap<>();
        taskRunProperties.put(TaskRun.PARTITION_START, range == null ? null : range.getPartitionStart());
        taskRunProperties.put(TaskRun.PARTITION_END, range == null ? null : range.getPartitionEnd());
        taskRunProperties.put(TaskRun.FORCE, Boolean.toString(force));

        ExecuteOption executeOption = new ExecuteOption(priority, mergeRedundant, taskRunProperties);
        if (isManual) {
            executeOption.setManual();
        }
        executeOption.setSync(isSync);
        return executeRefreshMvTask(dbName, materializedView, executeOption);
    }

    @Override
    public String refreshMaterializedView(RefreshMaterializedViewStatement refreshMaterializedViewStatement)
            throws DdlException, MetaNotFoundException {
        String dbName = refreshMaterializedViewStatement.getMvName().getDb();
        String mvName = refreshMaterializedViewStatement.getMvName().getTbl();
        boolean force = refreshMaterializedViewStatement.isForceRefresh();
        PartitionRangeDesc range =
                refreshMaterializedViewStatement.getPartitionRangeDesc();

        return refreshMaterializedView(dbName, mvName, force, range, Constants.TaskRunPriority.HIGH.value(),
                false, true, refreshMaterializedViewStatement.isSync());
    }

    @Override
    public void cancelRefreshMaterializedView(String dbName, String mvName) throws DdlException, MetaNotFoundException {
        MaterializedView materializedView = getMaterializedViewToRefresh(dbName, mvName);
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        Task refreshTask = taskManager.getTask(TaskBuilder.getMvTaskName(materializedView.getId()));
        if (refreshTask != null) {
            taskManager.killTask(refreshTask.getName(), false);
        }
    }

    /*
     * used for handling CacnelAlterStmt (for client is the CANCEL ALTER
     * command). including SchemaChangeHandler and RollupHandler
     */
    public void cancelAlter(CancelAlterTableStmt stmt) throws DdlException {
        if (stmt.getAlterType() == ShowAlterStmt.AlterType.ROLLUP) {
            stateMgr.getRollupHandler().cancel(stmt);
        } else if (stmt.getAlterType() == ShowAlterStmt.AlterType.COLUMN) {
            stateMgr.getSchemaChangeHandler().cancel(stmt);
        } else if (stmt.getAlterType() == ShowAlterStmt.AlterType.MATERIALIZED_VIEW) {
            stateMgr.getRollupHandler().cancelMV(stmt);
        } else {
            throw new DdlException("Cancel " + stmt.getAlterType() + " does not implement yet");
        }
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
        if (db.getTable(newTableName) != null) {
            throw new DdlException("Table name[" + newTableName + "] is already used");
        }

        olapTable.checkAndSetName(newTableName, false);

        db.dropTable(oldTableName);
        db.createTable(olapTable);
        inactiveRelatedMaterializedView(db, olapTable,
                MaterializedViewExceptions.inactiveReasonForBaseTableRenamed(oldTableName));

        TableInfo tableInfo = TableInfo.createForTableRename(db.getId(), olapTable.getId(), newTableName);
        GlobalStateMgr.getCurrentState().getEditLog().logTableRename(tableInfo);
        LOG.info("rename table[{}] to {}, tableId: {}", oldTableName, newTableName, olapTable.getId());
    }

    public static void inactiveRelatedMaterializedView(Database db, Table olapTable, String reason) {
        for (MvId mvId : olapTable.getRelatedMaterializedViews()) {
            MaterializedView mv = (MaterializedView) db.getTable(mvId.getId());
            if (mv != null) {
                LOG.warn("Setting the materialized view {}({}) to invalid because {}",
                        mv.getName(), mv.getId(), reason);
                mv.setInactiveAndReason(reason);
                inactiveRelatedMaterializedView(db, mv,
                        MaterializedViewExceptions.inactiveReasonForBaseTableActive(mv.getName()));
            } else {
                LOG.warn("Ignore materialized view {} does not exists", mvId);
            }
        }
    }

    public void replayRenameTable(TableInfo tableInfo) {
        long dbId = tableInfo.getDbId();
        long tableId = tableInfo.getTableId();
        String newTableName = tableInfo.getNewTableName();

        Database db = getDb(dbId);
        db.writeLock();
        try {
            OlapTable table = (OlapTable) db.getTable(tableId);
            String tableName = table.getName();
            db.dropTable(tableName);
            table.setName(newTableName);
            db.createTable(table);
            inactiveRelatedMaterializedView(db, table,
                    MaterializedViewExceptions.inactiveReasonForBaseTableRenamed(tableName));

            LOG.info("replay rename table[{}] to {}, tableId: {}", tableName, newTableName, table.getId());
        } finally {
            db.writeUnlock();
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

        olapTable.renamePartition(partitionName, newPartitionName);

        // log
        TableInfo tableInfo = TableInfo.createForPartitionRename(db.getId(), olapTable.getId(), partition.getId(),
                newPartitionName);
        GlobalStateMgr.getCurrentState().getEditLog().logPartitionRename(tableInfo);
        LOG.info("rename partition[{}] to {}", partitionName, newPartitionName);
    }

    public void replayRenamePartition(TableInfo tableInfo) {
        long dbId = tableInfo.getDbId();
        long tableId = tableInfo.getTableId();
        long partitionId = tableInfo.getPartitionId();
        String newPartitionName = tableInfo.getNewPartitionName();

        Database db = getDb(dbId);
        db.writeLock();
        try {
            OlapTable table = (OlapTable) db.getTable(tableId);
            Partition partition = table.getPartition(partitionId);
            table.renamePartition(partition.getName(), newPartitionName);
            LOG.info("replay rename partition[{}] to {}", partition.getName(), newPartitionName);
        } finally {
            db.writeUnlock();
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

        Map<String, Long> indexNameToIdMap = table.getIndexNameToId();
        if (indexNameToIdMap.get(rollupName) == null) {
            throw new DdlException("Rollup index[" + rollupName + "] does not exists");
        }

        // check if name is already used
        if (indexNameToIdMap.get(newRollupName) != null) {
            throw new DdlException("Rollup name[" + newRollupName + "] is already used");
        }

        long indexId = indexNameToIdMap.remove(rollupName);
        indexNameToIdMap.put(newRollupName, indexId);

        // log
        TableInfo tableInfo = TableInfo.createForRollupRename(db.getId(), table.getId(), indexId, newRollupName);
        GlobalStateMgr.getCurrentState().getEditLog().logRollupRename(tableInfo);
        LOG.info("rename rollup[{}] to {}", rollupName, newRollupName);
    }

    public void replayRenameRollup(TableInfo tableInfo) {
        long dbId = tableInfo.getDbId();
        long tableId = tableInfo.getTableId();
        long indexId = tableInfo.getIndexId();
        String newRollupName = tableInfo.getNewRollupName();

        Database db = getDb(dbId);
        db.writeLock();
        try {
            OlapTable table = (OlapTable) db.getTable(tableId);
            String rollupName = table.getIndexNameById(indexId);
            Map<String, Long> indexNameToIdMap = table.getIndexNameToId();
            indexNameToIdMap.remove(rollupName);
            indexNameToIdMap.put(newRollupName, indexId);

            LOG.info("replay rename rollup[{}] to {}", rollupName, newRollupName);
        } finally {
            db.writeUnlock();
        }
    }

    public void renameColumn(Database db, OlapTable table, ColumnRenameClause renameClause) throws DdlException {
        throw new DdlException("not implmented");
    }

    public void replayRenameColumn(TableInfo tableInfo) throws DdlException {
        throw new DdlException("not implmented");
    }

    public void modifyTableDynamicPartition(Database db, OlapTable table, Map<String, String> properties)
            throws DdlException {
        Map<String, String> logProperties = new HashMap<>(properties);
        TableProperty tableProperty = table.getTableProperty();
        if (tableProperty == null) {
            DynamicPartitionUtil.checkAndSetDynamicPartitionProperty(table, properties);
        } else {
            Map<String, String> analyzedDynamicPartition = DynamicPartitionUtil.analyzeDynamicPartition(properties);
            tableProperty.modifyTableProperties(analyzedDynamicPartition);
            tableProperty.buildDynamicProperty();
        }

        DynamicPartitionUtil.registerOrRemovePartitionScheduleInfo(db.getId(), table);

        ModifyTablePropertyOperationLog info =
                new ModifyTablePropertyOperationLog(db.getId(), table.getId(), logProperties);
        GlobalStateMgr.getCurrentState().getEditLog().logDynamicPartition(info);
    }


    public void alterTableProperties(Database db, OlapTable table, Map<String, String> properties)
            throws DdlException {
        Map<String, String> propertiesToPersist = new HashMap<>(properties);
        Map<String, Object> results = validateToBeModifiedProps(properties);

        TableProperty tableProperty = table.getTableProperty();
        for (String key : results.keySet()) {
            if (propertiesToPersist.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_LIVE_NUMBER)) {
                int partitionLiveNumber = (int) results.get(key);
                tableProperty.getProperties().put(PropertyAnalyzer.PROPERTIES_PARTITION_LIVE_NUMBER,
                        String.valueOf(partitionLiveNumber));
                if (partitionLiveNumber == TableProperty.INVALID) {
                    GlobalStateMgr.getCurrentState().getDynamicPartitionScheduler().removeTtlPartitionTable(db.getId(),
                            table.getId());
                } else {
                    GlobalStateMgr.getCurrentState().getDynamicPartitionScheduler().registerTtlPartitionTable(db.getId(),
                            table.getId());
                }
                tableProperty.setPartitionTTLNumber(partitionLiveNumber);
                ModifyTablePropertyOperationLog info =
                        new ModifyTablePropertyOperationLog(db.getId(), table.getId(),
                                ImmutableMap.of(key, propertiesToPersist.get(key)));
                GlobalStateMgr.getCurrentState().getEditLog().logAlterTableProperties(info);
            }
            if (propertiesToPersist.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM)) {
                DataProperty dataProperty = (DataProperty) results.get(key);
                TStorageMedium storageMedium = dataProperty.getStorageMedium();
                table.setStorageMedium(storageMedium);
                tableProperty.getProperties()
                        .put(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TIME,
                                String.valueOf(dataProperty.getCooldownTimeMs()));
                ModifyTablePropertyOperationLog info =
                        new ModifyTablePropertyOperationLog(db.getId(), table.getId(),
                                ImmutableMap.of(key, propertiesToPersist.get(key)));
                GlobalStateMgr.getCurrentState().getEditLog().logAlterTableProperties(info);
            }
            if (propertiesToPersist.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TTL)) {
                String storageCoolDownTTL = propertiesToPersist.get(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TTL);
                tableProperty.getProperties().put(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TTL, storageCoolDownTTL);
                tableProperty.buildStorageCoolDownTTL();
                ModifyTablePropertyOperationLog info =
                        new ModifyTablePropertyOperationLog(db.getId(), table.getId(),
                                ImmutableMap.of(key, propertiesToPersist.get(key)));
                GlobalStateMgr.getCurrentState().getEditLog().logAlterTableProperties(info);
            }
        }
    }

    private Map<String, Object> validateToBeModifiedProps(Map<String, String> properties) throws DdlException {
        Map<String, Object> results = Maps.newHashMap();
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_PARTITION_LIVE_NUMBER)) {
            try {
                int partitionLiveNumber = PropertyAnalyzer.analyzePartitionLiveNumber(properties, true);
                results.put(PropertyAnalyzer.PROPERTIES_PARTITION_LIVE_NUMBER, partitionLiveNumber);
            } catch (AnalysisException ex) {
                throw new DdlException(ex.getMessage());
            }
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM)) {
            try {
                DataProperty dataProperty = DataProperty.getInferredDefaultDataProperty();
                dataProperty = PropertyAnalyzer.analyzeDataProperty(properties, dataProperty, false);
                results.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, dataProperty);
            } catch (AnalysisException ex) {
                throw new RuntimeException(ex.getMessage());
            }
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TTL)) {
            try {
                PropertyAnalyzer.analyzeStorageCoolDownTTL(properties, true);
                results.put(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TTL, null);
            } catch (AnalysisException ex) {
                throw new RuntimeException(ex.getMessage());
            }
        }
        if (!properties.isEmpty()) {
            throw new DdlException("Modify failed because unknown properties: " + properties);
        }
        return results;
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
        Preconditions.checkArgument(db.isWriteLockHeldByCurrentThread());
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
        boolean isInMemory = partitionInfo.getIsInMemory(partition.getId());
        DataProperty newDataProperty = partitionInfo.getDataProperty(partition.getId());
        partitionInfo.setReplicationNum(partition.getId(), replicationNum);

        // update table default replication num
        table.setReplicationNum(replicationNum);

        // log
        ModifyPartitionInfo info = new ModifyPartitionInfo(db.getId(), table.getId(), partition.getId(),
                newDataProperty, replicationNum, isInMemory);
        GlobalStateMgr.getCurrentState().getEditLog().logModifyPartition(info);
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
        Preconditions.checkArgument(db.isWriteLockHeldByCurrentThread());
        if (colocateTableIndex.isColocateTable(table.getId())) {
            throw new DdlException("table " + table.getName() + " is colocate table, cannot change replicationNum");
        }

        // check unpartitioned table
        PartitionInfo partitionInfo = table.getPartitionInfo();
        Partition partition = null;
        boolean isUnpartitionedTable = false;
        if (partitionInfo.getType() == PartitionType.UNPARTITIONED) {
            isUnpartitionedTable = true;
            String partitionName = table.getName();
            partition = table.getPartition(partitionName);
            if (partition == null) {
                throw new DdlException("Partition does not exist. name: " + partitionName);
            }
        }

        TableProperty tableProperty = table.getTableProperty();
        if (tableProperty == null) {
            tableProperty = new TableProperty(properties);
            table.setTableProperty(tableProperty);
        } else {
            tableProperty.modifyTableProperties(properties);
        }
        tableProperty.buildReplicationNum();

        // update partition replication num if this table is unpartitioned table
        if (isUnpartitionedTable) {
            Preconditions.checkNotNull(partition);
            partitionInfo.setReplicationNum(partition.getId(), tableProperty.getReplicationNum());
        }

        // log
        ModifyTablePropertyOperationLog info =
                new ModifyTablePropertyOperationLog(db.getId(), table.getId(), properties);
        GlobalStateMgr.getCurrentState().getEditLog().logModifyReplicationNum(info);
        LOG.info("modify table[{}] replication num to {}", table.getName(),
                properties.get(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM));
    }

    public void modifyTableEnablePersistentIndexMeta(Database db, OlapTable table, Map<String, String> properties) {
        Preconditions.checkArgument(db.isWriteLockHeldByCurrentThread());
        TableProperty tableProperty = table.getTableProperty();
        if (tableProperty == null) {
            tableProperty = new TableProperty(properties);
            table.setTableProperty(tableProperty);
        } else {
            tableProperty.modifyTableProperties(properties);
        }
        tableProperty.buildEnablePersistentIndex();

        ModifyTablePropertyOperationLog info =
                new ModifyTablePropertyOperationLog(db.getId(), table.getId(), properties);
        GlobalStateMgr.getCurrentState().getEditLog().logModifyEnablePersistentIndex(info);

    }

    public void modifyBinlogMeta(Database db, OlapTable table, BinlogConfig binlogConfig) {
        Preconditions.checkArgument(db.isWriteLockHeldByCurrentThread());
        ModifyTablePropertyOperationLog log = new ModifyTablePropertyOperationLog(
                db.getId(),
                table.getId(),
                binlogConfig.toProperties());
        GlobalStateMgr.getCurrentState().getEditLog().logModifyBinlogConfig(log);

        if (!binlogConfig.getBinlogEnable()) {
            table.clearBinlogAvailableVersion();
            table.setBinlogTxnId(BinlogConfig.INVALID);
        }
        table.setCurBinlogConfig(binlogConfig);
    }

    // The caller need to hold the db write lock
    public void modifyTableInMemoryMeta(Database db, OlapTable table, Map<String, String> properties) {
        Preconditions.checkArgument(db.isWriteLockHeldByCurrentThread());
        TableProperty tableProperty = table.getTableProperty();
        if (tableProperty == null) {
            tableProperty = new TableProperty(properties);
            table.setTableProperty(tableProperty);
        } else {
            tableProperty.modifyTableProperties(properties);
        }
        tableProperty.buildInMemory();

        // need to update partition info meta
        for (Partition partition : table.getPartitions()) {
            table.getPartitionInfo().setIsInMemory(partition.getId(), tableProperty.isInMemory());
        }

        ModifyTablePropertyOperationLog info =
                new ModifyTablePropertyOperationLog(db.getId(), table.getId(), properties);
        GlobalStateMgr.getCurrentState().getEditLog().logModifyInMemory(info);
    }

    // The caller need to hold the db write lock
    public void modifyTableConstraint(Database db, String tableName, Map<String, String> properties) throws DdlException {
        Preconditions.checkArgument(db.isWriteLockHeldByCurrentThread());
        Table table = db.getTable(tableName);
        if (table == null) {
            throw new DdlException(String.format("table:%s does not exist", tableName));
        }
        OlapTable olapTable = (OlapTable) table;
        TableProperty tableProperty = olapTable.getTableProperty();
        if (tableProperty == null) {
            tableProperty = new TableProperty(properties);
            olapTable.setTableProperty(tableProperty);
        } else {
            tableProperty.modifyTableProperties(properties);
        }
        tableProperty.buildConstraint();

        ModifyTablePropertyOperationLog info =
                new ModifyTablePropertyOperationLog(db.getId(), olapTable.getId(), properties);
        GlobalStateMgr.getCurrentState().getEditLog().logModifyConstraint(info);
    }

    // The caller need to hold the db write lock
    public void modifyTableWriteQuorum(Database db, OlapTable table, Map<String, String> properties) {
        Preconditions.checkArgument(db.isWriteLockHeldByCurrentThread());
        TableProperty tableProperty = table.getTableProperty();
        if (tableProperty == null) {
            tableProperty = new TableProperty(properties);
            table.setTableProperty(tableProperty);
        } else {
            tableProperty.modifyTableProperties(properties);
        }
        tableProperty.buildWriteQuorum();

        ModifyTablePropertyOperationLog info =
                new ModifyTablePropertyOperationLog(db.getId(), table.getId(), properties);
        GlobalStateMgr.getCurrentState().getEditLog().logModifyWriteQuorum(info);
    }

    // The caller need to hold the db write lock
    public void modifyTableReplicatedStorage(Database db, OlapTable table, Map<String, String> properties) {
        Preconditions.checkArgument(db.isWriteLockHeldByCurrentThread());
        TableProperty tableProperty = table.getTableProperty();
        if (tableProperty == null) {
            tableProperty = new TableProperty(properties);
            table.setTableProperty(tableProperty);
        } else {
            tableProperty.modifyTableProperties(properties);
        }
        tableProperty.buildReplicatedStorage();

        ModifyTablePropertyOperationLog info =
                new ModifyTablePropertyOperationLog(db.getId(), table.getId(), properties);
        GlobalStateMgr.getCurrentState().getEditLog().logModifyReplicatedStorage(info);
    }

    public void modifyTableMeta(Database db, OlapTable table, Map<String, String> properties,
                                TTabletMetaType metaType) {
        if (metaType == TTabletMetaType.INMEMORY) {
            modifyTableInMemoryMeta(db, table, properties);
        } else if (metaType == TTabletMetaType.ENABLE_PERSISTENT_INDEX) {
            modifyTableEnablePersistentIndexMeta(db, table, properties);
        } else if (metaType == TTabletMetaType.WRITE_QUORUM) {
            modifyTableWriteQuorum(db, table, properties);
        } else if (metaType == TTabletMetaType.REPLICATED_STORAGE) {
            modifyTableReplicatedStorage(db, table, properties);
        }
    }

    public void setHasForbitGlobalDict(String dbName, String tableName, boolean isForbit) throws DdlException {
        Map<String, String> property = new HashMap<>();
        Database db = getDb(dbName);
        if (db == null) {
            throw new DdlException("the DB " + dbName + " is not exist");
        }
        db.readLock();
        try {
            Table table = db.getTable(tableName);
            if (table == null) {
                throw new DdlException("the DB " + dbName + " table: " + tableName + "isn't  exist");
            }

            if (table instanceof OlapTable) {
                OlapTable olapTable = (OlapTable) table;
                olapTable.setHasForbitGlobalDict(isForbit);
                if (isForbit) {
                    property.put(PropertyAnalyzer.ENABLE_LOW_CARD_DICT_TYPE, PropertyAnalyzer.DISABLE_LOW_CARD_DICT);
                    IDictManager.getInstance().disableGlobalDict(olapTable.getId());
                } else {
                    property.put(PropertyAnalyzer.ENABLE_LOW_CARD_DICT_TYPE, PropertyAnalyzer.ABLE_LOW_CARD_DICT);
                    IDictManager.getInstance().enableGlobalDict(olapTable.getId());
                }
                ModifyTablePropertyOperationLog info =
                        new ModifyTablePropertyOperationLog(db.getId(), table.getId(), property);
                GlobalStateMgr.getCurrentState().getEditLog().logSetHasForbitGlobalDict(info);
            }
        } finally {
            db.readUnlock();
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
        db.writeLock();
        try {
            Table tbl = db.getTable(hiveExternalTable);
            table = (HiveTable) tbl;
            table.setNewFullSchema(columns);
        } finally {
            db.writeUnlock();
        }
    }

    public void replayModifyTableProperty(short opCode, ModifyTablePropertyOperationLog info) {
        long dbId = info.getDbId();
        long tableId = info.getTableId();
        Map<String, String> properties = info.getProperties();

        Database db = getDb(dbId);
        db.writeLock();
        try {
            OlapTable olapTable = (OlapTable) db.getTable(tableId);
            if (opCode == OperationType.OP_SET_FORBIT_GLOBAL_DICT) {
                String enAble = properties.get(PropertyAnalyzer.ENABLE_LOW_CARD_DICT_TYPE);
                Preconditions.checkState(enAble != null);
                if (olapTable != null) {
                    if (enAble.equals(PropertyAnalyzer.DISABLE_LOW_CARD_DICT)) {
                        olapTable.setHasForbitGlobalDict(true);
                        IDictManager.getInstance().disableGlobalDict(olapTable.getId());
                    } else {
                        olapTable.setHasForbitGlobalDict(false);
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
                } else if (opCode == OperationType.OP_MODIFY_BINLOG_CONFIG) {
                    if (!olapTable.isBinlogEnabled()) {
                        olapTable.clearBinlogAvailableVersion();
                    }
                }
            }
        } catch (Exception ex) {
            LOG.warn("The replay log failed and this log was ignored.", ex);
        } finally {
            db.writeUnlock();
        }
    }

    @Override
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
        db.readLock();
        try {
            if (db.getTable(tableName) != null) {
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
            db.readUnlock();
        }

        if (existed) {
            // already existed, need to alter the view
            AlterJobMgr alterJobMgr = GlobalStateMgr.getCurrentState().getAlterJobMgr();
            try {
                AlterViewStmt alterViewStmt = AlterViewStmt.fromReplaceStmt(stmt);
                alterJobMgr.processAlterView(alterViewStmt, ConnectContext.get());
                LOG.info("replace view {} successfully", tableName);
            } catch (DdlException e) {
                LOG.warn("replace view failed due to {}", e.getMessage(), e);
                throw new DdlException("replace view failed due to " + e.getMessage(), e);
            }
        } else {
            List<Column> columns = stmt.getColumns();
            long tableId = getNextId();
            View view = new View(tableId, tableName, columns);
            view.setComment(stmt.getComment());
            view.setInlineViewDefWithSqlMode(stmt.getInlineViewDef(),
                    ConnectContext.get().getSessionVariable().getSqlMode());
            // init here in case the stmt string from view.toSql() has some syntax error.
            try {
                view.init();
            } catch (UserException e) {
                throw new DdlException("failed to init view stmt", e);
            }

            db.writeLock();
            try {
                if (!db.isExist()) {
                    throw new DdlException("database has been dropped when creating view");
                }
                if (!db.createTableWithLock(view, false)) {
                    if (!stmt.isSetIfNotExists()) {
                        ErrorReport.reportDdlException(ErrorCode.ERR_CANT_CREATE_TABLE, tableName,
                                "table already exists");
                    } else {
                        LOG.info("create table[{}] which already exists", tableName);
                        return;
                    }
                }
            } finally {
                db.writeUnlock();
            }

            LOG.info("successfully create view[" + tableName + "-" + view.getId() + "]");
        }
    }

    public void replayCreateCluster(Cluster cluster) {
        tryLock(true);
        try {
            unprotectCreateCluster(cluster);
        } finally {
            unlock();
        }
    }

    private void unprotectCreateCluster(Cluster cluster) {
        // This is only used for initDefaultCluster and in that case the backendIdList is empty.
        // So ASSERT the cluster's backend list size.
        Preconditions.checkState(cluster.isDefaultCluster(), "Cluster must be default cluster");
        Preconditions.checkState(cluster.isEmpty(), "Cluster backendIdList must be 0");

        defaultCluster = cluster;

        // create info schema db
        final InfoSchemaDb infoDb = new InfoSchemaDb();
        unprotectCreateDb(infoDb);
        final SysDb sysDb = new SysDb();
        unprotectCreateDb(sysDb);

        // only need to create default cluster once.
        stateMgr.setIsDefaultClusterCreated(true);
    }

    public Cluster getCluster() {
        return defaultCluster;
    }

    public long loadCluster(DataInputStream dis, long checksum) throws IOException {
        int clusterCount = dis.readInt();
        checksum ^= clusterCount;
        for (long i = 0; i < clusterCount; ++i) {
            final Cluster cluster = Cluster.read(dis);
            checksum ^= cluster.getId();

            Preconditions.checkState(cluster.isDefaultCluster(), "Cluster must be default_cluster");
            List<Long> latestBackendIds = stateMgr.getClusterInfo().getBackendIds();

            // The number of BE in cluster is not same as in SystemInfoService, when perform 'ALTER
            // SYSTEM ADD BACKEND TO ...' or 'ALTER SYSTEM ADD BACKEND ...', because both of them are
            // for adding BE to some Cluster, but loadCluster is after loadBackend.
            cluster.setBackendIdList(latestBackendIds);

            String dbName = InfoSchemaDb.DATABASE_NAME;
            InfoSchemaDb db;
            // Use real GlobalStateMgr instance to avoid InfoSchemaDb id continuously increment
            // when checkpoint thread load image.
            if (getFullNameToDb().containsKey(dbName)) {
                db = (InfoSchemaDb) GlobalStateMgr.getCurrentState().getFullNameToDb().get(dbName);
            } else {
                db = new InfoSchemaDb();
            }
            String errMsg = "InfoSchemaDb id shouldn't larger than 10000, please restart your FE server";
            // Every time we construct the InfoSchemaDb, which id will increment.
            // When InfoSchemaDb id larger than 10000 and put it to idToDb,
            // which may be overwrite the normal db meta in idToDb,
            // so we ensure InfoSchemaDb id less than 10000.
            Preconditions.checkState(db.getId() < NEXT_ID_INIT_VALUE, errMsg);
            idToDb.put(db.getId(), db);
            fullNameToDb.put(db.getFullName(), db);
            cluster.addDb(dbName, db.getId());

            if (getFullNameToDb().containsKey(SysDb.DATABASE_NAME)) {
                LOG.warn("Since the the database of sys already exists, " +
                        "the system will not automatically create the database of starrocks for system.");
            } else {
                SysDb starRocksDb = new SysDb();
                Preconditions.checkState(starRocksDb.getId() < NEXT_ID_INIT_VALUE, errMsg);
                idToDb.put(starRocksDb.getId(), starRocksDb);
                fullNameToDb.put(starRocksDb.getFullName(), starRocksDb);
                cluster.addDb(SysDb.DATABASE_NAME, starRocksDb.getId());
            }
            defaultCluster = cluster;
        }
        LOG.info("finished replay cluster from image");
        return checksum;
    }

    // TODO [meta-format-change] deprecated
    public void initDefaultCluster() {
        final List<Long> backendList = Lists.newArrayList();
        final List<Backend> defaultClusterBackends = GlobalStateMgr.getCurrentSystemInfo().getBackends();
        for (Backend backend : defaultClusterBackends) {
            backendList.add(backend.getId());
        }

        final long id = getNextId();
        final Cluster cluster = new Cluster(SystemInfoService.DEFAULT_CLUSTER, id);

        // make sure one host hold only one backend.
        Set<String> beHost = Sets.newHashSet();
        for (Backend be : defaultClusterBackends) {
            if (beHost.contains(be.getHost())) {
                // we can not handle this situation automatically.
                LOG.error("found more than one backends in same host: {}", be.getHost());
                System.exit(-1);
            } else {
                beHost.add(be.getHost());
            }
        }

        // we create default_cluster to meet the need for ease of use, because
        // most users hava no multi tenant needs.
        cluster.setBackendIdList(backendList);
        unprotectCreateCluster(cluster);
        for (Database db : idToDb.values()) {
            cluster.addDb(db.getFullName(), db.getId());
        }

        // no matter default_cluster is created or not,
        // mark isDefaultClusterCreated as true
        stateMgr.setIsDefaultClusterCreated(true);
        GlobalStateMgr.getCurrentState().getEditLog().logCreateCluster(cluster);
    }

    public long saveCluster(DataOutputStream dos, long checksum) throws IOException {
        final int clusterCount = 1;
        checksum ^= clusterCount;
        dos.writeInt(clusterCount);
        Cluster cluster = defaultCluster;
        long clusterId = defaultCluster.getId();
        if (clusterId >= NEXT_ID_INIT_VALUE) {
            checksum ^= clusterId;
            cluster.write(dos);
        }
        return checksum;
    }

    public void replayUpdateClusterAndBackends(BackendIdsUpdateInfo info) {
        for (long id : info.getBackendList()) {
            final Backend backend = stateMgr.getClusterInfo().getBackend(id);
            final Cluster cluster = defaultCluster;
            cluster.removeBackend(id);
            backend.setDecommissioned(false);
            backend.setBackendState(Backend.BackendState.free);
        }
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
    public void truncateTable(TruncateTableStmt truncateTableStmt) throws DdlException {
        TableRef tblRef = truncateTableStmt.getTblRef();
        TableName dbTbl = tblRef.getName();

        // check, and save some info which need to be checked again later
        Map<String, Partition> origPartitions = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        OlapTable copiedTbl;
        Database db = getDb(dbTbl.getDb());
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbTbl.getDb());
        }

        boolean truncateEntireTable = tblRef.getPartitionNames() == null;
        db.readLock();
        try {
            Table table = db.getTable(dbTbl.getTbl());
            if (table == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, dbTbl.getTbl());
            }

            if (!table.isOlapOrCloudNativeTable()) {
                throw new DdlException("Only support truncate OLAP table or LAKE table");
            }

            OlapTable olapTable = (OlapTable) table;
            if (olapTable.getState() != OlapTable.OlapTableState.NORMAL) {
                throw new DdlException("Table' state is not NORMAL: " + olapTable.getState());
            }

            if (!truncateEntireTable) {
                for (String partName : tblRef.getPartitionNames().getPartitionNames()) {
                    Partition partition = olapTable.getPartition(partName);
                    if (partition == null) {
                        throw new DdlException("Partition " + partName + " does not exist");
                    }

                    origPartitions.put(partName, partition);
                }
            } else {
                for (Partition partition : olapTable.getPartitions()) {
                    origPartitions.put(partition.getName(), partition);
                }
            }

            copiedTbl = olapTable.selectiveCopy(origPartitions.keySet(), true, MaterializedIndex.IndexExtState.VISIBLE);
        } finally {
            db.readUnlock();
        }

        // 2. use the copied table to create partitions
        List<Partition> newPartitions = Lists.newArrayListWithCapacity(origPartitions.size());
        // tabletIdSet to save all newly created tablet ids.
        Set<Long> tabletIdSet = Sets.newHashSet();
        try {
            for (Map.Entry<String, Partition> entry : origPartitions.entrySet()) {
                long oldPartitionId = entry.getValue().getId();
                long newPartitionId = getNextId();
                String newPartitionName = entry.getKey();

                PartitionInfo partitionInfo = copiedTbl.getPartitionInfo();
                partitionInfo.setTabletType(newPartitionId, partitionInfo.getTabletType(oldPartitionId));
                partitionInfo.setIsInMemory(newPartitionId, partitionInfo.getIsInMemory(oldPartitionId));
                partitionInfo.setReplicationNum(newPartitionId, partitionInfo.getReplicationNum(oldPartitionId));
                partitionInfo.setDataProperty(newPartitionId, partitionInfo.getDataProperty(oldPartitionId));

                if (copiedTbl.isCloudNativeTable()) {
                    partitionInfo.setStorageCacheInfo(newPartitionId,
                            partitionInfo.getStorageCacheInfo(oldPartitionId));
                }

                copiedTbl.setDefaultDistributionInfo(entry.getValue().getDistributionInfo());

                Partition newPartition =
                        createPartition(db, copiedTbl, newPartitionId, newPartitionName, null, tabletIdSet);
                newPartitions.add(newPartition);
            }
            buildPartitions(db, copiedTbl, newPartitions);
        } catch (DdlException e) {
            deleteUselessTablets(tabletIdSet);
            throw e;
        }
        Preconditions.checkState(origPartitions.size() == newPartitions.size());

        // all partitions are created successfully, try to replace the old partitions.
        // before replacing, we need to check again.
        // Things may be changed outside the database lock.
        db.writeLock();
        try {
            OlapTable olapTable = (OlapTable) db.getTable(copiedTbl.getId());
            if (olapTable == null) {
                throw new DdlException("Table[" + copiedTbl.getName() + "] is dropped");
            }

            if (olapTable.getState() != OlapTable.OlapTableState.NORMAL) {
                throw new DdlException("Table' state is not NORMAL: " + olapTable.getState());
            }

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
            if (olapTable.getIndexNameToId().size() != copiedTbl.getIndexNameToId().size()) {
                metaChanged = true;
            } else {
                // compare schemaHash
                Map<Long, Integer> copiedIndexIdToSchemaHash = copiedTbl.getIndexIdToSchemaHash();
                for (Map.Entry<Long, Integer> entry : olapTable.getIndexIdToSchemaHash().entrySet()) {
                    long indexId = entry.getKey();
                    if (!copiedIndexIdToSchemaHash.containsKey(indexId)) {
                        metaChanged = true;
                        break;
                    }
                    if (!copiedIndexIdToSchemaHash.get(indexId).equals(entry.getValue())) {
                        metaChanged = true;
                        break;
                    }
                }
            }

            if (metaChanged) {
                throw new DdlException("Table[" + copiedTbl.getName() + "]'s meta has been changed. try again.");
            }

            // replace
            truncateTableInternal(olapTable, newPartitions, truncateEntireTable, false);

            colocateTableIndex.updateLakeTableColocationInfo(olapTable, true /* isJoin */, null /* expectGroupId */);

            // write edit log
            TruncateTableInfo info = new TruncateTableInfo(db.getId(), olapTable.getId(), newPartitions,
                    truncateEntireTable);
            GlobalStateMgr.getCurrentState().getEditLog().logTruncateTable(info);

            // refresh mv
            Set<MvId> relatedMvs = olapTable.getRelatedMaterializedViews();
            for (MvId mvId : relatedMvs) {
                MaterializedView materializedView = (MaterializedView) db.getTable(mvId.getId());
                if (materializedView == null) {
                    LOG.warn("Table related materialized view {} can not be found", mvId.getId());
                    continue;
                }
                if (materializedView.isLoadTriggeredRefresh()) {
                    refreshMaterializedView(db.getFullName(), db.getTable(mvId.getId()).getName(), false, null,
                            Constants.TaskRunPriority.NORMAL.value(), true, false);
                }
            }
        } catch (DdlException e) {
            deleteUselessTablets(tabletIdSet);
            throw e;
        } catch (MetaNotFoundException e) {
            LOG.warn("Table related materialized view can not be found", e);
        } finally {
            db.writeUnlock();
        }

        LOG.info("finished to truncate table {}, partitions: {}",
                tblRef.getName().toSql(), tblRef.getPartitionNames());
    }

    private void deleteUselessTablets(Set<Long> tabletIdSet) {
        // create partition failed, remove all newly created tablets.
        // For lakeTable, shards cleanup is taken care in ShardDeleter.
        for (Long tabletId : tabletIdSet) {
            GlobalStateMgr.getCurrentInvertedIndex().deleteTablet(tabletId);
        }
    }

    private void truncateTableInternal(OlapTable olapTable, List<Partition> newPartitions,
                                       boolean isEntireTable, boolean isReplay) {
        // use new partitions to replace the old ones.
        Set<Tablet> oldTablets = Sets.newHashSet();
        for (Partition newPartition : newPartitions) {
            Partition oldPartition = olapTable.replacePartition(newPartition);
            // save old tablets to be removed
            for (MaterializedIndex index : oldPartition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                // let HashSet do the deduplicate work
                oldTablets.addAll(index.getTablets());
            }
        }

        if (isEntireTable) {
            // drop all temp partitions
            olapTable.dropAllTempPartitions();
        }

        // remove the tablets in old partitions
        for (Tablet tablet : oldTablets) {
            TabletInvertedIndex index = GlobalStateMgr.getCurrentInvertedIndex();
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
        db.writeLock();
        try {
            OlapTable olapTable = (OlapTable) db.getTable(info.getTblId());
            truncateTableInternal(olapTable, info.getPartitions(), info.isEntireTable(), true);

            if (!GlobalStateMgr.isCheckpointThread()) {
                // add tablet to inverted index
                TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentInvertedIndex();
                for (Partition partition : info.getPartitions()) {
                    long partitionId = partition.getId();
                    TStorageMedium medium = olapTable.getPartitionInfo().getDataProperty(
                            partitionId).getStorageMedium();
                    for (MaterializedIndex mIndex : partition.getMaterializedIndices(
                            MaterializedIndex.IndexExtState.ALL)) {
                        long indexId = mIndex.getId();
                        int schemaHash = olapTable.getSchemaHashByIndexId(indexId);
                        TabletMeta tabletMeta = new TabletMeta(db.getId(), olapTable.getId(),
                                partitionId, indexId, schemaHash, medium, olapTable.isCloudNativeTableOrMaterializedView());
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
        } finally {
            db.writeUnlock();
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
            db.writeLock();
            try {
                OlapTable tbl = (OlapTable) db.getTable(info.getTableId());
                if (tbl == null) {
                    continue;
                }
                Partition partition = tbl.getPartition(info.getPartitionId());
                if (partition == null) {
                    continue;
                }
                MaterializedIndex mindex = partition.getIndex(info.getIndexId());
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
                db.writeUnlock();
            }
        }
    }

    // Convert table's distribution type from random to hash.
    // random distribution is no longer supported.
    public void convertDistributionType(Database db, OlapTable tbl) throws DdlException {
        db.writeLock();
        try {
            TableInfo tableInfo = TableInfo.createForModifyDistribution(db.getId(), tbl.getId());
            GlobalStateMgr.getCurrentState().getEditLog().logModifyDistributionType(tableInfo);
            LOG.info("finished to modify distribution type of table: " + tbl.getName());
        } finally {
            db.writeUnlock();
        }
    }

    public void replayConvertDistributionType(TableInfo tableInfo) {
        Database db = getDb(tableInfo.getDbId());
        db.writeLock();
        try {
            OlapTable tbl = (OlapTable) db.getTable(tableInfo.getTableId());
            LOG.info("replay modify distribution type of table: " + tbl.getName());
        } finally {
            db.writeUnlock();
        }
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
        db.writeLock();
        try {
            Table table = db.getTable(tableName);
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

            olapTable.replaceTempPartitions(partitionNames, tempPartitionNames, isStrictRange, useTempPartitionName);

            // write log
            ReplacePartitionOperationLog info = new ReplacePartitionOperationLog(db.getId(), olapTable.getId(),
                    partitionNames, tempPartitionNames, isStrictRange, useTempPartitionName);
            GlobalStateMgr.getCurrentState().getEditLog().logReplaceTempPartition(info);
            LOG.info("finished to replace partitions {} with temp partitions {} from table: {}",
                    clause.getPartitionNames(), clause.getTempPartitionNames(), tableName);
        } finally {
            db.writeUnlock();
        }
    }

    public void replayReplaceTempPartition(ReplacePartitionOperationLog replaceTempPartitionLog) {
        Database db = getDb(replaceTempPartitionLog.getDbId());
        if (db == null) {
            return;
        }
        db.writeLock();
        try {
            OlapTable olapTable = (OlapTable) db.getTable(replaceTempPartitionLog.getTblId());
            if (olapTable == null) {
                return;
            }
            olapTable.replaceTempPartitions(replaceTempPartitionLog.getPartitions(),
                    replaceTempPartitionLog.getTempPartitions(),
                    replaceTempPartitionLog.isStrictRange(),
                    replaceTempPartitionLog.useTempPartitionName());
        } catch (DdlException e) {
            LOG.warn("should not happen.", e);
        } finally {
            db.writeUnlock();
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
        Replica.ReplicaStatus status = stmt.getStatus();
        setReplicaStatusInternal(tabletId, backendId, status, false);
    }

    public void replaySetReplicaStatus(SetReplicaStatusOperationLog log) {
        setReplicaStatusInternal(log.getTabletId(), log.getBackendId(), log.getReplicaStatus(), true);
    }

    private void setReplicaStatusInternal(long tabletId, long backendId, Replica.ReplicaStatus status,
                                          boolean isReplay) {
        TabletMeta meta = stateMgr.getTabletInvertedIndex().getTabletMeta(tabletId);
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
        db.writeLock();
        try {
            Replica replica = stateMgr.getTabletInvertedIndex().getReplica(tabletId, backendId);
            if (replica == null) {
                LOG.info("replica of tablet {} does not exist", tabletId);
                return;
            }
            if (status == Replica.ReplicaStatus.BAD || status == Replica.ReplicaStatus.OK) {
                if (replica.setBadForce(status == Replica.ReplicaStatus.BAD)) {
                    if (!isReplay) {
                        // Put this tablet into urgent table so that it can be repaired ASAP.
                        stateMgr.getTabletChecker().setTabletForUrgentRepair(dbId, meta.getTableId(),
                                meta.getPartitionId());
                        SetReplicaStatusOperationLog log =
                                new SetReplicaStatusOperationLog(backendId, tabletId, status);
                        GlobalStateMgr.getCurrentState().getEditLog().logSetReplicaStatus(log);
                    }
                    LOG.info("set replica {} of tablet {} on backend {} as {}. is replay: {}",
                            replica.getId(), tabletId, backendId, status, isReplay);
                }
            }
        } finally {
            db.writeUnlock();
        }
    }

    public void onEraseDatabase(long dbId) {
        // remove database transaction manager
        stateMgr.getGlobalTransactionMgr().removeDatabaseTransactionMgr(dbId);
    }

    public void onEraseTable(@NotNull OlapTable olapTable, boolean isReplay) {
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentInvertedIndex();
        Collection<Partition> allPartitions = olapTable.getAllPartitions();
        for (Partition partition : allPartitions) {
            for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                for (Tablet tablet : index.getTablets()) {
                    invertedIndex.deleteTablet(tablet.getId());
                }
            }
        }

        colocateTableIndex.removeTable(olapTable.getId(), olapTable, isReplay);
    }

    public void onErasePartition(Partition partition) {
        // remove tablet in inverted index
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentInvertedIndex();
        for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
            for (Tablet tablet : index.getTablets()) {
                long tabletId = tablet.getId();
                invertedIndex.deleteTablet(tabletId);
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
                                    Map<Long, String> origPartitions) {
        OlapTable copiedTbl;
        db.readLock();
        try {
            if (olapTable.getState() != OlapTable.OlapTableState.NORMAL) {
                throw new RuntimeException("Table' state is not NORMAL: " + olapTable.getState()
                        + ", tableId:" + olapTable.getId() + ", tabletName:" + olapTable.getName());
            }
            for (Long id : sourcePartitionIds) {
                origPartitions.put(id, olapTable.getPartition(id).getName());
            }
            copiedTbl = olapTable.selectiveCopy(origPartitions.values(), true, MaterializedIndex.IndexExtState.VISIBLE);
        } finally {
            db.readUnlock();
        }
        return copiedTbl;
    }

    @VisibleForTesting
    public List<Partition> getNewPartitionsFromPartitions(Database db, OlapTable olapTable,
                                                          List<Long> sourcePartitionIds,
                                                          Map<Long, String> origPartitions, OlapTable copiedTbl,
                                                          String namePostfix, Set<Long> tabletIdSet,
                                                          List<Long> tmpPartitionIds)
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
            partitionInfo.setTabletType(newPartitionId, partitionInfo.getTabletType(sourcePartitionId));
            partitionInfo.setIsInMemory(newPartitionId, partitionInfo.getIsInMemory(sourcePartitionId));
            partitionInfo.setReplicationNum(newPartitionId, partitionInfo.getReplicationNum(sourcePartitionId));
            partitionInfo.setDataProperty(newPartitionId, partitionInfo.getDataProperty(sourcePartitionId));
            if (copiedTbl.isCloudNativeTableOrMaterializedView()) {
                partitionInfo.setStorageCacheInfo(newPartitionId, partitionInfo.getStorageCacheInfo(sourcePartitionId));
            }

            Partition newPartition =
                    createPartition(db, copiedTbl, newPartitionId, newPartitionName, null, tabletIdSet);
            newPartitions.add(newPartition);
        }
        return newPartitions;
    }

    // create new partitions from source partitions.
    // new partitions have the same indexes as source partitions.
    public List<Partition> createTempPartitionsFromPartitions(Database db, Table table,
                                                              String namePostfix, List<Long> sourcePartitionIds,
                                                              List<Long> tmpPartitionIds) {
        Preconditions.checkState(table instanceof OlapTable);
        OlapTable olapTable = (OlapTable) table;
        Map<Long, String> origPartitions = Maps.newHashMap();
        OlapTable copiedTbl = getCopiedTable(db, olapTable, sourcePartitionIds, origPartitions);

        // 2. use the copied table to create partitions
        List<Partition> newPartitions = null;
        // tabletIdSet to save all newly created tablet ids.
        Set<Long> tabletIdSet = Sets.newHashSet();
        try {
            newPartitions = getNewPartitionsFromPartitions(db, olapTable, sourcePartitionIds, origPartitions,
                    copiedTbl, namePostfix, tabletIdSet, tmpPartitionIds);
            buildPartitions(db, copiedTbl, newPartitions);
        } catch (Exception e) {
            // create partition failed, remove all newly created tablets
            for (Long tabletId : tabletIdSet) {
                GlobalStateMgr.getCurrentInvertedIndex().deleteTablet(tabletId);
            }
            LOG.warn("create partitions from partitions failed.", e);
            throw new RuntimeException("create partitions failed: " + e.getMessage(), e);
        }
        return newPartitions;
    }

    public long saveAutoIncrementId(DataOutputStream dos, long checksum) throws IOException {
        AutoIncrementInfo info = new AutoIncrementInfo(tableIdToIncrementId);
        info.write(dos);
        return checksum;
    }

    public long loadAutoIncrementId(DataInputStream dis, long checksum) throws IOException {
        AutoIncrementInfo info = new AutoIncrementInfo(null);
        info.read(dis);
        for (Map.Entry<Long, Long> entry : info.tableIdToIncrementId().entrySet()) {
            Long tableId = entry.getKey();
            Long id = entry.getValue();

            tableIdToIncrementId.put(tableId, id);
        }
        return checksum;
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
        if (!isReplay) {
            ConcurrentHashMap<Long, Long> deltaMap = new ConcurrentHashMap<>();
            deltaMap.put(tableId, 0L);
            AutoIncrementInfo info = new AutoIncrementInfo(deltaMap);
            GlobalStateMgr.getCurrentState().getEditLog().logSaveDeleteAutoIncrementId(info);
        }

        tableIdToIncrementId.remove(tableId);
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

    public void load(SRMetaBlockReader reader) throws
            IOException, SRMetaBlockException, SRMetaBlockEOFException, DdlException {
        int dbSize = reader.readJson(int.class);
        for (int i = 0; i < dbSize; ++i) {
            Database db = reader.readJson(Database.class);
            int tableSize = reader.readInt();
            for (int j = 0; j < tableSize; ++j) {
                Table table = reader.readJson(Table.class);
                db.createTableWithLock(table, true);
            }

            idToDb.put(db.getId(), db);
            fullNameToDb.put(db.getFullName(), db);
            stateMgr.getGlobalTransactionMgr().addDatabaseTransactionMgr(db.getId());
            for (Table tbl : ListUtils.union(db.getMaterializedViews(), db.getHiveTables())) {
                try {
                    tbl.onCreate();
                } catch (Throwable e) {
                    LOG.error("reload table failed: {}", tbl, e);
                }
            }
        }

        // put built-in database into local metastore
        InfoSchemaDb infoSchemaDb = new InfoSchemaDb();
        Preconditions.checkState(infoSchemaDb.getId() < NEXT_ID_INIT_VALUE,
                "InfoSchemaDb id shouldn't larger than " + NEXT_ID_INIT_VALUE);
        idToDb.put(infoSchemaDb.getId(), infoSchemaDb);
        fullNameToDb.put(infoSchemaDb.getFullName(), infoSchemaDb);

        if (getFullNameToDb().containsKey(SysDb.DATABASE_NAME)) {
            LOG.warn("Since the the database of starrocks already exists, " +
                    "the system will not automatically create the database of starrocks for system.");
        } else {
            SysDb starRocksDb = new SysDb();
            Preconditions.checkState(infoSchemaDb.getId() < NEXT_ID_INIT_VALUE,
                    "starocks id shouldn't larger than " + NEXT_ID_INIT_VALUE);
            idToDb.put(starRocksDb.getId(), starRocksDb);
            fullNameToDb.put(starRocksDb.getFullName(), starRocksDb);
        }

        AutoIncrementInfo autoIncrementInfo = reader.readJson(AutoIncrementInfo.class);
        for (Map.Entry<Long, Long> entry : autoIncrementInfo.tableIdToIncrementId().entrySet()) {
            Long tableId = entry.getKey();
            Long id = entry.getValue();

            tableIdToIncrementId.put(tableId, id);
        }

        recreateTabletInvertIndex();
        GlobalStateMgr.getCurrentState().getEsRepository().loadTableFromCatalog();

        /*
         * defaultCluster has no meaning, it is only for compatibility with
         * old versions of the code (defaultCluster is required for 3.0 fallback)
         */
        defaultCluster = new Cluster(SystemInfoService.DEFAULT_CLUSTER, NEXT_ID_INIT_VALUE);
    }
}

