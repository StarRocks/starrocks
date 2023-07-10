// This file is made available under Elastic License 2.0.
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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.starrocks.analysis.AddPartitionClause;
import com.starrocks.analysis.AddRollupClause;
import com.starrocks.analysis.AdminCheckTabletsStmt;
import com.starrocks.analysis.AdminSetReplicaStatusStmt;
import com.starrocks.analysis.AlterClause;
import com.starrocks.analysis.AlterDatabaseQuotaStmt;
import com.starrocks.analysis.AlterDatabaseRename;
import com.starrocks.analysis.AlterTableStmt;
import com.starrocks.analysis.AlterViewStmt;
import com.starrocks.analysis.CancelAlterTableStmt;
import com.starrocks.analysis.ColumnRenameClause;
import com.starrocks.analysis.CreateDbStmt;
import com.starrocks.analysis.CreateMaterializedViewStmt;
import com.starrocks.analysis.CreateTableLikeStmt;
import com.starrocks.analysis.CreateTableStmt;
import com.starrocks.analysis.CreateViewStmt;
import com.starrocks.analysis.DistributionDesc;
import com.starrocks.analysis.DropDbStmt;
import com.starrocks.analysis.DropMaterializedViewStmt;
import com.starrocks.analysis.DropPartitionClause;
import com.starrocks.analysis.DropTableStmt;
import com.starrocks.analysis.KeysDesc;
import com.starrocks.analysis.ListPartitionDesc;
import com.starrocks.analysis.MultiRangePartitionDesc;
import com.starrocks.analysis.PartitionDesc;
import com.starrocks.analysis.PartitionRenameClause;
import com.starrocks.analysis.RangePartitionDesc;
import com.starrocks.analysis.RecoverDbStmt;
import com.starrocks.analysis.RecoverPartitionStmt;
import com.starrocks.analysis.RecoverTableStmt;
import com.starrocks.analysis.ReplacePartitionClause;
import com.starrocks.analysis.RollupRenameClause;
import com.starrocks.analysis.ShowAlterStmt;
import com.starrocks.analysis.SingleRangePartitionDesc;
import com.starrocks.analysis.StatementBase;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TableRef;
import com.starrocks.analysis.TableRenameClause;
import com.starrocks.analysis.TruncateTableStmt;
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
import com.starrocks.catalog.ExternalOlapTable;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.HudiTable;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.InfoSchemaDb;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
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
import com.starrocks.catalog.Type;
import com.starrocks.catalog.View;
import com.starrocks.catalog.lake.LakeTablet;
import com.starrocks.cluster.BaseParam;
import com.starrocks.cluster.Cluster;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeConstants;
import com.starrocks.common.FeMetaVersion;
import com.starrocks.common.MarkedCountDownLatch;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.common.UserException;
import com.starrocks.common.util.DynamicPartitionUtil;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.Util;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.meta.MetaContext;
import com.starrocks.persist.AddPartitionsInfo;
import com.starrocks.persist.BackendIdsUpdateInfo;
import com.starrocks.persist.BackendTabletsInfo;
import com.starrocks.persist.ColocatePersistInfo;
import com.starrocks.persist.DatabaseInfo;
import com.starrocks.persist.DropDbInfo;
import com.starrocks.persist.DropInfo;
import com.starrocks.persist.DropLinkDbAndUpdateDbInfo;
import com.starrocks.persist.DropPartitionInfo;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.ModifyPartitionInfo;
import com.starrocks.persist.ModifyTableColumnOperationLog;
import com.starrocks.persist.ModifyTablePropertyOperationLog;
import com.starrocks.persist.MultiEraseTableInfo;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.PartitionPersistInfo;
import com.starrocks.persist.RecoverInfo;
import com.starrocks.persist.ReplacePartitionOperationLog;
import com.starrocks.persist.ReplicaPersistInfo;
import com.starrocks.persist.SetReplicaStatusOperationLog;
import com.starrocks.persist.TableInfo;
import com.starrocks.persist.TruncateTableInfo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.optimizer.statistics.IDictManager;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.CreateReplicaTask;
import com.starrocks.task.DropReplicaTask;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TStorageFormat;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.thrift.TTabletMetaType;
import com.starrocks.thrift.TTabletType;
import com.starrocks.thrift.TTaskType;
import org.apache.hadoop.util.ThreadUtil;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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

import static com.starrocks.server.GlobalStateMgr.NEXT_ID_INIT_VALUE;
import static com.starrocks.server.GlobalStateMgr.isCheckpointThread;

public class LocalMetastore implements ConnectorMetadata {
    private static final Logger LOG = LogManager.getLogger(LocalMetastore.class);

    private final ConcurrentHashMap<Long, Database> idToDb = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Database> fullNameToDb = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<Long, Cluster> idToCluster = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Cluster> nameToCluster = new ConcurrentHashMap<>();

    private final GlobalStateMgr stateMgr;
    private EditLog editLog;
    private final CatalogRecycleBin recycleBin;
    private ColocateTableIndex colocateTableIndex;
    private final SystemInfoService systemInfoService;

    public LocalMetastore(GlobalStateMgr globalStateMgr, CatalogRecycleBin recycleBin,
                          ColocateTableIndex colocateTableIndex, SystemInfoService systemInfoService) {
        this.stateMgr = globalStateMgr;
        this.recycleBin = recycleBin;
        this.colocateTableIndex = colocateTableIndex;
        this.systemInfoService = systemInfoService;
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

    public void setEditLog(EditLog editLog) {
        this.editLog = editLog;
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
                if (table.getType() != Table.TableType.OLAP) {
                    continue;
                }

                OlapTable olapTable = (OlapTable) table;
                long tableId = olapTable.getId();
                Collection<Partition> allPartitions = olapTable.getAllPartitions();
                for (Partition partition : allPartitions) {
                    long partitionId = partition.getId();
                    TStorageMedium medium = olapTable.getPartitionInfo().getDataProperty(
                            partitionId).getStorageMedium();
                    boolean useStarOS = partition.isUseStarOS();
                    for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                        long indexId = index.getId();
                        int schemaHash = olapTable.getSchemaHashByIndexId(indexId);
                        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, schemaHash, medium);
                        for (Tablet tablet : index.getTablets()) {
                            long tabletId = tablet.getId();
                            invertedIndex.addTablet(tabletId, tabletMeta);
                            if (!useStarOS) {
                                for (Replica replica : ((LocalTablet) tablet).getReplicas()) {
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
            if (db.getDbState() == Database.DbState.LINK) {
                fullNameToDb.put(db.getAttachDb(), db);
            }
            stateMgr.getGlobalTransactionMgr().addDatabaseTransactionMgr(db.getId());
        }
        LOG.info("finished replay databases from image");
        return newChecksum;
    }

    public long saveDb(DataOutputStream dos, long checksum) throws IOException {
        int dbCount = idToDb.size() - nameToCluster.keySet().size();
        checksum ^= dbCount;
        dos.writeInt(dbCount);
        for (Map.Entry<Long, Database> entry : idToDb.entrySet()) {
            Database db = entry.getValue();
            String dbName = db.getFullName();
            // Don't write information_schema db meta
            if (!InfoSchemaDb.isInfoSchemaDb(dbName)) {
                checksum ^= entry.getKey();
                db.readLock();
                try {
                    db.write(dos);
                } finally {
                    db.readUnlock();
                }
            }
        }
        return checksum;
    }

    @Override
    public void createDb(CreateDbStmt stmt) throws DdlException {
        final String clusterName = stmt.getClusterName();
        String fullDbName = stmt.getFullDbName();
        long id = 0L;
        if (!tryLock(false)) {
            throw new DdlException("Failed to acquire globalStateMgr lock. Try again");
        }
        try {
            if (!nameToCluster.containsKey(clusterName)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_NO_SELECT_CLUSTER, clusterName);
            }
            if (fullNameToDb.containsKey(fullDbName)) {
                if (stmt.isSetIfNotExists()) {
                    LOG.info("create database[{}] which already exists", fullDbName);
                    return;
                } else {
                    ErrorReport.reportDdlException(ErrorCode.ERR_DB_CREATE_EXISTS, fullDbName);
                }
            } else {
                id = getNextId();
                Database db = new Database(id, fullDbName);
                db.setClusterName(clusterName);
                unprotectCreateDb(db);
                editLog.logCreateDb(db);
            }
        } finally {
            unlock();
        }
        LOG.info("createDb dbName = " + fullDbName + ", id = " + id);
    }

    // For replay edit log, needn't lock metadata
    public void unprotectCreateDb(Database db) {
        idToDb.put(db.getId(), db);
        fullNameToDb.put(db.getFullName(), db);
        db.writeLock();
        db.setExist(true);
        db.writeUnlock();
        final Cluster cluster = nameToCluster.get(db.getClusterName());
        cluster.addDb(db.getFullName(), db.getId());
        stateMgr.getGlobalTransactionMgr().addDatabaseTransactionMgr(db.getId());
    }

    // for test
    public void addCluster(Cluster cluster) {
        nameToCluster.put(cluster.getName(), cluster);
        idToCluster.put(cluster.getId(), cluster);
    }

    public ConcurrentHashMap<Long, Database> getIdToDb() {
        return idToDb;
    }

    public void replayCreateDb(Database db) {
        tryLock(true);
        try {
            unprotectCreateDb(db);
            LOG.info("finish replay create db, name: {}, id: {}", db.getFullName(), db.getId());
        } finally {
            unlock();
        }
    }

    @Override
    public void dropDb(DropDbStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();

        // 1. check if database exists
        if (!tryLock(false)) {
            throw new DdlException("Failed to acquire globalStateMgr lock. Try again");
        }
        try {
            if (!fullNameToDb.containsKey(dbName)) {
                if (stmt.isSetIfExists()) {
                    LOG.info("drop database[{}] which does not exist", dbName);
                    return;
                } else {
                    ErrorReport.reportDdlException(ErrorCode.ERR_DB_DROP_EXISTS, dbName);
                }
            }

            // 2. drop tables in db
            Database db = this.fullNameToDb.get(dbName);
            HashMap<Long, AgentBatchTask> batchTaskMap;
            db.writeLock();
            try {
                if (!stmt.isForceDrop()) {
                    if (stateMgr.getGlobalTransactionMgr()
                            .existCommittedTxns(db.getId(), null, null)) {
                        throw new DdlException(
                                "There are still some transactions in the COMMITTED state waiting to be completed. " +
                                        "The database [" + dbName +
                                        "] cannot be dropped. If you want to forcibly drop(cannot be recovered)," +
                                        " please use \"DROP database FORCE\".");
                    }
                }
                if (db.getDbState() == Database.DbState.LINK && dbName.equals(db.getAttachDb())) {
                    // We try to drop a hard link.
                    final DropLinkDbAndUpdateDbInfo info = new DropLinkDbAndUpdateDbInfo();
                    fullNameToDb.remove(db.getAttachDb());
                    db.setDbState(Database.DbState.NORMAL);
                    info.setUpdateDbState(Database.DbState.NORMAL);
                    final Cluster cluster = nameToCluster
                            .get(ClusterNamespace.getClusterNameFromFullName(db.getAttachDb()));
                    final BaseParam param = new BaseParam();
                    param.addStringParam(db.getAttachDb());
                    param.addLongParam(db.getId());
                    cluster.removeLinkDb(param);
                    info.setDropDbCluster(cluster.getName());
                    info.setDropDbId(db.getId());
                    info.setDropDbName(db.getAttachDb());
                    editLog.logDropLinkDb(info);
                    return;
                }

                if (db.getDbState() == Database.DbState.LINK && dbName.equals(db.getFullName())) {
                    // We try to drop a db which other dbs attach to it,
                    // which is not allowed.
                    ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_DB_STATE_LINK_OR_MIGRATE,
                            ClusterNamespace.getNameFromFullName(dbName));
                    return;
                }

                if (dbName.equals(db.getAttachDb()) && db.getDbState() == Database.DbState.MOVE) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_DB_STATE_LINK_OR_MIGRATE,
                            ClusterNamespace.getNameFromFullName(dbName));
                    return;
                }

                // save table names for recycling
                Set<String> tableNames = db.getTableNamesWithLock();
                batchTaskMap = unprotectDropDb(db, stmt.isForceDrop(), false);
                if (!stmt.isForceDrop()) {
                    recycleBin.recycleDatabase(db, tableNames);
                } else {
                    stateMgr.onEraseDatabase(db.getId());
                }
                db.setExist(false);
            } finally {
                db.writeUnlock();
            }
            sendDropTabletTasks(batchTaskMap);

            // 3. remove db from globalStateMgr
            idToDb.remove(db.getId());
            fullNameToDb.remove(db.getFullName());
            final Cluster cluster = nameToCluster.get(db.getClusterName());
            cluster.removeDb(dbName, db.getId());
            DropDbInfo info = new DropDbInfo(dbName, stmt.isForceDrop());
            editLog.logDropDb(info);

            LOG.info("finish drop database[{}], id: {}, is force : {}", dbName, db.getId(), stmt.isForceDrop());
        } finally {
            unlock();
        }
    }

    public HashMap<Long, AgentBatchTask> unprotectDropDb(Database db, boolean isForeDrop, boolean isReplay) {
        HashMap<Long, AgentBatchTask> batchTaskMap = new HashMap<>();
        for (Table table : db.getTables()) {
            HashMap<Long, AgentBatchTask> dropTasks = unprotectDropTable(db, table.getId(), isForeDrop, isReplay);
            if (!isReplay) {
                for (Long backendId : dropTasks.keySet()) {
                    AgentBatchTask batchTask = batchTaskMap.get(backendId);
                    if (batchTask == null) {
                        batchTask = new AgentBatchTask();
                        batchTaskMap.put(backendId, batchTask);
                    }
                    batchTask.addTasks(backendId, dropTasks.get(backendId).getAllTasks());
                }
            }
        }
        return batchTaskMap;
    }

    public HashMap<Long, AgentBatchTask> unprotectDropTable(Database db, long tableId, boolean isForceDrop,
                                                            boolean isReplay) {
        HashMap<Long, AgentBatchTask> batchTaskMap = new HashMap<>();
        Table table = db.getTable(tableId);
        // delete from db meta
        if (table == null) {
            return batchTaskMap;
        }

        table.onDrop();

        db.dropTable(table.getName());
        if (!isForceDrop) {
            Table oldTable = recycleBin.recycleTable(db.getId(), table);
            if (oldTable != null && oldTable.getType() == Table.TableType.OLAP) {
                batchTaskMap = stateMgr.onEraseOlapTable((OlapTable) oldTable, false);
            }
        } else {
            if (table.getType() == Table.TableType.OLAP) {
                batchTaskMap = stateMgr.onEraseOlapTable((OlapTable) table, isReplay);
            }
        }

        LOG.info("finished dropping table[{}] in db[{}], tableId: {}", table.getName(), db.getFullName(),
                table.getId());
        return batchTaskMap;
    }

    public void replayDropLinkDb(DropLinkDbAndUpdateDbInfo info) {
        tryLock(true);
        try {
            final Database db = this.fullNameToDb.remove(info.getDropDbName());
            db.setDbState(info.getUpdateDbState());
            final Cluster cluster = nameToCluster
                    .get(info.getDropDbCluster());
            final BaseParam param = new BaseParam();
            param.addStringParam(db.getAttachDb());
            param.addLongParam(db.getId());
            cluster.removeLinkDb(param);
        } finally {
            unlock();
        }
    }

    public void replayDropDb(String dbName, boolean isForceDrop) throws DdlException {
        tryLock(true);
        try {
            Database db = fullNameToDb.get(dbName);
            db.writeLock();
            try {
                Set<String> tableNames = db.getTableNamesWithLock();
                unprotectDropDb(db, isForceDrop, true);
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
            final Cluster cluster = nameToCluster.get(db.getClusterName());
            cluster.removeDb(dbName, db.getId());

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
                throw new DdlException("Database[" + db.getFullName() + "] already exist.");
                // it's ok that we do not put db back to CatalogRecycleBin
                // cause this db cannot recover any more
            }

            fullNameToDb.put(db.getFullName(), db);
            idToDb.put(db.getId(), db);
            db.writeLock();
            db.setExist(true);
            db.writeUnlock();
            final Cluster cluster = nameToCluster.get(db.getClusterName());
            cluster.addDb(db.getFullName(), db.getId());

            // log
            RecoverInfo recoverInfo = new RecoverInfo(db.getId(), -1L, -1L);
            editLog.logRecoverDb(recoverInfo);
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

            if (table.getType() != Table.TableType.OLAP) {
                throw new DdlException("table[" + tableName + "] is not OLAP table");
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

        LOG.info("replay recover db[{}], name: {}", dbId, db.getFullName());
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
        DatabaseInfo dbInfo = new DatabaseInfo(dbName, "", quota, quotaType);
        editLog.logAlterDb(dbInfo);
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

    public void renameDatabase(AlterDatabaseRename stmt) throws DdlException {
        String fullDbName = stmt.getDbName();
        String newFullDbName = stmt.getNewDbName();
        String clusterName = stmt.getClusterName();

        if (fullDbName.equals(newFullDbName)) {
            throw new DdlException("Same database name");
        }

        Database db = null;
        Cluster cluster = null;
        if (!tryLock(false)) {
            throw new DdlException("Failed to acquire globalStateMgr lock. Try again");
        }
        try {
            cluster = nameToCluster.get(clusterName);
            if (cluster == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_NO_EXISTS, clusterName);
            }
            // check if db exists
            db = fullNameToDb.get(fullDbName);
            if (db == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, fullDbName);
            }

            if (db.getDbState() == Database.DbState.LINK || db.getDbState() == Database.DbState.MOVE) {
                ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_RENAME_DB_ERR, fullDbName);
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

            DatabaseInfo dbInfo = new DatabaseInfo(fullDbName, newFullDbName, -1L, AlterDatabaseQuotaStmt.QuotaType.NONE);
            editLog.logDatabaseRename(dbInfo);
        } finally {
            unlock();
        }

        LOG.info("rename database[{}] to [{}], id: {}", fullDbName, newFullDbName, db.getId());
    }

    public void replayRenameDatabase(String dbName, String newDbName) {
        tryLock(true);
        try {
            Database db = fullNameToDb.get(dbName);
            Cluster cluster = nameToCluster.get(db.getClusterName());
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
            systemInfoService.checkClusterCapacity(stmt.getClusterName());
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

        if (engineName.equalsIgnoreCase("olap")) {
            createOlapTable(db, stmt);
        } else if (engineName.equalsIgnoreCase("mysql")) {
            createMysqlTable(db, stmt);
        } else if (engineName.equalsIgnoreCase("elasticsearch") || engineName.equalsIgnoreCase("es")) {
            createEsTable(db, stmt);
        } else if (engineName.equalsIgnoreCase("hive")) {
            createHiveTable(db, stmt);
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

    public void createTableLike(CreateTableLikeStmt stmt) throws DdlException {
        try {
            Database db = getDb(stmt.getExistedDbName());
            List<String> createTableStmt = Lists.newArrayList();
            db.readLock();
            try {
                Table table = db.getTable(stmt.getExistedTableName());
                if (table == null) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, stmt.getExistedTableName());
                }
                GlobalStateMgr.getDdlStmt(stmt.getDbName(), table, createTableStmt, null, null, false, false);
                if (createTableStmt.isEmpty()) {
                    ErrorReport.reportDdlException(ErrorCode.ERROR_CREATE_TABLE_LIKE_EMPTY, "CREATE");
                }
            } finally {
                db.readUnlock();
            }
            StatementBase statementBase = com.starrocks.sql.parser.SqlParser.parse(createTableStmt.get(0),
                    ConnectContext.get().getSessionVariable().getSqlMode()).get(0);
            com.starrocks.sql.analyzer.Analyzer.analyze(statementBase, ConnectContext.get());
            if (statementBase instanceof CreateTableStmt) {
                CreateTableStmt parsedCreateTableStmt = (CreateTableStmt) statementBase;
                parsedCreateTableStmt.setTableName(stmt.getTableName());
                if (stmt.isSetIfNotExists()) {
                    parsedCreateTableStmt.setIfNotExists();
                }
                createTable(parsedCreateTableStmt);
            } else if (statementBase instanceof CreateViewStmt) {
                ErrorReport.reportDdlException(ErrorCode.ERROR_CREATE_TABLE_LIKE_UNSUPPORTED_VIEW);
            }
        } catch (UserException e) {
            throw new DdlException("Failed to execute CREATE TABLE LIKE " + stmt.getExistedTableName() + ". Reason: " +
                    e.getMessage(), e);
        }
    }

    @Override
    public void addPartitions(Database db, String tableName, AddPartitionClause addPartitionClause)
            throws DdlException, AnalysisException {
        PartitionDesc partitionDesc = addPartitionClause.getPartitionDesc();
        if (partitionDesc instanceof SingleRangePartitionDesc) {
            addPartitions(db, tableName, ImmutableList.of((SingleRangePartitionDesc) partitionDesc),
                    addPartitionClause);
        } else if (partitionDesc instanceof MultiRangePartitionDesc) {
            db.readLock();
            RangePartitionInfo rangePartitionInfo;
            Map<String, String> tableProperties;
            try {
                Table table = db.getTable(tableName);
                CatalogUtils.checkTableExist(db, tableName);
                CatalogUtils.checkTableTypeOLAP(db, table);
                OlapTable olapTable = (OlapTable) table;
                tableProperties = olapTable.getTableProperty().getProperties();
                PartitionInfo partitionInfo = olapTable.getPartitionInfo();
                rangePartitionInfo = (RangePartitionInfo) partitionInfo;
            } finally {
                db.readUnlock();
            }

            if (rangePartitionInfo == null) {
                throw new DdlException("Alter batch get partition info failed.");
            }

            List<Column> partitionColumns = rangePartitionInfo.getPartitionColumns();
            if (partitionColumns.size() != 1) {
                throw new DdlException("Alter batch build partition only support single range column.");
            }

            Column firstPartitionColumn = partitionColumns.get(0);
            MultiRangePartitionDesc multiRangePartitionDesc = (MultiRangePartitionDesc) partitionDesc;
            Map<String, String> properties = addPartitionClause.getProperties();
            if (properties == null) {
                properties = Maps.newHashMap();
            }
            if (tableProperties != null && tableProperties.containsKey(DynamicPartitionProperty.START_DAY_OF_WEEK)) {
                properties.put(DynamicPartitionProperty.START_DAY_OF_WEEK,
                        tableProperties.get(DynamicPartitionProperty.START_DAY_OF_WEEK));
            }
            List<SingleRangePartitionDesc> singleRangePartitionDescs = multiRangePartitionDesc
                    .convertToSingle(firstPartitionColumn.getType(), properties);
            addPartitions(db, tableName, singleRangePartitionDescs, addPartitionClause);
        }
    }

    private void addPartitions(Database db, String tableName, List<SingleRangePartitionDesc> singleRangePartitionDescs,
                              AddPartitionClause addPartitionClause) throws DdlException {
        DistributionInfo distributionInfo;
        OlapTable olapTable;
        OlapTable copiedTable;

        boolean isTempPartition = addPartitionClause.isTempPartition();
        db.readLock();
        try {
            Table table = db.getTable(tableName);
            CatalogUtils.checkTableExist(db, tableName);
            CatalogUtils.checkTableTypeOLAP(db, table);
            olapTable = (OlapTable) table;
            CatalogUtils.checkTableState(olapTable, tableName);
            // check partition type
            PartitionInfo partitionInfo = olapTable.getPartitionInfo();
            if (partitionInfo.getType() != PartitionType.RANGE) {
                throw new DdlException("Only support adding partition to range partitioned table");
            }
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
            Set<String> existPartitionNameSet =
                    CatalogUtils.checkPartitionNameExistForAddPartitions(olapTable, singleRangePartitionDescs);
            // partition properties is prior to clause properties
            // clause properties is prior to table properties
            Map<String, String> properties = Maps.newHashMap();
            properties = getOrSetDefaultProperties(olapTable, properties);
            Map<String, String> clauseProperties = addPartitionClause.getProperties();
            if (clauseProperties != null && !clauseProperties.isEmpty()) {
                properties.putAll(clauseProperties);
            }
            for (SingleRangePartitionDesc singleRangePartitionDesc : singleRangePartitionDescs) {
                Map<String, String> cloneProperties = Maps.newHashMap(properties);
                Map<String, String> sourceProperties = singleRangePartitionDesc.getProperties();
                if (sourceProperties != null && !sourceProperties.isEmpty()) {
                    cloneProperties.putAll(sourceProperties);
                }
                singleRangePartitionDesc.analyze(rangePartitionInfo.getPartitionColumns().size(), cloneProperties);
                if (!existPartitionNameSet.contains(singleRangePartitionDesc.getPartitionName())) {
                    rangePartitionInfo.checkAndCreateRange(singleRangePartitionDesc, isTempPartition);
                }
            }

            // get distributionInfo
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
                    if (hashDistributionInfo.getBucketNum() <= 0) {
                        throw new DdlException("Cannot assign hash distribution buckets less than 1");
                    }
                }
            } else {
                distributionInfo = defaultDistributionInfo;
            }

            // check colocation
            if (colocateTableIndex.isColocateTable(olapTable.getId())) {
                String fullGroupName = db.getId() + "_" + olapTable.getColocateGroup();
                ColocateGroupSchema groupSchema = colocateTableIndex.getGroupSchema(fullGroupName);
                Preconditions.checkNotNull(groupSchema);
                groupSchema.checkDistribution(distributionInfo);
                for (SingleRangePartitionDesc singleRangePartitionDesc : singleRangePartitionDescs) {
                    groupSchema.checkReplicationNum(singleRangePartitionDesc.getReplicationNum());
                }
            }

            copiedTable = olapTable.selectiveCopy(null, false, MaterializedIndex.IndexExtState.VISIBLE);
            copiedTable.setDefaultDistributionInfo(distributionInfo);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        } finally {
            db.readUnlock();
        }

        Preconditions.checkNotNull(distributionInfo);
        Preconditions.checkNotNull(olapTable);
        Preconditions.checkNotNull(copiedTable);

        // create partition outside db lock
        for (SingleRangePartitionDesc singleRangePartitionDesc : singleRangePartitionDescs) {
            DataProperty dataProperty = singleRangePartitionDesc.getPartitionDataProperty();
            Preconditions.checkNotNull(dataProperty);
        }

        Set<Long> tabletIdSetForAll = Sets.newHashSet();
        HashMap<String, Set<Long>> partitionNameToTabletSet = Maps.newHashMap();
        try {
            List<Partition> partitionList = Lists.newArrayListWithCapacity(singleRangePartitionDescs.size());

            for (SingleRangePartitionDesc singleRangePartitionDesc : singleRangePartitionDescs) {
                long partitionId = getNextId();
                DataProperty dataProperty = singleRangePartitionDesc.getPartitionDataProperty();
                String partitionName = singleRangePartitionDesc.getPartitionName();
                Long version = singleRangePartitionDesc.getVersionInfo();
                Set<Long> tabletIdSet = Sets.newHashSet();

                copiedTable.getPartitionInfo().setDataProperty(partitionId, dataProperty);
                copiedTable.getPartitionInfo().setTabletType(partitionId, singleRangePartitionDesc.getTabletType());
                copiedTable.getPartitionInfo()
                        .setReplicationNum(partitionId, singleRangePartitionDesc.getReplicationNum());
                copiedTable.getPartitionInfo().setIsInMemory(partitionId, singleRangePartitionDesc.isInMemory());

                Partition partition =
                        createPartition(db, copiedTable, partitionId, partitionName, version, tabletIdSet);

                // createPartition() will set partition's partitionInfo to the copiedTable's partitionInfo,
                // and this will cause memory leak because every partition will refer to a distinct PartitionInfo Object.
                // Set the partitionInfo to the olapTable's partitionInfo here.
                partition.setPartitionInfo(olapTable.getPartitionInfo());

                partitionList.add(partition);
                tabletIdSetForAll.addAll(tabletIdSet);
                partitionNameToTabletSet.put(partitionName, tabletIdSet);
            }

            buildPartitions(db, copiedTable, partitionList);

            // check again
            if (!db.writeLockAndCheckExist()) {
                throw new DdlException("db " + db.getFullName()
                        + "(" + db.getId() + ") has been dropped");
            }
            Set<String> existPartitionNameSet = Sets.newHashSet();
            try {
                CatalogUtils.checkTableExist(db, tableName);
                Table table = db.getTable(tableName);
                CatalogUtils.checkTableTypeOLAP(db, table);
                olapTable = (OlapTable) table;
                CatalogUtils.checkTableState(olapTable, tableName);
                existPartitionNameSet = CatalogUtils.checkPartitionNameExistForAddPartitions(olapTable,
                        singleRangePartitionDescs);

                if (existPartitionNameSet.size() > 0) {
                    for (String partitionName : existPartitionNameSet) {
                        LOG.info("add partition[{}] which already exists", partitionName);
                    }
                }

                // check if meta changed
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

                // check partition type
                PartitionInfo partitionInfo = olapTable.getPartitionInfo();
                if (partitionInfo.getType() != PartitionType.RANGE) {
                    throw new DdlException("Only support adding partition to range partitioned table");
                }

                // update partition info
                RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
                rangePartitionInfo.handleNewRangePartitionDescs(singleRangePartitionDescs,
                        partitionList, existPartitionNameSet, isTempPartition);

                if (isTempPartition) {
                    for (Partition partition : partitionList) {
                        if (!existPartitionNameSet.contains(partition.getName())) {
                            olapTable.addTempPartition(partition);
                        }
                    }
                } else {
                    for (Partition partition : partitionList) {
                        if (!existPartitionNameSet.contains(partition.getName())) {
                            olapTable.addPartition(partition);
                        }
                    }
                }

                // log
                int partitionLen = partitionList.size();
                // Forward compatible with previous log formats
                // Version 1.15 is compatible if users only use single-partition syntax.
                // Otherwise, the followers will be crash when reading the new log
                if (partitionLen == 1) {
                    Partition partition = partitionList.get(0);
                    if (existPartitionNameSet.contains(partition.getName())) {
                        LOG.info("add partition[{}] which already exists", partition.getName());
                        return;
                    }
                    long partitionId = partition.getId();
                    PartitionPersistInfo info = new PartitionPersistInfo(db.getId(), olapTable.getId(), partition,
                            rangePartitionInfo.getRange(partitionId),
                            singleRangePartitionDescs.get(0).getPartitionDataProperty(),
                            rangePartitionInfo.getReplicationNum(partitionId),
                            rangePartitionInfo.getIsInMemory(partitionId),
                            isTempPartition);
                    editLog.logAddPartition(info);

                    LOG.info("succeed in creating partition[{}], name: {}, temp: {}", partitionId,
                            partition.getName(), isTempPartition);
                } else {
                    List<PartitionPersistInfo> partitionInfoList = Lists.newArrayListWithCapacity(partitionLen);
                    for (int i = 0; i < partitionLen; i++) {
                        Partition partition = partitionList.get(i);
                        if (!existPartitionNameSet.contains(partition.getName())) {
                            PartitionPersistInfo info =
                                    new PartitionPersistInfo(db.getId(), olapTable.getId(), partition,
                                            rangePartitionInfo.getRange(partition.getId()),
                                            singleRangePartitionDescs.get(i).getPartitionDataProperty(),
                                            rangePartitionInfo.getReplicationNum(partition.getId()),
                                            rangePartitionInfo.getIsInMemory(partition.getId()),
                                            isTempPartition);
                            partitionInfoList.add(info);
                        }
                    }

                    AddPartitionsInfo infos = new AddPartitionsInfo(partitionInfoList);
                    editLog.logAddPartitions(infos);

                    for (Partition partition : partitionList) {
                        LOG.info("succeed in creating partitions[{}], name: {}, temp: {}", partition.getId(),
                                partition.getName(), isTempPartition);
                    }
                }
            } finally {
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
                db.writeUnlock();
            }
        } catch (DdlException e) {
            for (Long tabletId : tabletIdSetForAll) {
                GlobalStateMgr.getCurrentInvertedIndex().deleteTablet(tabletId);
            }
            throw e;
        }
    }

    public Map<String, String> getOrSetDefaultProperties(OlapTable olapTable, Map<String, String> sourceProperties) {
        // partition properties should inherit table properties
        Short replicationNum = olapTable.getDefaultReplicationNum();
        if (sourceProperties == null) {
            sourceProperties = Maps.newConcurrentMap();
        }
        if (!sourceProperties.containsKey(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM)) {
            sourceProperties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM, replicationNum.toString());
        }
        if (!sourceProperties.containsKey(PropertyAnalyzer.PROPERTIES_INMEMORY)) {
            sourceProperties.put(PropertyAnalyzer.PROPERTIES_INMEMORY, olapTable.isInMemory().toString());
        }
        Map<String, String> tableProperty = olapTable.getTableProperty().getProperties();
        if (tableProperty != null && tableProperty.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM)) {
            sourceProperties.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM,
                    tableProperty.get(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM));
        }
        return sourceProperties;
    }

    public void replayAddPartition(PartitionPersistInfo info) throws DdlException {
        Database db = this.getDb(info.getDbId());
        db.writeLock();
        try {
            OlapTable olapTable = (OlapTable) db.getTable(info.getTableId());
            Partition partition = info.getPartition();
            if (olapTable == null) {
                // This usually caused by materialized view table downgrade
                LOG.warn("ignore add partition log for tableId={}, partitionId={}, partitionName={}",
                        info.getTableId(), partition.getId(), partition.getName());
                return;
            }
            PartitionInfo partitionInfo = olapTable.getPartitionInfo();
            if (info.isTempPartition()) {
                olapTable.addTempPartition(partition);
            } else {
                olapTable.addPartition(partition);
            }

            if (partitionInfo instanceof RangePartitionInfo) {
                ((RangePartitionInfo) partitionInfo).unprotectHandleNewSinglePartitionDesc(partition.getId(),
                        info.isTempPartition(), info.getRange(), info.getDataProperty(), info.getReplicationNum(),
                        info.isInMemory());
            } else {
                // for materialized view compatibility
                return;
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
                        for (Replica replica : ((LocalTablet) tablet).getReplicas()) {
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
        if (partitionInfo.getType() != PartitionType.RANGE) {
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
                                        " please use \"DROP partition FORCE\".");
                    }
                }
            }
            olapTable.dropPartition(db.getId(), partitionName, clause.isForceDrop());
        }

        // log
        DropPartitionInfo info = new DropPartitionInfo(db.getId(), olapTable.getId(), partitionName, isTempPartition,
                clause.isForceDrop());
        editLog.logDropPartition(info);

        LOG.info("succeed in droping partition[{}], is temp : {}, is force : {}", partitionName, isTempPartition,
                clause.isForceDrop());
    }

    public void replayDropPartition(DropPartitionInfo info) {
        Database db = this.getDb(info.getDbId());
        db.writeLock();
        try {
            OlapTable olapTable = (OlapTable) db.getTable(info.getTableId());
            if (olapTable == null) {
                // This usually caused by materialized view table downgrade
                LOG.warn("ignore drop partition log for tableId={}, dbId={}, partitionName={}",
                        info.getTableId(), info.getDbId(), info.getPartitionName());
                return;
            }
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
            rollup.setUseStarOS(partitionInfo.isUseStarOS(partitionId));
            indexMap.put(indexId, rollup);
        }
        DistributionInfo distributionInfo = table.getDefaultDistributionInfo();
        Partition partition =
                new Partition(partitionId, partitionName, indexMap.get(table.getBaseIndexId()), distributionInfo);
        partition.setPartitionInfo(partitionInfo);

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
                            storageMedium);
            createTablets(db.getClusterName(), index, Replica.ReplicaState.NORMAL, distributionInfo,
                    partition.getVisibleVersion(),
                    replicationNum, tabletMeta, tabletIdSet);
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
        int numAliveBackends = systemInfoService.getBackendIds(true).size();
        int numReplicas = 0;
        for (Partition partition : partitions) {
            numReplicas += partition.getReplicaCount();
        }

        if (partitions.size() >= 3 && numAliveBackends >= 3 && numReplicas >= numAliveBackends * 500) {
            LOG.info("creating {} partitions of table {} concurrently", partitions.size(), table.getName());
            buildPartitionsConcurrently(db.getId(), table, partitions, numReplicas, numAliveBackends);
        } else if (numAliveBackends > 0) {
            buildPartitionsSequentially(db.getId(), table, partitions, numReplicas, numAliveBackends);
        } else {
            throw new DdlException("no alive backend");
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
                sendCreateReplicaTasksAndWaitForFinished(tasks, Math.min(timeout, maxTimeout));
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
        int timeout = numReplicas / numBackends * Config.tablet_create_timeout_second;
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
            waitForFinished(countDownLatch, Math.min(timeout, maxTimeout));
        } catch (Exception e) {
            countDownLatch.countDownToZero(new Status(TStatusCode.UNKNOWN, e.getMessage()));
            throw e;
        }
    }

    private List<CreateReplicaTask> buildCreateReplicaTasks(long dbId, OlapTable table, List<Partition> partitions) {
        List<CreateReplicaTask> tasks = new ArrayList<>();
        for (Partition partition : partitions) {
            tasks.addAll(buildCreateReplicaTasks(dbId, table, partition));
        }
        return tasks;
    }

    private List<CreateReplicaTask> buildCreateReplicaTasks(long dbId, OlapTable table, Partition partition) {
        ArrayList<CreateReplicaTask> tasks = new ArrayList<>((int) partition.getReplicaCount());
        for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE)) {
            tasks.addAll(buildCreateReplicaTasks(dbId, table, partition, index));
        }
        return tasks;
    }

    private List<CreateReplicaTask> buildCreateReplicaTasks(long dbId, OlapTable table, Partition partition,
                                                            MaterializedIndex index) {
        List<CreateReplicaTask> tasks = new ArrayList<>((int) index.getReplicaCount());
        boolean useStarOS = partition.isUseStarOS();
        MaterializedIndexMeta indexMeta = table.getIndexMetaByIndexId(index.getId());
        for (Tablet tablet : index.getTablets()) {
            if (useStarOS) {
                CreateReplicaTask task = new CreateReplicaTask(
                        ((LakeTablet) tablet).getPrimaryBackendId(),
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
                        table.getPartitionInfo().getTabletType(partition.getId()));
                tasks.add(task);
            } else {
                for (Replica replica : ((LocalTablet) tablet).getReplicas()) {
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
                            table.getPartitionInfo().getTabletType(partition.getId()));
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
                        "To increase the config \"tablet_create_timeout_second\" (currently %d), run the following command:\n" +
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

    // Create olap table and related base index synchronously.
    private void createOlapTable(Database db, CreateTableStmt stmt) throws DdlException {
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
            } else {
                throw new DdlException("Currently only support range or list partition with engine type olap");
            }
            partitionInfo = partitionDesc.toPartitionInfo(baseSchema, partitionNameToId, false);
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

        // calc short key column count
        short shortKeyColumnCount = GlobalStateMgr.calcShortKeyColumnCount(baseSchema, stmt.getProperties());
        LOG.debug("create table[{}] short key column count: {}", tableName, shortKeyColumnCount);

        // indexes
        TableIndexes indexes = new TableIndexes(stmt.getIndexes());

        // create table
        long tableId = GlobalStateMgr.getCurrentState().getNextId();
        OlapTable olapTable = null;
        if (stmt.isExternal()) {
            olapTable = new ExternalOlapTable(db.getId(), tableId, tableName, baseSchema, keysType, partitionInfo,
                    distributionInfo, indexes, stmt.getProperties());
        } else {
            olapTable = new OlapTable(tableId, tableName, baseSchema, keysType, partitionInfo,
                    distributionInfo, indexes);
        }
        olapTable.setComment(stmt.getComment());

        // set base index id
        long baseIndexId = getNextId();
        olapTable.setBaseIndexId(baseIndexId);

        // set base index info to table
        // this should be done before create partition.
        Map<String, String> properties = stmt.getProperties();

        // analyze bloom filter columns
        Set<String> bfColumns = null;
        double bfFpp = 0;
        try {
            bfColumns = PropertyAnalyzer.analyzeBloomFilterColumns(properties, baseSchema,
                    olapTable.getKeysType() == KeysType.PRIMARY_KEYS);
            if (bfColumns != null && bfColumns.isEmpty()) {
                bfColumns = null;
            }

            bfFpp = PropertyAnalyzer.analyzeBloomFilterFpp(properties);
            if (bfColumns != null && bfFpp == 0) {
                bfFpp = FeConstants.default_bloom_filter_fpp;
            } else if (bfColumns == null) {
                bfFpp = 0;
            }

            olapTable.setBloomFilterInfo(bfColumns, bfFpp);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }

        // analyze replication_num
        short replicationNum = FeConstants.default_replication_num;
        try {
            boolean isReplicationNumSet =
                    properties != null && properties.containsKey(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM);
            replicationNum = PropertyAnalyzer.analyzeReplicationNum(properties, replicationNum);
            if (isReplicationNumSet) {
                olapTable.setReplicationNum(replicationNum);
            }
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }

        // set in memory
        boolean isInMemory =
                PropertyAnalyzer.analyzeBooleanProp(properties, PropertyAnalyzer.PROPERTIES_INMEMORY, false);
        olapTable.setIsInMemory(isInMemory);

        boolean enablePersistentIndex =
                PropertyAnalyzer.analyzeBooleanProp(properties, PropertyAnalyzer.PROPERTIES_ENABLE_PERSISTENT_INDEX,
                        false);
        olapTable.setEnablePersistentIndex(enablePersistentIndex);

        if (olapTable.getKeysType() == KeysType.PRIMARY_KEYS && olapTable.enablePersistentIndex()) {
            if (!olapTable.checkPersistentIndex()) {
                throw new DdlException("PrimaryKey table using persistent index don't support varchar(char) as key so far," +
                                       " and key length should be no more than 64 Bytes");
            }
        }

        TTabletType tabletType = TTabletType.TABLET_TYPE_DISK;
        try {
            tabletType = PropertyAnalyzer.analyzeTabletType(properties);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
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
                        DataProperty.getInferredDefaultDataProperty());
                if (hasMedium) {
                    olapTable.setStorageMedium(dataProperty.getStorageMedium());
                }
            } catch (AnalysisException e) {
                throw new DdlException(e.getMessage());
            }
            Preconditions.checkNotNull(dataProperty);
            partitionInfo.setDataProperty(partitionId, dataProperty);
            partitionInfo.setReplicationNum(partitionId, replicationNum);
            partitionInfo.setIsInMemory(partitionId, isInMemory);
            partitionInfo.setTabletType(partitionId, tabletType);
        }

        // check colocation properties
        try {
            String colocateGroup = PropertyAnalyzer.analyzeColocate(properties);
            if (!Strings.isNullOrEmpty(colocateGroup)) {
                String fullGroupName = db.getId() + "_" + colocateGroup;
                ColocateGroupSchema groupSchema = colocateTableIndex.getGroupSchema(fullGroupName);
                if (groupSchema != null) {
                    // group already exist, check if this table can be added to this group
                    groupSchema.checkColocateSchema(olapTable);
                }
                // add table to this group, if group does not exist, create a new one
                colocateTableIndex.addTableToGroup(db.getId(), olapTable, colocateGroup,
                        null /* generate group id inside */);
                olapTable.setColocateGroup(colocateGroup);
            }
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
        olapTable.setIndexMeta(baseIndexId, tableName, baseSchema, schemaVersion, schemaHash,
                shortKeyColumnCount, baseIndexStorageType, keysType);

        for (AlterClause alterClause : stmt.getRollupAlterClauseList()) {
            AddRollupClause addRollupClause = (AddRollupClause) alterClause;

            Long baseRollupIndex = olapTable.getIndexIdByName(tableName);

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
                    olapTable, baseRollupIndex, false);
            short rollupShortKeyColumnCount =
                    GlobalStateMgr.calcShortKeyColumnCount(rollupColumns, alterClause.getProperties());
            int rollupSchemaHash = Util.schemaHash(schemaVersion, rollupColumns, bfColumns, bfFpp);
            long rollupIndexId = getNextId();
            olapTable.setIndexMeta(rollupIndexId, addRollupClause.getRollupName(), rollupColumns, schemaVersion,
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
        olapTable.setStorageFormat(storageFormat);

        // a set to record every new tablet created when create table
        // if failed in any step, use this set to do clear things
        Set<Long> tabletIdSet = new HashSet<Long>();

        boolean createTblSuccess = false;
        boolean addToColocateGroupSuccess = false;
        // create partition
        try {
            // do not create partition for external table
            if (olapTable.getType() == Table.TableType.OLAP) {
                if (partitionInfo.getType() == PartitionType.UNPARTITIONED) {
                    // this is a 1-level partitioned table, use table name as partition name
                    long partitionId = partitionNameToId.get(tableName);
                    Partition partition = createPartition(db, olapTable, partitionId, tableName, version, tabletIdSet);
                    buildPartitions(db, olapTable, Collections.singletonList(partition));
                    olapTable.addPartition(partition);
                } else if (partitionInfo.getType() == PartitionType.RANGE
                        || partitionInfo.getType() == PartitionType.LIST) {
                    try {
                        // just for remove entries in stmt.getProperties(),
                        // and then check if there still has unknown properties
                        boolean hasMedium = false;
                        if (properties != null) {
                            hasMedium = properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM);
                        }
                        DataProperty dataProperty = PropertyAnalyzer.analyzeDataProperty(properties,
                                DataProperty.getInferredDefaultDataProperty());
                        DynamicPartitionUtil
                                .checkAndSetDynamicPartitionBuckets(properties, distributionDesc.getBuckets());
                        DynamicPartitionUtil.checkAndSetDynamicPartitionProperty(olapTable, properties);
                        if (olapTable.dynamicPartitionExists() && olapTable.getColocateGroup() != null) {
                            HashDistributionInfo info = (HashDistributionInfo) distributionInfo;
                            if (info.getBucketNum() !=
                                    olapTable.getTableProperty().getDynamicPartitionProperty().getBuckets()) {
                                throw new DdlException("dynamic_partition.buckets should equal the distribution buckets"
                                        + " if creating a colocate table");
                            }
                        }
                        if (hasMedium) {
                            olapTable.setStorageMedium(dataProperty.getStorageMedium());
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
                        Partition partition = createPartition(db, olapTable, entry.getValue(), entry.getKey(), version,
                                tabletIdSet);
                        partitions.add(partition);
                    }
                    // It's ok if partitions is empty.
                    buildPartitions(db, olapTable, partitions);
                    for (Partition partition : partitions) {
                        olapTable.addPartition(partition);
                    }
                } else {
                    throw new DdlException("Unsupported partition method: " + partitionInfo.getType().name());
                }
            }

            // check database exists again, because database can be dropped when creating table
            if (!tryLock(false)) {
                throw new DdlException("Failed to acquire globalStateMgr lock. Try again");
            }
            try {
                if (getDb(db.getId()) == null) {
                    throw new DdlException("database has been dropped when creating table");
                }
                createTblSuccess = db.createTableWithLock(olapTable, false);
                if (!createTblSuccess) {
                    if (!stmt.isSetIfNotExists()) {
                        ErrorReport
                                .reportDdlException(ErrorCode.ERR_CANT_CREATE_TABLE, tableName, "table already exists");
                    } else {
                        LOG.info("Create table[{}] which already exists", tableName);
                        return;
                    }
                }
            } finally {
                unlock();
            }

            // NOTE: The table has been added to the database, and the following procedure cannot throw exception.

            // we have added these index to memory, only need to persist here
            if (colocateTableIndex.isColocateTable(tableId)) {
                ColocateTableIndex.GroupId groupId = colocateTableIndex.getGroup(tableId);
                List<List<Long>> backendsPerBucketSeq = colocateTableIndex.getBackendsPerBucketSeq(groupId);
                ColocatePersistInfo info =
                        ColocatePersistInfo.createForAddTable(groupId, tableId, backendsPerBucketSeq);
                editLog.logColocateAddTable(info);
                addToColocateGroupSuccess = true;
            }
            LOG.info("Successfully create table[{};{}]", tableName, tableId);
            DynamicPartitionUtil.registerOrRemovePartitionScheduleInfo(db.getId(), olapTable);
        } finally {
            if (!createTblSuccess) {
                for (Long tabletId : tabletIdSet) {
                    GlobalStateMgr.getCurrentInvertedIndex().deleteTablet(tabletId);
                }
            }
            // only remove from memory, because we have not persist it
            if (colocateTableIndex.isColocateTable(tableId) && !addToColocateGroupSuccess) {
                colocateTableIndex.removeTable(tableId);
            }
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
            throw new DdlException("Failed to acquire globalStateMgr lock. Try again");
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

        LOG.info("successfully create table{} with id {}", tableName, tableId);
    }

    private void createHiveTable(Database db, CreateTableStmt stmt) throws DdlException {
        String tableName = stmt.getTableName();
        List<Column> columns = stmt.getColumns();
        long tableId = getNextId();
        HiveTable hiveTable = new HiveTable(tableId, tableName, columns, stmt.getProperties());
        // partition key, commented for show partition key
        String partitionCmt = "PARTITION BY (" + String.join(", ", hiveTable.getPartitionColumnNames()) + ")";
        if (Strings.isNullOrEmpty(stmt.getComment())) {
            hiveTable.setComment(partitionCmt);
        } else {
            hiveTable.setComment(stmt.getComment());
        }

        // check database exists again, because database can be dropped when creating table
        if (!tryLock(false)) {
            throw new DdlException("Failed to acquire globalStateMgr lock. Try again");
        }
        try {
            if (getDb(db.getId()) == null) {
                throw new DdlException("database has been dropped when creating table");
            }
            if (!db.createTableWithLock(hiveTable, false)) {
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

        LOG.info("successfully create table[{}-{}]", tableName, tableId);
    }

    private void createIcebergTable(Database db, CreateTableStmt stmt) throws DdlException {
        String tableName = stmt.getTableName();
        List<Column> columns = stmt.getColumns();
        long tableId = getNextId();
        IcebergTable icebergTable = new IcebergTable(tableId, tableName, columns, stmt.getProperties());
        if (!Strings.isNullOrEmpty(stmt.getComment())) {
            icebergTable.setComment(stmt.getComment());
        }

        // check database exists again, because database can be dropped when creating table
        if (!tryLock(false)) {
            throw new DdlException("Failed to acquire globalStateMgr lock. Try again");
        }
        try {
            if (getDb(db.getId()) == null) {
                throw new DdlException("database has been dropped when creating table");
            }
            if (!db.createTableWithLock(icebergTable, false)) {
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

        LOG.info("successfully create table[{}-{}]", tableName, tableId);
    }

    private void createHudiTable(Database db, CreateTableStmt stmt) throws DdlException {
        String tableName = stmt.getTableName();
        List<Column> columns = stmt.getColumns();

        Set<String> metaFields = new HashSet<>(Arrays.asList(
                HoodieRecord.COMMIT_TIME_METADATA_FIELD,
                HoodieRecord.COMMIT_SEQNO_METADATA_FIELD,
                HoodieRecord.RECORD_KEY_METADATA_FIELD,
                HoodieRecord.PARTITION_PATH_METADATA_FIELD,
                HoodieRecord.FILENAME_METADATA_FIELD));
        Set<String> includedMetaFields = columns.stream().map(Column::getName)
                .filter(metaFields::contains).collect(Collectors.toSet());
        metaFields.removeAll(includedMetaFields);
        metaFields.forEach(f -> columns.add(new Column(f, Type.STRING, true)));

        long tableId = getNextId();
        HudiTable hudiTable = new HudiTable(tableId, tableName, columns, stmt.getProperties());
        // partition key, commented for show partition key
        String partitionCmt = "PARTITION BY (" + String.join(", ", hudiTable.getPartitionColumnNames()) + ")";
        if (Strings.isNullOrEmpty(stmt.getComment())) {
            hudiTable.setComment(partitionCmt);
        } else {
            hudiTable.setComment(stmt.getComment());
        }

        // check database exists again, because database can be dropped when creating table
        if (!tryLock(false)) {
            throw new DdlException("Failed to acquire globalStateMgr lock. Try again");
        }
        try {
            if (getDb(db.getFullName()) == null) {
                throw new DdlException("Database has been dropped when creating table");
            }
            if (!db.createTableWithLock(hudiTable, false)) {
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
            if (getDb(db.getFullName()) == null) {
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

    public void replayCreateTable(String dbName, Table table) {
        Database db = this.fullNameToDb.get(dbName);
        db.createTableWithLock(table, true);

        if (!isCheckpointThread()) {
            // add to inverted index
            if (table.getType() == Table.TableType.OLAP) {
                TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentInvertedIndex();
                OlapTable olapTable = (OlapTable) table;
                long dbId = db.getId();
                long tableId = table.getId();
                for (Partition partition : olapTable.getAllPartitions()) {
                    long partitionId = partition.getId();
                    TStorageMedium medium = olapTable.getPartitionInfo().getDataProperty(
                            partitionId).getStorageMedium();
                    boolean useStarOS = partition.isUseStarOS();
                    for (MaterializedIndex mIndex : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                        long indexId = mIndex.getId();
                        int schemaHash = olapTable.getSchemaHashByIndexId(indexId);
                        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, schemaHash, medium);
                        for (Tablet tablet : mIndex.getTablets()) {
                            long tabletId = tablet.getId();
                            invertedIndex.addTablet(tabletId, tabletMeta);
                            if (!useStarOS) {
                                for (Replica replica : ((LocalTablet) tablet).getReplicas()) {
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

    private void createTablets(String clusterName, MaterializedIndex index, Replica.ReplicaState replicaState,
                               DistributionInfo distributionInfo, long version, short replicationNum,
                               TabletMeta tabletMeta, Set<Long> tabletIdSet) throws DdlException {
        Preconditions.checkArgument(replicationNum > 0);

        DistributionInfo.DistributionInfoType distributionInfoType = distributionInfo.getType();
        if (distributionInfoType != DistributionInfo.DistributionInfoType.HASH) {
            throw new DdlException("Unknown distribution type: " + distributionInfoType);
        }

        if (tabletMeta.isUseStarOS()) {
            // TODO: support colocate table and drop shards when creating table failed
            int bucketNum = distributionInfo.getBucketNum();
            List<Long> shardIds = stateMgr.getStarOSAgent().createShards(bucketNum);
            Preconditions.checkState(bucketNum == shardIds.size());
            for (long shardId : shardIds) {
                Tablet tablet = new LakeTablet(getNextId(), shardId);
                index.addTablet(tablet, tabletMeta);
                tabletIdSet.add(tablet.getId());
            }
        } else {
            List<List<Long>> backendsPerBucketSeq = null;
            ColocateTableIndex.GroupId groupId = null;
            if (colocateTableIndex.isColocateTable(tabletMeta.getTableId())) {
                // if this is a colocate table, try to get backend seqs from colocation index.
                Database db = getDb(tabletMeta.getDbId());
                groupId = colocateTableIndex.getGroup(tabletMeta.getTableId());
                // Use db write lock here to make sure the backendsPerBucketSeq is consistent when the backendsPerBucketSeq is updating.
                // This lock will release very fast.
                db.writeLock();
                try {
                    backendsPerBucketSeq = colocateTableIndex.getBackendsPerBucketSeq(groupId);
                } finally {
                    db.writeUnlock();
                }
            }

            // chooseBackendsArbitrary is true, means this may be the first table of colocation group,
            // or this is just a normal table, and we can choose backends arbitrary.
            // otherwise, backends should be chosen from backendsPerBucketSeq;
            boolean chooseBackendsArbitrary = backendsPerBucketSeq == null || backendsPerBucketSeq.isEmpty();
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
                                chosenBackendIdBySeq(replicationNum, clusterName, tabletMeta.getStorageMedium());
                    } else {
                        chosenBackendIds = chosenBackendIdBySeq(replicationNum, clusterName);
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

            if (groupId != null && chooseBackendsArbitrary) {
                colocateTableIndex.addBackendsPerBucketSeq(groupId, backendsPerBucketSeq);
                ColocatePersistInfo info =
                        ColocatePersistInfo.createForBackendsPerBucketSeq(groupId, backendsPerBucketSeq);
                editLog.logColocateBackendsPerBucketSeq(info);
            }
        }
    }

    // create replicas for tablet with random chosen backends
    private List<Long> chosenBackendIdBySeq(int replicationNum, String clusterName, TStorageMedium storageMedium)
            throws DdlException {
        List<Long> chosenBackendIds = systemInfoService.seqChooseBackendIdsByStorageMedium(replicationNum,
                        true, true, clusterName, storageMedium);
        if (chosenBackendIds == null) {
            throw new DdlException(
                    "Failed to find enough host with storage medium is " + storageMedium + " in all backends. need: " +
                            replicationNum);
        }
        return chosenBackendIds;
    }

    private List<Long> chosenBackendIdBySeq(int replicationNum, String clusterName) throws DdlException {
        List<Long> chosenBackendIds =
                systemInfoService.seqChooseBackendIds(replicationNum, true, true, clusterName);
        if (chosenBackendIds == null) {
            List<Long> backendIds = systemInfoService.getBackendIds(true);
            throw new DdlException("Failed to find enough host in all backends. need: " + replicationNum +
                    ", Current alive backend is [" + Joiner.on(",").join(backendIds) + "]");
        }
        return chosenBackendIds;
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

        Table table;
        HashMap<Long, AgentBatchTask> batchTaskMap;
        db.writeLock();
        try {
            table = db.getTable(tableName);
            if (table == null) {
                if (stmt.isSetIfExists()) {
                    LOG.info("drop table[{}] which does not exist", tableName);
                    return;
                } else {
                    ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName);
                }
            }

            // Check if a view
            if (stmt.isView()) {
                if (!(table instanceof View)) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_WRONG_OBJECT, dbName, tableName, "VIEW");
                }
            } else {
                if (table instanceof View) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_WRONG_OBJECT, dbName, tableName, "TABLE");
                }
            }

            if (!stmt.isForceDrop()) {
                if (stateMgr.getGlobalTransactionMgr()
                        .existCommittedTxns(db.getId(), table.getId(), null)) {
                    throw new DdlException(
                            "There are still some transactions in the COMMITTED state waiting to be completed. " +
                                    "The table [" + tableName +
                                    "] cannot be dropped. If you want to forcibly drop(cannot be recovered)," +
                                    " please use \"DROP table FORCE\".");
                }
            }
            batchTaskMap = unprotectDropTable(db, table.getId(), stmt.isForceDrop(), false);
            DropInfo info = new DropInfo(db.getId(), table.getId(), -1L, stmt.isForceDrop());
            editLog.logDropTable(info);
        } finally {
            db.writeUnlock();
        }
        sendDropTabletTasks(batchTaskMap);
        LOG.info("finished dropping table: {} from db: {}, is force: {}", tableName, dbName, stmt.isForceDrop());
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
        db.writeLock();
        try {
            unprotectDropTable(db, tableId, isForceDrop, true);
        } finally {
            db.writeUnlock();
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

    private void unprotectAddReplica(ReplicaPersistInfo info) {
        LOG.debug("replay add a replica {}", info);
        Database db = getDbIncludeRecycleBin(info.getDbId());
        OlapTable olapTable = (OlapTable) getTableIncludeRecycleBin(db, info.getTableId());
        Partition partition = getPartitionIncludeRecycleBin(olapTable, info.getPartitionId());
        MaterializedIndex materializedIndex = partition.getIndex(info.getIndexId());
        LocalTablet tablet = (LocalTablet) materializedIndex.getTablet(info.getTabletId());

        // for compatibility
        int schemaHash = info.getSchemaHash();
        if (schemaHash == -1) {
            schemaHash = olapTable.getSchemaHashByIndexId(info.getIndexId());
        }

        Replica replica = new Replica(info.getReplicaId(), info.getBackendId(), info.getVersion(),
                schemaHash, info.getDataSize(), info.getRowCount(),
                Replica.ReplicaState.NORMAL,
                info.getLastFailedVersion(),
                info.getLastSuccessVersion());
        tablet.addReplica(replica);
    }

    private void unprotectUpdateReplica(ReplicaPersistInfo info) {
        LOG.debug("replay update a replica {}", info);
        Database db = getDbIncludeRecycleBin(info.getDbId());
        OlapTable olapTable = (OlapTable) getTableIncludeRecycleBin(db, info.getTableId());
        Partition partition = getPartitionIncludeRecycleBin(olapTable, info.getPartitionId());
        MaterializedIndex materializedIndex = partition.getIndex(info.getIndexId());
        LocalTablet tablet = (LocalTablet) materializedIndex.getTablet(info.getTabletId());
        Replica replica = tablet.getReplicaByBackendId(info.getBackendId());
        Preconditions.checkNotNull(replica, info);
        replica.updateRowCount(info.getVersion(), info.getDataSize(), info.getRowCount());
        replica.setBad(false);
    }

    public void replayAddReplica(ReplicaPersistInfo info) {
        Database db = getDbIncludeRecycleBin(info.getDbId());
        db.writeLock();
        try {
            unprotectAddReplica(info);
        } finally {
            db.writeUnlock();
        }
    }

    public void replayUpdateReplica(ReplicaPersistInfo info) {
        Database db = getDbIncludeRecycleBin(info.getDbId());
        db.writeLock();
        try {
            unprotectUpdateReplica(info);
        } finally {
            db.writeUnlock();
        }
    }

    public void unprotectDeleteReplica(ReplicaPersistInfo info) {
        Database db = getDbIncludeRecycleBin(info.getDbId());
        OlapTable olapTable = (OlapTable) getTableIncludeRecycleBin(db, info.getTableId());
        Partition partition = getPartitionIncludeRecycleBin(olapTable, info.getPartitionId());
        MaterializedIndex materializedIndex = partition.getIndex(info.getIndexId());
        LocalTablet tablet = (LocalTablet) materializedIndex.getTablet(info.getTabletId());
        tablet.deleteReplicaByBackendId(info.getBackendId());
    }

    public void replayDeleteReplica(ReplicaPersistInfo info) {
        Database db = getDbIncludeRecycleBin(info.getDbId());
        db.writeLock();
        try {
            unprotectDeleteReplica(info);
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
        if (fullNameToDb.containsKey(name)) {
            return fullNameToDb.get(name);
        } else {
            // This maybe a information_schema db request, and information_schema db name is case insensitive.
            // So, we first extract db name to check if it is information_schema.
            // Then we reassemble the origin cluster name with lower case db name,
            // and finally get information_schema db from the name map.
            String dbName = ClusterNamespace.getNameFromFullName(name);
            if (dbName.equalsIgnoreCase(InfoSchemaDb.DATABASE_NAME)) {
                String clusterName = ClusterNamespace.getClusterNameFromFullName(name);
                return fullNameToDb.get(ClusterNamespace.getFullName(clusterName, dbName.toLowerCase()));
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
    public List<String> listTableNames(String dbName) throws DdlException {
        Database database = getDb(dbName);
        if (database != null) {
            return database.getTables().stream()
                    .map(Table::getName).collect(Collectors.toList());
        } else {
            throw new DdlException("Database " + dbName + " doesn't exist");
        }
    }

    public List<String> getClusterDbNames(String clusterName) throws AnalysisException {
        final Cluster cluster = nameToCluster.get(clusterName);
        if (cluster == null) {
            throw new AnalysisException("No cluster selected");
        }
        return Lists.newArrayList(cluster.getDbNames());
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
                    if (table.getType() != Table.TableType.OLAP) {
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
                            editLog.logModifyPartition(info);
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
        stateMgr.getAlterInstance().processAlterTable(stmt);
    }

    /**
     * used for handling AlterViewStmt (the ALTER VIEW command).
     */
    @Override
    public void alterView(AlterViewStmt stmt) throws UserException {
        stateMgr.getAlterInstance().processAlterView(stmt, ConnectContext.get());
    }

    @Override
    public void createMaterializedView(CreateMaterializedViewStmt stmt)
            throws AnalysisException, DdlException {
        stateMgr.getAlterInstance().processCreateMaterializedView(stmt);
    }

    @Override
    public void createMaterializedView(CreateMaterializedViewStatement statement)
            throws DdlException {
        // check Mv exists,name must be different from view/mv/table which exists in metadata
        String mvName = statement.getTableName().getTbl();
        String dbName = statement.getTableName().getDb();
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
                if (statement.isIfNotExists()) {
                    LOG.info("Create materialized view [{}] which already exists", mvName);
                    return;
                } else {
                    ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, mvName);
                }
            }
        } finally {
            db.readUnlock();
        }
    }

    @Override
    public void dropMaterializedView(DropMaterializedViewStmt stmt) throws DdlException, MetaNotFoundException {
        stateMgr.getAlterInstance().processDropMaterializedView(stmt);
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

        TableInfo tableInfo = TableInfo.createForTableRename(db.getId(), olapTable.getId(), newTableName);
        editLog.logTableRename(tableInfo);
        LOG.info("rename table[{}] to {}, tableId: {}", oldTableName, newTableName, olapTable.getId());
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

        if (olapTable.getPartitionInfo().getType() != PartitionType.RANGE) {
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
        editLog.logPartitionRename(tableInfo);
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
        editLog.logRollupRename(tableInfo);
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
        editLog.logDynamicPartition(info);
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
        if (partitionInfo.getType() == PartitionType.RANGE) {
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
        editLog.logModifyPartition(info);
        LOG.info("modify partition[{}-{}-{}] replication num to {}", db.getFullName(), table.getName(),
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
        editLog.logModifyReplicationNum(info);
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
        editLog.logModifyEnablePersistentIndex(info);

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
        editLog.logModifyInMemory(info);
    }

    public void modifyTableMeta(Database db, OlapTable table, Map<String, String> properties,
                                TTabletMetaType metaType) {
        if (metaType == TTabletMetaType.INMEMORY) {
            modifyTableInMemoryMeta(db, table, properties);
        } else if (metaType == TTabletMetaType.ENABLE_PERSISTENT_INDEX) {
            modifyTableEnablePersistentIndexMeta(db, table, properties);
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
                editLog.logSetHasForbitGlobalDict(info);
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
        db.readLock();
        try {
            Table tbl = db.getTable(hiveExternalTable);
            table = (HiveTable) tbl;
            table.setNewFullSchema(columns);
        } finally {
            db.readUnlock();
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
                }
            }
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
        db.readLock();
        try {
            if (db.getTable(tableName) != null) {
                if (stmt.isSetIfNotExists()) {
                    LOG.info("create view[{}] which already exists", tableName);
                    return;
                } else {
                    ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
                }
            }
        } finally {
            db.readUnlock();
        }

        List<Column> columns = stmt.getColumns();

        long tableId = getNextId();
        View newView = new View(tableId, tableName, columns);
        newView.setComment(stmt.getComment());
        newView.setInlineViewDefWithSqlMode(stmt.getInlineViewDef(),
                ConnectContext.get().getSessionVariable().getSqlMode());
        // init here in case the stmt string from view.toSql() has some syntax error.
        try {
            newView.init();
        } catch (UserException e) {
            throw new DdlException("failed to init view stmt", e);
        }

        // check database exists again, because database can be dropped when creating table
        if (!tryLock(false)) {
            throw new DdlException("Failed to acquire globalStateMgr lock. Try again");
        }
        try {
            if (getDb(db.getId()) == null) {
                throw new DdlException("database has been dropped when creating view");
            }
            if (!db.createTableWithLock(newView, false)) {
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

        LOG.info("successfully create view[" + tableName + "-" + newView.getId() + "]");
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
        for (Long id : cluster.getBackendIdList()) {
            final Backend backend = stateMgr.getClusterInfo().getBackend(id);
            backend.setOwnerClusterName(cluster.getName());
            backend.setBackendState(Backend.BackendState.using);
        }

        idToCluster.put(cluster.getId(), cluster);
        nameToCluster.put(cluster.getName(), cluster);

        // create info schema db
        final InfoSchemaDb infoDb = new InfoSchemaDb(cluster.getName());
        infoDb.setClusterName(cluster.getName());
        unprotectCreateDb(infoDb);

        // only need to create default cluster once.
        if (cluster.getName().equalsIgnoreCase(SystemInfoService.DEFAULT_CLUSTER)) {
            stateMgr.setIsDefaultClusterCreated(true);
        }
    }

    /**
     * @param ctx
     * @param clusterName
     * @throws DdlException
     */
    public void changeCluster(ConnectContext ctx, String clusterName) throws DdlException {
        if (!stateMgr.getAuth().checkCanEnterCluster(ConnectContext.get(), clusterName)) {
            ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_NO_AUTHORITY,
                    ConnectContext.get().getQualifiedUser(), "enter");
        }

        if (!nameToCluster.containsKey(clusterName)) {
            ErrorReport.reportDdlException(ErrorCode.ERR_CLUSTER_NO_EXISTS, clusterName);
        }

        ctx.setCluster(clusterName);
    }

    public Cluster getCluster(String clusterName) {
        return nameToCluster.get(clusterName);
    }

    public List<String> getClusterNames() {
        return new ArrayList<String>(nameToCluster.keySet());
    }

    /**
     * get migrate progress , when finish migration, next clonecheck will reset dbState
     *
     * @return
     */
    public Set<BaseParam> getMigrations() {
        final Set<BaseParam> infos = Sets.newHashSet();
        for (Database db : fullNameToDb.values()) {
            db.readLock();
            try {
                if (db.getDbState() == Database.DbState.MOVE) {
                    int tabletTotal = 0;
                    int tabletQuorum = 0;
                    final Set<Long> beIds = Sets.newHashSet(stateMgr.getClusterInfo()
                            .getClusterBackendIds(db.getClusterName()));
                    final Set<String> tableNames = db.getTableNamesWithLock();
                    for (String tableName : tableNames) {

                        Table table = db.getTable(tableName);
                        if (table == null || table.getType() != Table.TableType.OLAP) {
                            continue;
                        }

                        OlapTable olapTable = (OlapTable) table;
                        for (Partition partition : olapTable.getPartitions()) {
                            final short replicationNum = olapTable.getPartitionInfo()
                                    .getReplicationNum(partition.getId());
                            for (MaterializedIndex materializedIndex : partition
                                    .getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                                if (materializedIndex.getState() != MaterializedIndex.IndexState.NORMAL) {
                                    continue;
                                }
                                for (Tablet tablet : materializedIndex.getTablets()) {
                                    int replicaNum = 0;
                                    int quorum = replicationNum / 2 + 1;
                                    for (Replica replica : ((LocalTablet) tablet).getReplicas()) {
                                        if (replica.getState() != Replica.ReplicaState.CLONE
                                                && beIds.contains(replica.getBackendId())) {
                                            replicaNum++;
                                        }
                                    }
                                    if (replicaNum > quorum) {
                                        replicaNum = quorum;
                                    }

                                    tabletQuorum = tabletQuorum + replicaNum;
                                    tabletTotal = tabletTotal + quorum;
                                }
                            }
                        }
                    }
                    final BaseParam info = new BaseParam();
                    info.addStringParam(db.getClusterName());
                    info.addStringParam(db.getAttachDb());
                    info.addStringParam(db.getFullName());
                    final float percentage = tabletTotal > 0 ? (float) tabletQuorum / (float) tabletTotal : 0f;
                    info.addFloatParam(percentage);
                    infos.add(info);
                }
            } finally {
                db.readUnlock();
            }
        }

        return infos;
    }

    public long loadCluster(DataInputStream dis, long checksum) throws IOException {
        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_30) {
            int clusterCount = dis.readInt();
            checksum ^= clusterCount;
            for (long i = 0; i < clusterCount; ++i) {
                final Cluster cluster = Cluster.read(dis);
                checksum ^= cluster.getId();

                List<Long> latestBackendIds = stateMgr.getClusterInfo().getClusterBackendIds(cluster.getName());
                if (latestBackendIds.size() != cluster.getBackendIdList().size()) {
                    LOG.warn("Cluster:" + cluster.getName() + ", backends in Cluster is "
                            + cluster.getBackendIdList().size() + ", backends in SystemInfoService is "
                            + cluster.getBackendIdList().size());
                }
                // The number of BE in cluster is not same as in SystemInfoService, when perform 'ALTER
                // SYSTEM ADD BACKEND TO ...' or 'ALTER SYSTEM ADD BACKEND ...', because both of them are
                // for adding BE to some Cluster, but loadCluster is after loadBackend.
                cluster.setBackendIdList(latestBackendIds);

                String dbName = InfoSchemaDb.getFullInfoSchemaDbName(cluster.getName());
                InfoSchemaDb db;
                // Use real GlobalStateMgr instance to avoid InfoSchemaDb id continuously increment
                // when checkpoint thread load image.
                if (getFullNameToDb().containsKey(dbName)) {
                    db = (InfoSchemaDb) GlobalStateMgr.getCurrentState().getFullNameToDb().get(dbName);
                } else {
                    db = new InfoSchemaDb(cluster.getName());
                    db.setClusterName(cluster.getName());
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
                idToCluster.put(cluster.getId(), cluster);
                nameToCluster.put(cluster.getName(), cluster);
            }
        }
        LOG.info("finished replay cluster from image");
        return checksum;
    }

    public void initDefaultCluster() {
        final List<Long> backendList = Lists.newArrayList();
        final List<Backend> defaultClusterBackends = systemInfoService.getClusterBackends(SystemInfoService.DEFAULT_CLUSTER);
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
            db.setClusterName(SystemInfoService.DEFAULT_CLUSTER);
            cluster.addDb(db.getFullName(), db.getId());
        }

        // no matter default_cluster is created or not,
        // mark isDefaultClusterCreated as true
        stateMgr.setIsDefaultClusterCreated(true);
        editLog.logCreateCluster(cluster);
    }

    public void replayUpdateDb(DatabaseInfo info) {
        final Database db = fullNameToDb.get(info.getDbName());
        db.setClusterName(info.getClusterName());
        db.setDbState(info.getDbState());
    }

    public long saveCluster(DataOutputStream dos, long checksum) throws IOException {
        final int clusterCount = idToCluster.size();
        checksum ^= clusterCount;
        dos.writeInt(clusterCount);
        for (Map.Entry<Long, Cluster> entry : idToCluster.entrySet()) {
            long clusterId = entry.getKey();
            if (clusterId >= NEXT_ID_INIT_VALUE) {
                checksum ^= clusterId;
                final Cluster cluster = entry.getValue();
                cluster.write(dos);
            }
        }
        return checksum;
    }

    public void replayUpdateClusterAndBackends(BackendIdsUpdateInfo info) {
        for (long id : info.getBackendList()) {
            final Backend backend = stateMgr.getClusterInfo().getBackend(id);
            final Cluster cluster = nameToCluster.get(backend.getOwnerClusterName());
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

            if (table.getType() != Table.TableType.OLAP) {
                throw new DdlException("Only support truncate OLAP table");
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

                copiedTbl.setDefaultDistributionInfo(entry.getValue().getDistributionInfo());

                Partition newPartition =
                        createPartition(db, copiedTbl, newPartitionId, newPartitionName, null, tabletIdSet);
                newPartitions.add(newPartition);
            }
            buildPartitions(db, copiedTbl, newPartitions);
        } catch (DdlException e) {
            // create partition failed, remove all newly created tablets
            for (Long tabletId : tabletIdSet) {
                GlobalStateMgr.getCurrentInvertedIndex().deleteTablet(tabletId);
            }
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

            // write edit log
            TruncateTableInfo info = new TruncateTableInfo(db.getId(), olapTable.getId(), newPartitions,
                    truncateEntireTable);
            editLog.logTruncateTable(info);
        } finally {
            db.writeUnlock();
        }

        LOG.info("finished to truncate table {}, partitions: {}",
                tblRef.getName().toSql(), tblRef.getPartitionNames());
    }

    private void truncateTableInternal(OlapTable olapTable, List<Partition> newPartitions,
                                       boolean isEntireTable, boolean isReplay) {
        // use new partitions to replace the old ones.
        Map<Long, Set<Long>> oldTabletIds = Maps.newHashMap();
        for (Partition newPartition : newPartitions) {
            Partition oldPartition = olapTable.replacePartition(newPartition);
            // save old tablets to be removed
            for (MaterializedIndex index : oldPartition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                for (Tablet tablet : index.getTablets()) {
                    if (!oldTabletIds.containsKey(tablet.getId())) {
                        oldTabletIds.put(tablet.getId(), tablet.getBackendIds());
                    }
                }
            }
        }

        if (isEntireTable) {
            // drop all temp partitions
            olapTable.dropAllTempPartitions();
        }

        // remove the tablets in old partitions
        for (Long tabletId : oldTabletIds.keySet()) {
            GlobalStateMgr.getCurrentInvertedIndex().deleteTablet(tabletId);
            // Ensure that only the leader records truncate information.
            // TODO(yangzaorang): the information will be lost when failover occurs. The probability of this case
            // happening is small, and the trash data will be deleted by BE anyway, but we need to find a better
            // solution.
            if (!isReplay) {
                GlobalStateMgr.getCurrentInvertedIndex().markTabletForceDelete(tabletId, oldTabletIds.get(tabletId));
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
                                partitionId, indexId, schemaHash, medium);
                        for (Tablet tablet : mIndex.getTablets()) {
                            long tabletId = tablet.getId();
                            invertedIndex.addTablet(tabletId, tabletMeta);
                            for (Replica replica : ((LocalTablet) tablet).getReplicas()) {
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
            if (!tbl.convertRandomDistributionToHashDistribution()) {
                throw new DdlException("Table " + tbl.getName() + " is not random distributed");
            }
            TableInfo tableInfo = TableInfo.createForModifyDistribution(db.getId(), tbl.getId());
            editLog.logModifyDistributionType(tableInfo);
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
            tbl.convertRandomDistributionToHashDistribution();
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

            if (table.getType() != Table.TableType.OLAP) {
                throw new DdlException("Table[" + tableName + "] is not OLAP table");
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
            editLog.logReplaceTempPartition(info);
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

    private void setReplicaStatusInternal(long tabletId, long backendId, Replica.ReplicaStatus status, boolean isReplay) {
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
                        editLog.logSetReplicaStatus(log);
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
        // remove jobs
        stateMgr.getRollupHandler().removeDbAlterJob(dbId);
        stateMgr.getSchemaChangeHandler().removeDbAlterJob(dbId);

        // remove database transaction manager
        stateMgr.getGlobalTransactionMgr().removeDatabaseTransactionMgr(dbId);
    }

    public HashMap<Long, AgentBatchTask> onEraseOlapTable(OlapTable olapTable, boolean isReplay) {
        // inverted index
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentInvertedIndex();
        Collection<Partition> allPartitions = olapTable.getAllPartitions();
        for (Partition partition : allPartitions) {
            for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                for (Tablet tablet : index.getTablets()) {
                    invertedIndex.deleteTablet(tablet.getId());
                }
            }
        }

        HashMap<Long, AgentBatchTask> batchTaskMap = new HashMap<>();
        if (!isReplay) {
            // drop all replicas
            for (Partition partition : olapTable.getAllPartitions()) {
                List<MaterializedIndex> allIndices = partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL);
                for (MaterializedIndex materializedIndex : allIndices) {
                    long indexId = materializedIndex.getId();
                    int schemaHash = olapTable.getSchemaHashByIndexId(indexId);
                    for (Tablet tablet : materializedIndex.getTablets()) {
                        long tabletId = tablet.getId();
                        if (partition.isUseStarOS()) {
                            long backendId = ((LakeTablet) tablet).getPrimaryBackendId();
                            DropReplicaTask dropTask = new DropReplicaTask(backendId, tabletId, schemaHash, true);
                            AgentBatchTask batchTask = batchTaskMap.get(backendId);
                            if (batchTask == null) {
                                batchTask = new AgentBatchTask();
                                batchTaskMap.put(backendId, batchTask);
                            }
                            batchTask.addTask(dropTask);
                        } else {
                            List<Replica> replicas = ((LocalTablet) tablet).getReplicas();
                            for (Replica replica : replicas) {
                                long backendId = replica.getBackendId();
                                DropReplicaTask dropTask = new DropReplicaTask(backendId, tabletId, schemaHash, true);
                                AgentBatchTask batchTask = batchTaskMap.get(backendId);
                                if (batchTask == null) {
                                    batchTask = new AgentBatchTask();
                                    batchTaskMap.put(backendId, batchTask);
                                }
                                batchTask.addTask(dropTask);
                            } // end for replicas
                        }
                    } // end for tablets
                } // end for indices
            } // end for partitions
        }
        // colocation
        colocateTableIndex.removeTable(olapTable.getId());
        return batchTaskMap;
    }

    public void onErasePartition(Partition partition) {
        // remove tablet in inverted index
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentInvertedIndex();
        for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
            for (Tablet tablet : index.getTablets()) {
                invertedIndex.deleteTablet(tablet.getId());
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

        stateMgr.getRollupHandler().unprotectedGetAlterJobs().clear();
        stateMgr.getSchemaChangeHandler().unprotectedGetAlterJobs().clear();
        System.gc();
    }
}
