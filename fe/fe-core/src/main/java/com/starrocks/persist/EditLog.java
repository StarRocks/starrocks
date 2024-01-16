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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/persist/EditLog.java

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

package com.starrocks.persist;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.alter.AlterJobV2;
import com.starrocks.alter.BatchAlterJobPersistInfo;
import com.starrocks.authentication.UserAuthenticationInfo;
import com.starrocks.authentication.UserProperty;
import com.starrocks.authentication.UserPropertyInfo;
import com.starrocks.backup.BackupJob;
import com.starrocks.backup.Repository;
import com.starrocks.backup.RestoreJob;
import com.starrocks.catalog.BrokerMgr;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSearchDesc;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MetaVersion;
import com.starrocks.catalog.Resource;
import com.starrocks.cluster.Cluster;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.io.DataOutputBuffer;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.SmallFileMgr.SmallFile;
import com.starrocks.ha.LeaderInfo;
import com.starrocks.journal.JournalEntity;
import com.starrocks.journal.JournalInconsistentException;
import com.starrocks.journal.JournalTask;
import com.starrocks.journal.bdbje.Timestamp;
import com.starrocks.load.DeleteInfo;
import com.starrocks.load.DeleteMgr;
import com.starrocks.load.ExportFailMsg;
import com.starrocks.load.ExportJob;
import com.starrocks.load.ExportMgr;
import com.starrocks.load.LoadErrorHub;
import com.starrocks.load.MultiDeleteInfo;
import com.starrocks.load.loadv2.LoadJob.LoadJobStateUpdateInfo;
import com.starrocks.load.loadv2.LoadJobFinalOperation;
import com.starrocks.load.routineload.RoutineLoadJob;
import com.starrocks.load.streamload.StreamLoadTask;
import com.starrocks.meta.MetaContext;
import com.starrocks.metric.MetricRepo;
import com.starrocks.plugin.PluginInfo;
import com.starrocks.privilege.RolePrivilegeCollectionV2;
import com.starrocks.privilege.UserPrivilegeCollectionV2;
import com.starrocks.qe.SessionVariable;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.mv.MVEpoch;
import com.starrocks.scheduler.mv.MVMaintenanceJob;
import com.starrocks.scheduler.mv.MaterializedViewMgr;
import com.starrocks.scheduler.persist.DropTaskRunsLog;
import com.starrocks.scheduler.persist.DropTasksLog;
import com.starrocks.scheduler.persist.TaskRunPeriodStatusChange;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.scheduler.persist.TaskRunStatusChange;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.staros.StarMgrJournal;
import com.starrocks.staros.StarMgrServer;
import com.starrocks.statistic.AnalyzeJob;
import com.starrocks.statistic.AnalyzeStatus;
import com.starrocks.statistic.BasicStatsMeta;
import com.starrocks.statistic.HistogramStatsMeta;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.Frontend;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.transaction.TransactionState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * EditLog maintains a log of the memory modifications.
 * Current we support only file editLog.
 */
public class EditLog {
    public static final Logger LOG = LogManager.getLogger(EditLog.class);
    private static final int OUTPUT_BUFFER_INIT_SIZE = 128;

    private final BlockingQueue<JournalTask> journalQueue;

    public EditLog(BlockingQueue<JournalTask> journalQueue) {
        this.journalQueue = journalQueue;
    }

    public static void loadJournal(GlobalStateMgr globalStateMgr, JournalEntity journal)
            throws JournalInconsistentException {
        short opCode = journal.getOpCode();
        if (opCode != OperationType.OP_SAVE_NEXTID
                && opCode != OperationType.OP_TIMESTAMP
                && opCode != OperationType.OP_TIMESTAMP_V2) {
            LOG.debug("replay journal op code: {}", opCode);
        }
        try {
            switch (opCode) {
                case OperationType.OP_SAVE_NEXTID: {
                    String idString = journal.getData().toString();
                    long id = Long.parseLong(idString);
                    globalStateMgr.setNextId(id + 1);
                    break;
                }
                case OperationType.OP_SAVE_TRANSACTION_ID: {
                    String idString = journal.getData().toString();
                    long id = Long.parseLong(idString);
                    GlobalStateMgr.getCurrentGlobalTransactionMgr().getTransactionIDGenerator()
                            .initTransactionId(id + 1);
                    break;
                }
                case OperationType.OP_SAVE_TRANSACTION_ID_V2: {
                    TransactionIdInfo idInfo = (TransactionIdInfo) journal.getData();
                    GlobalStateMgr.getCurrentGlobalTransactionMgr().getTransactionIDGenerator()
                            .initTransactionId(idInfo.getTxnId() + 1);
                    break;
                }
                case OperationType.OP_SAVE_AUTO_INCREMENT_ID:
                case OperationType.OP_DELETE_AUTO_INCREMENT_ID: {
                    AutoIncrementInfo info = (AutoIncrementInfo) journal.getData();
                    LocalMetastore metastore = globalStateMgr.getLocalMetastore();
                    if (opCode == OperationType.OP_SAVE_AUTO_INCREMENT_ID) {
                        metastore.replayAutoIncrementId(info);
                    } else if (opCode == OperationType.OP_DELETE_AUTO_INCREMENT_ID) {
                        metastore.replayDeleteAutoIncrementId(info);
                    }
                    break;
                }
                case OperationType.OP_CREATE_DB: {
                    Database db = (Database) journal.getData();
                    LocalMetastore metastore = (LocalMetastore) globalStateMgr.getMetadata();
                    metastore.replayCreateDb(db);
                    break;
                }
                case OperationType.OP_CREATE_DB_V2: {
                    CreateDbInfo db = (CreateDbInfo) journal.getData();
                    LocalMetastore metastore = (LocalMetastore) globalStateMgr.getMetadata();
                    metastore.replayCreateDb(db);
                    break;
                }
                case OperationType.OP_DROP_DB: {
                    DropDbInfo dropDbInfo = (DropDbInfo) journal.getData();
                    LocalMetastore metastore = (LocalMetastore) globalStateMgr.getMetadata();
                    metastore.replayDropDb(dropDbInfo.getDbName(), dropDbInfo.isForceDrop());
                    break;
                }
                case OperationType.OP_ALTER_DB:
                case OperationType.OP_ALTER_DB_V2: {
                    DatabaseInfo dbInfo = (DatabaseInfo) journal.getData();
                    String dbName = dbInfo.getDbName();
                    LOG.info("Begin to unprotect alter db info {}", dbName);
                    globalStateMgr.replayAlterDatabaseQuota(dbName, dbInfo.getQuota(), dbInfo.getQuotaType());
                    break;
                }
                case OperationType.OP_ERASE_DB: {
                    Text dbId = (Text) journal.getData();
                    globalStateMgr.replayEraseDatabase(Long.parseLong(dbId.toString()));
                    break;
                }
                case OperationType.OP_RECOVER_DB:
                case OperationType.OP_RECOVER_DB_V2: {
                    RecoverInfo info = (RecoverInfo) journal.getData();
                    globalStateMgr.replayRecoverDatabase(info);
                    break;
                }
                case OperationType.OP_RENAME_DB:
                case OperationType.OP_RENAME_DB_V2: {
                    DatabaseInfo dbInfo = (DatabaseInfo) journal.getData();
                    String dbName = dbInfo.getDbName();
                    LOG.info("Begin to unprotect rename db {}", dbName);
                    globalStateMgr.replayRenameDatabase(dbName, dbInfo.getNewDbName());
                    break;
                }
                case OperationType.OP_CREATE_TABLE:
                case OperationType.OP_CREATE_TABLE_V2: {
                    CreateTableInfo info = (CreateTableInfo) journal.getData();
                    LOG.info("Begin to unprotect create table. db = "
                            + info.getDbName() + " table = " + info.getTable().getId());
                    globalStateMgr.replayCreateTable(info.getDbName(), info.getTable());
                    break;
                }
                case OperationType.OP_DROP_TABLE:
                case OperationType.OP_DROP_TABLE_V2: {
                    DropInfo info = (DropInfo) journal.getData();
                    Database db = globalStateMgr.getDb(info.getDbId());
                    if (db == null) {
                        LOG.warn("failed to get db[{}]", info.getDbId());
                        break;
                    }
                    LOG.info("Begin to unprotect drop table. db = "
                            + db.getOriginName() + " table = " + info.getTableId());
                    globalStateMgr.replayDropTable(db, info.getTableId(), info.isForceDrop());
                    break;
                }
                case OperationType.OP_CREATE_MATERIALIZED_VIEW:
                case OperationType.OP_CREATE_MATERIALIZED_VIEW_V2: {
                    CreateTableInfo info = (CreateTableInfo) journal.getData();
                    LOG.info("Begin to unprotect create materialized view. db = " + info.getDbName()
                            + " create materialized view = " + info.getTable().getId()
                            + " tableName = " + info.getTable().getName());
                    globalStateMgr.replayCreateMaterializedView(info.getDbName(), ((MaterializedView) info.getTable()));
                    break;
                }
                case OperationType.OP_ADD_PARTITION_V2: {
                    PartitionPersistInfoV2 info = (PartitionPersistInfoV2) journal.getData();
                    LOG.info("Begin to unprotect add partition. db = " + info.getDbId()
                            + " table = " + info.getTableId()
                            + " partitionName = " + info.getPartition().getName());
                    globalStateMgr.replayAddPartition(info);
                    break;
                }
                case OperationType.OP_ADD_PARTITION: {
                    PartitionPersistInfo info = (PartitionPersistInfo) journal.getData();
                    LOG.info("Begin to unprotect add partition. db = " + info.getDbId()
                            + " table = " + info.getTableId()
                            + " partitionName = " + info.getPartition().getName());
                    globalStateMgr.replayAddPartition(info);
                    break;
                }
                case OperationType.OP_ADD_PARTITIONS: {
                    AddPartitionsInfo infos = (AddPartitionsInfo) journal.getData();
                    for (PartitionPersistInfo info : infos.getAddPartitionInfos()) {
                        globalStateMgr.replayAddPartition(info);
                    }
                    break;
                }
                case OperationType.OP_ADD_PARTITIONS_V2: {
                    AddPartitionsInfoV2 infos = (AddPartitionsInfoV2) journal.getData();
                    for (PartitionPersistInfoV2 info : infos.getAddPartitionInfos()) {
                        globalStateMgr.replayAddPartition(info);
                    }
                    break;
                }
                case OperationType.OP_DROP_PARTITION: {
                    DropPartitionInfo info = (DropPartitionInfo) journal.getData();
                    LOG.info("Begin to unprotect drop partition. db = " + info.getDbId()
                            + " table = " + info.getTableId()
                            + " partitionName = " + info.getPartitionName());
                    globalStateMgr.replayDropPartition(info);
                    break;
                }
                case OperationType.OP_MODIFY_PARTITION:
                case OperationType.OP_MODIFY_PARTITION_V2: {
                    ModifyPartitionInfo info = (ModifyPartitionInfo) journal.getData();
                    LOG.info("Begin to unprotect modify partition. db = " + info.getDbId()
                            + " table = " + info.getTableId() + " partitionId = " + info.getPartitionId());
                    globalStateMgr.getAlterJobMgr().replayModifyPartition(info);
                    break;
                }
                case OperationType.OP_BATCH_MODIFY_PARTITION: {
                    BatchModifyPartitionsInfo info = (BatchModifyPartitionsInfo) journal.getData();
                    for (ModifyPartitionInfo modifyPartitionInfo : info.getModifyPartitionInfos()) {
                        globalStateMgr.getAlterJobMgr().replayModifyPartition(modifyPartitionInfo);
                    }
                    break;
                }
                case OperationType.OP_ERASE_TABLE: {
                    Text tableId = (Text) journal.getData();
                    globalStateMgr.replayEraseTable(Long.parseLong(tableId.toString()));
                    break;
                }
                case OperationType.OP_ERASE_MULTI_TABLES: {
                    MultiEraseTableInfo multiEraseTableInfo = (MultiEraseTableInfo) journal.getData();
                    globalStateMgr.replayEraseMultiTables(multiEraseTableInfo);
                    break;
                }
                case OperationType.OP_ERASE_PARTITION: {
                    Text partitionId = (Text) journal.getData();
                    globalStateMgr.replayErasePartition(Long.parseLong(partitionId.toString()));
                    break;
                }
                case OperationType.OP_RECOVER_TABLE:
                case OperationType.OP_RECOVER_TABLE_V2: {
                    RecoverInfo info = (RecoverInfo) journal.getData();
                    globalStateMgr.replayRecoverTable(info);
                    break;
                }
                case OperationType.OP_RECOVER_PARTITION:
                case OperationType.OP_RECOVER_PARTITION_V2: {
                    RecoverInfo info = (RecoverInfo) journal.getData();
                    globalStateMgr.replayRecoverPartition(info);
                    break;
                }
                case OperationType.OP_RENAME_TABLE:
                case OperationType.OP_RENAME_TABLE_V2: {
                    TableInfo info = (TableInfo) journal.getData();
                    globalStateMgr.replayRenameTable(info);
                    break;
                }
                case OperationType.OP_CHANGE_MATERIALIZED_VIEW_REFRESH_SCHEME: {
                    ChangeMaterializedViewRefreshSchemeLog log =
                            (ChangeMaterializedViewRefreshSchemeLog) journal.getData();
                    globalStateMgr.replayChangeMaterializedViewRefreshScheme(log);
                    break;
                }
                case OperationType.OP_ALTER_MATERIALIZED_VIEW_PROPERTIES: {
                    ModifyTablePropertyOperationLog log =
                            (ModifyTablePropertyOperationLog) journal.getData();
                    globalStateMgr.replayAlterMaterializedViewProperties(opCode, log);
                    break;
                }
                case OperationType.OP_ALTER_MATERIALIZED_VIEW_STATUS: {
                    AlterMaterializedViewStatusLog log =
                            (AlterMaterializedViewStatusLog) journal.getData();
                    globalStateMgr.replayAlterMaterializedViewStatus(log);
                    break;
                }
                case OperationType.OP_RENAME_MATERIALIZED_VIEW: {
                    RenameMaterializedViewLog log = (RenameMaterializedViewLog) journal.getData();
                    globalStateMgr.replayRenameMaterializedView(log);
                    break;
                }
                case OperationType.OP_MODIFY_VIEW_DEF: {
                    AlterViewInfo info = (AlterViewInfo) journal.getData();
                    globalStateMgr.getAlterJobMgr().replayModifyViewDef(info);
                    break;
                }
                case OperationType.OP_RENAME_PARTITION:
                case OperationType.OP_RENAME_PARTITION_V2: {
                    TableInfo info = (TableInfo) journal.getData();
                    globalStateMgr.replayRenamePartition(info);
                    break;
                }
                case OperationType.OP_BACKUP_JOB:
                case OperationType.OP_BACKUP_JOB_V2: {
                    BackupJob job = (BackupJob) journal.getData();
                    globalStateMgr.getBackupHandler().replayAddJob(job);
                    break;
                }
                case OperationType.OP_RESTORE_JOB:
                case OperationType.OP_RESTORE_JOB_V2: {
                    RestoreJob job = (RestoreJob) journal.getData();
                    job.setGlobalStateMgr(globalStateMgr);
                    globalStateMgr.getBackupHandler().replayAddJob(job);
                    break;
                }
                case OperationType.OP_DROP_ROLLUP:
                case OperationType.OP_DROP_ROLLUP_V2: {
                    DropInfo info = (DropInfo) journal.getData();
                    globalStateMgr.getRollupHandler().replayDropRollup(info, globalStateMgr);
                    break;
                }
                case OperationType.OP_BATCH_DROP_ROLLUP: {
                    BatchDropInfo batchDropInfo = (BatchDropInfo) journal.getData();
                    for (long indexId : batchDropInfo.getIndexIdSet()) {
                        globalStateMgr.getRollupHandler().replayDropRollup(
                                new DropInfo(batchDropInfo.getDbId(), batchDropInfo.getTableId(), indexId, false),
                                globalStateMgr);
                    }
                    break;
                }
                case OperationType.OP_FINISH_CONSISTENCY_CHECK:
                case OperationType.OP_FINISH_CONSISTENCY_CHECK_V2: {
                    ConsistencyCheckInfo info = (ConsistencyCheckInfo) journal.getData();
                    globalStateMgr.getConsistencyChecker().replayFinishConsistencyCheck(info, globalStateMgr);
                    break;
                }
                case OperationType.OP_CLEAR_ROLLUP_INFO: {
                    // Nothing to do
                    break;
                }
                case OperationType.OP_RENAME_ROLLUP:
                case OperationType.OP_RENAME_ROLLUP_V2: {
                    TableInfo info = (TableInfo) journal.getData();
                    globalStateMgr.replayRenameRollup(info);
                    break;
                }
                case OperationType.OP_EXPORT_CREATE:
                case OperationType.OP_EXPORT_CREATE_V2: {
                    ExportJob job = (ExportJob) journal.getData();
                    ExportMgr exportMgr = globalStateMgr.getExportMgr();
                    exportMgr.replayCreateExportJob(job);
                    break;
                }
                case OperationType.OP_EXPORT_UPDATE_STATE:
                    ExportJob.StateTransfer op = (ExportJob.StateTransfer) journal.getData();
                    ExportMgr exportMgr = globalStateMgr.getExportMgr();
                    exportMgr.replayUpdateJobState(op.getJobId(), op.getState());
                    break;
                case OperationType.OP_EXPORT_UPDATE_INFO_V2:
                case OperationType.OP_EXPORT_UPDATE_INFO:
                    ExportJob.ExportUpdateInfo exportUpdateInfo = (ExportJob.ExportUpdateInfo) journal.getData();
                    globalStateMgr.getExportMgr().replayUpdateJobInfo(exportUpdateInfo);
                    break;
                case OperationType.OP_FINISH_DELETE: {
                    DeleteInfo info = (DeleteInfo) journal.getData();
                    DeleteMgr deleteHandler = globalStateMgr.getDeleteMgr();
                    deleteHandler.replayDelete(info, globalStateMgr);
                    break;
                }
                case OperationType.OP_FINISH_MULTI_DELETE: {
                    MultiDeleteInfo info = (MultiDeleteInfo) journal.getData();
                    DeleteMgr deleteHandler = globalStateMgr.getDeleteMgr();
                    deleteHandler.replayMultiDelete(info, globalStateMgr);
                    break;
                }
                case OperationType.OP_ADD_REPLICA:
                case OperationType.OP_ADD_REPLICA_V2: {
                    ReplicaPersistInfo info = (ReplicaPersistInfo) journal.getData();
                    globalStateMgr.replayAddReplica(info);
                    break;
                }
                case OperationType.OP_UPDATE_REPLICA:
                case OperationType.OP_UPDATE_REPLICA_V2: {
                    ReplicaPersistInfo info = (ReplicaPersistInfo) journal.getData();
                    globalStateMgr.replayUpdateReplica(info);
                    break;
                }
                case OperationType.OP_DELETE_REPLICA:
                case OperationType.OP_DELETE_REPLICA_V2: {
                    ReplicaPersistInfo info = (ReplicaPersistInfo) journal.getData();
                    globalStateMgr.replayDeleteReplica(info);
                    break;
                }
                case OperationType.OP_ADD_COMPUTE_NODE: {
                    ComputeNode computeNode = (ComputeNode) journal.getData();
                    GlobalStateMgr.getCurrentSystemInfo().replayAddComputeNode(computeNode);
                    break;
                }
                case OperationType.OP_DROP_COMPUTE_NODE: {
                    DropComputeNodeLog dropComputeNodeLog = (DropComputeNodeLog) journal.getData();
                    GlobalStateMgr.getCurrentSystemInfo().replayDropComputeNode(dropComputeNodeLog.getComputeNodeId());
                    break;
                }
                case OperationType.OP_ADD_BACKEND:
                case OperationType.OP_ADD_BACKEND_V2: {
                    Backend be = (Backend) journal.getData();
                    GlobalStateMgr.getCurrentSystemInfo().replayAddBackend(be);
                    break;
                }
                case OperationType.OP_DROP_BACKEND:
                case OperationType.OP_DROP_BACKEND_V2: {
                    Backend be = (Backend) journal.getData();
                    GlobalStateMgr.getCurrentSystemInfo().replayDropBackend(be);
                    break;
                }
                case OperationType.OP_BACKEND_STATE_CHANGE:
                case OperationType.OP_BACKEND_STATE_CHANGE_V2: {
                    Backend be = (Backend) journal.getData();
                    GlobalStateMgr.getCurrentSystemInfo().updateBackendState(be);
                    break;
                }
                case OperationType.OP_ADD_FIRST_FRONTEND:
                case OperationType.OP_ADD_FIRST_FRONTEND_V2:
                case OperationType.OP_ADD_FRONTEND:
                case OperationType.OP_ADD_FRONTEND_V2: {
                    Frontend fe = (Frontend) journal.getData();
                    globalStateMgr.replayAddFrontend(fe);
                    break;
                }
                case OperationType.OP_REMOVE_FRONTEND:
                case OperationType.OP_REMOVE_FRONTEND_V2: {
                    Frontend fe = (Frontend) journal.getData();
                    globalStateMgr.replayDropFrontend(fe);
                    if (fe.getNodeName().equals(GlobalStateMgr.getCurrentState().getNodeName())) {
                        throw new JournalInconsistentException("current fe " + fe + " is removed. will exit");
                    }
                    break;
                }
                case OperationType.OP_UPDATE_FRONTEND:
                case OperationType.OP_UPDATE_FRONTEND_V2: {
                    Frontend fe = (Frontend) journal.getData();
                    globalStateMgr.replayUpdateFrontend(fe);
                    break;
                }
                case OperationType.OP_CREATE_USER:
                case OperationType.OP_NEW_DROP_USER:
                case OperationType.OP_GRANT_PRIV:
                case OperationType.OP_REVOKE_PRIV:
                case OperationType.OP_SET_PASSWORD:
                case OperationType.OP_CREATE_ROLE:
                case OperationType.OP_DROP_ROLE:
                case OperationType.OP_GRANT_ROLE:
                case OperationType.OP_REVOKE_ROLE:
                case OperationType.OP_UPDATE_USER_PROPERTY:
                case OperationType.OP_GRANT_IMPERSONATE:
                case OperationType.OP_REVOKE_IMPERSONATE: {
                    GlobalStateMgr.getCurrentState().replayOldAuthJournal(opCode, journal.getData());
                    break;
                }
                case OperationType.OP_TIMESTAMP:
                case OperationType.OP_TIMESTAMP_V2: {
                    Timestamp stamp = (Timestamp) journal.getData();
                    globalStateMgr.setSynchronizedTime(stamp.getTimestamp());
                    break;
                }
                case OperationType.OP_LEADER_INFO_CHANGE:
                case OperationType.OP_LEADER_INFO_CHANGE_V2: {
                    LeaderInfo info = (LeaderInfo) journal.getData();
                    globalStateMgr.setLeader(info);
                    break;
                }
                //compatible with old community meta, newly added log using OP_META_VERSION_V2
                case OperationType.OP_META_VERSION: {
                    String versionString = ((Text) journal.getData()).toString();
                    int version = Integer.parseInt(versionString);
                    if (version > FeConstants.META_VERSION) {
                        throw new JournalInconsistentException(
                                "invalid meta data version found, cat not bigger than FeConstants.meta_version."
                                        + "please update FeConstants.meta_version bigger or equal to " + version +
                                        " and restart.");
                    }
                    MetaContext.get().setMetaVersion(version);
                    break;
                }
                case OperationType.OP_META_VERSION_V2: {
                    MetaVersion metaVersion = (MetaVersion) journal.getData();
                    if (!MetaVersion.isCompatible(metaVersion.getStarRocksVersion(), FeConstants.STARROCKS_META_VERSION)) {
                        throw new JournalInconsistentException("Not compatible with meta version "
                                + metaVersion.getStarRocksVersion()
                                + ", current version is " + FeConstants.STARROCKS_META_VERSION);
                    }
                    MetaContext.get().setMetaVersion(metaVersion.getCommunityVersion());
                    MetaContext.get().setStarRocksMetaVersion(metaVersion.getStarRocksVersion());
                    break;
                }
                case OperationType.OP_GLOBAL_VARIABLE: {
                    SessionVariable variable = (SessionVariable) journal.getData();
                    globalStateMgr.replayGlobalVariable(variable);
                    break;
                }
                case OperationType.OP_CREATE_CLUSTER: {
                    final Cluster value = (Cluster) journal.getData();
                    globalStateMgr.replayCreateCluster(value);
                    break;
                }
                case OperationType.OP_ADD_BROKER:
                case OperationType.OP_ADD_BROKER_V2: {
                    final BrokerMgr.ModifyBrokerInfo param = (BrokerMgr.ModifyBrokerInfo) journal.getData();
                    globalStateMgr.getBrokerMgr().replayAddBrokers(param.brokerName, param.brokerAddresses);
                    break;
                }
                case OperationType.OP_DROP_BROKER:
                case OperationType.OP_DROP_BROKER_V2: {
                    final BrokerMgr.ModifyBrokerInfo param = (BrokerMgr.ModifyBrokerInfo) journal.getData();
                    globalStateMgr.getBrokerMgr().replayDropBrokers(param.brokerName, param.brokerAddresses);
                    break;
                }
                case OperationType.OP_DROP_ALL_BROKER: {
                    final String param = journal.getData().toString();
                    globalStateMgr.getBrokerMgr().replayDropAllBroker(param);
                    break;
                }
                case OperationType.OP_SET_LOAD_ERROR_HUB: {
                    final LoadErrorHub.Param param = (LoadErrorHub.Param) journal.getData();
                    globalStateMgr.getLoadInstance().setLoadErrorHubInfo(param);
                    break;
                }
                case OperationType.OP_UPDATE_CLUSTER_AND_BACKENDS: {
                    final BackendIdsUpdateInfo info = (BackendIdsUpdateInfo) journal.getData();
                    globalStateMgr.replayUpdateClusterAndBackends(info);
                    break;
                }
                case OperationType.OP_UPSERT_TRANSACTION_STATE_V2:
                case OperationType.OP_UPSERT_TRANSACTION_STATE: {
                    final TransactionState state = (TransactionState) journal.getData();
                    GlobalStateMgr.getCurrentGlobalTransactionMgr().replayUpsertTransactionState(state);
                    LOG.debug("opcode: {}, tid: {}", opCode, state.getTransactionId());
                    break;
                }
                case OperationType.OP_DELETE_TRANSACTION_STATE: {
                    final TransactionState state = (TransactionState) journal.getData();
                    GlobalStateMgr.getCurrentGlobalTransactionMgr().replayDeleteTransactionState(state);
                    LOG.debug("opcode: {}, tid: {}", opCode, state.getTransactionId());
                    break;
                }
                case OperationType.OP_CREATE_REPOSITORY:
                case OperationType.OP_CREATE_REPOSITORY_V2: {
                    Repository repository = (Repository) journal.getData();
                    globalStateMgr.getBackupHandler().getRepoMgr().addAndInitRepoIfNotExist(repository, true);
                    break;
                }
                case OperationType.OP_DROP_REPOSITORY: {
                    String repoName = ((Text) journal.getData()).toString();
                    globalStateMgr.getBackupHandler().getRepoMgr().removeRepo(repoName, true);
                    break;
                }
                case OperationType.OP_TRUNCATE_TABLE: {
                    TruncateTableInfo info = (TruncateTableInfo) journal.getData();
                    globalStateMgr.replayTruncateTable(info);
                    break;
                }
                case OperationType.OP_COLOCATE_ADD_TABLE:
                case OperationType.OP_COLOCATE_ADD_TABLE_V2: {
                    final ColocatePersistInfo info = (ColocatePersistInfo) journal.getData();
                    globalStateMgr.getColocateTableIndex().replayAddTableToGroup(info);
                    break;
                }
                case OperationType.OP_COLOCATE_REMOVE_TABLE: {
                    final ColocatePersistInfo info = (ColocatePersistInfo) journal.getData();
                    globalStateMgr.getColocateTableIndex().replayRemoveTable(info);
                    break;
                }
                case OperationType.OP_COLOCATE_BACKENDS_PER_BUCKETSEQ:
                case OperationType.OP_COLOCATE_BACKENDS_PER_BUCKETSEQ_V2: {
                    final ColocatePersistInfo info = (ColocatePersistInfo) journal.getData();
                    globalStateMgr.getColocateTableIndex().replayAddBackendsPerBucketSeq(info);
                    break;
                }
                case OperationType.OP_COLOCATE_MARK_UNSTABLE:
                case OperationType.OP_COLOCATE_MARK_UNSTABLE_V2: {
                    final ColocatePersistInfo info = (ColocatePersistInfo) journal.getData();
                    globalStateMgr.getColocateTableIndex().replayMarkGroupUnstable(info);
                    break;
                }
                case OperationType.OP_COLOCATE_MARK_STABLE:
                case OperationType.OP_COLOCATE_MARK_STABLE_V2: {
                    final ColocatePersistInfo info = (ColocatePersistInfo) journal.getData();
                    globalStateMgr.getColocateTableIndex().replayMarkGroupStable(info);
                    break;
                }
                case OperationType.OP_MODIFY_TABLE_COLOCATE:
                case OperationType.OP_MODIFY_TABLE_COLOCATE_V2: {
                    final TablePropertyInfo info = (TablePropertyInfo) journal.getData();
                    globalStateMgr.replayModifyTableColocate(info);
                    break;
                }
                case OperationType.OP_HEARTBEAT_V2:
                case OperationType.OP_HEARTBEAT: {
                    final HbPackage hbPackage = (HbPackage) journal.getData();
                    GlobalStateMgr.getCurrentHeartbeatMgr().replayHearbeat(hbPackage);
                    break;
                }
                case OperationType.OP_ADD_FUNCTION:
                case OperationType.OP_ADD_FUNCTION_V2: {
                    final Function function = (Function) journal.getData();
                    if (function.getFunctionName().isGlobalFunction()) {
                        GlobalStateMgr.getCurrentState().getGlobalFunctionMgr().replayAddFunction(function);
                    } else {
                        Database.replayCreateFunctionLog(function);
                    }
                    break;
                }
                case OperationType.OP_DROP_FUNCTION:
                case OperationType.OP_DROP_FUNCTION_V2: {
                    FunctionSearchDesc function = (FunctionSearchDesc) journal.getData();
                    if (function.getName().isGlobalFunction()) {
                        GlobalStateMgr.getCurrentState().getGlobalFunctionMgr().replayDropFunction(function);
                    } else {
                        Database.replayDropFunctionLog(function);
                    }
                    break;
                }
                case OperationType.OP_BACKEND_TABLETS_INFO:
                case OperationType.OP_BACKEND_TABLETS_INFO_V2: {
                    BackendTabletsInfo backendTabletsInfo = (BackendTabletsInfo) journal.getData();
                    GlobalStateMgr.getCurrentState().replayBackendTabletsInfo(backendTabletsInfo);
                    break;
                }
                case OperationType.OP_CREATE_ROUTINE_LOAD_JOB_V2:
                case OperationType.OP_CREATE_ROUTINE_LOAD_JOB: {
                    RoutineLoadJob routineLoadJob = (RoutineLoadJob) journal.getData();
                    GlobalStateMgr.getCurrentState().getRoutineLoadMgr().replayCreateRoutineLoadJob(routineLoadJob);
                    break;
                }
                case OperationType.OP_CHANGE_ROUTINE_LOAD_JOB_V2:
                case OperationType.OP_CHANGE_ROUTINE_LOAD_JOB: {
                    RoutineLoadOperation operation = (RoutineLoadOperation) journal.getData();
                    GlobalStateMgr.getCurrentState().getRoutineLoadMgr().replayChangeRoutineLoadJob(operation);
                    break;
                }
                case OperationType.OP_REMOVE_ROUTINE_LOAD_JOB: {
                    RoutineLoadOperation operation = (RoutineLoadOperation) journal.getData();
                    globalStateMgr.getRoutineLoadMgr().replayRemoveOldRoutineLoad(operation);
                    break;
                }
                case OperationType.OP_CREATE_STREAM_LOAD_TASK:
                case OperationType.OP_CREATE_STREAM_LOAD_TASK_V2: {
                    StreamLoadTask streamLoadTask = (StreamLoadTask) journal.getData();
                    globalStateMgr.getStreamLoadMgr().replayCreateLoadTask(streamLoadTask);
                    break;
                }
                case OperationType.OP_CREATE_LOAD_JOB_V2:
                case OperationType.OP_CREATE_LOAD_JOB: {
                    com.starrocks.load.loadv2.LoadJob loadJob =
                            (com.starrocks.load.loadv2.LoadJob) journal.getData();
                    globalStateMgr.getLoadMgr().replayCreateLoadJob(loadJob);
                    break;
                }
                case OperationType.OP_END_LOAD_JOB_V2:
                case OperationType.OP_END_LOAD_JOB: {
                    LoadJobFinalOperation operation = (LoadJobFinalOperation) journal.getData();
                    globalStateMgr.getLoadMgr().replayEndLoadJob(operation);
                    break;
                }
                case OperationType.OP_UPDATE_LOAD_JOB: {
                    LoadJobStateUpdateInfo info = (LoadJobStateUpdateInfo) journal.getData();
                    globalStateMgr.getLoadMgr().replayUpdateLoadJobStateInfo(info);
                    break;
                }
                case OperationType.OP_CREATE_RESOURCE: {
                    final Resource resource = (Resource) journal.getData();
                    globalStateMgr.getResourceMgr().replayCreateResource(resource);
                    break;
                }
                case OperationType.OP_DROP_RESOURCE: {
                    final DropResourceOperationLog operationLog = (DropResourceOperationLog) journal.getData();
                    globalStateMgr.getResourceMgr().replayDropResource(operationLog);
                    break;
                }
                case OperationType.OP_RESOURCE_GROUP: {
                    final ResourceGroupOpEntry entry = (ResourceGroupOpEntry) journal.getData();
                    globalStateMgr.getResourceGroupMgr().replayResourceGroupOp(entry);
                    break;
                }
                case OperationType.OP_CREATE_TASK: {
                    final Task task = (Task) journal.getData();
                    globalStateMgr.getTaskManager().replayCreateTask(task);
                    break;
                }
                case OperationType.OP_DROP_TASKS: {
                    DropTasksLog dropTasksLog = (DropTasksLog) journal.getData();
                    globalStateMgr.getTaskManager().replayDropTasks(dropTasksLog.getTaskIdList());
                    break;
                }
                case OperationType.OP_ALTER_TASK: {
                    final Task task = (Task) journal.getData();
                    globalStateMgr.getTaskManager().replayAlterTask(task);
                    break;
                }
                case OperationType.OP_CREATE_TASK_RUN: {
                    final TaskRunStatus status = (TaskRunStatus) journal.getData();
                    globalStateMgr.getTaskManager().replayCreateTaskRun(status);
                    break;
                }
                case OperationType.OP_UPDATE_TASK_RUN: {
                    final TaskRunStatusChange statusChange =
                            (TaskRunStatusChange) journal.getData();
                    globalStateMgr.getTaskManager().replayUpdateTaskRun(statusChange);
                    break;
                }
                case OperationType.OP_DROP_TASK_RUNS: {
                    DropTaskRunsLog dropTaskRunsLog = (DropTaskRunsLog) journal.getData();
                    globalStateMgr.getTaskManager().replayDropTaskRuns(dropTaskRunsLog.getQueryIdList());
                    break;
                }
                case OperationType.OP_UPDATE_TASK_RUN_STATE:
                    TaskRunPeriodStatusChange taskRunPeriodStatusChange = (TaskRunPeriodStatusChange) journal.getData();
                    globalStateMgr.getTaskManager().replayAlterRunningTaskRunProgress(
                            taskRunPeriodStatusChange.getTaskRunProgressMap());
                    break;
                case OperationType.OP_CREATE_SMALL_FILE:
                case OperationType.OP_CREATE_SMALL_FILE_V2: {
                    SmallFile smallFile = (SmallFile) journal.getData();
                    globalStateMgr.getSmallFileMgr().replayCreateFile(smallFile);
                    break;
                }
                case OperationType.OP_DROP_SMALL_FILE:
                case OperationType.OP_DROP_SMALL_FILE_V2: {
                    SmallFile smallFile = (SmallFile) journal.getData();
                    globalStateMgr.getSmallFileMgr().replayRemoveFile(smallFile);
                    break;
                }
                case OperationType.OP_ALTER_JOB_V2: {
                    AlterJobV2 alterJob = (AlterJobV2) journal.getData();
                    switch (alterJob.getType()) {
                        case ROLLUP:
                            globalStateMgr.getRollupHandler().replayAlterJobV2(alterJob);
                            break;
                        case SCHEMA_CHANGE:
                            globalStateMgr.getSchemaChangeHandler().replayAlterJobV2(alterJob);
                            break;
                        default:
                            break;
                    }
                    break;
                }
                case OperationType.OP_BATCH_ADD_ROLLUP:
                case OperationType.OP_BATCH_ADD_ROLLUP_V2: {
                    BatchAlterJobPersistInfo batchAlterJobV2 = (BatchAlterJobPersistInfo) journal.getData();
                    for (AlterJobV2 alterJobV2 : batchAlterJobV2.getAlterJobV2List()) {
                        globalStateMgr.getRollupHandler().replayAlterJobV2(alterJobV2);
                    }
                    break;
                }
                case OperationType.OP_MODIFY_DISTRIBUTION_TYPE:
                case OperationType.OP_MODIFY_DISTRIBUTION_TYPE_V2: {
                    TableInfo tableInfo = (TableInfo) journal.getData();
                    globalStateMgr.replayConvertDistributionType(tableInfo);
                    break;
                }
                case OperationType.OP_DYNAMIC_PARTITION:
                case OperationType.OP_MODIFY_IN_MEMORY:
                case OperationType.OP_SET_FORBIDDEN_GLOBAL_DICT:
                case OperationType.OP_MODIFY_REPLICATION_NUM:
                case OperationType.OP_MODIFY_WRITE_QUORUM:
                case OperationType.OP_MODIFY_REPLICATED_STORAGE:
                case OperationType.OP_MODIFY_BINLOG_AVAILABLE_VERSION:
                case OperationType.OP_MODIFY_BINLOG_CONFIG:
                case OperationType.OP_MODIFY_ENABLE_PERSISTENT_INDEX:
                case OperationType.OP_ALTER_TABLE_PROPERTIES:
                case OperationType.OP_MODIFY_TABLE_CONSTRAINT_PROPERTY: {
                    ModifyTablePropertyOperationLog modifyTablePropertyOperationLog =
                            (ModifyTablePropertyOperationLog) journal.getData();
                    globalStateMgr.replayModifyTableProperty(opCode, modifyTablePropertyOperationLog);
                    break;
                }
                case OperationType.OP_REPLACE_TEMP_PARTITION: {
                    ReplacePartitionOperationLog replaceTempPartitionLog =
                            (ReplacePartitionOperationLog) journal.getData();
                    globalStateMgr.replayReplaceTempPartition(replaceTempPartitionLog);
                    break;
                }
                case OperationType.OP_INSTALL_PLUGIN: {
                    PluginInfo pluginInfo = (PluginInfo) journal.getData();
                    globalStateMgr.replayInstallPlugin(pluginInfo);
                    break;
                }
                case OperationType.OP_UNINSTALL_PLUGIN: {
                    PluginInfo pluginInfo = (PluginInfo) journal.getData();
                    globalStateMgr.replayUninstallPlugin(pluginInfo);
                    break;
                }
                case OperationType.OP_SET_REPLICA_STATUS: {
                    SetReplicaStatusOperationLog log = (SetReplicaStatusOperationLog) journal.getData();
                    globalStateMgr.replaySetReplicaStatus(log);
                    break;
                }
                case OperationType.OP_REMOVE_ALTER_JOB_V2: {
                    RemoveAlterJobV2OperationLog log = (RemoveAlterJobV2OperationLog) journal.getData();
                    switch (log.getType()) {
                        case ROLLUP:
                            globalStateMgr.getRollupHandler().replayRemoveAlterJobV2(log);
                            break;
                        case SCHEMA_CHANGE:
                            globalStateMgr.getSchemaChangeHandler().replayRemoveAlterJobV2(log);
                            break;
                        default:
                            break;
                    }
                    break;
                }
                case OperationType.OP_ALTER_ROUTINE_LOAD_JOB: {
                    AlterRoutineLoadJobOperationLog log = (AlterRoutineLoadJobOperationLog) journal.getData();
                    globalStateMgr.getRoutineLoadMgr().replayAlterRoutineLoadJob(log);
                    break;
                }
                case OperationType.OP_ALTER_LOAD_JOB: {
                    AlterLoadJobOperationLog log = (AlterLoadJobOperationLog) journal.getData();
                    globalStateMgr.getLoadMgr().replayAlterLoadJob(log);
                    break;
                }
                case OperationType.OP_GLOBAL_VARIABLE_V2: {
                    GlobalVarPersistInfo info = (GlobalVarPersistInfo) journal.getData();
                    globalStateMgr.replayGlobalVariableV2(info);
                    break;
                }
                case OperationType.OP_SWAP_TABLE: {
                    SwapTableOperationLog log = (SwapTableOperationLog) journal.getData();
                    globalStateMgr.getAlterJobMgr().replaySwapTable(log);
                    break;
                }
                case OperationType.OP_ADD_ANALYZER_JOB: {
                    AnalyzeJob analyzeJob = (AnalyzeJob) journal.getData();
                    globalStateMgr.getAnalyzeMgr().replayAddAnalyzeJob(analyzeJob);
                    break;
                }
                case OperationType.OP_REMOVE_ANALYZER_JOB: {
                    AnalyzeJob analyzeJob = (AnalyzeJob) journal.getData();
                    globalStateMgr.getAnalyzeMgr().replayRemoveAnalyzeJob(analyzeJob);
                    break;
                }
                case OperationType.OP_ADD_ANALYZE_STATUS: {
                    AnalyzeStatus analyzeStatus = (AnalyzeStatus) journal.getData();
                    globalStateMgr.getAnalyzeMgr().replayAddAnalyzeStatus(analyzeStatus);
                    break;
                }
                case OperationType.OP_REMOVE_ANALYZE_STATUS: {
                    AnalyzeStatus analyzeStatus = (AnalyzeStatus) journal.getData();
                    globalStateMgr.getAnalyzeMgr().replayRemoveAnalyzeStatus(analyzeStatus);
                    break;
                }
                case OperationType.OP_ADD_BASIC_STATS_META: {
                    BasicStatsMeta basicStatsMeta = (BasicStatsMeta) journal.getData();
                    globalStateMgr.getAnalyzeMgr().replayAddBasicStatsMeta(basicStatsMeta);
                    // The follower replays the stats meta log, indicating that the master has re-completed
                    // statistic, and the follower's should refresh cache here.
                    // We don't need to refresh statistics when checkpointing
                    if (!GlobalStateMgr.isCheckpointThread()) {
                        globalStateMgr.getAnalyzeMgr().refreshBasicStatisticsCache(basicStatsMeta.getDbId(),
                                basicStatsMeta.getTableId(), basicStatsMeta.getColumns(), true);
                    }
                    break;
                }
                case OperationType.OP_REMOVE_BASIC_STATS_META: {
                    BasicStatsMeta basicStatsMeta = (BasicStatsMeta) journal.getData();
                    globalStateMgr.getAnalyzeMgr().replayRemoveBasicStatsMeta(basicStatsMeta);
                    break;
                }
                case OperationType.OP_ADD_HISTOGRAM_STATS_META: {
                    HistogramStatsMeta histogramStatsMeta = (HistogramStatsMeta) journal.getData();
                    globalStateMgr.getAnalyzeMgr().replayAddHistogramStatsMeta(histogramStatsMeta);
                    // The follower replays the stats meta log, indicating that the master has re-completed
                    // statistic, and the follower's should expire cache here.
                    // We don't need to refresh statistics when checkpointing
                    if (!GlobalStateMgr.isCheckpointThread()) {
                        globalStateMgr.getAnalyzeMgr().refreshHistogramStatisticsCache(
                                histogramStatsMeta.getDbId(), histogramStatsMeta.getTableId(),
                                Lists.newArrayList(histogramStatsMeta.getColumn()), true);
                    }
                    break;
                }
                case OperationType.OP_REMOVE_HISTOGRAM_STATS_META: {
                    HistogramStatsMeta histogramStatsMeta = (HistogramStatsMeta) journal.getData();
                    globalStateMgr.getAnalyzeMgr().replayRemoveHistogramStatsMeta(histogramStatsMeta);
                    break;
                }
                case OperationType.OP_MODIFY_HIVE_TABLE_COLUMN: {
                    ModifyTableColumnOperationLog modifyTableColumnOperationLog =
                            (ModifyTableColumnOperationLog) journal.getData();
                    globalStateMgr.replayModifyHiveTableColumn(opCode, modifyTableColumnOperationLog);
                    break;
                }
                case OperationType.OP_CREATE_CATALOG: {
                    Catalog catalog = (Catalog) journal.getData();
                    globalStateMgr.getCatalogMgr().replayCreateCatalog(catalog);
                    break;
                }
                case OperationType.OP_DROP_CATALOG: {
                    DropCatalogLog dropCatalogLog =
                            (DropCatalogLog) journal.getData();
                    globalStateMgr.getCatalogMgr().replayDropCatalog(dropCatalogLog);
                    break;
                }

                case OperationType.OP_CREATE_INSERT_OVERWRITE: {
                    CreateInsertOverwriteJobLog jobInfo = (CreateInsertOverwriteJobLog) journal.getData();
                    globalStateMgr.getInsertOverwriteJobMgr().replayCreateInsertOverwrite(jobInfo);
                    break;
                }
                case OperationType.OP_INSERT_OVERWRITE_STATE_CHANGE: {
                    InsertOverwriteStateChangeInfo stateChangeInfo = (InsertOverwriteStateChangeInfo) journal.getData();
                    globalStateMgr.getInsertOverwriteJobMgr().replayInsertOverwriteStateChange(stateChangeInfo);
                    break;
                }
                case OperationType.OP_ADD_UNUSED_SHARD:
                case OperationType.OP_DELETE_UNUSED_SHARD:
                    // Deprecated: Nothing to do
                    break;
                case OperationType.OP_STARMGR: {
                    StarMgrJournal j = (StarMgrJournal) journal.getData();
                    StarMgrServer.getCurrentState().getStarMgr().replay(j.getJournal());
                    break;
                }
                case OperationType.OP_CREATE_USER_V2: {
                    CreateUserInfo info = (CreateUserInfo) journal.getData();
                    globalStateMgr.getAuthenticationMgr().replayCreateUser(
                            info.getUserIdentity(),
                            info.getAuthenticationInfo(),
                            info.getUserProperty(),
                            info.getUserPrivilegeCollection(),
                            info.getPluginId(),
                            info.getPluginVersion());
                    break;
                }
                case OperationType.OP_UPDATE_USER_PRIVILEGE_V2: {
                    UserPrivilegeCollectionInfo info = (UserPrivilegeCollectionInfo) journal.getData();
                    globalStateMgr.getAuthorizationMgr().replayUpdateUserPrivilegeCollection(
                            info.getUserIdentity(),
                            info.getPrivilegeCollection(),
                            info.getPluginId(),
                            info.getPluginVersion());
                    break;
                }
                case OperationType.OP_ALTER_USER_V2: {
                    AlterUserInfo info = (AlterUserInfo) journal.getData();
                    globalStateMgr.getAuthenticationMgr().replayAlterUser(
                            info.getUserIdentity(), info.getAuthenticationInfo());
                    break;
                }
                case OperationType.OP_UPDATE_USER_PROP_V2:
                case OperationType.OP_UPDATE_USER_PROP_V3: {
                    UserPropertyInfo info = (UserPropertyInfo) journal.getData();
                    globalStateMgr.getAuthenticationMgr().replayUpdateUserProperty(info);
                    break;
                }
                case OperationType.OP_DROP_USER_V2:
                case OperationType.OP_DROP_USER_V3: {
                    UserIdentity userIdentity = (UserIdentity) journal.getData();
                    globalStateMgr.getAuthenticationMgr().replayDropUser(userIdentity);
                    break;
                }
                case OperationType.OP_UPDATE_ROLE_PRIVILEGE_V2: {
                    RolePrivilegeCollectionInfo info = (RolePrivilegeCollectionInfo) journal.getData();
                    globalStateMgr.getAuthorizationMgr().replayUpdateRolePrivilegeCollection(info);
                    break;
                }
                case OperationType.OP_DROP_ROLE_V2: {
                    RolePrivilegeCollectionInfo info = (RolePrivilegeCollectionInfo) journal.getData();
                    globalStateMgr.getAuthorizationMgr().replayDropRole(info);
                    break;
                }
                case OperationType.OP_AUTH_UPGRADE_V2: {
                    AuthUpgradeInfo info = (AuthUpgradeInfo) journal.getData();
                    globalStateMgr.replayAuthUpgrade(info);
                    break;
                }
                case OperationType.OP_MV_JOB_STATE: {
                    MVMaintenanceJob job = (MVMaintenanceJob) journal.getData();
                    MaterializedViewMgr.getInstance().replay(job);
                    break;
                }
                case OperationType.OP_MV_EPOCH_UPDATE: {
                    MVEpoch epoch = (MVEpoch) journal.getData();
                    MaterializedViewMgr.getInstance().replayEpoch(epoch);
                    break;
                }
                default: {
                    if (Config.ignore_unknown_log_id) {
                        LOG.warn("UNKNOWN Operation Type {}", opCode);
                    } else {
                        throw new IOException("UNKNOWN Operation Type " + opCode);
                    }
                }
            }
        } catch (Exception e) {
            JournalInconsistentException exception =
                    new JournalInconsistentException("failed to load journal type " + opCode);
            exception.initCause(e);
            throw exception;
        }
    }

    /**
     * submit log to queue, wait for JournalWriter
     */
    protected void logEdit(short op, Writable writable) {
        long start = System.nanoTime();
        Future<Boolean> task = submitLog(op, writable, -1);
        waitInfinity(start, task);
    }

    /**
     * submit log in queue and return immediately
     */
    private Future<Boolean> submitLog(short op, Writable writable, long maxWaitIntervalMs) {
        // do not check whether global state mgr is leader in non shared-nothing mode,
        // because starmgr state change happens before global state mgr state change,
        // it will write log before global state mgr becomes leader
        Preconditions.checkState(RunMode.getCurrentRunMode() != RunMode.SHARED_NOTHING ||
                                 GlobalStateMgr.getCurrentState().isLeader(),
                "Current node is not leader, submit log is not allowed");
        DataOutputBuffer buffer = new DataOutputBuffer(OUTPUT_BUFFER_INIT_SIZE);

        // 1. serialized
        try {
            JournalEntity entity = new JournalEntity();
            entity.setOpCode(op);
            entity.setData(writable);
            entity.write(buffer);
        } catch (IOException e) {
            // The old implementation swallow exception like this
            LOG.info("failed to serialize, ", e);
        }
        JournalTask task = new JournalTask(buffer, maxWaitIntervalMs);

        /*
         * for historical reasons, logEdit is not allowed to raise Exception, which is really unreasonable to me.
         * This PR will continue to swallow exception and retry till the end of the world like before.
         * Hope some day we'll fix it.
         */
        // 2. put to queue
        int cnt = 0;
        while (true) {
            try {
                if (cnt != 0) {
                    Thread.sleep(1000);
                }
                this.journalQueue.put(task);
                break;
            } catch (InterruptedException e) {
                // got interrupted while waiting if necessary for space to become available
                LOG.warn("failed to put queue, wait and retry {} times..: {}", cnt, e);
            }
            cnt++;
        }
        return task;
    }

    /**
     * wait for JournalWriter commit all logs
     */
    public static void waitInfinity(long startTime, Future<Boolean> task) {
        boolean result;
        int cnt = 0;
        while (true) {
            try {
                if (cnt != 0) {
                    Thread.sleep(1000);
                }
                // return true if JournalWriter wrote log successfully
                // return false if JournalWriter wrote log failed, which WON'T HAPPEN for now because on such
                // scenario JournalWriter will simply exit the whole process
                result = task.get();
                break;
            } catch (InterruptedException | ExecutionException e) {
                LOG.warn("failed to wait, wait and retry {} times..: {}", cnt, e);
                cnt++;
            }
        }

        // for now if journal writer fails, it will exit directly, so this property should always be true.
        assert (result);
        if (MetricRepo.hasInit) {
            MetricRepo.HISTO_EDIT_LOG_WRITE_LATENCY.update((System.nanoTime() - startTime) / 1000000);
        }
    }

    public void logSaveNextId(long nextId) {
        logEdit(OperationType.OP_SAVE_NEXTID, new Text(Long.toString(nextId)));
    }

    public void logSaveTransactionId(long transactionId) {
        logEdit(OperationType.OP_SAVE_TRANSACTION_ID, new Text(Long.toString(transactionId)));
    }

    public void logSaveAutoIncrementId(AutoIncrementInfo info) {
        logEdit(OperationType.OP_SAVE_AUTO_INCREMENT_ID, info);
    }

    public void logSaveDeleteAutoIncrementId(AutoIncrementInfo info) {
        logEdit(OperationType.OP_DELETE_AUTO_INCREMENT_ID, info);
    }

    public void logCreateDb(Database db) {
        logEdit(OperationType.OP_CREATE_DB, db);
    }

    public void logDropDb(DropDbInfo dropDbInfo) {
        logEdit(OperationType.OP_DROP_DB, dropDbInfo);
    }

    public void logEraseDb(long dbId) {
        logEdit(OperationType.OP_ERASE_DB, new Text(Long.toString(dbId)));
    }

    public void logRecoverDb(RecoverInfo info) {
        logEdit(OperationType.OP_RECOVER_DB, info);
    }

    public void logAlterDb(DatabaseInfo dbInfo) {
        logEdit(OperationType.OP_ALTER_DB, dbInfo);
    }

    public void logCreateTable(CreateTableInfo info) {
        logEdit(OperationType.OP_CREATE_TABLE, info);
    }

    public void logCreateMaterializedView(CreateTableInfo info) {
        logEdit(OperationType.OP_CREATE_MATERIALIZED_VIEW, info);
    }

    public void logResourceGroupOp(ResourceGroupOpEntry op) {
        logEdit(OperationType.OP_RESOURCE_GROUP, op);
    }

    public void logCreateTask(Task info) {
        logEdit(OperationType.OP_CREATE_TASK, info);
    }

    public void logDropTasks(List<Long> taskIdList) {
        logEdit(OperationType.OP_DROP_TASKS, new DropTasksLog(taskIdList));
    }

    public void logTaskRunCreateStatus(TaskRunStatus status) {
        logEdit(OperationType.OP_CREATE_TASK_RUN, status);
    }

    public void logUpdateTaskRun(TaskRunStatusChange statusChange) {
        logEdit(OperationType.OP_UPDATE_TASK_RUN, statusChange);
    }

    public void logAlterRunningTaskRunProgress(TaskRunPeriodStatusChange info) {
        logEdit(OperationType.OP_UPDATE_TASK_RUN_STATE, info);
    }

    public void logAddPartition(PartitionPersistInfoV2 info) {
        logEdit(OperationType.OP_ADD_PARTITION_V2, info);
    }

    public void logAddPartition(PartitionPersistInfo info) {
        logEdit(OperationType.OP_ADD_PARTITION, info);
    }

    public void logAddPartitions(AddPartitionsInfo info) {
        logEdit(OperationType.OP_ADD_PARTITIONS, info);
    }

    public void logAddPartitions(AddPartitionsInfoV2 info) {
        logEdit(OperationType.OP_ADD_PARTITIONS_V2, info);
    }

    public void logDropPartition(DropPartitionInfo info) {
        logEdit(OperationType.OP_DROP_PARTITION, info);
    }

    public void logErasePartition(long partitionId) {
        logEdit(OperationType.OP_ERASE_PARTITION, new Text(Long.toString(partitionId)));
    }

    public void logRecoverPartition(RecoverInfo info) {
        logEdit(OperationType.OP_RECOVER_PARTITION, info);
    }

    public void logModifyPartition(ModifyPartitionInfo info) {
        logEdit(OperationType.OP_MODIFY_PARTITION, info);
    }

    public void logBatchModifyPartition(BatchModifyPartitionsInfo info) {
        logEdit(OperationType.OP_BATCH_MODIFY_PARTITION, info);
    }

    public void logDropTable(DropInfo info) {
        logEdit(OperationType.OP_DROP_TABLE, info);
    }

    public void logEraseMultiTables(List<Long> tableIds) {
        logEdit(OperationType.OP_ERASE_MULTI_TABLES, new MultiEraseTableInfo(tableIds));
    }

    public void logRecoverTable(RecoverInfo info) {
        logEdit(OperationType.OP_RECOVER_TABLE, info);
    }

    public void logDropRollup(DropInfo info) {
        logEdit(OperationType.OP_DROP_ROLLUP, info);
    }

    public void logBatchDropRollup(BatchDropInfo batchDropInfo) {
        logEdit(OperationType.OP_BATCH_DROP_ROLLUP, batchDropInfo);
    }

    public void logFinishConsistencyCheck(ConsistencyCheckInfo info) {
        logEdit(OperationType.OP_FINISH_CONSISTENCY_CHECK, info);
    }

    public void logAddComputeNode(ComputeNode computeNode) {
        logEdit(OperationType.OP_ADD_COMPUTE_NODE, computeNode);
    }

    public void logAddBackend(Backend be) {
        logEdit(OperationType.OP_ADD_BACKEND, be);
    }

    public void logDropComputeNode(DropComputeNodeLog log) {
        logEdit(OperationType.OP_DROP_COMPUTE_NODE, log);
    }

    public void logDropBackend(Backend be) {
        logEdit(OperationType.OP_DROP_BACKEND, be);
    }

    public void logAddFrontend(Frontend fe) {
        logEdit(OperationType.OP_ADD_FRONTEND, fe);
    }

    public void logAddFirstFrontend(Frontend fe) {
        logEdit(OperationType.OP_ADD_FIRST_FRONTEND, fe);
    }

    public void logRemoveFrontend(Frontend fe) {
        logEdit(OperationType.OP_REMOVE_FRONTEND, fe);
    }

    public void logUpdateFrontend(Frontend fe) {
        logEdit(OperationType.OP_UPDATE_FRONTEND, fe);
    }

    public void logFinishMultiDelete(MultiDeleteInfo info) {
        logEdit(OperationType.OP_FINISH_MULTI_DELETE, info);
    }

    public void logAddReplica(ReplicaPersistInfo info) {
        logEdit(OperationType.OP_ADD_REPLICA, info);
    }

    public void logUpdateReplica(ReplicaPersistInfo info) {
        logEdit(OperationType.OP_UPDATE_REPLICA, info);
    }

    public void logDeleteReplica(ReplicaPersistInfo info) {
        logEdit(OperationType.OP_DELETE_REPLICA, info);
    }

    public void logTimestamp(Timestamp stamp) {
        logEdit(OperationType.OP_TIMESTAMP, stamp);
    }

    public void logLeaderInfo(LeaderInfo info) {
        logEdit(OperationType.OP_LEADER_INFO_CHANGE, info);
    }

    public void logMetaVersion(MetaVersion metaVersion) {
        logEdit(OperationType.OP_META_VERSION_V2, metaVersion);
    }

    public void logBackendStateChange(Backend be) {
        logEdit(OperationType.OP_BACKEND_STATE_CHANGE, be);
    }

    public void logCreateUser(PrivInfo info) {
        logEdit(OperationType.OP_CREATE_USER, info);
    }

    public void logNewDropUser(UserIdentity userIdent) {
        logEdit(OperationType.OP_NEW_DROP_USER, userIdent);
    }

    public void logGrantPriv(PrivInfo info) {
        logEdit(OperationType.OP_GRANT_PRIV, info);
    }

    public void logRevokePriv(PrivInfo info) {
        logEdit(OperationType.OP_REVOKE_PRIV, info);
    }

    public void logGrantImpersonate(ImpersonatePrivInfo info) {
        logEdit(OperationType.OP_GRANT_IMPERSONATE, info);
    }

    public void logRevokeImpersonate(ImpersonatePrivInfo info) {
        logEdit(OperationType.OP_REVOKE_IMPERSONATE, info);
    }

    public void logSetPassword(PrivInfo info) {
        logEdit(OperationType.OP_SET_PASSWORD, info);
    }

    public void logCreateRole(PrivInfo info) {
        logEdit(OperationType.OP_CREATE_ROLE, info);
    }

    public void logDropRole(PrivInfo info) {
        logEdit(OperationType.OP_DROP_ROLE, info);
    }

    public void logGrantRole(PrivInfo info) {
        logEdit(OperationType.OP_GRANT_ROLE, info);
    }

    public void logRevokeRole(PrivInfo info) {
        logEdit(OperationType.OP_REVOKE_ROLE, info);
    }

    public void logDatabaseRename(DatabaseInfo databaseInfo) {
        logEdit(OperationType.OP_RENAME_DB, databaseInfo);
    }

    public void logTableRename(TableInfo tableInfo) {
        logEdit(OperationType.OP_RENAME_TABLE, tableInfo);
    }

    public void logModifyViewDef(AlterViewInfo alterViewInfo) {
        logEdit(OperationType.OP_MODIFY_VIEW_DEF, alterViewInfo);
    }

    public void logRollupRename(TableInfo tableInfo) {
        logEdit(OperationType.OP_RENAME_ROLLUP, tableInfo);
    }

    public void logPartitionRename(TableInfo tableInfo) {
        logEdit(OperationType.OP_RENAME_PARTITION, tableInfo);
    }

    public void logGlobalVariable(SessionVariable variable) {
        logEdit(OperationType.OP_GLOBAL_VARIABLE, variable);
    }

    public void logCreateCluster(Cluster cluster) {
        logEdit(OperationType.OP_CREATE_CLUSTER, cluster);
    }

    public void logAddBroker(BrokerMgr.ModifyBrokerInfo info) {
        logEdit(OperationType.OP_ADD_BROKER, info);
    }

    public void logDropBroker(BrokerMgr.ModifyBrokerInfo info) {
        logEdit(OperationType.OP_DROP_BROKER, info);
    }

    public void logDropAllBroker(String brokerName) {
        logEdit(OperationType.OP_DROP_ALL_BROKER, new Text(brokerName));
    }

    public void logSetLoadErrorHub(LoadErrorHub.Param param) {
        logEdit(OperationType.OP_SET_LOAD_ERROR_HUB, param);
    }

    public void logExportCreate(ExportJob job) {
        logEdit(OperationType.OP_EXPORT_CREATE, job);
    }

    public void logExportUpdateState(long jobId, ExportJob.JobState newState,
                                     List<Pair<TNetworkAddress, String>> snapshotPaths, String exportTempPath,
                                     Set<String> exportedFiles, ExportFailMsg failMsg) {
        ExportJob.ExportUpdateInfo updateInfo = new ExportJob.ExportUpdateInfo(jobId, newState,
                snapshotPaths, exportTempPath, exportedFiles, failMsg);
        logEdit(OperationType.OP_EXPORT_UPDATE_INFO, updateInfo);
    }

    // for TransactionState
    public void logInsertTransactionState(TransactionState transactionState) {
        logEdit(OperationType.OP_UPSERT_TRANSACTION_STATE, transactionState);
    }

    public void logBackupJob(BackupJob job) {
        logEdit(OperationType.OP_BACKUP_JOB, job);
    }

    public void logCreateRepository(Repository repo) {
        logEdit(OperationType.OP_CREATE_REPOSITORY, repo);
    }

    public void logDropRepository(String repoName) {
        logEdit(OperationType.OP_DROP_REPOSITORY, new Text(repoName));
    }

    public void logRestoreJob(RestoreJob job) {
        logEdit(OperationType.OP_RESTORE_JOB, job);
    }

    public void logUpdateUserProperty(UserPropertyInfo propertyInfo) {
        logEdit(OperationType.OP_UPDATE_USER_PROPERTY, propertyInfo);
    }

    public void logTruncateTable(TruncateTableInfo info) {
        logEdit(OperationType.OP_TRUNCATE_TABLE, info);
    }

    public void logColocateAddTable(ColocatePersistInfo info) {
        logEdit(OperationType.OP_COLOCATE_ADD_TABLE, info);
    }

    public void logColocateBackendsPerBucketSeq(ColocatePersistInfo info) {
        logEdit(OperationType.OP_COLOCATE_BACKENDS_PER_BUCKETSEQ, info);
    }

    public void logColocateMarkUnstable(ColocatePersistInfo info) {
        logEdit(OperationType.OP_COLOCATE_MARK_UNSTABLE, info);
    }

    public void logColocateMarkStable(ColocatePersistInfo info) {
        logEdit(OperationType.OP_COLOCATE_MARK_STABLE, info);
    }

    public void logModifyTableColocate(TablePropertyInfo info) {
        logEdit(OperationType.OP_MODIFY_TABLE_COLOCATE, info);
    }

    public void logHeartbeat(HbPackage hbPackage) {
        logEdit(OperationType.OP_HEARTBEAT_V2, hbPackage);
    }

    public void logAddFunction(Function function) {
        logEdit(OperationType.OP_ADD_FUNCTION, function);
    }

    public void logDropFunction(FunctionSearchDesc function) {
        logEdit(OperationType.OP_DROP_FUNCTION, function);
    }

    public void logSetHasForbiddenGlobalDict(ModifyTablePropertyOperationLog info) {
        logEdit(OperationType.OP_SET_FORBIDDEN_GLOBAL_DICT, info);
    }

    public void logBackendTabletsInfo(BackendTabletsInfo backendTabletsInfo) {
        logEdit(OperationType.OP_BACKEND_TABLETS_INFO, backendTabletsInfo);
    }

    public void logCreateRoutineLoadJob(RoutineLoadJob routineLoadJob) {
        logEdit(OperationType.OP_CREATE_ROUTINE_LOAD_JOB, routineLoadJob);
    }

    public void logOpRoutineLoadJob(RoutineLoadOperation routineLoadOperation) {
        logEdit(OperationType.OP_CHANGE_ROUTINE_LOAD_JOB, routineLoadOperation);
    }

    public void logCreateStreamLoadJob(StreamLoadTask streamLoadTask) {
        logEdit(OperationType.OP_CREATE_STREAM_LOAD_TASK, streamLoadTask);
    }

    public void logCreateLoadJob(com.starrocks.load.loadv2.LoadJob loadJob) {
        logEdit(OperationType.OP_CREATE_LOAD_JOB, loadJob);
    }

    public void logEndLoadJob(LoadJobFinalOperation loadJobFinalOperation) {
        logEdit(OperationType.OP_END_LOAD_JOB, loadJobFinalOperation);
    }

    public void logUpdateLoadJob(LoadJobStateUpdateInfo info) {
        logEdit(OperationType.OP_UPDATE_LOAD_JOB, info);
    }

    public void logCreateResource(Resource resource) {
        logEdit(OperationType.OP_CREATE_RESOURCE, resource);
    }

    public void logDropResource(DropResourceOperationLog operationLog) {
        logEdit(OperationType.OP_DROP_RESOURCE, operationLog);
    }

    public void logCreateSmallFile(SmallFile info) {
        logEdit(OperationType.OP_CREATE_SMALL_FILE, info);
    }

    public void logDropSmallFile(SmallFile info) {
        logEdit(OperationType.OP_DROP_SMALL_FILE, info);
    }

    public void logAlterJob(AlterJobV2 alterJob) {
        logEdit(OperationType.OP_ALTER_JOB_V2, alterJob);
    }

    public Future<Boolean> logAlterJobNoWait(AlterJobV2 alterJob) {
        return submitLog(OperationType.OP_ALTER_JOB_V2, alterJob, -1);
    }

    public void logBatchAlterJob(BatchAlterJobPersistInfo batchAlterJobV2) {
        logEdit(OperationType.OP_BATCH_ADD_ROLLUP, batchAlterJobV2);
    }

    public void logModifyDistributionType(TableInfo tableInfo) {
        logEdit(OperationType.OP_MODIFY_DISTRIBUTION_TYPE, tableInfo);
    }

    public void logDynamicPartition(ModifyTablePropertyOperationLog info) {
        logEdit(OperationType.OP_DYNAMIC_PARTITION, info);
    }

    public void logModifyReplicationNum(ModifyTablePropertyOperationLog info) {
        logEdit(OperationType.OP_MODIFY_REPLICATION_NUM, info);
    }

    public void logModifyInMemory(ModifyTablePropertyOperationLog info) {
        logEdit(OperationType.OP_MODIFY_IN_MEMORY, info);
    }

    public void logModifyConstraint(ModifyTablePropertyOperationLog info) {
        logEdit(OperationType.OP_MODIFY_TABLE_CONSTRAINT_PROPERTY, info);
    }

    public void logModifyEnablePersistentIndex(ModifyTablePropertyOperationLog info) {
        logEdit(OperationType.OP_MODIFY_ENABLE_PERSISTENT_INDEX, info);
    }

    public void logModifyWriteQuorum(ModifyTablePropertyOperationLog info) {
        logEdit(OperationType.OP_MODIFY_WRITE_QUORUM, info);
    }

    public void logModifyReplicatedStorage(ModifyTablePropertyOperationLog info) {
        logEdit(OperationType.OP_MODIFY_REPLICATED_STORAGE, info);
    }

    public void logReplaceTempPartition(ReplacePartitionOperationLog info) {
        logEdit(OperationType.OP_REPLACE_TEMP_PARTITION, info);
    }

    public void logInstallPlugin(PluginInfo plugin) {
        logEdit(OperationType.OP_INSTALL_PLUGIN, plugin);
    }

    public void logUninstallPlugin(PluginInfo plugin) {
        logEdit(OperationType.OP_UNINSTALL_PLUGIN, plugin);
    }

    public void logSetReplicaStatus(SetReplicaStatusOperationLog log) {
        logEdit(OperationType.OP_SET_REPLICA_STATUS, log);
    }

    public void logRemoveExpiredAlterJobV2(RemoveAlterJobV2OperationLog log) {
        logEdit(OperationType.OP_REMOVE_ALTER_JOB_V2, log);
    }

    public void logAlterRoutineLoadJob(AlterRoutineLoadJobOperationLog log) {
        logEdit(OperationType.OP_ALTER_ROUTINE_LOAD_JOB, log);
    }

    public void logAlterLoadJob(AlterLoadJobOperationLog log) {
        logEdit(OperationType.OP_ALTER_LOAD_JOB, log);
    }

    public void logGlobalVariableV2(GlobalVarPersistInfo info) {
        logEdit(OperationType.OP_GLOBAL_VARIABLE_V2, info);
    }

    public void logSwapTable(SwapTableOperationLog log) {
        logEdit(OperationType.OP_SWAP_TABLE, log);
    }

    public void logAddAnalyzeJob(AnalyzeJob job) {
        logEdit(OperationType.OP_ADD_ANALYZER_JOB, job);
    }

    public void logRemoveAnalyzeJob(AnalyzeJob job) {
        logEdit(OperationType.OP_REMOVE_ANALYZER_JOB, job);
    }

    public void logAddAnalyzeStatus(AnalyzeStatus status) {
        logEdit(OperationType.OP_ADD_ANALYZE_STATUS, status);
    }

    public void logRemoveAnalyzeStatus(AnalyzeStatus status) {
        logEdit(OperationType.OP_REMOVE_ANALYZE_STATUS, status);
    }

    public void logAddBasicStatsMeta(BasicStatsMeta meta) {
        logEdit(OperationType.OP_ADD_BASIC_STATS_META, meta);
    }

    public void logRemoveBasicStatsMeta(BasicStatsMeta meta) {
        logEdit(OperationType.OP_REMOVE_BASIC_STATS_META, meta);
    }

    public void logAddHistogramStatsMeta(HistogramStatsMeta meta) {
        logEdit(OperationType.OP_ADD_HISTOGRAM_STATS_META, meta);
    }

    public void logRemoveHistogramStatsMeta(HistogramStatsMeta meta) {
        logEdit(OperationType.OP_REMOVE_HISTOGRAM_STATS_META, meta);
    }

    public void logModifyTableColumn(ModifyTableColumnOperationLog log) {
        logEdit(OperationType.OP_MODIFY_HIVE_TABLE_COLUMN, log);
    }

    public void logCreateCatalog(Catalog log) {
        logEdit(OperationType.OP_CREATE_CATALOG, log);
    }

    public void logDropCatalog(DropCatalogLog log) {
        logEdit(OperationType.OP_DROP_CATALOG, log);
    }

    public void logCreateInsertOverwrite(CreateInsertOverwriteJobLog info) {
        logEdit(OperationType.OP_CREATE_INSERT_OVERWRITE, info);
    }

    public void logInsertOverwriteStateChange(InsertOverwriteStateChangeInfo info) {
        logEdit(OperationType.OP_INSERT_OVERWRITE_STATE_CHANGE, info);
    }

    public void logAlterMvStatus(AlterMaterializedViewStatusLog log) {
        logEdit(OperationType.OP_ALTER_MATERIALIZED_VIEW_STATUS, log);
    }

    public void logMvRename(RenameMaterializedViewLog log) {
        logEdit(OperationType.OP_RENAME_MATERIALIZED_VIEW, log);
    }

    public void logMvChangeRefreshScheme(ChangeMaterializedViewRefreshSchemeLog log) {
        logEdit(OperationType.OP_CHANGE_MATERIALIZED_VIEW_REFRESH_SCHEME, log);
    }

    public void logAlterMaterializedViewProperties(ModifyTablePropertyOperationLog log) {
        logEdit(OperationType.OP_ALTER_MATERIALIZED_VIEW_PROPERTIES, log);
    }

    public void logStarMgrOperation(StarMgrJournal journal) {
        logEdit(OperationType.OP_STARMGR, journal);
    }

    public void logCreateUser(
            UserIdentity userIdentity,
            UserAuthenticationInfo authenticationInfo,
            UserProperty userProperty,
            UserPrivilegeCollectionV2 privilegeCollection,
            short pluginId,
            short pluginVersion) {
        CreateUserInfo info = new CreateUserInfo(
                userIdentity, authenticationInfo, userProperty, privilegeCollection, pluginId, pluginVersion);
        logEdit(OperationType.OP_CREATE_USER_V2, info);
    }

    public void logAlterUser(UserIdentity userIdentity, UserAuthenticationInfo authenticationInfo) {
        AlterUserInfo info = new AlterUserInfo(userIdentity, authenticationInfo);
        logEdit(OperationType.OP_ALTER_USER_V2, info);
    }

    public void logUpdateUserPropertyV2(UserPropertyInfo propertyInfo) {
        logEdit(OperationType.OP_UPDATE_USER_PROP_V2, propertyInfo);
    }

    public void logDropUser(UserIdentity userIdentity) {
        logEdit(OperationType.OP_DROP_USER_V2, userIdentity);
    }

    public void logUpdateUserPrivilege(
            UserIdentity userIdentity,
            UserPrivilegeCollectionV2 privilegeCollection,
            short pluginId,
            short pluginVersion) {
        UserPrivilegeCollectionInfo info = new UserPrivilegeCollectionInfo(
                userIdentity, privilegeCollection, pluginId, pluginVersion);
        logEdit(OperationType.OP_UPDATE_USER_PRIVILEGE_V2, info);
    }

    public void logUpdateRolePrivilege(
            Map<Long, RolePrivilegeCollectionV2> rolePrivCollectionModified,
            short pluginId,
            short pluginVersion) {
        RolePrivilegeCollectionInfo info = new RolePrivilegeCollectionInfo(rolePrivCollectionModified, pluginId, pluginVersion);
        logUpdateRolePrivilege(info);
    }

    public void logUpdateRolePrivilege(RolePrivilegeCollectionInfo info) {
        logEdit(OperationType.OP_UPDATE_ROLE_PRIVILEGE_V2, info);
    }

    public void logDropRole(
            Map<Long, RolePrivilegeCollectionV2> rolePrivCollectionModified,
            short pluginId,
            short pluginVersion) {
        RolePrivilegeCollectionInfo info = new RolePrivilegeCollectionInfo(rolePrivCollectionModified, pluginId, pluginVersion);
        logEdit(OperationType.OP_DROP_ROLE_V2, info);
    }

    public void logAuthUpgrade(Map<String, Long> roleNameToId) {
        logEdit(OperationType.OP_AUTH_UPGRADE_V2, new AuthUpgradeInfo(roleNameToId));
    }

    public void logModifyBinlogConfig(ModifyTablePropertyOperationLog log) {
        logEdit(OperationType.OP_MODIFY_BINLOG_CONFIG, log);
    }

    public void logModifyBinlogAvailableVersion(ModifyTablePropertyOperationLog log) {
        logEdit(OperationType.OP_MODIFY_BINLOG_AVAILABLE_VERSION, log);
    }

    public void logMVJobState(MVMaintenanceJob job) {
        logEdit(OperationType.OP_MV_JOB_STATE, job);
    }

    public void logMVEpochChange(MVEpoch epoch) {
        logEdit(OperationType.OP_MV_EPOCH_UPDATE, epoch);
    }

    public void logAlterTableProperties(ModifyTablePropertyOperationLog info) {
        logEdit(OperationType.OP_ALTER_TABLE_PROPERTIES, info);
    }

    public void logAlterTask(Task changedTask) {
        logEdit(OperationType.OP_ALTER_TASK, changedTask);
    }

}
