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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/catalog/FakeEditLog.java

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

import com.starrocks.alter.AlterJobV2;
import com.starrocks.alter.BatchAlterJobPersistInfo;
import com.starrocks.authentication.UserPropertyInfo;
import com.starrocks.backup.BackupJob;
import com.starrocks.backup.Repository;
import com.starrocks.backup.RestoreJob;
import com.starrocks.common.util.SmallFileMgr.SmallFile;
import com.starrocks.ha.LeaderInfo;
import com.starrocks.load.ExportJob;
import com.starrocks.load.MultiDeleteInfo;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.load.loadv2.LoadJob.LoadJobStateUpdateInfo;
import com.starrocks.load.loadv2.LoadJobFinalOperation;
import com.starrocks.load.routineload.RoutineLoadJob;
import com.starrocks.load.streamload.StreamLoadMultiStmtTask;
import com.starrocks.load.streamload.StreamLoadTask;
import com.starrocks.persist.AlterCatalogLog;
import com.starrocks.persist.AlterLoadJobOperationLog;
import com.starrocks.persist.AlterPipeLog;
import com.starrocks.persist.AlterResourceGroupLog;
import com.starrocks.persist.AlterResourceInfo;
import com.starrocks.persist.AlterRoutineLoadJobOperationLog;
import com.starrocks.persist.AlterTaskInfo;
import com.starrocks.persist.AlterUserInfo;
import com.starrocks.persist.AlterViewInfo;
import com.starrocks.persist.AutoIncrementInfo;
import com.starrocks.persist.BackendTabletsInfo;
import com.starrocks.persist.BatchDeleteReplicaInfo;
import com.starrocks.persist.CancelDecommissionDiskInfo;
import com.starrocks.persist.ClusterSnapshotLog;
import com.starrocks.persist.ColumnRenameInfo;
import com.starrocks.persist.CreateUserInfo;
import com.starrocks.persist.DatabaseInfo;
import com.starrocks.persist.DecommissionDiskInfo;
import com.starrocks.persist.DeleteSqlBlackLists;
import com.starrocks.persist.DeleteSqlDigestBlackLists;
import com.starrocks.persist.DisableDiskInfo;
import com.starrocks.persist.DisablePartitionRecoveryInfo;
import com.starrocks.persist.DisableTableRecoveryInfo;
import com.starrocks.persist.DropBackendInfo;
import com.starrocks.persist.DropBrokerLog;
import com.starrocks.persist.DropCatalogLog;
import com.starrocks.persist.DropComputeNodeLog;
import com.starrocks.persist.DropDictionaryInfo;
import com.starrocks.persist.DropFrontendInfo;
import com.starrocks.persist.DropPartitionsInfo;
import com.starrocks.persist.DropRepositoryLog;
import com.starrocks.persist.DropResourceOperationLog;
import com.starrocks.persist.DropStorageVolumeLog;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.EraseDbLog;
import com.starrocks.persist.ErasePartitionLog;
import com.starrocks.persist.GlobalVarPersistInfo;
import com.starrocks.persist.GroupProviderLog;
import com.starrocks.persist.InsertOverwriteStateChangeInfo;
import com.starrocks.persist.ModifyBrokerInfo;
import com.starrocks.persist.ModifyColumnCommentLog;
import com.starrocks.persist.ModifyPartitionInfo;
import com.starrocks.persist.ModifyTableColumnOperationLog;
import com.starrocks.persist.ModifyTablePropertyOperationLog;
import com.starrocks.persist.MultiEraseTableInfo;
import com.starrocks.persist.NextIdLog;
import com.starrocks.persist.PartitionVersionRecoveryInfo;
import com.starrocks.persist.PipeOpEntry;
import com.starrocks.persist.RecoverInfo;
import com.starrocks.persist.RemoveAlterJobV2OperationLog;
import com.starrocks.persist.RemoveSmallFileLog;
import com.starrocks.persist.RemoveTabletReshardJobLog;
import com.starrocks.persist.RenameMaterializedViewLog;
import com.starrocks.persist.ReplicaPersistInfo;
import com.starrocks.persist.ReplicationJobLog;
import com.starrocks.persist.ResourceGroupOpEntry;
import com.starrocks.persist.RolePrivilegeCollectionInfo;
import com.starrocks.persist.RoutineLoadOperation;
import com.starrocks.persist.SecurityIntegrationPersistInfo;
import com.starrocks.persist.SetDefaultStorageVolumeLog;
import com.starrocks.persist.SetReplicaStatusOperationLog;
import com.starrocks.persist.SqlBlackListPersistInfo;
import com.starrocks.persist.SqlDigestBlackListPersistInfo;
import com.starrocks.persist.TableInfo;
import com.starrocks.persist.TableStorageInfos;
import com.starrocks.persist.TransactionIdInfo;
import com.starrocks.persist.TruncateTableInfo;
import com.starrocks.persist.UninstallPluginLog;
import com.starrocks.persist.UpdateBackendInfo;
import com.starrocks.persist.UpdateDictionaryMgrLog;
import com.starrocks.persist.UpdateFrontendInfo;
import com.starrocks.persist.UpdateHistoricalNodeLog;
import com.starrocks.persist.UserPrivilegeCollectionInfo;
import com.starrocks.persist.WALApplier;
import com.starrocks.plugin.PluginInfo;
import com.starrocks.proto.EncryptionKeyPB;
import com.starrocks.replication.ReplicationJob;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.mv.MVMaintenanceJob;
import com.starrocks.scheduler.persist.ArchiveTaskRunsLog;
import com.starrocks.scheduler.persist.DropTasksLog;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.scheduler.persist.TaskRunStatusChange;
import com.starrocks.sql.spm.BaselinePlan;
import com.starrocks.statistic.AnalyzeJob;
import com.starrocks.statistic.AnalyzeStatus;
import com.starrocks.statistic.BasicStatsMeta;
import com.starrocks.statistic.BatchRemoveBasicStatsMetaLog;
import com.starrocks.statistic.BatchRemoveHistogramStatsMetaLog;
import com.starrocks.statistic.BatchRemoveMultiColumnStatsMetaLog;
import com.starrocks.statistic.ExternalBasicStatsMeta;
import com.starrocks.statistic.ExternalHistogramStatsMeta;
import com.starrocks.statistic.HistogramStatsMeta;
import com.starrocks.statistic.MultiColumnStatsMeta;
import com.starrocks.storagevolume.StorageVolume;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.Frontend;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStateBatch;
import mockit.Mock;
import mockit.MockUp;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FakeEditLog extends MockUp<EditLog> {

    private Map<Long, TransactionState> allTransactionState = new HashMap<>();

    @Mock
    public void init(String nodeName) {
    }

    @Mock
    public void logInsertTransactionState(TransactionState transactionState) {
        allTransactionState.put(transactionState.getTransactionId(), transactionState);
    }

    @Mock
    public void logInsertTransactionStateBatch(TransactionStateBatch stateBatch) {
        for (TransactionState transactionState : stateBatch.getTransactionStates()) {
            allTransactionState.put(transactionState.getTransactionId(), transactionState);
        }
    }

    @Mock
    public void logDeleteTransactionState(TransactionState transactionState) {
        allTransactionState.remove(transactionState.getTransactionId());
    }

    @Mock
    public void logJsonObject(short op, Object obj, WALApplier walApplier) {
        apply(walApplier, obj);
    }

    @Mock
    public void logSaveNextId(long nextId, WALApplier walApplier) {
        apply(walApplier, new NextIdLog(nextId));
    }

    @Mock
    public void logSaveTransactionId(long transactionId, WALApplier walApplier) {
        apply(walApplier, new TransactionIdInfo(transactionId));
    }

    @Mock
    public void logSaveAutoIncrementId(AutoIncrementInfo info, WALApplier walApplier) {
        apply(walApplier, info);
    }

    @Mock
    public void logSaveDeleteAutoIncrementId(AutoIncrementInfo info, WALApplier walApplier) {
        apply(walApplier, info);
    }

    @Mock
    public void logCreateCluster() {
    }

    @Mock
    public void logEraseDb(long dbId, WALApplier walApplier) {
        apply(walApplier, new EraseDbLog(dbId));
    }

    @Mock
    public void logAlterDb(DatabaseInfo dbInfo, WALApplier walApplier) {
        apply(walApplier, dbInfo);
    }

    @Mock
    public void logResourceGroupOp(ResourceGroupOpEntry op, WALApplier applier) {
        apply(applier, op);
    }

    @Mock
    public void logAlterResourceGroup(AlterResourceGroupLog log, WALApplier applier) {
        apply(applier, log);
    }

    @Mock
    public void logCreateTask(Task info, WALApplier applier) {
        apply(applier, info);
    }

    @Mock
    public void logDropTasks(DropTasksLog tasksLog, WALApplier applier) {
        apply(applier, tasksLog);
    }

    @Mock
    public void logTaskRunCreateStatus(TaskRunStatus status, WALApplier applier) {
        apply(applier, status);
    }

    @Mock
    public void logUpdateTaskRun(TaskRunStatusChange statusChange, WALApplier applier) {
        apply(applier, statusChange);
    }

    @Mock
    public void logArchiveTaskRuns(ArchiveTaskRunsLog log, WALApplier applier) {
        apply(applier, log);
    }

    @Mock
    public void logDropPartitions(DropPartitionsInfo info, WALApplier walApplier) {
        apply(walApplier, info);
    }

    @Mock
    public void logErasePartition(long partitionId, WALApplier walApplier) {
        apply(walApplier, new ErasePartitionLog(partitionId));
    }

    @Mock
    public void logRecoverPartition(RecoverInfo info, WALApplier walApplier) {
        apply(walApplier, info);
    }

    @Mock
    public void logModifyPartition(ModifyPartitionInfo info, WALApplier walApplier) {
        apply(walApplier, info);
    }

    @Mock
    public void logDisablePartitionRecovery(long partitionId, WALApplier walApplier) {
        apply(walApplier, new DisablePartitionRecoveryInfo(partitionId));
    }

    @Mock
    public void logDisableTableRecovery(List<Long> tableIds, WALApplier walApplier) {
        apply(walApplier, new DisableTableRecoveryInfo(tableIds));
    }

    @Mock
    public void logEraseMultiTables(List<Long> tableIds, WALApplier walApplier) {
        apply(walApplier, new MultiEraseTableInfo(tableIds));
    }

    @Mock
    public void logRecoverTable(RecoverInfo info, WALApplier walApplier) {
        apply(walApplier, info);
    }

    @Mock
    public void logBackendStateChange(UpdateBackendInfo info, WALApplier applier) {
        apply(applier, info);
    }

    @Mock
    public void logAlterJob(AlterJobV2 alterJob, WALApplier applier) {
        apply(applier, alterJob);
    }

    @Mock
    public void logBatchAlterJob(BatchAlterJobPersistInfo batchAlterJobV2, WALApplier walApplier) {
        apply(walApplier, batchAlterJobV2);
    }

    @Mock
    public void logDynamicPartition(ModifyTablePropertyOperationLog info, WALApplier walApplier) {
        apply(walApplier, info);
    }

    @Mock
    public void logModifyReplicationNum(ModifyTablePropertyOperationLog info, WALApplier walApplier) {
        apply(walApplier, info);
    }

    @Mock
    public void logModifyConstraint(ModifyTablePropertyOperationLog info, WALApplier walApplier) {
        apply(walApplier, info);
    }

    @Mock
    public void logModifyEnablePersistentIndex(ModifyTablePropertyOperationLog info, WALApplier walApplier) {
        apply(walApplier, info);
    }

    @Mock
    public void logModifyPrimaryIndexCacheExpireSec(ModifyTablePropertyOperationLog info, WALApplier walApplier) {
        apply(walApplier, info);
    }

    @Mock
    public void logModifyWriteQuorum(ModifyTablePropertyOperationLog info, WALApplier walApplier) {
        apply(walApplier, info);
    }

    @Mock
    public void logModifyReplicatedStorage(ModifyTablePropertyOperationLog info, WALApplier walApplier) {
        apply(walApplier, info);
    }

    @Mock
    public void logModifyBucketSize(ModifyTablePropertyOperationLog info, WALApplier walApplier) {
        apply(walApplier, info);
    }

    @Mock
    public void logModifyMutableBucketNum(ModifyTablePropertyOperationLog info, WALApplier walApplier) {
        apply(walApplier, info);
    }

    @Mock
    public void logModifyDefaultBucketNum(ModifyTablePropertyOperationLog info, WALApplier walApplier) {
        apply(walApplier, info);
    }

    @Mock
    public void logModifyEnableLoadProfile(ModifyTablePropertyOperationLog info, WALApplier walApplier) {
        apply(walApplier, info);
    }

    @Mock
    public void logModifyBaseCompactionForbiddenTimeRanges(ModifyTablePropertyOperationLog info, WALApplier walApplier) {
        apply(walApplier, info);
    }

    @Mock
    public void logAddReplica(ReplicaPersistInfo info) {
    }

    @Mock
    public void logBackupJob(BackupJob job, WALApplier walApplier) {
        apply(walApplier, job);
    }

    @Mock
    public void logCreateRepository(Repository repo, WALApplier walApplier) {
        apply(walApplier, repo);
    }

    @Mock
    public void logDropRepository(String repoName, WALApplier walApplier) {
        apply(walApplier, new DropRepositoryLog(repoName));
    }

    @Mock
    public void logRestoreJob(RestoreJob job, WALApplier walApplier) {
        apply(walApplier, job);
    }

    @Mock
    public void logTruncateTable(TruncateTableInfo info, WALApplier walApplier) {
        apply(walApplier, info);
    }

    @Mock
    public void logAddBackend(Backend be, WALApplier applier) {
        apply(applier, be);
    }

    @Mock
    public void logAddComputeNode(ComputeNode computeNode, WALApplier applier) {
        apply(applier, computeNode);
    }

    @Mock
    public void logDropBackend(DropBackendInfo info, WALApplier applier) {
        apply(applier, info);
    }

    @Mock
    public void logDropComputeNode(DropComputeNodeLog log, WALApplier applier) {
        apply(applier, log);
    }

    @Mock
    public void logUpdateHistoricalNode(UpdateHistoricalNodeLog log, WALApplier applier) {
        apply(applier, log);
    }

    @Mock
    public void logAddFrontend(Frontend fe, WALApplier applier) {
        apply(applier, fe);
    }

    @Mock
    public void logRemoveFrontend(DropFrontendInfo dropFrontendInfo, WALApplier applier) {
        apply(applier, dropFrontendInfo);
    }

    @Mock
    public void logUpdateFrontend(UpdateFrontendInfo info, WALApplier applier) {
        apply(applier, info);
    }

    @Mock
    public void logFinishMultiDelete(MultiDeleteInfo info, WALApplier walApplier) {
        apply(walApplier, info);
    }

    @Mock
    public void logDeleteReplica(ReplicaPersistInfo info, WALApplier walApplier) {
        apply(walApplier, info);
    }

    @Mock
    public void logBatchDeleteReplica(BatchDeleteReplicaInfo info, WALApplier walApplier) {
        apply(walApplier, info);
    }

    @Mock
    public void logAddKey(EncryptionKeyPB key, WALApplier walApplier) {
        apply(walApplier, key);
    }

    @Mock
    public void logLeaderInfo(LeaderInfo info, WALApplier walApplier) {
        apply(walApplier, info);
    }

    @Mock
    public void logResetFrontends(Frontend frontend, WALApplier walApplier) {
        apply(walApplier, frontend);
    }

    @Mock
    public void logDatabaseRename(DatabaseInfo databaseInfo, WALApplier walApplier) {
        apply(walApplier, databaseInfo);
    }

    @Mock
    public void logTableRename(TableInfo tableInfo, WALApplier walApplier) {
        apply(walApplier, tableInfo);
    }

    @Mock
    public void logModifyViewDef(AlterViewInfo alterViewInfo, WALApplier walApplier) {
        apply(walApplier, alterViewInfo);
    }

    @Mock
    public void logRollupRename(TableInfo tableInfo, WALApplier walApplier) {
        apply(walApplier, tableInfo);
    }

    @Mock
    public void logPartitionRename(TableInfo tableInfo, WALApplier walApplier) {
        apply(walApplier, tableInfo);
    }

    @Mock
    public void logAddBroker(ModifyBrokerInfo info, WALApplier applier) {
        apply(applier, info);
    }

    @Mock
    public void logDropBroker(ModifyBrokerInfo info, WALApplier applier) {
        apply(applier, info);
    }

    @Mock
    public void logDropAllBroker(String brokerName, WALApplier applier) {
        apply(applier, new DropBrokerLog(brokerName));
    }

    @Mock
    public void logExportCreate(ExportJob job, WALApplier walApplier) {
        apply(walApplier, job);
    }

    @Mock
    public void logExportUpdateState(ExportJob.ExportUpdateInfo updateInfo, WALApplier applier) {
        apply(applier, updateInfo);
    }

    @Mock
    public void logAddFunction(Function function, WALApplier walApplier) {
        apply(walApplier, function);
    }

    @Mock
    public void logDropFunction(FunctionSearchDesc function, WALApplier walApplier) {
        apply(walApplier, function);
    }

    @Mock
    public void logSetHasForbiddenGlobalDict(ModifyTablePropertyOperationLog info, WALApplier walApplier) {
        apply(walApplier, info);
    }

    @Mock
    public void logSetHasDelete(ModifyTablePropertyOperationLog info, WALApplier walApplier) {
        apply(walApplier, info);
    }

    @Mock
    public void logBackendTabletsInfo(BackendTabletsInfo backendTabletsInfo, WALApplier walApplier) {
        apply(walApplier, backendTabletsInfo);
    }

    @Mock
    public void logCreateRoutineLoadJob(RoutineLoadJob routineLoadJob, WALApplier walApplier) {
        apply(walApplier, routineLoadJob);
    }

    @Mock
    public void logOpRoutineLoadJob(RoutineLoadOperation routineLoadOperation, WALApplier walApplier) {
        apply(walApplier, routineLoadOperation);
    }

    @Mock
    public void logCreateStreamLoadJob(StreamLoadTask streamLoadTask, WALApplier walApplier) {
        apply(walApplier, streamLoadTask);
    }

    @Mock
    public void logCreateMultiStmtStreamLoadJob(StreamLoadMultiStmtTask streamLoadTask, WALApplier walApplier) {
        apply(walApplier, streamLoadTask);
    }

    @Mock
    public void logCreateLoadJob(LoadJob loadJob, WALApplier walApplier) {
        apply(walApplier, loadJob);
    }

    @Mock
    public void logEndLoadJob(LoadJobFinalOperation loadJobFinalOperation, WALApplier walApplier) {
        apply(walApplier, loadJobFinalOperation);
    }

    @Mock
    public void logUpdateLoadJob(LoadJobStateUpdateInfo info, WALApplier walApplier) {
        apply(walApplier, info);
    }

    @Mock
    public void logCreateResource(Resource resource, WALApplier walApplier) {
        apply(walApplier, resource);
    }

    @Mock
    public void logAlterResource(AlterResourceInfo info, WALApplier walApplier) {
        apply(walApplier, info);
    }

    @Mock
    public void logDropResource(DropResourceOperationLog operationLog, WALApplier walApplier) {
        apply(walApplier, operationLog);
    }

    @Mock
    public void logCreateSmallFile(SmallFile info, WALApplier walApplier) {
        apply(walApplier, info);
    }

    @Mock
    public void logDropSmallFile(RemoveSmallFileLog log, WALApplier walApplier) {
        apply(walApplier, log);
    }

    @Mock
    public void logInstallPlugin(PluginInfo plugin, WALApplier walApplier) {
        apply(walApplier, plugin);
    }

    @Mock
    public void logUninstallPlugin(UninstallPluginLog log, WALApplier walApplier) {
        apply(walApplier, log);
    }

    @Mock
    public void logSetReplicaStatus(SetReplicaStatusOperationLog log, WALApplier walApplier) {
        apply(walApplier, log);
    }

    @Mock
    public void logRemoveExpiredAlterJobV2(RemoveAlterJobV2OperationLog log, WALApplier walApplier) {
        apply(walApplier, log);
    }

    @Mock
    public void logAlterRoutineLoadJob(AlterRoutineLoadJobOperationLog log, WALApplier walApplier) {
        apply(walApplier, log);
    }

    @Mock
    public void logAlterLoadJob(AlterLoadJobOperationLog log, WALApplier walApplier) {
        apply(walApplier, log);
    }

    @Mock
    public void logGlobalVariableV2(GlobalVarPersistInfo info, WALApplier walApplier) {
        apply(walApplier, info);
    }

    @Mock
    public void logAddAnalyzeJob(AnalyzeJob job, WALApplier walApplier) {
        apply(walApplier, job);
    }

    @Mock
    public void logRemoveAnalyzeJob(AnalyzeJob job, WALApplier walApplier) {
        apply(walApplier, job);
    }

    @Mock
    public void logAddAnalyzeStatus(AnalyzeStatus status, WALApplier walApplier) {
        apply(walApplier, status);
    }

    @Mock
    public void logRemoveAnalyzeStatus(AnalyzeStatus status, WALApplier walApplier) {
        apply(walApplier, status);
    }

    @Mock
    public void logAddBasicStatsMeta(BasicStatsMeta meta, WALApplier walApplier) {
        apply(walApplier, meta);
    }

    @Mock
    public void logRemoveBasicStatsMeta(BasicStatsMeta meta, WALApplier walApplier) {
        apply(walApplier, meta);
    }

    @Mock
    public void logRemoveBasicStatsMetaBatch(List<BasicStatsMeta> metas, WALApplier walApplier) {
        apply(walApplier, new BatchRemoveBasicStatsMetaLog(metas));
    }

    @Mock
    public void logAddHistogramStatsMeta(HistogramStatsMeta meta, WALApplier walApplier) {
        apply(walApplier, meta);
    }

    @Mock
    public void logRemoveHistogramStatsMeta(HistogramStatsMeta meta, WALApplier walApplier) {
        apply(walApplier, meta);
    }

    @Mock
    public void logRemoveHistogramStatsMetaBatch(List<HistogramStatsMeta> metas, WALApplier walApplier) {
        apply(walApplier, new BatchRemoveHistogramStatsMetaLog(metas));
    }

    @Mock
    public void logAddMultiColumnStatsMeta(MultiColumnStatsMeta meta, WALApplier walApplier) {
        apply(walApplier, meta);
    }

    @Mock
    public void logRemoveMultiColumnStatsMeta(MultiColumnStatsMeta meta, WALApplier walApplier) {
        apply(walApplier, meta);
    }

    @Mock
    public void logRemoveMultiColumnStatsMetaBatch(List<MultiColumnStatsMeta> metas, WALApplier walApplier) {
        apply(walApplier, new BatchRemoveMultiColumnStatsMetaLog(metas));
    }

    @Mock
    public void logAddExternalBasicStatsMeta(ExternalBasicStatsMeta meta, WALApplier walApplier) {
        apply(walApplier, meta);
    }

    @Mock
    public void logRemoveExternalBasicStatsMeta(ExternalBasicStatsMeta meta, WALApplier walApplier) {
        apply(walApplier, meta);
    }

    @Mock
    public void logAddExternalHistogramStatsMeta(ExternalHistogramStatsMeta meta, WALApplier walApplier) {
        apply(walApplier, meta);
    }

    @Mock
    public void logRemoveExternalHistogramStatsMeta(ExternalHistogramStatsMeta meta, WALApplier walApplier) {
        apply(walApplier, meta);
    }

    @Mock
    public void logModifyTableColumn(ModifyTableColumnOperationLog log, WALApplier walApplier) {
        apply(walApplier, log);
    }

    @Mock
    public void logModifyColumnComment(ModifyColumnCommentLog log, WALApplier walApplier) {
        apply(walApplier, log);
    }

    @Mock
    public void logCreateCatalog(Catalog log, WALApplier walApplier) {
        apply(walApplier, log);
    }

    @Mock
    public void logDropCatalog(DropCatalogLog log, WALApplier walApplier) {
        apply(walApplier, log);
    }

    @Mock
    public void logAlterCatalog(AlterCatalogLog log, WALApplier walApplier) {
        apply(walApplier, log);
    }

    @Mock
    public void logInsertOverwriteStateChange(InsertOverwriteStateChangeInfo info, WALApplier walApplier) {
        apply(walApplier, info);
    }

    @Mock
    public void logMvRename(RenameMaterializedViewLog log, WALApplier walApplier) {
        apply(walApplier, log);
    }

    @Mock
    public void logAlterMaterializedViewProperties(ModifyTablePropertyOperationLog log, WALApplier walApplier) {
        apply(walApplier, log);
    }

    @Mock
    public void logAddSQLBlackList(SqlBlackListPersistInfo addBlackList, WALApplier walApplier) {
        apply(walApplier, addBlackList);
    }

    @Mock
    public void logDeleteSQLBlackList(DeleteSqlBlackLists deleteBlacklists, WALApplier walApplier) {
        apply(walApplier, deleteBlacklists);
    }

    @Mock
    public void logAddSqlDigestBlackList(SqlDigestBlackListPersistInfo addBlackList, WALApplier walApplier) {
        apply(walApplier, addBlackList);
    }

    @Mock
    public void logDeleteSqlDigestBlackList(DeleteSqlDigestBlackLists deleteBlacklists, WALApplier walApplier) {
        apply(walApplier, deleteBlacklists);
    }

    @Mock
    public void logCreateUser(CreateUserInfo info, WALApplier walApplier) {
        apply(walApplier, info);
    }

    @Mock
    public void logAlterUser(AlterUserInfo info, WALApplier walApplier) {
        apply(walApplier, info);
    }

    @Mock
    public void logUpdateUserPropertyV2(UserPropertyInfo propertyInfo, WALApplier walApplier) {
        apply(walApplier, propertyInfo);
    }

    @Mock
    public void logDropUser(UserIdentity userIdentity, WALApplier walApplier) {
        apply(walApplier, userIdentity);
    }

    @Mock
    public void logUpdateUserPrivilege(UserPrivilegeCollectionInfo info, WALApplier walApplier) {
        apply(walApplier, info);
    }

    @Mock
    public void logUpdateRolePrivilege(RolePrivilegeCollectionInfo info, WALApplier walApplier) {
        apply(walApplier, info);
    }

    @Mock
    public void logDropRole(RolePrivilegeCollectionInfo info, WALApplier walApplier) {
        apply(walApplier, info);
    }

    @Mock
    public void logCreateSecurityIntegration(SecurityIntegrationPersistInfo info, WALApplier walApplier) {
        apply(walApplier, info);
    }

    @Mock
    public void logAlterSecurityIntegration(SecurityIntegrationPersistInfo info, WALApplier walApplier) {
        apply(walApplier, info);
    }

    @Mock
    public void logDropSecurityIntegration(SecurityIntegrationPersistInfo info, WALApplier walApplier) {
        apply(walApplier, info);
    }

    @Mock
    public void logModifyBinlogConfig(ModifyTablePropertyOperationLog log, WALApplier walApplier) {
        apply(walApplier, log);
    }

    @Mock
    public void logModifyFlatJsonConfig(ModifyTablePropertyOperationLog log, WALApplier walApplier) {
        apply(walApplier, log);
    }

    @Mock
    public void logModifyBinlogAvailableVersion(ModifyTablePropertyOperationLog log, WALApplier walApplier) {
        apply(walApplier, log);
    }

    @Mock
    public void logMVJobState(MVMaintenanceJob job, WALApplier walApplier) {
        apply(walApplier, job);
    }

    @Mock
    public void logAlterTableProperties(ModifyTablePropertyOperationLog info, WALApplier walApplier) {
        apply(walApplier, info);
    }

    @Mock
    public void logPipeOp(PipeOpEntry opEntry, WALApplier walApplier) {
        apply(walApplier, opEntry);
    }

    @Mock
    public void logAlterPipe(AlterPipeLog log, WALApplier walApplier) {
        apply(walApplier, log);
    }

    @Mock
    public void logAlterTask(AlterTaskInfo info, WALApplier applier) {
        apply(applier, info);
    }

    @Mock
    public void logSetDefaultStorageVolume(SetDefaultStorageVolumeLog log, WALApplier walApplier) {
        apply(walApplier, log);
    }

    @Mock
    public void logCreateStorageVolume(StorageVolume storageVolume, WALApplier walApplier) {
        apply(walApplier, storageVolume);
    }

    @Mock
    public void logUpdateStorageVolume(StorageVolume storageVolume, WALApplier walApplier) {
        apply(walApplier, storageVolume);
    }

    @Mock
    public void logDropStorageVolume(DropStorageVolumeLog log, WALApplier walApplier) {
        apply(walApplier, log);
    }

    @Mock
    public void logUpdateTableStorageInfos(TableStorageInfos tableStorageInfos, WALApplier walApplier) {
        apply(walApplier, tableStorageInfos);
    }

    @Mock
    public void logReplicationJob(ReplicationJob replicationJob, WALApplier walApplier) {
        apply(walApplier, new ReplicationJobLog(replicationJob));
    }

    @Mock
    public void logDeleteReplicationJob(ReplicationJob replicationJob, WALApplier walApplier) {
        apply(walApplier, new ReplicationJobLog(replicationJob));
    }

    @Mock
    public void logColumnRename(ColumnRenameInfo columnRenameInfo, WALApplier walApplier) {
        apply(walApplier, columnRenameInfo);
    }

    @Mock
    public void logCreateDictionary(Dictionary info, WALApplier walApplier) {
        apply(walApplier, info);
    }

    @Mock
    public void logDropDictionary(DropDictionaryInfo info, WALApplier walApplier) {
        apply(walApplier, info);
    }

    @Mock
    public void logModifyDictionaryMgr(UpdateDictionaryMgrLog info, WALApplier walApplier) {
        apply(walApplier, info);
    }

    @Mock
    public void logDecommissionDisk(DecommissionDiskInfo info, WALApplier applier) {
        apply(applier, info);
    }

    @Mock
    public void logCancelDecommissionDisk(CancelDecommissionDiskInfo info, WALApplier applier) {
        apply(applier, info);
    }

    @Mock
    public void logDisableDisk(DisableDiskInfo info, WALApplier applier) {
        apply(applier, info);
    }

    @Mock
    public void logRecoverPartitionVersion(PartitionVersionRecoveryInfo info, WALApplier walApplier) {
        apply(walApplier, info);
    }

    @Mock
    public void logClusterSnapshotLog(ClusterSnapshotLog info, WALApplier walApplier) {
        apply(walApplier, info);
    }

    @Mock
    public void logCreateSPMBaseline(BaselinePlan.Info info, WALApplier walApplier) {
        apply(walApplier, info);
    }

    @Mock
    public void logDropSPMBaseline(BaselinePlan.Info info, WALApplier walApplier) {
        apply(walApplier, info);
    }

    @Mock
    public void logUpdateSPMBaseline(BaselinePlan.Info info, boolean isEnable, WALApplier walApplier) {
        apply(walApplier, info);
    }

    @Mock
    public void logRemoveTabletReshardJob(long jobId, WALApplier walApplier) {
        apply(walApplier, new RemoveTabletReshardJobLog(jobId));
    }

    @Mock
    public void logCreateGroupProvider(GroupProviderLog provider, WALApplier walApplier) {
        apply(walApplier, provider);
    }

    @Mock
    public void logDropGroupProvider(GroupProviderLog groupProviderLog, WALApplier walApplier) {
        apply(walApplier, groupProviderLog);
    }

    public TransactionState getTransaction(long transactionId) {
        return allTransactionState.get(transactionId);
    }

    private void apply(WALApplier walApplier, Object wal) {
        if (walApplier != null) {
            walApplier.apply(wal);
        }
    }
}
