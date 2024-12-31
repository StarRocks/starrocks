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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/journal/JournalEntity.java

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

package com.starrocks.journal;

import com.starrocks.alter.AlterJobV2;
import com.starrocks.alter.BatchAlterJobPersistInfo;
import com.starrocks.authentication.UserPropertyInfo;
import com.starrocks.backup.AbstractJob;
import com.starrocks.backup.Repository;
import com.starrocks.catalog.BrokerMgr;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Dictionary;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSearchDesc;
import com.starrocks.catalog.MetaVersion;
import com.starrocks.catalog.Resource;
import com.starrocks.common.Config;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.SmallFileMgr.SmallFile;
import com.starrocks.ha.LeaderInfo;
import com.starrocks.journal.bdbje.Timestamp;
import com.starrocks.load.ExportJob;
import com.starrocks.load.MultiDeleteInfo;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.load.loadv2.LoadJob.LoadJobStateUpdateInfo;
import com.starrocks.load.loadv2.LoadJobFinalOperation;
import com.starrocks.load.routineload.RoutineLoadJob;
import com.starrocks.load.streamload.StreamLoadTask;
import com.starrocks.persist.AddPartitionsInfoV2;
import com.starrocks.persist.AddSubPartitionsInfoV2;
import com.starrocks.persist.AlterCatalogLog;
import com.starrocks.persist.AlterLoadJobOperationLog;
import com.starrocks.persist.AlterMaterializedViewBaseTableInfosLog;
import com.starrocks.persist.AlterMaterializedViewStatusLog;
import com.starrocks.persist.AlterRoutineLoadJobOperationLog;
import com.starrocks.persist.AlterUserInfo;
import com.starrocks.persist.AlterViewInfo;
import com.starrocks.persist.AutoIncrementInfo;
import com.starrocks.persist.BackendTabletsInfo;
import com.starrocks.persist.BatchDeleteReplicaInfo;
import com.starrocks.persist.BatchDropInfo;
import com.starrocks.persist.BatchModifyPartitionsInfo;
import com.starrocks.persist.CancelDecommissionDiskInfo;
import com.starrocks.persist.CancelDisableDiskInfo;
import com.starrocks.persist.ChangeMaterializedViewRefreshSchemeLog;
import com.starrocks.persist.ClusterSnapshotLog;
import com.starrocks.persist.ColocatePersistInfo;
import com.starrocks.persist.ColumnRenameInfo;
import com.starrocks.persist.ConsistencyCheckInfo;
import com.starrocks.persist.CreateDbInfo;
import com.starrocks.persist.CreateInsertOverwriteJobLog;
import com.starrocks.persist.CreateTableInfo;
import com.starrocks.persist.CreateUserInfo;
import com.starrocks.persist.DatabaseInfo;
import com.starrocks.persist.DecommissionDiskInfo;
import com.starrocks.persist.DictionaryMgrInfo;
import com.starrocks.persist.DisableDiskInfo;
import com.starrocks.persist.DisablePartitionRecoveryInfo;
import com.starrocks.persist.DisableTableRecoveryInfo;
import com.starrocks.persist.DropCatalogLog;
import com.starrocks.persist.DropComputeNodeLog;
import com.starrocks.persist.DropDbInfo;
import com.starrocks.persist.DropDictionaryInfo;
import com.starrocks.persist.DropInfo;
import com.starrocks.persist.DropPartitionInfo;
import com.starrocks.persist.DropPartitionsInfo;
import com.starrocks.persist.DropResourceOperationLog;
import com.starrocks.persist.DropStorageVolumeLog;
import com.starrocks.persist.DropWarehouseLog;
import com.starrocks.persist.GlobalVarPersistInfo;
import com.starrocks.persist.HbPackage;
import com.starrocks.persist.InsertOverwriteStateChangeInfo;
import com.starrocks.persist.ModifyPartitionInfo;
import com.starrocks.persist.ModifyTableColumnOperationLog;
import com.starrocks.persist.ModifyTablePropertyOperationLog;
import com.starrocks.persist.MultiEraseTableInfo;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.PartitionPersistInfoV2;
import com.starrocks.persist.PartitionVersionRecoveryInfo;
import com.starrocks.persist.PipeOpEntry;
import com.starrocks.persist.RecoverInfo;
import com.starrocks.persist.RemoveAlterJobV2OperationLog;
import com.starrocks.persist.RenameMaterializedViewLog;
import com.starrocks.persist.ReplacePartitionOperationLog;
import com.starrocks.persist.ReplicaPersistInfo;
import com.starrocks.persist.ReplicationJobLog;
import com.starrocks.persist.ResourceGroupOpEntry;
import com.starrocks.persist.RolePrivilegeCollectionInfo;
import com.starrocks.persist.RoutineLoadOperation;
import com.starrocks.persist.SetDefaultStorageVolumeLog;
import com.starrocks.persist.SetReplicaStatusOperationLog;
import com.starrocks.persist.SwapTableOperationLog;
import com.starrocks.persist.TableAddOrDropColumnsInfo;
import com.starrocks.persist.TableInfo;
import com.starrocks.persist.TablePropertyInfo;
import com.starrocks.persist.TransactionIdInfo;
import com.starrocks.persist.TruncateTableInfo;
import com.starrocks.persist.UserPrivilegeCollectionInfo;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.plugin.PluginInfo;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.mv.MVEpoch;
import com.starrocks.scheduler.mv.MVMaintenanceJob;
import com.starrocks.scheduler.persist.ArchiveTaskRunsLog;
import com.starrocks.scheduler.persist.DropTasksLog;
import com.starrocks.scheduler.persist.TaskRunPeriodStatusChange;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.scheduler.persist.TaskRunStatusChange;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.staros.StarMgrJournal;
import com.starrocks.statistic.BasicStatsMeta;
import com.starrocks.statistic.ExternalAnalyzeJob;
import com.starrocks.statistic.ExternalAnalyzeStatus;
import com.starrocks.statistic.ExternalBasicStatsMeta;
import com.starrocks.statistic.ExternalHistogramStatsMeta;
import com.starrocks.statistic.HistogramStatsMeta;
import com.starrocks.statistic.NativeAnalyzeJob;
import com.starrocks.statistic.NativeAnalyzeStatus;
import com.starrocks.storagevolume.StorageVolume;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.Frontend;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStateBatch;
import com.starrocks.warehouse.Warehouse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// this is the value written to bdb or local edit files. key is an auto-increasing long.
public class JournalEntity implements Writable {
    public static final Logger LOG = LogManager.getLogger(JournalEntity.class);

    private short opCode = OperationType.OP_INVALID;
    private Writable data;

    public short getOpCode() {
        return this.opCode;
    }

    public void setOpCode(short opCode) {
        this.opCode = opCode;
    }

    public Writable getData() {
        return this.data;
    }

    public void setData(Writable data) {
        this.data = data;
    }

    public String toString() {
        return " opCode=" + opCode + " " + data;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeShort(opCode);
        data.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        opCode = in.readShort();
        LOG.debug("get opcode: {}", opCode);
        switch (opCode) {
            case OperationType.OP_SAVE_NEXTID:
            case OperationType.OP_ERASE_DB:
            case OperationType.OP_ERASE_PARTITION:
            case OperationType.OP_DROP_ALL_BROKER:
            case OperationType.OP_DROP_REPOSITORY: {
                data = new Text();
                ((Text) data).readFields(in);
                break;
            }
            case OperationType.OP_SAVE_TRANSACTION_ID_V2: {
                data = GsonUtils.GSON.fromJson(Text.readString(in), TransactionIdInfo.class);
                break;
            }
            case OperationType.OP_SAVE_AUTO_INCREMENT_ID:
            case OperationType.OP_DELETE_AUTO_INCREMENT_ID: {
                data = new AutoIncrementInfo(null);
                ((AutoIncrementInfo) data).read(in);
                break;
            }
            case OperationType.OP_CREATE_DB_V2: {
                data = CreateDbInfo.read(in);
                break;
            }
            case OperationType.OP_DROP_DB: {
                data = DropDbInfo.read(in);
                break;
            }
            case OperationType.OP_ALTER_DB_V2:
            case OperationType.OP_RENAME_DB_V2: {
                data = GsonUtils.GSON.fromJson(Text.readString(in), DatabaseInfo.class);
                break;
            }
            case OperationType.OP_CREATE_TABLE_V2: {
                data = GsonUtils.GSON.fromJson(Text.readString(in), CreateTableInfo.class);
                break;
            }
            case OperationType.OP_DROP_TABLE_V2:
            case OperationType.OP_DROP_ROLLUP_V2: {
                data = GsonUtils.GSON.fromJson(Text.readString(in), DropInfo.class);
                break;
            }
            case OperationType.OP_ERASE_MULTI_TABLES: {
                data = MultiEraseTableInfo.read(in);
                break;
            }
            case OperationType.OP_DISABLE_TABLE_RECOVERY: {
                data = DisableTableRecoveryInfo.read(in);
                break;
            }
            case OperationType.OP_DISABLE_PARTITION_RECOVERY: {
                data = DisablePartitionRecoveryInfo.read(in);
                break;
            }
            case OperationType.OP_ADD_PARTITION_V2: {
                data = PartitionPersistInfoV2.read(in);
                break;
            }
            case OperationType.OP_ADD_SUB_PARTITIONS_V2: {
                data = AddSubPartitionsInfoV2.read(in);
                break;
            }
            case OperationType.OP_ADD_PARTITIONS_V2: {
                data = AddPartitionsInfoV2.read(in);
                break;
            }
            case OperationType.OP_DROP_PARTITION: {
                data = DropPartitionInfo.read(in);
                break;
            }
            case OperationType.OP_DROP_PARTITIONS: {
                data = DropPartitionsInfo.read(in);
                break;
            }
            case OperationType.OP_MODIFY_PARTITION_V2: {
                data = GsonUtils.GSON.fromJson(Text.readString(in), ModifyPartitionInfo.class);
                break;
            }
            case OperationType.OP_BATCH_MODIFY_PARTITION: {
                data = BatchModifyPartitionsInfo.read(in);
                break;
            }
            case OperationType.OP_RECOVER_DB_V2:
            case OperationType.OP_RECOVER_TABLE_V2:
            case OperationType.OP_RECOVER_PARTITION_V2: {
                data = GsonUtils.GSON.fromJson(Text.readString(in), RecoverInfo.class);
                break;
            }
            case OperationType.OP_BATCH_DROP_ROLLUP: {
                data = BatchDropInfo.read(in);
                break;
            }
            case OperationType.OP_RENAME_TABLE_V2:
            case OperationType.OP_RENAME_ROLLUP_V2:
            case OperationType.OP_RENAME_PARTITION_V2: {
                data = GsonUtils.GSON.fromJson(Text.readString(in), TableInfo.class);
                break;
            }
            case OperationType.OP_RENAME_COLUMN_V2: {
                data = GsonUtils.GSON.fromJson(Text.readString(in), ColumnRenameInfo.class);
                break;
            }
            case OperationType.OP_MODIFY_VIEW_DEF: {
                data = AlterViewInfo.read(in);
                break;
            }
            case OperationType.OP_CHANGE_MATERIALIZED_VIEW_REFRESH_SCHEME:
                data = ChangeMaterializedViewRefreshSchemeLog.read(in);
                break;
            case OperationType.OP_ALTER_MATERIALIZED_VIEW_PROPERTIES:
                data = ModifyTablePropertyOperationLog.read(in);
                break;
            case OperationType.OP_RENAME_MATERIALIZED_VIEW:
                data = RenameMaterializedViewLog.read(in);
                break;
            case OperationType.OP_ALTER_MATERIALIZED_VIEW_STATUS:
                data = AlterMaterializedViewStatusLog.read(in);
                break;
            case OperationType.OP_ALTER_MATERIALIZED_VIEW_BASE_TABLE_INFOS:
                data = AlterMaterializedViewBaseTableInfosLog.read(in);
                break;
            case OperationType.OP_BACKUP_JOB_V2: {
                data = GsonUtils.GSON.fromJson(Text.readString(in), AbstractJob.class);
                break;
            }
            case OperationType.OP_RESTORE_JOB_V2: {
                data = GsonUtils.GSON.fromJson(Text.readString(in), AbstractJob.class);
                break;
            }
            case OperationType.OP_FINISH_CONSISTENCY_CHECK: {
                data = new ConsistencyCheckInfo();
                ((ConsistencyCheckInfo) data).readFields(in);
                break;
            }
            case OperationType.OP_FINISH_CONSISTENCY_CHECK_V2: {
                data = GsonUtils.GSON.fromJson(Text.readString(in), ConsistencyCheckInfo.class);
                break;
            }
            case OperationType.OP_EXPORT_CREATE_V2: {
                data = GsonUtils.GSON.fromJson(Text.readString(in), ExportJob.class);
                break;
            }
            case OperationType.OP_EXPORT_UPDATE_INFO_V2:
                data = GsonUtils.GSON.fromJson(Text.readString(in), ExportJob.ExportUpdateInfo.class);
                break;
            case OperationType.OP_FINISH_MULTI_DELETE: {
                data = MultiDeleteInfo.read(in);
                break;
            }
            case OperationType.OP_ADD_REPLICA: {
                data = ReplicaPersistInfo.read(in);
                break;
            }
            case OperationType.OP_BATCH_DELETE_REPLICA: {
                data = GsonUtils.GSON.fromJson(Text.readString(in), BatchDeleteReplicaInfo.class);
                break;
            }
            case OperationType.OP_ADD_REPLICA_V2:
            case OperationType.OP_UPDATE_REPLICA_V2:
            case OperationType.OP_DELETE_REPLICA_V2: {
                data = GsonUtils.GSON.fromJson(Text.readString(in), ReplicaPersistInfo.class);
                break;
            }
            case OperationType.OP_ADD_BACKEND_V2:
            case OperationType.OP_DROP_BACKEND_V2:
            case OperationType.OP_BACKEND_STATE_CHANGE_V2: {
                data = GsonUtils.GSON.fromJson(Text.readString(in), Backend.class);
                break;
            }
            case OperationType.OP_ADD_COMPUTE_NODE: {
                data = ComputeNode.read(in);
                break;
            }
            case OperationType.OP_DROP_COMPUTE_NODE: {
                data = DropComputeNodeLog.read(in);
                break;
            }
            case OperationType.OP_ADD_FRONTEND_V2:
            case OperationType.OP_ADD_FIRST_FRONTEND_V2:
            case OperationType.OP_UPDATE_FRONTEND_V2:
            case OperationType.OP_REMOVE_FRONTEND_V2:
            case OperationType.OP_RESET_FRONTENDS: {
                data = GsonUtils.GSON.fromJson(Text.readString(in), Frontend.class);
                break;
            }
            case OperationType.OP_LEADER_INFO_CHANGE_V2: {
                data = GsonUtils.GSON.fromJson(Text.readString(in), LeaderInfo.class);
                break;
            }
            case OperationType.OP_TIMESTAMP_V2: {
                data = GsonUtils.GSON.fromJson(Text.readString(in), Timestamp.class);
                break;
            }
            case OperationType.OP_META_VERSION_V2: {
                data = MetaVersion.read(in);
                break;
            }
            case OperationType.OP_ADD_BROKER_V2:
            case OperationType.OP_DROP_BROKER_V2: {
                data = GsonUtils.GSON.fromJson(Text.readString(in), BrokerMgr.ModifyBrokerInfo.class);
                break;
            }
            case OperationType.OP_UPSERT_TRANSACTION_STATE_V2: {
                data = GsonUtils.GSON.fromJson(Text.readString(in), TransactionState.class);
                break;
            }
            case OperationType.OP_UPSERT_TRANSACTION_STATE_BATCH: {
                data = TransactionStateBatch.read(in);
                break;
            }
            case OperationType.OP_CREATE_REPOSITORY_V2: {
                data = GsonUtils.GSON.fromJson(Text.readString(in), Repository.class);
                break;
            }
            case OperationType.OP_TRUNCATE_TABLE: {
                data = TruncateTableInfo.read(in);
                break;
            }
            case OperationType.OP_COLOCATE_ADD_TABLE_V2:
            case OperationType.OP_COLOCATE_BACKENDS_PER_BUCKETSEQ_V2:
            case OperationType.OP_COLOCATE_MARK_UNSTABLE_V2:
            case OperationType.OP_COLOCATE_MARK_STABLE_V2: {
                data = GsonUtils.GSON.fromJson(Text.readString(in), ColocatePersistInfo.class);
                break;
            }
            case OperationType.OP_MODIFY_TABLE_COLOCATE_V2: {
                data = GsonUtils.GSON.fromJson(Text.readString(in), TablePropertyInfo.class);
                break;
            }
            case OperationType.OP_HEARTBEAT_V2: {
                data = HbPackage.readV2(in);
                break;
            }
            case OperationType.OP_ADD_FUNCTION_V2: {
                data = GsonUtils.GSON.fromJson(Text.readString(in), Function.class);
                break;
            }
            case OperationType.OP_DROP_FUNCTION_V2: {
                data = GsonUtils.GSON.fromJson(Text.readString(in), FunctionSearchDesc.class);
                break;
            }
            case OperationType.OP_BACKEND_TABLETS_INFO_V2: {
                data = GsonUtils.GSON.fromJson(Text.readString(in), BackendTabletsInfo.class);
                break;
            }
            case OperationType.OP_CREATE_ROUTINE_LOAD_JOB_V2: {
                data = GsonUtils.GSON.fromJson(Text.readString(in), RoutineLoadJob.class);
                break;
            }
            case OperationType.OP_CHANGE_ROUTINE_LOAD_JOB_V2: {
                data = GsonUtils.GSON.fromJson(Text.readString(in), RoutineLoadOperation.class);
                break;
            }
            case OperationType.OP_CREATE_STREAM_LOAD_TASK_V2: {
                data = GsonUtils.GSON.fromJson(Text.readString(in), StreamLoadTask.class);
                break;
            }
            case OperationType.OP_CREATE_LOAD_JOB_V2: {
                data = GsonUtils.GSON.fromJson(Text.readString(in), LoadJob.class);
                break;
            }
            case OperationType.OP_END_LOAD_JOB_V2: {
                data = GsonUtils.GSON.fromJson(Text.readString(in), LoadJobFinalOperation.class);
                break;
            }
            case OperationType.OP_UPDATE_LOAD_JOB: {
                data = LoadJobStateUpdateInfo.read(in);
                break;
            }
            case OperationType.OP_CREATE_RESOURCE: {
                data = Resource.read(in);
                break;
            }
            case OperationType.OP_DROP_RESOURCE: {
                data = DropResourceOperationLog.read(in);
                break;
            }
            case OperationType.OP_RESOURCE_GROUP: {
                data = ResourceGroupOpEntry.read(in);
                break;
            }
            case OperationType.OP_CREATE_TASK:
            case OperationType.OP_ALTER_TASK:
                data = Task.read(in);
                break;
            case OperationType.OP_DROP_TASKS:
                data = DropTasksLog.read(in);
                break;
            case OperationType.OP_CREATE_TASK_RUN:
                data = TaskRunStatus.read(in);
                break;
            case OperationType.OP_UPDATE_TASK_RUN:
                data = TaskRunStatusChange.read(in);
                break;
            // only update the progress of task run
            case OperationType.OP_UPDATE_TASK_RUN_STATE:
                data = TaskRunPeriodStatusChange.read(in);
                break;
            case OperationType.OP_ARCHIVE_TASK_RUNS: {
                data = ArchiveTaskRunsLog.read(in);
                break;
            }
            case OperationType.OP_CREATE_SMALL_FILE_V2:
            case OperationType.OP_DROP_SMALL_FILE_V2: {
                data = GsonUtils.GSON.fromJson(Text.readString(in), SmallFile.class);
                break;
            }
            case OperationType.OP_ALTER_JOB_V2: {
                data = AlterJobV2.read(in);
                break;
            }
            case OperationType.OP_BATCH_ADD_ROLLUP_V2: {
                data = GsonUtils.GSON.fromJson(Text.readString(in), BatchAlterJobPersistInfo.class);
                break;
            }
            case OperationType.OP_MODIFY_DISTRIBUTION_TYPE_V2: {
                data = GsonUtils.GSON.fromJson(Text.readString(in), TableInfo.class);
                break;
            }
            case OperationType.OP_SET_REPLICA_STATUS: {
                data = SetReplicaStatusOperationLog.read(in);
                break;
            }
            case OperationType.OP_DYNAMIC_PARTITION:
            case OperationType.OP_MODIFY_IN_MEMORY:
            case OperationType.OP_SET_FORBIDDEN_GLOBAL_DICT:
            case OperationType.OP_SET_HAS_DELETE:
            case OperationType.OP_MODIFY_REPLICATION_NUM:
            case OperationType.OP_MODIFY_WRITE_QUORUM:
            case OperationType.OP_MODIFY_REPLICATED_STORAGE:
            case OperationType.OP_MODIFY_BUCKET_SIZE:
            case OperationType.OP_MODIFY_MUTABLE_BUCKET_NUM:
            case OperationType.OP_MODIFY_ENABLE_LOAD_PROFILE:
            case OperationType.OP_MODIFY_BASE_COMPACTION_FORBIDDEN_TIME_RANGES:
            case OperationType.OP_MODIFY_BINLOG_CONFIG:
            case OperationType.OP_MODIFY_BINLOG_AVAILABLE_VERSION:
            case OperationType.OP_MODIFY_ENABLE_PERSISTENT_INDEX:
            case OperationType.OP_MODIFY_PRIMARY_INDEX_CACHE_EXPIRE_SEC:
            case OperationType.OP_ALTER_TABLE_PROPERTIES:
            case OperationType.OP_MODIFY_TABLE_CONSTRAINT_PROPERTY: {
                data = ModifyTablePropertyOperationLog.read(in);
                break;
            }
            case OperationType.OP_REPLACE_TEMP_PARTITION: {
                data = ReplacePartitionOperationLog.read(in);
                break;
            }
            case OperationType.OP_INSTALL_PLUGIN: {
                data = PluginInfo.read(in);
                break;
            }
            case OperationType.OP_UNINSTALL_PLUGIN: {
                data = PluginInfo.read(in);
                break;
            }
            case OperationType.OP_REMOVE_ALTER_JOB_V2: {
                data = RemoveAlterJobV2OperationLog.read(in);
                break;
            }
            case OperationType.OP_ALTER_ROUTINE_LOAD_JOB: {
                data = AlterRoutineLoadJobOperationLog.read(in);
                break;
            }
            case OperationType.OP_ALTER_LOAD_JOB: {
                data = AlterLoadJobOperationLog.read(in);
                break;
            }
            case OperationType.OP_GLOBAL_VARIABLE_V2: {
                data = GlobalVarPersistInfo.read(in);
                break;
            }
            case OperationType.OP_SWAP_TABLE: {
                data = SwapTableOperationLog.read(in);
                break;
            }
            case OperationType.OP_ADD_ANALYZER_JOB: {
                data = NativeAnalyzeJob.read(in);
                break;
            }
            case OperationType.OP_REMOVE_ANALYZER_JOB: {
                data = NativeAnalyzeJob.read(in);
                break;
            }
            case OperationType.OP_ADD_ANALYZE_STATUS: {
                data = NativeAnalyzeStatus.read(in);
                break;
            }
            case OperationType.OP_REMOVE_ANALYZE_STATUS: {
                data = NativeAnalyzeStatus.read(in);
                break;
            }
            case OperationType.OP_ADD_EXTERNAL_ANALYZE_STATUS: {
                data = ExternalAnalyzeStatus.read(in);
                break;
            }
            case OperationType.OP_REMOVE_EXTERNAL_ANALYZE_STATUS: {
                data = ExternalAnalyzeStatus.read(in);
                break;
            }
            case OperationType.OP_ADD_EXTERNAL_ANALYZER_JOB: {
                data = ExternalAnalyzeJob.read(in);
                break;
            }
            case OperationType.OP_REMOVE_EXTERNAL_ANALYZER_JOB: {
                data = ExternalAnalyzeJob.read(in);
                break;
            }
            case OperationType.OP_ADD_BASIC_STATS_META: {
                data = BasicStatsMeta.read(in);
                break;
            }
            case OperationType.OP_REMOVE_BASIC_STATS_META: {
                data = BasicStatsMeta.read(in);
                break;
            }
            case OperationType.OP_ADD_HISTOGRAM_STATS_META: {
                data = HistogramStatsMeta.read(in);
                break;
            }
            case OperationType.OP_REMOVE_HISTOGRAM_STATS_META: {
                data = HistogramStatsMeta.read(in);
                break;
            }
            case OperationType.OP_ADD_EXTERNAL_BASIC_STATS_META: {
                data = ExternalBasicStatsMeta.read(in);
                break;
            }
            case OperationType.OP_REMOVE_EXTERNAL_BASIC_STATS_META: {
                data = ExternalBasicStatsMeta.read(in);
                break;
            }
            case OperationType.OP_ADD_EXTERNAL_HISTOGRAM_STATS_META: {
                data = ExternalHistogramStatsMeta.read(in);
                break;
            }
            case OperationType.OP_REMOVE_EXTERNAL_HISTOGRAM_STATS_META: {
                data = ExternalHistogramStatsMeta.read(in);
                break;
            }
            case OperationType.OP_MODIFY_HIVE_TABLE_COLUMN: {
                data = ModifyTableColumnOperationLog.read(in);
                break;
            }
            case OperationType.OP_CREATE_CATALOG: {
                data = Catalog.read(in);
                break;
            }
            case OperationType.OP_DROP_CATALOG: {
                data = DropCatalogLog.read(in);
                break;
            }
            case OperationType.OP_ALTER_CATALOG: {
                data = AlterCatalogLog.read(in);
                break;
            }
            case OperationType.OP_CREATE_INSERT_OVERWRITE: {
                data = CreateInsertOverwriteJobLog.read(in);
                break;
            }
            case OperationType.OP_INSERT_OVERWRITE_STATE_CHANGE: {
                data = InsertOverwriteStateChangeInfo.read(in);
                break;
            }
            case OperationType.OP_STARMGR: {
                data = StarMgrJournal.read(in);
                break;
            }
            case OperationType.OP_CREATE_USER_V2: {
                data = CreateUserInfo.read(in);
                break;
            }
            case OperationType.OP_ALTER_USER_V2: {
                data = AlterUserInfo.read(in);
                break;
            }
            case OperationType.OP_UPDATE_USER_PROP_V3: {
                data = GsonUtils.GSON.fromJson(Text.readString(in), UserPropertyInfo.class);
                break;
            }
            case OperationType.OP_DROP_USER_V3: {
                data = GsonUtils.GSON.fromJson(Text.readString(in), UserIdentity.class);
                break;
            }
            case OperationType.OP_UPDATE_USER_PRIVILEGE_V2: {
                data = UserPrivilegeCollectionInfo.read(in);
                break;
            }
            case OperationType.OP_DROP_ROLE_V2:
            case OperationType.OP_UPDATE_ROLE_PRIVILEGE_V2: {
                data = RolePrivilegeCollectionInfo.read(in);
                break;
            }
            case OperationType.OP_AUTH_UPGRADE_V2: {
                // for compatibility reason, just ignore the auth upgrade log
                break;
            }
            case OperationType.OP_MV_JOB_STATE: {
                data = MVMaintenanceJob.read(in);
                break;
            }
            case OperationType.OP_MV_EPOCH_UPDATE: {
                data = MVEpoch.read(in);
                break;
            }
            case OperationType.OP_MODIFY_TABLE_ADD_OR_DROP_COLUMNS: {
                data = TableAddOrDropColumnsInfo.read(in);
                break;
            }
            case OperationType.OP_SET_DEFAULT_STORAGE_VOLUME: {
                data = SetDefaultStorageVolumeLog.read(in);
                break;
            }
            case OperationType.OP_DROP_STORAGE_VOLUME: {
                data = DropStorageVolumeLog.read(in);
                break;
            }
            case OperationType.OP_CREATE_STORAGE_VOLUME:
            case OperationType.OP_UPDATE_STORAGE_VOLUME: {
                data = StorageVolume.read(in);
                break;
            }
            case OperationType.OP_PIPE: {
                data = PipeOpEntry.read(in);
                break;
            }
            case OperationType.OP_CREATE_DICTIONARY: {
                data = Dictionary.read(in);
                break;
            }
            case OperationType.OP_DROP_DICTIONARY: {
                data = DropDictionaryInfo.read(in);
                break;
            }
            case OperationType.OP_MODIFY_DICTIONARY_MGR: {
                data = DictionaryMgrInfo.read(in);
                break;
            }
            case OperationType.OP_DECOMMISSION_DISK: {
                data = GsonUtils.GSON.fromJson(Text.readString(in), DecommissionDiskInfo.class);
                break;
            }
            case OperationType.OP_CANCEL_DECOMMISSION_DISK: {
                data = GsonUtils.GSON.fromJson(Text.readString(in), CancelDecommissionDiskInfo.class);
                break;
            }
            case OperationType.OP_DISABLE_DISK: {
                data = GsonUtils.GSON.fromJson(Text.readString(in), DisableDiskInfo.class);
                break;
            }
            case OperationType.OP_CANCEL_DISABLE_DISK: {
                data = GsonUtils.GSON.fromJson(Text.readString(in), CancelDisableDiskInfo.class);
                break;
            }
            case OperationType.OP_REPLICATION_JOB: {
                data = ReplicationJobLog.read(in);
                break;
            }
            case OperationType.OP_DELETE_REPLICATION_JOB: {
                data = ReplicationJobLog.read(in);
                break;
            }
            case OperationType.OP_RECOVER_PARTITION_VERSION: {
                data = GsonUtils.GSON.fromJson(Text.readString(in), PartitionVersionRecoveryInfo.class);
                break;
            }
            case OperationType.OP_ADD_KEY: {
                data = new Text(Text.readBinary(in));
                break;
            }
            case OperationType.OP_CREATE_WAREHOUSE:
            case OperationType.OP_ALTER_WAREHOUSE:
                data = GsonUtils.GSON.fromJson(Text.readString(in), Warehouse.class);
                break;
            case OperationType.OP_DROP_WAREHOUSE: {
                data = DropWarehouseLog.read(in);
                break;
            }
            case OperationType.OP_CLUSTER_SNAPSHOT_LOG: {
                data = ClusterSnapshotLog.read(in);
                break;
            }
            default: {
                if (Config.metadata_ignore_unknown_operation_type) {
                    LOG.warn("UNKNOWN Operation Type {}", opCode);
                } else {
                    throw new IOException("UNKNOWN Operation Type " + opCode);
                }
            }
        } // end switch
    }
}
