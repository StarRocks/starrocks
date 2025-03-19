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

package com.starrocks.persist;

import com.google.common.collect.ImmutableMap;
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
import com.starrocks.common.util.SmallFileMgr;
import com.starrocks.ha.LeaderInfo;
import com.starrocks.journal.bdbje.Timestamp;
import com.starrocks.load.ExportJob;
import com.starrocks.load.MultiDeleteInfo;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.load.loadv2.LoadJobFinalOperation;
import com.starrocks.load.routineload.RoutineLoadJob;
import com.starrocks.load.streamload.StreamLoadTask;
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
import com.starrocks.sql.spm.BaselinePlan;
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
import java.io.IOException;

public class EditLogDeserializer {
    private static final Logger LOG = LogManager.getLogger(EditLogDeserializer.class);

    private static final ImmutableMap<Short, Class<? extends Writable>> OPTYPE_TO_DESER_CLASS
            = ImmutableMap.<Short, Class<? extends Writable>>builder()
            .put(OperationType.OP_SAVE_TRANSACTION_ID_V2, TransactionIdInfo.class)
            .put(OperationType.OP_SAVE_AUTO_INCREMENT_ID, AutoIncrementInfo.class)
            .put(OperationType.OP_DELETE_AUTO_INCREMENT_ID, AutoIncrementInfo.class)
            .put(OperationType.OP_CREATE_DB_V2, CreateDbInfo.class)
            .put(OperationType.OP_DROP_DB, DropDbInfo.class)
            .put(OperationType.OP_ALTER_DB_V2, DatabaseInfo.class)
            .put(OperationType.OP_RENAME_DB_V2, DatabaseInfo.class)
            .put(OperationType.OP_CREATE_TABLE_V2, CreateTableInfo.class)
            .put(OperationType.OP_DROP_TABLE_V2, DropInfo.class)
            .put(OperationType.OP_DROP_ROLLUP_V2, DropInfo.class)
            .put(OperationType.OP_ERASE_MULTI_TABLES, MultiEraseTableInfo.class)
            .put(OperationType.OP_DISABLE_TABLE_RECOVERY, DisableTableRecoveryInfo.class)
            .put(OperationType.OP_DISABLE_PARTITION_RECOVERY, DisablePartitionRecoveryInfo.class)
            .put(OperationType.OP_ADD_PARTITION_V2, PartitionPersistInfoV2.class)
            .put(OperationType.OP_ADD_SUB_PARTITIONS_V2, AddSubPartitionsInfoV2.class)
            .put(OperationType.OP_ADD_PARTITIONS_V2, AddPartitionsInfoV2.class)
            .put(OperationType.OP_DROP_PARTITION, DropPartitionInfo.class)
            .put(OperationType.OP_DROP_PARTITIONS, DropPartitionsInfo.class)
            .put(OperationType.OP_MODIFY_PARTITION_V2, ModifyPartitionInfo.class)
            .put(OperationType.OP_BATCH_MODIFY_PARTITION, BatchModifyPartitionsInfo.class)
            .put(OperationType.OP_RECOVER_DB_V2, RecoverInfo.class)
            .put(OperationType.OP_RECOVER_TABLE_V2, RecoverInfo.class)
            .put(OperationType.OP_RECOVER_PARTITION_V2, RecoverInfo.class)
            .put(OperationType.OP_BATCH_DROP_ROLLUP, BatchDropInfo.class)
            .put(OperationType.OP_RENAME_TABLE_V2, TableInfo.class)
            .put(OperationType.OP_RENAME_ROLLUP_V2, TableInfo.class)
            .put(OperationType.OP_RENAME_PARTITION_V2, TableInfo.class)
            .put(OperationType.OP_RENAME_COLUMN_V2, ColumnRenameInfo.class)
            .put(OperationType.OP_MODIFY_VIEW_DEF, AlterViewInfo.class)
            .put(OperationType.OP_ALTER_MATERIALIZED_VIEW_PROPERTIES, ModifyTablePropertyOperationLog.class)
            .put(OperationType.OP_RENAME_MATERIALIZED_VIEW, RenameMaterializedViewLog.class)
            .put(OperationType.OP_ALTER_MATERIALIZED_VIEW_STATUS, AlterMaterializedViewStatusLog.class)
            .put(OperationType.OP_ALTER_MATERIALIZED_VIEW_BASE_TABLE_INFOS, AlterMaterializedViewBaseTableInfosLog.class)
            .put(OperationType.OP_BACKUP_JOB_V2, AbstractJob.class)
            .put(OperationType.OP_RESTORE_JOB_V2, AbstractJob.class)
            .put(OperationType.OP_FINISH_CONSISTENCY_CHECK_V2, ConsistencyCheckInfo.class)
            .put(OperationType.OP_EXPORT_CREATE_V2, ExportJob.class)
            .put(OperationType.OP_EXPORT_UPDATE_INFO_V2, ExportJob.ExportUpdateInfo.class)
            .put(OperationType.OP_FINISH_MULTI_DELETE, MultiDeleteInfo.class)
            .put(OperationType.OP_BATCH_DELETE_REPLICA, BatchDeleteReplicaInfo.class)
            .put(OperationType.OP_ADD_REPLICA_V2, ReplicaPersistInfo.class)
            .put(OperationType.OP_UPDATE_REPLICA_V2, ReplicaPersistInfo.class)
            .put(OperationType.OP_DELETE_REPLICA_V2, ReplicaPersistInfo.class)
            .put(OperationType.OP_ADD_BACKEND_V2, Backend.class)
            .put(OperationType.OP_DROP_BACKEND_V2, Backend.class)
            .put(OperationType.OP_BACKEND_STATE_CHANGE_V2, Backend.class)
            .put(OperationType.OP_ADD_COMPUTE_NODE, ComputeNode.class)
            .put(OperationType.OP_DROP_COMPUTE_NODE, DropComputeNodeLog.class)
            .put(OperationType.OP_ADD_FRONTEND_V2, Frontend.class)
            .put(OperationType.OP_ADD_FIRST_FRONTEND_V2, Frontend.class)
            .put(OperationType.OP_UPDATE_FRONTEND_V2, Frontend.class)
            .put(OperationType.OP_REMOVE_FRONTEND_V2, Frontend.class)
            .put(OperationType.OP_RESET_FRONTENDS, Frontend.class)
            .put(OperationType.OP_LEADER_INFO_CHANGE_V2, LeaderInfo.class)
            .put(OperationType.OP_TIMESTAMP_V2, Timestamp.class)
            .put(OperationType.OP_ADD_BROKER_V2, BrokerMgr.ModifyBrokerInfo.class)
            .put(OperationType.OP_DROP_BROKER_V2, BrokerMgr.ModifyBrokerInfo.class)
            .put(OperationType.OP_UPSERT_TRANSACTION_STATE_V2, TransactionState.class)
            .put(OperationType.OP_UPSERT_TRANSACTION_STATE_BATCH, TransactionStateBatch.class)
            .put(OperationType.OP_CREATE_REPOSITORY_V2, Repository.class)
            .put(OperationType.OP_TRUNCATE_TABLE, TruncateTableInfo.class)
            .put(OperationType.OP_COLOCATE_ADD_TABLE_V2, ColocatePersistInfo.class)
            .put(OperationType.OP_COLOCATE_BACKENDS_PER_BUCKETSEQ_V2, ColocatePersistInfo.class)
            .put(OperationType.OP_COLOCATE_MARK_UNSTABLE_V2, ColocatePersistInfo.class)
            .put(OperationType.OP_COLOCATE_MARK_STABLE_V2, ColocatePersistInfo.class)
            .put(OperationType.OP_MODIFY_TABLE_COLOCATE_V2, TablePropertyInfo.class)
            .put(OperationType.OP_HEARTBEAT_V2, HbPackage.class)
            .put(OperationType.OP_ADD_FUNCTION_V2, Function.class)
            .put(OperationType.OP_DROP_FUNCTION_V2, FunctionSearchDesc.class)
            .put(OperationType.OP_BACKEND_TABLETS_INFO_V2, BackendTabletsInfo.class)
            .put(OperationType.OP_CREATE_ROUTINE_LOAD_JOB_V2, RoutineLoadJob.class)
            .put(OperationType.OP_CHANGE_ROUTINE_LOAD_JOB_V2, RoutineLoadOperation.class)
            .put(OperationType.OP_CREATE_STREAM_LOAD_TASK_V2, StreamLoadTask.class)
            .put(OperationType.OP_CREATE_LOAD_JOB_V2, LoadJob.class)
            .put(OperationType.OP_END_LOAD_JOB_V2, LoadJobFinalOperation.class)
            .put(OperationType.OP_UPDATE_LOAD_JOB, LoadJob.LoadJobStateUpdateInfo.class)
            .put(OperationType.OP_CREATE_RESOURCE, Resource.class)
            .put(OperationType.OP_DROP_RESOURCE, DropResourceOperationLog.class)
            .put(OperationType.OP_RESOURCE_GROUP, ResourceGroupOpEntry.class)
            .put(OperationType.OP_CREATE_TASK, Task.class)
            .put(OperationType.OP_ALTER_TASK, Task.class)
            .put(OperationType.OP_DROP_TASKS, DropTasksLog.class)
            .put(OperationType.OP_CREATE_TASK_RUN, TaskRunStatus.class)
            .put(OperationType.OP_UPDATE_TASK_RUN, TaskRunStatusChange.class)
            .put(OperationType.OP_UPDATE_TASK_RUN_STATE, TaskRunPeriodStatusChange.class)
            .put(OperationType.OP_ARCHIVE_TASK_RUNS, ArchiveTaskRunsLog.class)
            .put(OperationType.OP_CREATE_SMALL_FILE_V2, SmallFileMgr.SmallFile.class)
            .put(OperationType.OP_DROP_SMALL_FILE_V2, SmallFileMgr.SmallFile.class)
            .put(OperationType.OP_ALTER_JOB_V2, AlterJobV2.class)
            .put(OperationType.OP_BATCH_ADD_ROLLUP_V2, BatchAlterJobPersistInfo.class)
            .put(OperationType.OP_MODIFY_DISTRIBUTION_TYPE_V2, TableInfo.class)
            .put(OperationType.OP_SET_REPLICA_STATUS, SetReplicaStatusOperationLog.class)
            .put(OperationType.OP_DYNAMIC_PARTITION, ModifyTablePropertyOperationLog.class)
            .put(OperationType.OP_MODIFY_IN_MEMORY, ModifyTablePropertyOperationLog.class)
            .put(OperationType.OP_SET_FORBIDDEN_GLOBAL_DICT, ModifyTablePropertyOperationLog.class)
            .put(OperationType.OP_SET_HAS_DELETE, ModifyTablePropertyOperationLog.class)
            .put(OperationType.OP_MODIFY_REPLICATION_NUM, ModifyTablePropertyOperationLog.class)
            .put(OperationType.OP_MODIFY_WRITE_QUORUM, ModifyTablePropertyOperationLog.class)
            .put(OperationType.OP_MODIFY_REPLICATED_STORAGE, ModifyTablePropertyOperationLog.class)
            .put(OperationType.OP_MODIFY_BUCKET_SIZE, ModifyTablePropertyOperationLog.class)
            .put(OperationType.OP_MODIFY_MUTABLE_BUCKET_NUM, ModifyTablePropertyOperationLog.class)
            .put(OperationType.OP_MODIFY_ENABLE_LOAD_PROFILE, ModifyTablePropertyOperationLog.class)
            .put(OperationType.OP_MODIFY_BASE_COMPACTION_FORBIDDEN_TIME_RANGES, ModifyTablePropertyOperationLog.class)
            .put(OperationType.OP_MODIFY_BINLOG_CONFIG, ModifyTablePropertyOperationLog.class)
            .put(OperationType.OP_MODIFY_BINLOG_AVAILABLE_VERSION, ModifyTablePropertyOperationLog.class)
            .put(OperationType.OP_MODIFY_ENABLE_PERSISTENT_INDEX, ModifyTablePropertyOperationLog.class)
            .put(OperationType.OP_MODIFY_PRIMARY_INDEX_CACHE_EXPIRE_SEC, ModifyTablePropertyOperationLog.class)
            .put(OperationType.OP_ALTER_TABLE_PROPERTIES, ModifyTablePropertyOperationLog.class)
            .put(OperationType.OP_MODIFY_TABLE_CONSTRAINT_PROPERTY, ModifyTablePropertyOperationLog.class)
            .put(OperationType.OP_REPLACE_TEMP_PARTITION, ReplacePartitionOperationLog.class)
            .put(OperationType.OP_INSTALL_PLUGIN, PluginInfo.class)
            .put(OperationType.OP_UNINSTALL_PLUGIN, PluginInfo.class)
            .put(OperationType.OP_REMOVE_ALTER_JOB_V2, RemoveAlterJobV2OperationLog.class)
            .put(OperationType.OP_ALTER_ROUTINE_LOAD_JOB, AlterRoutineLoadJobOperationLog.class)
            .put(OperationType.OP_ALTER_LOAD_JOB, AlterLoadJobOperationLog.class)
            .put(OperationType.OP_SWAP_TABLE, SwapTableOperationLog.class)
            .put(OperationType.OP_ADD_ANALYZER_JOB, NativeAnalyzeJob.class)
            .put(OperationType.OP_REMOVE_ANALYZER_JOB, NativeAnalyzeJob.class)
            .put(OperationType.OP_ADD_ANALYZE_STATUS, NativeAnalyzeStatus.class)
            .put(OperationType.OP_REMOVE_ANALYZE_STATUS, NativeAnalyzeStatus.class)
            .put(OperationType.OP_ADD_EXTERNAL_ANALYZE_STATUS, ExternalAnalyzeStatus.class)
            .put(OperationType.OP_REMOVE_EXTERNAL_ANALYZE_STATUS, ExternalAnalyzeStatus.class)
            .put(OperationType.OP_ADD_EXTERNAL_ANALYZER_JOB, ExternalAnalyzeJob.class)
            .put(OperationType.OP_REMOVE_EXTERNAL_ANALYZER_JOB, ExternalAnalyzeJob.class)
            .put(OperationType.OP_ADD_BASIC_STATS_META, BasicStatsMeta.class)
            .put(OperationType.OP_REMOVE_BASIC_STATS_META, BasicStatsMeta.class)
            .put(OperationType.OP_ADD_HISTOGRAM_STATS_META, HistogramStatsMeta.class)
            .put(OperationType.OP_REMOVE_HISTOGRAM_STATS_META, HistogramStatsMeta.class)
            .put(OperationType.OP_ADD_EXTERNAL_BASIC_STATS_META, ExternalBasicStatsMeta.class)
            .put(OperationType.OP_REMOVE_EXTERNAL_BASIC_STATS_META, ExternalBasicStatsMeta.class)
            .put(OperationType.OP_ADD_EXTERNAL_HISTOGRAM_STATS_META, ExternalHistogramStatsMeta.class)
            .put(OperationType.OP_REMOVE_EXTERNAL_HISTOGRAM_STATS_META, ExternalHistogramStatsMeta.class)
            .put(OperationType.OP_MODIFY_HIVE_TABLE_COLUMN, ModifyTableColumnOperationLog.class)
            .put(OperationType.OP_CREATE_CATALOG, Catalog.class)
            .put(OperationType.OP_DROP_CATALOG, DropCatalogLog.class)
            .put(OperationType.OP_ALTER_CATALOG, AlterCatalogLog.class)
            .put(OperationType.OP_CREATE_INSERT_OVERWRITE, CreateInsertOverwriteJobLog.class)
            .put(OperationType.OP_INSERT_OVERWRITE_STATE_CHANGE, InsertOverwriteStateChangeInfo.class)
            .put(OperationType.OP_CREATE_USER_V2, CreateUserInfo.class)
            .put(OperationType.OP_ALTER_USER_V2, AlterUserInfo.class)
            .put(OperationType.OP_UPDATE_USER_PROP_V3, UserPropertyInfo.class)
            .put(OperationType.OP_DROP_USER_V3, UserIdentity.class)
            .put(OperationType.OP_UPDATE_USER_PRIVILEGE_V2, UserPrivilegeCollectionInfo.class)
            .put(OperationType.OP_DROP_ROLE_V2, RolePrivilegeCollectionInfo.class)
            .put(OperationType.OP_UPDATE_ROLE_PRIVILEGE_V2, RolePrivilegeCollectionInfo.class)
            .put(OperationType.OP_MV_JOB_STATE, MVMaintenanceJob.class)
            .put(OperationType.OP_MV_EPOCH_UPDATE, MVEpoch.class)
            .put(OperationType.OP_MODIFY_TABLE_ADD_OR_DROP_COLUMNS, TableAddOrDropColumnsInfo.class)
            .put(OperationType.OP_SET_DEFAULT_STORAGE_VOLUME, SetDefaultStorageVolumeLog.class)
            .put(OperationType.OP_DROP_STORAGE_VOLUME, DropStorageVolumeLog.class)
            .put(OperationType.OP_CREATE_STORAGE_VOLUME, StorageVolume.class)
            .put(OperationType.OP_UPDATE_STORAGE_VOLUME, StorageVolume.class)
            .put(OperationType.OP_PIPE, PipeOpEntry.class)
            .put(OperationType.OP_CREATE_DICTIONARY, Dictionary.class)
            .put(OperationType.OP_DROP_DICTIONARY, DropDictionaryInfo.class)
            .put(OperationType.OP_MODIFY_DICTIONARY_MGR, DictionaryMgrInfo.class)
            .put(OperationType.OP_DECOMMISSION_DISK, DecommissionDiskInfo.class)
            .put(OperationType.OP_CANCEL_DECOMMISSION_DISK, CancelDecommissionDiskInfo.class)
            .put(OperationType.OP_DISABLE_DISK, DisableDiskInfo.class)
            .put(OperationType.OP_REPLICATION_JOB, ReplicationJobLog.class)
            .put(OperationType.OP_DELETE_REPLICATION_JOB, ReplicationJobLog.class)
            .put(OperationType.OP_RECOVER_PARTITION_VERSION, PartitionVersionRecoveryInfo.class)
            .put(OperationType.OP_CREATE_WAREHOUSE, Warehouse.class)
            .put(OperationType.OP_ALTER_WAREHOUSE, Warehouse.class)
            .put(OperationType.OP_DROP_WAREHOUSE, DropWarehouseLog.class)
            .put(OperationType.OP_CLUSTER_SNAPSHOT_LOG, ClusterSnapshotLog.class)
            .put(OperationType.OP_ADD_SQL_QUERY_BLACK_LIST, SqlBlackListPersistInfo.class)
            .put(OperationType.OP_DELETE_SQL_QUERY_BLACK_LIST, DeleteSqlBlackLists.class)
            .put(OperationType.OP_CREATE_SECURITY_INTEGRATION, SecurityIntegrationPersistInfo.class)
            .put(OperationType.OP_ALTER_SECURITY_INTEGRATION, SecurityIntegrationPersistInfo.class)
            .put(OperationType.OP_DROP_SECURITY_INTEGRATION, SecurityIntegrationPersistInfo.class)
            .put(OperationType.OP_CREATE_GROUP_PROVIDER, GroupProviderLog.class)
            .put(OperationType.OP_DROP_GROUP_PROVIDER, GroupProviderLog.class)
            .put(OperationType.OP_CREATE_SPM_BASELINE_LOG, BaselinePlan.class)
            .put(OperationType.OP_DROP_SPM_BASELINE_LOG, BaselinePlan.class)
            .build();

    public static Writable deserialize(Short opCode, DataInput in) throws IOException {
        LOG.debug("get opcode: {}", opCode);

        Writable data = null;
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
            case OperationType.OP_ADD_REPLICA: {
                data = ReplicaPersistInfo.read(in);
                break;
            }
            case OperationType.OP_CHANGE_MATERIALIZED_VIEW_REFRESH_SCHEME: {
                data = ChangeMaterializedViewRefreshSchemeLog.read(in);
                break;
            }
            case OperationType.OP_FINISH_CONSISTENCY_CHECK: {
                data = new ConsistencyCheckInfo();
                ((ConsistencyCheckInfo) data).readFields(in);
                break;
            }
            case OperationType.OP_META_VERSION_V2: {
                data = MetaVersion.read(in);
                break;
            }
            case OperationType.OP_GLOBAL_VARIABLE_V2: {
                data = GlobalVarPersistInfo.read(in);
                break;
            }
            case OperationType.OP_STARMGR: {
                data = StarMgrJournal.read(in);
                break;
            }
            case OperationType.OP_ADD_KEY: {
                data = new Text(Text.readBinary(in));
                break;
            }
            default: {
                if (OPTYPE_TO_DESER_CLASS.get(opCode) == null) {
                    if (Config.metadata_ignore_unknown_operation_type) {
                        LOG.warn("UNKNOWN Operation Type {}", opCode);
                    } else {
                        throw new IOException("UNKNOWN Operation Type " + opCode);
                    }
                } else {
                    data = GsonUtils.GSON.fromJson(Text.readString(in), OPTYPE_TO_DESER_CLASS.get(opCode));
                }
                break;
            }
        }

        return data;
    }
}