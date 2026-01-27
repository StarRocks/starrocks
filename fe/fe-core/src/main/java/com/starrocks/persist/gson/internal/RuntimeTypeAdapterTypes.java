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

package com.starrocks.persist.gson.internal;

import com.google.common.collect.Maps;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;
import com.starrocks.alter.AlterJobV2;
import com.starrocks.alter.LakeRollupJob;
import com.starrocks.alter.LakeTableAlterMetaJob;
import com.starrocks.alter.LakeTableAsyncFastSchemaChangeJob;
import com.starrocks.alter.LakeTableSchemaChangeJob;
import com.starrocks.alter.MergePartitionJob;
import com.starrocks.alter.OnlineOptimizeJobV2;
import com.starrocks.alter.OptimizeJobV2;
import com.starrocks.alter.RollupJobV2;
import com.starrocks.alter.SchemaChangeJobV2;
import com.starrocks.alter.reshard.IdenticalTablet;
import com.starrocks.alter.reshard.MergeTabletJob;
import com.starrocks.alter.reshard.MergingTablet;
import com.starrocks.alter.reshard.ReshardingTablet;
import com.starrocks.alter.reshard.SplitTabletJob;
import com.starrocks.alter.reshard.SplittingTablet;
import com.starrocks.alter.reshard.TabletReshardJob;
import com.starrocks.authentication.FileGroupProvider;
import com.starrocks.authentication.GroupProvider;
import com.starrocks.authentication.JWTSecurityIntegration;
import com.starrocks.authentication.LDAPGroupProvider;
import com.starrocks.authentication.OAuth2SecurityIntegration;
import com.starrocks.authentication.SecurityIntegration;
import com.starrocks.authentication.SimpleLDAPSecurityIntegration;
import com.starrocks.authentication.UnixGroupProvider;
import com.starrocks.authorization.CatalogPEntryObject;
import com.starrocks.authorization.DbPEntryObject;
import com.starrocks.authorization.FunctionPEntryObject;
import com.starrocks.authorization.GlobalFunctionPEntryObject;
import com.starrocks.authorization.MaterializedViewPEntryObject;
import com.starrocks.authorization.PEntryObject;
import com.starrocks.authorization.PipePEntryObject;
import com.starrocks.authorization.PolicyFCEntryObject;
import com.starrocks.authorization.ResourceGroupPEntryObject;
import com.starrocks.authorization.ResourcePEntryObject;
import com.starrocks.authorization.StorageVolumePEntryObject;
import com.starrocks.authorization.TablePEntryObject;
import com.starrocks.authorization.UserPEntryObject;
import com.starrocks.authorization.ViewPEntryObject;
import com.starrocks.authorization.WarehousePEntryObject;
import com.starrocks.backup.AbstractJob;
import com.starrocks.backup.BackupJob;
import com.starrocks.backup.RestoreJob;
import com.starrocks.backup.SnapshotInfo;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.BoolVariant;
import com.starrocks.catalog.DateVariant;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.EsTable;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.ExpressionRangePartitionInfoV2;
import com.starrocks.catalog.ExternalOlapTable;
import com.starrocks.catalog.FileTable;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.HiveResource;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.HudiResource;
import com.starrocks.catalog.HudiTable;
import com.starrocks.catalog.IcebergResource;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.IntVariant;
import com.starrocks.catalog.JDBCResource;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.catalog.LargeIntVariant;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MysqlTable;
import com.starrocks.catalog.OdbcCatalogResource;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.RangeDistributionInfo;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.RecycleListPartitionInfo;
import com.starrocks.catalog.RecyclePartitionInfoV2;
import com.starrocks.catalog.RecycleRangePartitionInfo;
import com.starrocks.catalog.RecycleUnPartitionInfo;
import com.starrocks.catalog.Resource;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.SparkResource;
import com.starrocks.catalog.SqlFunction;
import com.starrocks.catalog.StringVariant;
import com.starrocks.catalog.TableFunction;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.Variant;
import com.starrocks.catalog.View;
import com.starrocks.common.tvr.TvrTableDelta;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.lake.LakeMaterializedView;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.RecycleLakeListPartitionInfo;
import com.starrocks.lake.RecycleLakeRangePartitionInfo;
import com.starrocks.lake.RecycleLakeUnPartitionInfo;
import com.starrocks.lake.backup.LakeBackupJob;
import com.starrocks.lake.backup.LakeRestoreJob;
import com.starrocks.lake.backup.LakeTableSnapshotInfo;
import com.starrocks.lake.compaction.CompactionTxnCommitAttachment;
import com.starrocks.load.loadv2.BrokerLoadJob;
import com.starrocks.load.loadv2.InsertLoadJob;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.load.loadv2.LoadJobFinalOperation;
import com.starrocks.load.loadv2.ManualLoadTxnCommitAttachment;
import com.starrocks.load.loadv2.MiniLoadTxnCommitAttachment;
import com.starrocks.load.loadv2.SparkLoadJob;
import com.starrocks.load.routineload.KafkaProgress;
import com.starrocks.load.routineload.KafkaRoutineLoadJob;
import com.starrocks.load.routineload.PulsarProgress;
import com.starrocks.load.routineload.PulsarRoutineLoadJob;
import com.starrocks.load.routineload.RLTaskTxnCommitAttachment;
import com.starrocks.load.routineload.RoutineLoadJob;
import com.starrocks.load.routineload.RoutineLoadProgress;
import com.starrocks.load.streamload.StreamLoadTxnCommitAttachment;
import com.starrocks.persist.ListPartitionPersistInfo;
import com.starrocks.persist.PartitionPersistInfoV2;
import com.starrocks.persist.RangePartitionPersistInfo;
import com.starrocks.persist.SinglePartitionPersistInfo;
import com.starrocks.persist.gson.RuntimeTypeAdapterFactory;
import com.starrocks.replication.LakeReplicationJob;
import com.starrocks.replication.ReplicationJob;
import com.starrocks.replication.ReplicationTxnCommitAttachment;
import com.starrocks.server.SharedDataStorageVolumeMgr;
import com.starrocks.server.SharedNothingStorageVolumeMgr;
import com.starrocks.server.StorageVolumeMgr;
import com.starrocks.sql.optimizer.dump.HiveTableDumpInfo;
import com.starrocks.sql.optimizer.dump.QueryDumpDeserializer;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.sql.optimizer.dump.QueryDumpSerializer;
import com.starrocks.statistic.AnalyzeJob;
import com.starrocks.statistic.AnalyzeStatus;
import com.starrocks.statistic.ExternalAnalyzeJob;
import com.starrocks.statistic.ExternalAnalyzeStatus;
import com.starrocks.statistic.NativeAnalyzeJob;
import com.starrocks.statistic.NativeAnalyzeStatus;
import com.starrocks.system.BackendHbResponse;
import com.starrocks.system.BrokerHbResponse;
import com.starrocks.system.FrontendHbResponse;
import com.starrocks.system.HeartbeatResponse;
import com.starrocks.transaction.InsertTxnCommitAttachment;
import com.starrocks.transaction.TxnCommitAttachment;
import com.starrocks.type.AnyArrayType;
import com.starrocks.type.AnyElementType;
import com.starrocks.type.ArrayType;
import com.starrocks.type.MapType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.PseudoType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
import com.starrocks.warehouse.DefaultWarehouse;
import com.starrocks.warehouse.Warehouse;
import com.starrocks.warehouse.cngroup.ComputeResource;
import com.starrocks.warehouse.cngroup.WarehouseComputeResource;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;

public class RuntimeTypeAdapterTypes {
    public static final Map<Class<?>, RuntimeTypeAdapterFactory<?>> CLAZZ_TO_RUNTIME_TYPE_ADAPTOR_FACTORIES =
            Maps.newHashMap();

    static {
        // runtime adapter for class "Type"
        final RuntimeTypeAdapterFactory<Type> column_type_adaptor_factory =
                RuntimeTypeAdapterFactory
                        .of(com.starrocks.type.Type.class, "clazz")
                        .registerSubtype(ScalarType.class, "ScalarType", true)
                        .registerSubtype(ArrayType.class, "ArrayType")
                        .registerSubtype(MapType.class, "MapType")
                        .registerSubtype(StructType.class, "StructType")
                        .registerSubtype(PseudoType.class, "PseudoType")
                        .registerSubtype(AnyElementType.class, "AnyElementType")
                        .registerSubtype(AnyArrayType.class, "AnyArrayType");
        CLAZZ_TO_RUNTIME_TYPE_ADAPTOR_FACTORIES.put(com.starrocks.type.Type.class, column_type_adaptor_factory);

        // runtime adapter for class "DistributionInfo"
        final RuntimeTypeAdapterFactory<DistributionInfo> distribution_info_type_adapter_factory =
                RuntimeTypeAdapterFactory
                        .of(DistributionInfo.class, "clazz")
                        .registerSubtype(HashDistributionInfo.class, "HashDistributionInfo")
                        .registerSubtype(RandomDistributionInfo.class, "RandomDistributionInfo")
                        .registerSubtype(RangeDistributionInfo.class, "RangeDistributionInfo");
        CLAZZ_TO_RUNTIME_TYPE_ADAPTOR_FACTORIES.put(DistributionInfo.class, distribution_info_type_adapter_factory);

        final RuntimeTypeAdapterFactory<PartitionInfo> partition_info_type_adapter_factory =
                RuntimeTypeAdapterFactory
                        .of(PartitionInfo.class, "clazz")
                        .registerSubtype(RangePartitionInfo.class, "RangePartitionInfo")
                        .registerSubtype(ListPartitionInfo.class, "ListPartitionInfo")
                        .registerSubtype(SinglePartitionInfo.class, "SinglePartitionInfo")
                        .registerSubtype(ExpressionRangePartitionInfo.class, "ExpressionRangePartitionInfo")
                        .registerSubtype(ExpressionRangePartitionInfoV2.class, "ExpressionRangePartitionInfoV2");
        CLAZZ_TO_RUNTIME_TYPE_ADAPTOR_FACTORIES.put(PartitionInfo.class, partition_info_type_adapter_factory);

        // runtime adapter for class "Resource"
        final RuntimeTypeAdapterFactory<Resource> resource_type_adapter_factory = RuntimeTypeAdapterFactory
                .of(Resource.class, "clazz")
                .registerSubtype(SparkResource.class, "SparkResource")
                .registerSubtype(HiveResource.class, "HiveResource")
                .registerSubtype(IcebergResource.class, "IcebergResource")
                .registerSubtype(HudiResource.class, "HudiResource")
                .registerSubtype(OdbcCatalogResource.class, "OdbcCatalogResource")
                .registerSubtype(JDBCResource.class, "JDBCResource");
        CLAZZ_TO_RUNTIME_TYPE_ADAPTOR_FACTORIES.put(Resource.class, resource_type_adapter_factory);

        // runtime adapter for class "AlterJobV2"
        final RuntimeTypeAdapterFactory<AlterJobV2> alter_job_v2_type_adapter_factory =
                RuntimeTypeAdapterFactory.of(AlterJobV2.class, "clazz")
                        .registerSubtype(RollupJobV2.class, "RollupJobV2")
                        .registerSubtype(SchemaChangeJobV2.class, "SchemaChangeJobV2")
                        .registerSubtype(OptimizeJobV2.class, "OptimizeJobV2")
                        .registerSubtype(OnlineOptimizeJobV2.class, "OnlineOptimizeJobV2")
                        .registerSubtype(MergePartitionJob.class, "MergePartitionJob")
                        .registerSubtype(LakeTableSchemaChangeJob.class, "LakeTableSchemaChangeJob")
                        .registerSubtype(LakeTableAlterMetaJob.class, "LakeTableAlterMetaJob")
                        .registerSubtype(LakeRollupJob.class, "LakeRollupJob")
                        .registerSubtype(LakeTableAsyncFastSchemaChangeJob.class, "LakeTableFastSchemaEvolutionJob");

        CLAZZ_TO_RUNTIME_TYPE_ADAPTOR_FACTORIES.put(AlterJobV2.class, alter_job_v2_type_adapter_factory);

        // runtime adapter for class "LoadJobStateUpdateInfo"
        final RuntimeTypeAdapterFactory<LoadJob.LoadJobStateUpdateInfo>
                load_job_state_update_info_type_adapter_factory = RuntimeTypeAdapterFactory
                .of(LoadJob.LoadJobStateUpdateInfo.class, "clazz")
                .registerSubtype(SparkLoadJob.SparkLoadJobStateUpdateInfo.class, "SparkLoadJobStateUpdateInfo");
        CLAZZ_TO_RUNTIME_TYPE_ADAPTOR_FACTORIES.put(LoadJob.LoadJobStateUpdateInfo.class,
                load_job_state_update_info_type_adapter_factory);

        // runtime adapter for class "Tablet"
        final RuntimeTypeAdapterFactory<Tablet> tablet_type_adapter_factory = RuntimeTypeAdapterFactory
                .of(Tablet.class, "clazz")
                .registerSubtype(LocalTablet.class, "LocalTablet")
                .registerSubtype(LakeTablet.class, "LakeTablet");
        CLAZZ_TO_RUNTIME_TYPE_ADAPTOR_FACTORIES.put(Tablet.class,
                tablet_type_adapter_factory);
        // runtime adapter for HeartbeatResponse
        final RuntimeTypeAdapterFactory<HeartbeatResponse> heartbeat_response_adapter_factory =
                RuntimeTypeAdapterFactory
                        .of(HeartbeatResponse.class, "clazz")
                        .registerSubtype(BackendHbResponse.class, "BackendHbResponse")
                        .registerSubtype(FrontendHbResponse.class, "FrontendHbResponse")
                        .registerSubtype(BrokerHbResponse.class, "BrokerHbResponse");
        CLAZZ_TO_RUNTIME_TYPE_ADAPTOR_FACTORIES.put(HeartbeatResponse.class, heartbeat_response_adapter_factory);

        final RuntimeTypeAdapterFactory<PartitionPersistInfoV2> partition_persist_info_v_2_adapter_factory
                = RuntimeTypeAdapterFactory.of(PartitionPersistInfoV2.class, "clazz")
                .registerSubtype(ListPartitionPersistInfo.class, "ListPartitionPersistInfo")
                .registerSubtype(RangePartitionPersistInfo.class, "RangePartitionPersistInfo")
                .registerSubtype(SinglePartitionPersistInfo.class, "SinglePartitionPersistInfo");
        CLAZZ_TO_RUNTIME_TYPE_ADAPTOR_FACTORIES.put(PartitionPersistInfoV2.class,
                partition_persist_info_v_2_adapter_factory);

        final RuntimeTypeAdapterFactory<RecyclePartitionInfoV2>
                recycle_partition_info_v_2_adapter_factory
                = RuntimeTypeAdapterFactory.of(RecyclePartitionInfoV2.class, "clazz")
                .registerSubtype(RecycleRangePartitionInfo.class, "RecycleRangePartitionInfo")
                .registerSubtype(RecycleLakeRangePartitionInfo.class, "RecycleLakeRangePartitionInfo")
                .registerSubtype(RecycleListPartitionInfo.class, "RecycleListPartitionInfo")
                .registerSubtype(RecycleLakeListPartitionInfo.class, "RecycleLakeListPartitionInfo")
                .registerSubtype(RecycleUnPartitionInfo.class, "RecycleUnPartitionInfo")
                .registerSubtype(RecycleLakeUnPartitionInfo.class, "RecycleLakeUnPartitionInfo");

        CLAZZ_TO_RUNTIME_TYPE_ADAPTOR_FACTORIES.put(RecyclePartitionInfoV2.class,
                recycle_partition_info_v_2_adapter_factory);

        final RuntimeTypeAdapterFactory<com.starrocks.catalog.Table> table_type_adapter_factory
                = RuntimeTypeAdapterFactory.of(com.starrocks.catalog.Table.class, "clazz")
                .registerSubtype(EsTable.class, "EsTable")
                .registerSubtype(ExternalOlapTable.class, "ExternalOlapTable")
                .registerSubtype(FileTable.class, "FileTable")
                .registerSubtype(HiveTable.class, "HiveTable")
                .registerSubtype(HudiTable.class, "HudiTable")
                .registerSubtype(IcebergTable.class, "IcebergTable")
                .registerSubtype(JDBCTable.class, "JDBCTable")
                .registerSubtype(LakeMaterializedView.class, "LakeMaterializedView")
                .registerSubtype(LakeTable.class, "LakeTable")
                .registerSubtype(MaterializedView.class, "MaterializedView")
                .registerSubtype(MysqlTable.class, "MysqlTable")
                .registerSubtype(OlapTable.class, "OlapTable")
                .registerSubtype(View.class, "View");
        CLAZZ_TO_RUNTIME_TYPE_ADAPTOR_FACTORIES.put(com.starrocks.catalog.Table.class, table_type_adapter_factory);

        final RuntimeTypeAdapterFactory<SnapshotInfo> snapshot_info_type_adapter_factory
                = RuntimeTypeAdapterFactory.of(SnapshotInfo.class, "clazz")
                .registerSubtype(LakeTableSnapshotInfo.class, "LakeTableSnapshotInfo")
                .registerSubtype(SnapshotInfo.class, "SnapshotInfo");
        CLAZZ_TO_RUNTIME_TYPE_ADAPTOR_FACTORIES.put(SnapshotInfo.class, snapshot_info_type_adapter_factory);

        final RuntimeTypeAdapterFactory<PEntryObject> p_entry_object_runtime_type_adapter_factory =
                RuntimeTypeAdapterFactory.of(PEntryObject.class, "clazz")
                        .registerSubtype(DbPEntryObject.class, "DbPEntryObject")
                        .registerSubtype(TablePEntryObject.class, "TablePEntryObject")
                        .registerSubtype(UserPEntryObject.class, "UserPEntryObject")
                        .registerSubtype(ResourcePEntryObject.class, "ResourcePEntryObject")
                        .registerSubtype(ViewPEntryObject.class, "ViewPEntryObject")
                        .registerSubtype(MaterializedViewPEntryObject.class, "MaterializedViewPEntryObject")
                        .registerSubtype(GlobalFunctionPEntryObject.class, "GlobalFunctionPEntryObject")
                        .registerSubtype(FunctionPEntryObject.class, "FunctionPEntryObject")
                        .registerSubtype(CatalogPEntryObject.class, "CatalogPEntryObject")
                        .registerSubtype(ResourceGroupPEntryObject.class, "ResourceGroupPEntryObject")
                        .registerSubtype(StorageVolumePEntryObject.class, "StorageVolumePEntryObject")
                        .registerSubtype(WarehousePEntryObject.class, "WarehousePEntryObject")
                        .registerSubtype(PipePEntryObject.class, "PipePEntryObject")
                        .registerSubtype(PolicyFCEntryObject.class, "PolicyPEntryObject");
        CLAZZ_TO_RUNTIME_TYPE_ADAPTOR_FACTORIES.put(PEntryObject.class, p_entry_object_runtime_type_adapter_factory);

        final RuntimeTypeAdapterFactory<SecurityIntegration> sec_integration_runtime_type_adapter_factory =
                RuntimeTypeAdapterFactory.of(SecurityIntegration.class, "clazz")
                        .registerSubtype(JWTSecurityIntegration.class, "JWTSecurityIntegration")
                        .registerSubtype(SimpleLDAPSecurityIntegration.class, "SimpleLDAPSecurityIntegration")
                        .registerSubtype(OAuth2SecurityIntegration.class, "OAuth2SecurityIntegration");
        CLAZZ_TO_RUNTIME_TYPE_ADAPTOR_FACTORIES.put(SecurityIntegration.class,
                sec_integration_runtime_type_adapter_factory);

        final RuntimeTypeAdapterFactory<GroupProvider> group_provider_runtime_type_adapter_factory =
                RuntimeTypeAdapterFactory.of(GroupProvider.class, "clazz")
                        .registerSubtype(FileGroupProvider.class, "FileGroupProvider")
                        .registerSubtype(UnixGroupProvider.class, "UnixGroupProvider")
                        .registerSubtype(LDAPGroupProvider.class, "LDAPGroupProvider");
        CLAZZ_TO_RUNTIME_TYPE_ADAPTOR_FACTORIES.put(GroupProvider.class, group_provider_runtime_type_adapter_factory);

        final RuntimeTypeAdapterFactory<Warehouse> warehouse_type_adapter_factory = RuntimeTypeAdapterFactory
                .of(Warehouse.class, "clazz")
                .registerSubtype(DefaultWarehouse.class, "DefaultWarehouse");
        CLAZZ_TO_RUNTIME_TYPE_ADAPTOR_FACTORIES.put(Warehouse.class, warehouse_type_adapter_factory);

        final RuntimeTypeAdapterFactory<LoadJob> load_job_type_runtime_adapter_factory =
                RuntimeTypeAdapterFactory.of(LoadJob.class, "clazz")
                        .registerSubtype(InsertLoadJob.class, "InsertLoadJob")
                        .registerSubtype(SparkLoadJob.class, "SparkLoadJob")
                        .registerSubtype(BrokerLoadJob.class, "BrokerLoadJob");
        CLAZZ_TO_RUNTIME_TYPE_ADAPTOR_FACTORIES.put(LoadJob.class, load_job_type_runtime_adapter_factory);

        final RuntimeTypeAdapterFactory<TxnCommitAttachment> txn_commit_attachment_type_runtime_adapter_factory =
                RuntimeTypeAdapterFactory.of(TxnCommitAttachment.class, "clazz")
                        .registerSubtype(InsertTxnCommitAttachment.class, "InsertTxnCommitAttachment")
                        .registerSubtype(LoadJobFinalOperation.class, "LoadJobFinalOperation")
                        .registerSubtype(ManualLoadTxnCommitAttachment.class, "ManualLoadTxnCommitAttachment")
                        .registerSubtype(MiniLoadTxnCommitAttachment.class, "MiniLoadTxnCommitAttachment")
                        .registerSubtype(RLTaskTxnCommitAttachment.class, "RLTaskTxnCommitAttachment")
                        .registerSubtype(StreamLoadTxnCommitAttachment.class, "StreamLoadTxnCommitAttachment")
                        .registerSubtype(ReplicationTxnCommitAttachment.class, "ReplicationTxnCommitAttachment")
                        .registerSubtype(CompactionTxnCommitAttachment.class, "CompactionTxnCommitAttachment");
        CLAZZ_TO_RUNTIME_TYPE_ADAPTOR_FACTORIES.put(TxnCommitAttachment.class,
                txn_commit_attachment_type_runtime_adapter_factory);

        final RuntimeTypeAdapterFactory<RoutineLoadProgress> routine_load_progress_type_runtime_adapter_factory =
                RuntimeTypeAdapterFactory.of(RoutineLoadProgress.class, "clazz")
                        .registerSubtype(KafkaProgress.class, "KafkaProgress")
                        .registerSubtype(PulsarProgress.class, "PulsarProgress");
        CLAZZ_TO_RUNTIME_TYPE_ADAPTOR_FACTORIES.put(RoutineLoadProgress.class,
                routine_load_progress_type_runtime_adapter_factory);

        final RuntimeTypeAdapterFactory<RoutineLoadJob> routine_load_job_type_runtime_adapter_factory =
                RuntimeTypeAdapterFactory.of(RoutineLoadJob.class, "clazz")
                        .registerSubtype(KafkaRoutineLoadJob.class, "KafkaRoutineLoadJob")
                        .registerSubtype(PulsarRoutineLoadJob.class, "PulsarRoutineLoadJob");
        CLAZZ_TO_RUNTIME_TYPE_ADAPTOR_FACTORIES.put(RoutineLoadJob.class,
                routine_load_job_type_runtime_adapter_factory);

        final RuntimeTypeAdapterFactory<AbstractJob> abstract_job_type_runtime_adapter_factory =
                RuntimeTypeAdapterFactory.of(AbstractJob.class, "clazz")
                        .registerSubtype(BackupJob.class, "BackupJob")
                        .registerSubtype(LakeBackupJob.class, "LakeBackupJob")
                        .registerSubtype(RestoreJob.class, "RestoreJob")
                        .registerSubtype(LakeRestoreJob.class, "LakeRestoreJob");
        CLAZZ_TO_RUNTIME_TYPE_ADAPTOR_FACTORIES.put(AbstractJob.class, abstract_job_type_runtime_adapter_factory);

        final RuntimeTypeAdapterFactory<Function> function_type_runtime_adapter_factory =
                RuntimeTypeAdapterFactory.of(Function.class, "clazz")
                        .registerSubtype(ScalarFunction.class, "ScalarFunction")
                        .registerSubtype(AggregateFunction.class, "AggregateFunction")
                        .registerSubtype(TableFunction.class, "TableFunction")
                        .registerSubtype(SqlFunction.class, "SqlFunction");
        CLAZZ_TO_RUNTIME_TYPE_ADAPTOR_FACTORIES.put(Function.class, function_type_runtime_adapter_factory);

        final RuntimeTypeAdapterFactory<StorageVolumeMgr> storage_volume_mgr_type_runtime_adapter_factory =
                RuntimeTypeAdapterFactory.of(StorageVolumeMgr.class, "clazz")
                        .registerSubtype(SharedNothingStorageVolumeMgr.class, "SharedNothingStorageVolumeMgr")
                        .registerSubtype(SharedDataStorageVolumeMgr.class, "SharedDataStorageVolumeMgr");
        CLAZZ_TO_RUNTIME_TYPE_ADAPTOR_FACTORIES.put(StorageVolumeMgr.class,
                storage_volume_mgr_type_runtime_adapter_factory);

        final RuntimeTypeAdapterFactory<AnalyzeStatus> ANALYZE_STATUS_RUNTIME_TYPE_ADAPTER_FACTORY =
                RuntimeTypeAdapterFactory.of(AnalyzeStatus.class, "clazz")
                        .registerSubtype(NativeAnalyzeStatus.class, "NativeAnalyzeStatus", true)
                        .registerSubtype(ExternalAnalyzeStatus.class, "ExternalAnalyzeStatus");
        CLAZZ_TO_RUNTIME_TYPE_ADAPTOR_FACTORIES.put(AnalyzeStatus.class, ANALYZE_STATUS_RUNTIME_TYPE_ADAPTER_FACTORY);

        final RuntimeTypeAdapterFactory<AnalyzeJob> ANALYZE_JOB_RUNTIME_TYPE_ADAPTER_FACTORY =
                RuntimeTypeAdapterFactory.of(AnalyzeJob.class, "clazz")
                        .registerSubtype(NativeAnalyzeJob.class, "NativeAnalyzeJob", true)
                        .registerSubtype(ExternalAnalyzeJob.class, "ExternalAnalyzeJob");
        CLAZZ_TO_RUNTIME_TYPE_ADAPTOR_FACTORIES.put(AnalyzeJob.class, ANALYZE_JOB_RUNTIME_TYPE_ADAPTER_FACTORY);

        final RuntimeTypeAdapterFactory<ComputeResource> compute_resource_runtime_type_adapter_factory =
                RuntimeTypeAdapterFactory.of(ComputeResource.class, "clazz")
                        .registerSubtype(WarehouseComputeResource.class, "WarehouseComputeResource", true);
        CLAZZ_TO_RUNTIME_TYPE_ADAPTOR_FACTORIES.put(ComputeResource.class,
                compute_resource_runtime_type_adapter_factory);

        final RuntimeTypeAdapterFactory<TabletReshardJob> tablet_reshard_job_runtime_type_adapter_factory =
                RuntimeTypeAdapterFactory.of(TabletReshardJob.class, "clazz")
                        .registerSubtype(SplitTabletJob.class, "SplitTabletJob")
                        .registerSubtype(MergeTabletJob.class, "MergeTabletJob");
        CLAZZ_TO_RUNTIME_TYPE_ADAPTOR_FACTORIES.put(TabletReshardJob.class,
                tablet_reshard_job_runtime_type_adapter_factory);

        final RuntimeTypeAdapterFactory<ReshardingTablet> resharding_tablet_runtime_type_adapter_factory =
                RuntimeTypeAdapterFactory.of(ReshardingTablet.class, "clazz")
                        .registerSubtype(SplittingTablet.class, "SplittingTablet")
                        .registerSubtype(MergingTablet.class, "MergingTablet")
                        .registerSubtype(IdenticalTablet.class, "IdenticalTablet");
        CLAZZ_TO_RUNTIME_TYPE_ADAPTOR_FACTORIES.put(ReshardingTablet.class,
                resharding_tablet_runtime_type_adapter_factory);

        final RuntimeTypeAdapterFactory<Variant> variant_runtime_type_adapter_factory =
                RuntimeTypeAdapterFactory.of(Variant.class, "clazz")
                        .registerSubtype(BoolVariant.class, "BoolVariant")
                        .registerSubtype(IntVariant.class, "IntVariant")
                        .registerSubtype(LargeIntVariant.class, "LargeIntVariant")
                        .registerSubtype(StringVariant.class, "StringVariant")
                        .registerSubtype(DateVariant.class, "DateVariant");
        CLAZZ_TO_RUNTIME_TYPE_ADAPTOR_FACTORIES.put(Variant.class, variant_runtime_type_adapter_factory);

        final RuntimeTypeAdapterFactory<TvrVersionRange> tvr_delta_runtime_type_adapter_factory =
                RuntimeTypeAdapterFactory.of(TvrVersionRange.class, "clazz")
                        .registerSubtype(TvrTableSnapshot.class, "TvrTableSnapshot")
                        .registerSubtype(TvrTableDelta.class, "TvrTableDelta");
        CLAZZ_TO_RUNTIME_TYPE_ADAPTOR_FACTORIES.put(TvrVersionRange.class, tvr_delta_runtime_type_adapter_factory);

        final RuntimeTypeAdapterFactory<ReplicationJob> replication_job_type_adapter_factory =
                RuntimeTypeAdapterFactory.of(ReplicationJob.class, "clazz")
                        .registerSubtype(ReplicationJob.class, "ReplicationJob", true)
                        .registerSubtype(LakeReplicationJob.class, "LakeReplicationJob");
        CLAZZ_TO_RUNTIME_TYPE_ADAPTOR_FACTORIES.put(ReplicationJob.class, replication_job_type_adapter_factory);
    }

    public static final JsonSerializer<LocalDateTime> LOCAL_DATE_TIME_TYPE_SERIALIZER =
            (dateTime, type, jsonSerializationContext) -> new JsonPrimitive(dateTime.toEpochSecond(ZoneOffset.UTC));

    public static final JsonDeserializer<LocalDateTime> LOCAL_DATE_TIME_TYPE_DESERIALIZER =
            (jsonElement, type, jsonDeserializationContext) -> LocalDateTime
                    .ofEpochSecond(jsonElement.getAsJsonPrimitive().getAsLong(), 0, ZoneOffset.UTC);

    public static final JsonSerializer<QueryDumpInfo> DUMP_INFO_SERIALIZER = new QueryDumpSerializer();

    public static final JsonDeserializer<QueryDumpInfo> DUMP_INFO_DESERIALIZER = new QueryDumpDeserializer();

    public static final JsonSerializer<HiveTableDumpInfo> HIVE_TABLE_DUMP_INFO_SERIALIZER = new HiveTableDumpInfo.
            HiveTableDumpInfoSerializer();

    public static final JsonDeserializer<HiveTableDumpInfo> HIVE_TABLE_DUMP_INFO_DESERIALIZER = new HiveTableDumpInfo.
            HiveTableDumpInfoDeserializer();

    //public static final JsonSerializer<Expr> EXPRESSION_SERIALIZER = new ExpressionSerializer();

    //public static final JsonDeserializer<Expr> EXPRESSION_DESERIALIZER = new ExpressionDeserializer();

    public static final JsonDeserializer<PrimitiveType> PRIMITIVE_TYPE_DESERIALIZER = new PrimitiveTypeDeserializer();

}
