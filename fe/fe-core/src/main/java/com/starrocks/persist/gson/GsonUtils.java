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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/persist/gson/GsonUtils.java

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

package com.starrocks.persist.gson;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;
import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.starrocks.alter.AlterJobV2;
import com.starrocks.alter.LakeTableAlterMetaJob;
import com.starrocks.alter.LakeTableSchemaChangeJob;
import com.starrocks.alter.RollupJobV2;
import com.starrocks.alter.SchemaChangeJobV2;
import com.starrocks.authentication.LDAPSecurityIntegration;
import com.starrocks.authentication.SecurityIntegration;
import com.starrocks.backup.AbstractJob;
import com.starrocks.backup.BackupJob;
import com.starrocks.backup.RestoreJob;
import com.starrocks.backup.SnapshotInfo;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.AnyArrayType;
import com.starrocks.catalog.AnyElementType;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.CatalogRecycleBin;
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
import com.starrocks.catalog.JDBCResource;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MysqlTable;
import com.starrocks.catalog.OdbcCatalogResource;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.PseudoType;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Resource;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.SparkResource;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.TableFunction;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.View;
import com.starrocks.lake.LakeMaterializedView;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.backup.LakeBackupJob;
import com.starrocks.lake.backup.LakeRestoreJob;
import com.starrocks.lake.backup.LakeTableSnapshotInfo;
import com.starrocks.load.loadv2.BrokerLoadJob;
import com.starrocks.load.loadv2.InsertLoadJob;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.load.loadv2.LoadJob.LoadJobStateUpdateInfo;
import com.starrocks.load.loadv2.LoadJobFinalOperation;
import com.starrocks.load.loadv2.ManualLoadTxnCommitAttachment;
import com.starrocks.load.loadv2.MiniLoadTxnCommitAttachment;
import com.starrocks.load.loadv2.SparkLoadJob;
import com.starrocks.load.loadv2.SparkLoadJob.SparkLoadJobStateUpdateInfo;
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
import com.starrocks.privilege.CatalogPEntryObject;
import com.starrocks.privilege.DbPEntryObject;
import com.starrocks.privilege.FunctionPEntryObject;
import com.starrocks.privilege.GlobalFunctionPEntryObject;
import com.starrocks.privilege.MaterializedViewPEntryObject;
import com.starrocks.privilege.PEntryObject;
import com.starrocks.privilege.PolicyFCEntryObject;
import com.starrocks.privilege.ResourceGroupPEntryObject;
import com.starrocks.privilege.ResourcePEntryObject;
import com.starrocks.privilege.StorageVolumePEntryObject;
import com.starrocks.privilege.TablePEntryObject;
import com.starrocks.privilege.UserPEntryObject;
import com.starrocks.privilege.ViewPEntryObject;
import com.starrocks.privilege.WarehouseFCPEntryObject;
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
import com.starrocks.warehouse.LocalWarehouse;
import com.starrocks.warehouse.Warehouse;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/*
 * Some utilities about Gson.
 * User should get GSON instance from this class to do the serialization.
 *
 *      GsonUtils.GSON.toJson(...)
 *      GsonUtils.GSON.fromJson(...)
 *
 * More example can be seen in unit test case: "com.starrocks.common.util.GsonSerializationTest.java".
 *
 * For inherited class serialization, see "com.starrocks.common.util.GsonDerivedClassSerializationTest.java"
 *
 * And developers may need to add other serialization adapters for custom complex java classes.
 * You need implement a class to implements JsonSerializer and JsonDeserializer, and register it to GSON_BUILDER.
 * See the following "GuavaTableAdapter" and "GuavaMultimapAdapter" for example.
 */
public class GsonUtils {

    // runtime adapter for class "Type"
    private static final RuntimeTypeAdapterFactory<com.starrocks.catalog.Type> COLUMN_TYPE_ADAPTER_FACTORY =
            RuntimeTypeAdapterFactory
                    .of(com.starrocks.catalog.Type.class, "clazz")
                    .registerSubtype(ScalarType.class, "ScalarType")
                    .registerSubtype(ArrayType.class, "ArrayType")
                    .registerSubtype(MapType.class, "MapType")
                    .registerSubtype(StructType.class, "StructType")
                    .registerSubtype(PseudoType.class, "PseudoType")
                    .registerSubtype(AnyElementType.class, "AnyElementType")
                    .registerSubtype(AnyArrayType.class, "AnyArrayType");

    // runtime adapter for class "DistributionInfo"
    private static final RuntimeTypeAdapterFactory<DistributionInfo> DISTRIBUTION_INFO_TYPE_ADAPTER_FACTORY =
            RuntimeTypeAdapterFactory
                    .of(DistributionInfo.class, "clazz")
                    .registerSubtype(HashDistributionInfo.class, "HashDistributionInfo")
                    .registerSubtype(RandomDistributionInfo.class, "RandomDistributionInfo");

    private static final RuntimeTypeAdapterFactory<PartitionInfo> PARTITION_INFO_TYPE_ADAPTER_FACTORY =
            RuntimeTypeAdapterFactory
                    .of(PartitionInfo.class, "clazz")
                    .registerSubtype(RangePartitionInfo.class, "RangePartitionInfo")
                    .registerSubtype(ListPartitionInfo.class, "ListPartitionInfo")
                    .registerSubtype(SinglePartitionInfo.class, "SinglePartitionInfo")
                    .registerSubtype(ExpressionRangePartitionInfo.class, "ExpressionRangePartitionInfo")
                    .registerSubtype(ExpressionRangePartitionInfoV2.class, "ExpressionRangePartitionInfoV2");

    // runtime adapter for class "Resource"
    private static final RuntimeTypeAdapterFactory<Resource> RESOURCE_TYPE_ADAPTER_FACTORY = RuntimeTypeAdapterFactory
            .of(Resource.class, "clazz")
            .registerSubtype(SparkResource.class, "SparkResource")
            .registerSubtype(HiveResource.class, "HiveResource")
            .registerSubtype(IcebergResource.class, "IcebergResource")
            .registerSubtype(HudiResource.class, "HudiResource")
            .registerSubtype(OdbcCatalogResource.class, "OdbcCatalogResource")
            .registerSubtype(JDBCResource.class, "JDBCResource");

    // runtime adapter for class "AlterJobV2"
    private static final RuntimeTypeAdapterFactory<AlterJobV2> ALTER_JOB_V2_TYPE_ADAPTER_FACTORY =
            RuntimeTypeAdapterFactory.of(AlterJobV2.class, "clazz")
                    .registerSubtype(RollupJobV2.class, "RollupJobV2")
                    .registerSubtype(SchemaChangeJobV2.class, "SchemaChangeJobV2")
                    .registerSubtype(LakeTableSchemaChangeJob.class, "LakeTableSchemaChangeJob")
                    .registerSubtype(LakeTableAlterMetaJob.class, "LakeTableAlterMetaJob");

    // runtime adapter for class "LoadJobStateUpdateInfo"
    private static final RuntimeTypeAdapterFactory<LoadJobStateUpdateInfo>
            LOAD_JOB_STATE_UPDATE_INFO_TYPE_ADAPTER_FACTORY = RuntimeTypeAdapterFactory
            .of(LoadJobStateUpdateInfo.class, "clazz")
            .registerSubtype(SparkLoadJobStateUpdateInfo.class, "SparkLoadJobStateUpdateInfo");

    // runtime adapter for class "Tablet"
    private static final RuntimeTypeAdapterFactory<Tablet> TABLET_TYPE_ADAPTER_FACTORY = RuntimeTypeAdapterFactory
            .of(Tablet.class, "clazz")
            .registerSubtype(LocalTablet.class, "LocalTablet")
            .registerSubtype(LakeTablet.class, "LakeTablet");

    // runtime adapter for HeartbeatResponse
    private static final RuntimeTypeAdapterFactory<HeartbeatResponse> HEARTBEAT_RESPONSE_ADAPTER_FACTOR =
            RuntimeTypeAdapterFactory
                    .of(HeartbeatResponse.class, "clazz")
                    .registerSubtype(BackendHbResponse.class, "BackendHbResponse")
                    .registerSubtype(FrontendHbResponse.class, "FrontendHbResponse")
                    .registerSubtype(BrokerHbResponse.class, "BrokerHbResponse");

    private static final RuntimeTypeAdapterFactory<PartitionPersistInfoV2> PARTITION_PERSIST_INFO_V_2_ADAPTER_FACTORY
            = RuntimeTypeAdapterFactory.of(PartitionPersistInfoV2.class, "clazz")
            .registerSubtype(ListPartitionPersistInfo.class, "ListPartitionPersistInfo")
            .registerSubtype(RangePartitionPersistInfo.class, "RangePartitionPersistInfo")
            .registerSubtype(SinglePartitionPersistInfo.class, "SinglePartitionPersistInfo");

    private static final RuntimeTypeAdapterFactory<CatalogRecycleBin.RecyclePartitionInfoV2>
            RECYCLE_PARTITION_INFO_V_2_ADAPTER_FACTORY
            = RuntimeTypeAdapterFactory.of(CatalogRecycleBin.RecyclePartitionInfoV2.class, "clazz")
            .registerSubtype(CatalogRecycleBin.RecycleRangePartitionInfo.class, "RecycleRangePartitionInfo");

    private static final RuntimeTypeAdapterFactory<com.starrocks.catalog.Table> TABLE_TYPE_ADAPTER_FACTORY
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

    private static final RuntimeTypeAdapterFactory<SnapshotInfo> SNAPSHOT_INFO_TYPE_ADAPTER_FACTORY
            = RuntimeTypeAdapterFactory.of(SnapshotInfo.class, "clazz")
            .registerSubtype(LakeTableSnapshotInfo.class, "LakeTableSnapshotInfo")
            .registerSubtype(SnapshotInfo.class, "SnapshotInfo");

    private static final RuntimeTypeAdapterFactory<PEntryObject> P_ENTRY_OBJECT_RUNTIME_TYPE_ADAPTER_FACTORY =
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
                    .registerSubtype(WarehouseFCPEntryObject.class, "WarehousePEntryObject")
                    .registerSubtype(PolicyFCEntryObject.class, "PolicyPEntryObject");

    private static final RuntimeTypeAdapterFactory<SecurityIntegration> SEC_INTEGRATION_RUNTIME_TYPE_ADAPTER_FACTORY =
            RuntimeTypeAdapterFactory.of(SecurityIntegration.class, "clazz")
                    .registerSubtype(LDAPSecurityIntegration.class, "LDAPSecurityIntegration");

    private static final RuntimeTypeAdapterFactory<Warehouse> WAREHOUSE_TYPE_ADAPTER_FACTORY = RuntimeTypeAdapterFactory
            .of(Warehouse.class, "clazz")
            .registerSubtype(LocalWarehouse.class, "LocalWarehouse");

    public static final RuntimeTypeAdapterFactory<LoadJob> LOAD_JOB_TYPE_RUNTIME_ADAPTER_FACTORY =
            RuntimeTypeAdapterFactory.of(LoadJob.class, "clazz")
                    .registerSubtype(InsertLoadJob.class, "InsertLoadJob")
                    .registerSubtype(SparkLoadJob.class, "SparkLoadJob")
                    .registerSubtype(BrokerLoadJob.class, "BrokerLoadJob");

    public static final RuntimeTypeAdapterFactory<TxnCommitAttachment> TXN_COMMIT_ATTACHMENT_TYPE_RUNTIME_ADAPTER_FACTORY =
            RuntimeTypeAdapterFactory.of(TxnCommitAttachment.class, "clazz")
                    .registerSubtype(InsertTxnCommitAttachment.class, "InsertTxnCommitAttachment")
                    .registerSubtype(LoadJobFinalOperation.class, "LoadJobFinalOperation")
                    .registerSubtype(ManualLoadTxnCommitAttachment.class, "ManualLoadTxnCommitAttachment")
                    .registerSubtype(MiniLoadTxnCommitAttachment.class, "MiniLoadTxnCommitAttachment")
                    .registerSubtype(RLTaskTxnCommitAttachment.class, "RLTaskTxnCommitAttachment")
                    .registerSubtype(StreamLoadTxnCommitAttachment.class, "StreamLoadTxnCommitAttachment");

    public static final RuntimeTypeAdapterFactory<RoutineLoadProgress> ROUTINE_LOAD_PROGRESS_TYPE_RUNTIME_ADAPTER_FACTORY =
            RuntimeTypeAdapterFactory.of(RoutineLoadProgress.class, "clazz")
                    .registerSubtype(KafkaProgress.class, "KafkaProgress")
                    .registerSubtype(PulsarProgress.class, "PulsarProgress");

    public static final RuntimeTypeAdapterFactory<RoutineLoadJob> ROUTINE_LOAD_JOB_TYPE_RUNTIME_ADAPTER_FACTORY =
            RuntimeTypeAdapterFactory.of(RoutineLoadJob.class, "clazz")
                    .registerSubtype(KafkaRoutineLoadJob.class, "KafkaRoutineLoadJob")
                    .registerSubtype(PulsarRoutineLoadJob.class, "PulsarRoutineLoadJob");

    public static final RuntimeTypeAdapterFactory<AbstractJob> ABSTRACT_JOB_TYPE_RUNTIME_ADAPTER_FACTORY =
            RuntimeTypeAdapterFactory.of(AbstractJob.class, "clazz")
                    .registerSubtype(BackupJob.class, "BackupJob")
                    .registerSubtype(LakeBackupJob.class, "LakeBackupJob")
                    .registerSubtype(RestoreJob.class, "RestoreJob")
                    .registerSubtype(LakeRestoreJob.class, "LakeRestoreJob");

    public static final RuntimeTypeAdapterFactory<Function> FUNCTION_TYPE_RUNTIME_ADAPTER_FACTORY =
            RuntimeTypeAdapterFactory.of(Function.class, "clazz")
                    .registerSubtype(ScalarFunction.class, "ScalarFunction")
                    .registerSubtype(AggregateFunction.class, "AggregateFunction")
                    .registerSubtype(TableFunction.class, "TableFunction");

    public static final RuntimeTypeAdapterFactory<StorageVolumeMgr> STORAGE_VOLUME_MGR_TYPE_RUNTIME_ADAPTER_FACTORY =
            RuntimeTypeAdapterFactory.of(StorageVolumeMgr.class, "clazz")
                    .registerSubtype(SharedNothingStorageVolumeMgr.class, "SharedNothingStorageVolumeMgr")
                    .registerSubtype(SharedDataStorageVolumeMgr.class, "SharedDataStorageVolumeMgr");

    public static final RuntimeTypeAdapterFactory<AnalyzeStatus> ANALYZE_STATUS_RUNTIME_TYPE_ADAPTER_FACTORY =
            RuntimeTypeAdapterFactory.of(AnalyzeStatus.class, "clazz")
                    .registerSubtype(NativeAnalyzeStatus.class, "NativeAnalyzeStatus", true)
                    .registerSubtype(ExternalAnalyzeStatus.class, "ExternalAnalyzeStatus");

    public static final RuntimeTypeAdapterFactory<AnalyzeJob> ANALYZE_JOB_RUNTIME_TYPE_ADAPTER_FACTORY =
            RuntimeTypeAdapterFactory.of(AnalyzeJob.class, "clazz")
                    .registerSubtype(NativeAnalyzeJob.class, "NativeAnalyzeJob", true)
                    .registerSubtype(ExternalAnalyzeJob.class, "ExternalAnalyzeJob");


    private static final JsonSerializer<LocalDateTime> LOCAL_DATE_TIME_TYPE_SERIALIZER =
            (dateTime, type, jsonSerializationContext) -> new JsonPrimitive(dateTime.toEpochSecond(ZoneOffset.UTC));

    private static final JsonDeserializer<LocalDateTime> LOCAL_DATE_TIME_TYPE_DESERIALIZER =
            (jsonElement, type, jsonDeserializationContext) -> LocalDateTime
                    .ofEpochSecond(jsonElement.getAsJsonPrimitive().getAsLong(), 0, ZoneOffset.UTC);

    private static final JsonSerializer<QueryDumpInfo> DUMP_INFO_SERIALIZER = new QueryDumpSerializer();

    private static final JsonDeserializer<QueryDumpInfo> DUMP_INFO_DESERIALIZER = new QueryDumpDeserializer();

    private static final JsonSerializer<HiveTableDumpInfo> HIVE_TABLE_DUMP_INFO_SERIALIZER = new HiveTableDumpInfo.
            HiveTableDumpInfoSerializer();

    private static final JsonDeserializer<HiveTableDumpInfo> HIVE_TABLE_DUMP_INFO_DESERIALIZER = new HiveTableDumpInfo.
            HiveTableDumpInfoDeserializer();

    //private static final JsonSerializer<Expr> EXPRESSION_SERIALIZER = new ExpressionSerializer();

    //private static final JsonDeserializer<Expr> EXPRESSION_DESERIALIZER = new ExpressionDeserializer();

    private static final JsonDeserializer<PrimitiveType> PRIMITIVE_TYPE_DESERIALIZER = new PrimitiveTypeDeserializer();

    // the builder of GSON instance.
    // Add any other adapters if necessary.
    private static final GsonBuilder GSON_BUILDER = new GsonBuilder()
            .addSerializationExclusionStrategy(new HiddenAnnotationExclusionStrategy())
            .addDeserializationExclusionStrategy(new HiddenAnnotationExclusionStrategy())
            .enableComplexMapKeySerialization()
            .registerTypeHierarchyAdapter(Table.class, new GuavaTableAdapter())
            .registerTypeHierarchyAdapter(Multimap.class, new GuavaMultimapAdapter())
            .registerTypeAdapterFactory(new ProcessHookTypeAdapterFactory())
            // For call constructor with selectedFields
            .registerTypeAdapter(MapType.class, new MapType.MapTypeDeserializer())
            .registerTypeAdapter(StructType.class, new StructType.StructTypeDeserializer())
            .registerTypeAdapterFactory(COLUMN_TYPE_ADAPTER_FACTORY)
            .registerTypeAdapterFactory(DISTRIBUTION_INFO_TYPE_ADAPTER_FACTORY)
            .registerTypeAdapterFactory(RESOURCE_TYPE_ADAPTER_FACTORY)
            .registerTypeAdapterFactory(ALTER_JOB_V2_TYPE_ADAPTER_FACTORY)
            .registerTypeAdapterFactory(LOAD_JOB_STATE_UPDATE_INFO_TYPE_ADAPTER_FACTORY)
            .registerTypeAdapterFactory(TABLET_TYPE_ADAPTER_FACTORY)
            .registerTypeAdapterFactory(HEARTBEAT_RESPONSE_ADAPTER_FACTOR)
            .registerTypeAdapterFactory(PARTITION_INFO_TYPE_ADAPTER_FACTORY)
            .registerTypeAdapterFactory(PARTITION_PERSIST_INFO_V_2_ADAPTER_FACTORY)
            .registerTypeAdapterFactory(RECYCLE_PARTITION_INFO_V_2_ADAPTER_FACTORY)
            .registerTypeAdapterFactory(TABLE_TYPE_ADAPTER_FACTORY)
            .registerTypeAdapterFactory(SNAPSHOT_INFO_TYPE_ADAPTER_FACTORY)
            .registerTypeAdapterFactory(P_ENTRY_OBJECT_RUNTIME_TYPE_ADAPTER_FACTORY)
            .registerTypeAdapterFactory(SEC_INTEGRATION_RUNTIME_TYPE_ADAPTER_FACTORY)
            .registerTypeAdapterFactory(WAREHOUSE_TYPE_ADAPTER_FACTORY)
            .registerTypeAdapterFactory(LOAD_JOB_TYPE_RUNTIME_ADAPTER_FACTORY)
            .registerTypeAdapterFactory(TXN_COMMIT_ATTACHMENT_TYPE_RUNTIME_ADAPTER_FACTORY)
            .registerTypeAdapterFactory(ROUTINE_LOAD_PROGRESS_TYPE_RUNTIME_ADAPTER_FACTORY)
            .registerTypeAdapterFactory(ROUTINE_LOAD_JOB_TYPE_RUNTIME_ADAPTER_FACTORY)
            .registerTypeAdapterFactory(ABSTRACT_JOB_TYPE_RUNTIME_ADAPTER_FACTORY)
            .registerTypeAdapterFactory(FUNCTION_TYPE_RUNTIME_ADAPTER_FACTORY)
            .registerTypeAdapterFactory(STORAGE_VOLUME_MGR_TYPE_RUNTIME_ADAPTER_FACTORY)
            .registerTypeAdapterFactory(ANALYZE_STATUS_RUNTIME_TYPE_ADAPTER_FACTORY)
            .registerTypeAdapterFactory(ANALYZE_JOB_RUNTIME_TYPE_ADAPTER_FACTORY)
            .registerTypeAdapter(LocalDateTime.class, LOCAL_DATE_TIME_TYPE_SERIALIZER)
            .registerTypeAdapter(LocalDateTime.class, LOCAL_DATE_TIME_TYPE_DESERIALIZER)
            .registerTypeAdapter(QueryDumpInfo.class, DUMP_INFO_SERIALIZER)
            .registerTypeAdapter(QueryDumpInfo.class, DUMP_INFO_DESERIALIZER)
            .registerTypeAdapter(HiveTableDumpInfo.class, HIVE_TABLE_DUMP_INFO_SERIALIZER)
            .registerTypeAdapter(HiveTableDumpInfo.class, HIVE_TABLE_DUMP_INFO_DESERIALIZER)
            .registerTypeAdapter(PrimitiveType.class, PRIMITIVE_TYPE_DESERIALIZER);

    // this instance is thread-safe.
    public static final Gson GSON = GSON_BUILDER.create();

    /*
     * The exclusion strategy of GSON serialization.
     * Any fields without "@SerializedName" annotation with be ignored with
     * serializing and deserializing.
     */
    public static class HiddenAnnotationExclusionStrategy implements ExclusionStrategy {
        public boolean shouldSkipField(FieldAttributes f) {
            return f.getAnnotation(SerializedName.class) == null;
        }

        @Override
        public boolean shouldSkipClass(Class<?> clazz) {
            return false;
        }
    }

    /*
     *
     * The json adapter for Guava Table.
     * Current support:
     * 1. HashBasedTable
     *
     * The RowKey, ColumnKey and Value classes in Table should also be serializable.
     *
     * What is Adapter and Why we should implement it?
     *
     * Adapter is mainly used to provide serialization and deserialization methods for some complex classes.
     * Complex classes here usually refer to classes that are complex and cannot be modified.
     * These classes mainly include third-party library classes or some inherited classes.
     */
    private static class GuavaTableAdapter<R, C, V>
            implements JsonSerializer<Table<R, C, V>>, JsonDeserializer<Table<R, C, V>> {
        /*
         * serialize Table<R, C, V> as:
         * {
         * "rowKeys": [ "rowKey1", "rowKey2", ...],
         * "columnKeys": [ "colKey1", "colKey2", ...],
         * "cells" : [[0, 0, value1], [0, 1, value2], ...]
         * }
         *
         * the [0, 0] .. in cells are the indexes of rowKeys array and columnKeys array.
         * This serialization method can reduce the size of json string because it
         * replace the same row key
         * and column key to integer.
         */
        @Override
        public JsonElement serialize(Table<R, C, V> src, Type typeOfSrc, JsonSerializationContext context) {
            JsonArray rowKeysJsonArray = new JsonArray();
            Map<R, Integer> rowKeyToIndex = new HashMap<>();
            for (R rowKey : src.rowKeySet()) {
                rowKeyToIndex.put(rowKey, rowKeyToIndex.size());
                rowKeysJsonArray.add(context.serialize(rowKey));
            }
            JsonArray columnKeysJsonArray = new JsonArray();
            Map<C, Integer> columnKeyToIndex = new HashMap<>();
            for (C columnKey : src.columnKeySet()) {
                columnKeyToIndex.put(columnKey, columnKeyToIndex.size());
                columnKeysJsonArray.add(context.serialize(columnKey));
            }
            JsonArray cellsJsonArray = new JsonArray();
            for (Table.Cell<R, C, V> cell : src.cellSet()) {
                int rowIndex = rowKeyToIndex.get(cell.getRowKey());
                int columnIndex = columnKeyToIndex.get(cell.getColumnKey());
                cellsJsonArray.add(rowIndex);
                cellsJsonArray.add(columnIndex);
                cellsJsonArray.add(context.serialize(cell.getValue()));
            }
            JsonObject tableJsonObject = new JsonObject();
            tableJsonObject.addProperty("clazz", src.getClass().getSimpleName());
            tableJsonObject.add("rowKeys", rowKeysJsonArray);
            tableJsonObject.add("columnKeys", columnKeysJsonArray);
            tableJsonObject.add("cells", cellsJsonArray);
            return tableJsonObject;
        }

        @Override
        public Table<R, C, V> deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) {
            Type typeOfR;
            Type typeOfC;
            Type typeOfV;
            {
                ParameterizedType parameterizedType = (ParameterizedType) typeOfT;
                Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
                typeOfR = actualTypeArguments[0];
                typeOfC = actualTypeArguments[1];
                typeOfV = actualTypeArguments[2];
            }
            JsonObject tableJsonObject = json.getAsJsonObject();
            String tableClazz = tableJsonObject.get("clazz").getAsString();
            JsonArray rowKeysJsonArray = tableJsonObject.getAsJsonArray("rowKeys");
            Map<Integer, R> rowIndexToKey = new HashMap<>();
            for (JsonElement jsonElement : rowKeysJsonArray) {
                R rowKey = context.deserialize(jsonElement, typeOfR);
                rowIndexToKey.put(rowIndexToKey.size(), rowKey);
            }
            JsonArray columnKeysJsonArray = tableJsonObject.getAsJsonArray("columnKeys");
            Map<Integer, C> columnIndexToKey = new HashMap<>();
            for (JsonElement jsonElement : columnKeysJsonArray) {
                C columnKey = context.deserialize(jsonElement, typeOfC);
                columnIndexToKey.put(columnIndexToKey.size(), columnKey);
            }
            JsonArray cellsJsonArray = tableJsonObject.getAsJsonArray("cells");
            Table<R, C, V> table = null;
            switch (tableClazz) {
                case "HashBasedTable":
                    table = HashBasedTable.create();
                    break;
                default:
                    Preconditions.checkState(false, "unknown guava table class: " + tableClazz);
                    break;
            }
            for (int i = 0; i < cellsJsonArray.size(); i = i + 3) {
                // format is [rowIndex, columnIndex, value]
                int rowIndex = cellsJsonArray.get(i).getAsInt();
                int columnIndex = cellsJsonArray.get(i + 1).getAsInt();
                R rowKey = rowIndexToKey.get(rowIndex);
                C columnKey = columnIndexToKey.get(columnIndex);
                V value = context.deserialize(cellsJsonArray.get(i + 2), typeOfV);
                table.put(rowKey, columnKey, value);
            }
            return table;
        }
    }

    /*
     * The json adapter for Guava Multimap.
     * Current support:
     * 1. ArrayListMultimap
     * 2. HashMultimap
     * 3. LinkedListMultimap
     * 4. LinkedHashMultimap
     *
     * The key and value classes of multi map should also be json serializable.
     */
    private static class GuavaMultimapAdapter<K, V>
            implements JsonSerializer<Multimap<K, V>>, JsonDeserializer<Multimap<K, V>> {

        private static final Type AS_MAP_RETURN_TYPE = getAsMapMethod().getGenericReturnType();

        private static Type asMapType(Type multimapType) {
            return com.google.common.reflect.TypeToken.of(multimapType).resolveType(AS_MAP_RETURN_TYPE).getType();
        }

        private static Method getAsMapMethod() {
            try {
                return Multimap.class.getDeclaredMethod("asMap");
            } catch (NoSuchMethodException e) {
                throw new AssertionError(e);
            }
        }

        @Override
        public JsonElement serialize(Multimap<K, V> map, Type typeOfSrc, JsonSerializationContext context) {
            JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty("clazz", map.getClass().getSimpleName());
            Map<K, Collection<V>> asMap = map.asMap();
            Type type = asMapType(typeOfSrc);
            JsonElement jsonElement = context.serialize(asMap, type);
            jsonObject.add("map", jsonElement);
            return jsonObject;
        }

        @Override
        public Multimap<K, V> deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
                throws JsonParseException {
            JsonObject jsonObject = json.getAsJsonObject();
            String clazz = jsonObject.get("clazz").getAsString();

            JsonElement mapElement = jsonObject.get("map");
            Map<K, Collection<V>> asMap = context.deserialize(mapElement, asMapType(typeOfT));

            Multimap<K, V> map = null;
            switch (clazz) {
                case "ArrayListMultimap":
                    map = ArrayListMultimap.create();
                    break;
                case "HashMultimap":
                    map = HashMultimap.create();
                    break;
                case "LinkedListMultimap":
                    map = LinkedListMultimap.create();
                    break;
                case "LinkedHashMultimap":
                    map = LinkedHashMultimap.create();
                    break;
                default:
                    Preconditions.checkState(false, "unknown guava multi map class: " + clazz);
                    break;
            }

            for (Map.Entry<K, Collection<V>> entry : asMap.entrySet()) {
                map.putAll(entry.getKey(), entry.getValue());
            }
            return map;
        }
    }

    public static class ProcessHookTypeAdapterFactory implements TypeAdapterFactory {

        public ProcessHookTypeAdapterFactory() {
        }

        @Override
        public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
            TypeAdapter<T> delegate = gson.getDelegateAdapter(this, type);

            return new TypeAdapter<T>() {
                public void write(JsonWriter out, T obj) throws IOException {
                    // Use Object lock to protect its properties from changed by other serialize thread.
                    // But this will only take effect when all threads uses GSONUtils to serialize object,
                    // because other methods of changing properties do not necessarily require the acquisition of the object lock
                    if (obj instanceof GsonPreProcessable) {
                        synchronized (obj) {
                            ((GsonPreProcessable) obj).gsonPreProcess();
                            delegate.write(out, obj);
                        }
                    } else {
                        delegate.write(out, obj);
                    }
                }

                public T read(JsonReader reader) throws IOException {
                    T obj = delegate.read(reader);
                    if (obj instanceof GsonPostProcessable) {
                        ((GsonPostProcessable) obj).gsonPostProcess();
                    }
                    return obj;
                }
            };
        }
    }

    private static class PrimitiveTypeDeserializer implements JsonDeserializer<PrimitiveType> {
        @Override
        public PrimitiveType deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
                throws JsonParseException {
            try {
                return PrimitiveType.valueOf(json.getAsJsonPrimitive().getAsString());
            } catch (Throwable t) {
                return PrimitiveType.INVALID_TYPE;
            }
        }
    }

    /*
    * For historical reasons, there was a period of time when the code serialized Expr directly in GsonUtils,
    * which would cause problems for the future expansion of Expr. This class is for code compatibility.
    * Starting from version 3.2, this compatibility class can be deleted.
    *
    *
    private static class ExpressionSerializer implements JsonSerializer<Expr> {
        @Override
        public JsonElement serialize(Expr expr, Type type, JsonSerializationContext context) {
            JsonObject expressionJson = new JsonObject();
            expressionJson.addProperty("expr", expr.toSql());
            return expressionJson;
        }
    }

    private static class ExpressionDeserializer implements JsonDeserializer<Expr> {
        @Override
        public Expr deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext context)
                throws JsonParseException {
            JsonObject expressionObject = jsonElement.getAsJsonObject();
            String expressionSql = expressionObject.get("expr").getAsString();
            return SqlParser.parseSqlToExpr(expressionSql, SqlModeHelper.MODE_DEFAULT);
        }
    }
     */
    public static class ExpressionSerializedObject {
        public ExpressionSerializedObject(String expressionSql) {
            this.expressionSql = expressionSql;
        }

        @SerializedName("expr")
        public String expressionSql;
    }
}
