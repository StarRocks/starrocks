// This file is made available under Elastic License 2.0.
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
import com.starrocks.alter.LakeTableSchemaChangeJob;
import com.starrocks.alter.RollupJobV2;
import com.starrocks.alter.SchemaChangeJobV2;
import com.starrocks.analysis.Expr;
import com.starrocks.backup.SnapshotInfo;
import com.starrocks.catalog.AnyArrayType;
import com.starrocks.catalog.AnyElementType;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.CatalogRecycleBin;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.HiveResource;
import com.starrocks.catalog.HudiResource;
import com.starrocks.catalog.IcebergResource;
import com.starrocks.catalog.JDBCResource;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.OdbcCatalogResource;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.PseudoType;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Resource;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.SparkResource;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.Tablet;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.backup.LakeTableSnapshotInfo;
import com.starrocks.load.loadv2.LoadJob.LoadJobStateUpdateInfo;
import com.starrocks.load.loadv2.SparkLoadJob.SparkLoadJobStateUpdateInfo;
import com.starrocks.persist.ListPartitionPersistInfo;
import com.starrocks.persist.PartitionPersistInfoV2;
import com.starrocks.persist.RangePartitionPersistInfo;
import com.starrocks.privilege.DbPEntryObject;
import com.starrocks.privilege.PEntryObject;
import com.starrocks.privilege.ResourcePEntryObject;
import com.starrocks.privilege.TablePEntryObject;
import com.starrocks.privilege.UserPEntryObject;
import com.starrocks.privilege.ViewPEntryObject;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.optimizer.dump.HiveTableDumpInfo;
import com.starrocks.sql.optimizer.dump.QueryDumpDeserializer;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.sql.optimizer.dump.QueryDumpSerializer;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.system.BackendHbResponse;
import com.starrocks.system.BrokerHbResponse;
import com.starrocks.system.FrontendHbResponse;
import com.starrocks.system.HeartbeatResponse;

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
                    .registerSubtype(ScalarType.class, ScalarType.class.getSimpleName())
                    .registerSubtype(ArrayType.class, ArrayType.class.getSimpleName())
                    .registerSubtype(MapType.class, MapType.class.getSimpleName())
                    .registerSubtype(StructType.class, StructType.class.getSimpleName())
                    .registerSubtype(PseudoType.class, PseudoType.class.getSimpleName())
                    .registerSubtype(AnyElementType.class, AnyElementType.class.getSimpleName())
                    .registerSubtype(AnyArrayType.class, AnyArrayType.class.getSimpleName());

    // runtime adapter for class "DistributionInfo"
    private static final RuntimeTypeAdapterFactory<DistributionInfo> DISTRIBUTION_INFO_TYPE_ADAPTER_FACTORY =
            RuntimeTypeAdapterFactory
                    .of(DistributionInfo.class, "clazz")
                    .registerSubtype(HashDistributionInfo.class, HashDistributionInfo.class.getSimpleName())
                    .registerSubtype(RandomDistributionInfo.class, RandomDistributionInfo.class.getSimpleName());

    private static final RuntimeTypeAdapterFactory<PartitionInfo> PARTITION_INFO_TYPE_ADAPTER_FACTORY =
            RuntimeTypeAdapterFactory
                    .of(PartitionInfo.class, "clazz")
                    .registerSubtype(RangePartitionInfo.class, RangePartitionInfo.class.getSimpleName())
                    .registerSubtype(SinglePartitionInfo.class, SinglePartitionInfo.class.getSimpleName())
                    .registerSubtype(ExpressionRangePartitionInfo.class,
                            ExpressionRangePartitionInfo.class.getSimpleName());

    // runtime adapter for class "Resource"
    private static final RuntimeTypeAdapterFactory<Resource> RESOURCE_TYPE_ADAPTER_FACTORY = RuntimeTypeAdapterFactory
            .of(Resource.class, "clazz")
            .registerSubtype(SparkResource.class, SparkResource.class.getSimpleName())
            .registerSubtype(HiveResource.class, HiveResource.class.getSimpleName())
            .registerSubtype(IcebergResource.class, IcebergResource.class.getSimpleName())
            .registerSubtype(HudiResource.class, HudiResource.class.getSimpleName())
            .registerSubtype(OdbcCatalogResource.class, OdbcCatalogResource.class.getSimpleName())
            .registerSubtype(JDBCResource.class, JDBCResource.class.getSimpleName());

    // runtime adapter for class "AlterJobV2"
    private static final RuntimeTypeAdapterFactory<AlterJobV2> ALTER_JOB_V2_TYPE_ADAPTER_FACTORY =
            RuntimeTypeAdapterFactory.of(AlterJobV2.class, "clazz")
                    .registerSubtype(RollupJobV2.class, RollupJobV2.class.getSimpleName())
                    .registerSubtype(SchemaChangeJobV2.class, SchemaChangeJobV2.class.getSimpleName())
                    .registerSubtype(LakeTableSchemaChangeJob.class, LakeTableSchemaChangeJob.class.getSimpleName());

    // runtime adapter for class "LoadJobStateUpdateInfo"
    private static final RuntimeTypeAdapterFactory<LoadJobStateUpdateInfo>
            LOAD_JOB_STATE_UPDATE_INFO_TYPE_ADAPTER_FACTORY = RuntimeTypeAdapterFactory
            .of(LoadJobStateUpdateInfo.class, "clazz")
            .registerSubtype(SparkLoadJobStateUpdateInfo.class, SparkLoadJobStateUpdateInfo.class.getSimpleName());

    // runtime adapter for class "Tablet"
    private static final RuntimeTypeAdapterFactory<Tablet> TABLET_TYPE_ADAPTER_FACTORY = RuntimeTypeAdapterFactory
            .of(Tablet.class, "clazz")
            .registerSubtype(LocalTablet.class, LocalTablet.class.getSimpleName(), true)
            .registerSubtype(LakeTablet.class, LakeTablet.class.getSimpleName());

    // runtime adapter for HeartbeatResponse
    private static final RuntimeTypeAdapterFactory<HeartbeatResponse> HEARTBEAT_RESPONSE_ADAPTER_FACTOR =
            RuntimeTypeAdapterFactory
                    .of(HeartbeatResponse.class, "clazz")
                    .registerSubtype(BackendHbResponse.class, BackendHbResponse.class.getSimpleName())
                    .registerSubtype(FrontendHbResponse.class, FrontendHbResponse.class.getSimpleName())
                    .registerSubtype(BrokerHbResponse.class, BrokerHbResponse.class.getSimpleName());

    private static final RuntimeTypeAdapterFactory<PartitionPersistInfoV2> PARTITION_PERSIST_INFO_V_2_ADAPTER_FACTORY
            = RuntimeTypeAdapterFactory.of(PartitionPersistInfoV2.class, "clazz")
            .registerSubtype(ListPartitionPersistInfo.class, ListPartitionPersistInfo.class.getSimpleName())
            .registerSubtype(RangePartitionPersistInfo.class, RangePartitionPersistInfo.class.getSimpleName());

    private static final RuntimeTypeAdapterFactory<CatalogRecycleBin.RecyclePartitionInfoV2>
            RECYCLE_PARTITION_INFO_V_2_ADAPTER_FACTORY
            = RuntimeTypeAdapterFactory.of(CatalogRecycleBin.RecyclePartitionInfoV2.class, "clazz")
            .registerSubtype(CatalogRecycleBin.RecycleRangePartitionInfo.class,
                    CatalogRecycleBin.RecycleRangePartitionInfo.class.getSimpleName());

    private static final RuntimeTypeAdapterFactory<com.starrocks.catalog.Table> TABLE_TYPE_ADAPTER_FACTORY
            = RuntimeTypeAdapterFactory.of(com.starrocks.catalog.Table.class, "clazz")
            .registerSubtype(LakeTable.class, LakeTable.class.getSimpleName());

    private static final RuntimeTypeAdapterFactory<SnapshotInfo> SNAPSHOT_INFO_TYPE_ADAPTER_FACTORY
            = RuntimeTypeAdapterFactory.of(SnapshotInfo.class, "clazz")
            .registerSubtype(LakeTableSnapshotInfo.class, LakeTableSnapshotInfo.class.getSimpleName());

    private static final RuntimeTypeAdapterFactory<PEntryObject> P_ENTRY_OBJECT_RUNTIME_TYPE_ADAPTER_FACTORY =
            RuntimeTypeAdapterFactory.of(PEntryObject.class, "clazz")
                    .registerSubtype(DbPEntryObject.class, DbPEntryObject.class.getSimpleName())
                    .registerSubtype(TablePEntryObject.class, TablePEntryObject.class.getSimpleName())
                    .registerSubtype(UserPEntryObject.class, UserPEntryObject.class.getSimpleName())
                    .registerSubtype(ResourcePEntryObject.class, ResourcePEntryObject.class.getSimpleName())
                    .registerSubtype(ViewPEntryObject.class, ViewPEntryObject.class.getSimpleName());

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

    private static final JsonSerializer<Expr> EXPRESSION_SERIALIZER = new ExpressionSerializer();

    private static final JsonDeserializer<Expr> EXPRESSION_DESERIALIZER = new ExpressionDeserializer();

    private static final JsonDeserializer<PrimitiveType> PRIMITIVE_TYPE_DESERIALIZER = new PrimitiveTypeDeserializer();

    private static final JsonDeserializer<MapType> MAP_TYPE_JSON_DESERIALIZER = new MapType.MapTypeDeSerializer();

    private static final JsonDeserializer<StructType> STRUCT_TYPE_JSON_DESERIALIZER = new StructType.StructTypeDeSerializer();

    // the builder of GSON instance.
    // Add any other adapters if necessary.
    private static final GsonBuilder GSON_BUILDER = new GsonBuilder()
            .addSerializationExclusionStrategy(new HiddenAnnotationExclusionStrategy())
            .addDeserializationExclusionStrategy(new HiddenAnnotationExclusionStrategy())
            .enableComplexMapKeySerialization()
            .registerTypeHierarchyAdapter(Table.class, new GuavaTableAdapter())
            .registerTypeHierarchyAdapter(Multimap.class, new GuavaMultimapAdapter())
            .registerTypeAdapterFactory(new ProcessHookTypeAdapterFactory())
            // specified childcalss DeSerializer must be ahead of fatherclass,
            // because when GsonBuilder::create() will reverse it,
            // and gson::getDelegateAdapter will skip the factory ahead of father TypeAdapterFactory factory.
            .registerTypeAdapter(MapType.class, MAP_TYPE_JSON_DESERIALIZER)
            .registerTypeAdapter(StructType.class, STRUCT_TYPE_JSON_DESERIALIZER)
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
            .registerTypeAdapter(LocalDateTime.class, LOCAL_DATE_TIME_TYPE_SERIALIZER)
            .registerTypeAdapter(LocalDateTime.class, LOCAL_DATE_TIME_TYPE_DESERIALIZER)
            .registerTypeAdapter(QueryDumpInfo.class, DUMP_INFO_SERIALIZER)
            .registerTypeAdapter(QueryDumpInfo.class, DUMP_INFO_DESERIALIZER)
            .registerTypeAdapter(HiveTableDumpInfo.class, HIVE_TABLE_DUMP_INFO_SERIALIZER)
            .registerTypeAdapter(HiveTableDumpInfo.class, HIVE_TABLE_DUMP_INFO_DESERIALIZER)
            .registerTypeAdapter(PrimitiveType.class, PRIMITIVE_TYPE_DESERIALIZER)
            .registerTypeHierarchyAdapter(Expr.class, EXPRESSION_SERIALIZER)
            .registerTypeHierarchyAdapter(Expr.class, EXPRESSION_DESERIALIZER);

    // this instance is thread-safe.
    public static final Gson GSON = GSON_BUILDER.create();

    /*
     * The exclusion strategy of GSON serialization.
     * Any fields without "@SerializedName" annotation with be ignore with
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
}
