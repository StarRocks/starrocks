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

package com.starrocks.persist.gson;

import com.google.common.collect.Multimap;
import com.google.common.collect.Table;
import com.google.gson.GsonBuilder;
import com.starrocks.catalog.ColumnId;
import com.starrocks.common.Config;
import com.starrocks.common.Range;
import com.starrocks.encryption.EncryptionKeyPBAdapter;
import com.starrocks.persist.gson.internal.ColumnIdAdapter;
import com.starrocks.persist.gson.internal.GuavaMultimapAdapter;
import com.starrocks.persist.gson.internal.GuavaTableAdapter;
import com.starrocks.persist.gson.internal.MapTypeDeserializer;
import com.starrocks.persist.gson.internal.ProcessHookTypeAdapterFactory;
import com.starrocks.persist.gson.internal.RangeAdapter;
import com.starrocks.persist.gson.internal.RuntimeTypeAdapterTypes;
import com.starrocks.persist.gson.internal.StructTypeDeserializer;
import com.starrocks.proto.EncryptionKeyPB;
import com.starrocks.sql.optimizer.dump.HiveTableDumpInfo;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.type.MapType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.StructType;

import java.time.LocalDateTime;
import java.util.Map;

import static com.starrocks.persist.gson.internal.RuntimeTypeAdapterTypes.DUMP_INFO_DESERIALIZER;
import static com.starrocks.persist.gson.internal.RuntimeTypeAdapterTypes.DUMP_INFO_SERIALIZER;
import static com.starrocks.persist.gson.internal.RuntimeTypeAdapterTypes.HIVE_TABLE_DUMP_INFO_DESERIALIZER;
import static com.starrocks.persist.gson.internal.RuntimeTypeAdapterTypes.HIVE_TABLE_DUMP_INFO_SERIALIZER;
import static com.starrocks.persist.gson.internal.RuntimeTypeAdapterTypes.LOCAL_DATE_TIME_TYPE_DESERIALIZER;
import static com.starrocks.persist.gson.internal.RuntimeTypeAdapterTypes.LOCAL_DATE_TIME_TYPE_SERIALIZER;
import static com.starrocks.persist.gson.internal.RuntimeTypeAdapterTypes.PRIMITIVE_TYPE_DESERIALIZER;

public class DefaultGsonBuilderFactory implements IGsonBuilderFactory {

    public GsonBuilder create() {
        final Map<Class<?>, RuntimeTypeAdapterFactory<?>> clazz2Factory =
                RuntimeTypeAdapterTypes.CLAZZ_TO_RUNTIME_TYPE_ADAPTOR_FACTORIES;
        GsonBuilder builder = new GsonBuilder()
                .addSerializationExclusionStrategy(new GsonUtils.HiddenAnnotationExclusionStrategy())
                .addDeserializationExclusionStrategy(new GsonUtils.HiddenAnnotationExclusionStrategy())
                .enableComplexMapKeySerialization()
                .registerTypeHierarchyAdapter(Table.class, new GuavaTableAdapter())
                .registerTypeHierarchyAdapter(Multimap.class, new GuavaMultimapAdapter())
                .registerTypeHierarchyAdapter(ColumnId.class, new ColumnIdAdapter())
                .registerTypeHierarchyAdapter(Range.class, new RangeAdapter())
                .registerTypeAdapterFactory(new ProcessHookTypeAdapterFactory())
                // For call constructor with selectedFields
                .registerTypeAdapter(MapType.class, new MapTypeDeserializer())
                .registerTypeAdapter(StructType.class, new StructTypeDeserializer());

        clazz2Factory.forEach((k, v) -> {
            builder.registerTypeAdapterFactory(v);
        });

        builder.registerTypeAdapter(LocalDateTime.class, LOCAL_DATE_TIME_TYPE_SERIALIZER)
                .registerTypeAdapter(LocalDateTime.class, LOCAL_DATE_TIME_TYPE_DESERIALIZER)
                .registerTypeAdapter(QueryDumpInfo.class, DUMP_INFO_SERIALIZER)
                .registerTypeAdapter(QueryDumpInfo.class, DUMP_INFO_DESERIALIZER)
                .registerTypeAdapter(EncryptionKeyPB.class, new EncryptionKeyPBAdapter())
                .registerTypeAdapter(HiveTableDumpInfo.class, HIVE_TABLE_DUMP_INFO_SERIALIZER)
                .registerTypeAdapter(HiveTableDumpInfo.class, HIVE_TABLE_DUMP_INFO_DESERIALIZER)
                .registerTypeAdapter(PrimitiveType.class, PRIMITIVE_TYPE_DESERIALIZER);

        if (Config.metadata_ignore_unknown_subtype) {
            builder.registerTypeAdapterFactory(new SubtypeSkippingTypeAdapterFactory());
        }
        return builder;
    }
}
