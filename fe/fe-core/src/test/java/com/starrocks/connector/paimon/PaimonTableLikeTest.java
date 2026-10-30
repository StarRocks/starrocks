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

package com.starrocks.connector.paimon;

import com.starrocks.sql.analyzer.SemanticException;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class PaimonTableLikeTest {
    @Test
    public void testExactNativeSchemaDefaultsAndFreshFieldIds() {
        RowType nested = new RowType(false, List.of(
                new DataField(103, "ntz", DataTypes.TIMESTAMP(9).notNull(), "ntz comment", "2020-01-02 03:04:05"),
                new DataField(107, "ltz", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3), "ltz comment", null)));
        RowType mapNested = new RowType(false, List.of(
                new DataField(113, "ntz", DataTypes.TIMESTAMP(9).notNull(), "ntz comment", "2020-01-02 03:04:05"),
                new DataField(117, "ltz", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3), "ltz comment", null)));
        List<DataField> fields = List.of(
                new DataField(10, "Id", DataTypes.BIGINT().notNull(), "key comment", null),
                new DataField(19, "dt", DataTypes.DATE().notNull()),
                new DataField(27, "payload", DataTypes.ARRAY(nested).notNull(), "array comment", null),
                new DataField(40, "attributes", DataTypes.MAP(DataTypes.VARCHAR(11).notNull(), mapNested)),
                new DataField(51, "v", DataTypes.VARCHAR(37), "value comment", "native default"),
                new DataField(62, "amount", DataTypes.DECIMAL(29, 7).notNull()),
                new DataField(73, "bytes", DataTypes.VARBINARY(23)));
        TableSchema sourceSchema = new TableSchema(12, fields, 201, List.of("dt"), List.of("Id", "dt"),
                new LinkedHashMap<>(Map.of("bucket", "4", "bucket-key", "Id", "file.format", "parquet")), "table comment");
        FileStoreTable source = nativeTable(sourceSchema);

        Schema copied = PaimonTableLikeHelper.copySchema(source, null);

        Assertions.assertEquals(sourceSchema.partitionKeys(), copied.partitionKeys());
        Assertions.assertEquals(sourceSchema.primaryKeys(), copied.primaryKeys());
        Assertions.assertEquals(sourceSchema.comment(), copied.comment());
        Assertions.assertEquals(sourceSchema.options(), copied.options());
        Assertions.assertTrue(new RowType(sourceSchema.fields()).equalsIgnoreFieldId(new RowType(copied.fields())));
        for (int i = 0; i < fields.size(); i++) {
            Assertions.assertEquals(fields.get(i).name(), copied.fields().get(i).name());
            Assertions.assertEquals(fields.get(i).description(), copied.fields().get(i).description());
            Assertions.assertEquals(fields.get(i).defaultValue(), copied.fields().get(i).defaultValue());
        }
        RowType copiedNested = (RowType) ((org.apache.paimon.types.ArrayType) copied.fields().get(2).type()).getElementType();
        Assertions.assertEquals(nested.getFields().get(0).type(), copiedNested.getFields().get(0).type());
        Assertions.assertEquals(nested.getFields().get(0).defaultValue(), copiedNested.getFields().get(0).defaultValue());
        Assertions.assertEquals(nested.getFields().get(1).type(), copiedNested.getFields().get(1).type());
        Set<Integer> ids = new HashSet<>();
        collectFieldIds(new RowType(copied.fields()), ids);
        Assertions.assertEquals(11, ids.size());
        Assertions.assertEquals(0, copied.fields().get(0).id());
        Assertions.assertFalse(ids.contains(103));
        Assertions.assertEquals(10, sourceSchema.fields().get(0).id());
        Assertions.assertEquals(201, sourceSchema.highestFieldId());
        verify(source).schema();
        verifyNoMoreInteractions(source);
    }

    @Test
    public void testOptionsAreIndependentAndSourceIdentityIsNotCopied() {
        Map<String, String> sourceOptions = new LinkedHashMap<>(Map.of(
                "bucket", "3", "bucket-key", "id", "file.compression", "zstd", "fields.v.aggregate-function", "last_non_null"));
        for (String key : List.of("path", "data-file.external-paths", "warehouse", "owner", "uuid", "table.uuid",
                "table-id", "schema-id", "catalog.type", "paimon.catalog.warehouse", "hive.metastore.uris", "s3.access_key",
                "branch", "scan.snapshot-id", "consumer.id", "numRows", "lastUpdateTime", "metastore")) {
            sourceOptions.put(key, "source-only");
        }
        Map<String, String> originalOptions = new LinkedHashMap<>(sourceOptions);
        Map<String, String> overrides = new LinkedHashMap<>(Map.of("FiLe.Compression", "lz4", "snapshot.num-retained.min", "2"));
        Map<String, String> originalOverrides = new LinkedHashMap<>(overrides);
        TableSchema sourceSchema = primaryKeySchema(sourceOptions);
        FileStoreTable source = nativeTable(sourceSchema);

        Schema copied = PaimonTableLikeHelper.copySchema(source, overrides);

        Assertions.assertEquals(Map.of("bucket", "3", "bucket-key", "id", "file.compression", "lz4",
                "fields.v.aggregate-function", "last_non_null", "snapshot.num-retained.min", "2"), copied.options());
        Assertions.assertEquals(originalOptions, sourceSchema.options());
        Assertions.assertEquals(originalOverrides, overrides);
        copied.options().put("file.compression", "zstd");
        Assertions.assertEquals(originalOverrides, overrides);
        Assertions.assertEquals(originalOptions, sourceOptions);
        verify(source).schema();
        verifyNoMoreInteractions(source);
    }

    @Test
    public void testPrimaryKeyOptionCannotChangeNativeSchema() {
        Map<String, String> options = new LinkedHashMap<>(Map.of("bucket", "2", "primary-key", "id"));
        FileStoreTable source = nativeTable(primaryKeySchema(options));
        Schema copied = PaimonTableLikeHelper.copySchema(source, Map.of("primary-key", " ID "));
        Assertions.assertEquals(List.of("id"), copied.primaryKeys());
        Assertions.assertFalse(copied.options().containsKey("primary-key"));
        for (String keys : List.of("v", "id,id", "id,v", "")) {
            SemanticException error = Assertions.assertThrows(SemanticException.class,
                    () -> PaimonTableLikeHelper.copySchema(source, Map.of("primary-key", keys)));
            Assertions.assertTrue(error.getMessage().contains("cannot change primary-key"));
        }
        Assertions.assertThrows(SemanticException.class, () -> PaimonTableLikeHelper.copySchema(
                nativeTable(primaryKeySchema(Map.of("bucket", "2", "primary-key", "v"))), Map.of()));
    }

    @Test
    public void testUnsafeUnknownAndInconsistentOverridesFailWithoutCopyingData() {
        FileStoreTable source = nativeTable(primaryKeySchema(Map.of("bucket", "2")));
        for (String key : List.of("path", "warehouse", "catalog.type", "uuid", "table-id", "unknown-option",
                "scan.snapshot-id", "consumer.id")) {
            SemanticException error = Assertions.assertThrows(SemanticException.class,
                    () -> PaimonTableLikeHelper.copySchema(source, Map.of(key, "redacted-value")));
            Assertions.assertFalse(error.getMessage().contains("redacted-value"));
        }
        Assertions.assertThrows(SemanticException.class,
                () -> PaimonTableLikeHelper.copySchema(source, Map.of("bucket-key", "missing")));
        Assertions.assertThrows(SemanticException.class,
                () -> PaimonTableLikeHelper.copySchema(source, Map.of("bucket", "not-an-integer")));
        Assertions.assertThrows(SemanticException.class,
                () -> PaimonTableLikeHelper.copySchema(source, Map.of("bucket", "0")));
        Assertions.assertThrows(SemanticException.class,
                () -> PaimonTableLikeHelper.copySchema(source, Map.of("snapshot.num-retained.min", "20",
                        "snapshot.num-retained.max", "2")));
    }

    @Test
    public void testAppendSchemaDoesNotAcquirePrimaryKeysOrExplicitBucketDefaults() {
        Schema schema = Schema.newBuilder().column("id", DataTypes.INT()).column("dt", DataTypes.DATE())
                .partitionKeys("dt").build();
        FileStoreTable source = nativeTable(TableSchema.create(4, schema));
        Schema copied = PaimonTableLikeHelper.copySchema(source, Map.of());
        Assertions.assertTrue(copied.primaryKeys().isEmpty());
        Assertions.assertEquals(List.of("dt"), copied.partitionKeys());
        Assertions.assertFalse(copied.options().containsKey("bucket"));
        Assertions.assertTrue(copied.fields().get(1).type().isNullable());
        Assertions.assertThrows(SemanticException.class,
                () -> PaimonTableLikeHelper.copySchema(source, Map.of("primary-key", "id")));
    }

    private static TableSchema primaryKeySchema(Map<String, String> options) {
        return new TableSchema(5, new ArrayList<>(List.of(
                new DataField(7, "id", DataTypes.INT().notNull()), new DataField(12, "v", DataTypes.INT()))),
                12, List.of(), List.of("id"), options, "native comment");
    }

    private static FileStoreTable nativeTable(TableSchema schema) {
        FileStoreTable table = mock(FileStoreTable.class);
        when(table.schema()).thenReturn(schema);
        return table;
    }

    private static void collectFieldIds(DataType type, Set<Integer> ids) {
        if (type instanceof RowType) {
            for (DataField field : ((RowType) type).getFields()) {
                Assertions.assertTrue(ids.add(field.id()), "Duplicate new field ID");
                collectFieldIds(field.type(), ids);
            }
        } else if (type instanceof org.apache.paimon.types.ArrayType) {
            collectFieldIds(((org.apache.paimon.types.ArrayType) type).getElementType(), ids);
        } else if (type instanceof org.apache.paimon.types.MapType) {
            collectFieldIds(((org.apache.paimon.types.MapType) type).getKeyType(), ids);
            collectFieldIds(((org.apache.paimon.types.MapType) type).getValueType(), ids);
        }
    }
}
