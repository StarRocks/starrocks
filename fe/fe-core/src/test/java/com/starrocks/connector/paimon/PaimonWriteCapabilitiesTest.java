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

import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PaimonWriteCapabilitiesTest {
    @Test
    public void testAppendUnawareAndFixedBuckets() {
        Assertions.assertDoesNotThrow(() -> PaimonWriteCapabilities.validateWrite(table(false, Map.of("bucket", "-1"))));
        Assertions.assertDoesNotThrow(() -> PaimonWriteCapabilities.validateWrite(
                table(false, Map.of("bucket", "3", "bucket-key", "id"))));
        Assertions.assertDoesNotThrow(() -> PaimonWriteCapabilities.validateWrite(table(false, Map.of())));
        assertUnsupported(false, Map.of("bucket", "0"), "bucket");
        assertUnsupported(false, Map.of("bucket", "-2"), "bucket");
    }

    @Test
    public void testPrimaryKeyRequiresFixedBucketsIncludingSdkDefault() {
        Assertions.assertDoesNotThrow(() -> PaimonWriteCapabilities.validateWrite(table(true, Map.of("bucket", "4"))));
        assertUnsupported(true, Map.of(), "bucket");
        assertUnsupported(true, Map.of("bucket", "-1"), "bucket");
        assertUnsupported(true, Map.of("bucket", "-2"), "bucket");
        assertUnsupported(true, Map.of("bucket", "0"), "bucket");
    }

    @Test
    public void testWriteOptionsAreCheckedWithoutRestrictingDdlOptions() {
        assertUnsupported(false, Map.of("bucket", "bad"), "Invalid value");
        assertUnsupported(false, Map.of("file.format", "orc"), "Parquet");
        assertUnsupported(false, Map.of("file.format", "avro"), "Parquet");
        assertUnsupported(false, Map.of("file.format.per.level", "1:avro"), "Parquet");
        assertUnsupported(true, Map.of("bucket", "1", "merge-engine", "first-row"), "deduplicate");
        assertUnsupported(true, Map.of("bucket", "1", "merge-engine", "partial-update"), "deduplicate");
        assertUnsupported(true, Map.of("bucket", "1", "merge-engine", "aggregation"), "deduplicate");
        assertUnsupported(false, Map.of("data-evolution.enabled", "true"), "data evolution");
        assertUnsupported(false, Map.of("row-tracking.enabled", "true"), "row tracking");
        assertUnsupported(false, Map.of("global-index.enabled", "true"), "global indexes");
        assertUnsupported(false, Map.of("data-file.external-paths", "file:///test-external-data"), "external data paths");
        assertUnsupported(false, Map.of("data-file.path-directory", "data"), "data-file.path-directory");
        Assertions.assertDoesNotThrow(() -> PaimonWriteCapabilities.validateWrite(table(true,
                Map.of("bucket", "2", "sequence.field", "value", "file.compression", "zstd"))));
    }

    @Test
    public void testComplexAndVariantTypesAreNotBlanketRejected() {
        DataType payload = DataTypes.ARRAY(DataTypes.MAP(DataTypes.STRING(),
                DataTypes.ROW(DataTypes.FIELD(0, "nested", DataTypes.VARIANT()))));
        Assertions.assertDoesNotThrow(() -> PaimonWriteCapabilities.validateWrite(
                table(false, Map.of(), payload)));
        for (DataType type : new DataType[] {DataTypes.BLOB(), DataTypes.TIME(), DataTypes.ARRAY(DataTypes.BLOB())}) {
            StarRocksConnectorException error = Assertions.assertThrows(StarRocksConnectorException.class,
                    () -> PaimonWriteCapabilities.validateWrite(table(false, Map.of(), type)));
            Assertions.assertTrue(error.getMessage().contains("Unsupported Paimon write type"));
        }
    }

    @Test
    public void testExistingLocalZonedTimestampsAreWritable() {
        DataType timestamp = DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6);
        Assertions.assertDoesNotThrow(() -> PaimonWriteCapabilities.validateWrite(table(false, Map.of(), timestamp)));
        Assertions.assertDoesNotThrow(() -> PaimonWriteCapabilities.validateWrite(
                table(false, Map.of(), DataTypes.ARRAY(timestamp))));
    }

    @Test
    public void testTimestampPrecisionMatchesNativeWriterUnits() {
        for (int precision : new int[] {0, 3, 6, 9}) {
            Assertions.assertDoesNotThrow(() -> PaimonWriteCapabilities.validateWrite(
                    table(false, Map.of(), DataTypes.TIMESTAMP(precision))));
            Assertions.assertDoesNotThrow(() -> PaimonWriteCapabilities.validateWrite(
                    table(false, Map.of(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(precision))));
        }
        for (int precision : new int[] {2, 5}) {
            for (DataType timestamp : new DataType[] {DataTypes.TIMESTAMP(precision),
                    DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(precision)}) {
                StarRocksConnectorException error = Assertions.assertThrows(StarRocksConnectorException.class,
                        () -> PaimonWriteCapabilities.validateWrite(table(false, Map.of(), timestamp)));
                Assertions.assertTrue(error.getMessage().contains("timestamp precision"));
                Assertions.assertThrows(StarRocksConnectorException.class,
                        () -> PaimonWriteCapabilities.validateWrite(table(false, Map.of(), DataTypes.ARRAY(timestamp))));
            }
        }
    }

    @Test
    public void testCrossPartitionUpdatesAreRejectedWithoutChangingSchema() {
        Schema.Builder builder = Schema.newBuilder().column("id", DataTypes.INT()).column("dt", DataTypes.DATE())
                .primaryKey("id").partitionKeys("dt").option("bucket", "2");
        Schema schema = builder.build();
        TableSchema tableSchema = new TableSchema(0, schema.fields(), builder.getHighestFieldId(),
                schema.partitionKeys(), schema.primaryKeys(), schema.options(), null);
        FileStoreTable table = mock(FileStoreTable.class);
        when(table.schema()).thenReturn(tableSchema);
        when(table.options()).thenReturn(schema.options());
        StarRocksConnectorException error = Assertions.assertThrows(StarRocksConnectorException.class,
                () -> PaimonWriteCapabilities.validateWrite(table));
        Assertions.assertTrue(error.getMessage().contains("partition columns"));
        Assertions.assertEquals(java.util.List.of("id"), tableSchema.primaryKeys());
    }

    @Test
    public void testTimestampAndVariantPartitionTypesAreRejected() {
        for (DataType type : new DataType[] {DataTypes.TIMESTAMP(6),
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6), DataTypes.VARIANT()}) {
            StarRocksConnectorException error = Assertions.assertThrows(StarRocksConnectorException.class,
                    () -> PaimonWriteCapabilities.validateWrite(partitionedTable(type)));
            Assertions.assertTrue(error.getMessage().contains("Unsupported Paimon write partition type"));
        }
    }

    @Test
    public void testSupportedPartitionTypesRemainWritable() {
        for (DataType type : new DataType[] {DataTypes.BOOLEAN(), DataTypes.TINYINT(), DataTypes.SMALLINT(),
                DataTypes.INT(), DataTypes.BIGINT(), DataTypes.CHAR(5), DataTypes.VARCHAR(17), DataTypes.DATE()}) {
            Assertions.assertDoesNotThrow(() -> PaimonWriteCapabilities.validateWrite(partitionedTable(type)));
        }
    }

    @Test
    public void testNonFileStoreTableIsRejected() {
        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> PaimonWriteCapabilities.validateWrite(mock(Table.class)));
    }

    private static void assertUnsupported(boolean primaryKey, Map<String, String> options, String message) {
        StarRocksConnectorException error = Assertions.assertThrows(StarRocksConnectorException.class,
                () -> PaimonWriteCapabilities.validateWrite(table(primaryKey, options)));
        Assertions.assertTrue(error.getMessage().contains(message), error.getMessage());
    }

    private static FileStoreTable partitionedTable(DataType partitionType) {
        Schema.Builder builder = Schema.newBuilder().column("id", DataTypes.INT()).column("part", partitionType)
                .partitionKeys("part");
        Schema schema = builder.build();
        TableSchema tableSchema = new TableSchema(0, schema.fields(), builder.getHighestFieldId(),
                schema.partitionKeys(), schema.primaryKeys(), schema.options(), null);
        FileStoreTable table = mock(FileStoreTable.class);
        when(table.schema()).thenReturn(tableSchema);
        when(table.options()).thenReturn(schema.options());
        return table;
    }

    private static FileStoreTable table(boolean primaryKey, Map<String, String> options) {
        return table(primaryKey, options, DataTypes.INT());
    }

    private static FileStoreTable table(boolean primaryKey, Map<String, String> options, DataType valueType) {
        // Keep the SDK default out of the schema: validateWrite must resolve it, not change the table.
        Schema.Builder builder = Schema.newBuilder().column("id", DataTypes.INT()).column("value", valueType);
        if (primaryKey) {
            builder.primaryKey("id");
        }
        Schema schema = builder.build();
        TableSchema tableSchema = new TableSchema(0, schema.fields(), builder.getHighestFieldId(),
                schema.partitionKeys(), schema.primaryKeys(), new HashMap<>(), null);
        FileStoreTable table = mock(FileStoreTable.class);
        when(table.schema()).thenReturn(tableSchema);
        when(table.options()).thenReturn(new HashMap<>(options));
        return table;
    }
}
