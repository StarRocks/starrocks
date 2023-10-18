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

package com.starrocks.connector.hive;

import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.SerDeInfo;
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.amazonaws.services.glue.model.Table;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.hive.glue.converters.CatalogToHiveConverter;
import org.apache.hadoop.hive.metastore.TableType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static com.amazonaws.util.CollectionUtils.isNullOrEmpty;
import static com.starrocks.connector.unified.UnifiedMetadata.ICEBERG_TABLE_TYPE_NAME;
import static com.starrocks.connector.unified.UnifiedMetadata.ICEBERG_TABLE_TYPE_VALUE;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class GlueToHiveConverterTest {

    @Test
    public void testConvertTable() {
        com.amazonaws.services.glue.model.Table glueTable = new Table()
                .withDatabaseName("test-db")
                .withName("test-tbl")
                .withOwner("owner")
                .withParameters(ImmutableMap.of())
                .withPartitionKeys(ImmutableList.of())
                .withStorageDescriptor(getGlueTestStorageDescriptor())
                .withTableType(TableType.EXTERNAL_TABLE.name())
                .withViewOriginalText("originalText")
                .withViewExpandedText("expandedText")
                .withRetention(100);

        org.apache.hadoop.hive.metastore.api.Table hmsTable = CatalogToHiveConverter.convertTable(glueTable, "test-db");
        assertEquals(hmsTable.getTableName(), glueTable.getName());
        assertEquals(hmsTable.getDbName(), glueTable.getDatabaseName());
        assertEquals(hmsTable.getTableType(), glueTable.getTableType());
        assertEquals(hmsTable.getOwner(), glueTable.getOwner());
        assertEquals(hmsTable.getParameters(), glueTable.getParameters());
        assertColumnList(hmsTable.getSd().getCols(), glueTable.getStorageDescriptor().getColumns());
        assertColumnList(hmsTable.getPartitionKeys(), glueTable.getPartitionKeys());
        assertStorage(hmsTable.getSd(), glueTable.getStorageDescriptor());
        assertEquals(hmsTable.getViewOriginalText(), glueTable.getViewOriginalText());
        assertEquals(hmsTable.getViewExpandedText(), glueTable.getViewExpandedText());
        assertEquals(hmsTable.getRetention(), glueTable.getRetention());
    }

    @Test
    public void testConvertIcebergTableWithoutSd() {
        com.amazonaws.services.glue.model.Table glueTable = new Table()
                .withDatabaseName("test-db")
                .withName("test-tbl")
                .withOwner("owner")
                .withParameters(ImmutableMap.of(ICEBERG_TABLE_TYPE_NAME, ICEBERG_TABLE_TYPE_VALUE))
                .withPartitionKeys(ImmutableList.of())
                .withStorageDescriptor(null)
                .withTableType(TableType.EXTERNAL_TABLE.name())
                .withViewOriginalText("originalText")
                .withViewExpandedText("expandedText")
                .withRetention(100);

        org.apache.hadoop.hive.metastore.api.Table hmsTable = CatalogToHiveConverter.convertTable(glueTable, "test-db");
        assertEquals(hmsTable.getTableName(), glueTable.getName());
        assertEquals(hmsTable.getDbName(), glueTable.getDatabaseName());
        assertEquals(hmsTable.getTableType(), glueTable.getTableType());
        assertEquals(hmsTable.getOwner(), glueTable.getOwner());
        assertEquals(hmsTable.getParameters(), glueTable.getParameters());
        assertColumnList(hmsTable.getPartitionKeys(), glueTable.getPartitionKeys());
        assertNotNull(hmsTable.getSd()); // dummy Sd
        assertEquals(hmsTable.getViewOriginalText(), glueTable.getViewOriginalText());
        assertEquals(hmsTable.getViewExpandedText(), glueTable.getViewExpandedText());
        assertEquals(hmsTable.getRetention(), glueTable.getRetention());
    }

    @Test
    public void testConvertTableWithoutSd() {
        com.amazonaws.services.glue.model.Table glueTable = new Table()
                .withDatabaseName("test-db")
                .withName("test-tbl")
                .withOwner("owner")
                .withParameters(ImmutableMap.of())
                .withPartitionKeys(ImmutableList.of())
                .withStorageDescriptor(null)
                .withTableType(TableType.EXTERNAL_TABLE.name())
                .withViewOriginalText("originalText")
                .withViewExpandedText("expandedText")
                .withRetention(100);

        assertThrows(StarRocksConnectorException.class, () -> CatalogToHiveConverter.convertTable(glueTable, "test-db"));
    }

    private static void assertColumnList(List<org.apache.hadoop.hive.metastore.api.FieldSchema> actual,
                                         List<com.amazonaws.services.glue.model.Column> expected) {
        if (expected == null) {
            assertNull(actual);
        }
        assertEquals(actual.size(), expected.size());

        for (int i = 0; i < expected.size(); i++) {
            assertColumn(actual.get(i), expected.get(i));
        }
    }

    private static void assertColumn(org.apache.hadoop.hive.metastore.api.FieldSchema actual,
                                     com.amazonaws.services.glue.model.Column expected) {
        assertEquals(actual.getName(), expected.getName());
        assertEquals(actual.getType(), expected.getType());
        assertEquals(actual.getComment(), expected.getComment());
    }

    private static void assertStorage(org.apache.hadoop.hive.metastore.api.StorageDescriptor actual,
                                      com.amazonaws.services.glue.model.StorageDescriptor expected) {
        assertEquals(actual.getLocation(), expected.getLocation());
        assertEquals(actual.getSerdeInfo().getSerializationLib(), expected.getSerdeInfo().getSerializationLibrary());
        assertEquals(actual.getInputFormat(), expected.getInputFormat());
        assertEquals(actual.getOutputFormat(), expected.getOutputFormat());
        if (!isNullOrEmpty(expected.getBucketColumns())) {
            assertEquals(actual.getBucketCols(), expected.getBucketColumns());
            assertEquals(actual.getBucketColsSize(), expected.getNumberOfBuckets().intValue());
        }
    }

    public static Column getGlueTestColumn() {
        return getGlueTestColumn("string");
    }

    public static Column getGlueTestColumn(String type) {
        return new Column()
                .withName("test-col" + generateRandom())
                .withType(type)
                .withComment("column comment");
    }

    private static String generateRandom() {
        return format("%04x", ThreadLocalRandom.current().nextInt());
    }

    public static StorageDescriptor getGlueTestStorageDescriptor() {
        return getGlueTestStorageDescriptor(ImmutableList.of(getGlueTestColumn()), "SerdeLib");
    }

    public static StorageDescriptor getGlueTestStorageDescriptor(List<Column> columns, String serde) {
        return new StorageDescriptor()
                .withBucketColumns(ImmutableList.of("test-bucket-col"))
                .withColumns(columns)
                .withParameters(ImmutableMap.of())
                .withSerdeInfo(new SerDeInfo()
                        .withSerializationLibrary(serde)
                        .withParameters(ImmutableMap.of()))
                .withInputFormat("InputFormat")
                .withOutputFormat("OutputFormat")
                .withLocation("/test-tbl")
                .withNumberOfBuckets(1)
                .withCompressed(false)
                .withStoredAsSubDirectories(false);
    }
}
