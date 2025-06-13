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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.hive.glue.converters.CatalogToHiveConverter;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.SerDeInfo;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static com.starrocks.connector.hive.glue.util.ExpressionHelper.buildExpressionFromPartialSpecification;
import static com.starrocks.connector.unified.UnifiedMetadata.ICEBERG_TABLE_TYPE_NAME;
import static com.starrocks.connector.unified.UnifiedMetadata.ICEBERG_TABLE_TYPE_VALUE;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static software.amazon.awssdk.utils.CollectionUtils.isNullOrEmpty;

public class GlueToHiveConverterTest {

    @Test
    public void testConvertTable() {
        String dbName = "test-db";
        String tableName = "test-table";
        String owner = "owner";
        String viewOriginalText = "originalText";
        String viewExpandedText = "expandedText";
        int retention = 100;

        StorageDescriptor sd = getGlueTestStorageDescriptor();

        Table.Builder glueTable = Table.builder()
                .databaseName(dbName)
                .name(tableName)
                .owner(owner)
                .parameters(ImmutableMap.of())
                .partitionKeys(ImmutableList.of())
                .storageDescriptor(sd)
                .tableType(TableType.EXTERNAL_TABLE.name())
                .viewOriginalText(viewOriginalText)
                .viewExpandedText(viewExpandedText)
                .retention(retention);

        org.apache.hadoop.hive.metastore.api.Table hmsTable =
                CatalogToHiveConverter.convertTable(glueTable.build(), "test-db");
        assertEquals(hmsTable.getTableName(), tableName);
        assertEquals(hmsTable.getDbName(), dbName);
        assertEquals(hmsTable.getTableType(), TableType.EXTERNAL_TABLE.name());
        assertEquals(hmsTable.getOwner(), owner);
        assertEquals(hmsTable.getParameters(), ImmutableMap.of());
        assertColumnList(hmsTable.getSd().getCols(), sd.columns());
        assertColumnList(hmsTable.getPartitionKeys(), ImmutableList.of());
        assertStorage(hmsTable.getSd(), getGlueTestStorageDescriptor());
        assertEquals(hmsTable.getViewOriginalText(), viewOriginalText);
        assertEquals(hmsTable.getViewExpandedText(), viewExpandedText);
        assertEquals(hmsTable.getRetention(), retention);
    }

    @Test
    public void testConvertIcebergTableWithoutSd() {
        String dbName = "test-db";
        String tableName = "test-tbl";
        String owner = "owner";
        String viewOriginalText = "originalText";
        String viewExpandedText = "expandedText";
        int retention = 100;

        Table.Builder glueTable = Table.builder()
                .databaseName(dbName)
                .name(tableName)
                .owner(owner)
                .parameters(ImmutableMap.of(ICEBERG_TABLE_TYPE_NAME, ICEBERG_TABLE_TYPE_VALUE))
                .partitionKeys(ImmutableList.of())
                .storageDescriptor((StorageDescriptor) null)
                .tableType(TableType.EXTERNAL_TABLE.name())
                .viewOriginalText(viewOriginalText)
                .viewExpandedText(viewExpandedText)
                .retention(retention);

        org.apache.hadoop.hive.metastore.api.Table hmsTable =
                CatalogToHiveConverter.convertTable(glueTable.build(), dbName);
        assertEquals(hmsTable.getTableName(), tableName);
        assertEquals(hmsTable.getDbName(), dbName);
        assertEquals(hmsTable.getTableType(), TableType.EXTERNAL_TABLE.name());
        assertEquals(hmsTable.getOwner(), owner);
        assertEquals(hmsTable.getParameters(), ImmutableMap.of(ICEBERG_TABLE_TYPE_NAME, ICEBERG_TABLE_TYPE_VALUE));
        assertColumnList(hmsTable.getPartitionKeys(), ImmutableList.of());
        assertNotNull(hmsTable.getSd()); // dummy Sd
        assertEquals(hmsTable.getViewOriginalText(), viewOriginalText);
        assertEquals(hmsTable.getViewExpandedText(), viewExpandedText);
        assertEquals(hmsTable.getRetention(), retention);
    }

    @Test
    public void testConvertTableWithoutSd() {
        String dbName = "test-db";
        String tableName = "test-tbl";
        String owner = "owner";
        String viewOriginalText = "originalText";
        String viewExpandedText = "expandedText";
        int retention = 100;

        Table.Builder glueTable = Table.builder()
                .databaseName(dbName)
                .name(tableName)
                .owner(owner)
                .parameters(ImmutableMap.of())
                .partitionKeys(ImmutableList.of())
                .storageDescriptor((StorageDescriptor) null)
                .tableType(TableType.EXTERNAL_TABLE.name())
                .viewOriginalText(viewOriginalText)
                .viewExpandedText(viewExpandedText)
                .retention(retention);

        assertThrows(StarRocksConnectorException.class,
                () -> CatalogToHiveConverter.convertTable(glueTable.build(), dbName));
    }

    @Test
    public void testExpressionConversion() throws MetaException {
        String dbName = "test-db";
        String tableName = "test-tbl";
        String owner = "owner";
        String viewOriginalText = "originalText";
        String viewExpandedText = "expandedText";
        int retention = 100;

        Column column1 = Column.builder().name("k1").type("varchar(65536)").build();
        Column column2 = Column.builder().name("k2").type("int").build();

        Table glueTable = Table.builder()
                .databaseName(dbName)
                .name(tableName)
                .owner(owner)
                .parameters(ImmutableMap.of(ICEBERG_TABLE_TYPE_NAME, ICEBERG_TABLE_TYPE_VALUE))
                .partitionKeys(ImmutableList.of(column1, column2))
                .storageDescriptor((StorageDescriptor) null)
                .tableType(TableType.EXTERNAL_TABLE.name())
                .viewOriginalText(viewOriginalText)
                .viewExpandedText(viewExpandedText)
                .retention(retention)
                .build();

        org.apache.hadoop.hive.metastore.api.Table hmsTable =
                CatalogToHiveConverter.convertTable(glueTable, dbName);

        String expression = buildExpressionFromPartialSpecification(hmsTable, ImmutableList.of("abc", "1"));
        assertEquals(expression, "(k1='abc') AND (k2=1)");
    }

    private static void assertColumnList(List<org.apache.hadoop.hive.metastore.api.FieldSchema> actual,
                                         List<software.amazon.awssdk.services.glue.model.Column> expected) {
        if (expected == null) {
            assertNull(actual);
        }
        assertEquals(actual.size(), expected.size());

        for (int i = 0; i < expected.size(); i++) {
            assertColumn(actual.get(i), expected.get(i));
        }
    }

    private static void assertColumn(org.apache.hadoop.hive.metastore.api.FieldSchema actual,
                                     software.amazon.awssdk.services.glue.model.Column expected) {
        assertEquals(actual.getName(), expected.name());
        assertEquals(actual.getType(), expected.type());
        assertEquals(actual.getComment(), expected.comment());
    }

    private static void assertStorage(org.apache.hadoop.hive.metastore.api.StorageDescriptor actual,
                                      software.amazon.awssdk.services.glue.model.StorageDescriptor expected) {
        assertEquals(actual.getLocation(), expected.location());
        assertEquals(actual.getSerdeInfo().getSerializationLib(), expected.serdeInfo().serializationLibrary());
        assertEquals(actual.getInputFormat(), expected.inputFormat());
        assertEquals(actual.getOutputFormat(), expected.outputFormat());
        if (!isNullOrEmpty(expected.bucketColumns())) {
            assertEquals(actual.getBucketCols(), expected.bucketColumns());
            assertEquals(actual.getBucketColsSize(), expected.numberOfBuckets().intValue());
        }
    }

    public static Column getGlueTestColumn() {
        return getGlueTestColumn("string");
    }

    public static Column getGlueTestColumn(String type) {
        return Column.builder()
                .name("test-col" + generateRandom())
                .type(type)
                .comment("column comment").build();
    }

    private static String generateRandom() {
        return format("%04x", ThreadLocalRandom.current().nextInt());
    }

    public static StorageDescriptor getGlueTestStorageDescriptor() {
        return getGlueTestStorageDescriptor(ImmutableList.of(getGlueTestColumn()), "SerdeLib");
    }

    public static StorageDescriptor getGlueTestStorageDescriptor(List<Column> columns, String serde) {
        return StorageDescriptor.builder()
                .bucketColumns(ImmutableList.of("test-bucket-col"))
                .columns(columns)
                .parameters(ImmutableMap.of())
                .serdeInfo(SerDeInfo.builder()
                        .serializationLibrary(serde)
                        .parameters(ImmutableMap.of()).build())
                .inputFormat("InputFormat")
                .outputFormat("OutputFormat")
                .location("/test-tbl")
                .numberOfBuckets(1)
                .compressed(false)
                .storedAsSubDirectories(false).build();
    }
}
