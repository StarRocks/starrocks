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

package com.starrocks.catalog;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.constraint.UniqueConstraint;
import com.starrocks.common.DdlException;
import com.starrocks.connector.BucketProperty;
import com.starrocks.connector.ConnectorSinkSortScope;
import com.starrocks.connector.iceberg.TableTestBase;
import com.starrocks.connector.iceberg.TestTables;
import com.starrocks.planner.DescriptorTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.IcebergTableFactory;
import com.starrocks.thrift.TBucketFunction;
import com.starrocks.thrift.TIcebergTable;
import com.starrocks.thrift.TTableDescriptor;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.server.ExternalTableFactory.RESOURCE;
import static com.starrocks.type.ArrayType.ARRAY_BIGINT;
import static com.starrocks.type.DateType.DATETIME;
import static com.starrocks.type.IntegerType.INT;
import static com.starrocks.type.StringType.STRING;
import static com.starrocks.type.VarcharType.VARCHAR;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class IcebergTableTest extends TableTestBase {

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    @Test
    public void testValidateIcebergColumnType() {
        assertThrows(DdlException.class, () -> {
            List<Column> columns = Lists.newArrayList(new Column("k1", INT), new Column("k2", INT));
            IcebergTable oTable = new IcebergTable(1, "srTableName", "iceberg_catalog",
                    "resource_name", "iceberg_db", "iceberg_table", "", columns, mockedNativeTableB, Maps.newHashMap());
            List<Column> inputColumns = Lists.newArrayList(new Column("k1", INT, true));
            IcebergTableFactory.validateIcebergColumnType(inputColumns, oTable);
        });
    }

    @Test
    public void testCreateTableResourceName(@Mocked Table icebergNativeTable) throws DdlException {

        String resourceName = "Iceberg_resource_29bb53dc_7e04_11ee_9b35_00163e0e489a";
        Map<String, String> properties = new HashMap() {
            {
                put(RESOURCE, resourceName);
            }
        };

        IcebergTable.Builder tableBuilder = IcebergTable.builder()
                .setId(1000)
                .setSrTableName("supplier")
                .setCatalogName("iceberg_catalog")
                .setCatalogDBName("iceberg_oss_tpch_1g_parquet_gzip")
                .setCatalogTableName("supplier")
                .setResourceName(resourceName)
                .setFullSchema(new ArrayList<>())
                .setNativeTable(icebergNativeTable)
                .setIcebergProperties(new HashMap<>());
        IcebergTable oTable = tableBuilder.build();
        IcebergTable.Builder newBuilder = IcebergTable.builder();
        IcebergTableFactory.copyFromCatalogTable(newBuilder, oTable, properties);
        IcebergTable table = newBuilder.build();
        Assertions.assertEquals(table.getResourceName(), resourceName);
    }

    @Test
    public void testIcebergTableRepresentativeColumn() {
        List<Column> columns = Lists.newArrayList(
                new Column("k1", INT),
                new Column("k2", STRING),
                new Column("k3", ARRAY_BIGINT));
        IcebergTable.Builder tableBuilder = IcebergTable.builder()
                .setId(1000)
                .setSrTableName("supplier")
                .setCatalogName("iceberg_catalog")
                .setCatalogDBName("iceberg_oss_tpch_1g_parquet_gzip")
                .setCatalogTableName("supplier")
                .setFullSchema(columns)
                .setNativeTable(null)
                .setIcebergProperties(new HashMap<>());
        // by default use k1 as column
        IcebergTable table = tableBuilder.build();
        {
            Column c = table.getPresentivateColumn();
            Assertions.assertEquals(c.getName(), "k1");
        }

        // use k3 as unique column
        List<ColumnId> uniqueColumns = Lists.newArrayList(columns.get(2).getColumnId());
        table.setUniqueConstraints(Lists.newArrayList(new UniqueConstraint("cat", "db", "tbl", uniqueColumns)));
        {
            Column c = table.getPresentivateColumn();
            Assertions.assertEquals(c.getName(), "k3");
        }
    }

    @Test
    public void testIcebergTableRToThrift(@Mocked Table icebergNativeTable) {
        List<Column> columns = Lists.newArrayList(
                new Column("k1", INT),
                new Column("k2", STRING),
                new Column("k3", ARRAY_BIGINT));
        IcebergTable.Builder tableBuilder = IcebergTable.builder()
                .setId(1000)
                .setSrTableName("supplier")
                .setCatalogName("iceberg_catalog")
                .setCatalogDBName("iceberg_oss_tpch_1g_parquet_gzip")
                .setCatalogTableName("supplier")
                .setFullSchema(columns)
                .setNativeTable(icebergNativeTable)
                .setIcebergProperties(new HashMap<>());
        // by default use k1 as column
        IcebergTable table = tableBuilder.build();
        {
            Column c = table.getPresentivateColumn();
            Assertions.assertEquals(c.getName(), "k1");
        }

        TTableDescriptor tds = table.toThrift(new ArrayList<DescriptorTable.ReferencedPartitionInfo>());
    }

    @Test
    public void testGetBucketProperties() {
        List<Column> schema = new ArrayList<>();
        schema.add(new Column("id", INT));
        schema.add(new Column("data", VARCHAR));
        // mock table with one spec which has bucket field
        IcebergTable icebergTable = new IcebergTable(1, "iceberg_table", "iceberg_catalog",
                "resource", "db", "table", "", schema, mockedNativeTableA, Maps.newHashMap());
        Assertions.assertTrue(icebergTable.hasBucketProperties());
        List<BucketProperty> bucketProperties = icebergTable.getBucketProperties();
        Assertions.assertEquals(1, bucketProperties.size());
        BucketProperty bucketProperty = bucketProperties.get(0);
        Assertions.assertEquals("data", bucketProperty.getColumn().getName());
        Assertions.assertEquals(BUCKETS_NUMBER, bucketProperty.getBucketNum());
        Assertions.assertEquals(TBucketFunction.MURMUR3_X86_32, bucketProperty.getBucketFunction());
    }

    @Test
    public void testHasBucketProperties() {
        // two specs
        new MockUp<TestTables.TestTable>() {
            @Mock
            public Map<Integer, PartitionSpec> specs() {
                return ImmutableMap.of(1, SPEC_B, 2, SPEC_A);
            }
        };
        IcebergTable icebergTable = new IcebergTable(1, "iceberg_table", "iceberg_catalog",
                "resource", "db", "table", "", Lists.newArrayList(), mockedNativeTableA, Maps.newHashMap());
        Assertions.assertFalse(icebergTable.hasBucketProperties());
    }

    @Test
    public void testNoBucketProperties() {
        IcebergTable icebergTable = new IcebergTable(1, "iceberg_table", "iceberg_catalog",
                "resource", "db", "table", "", Lists.newArrayList(), mockedNativeTableB, Maps.newHashMap());
        Assertions.assertFalse(icebergTable.hasBucketProperties());
    }

    @Test
    public void testTwoBucketPropertiesAndMultiPartitions() {
        List<Column> schema = new ArrayList<>();
        schema.add(new Column("id", INT));
        schema.add(new Column("data", VARCHAR));
        schema.add(new Column("k1", INT));
        schema.add(new Column("k2", VARCHAR));
        schema.add(new Column("ts1", DATETIME));
        schema.add(new Column("ts2", DATETIME));
        schema.add(new Column("ts3", DATETIME));
        schema.add(new Column("ts4", DATETIME));
        IcebergTable icebergTable = new IcebergTable(1, "iceberg_table", "iceberg_catalog",
                "resource", "db", "table", "", schema, mockedNativeTableMultiPartition, Maps.newHashMap());
        Assertions.assertTrue(icebergTable.hasBucketProperties());
        List<BucketProperty> bucketProperties = icebergTable.getBucketProperties();
        Assertions.assertEquals(2, bucketProperties.size());
        if (bucketProperties.get(0).getColumn().getName().equals("id")) {
            Assertions.assertEquals(BUCKETS_NUMBER, bucketProperties.get(0).getBucketNum());
            Assertions.assertEquals(BUCKETS_NUMBER2, bucketProperties.get(1).getBucketNum());
        } else {
            Assertions.assertEquals(BUCKETS_NUMBER2, bucketProperties.get(0).getBucketNum());
            Assertions.assertEquals(BUCKETS_NUMBER, bucketProperties.get(1).getBucketNum());
        }
    }

    @Test
    public void testGetSortKeyIndexesInSortOrder(@Mocked Table icebergNativeTable) {
        List<Column> columns = Lists.newArrayList(
                new Column("a", INT),
                new Column("b", INT));

        Schema schema = new Schema(
                Types.NestedField.optional(1, "a", Types.IntegerType.get()),
                Types.NestedField.optional(2, "b", Types.IntegerType.get()));
        SortOrder sortOrder = SortOrder.builderFor(schema)
                .desc("b", NullOrder.NULLS_LAST)
                .asc("a", NullOrder.NULLS_FIRST)
                .build();

        new mockit.Expectations() {
            {
                icebergNativeTable.schema();
                result = schema;
                minTimes = 0;

                icebergNativeTable.sortOrder();
                result = sortOrder;
                minTimes = 0;
            }
        };

        IcebergTable icebergTable = new IcebergTable(1, "iceberg_table", "iceberg_catalog",
                "resource", "db", "table", "", columns, icebergNativeTable, Maps.newHashMap());
        Assertions.assertEquals(Lists.newArrayList(1, 0), icebergTable.getSortKeyIndexes());
    }

    @Test
    public void testToThriftWithHostLevelSortMode(@Mocked Table icebergNativeTable) {
        List<Column> columns = Lists.newArrayList(
                new Column("k1", INT),
                new Column("k2", STRING),
                new Column("k3", ARRAY_BIGINT));

        IcebergTable.Builder tableBuilder = IcebergTable.builder()
                .setId(1000)
                .setSrTableName("test_table")
                .setCatalogName("iceberg_catalog")
                .setCatalogDBName("test_db")
                .setCatalogTableName("test_table")
                .setFullSchema(columns)
                .setNativeTable(icebergNativeTable)
                .setIcebergProperties(new HashMap<>());
        IcebergTable table = tableBuilder.build();

        // Test with FILE scope (default) - sort_order should be included
        ConnectContext context = new ConnectContext();
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setConnectorSinkSortScope(ConnectorSinkSortScope.FILE.scopeName());
        context.setSessionVariable(sessionVariable);
        ConnectContext.set(context);

        TTableDescriptor descriptor = table.toThrift(new ArrayList<>());
        TIcebergTable tIcebergTable = descriptor.getIcebergTable();
        // When scope is FILE, sort_order should be set (if table has sort order)
        // Note: mockedNativeTable may not have sort order, so this test mainly verifies
        // that the code path doesn't throw an exception

        // Test with HOST scope - sort_order should NOT be included
        sessionVariable.setConnectorSinkSortScope(ConnectorSinkSortScope.HOST.scopeName());
        descriptor = table.toThrift(new ArrayList<>());
        tIcebergTable = descriptor.getIcebergTable();
        // When scope is HOST, sort_order should NOT be set
        Assertions.assertFalse(tIcebergTable.isSetSort_order(),
                "sort_order should not be set when connectorSinkSortScope=HOST");
    }

    @Test
    public void testBasicFieldsCopied() {
        // prepare a full IcebergTable
        List<Column> schema = Lists.newArrayList(new Column("c1", com.starrocks.type.IntegerType.INT));
        Map<String, String> props = Maps.newHashMap();
        props.put("k", "v");

        org.apache.iceberg.Schema icebergSchema =
                new org.apache.iceberg.Schema(
                        org.apache.iceberg.types.Types.NestedField.required(1, "c1", 
                                org.apache.iceberg.types.Types.IntegerType.get()));
        org.apache.iceberg.BaseTable nativeTable = Mockito.mock(org.apache.iceberg.BaseTable.class);
        org.apache.iceberg.TableMetadata meta = Mockito.mock(org.apache.iceberg.TableMetadata.class);
        org.apache.iceberg.TableOperations ops = Mockito.mock(org.apache.iceberg.TableOperations.class);
        Mockito.when(nativeTable.schema()).thenReturn(icebergSchema);
        Mockito.when(nativeTable.spec()).thenReturn(org.apache.iceberg.PartitionSpec.unpartitioned());
        Mockito.when(nativeTable.operations()).thenReturn(ops);
        Mockito.when(ops.current()).thenReturn(meta);
        Mockito.when(meta.uuid()).thenReturn("uuid-1234");


        IcebergTable full = IcebergTable.builder()
                .setId(10)
                .setSrTableName("sr")
                .setCatalogName("cat")
                .setResourceName("res")
                .setCatalogDBName("db")
                .setCatalogTableName("tbl")
                .setComment("cmt")
                .setFullSchema(schema)
                .setIcebergProperties(props)
                .setNativeTable(nativeTable)
                .build();

        LightWeightIcebergTable light = new LightWeightIcebergTable(full);

        Assertions.assertEquals(full.getId(), light.getId());
        Assertions.assertEquals(full.getName(), light.getName());
        Assertions.assertEquals(full.getCatalogName(), light.getCatalogName());
        Assertions.assertEquals(full.getCatalogDBName(), light.getCatalogDBName());
        Assertions.assertEquals(full.getCatalogTableName(), light.getCatalogTableName());
        Assertions.assertEquals(full.getComment(), light.getComment());
        Assertions.assertTrue(full.hashCode() == light.hashCode());
    }

    @Test
    public void testGetNativeTableThrows() {
        org.apache.iceberg.Schema icebergSchema =
                new org.apache.iceberg.Schema(
                        org.apache.iceberg.types.Types.NestedField.required(1, "c1", 
                                org.apache.iceberg.types.Types.IntegerType.get()));
        org.apache.iceberg.BaseTable nativeTable = Mockito.mock(org.apache.iceberg.BaseTable.class);
        org.apache.iceberg.TableMetadata meta = Mockito.mock(org.apache.iceberg.TableMetadata.class);
        org.apache.iceberg.TableOperations ops = Mockito.mock(org.apache.iceberg.TableOperations.class);
        Mockito.when(nativeTable.schema()).thenReturn(icebergSchema);
        Mockito.when(nativeTable.spec()).thenReturn(org.apache.iceberg.PartitionSpec.unpartitioned());
        Mockito.when(nativeTable.operations()).thenReturn(ops);
        Mockito.when(ops.current()).thenReturn(meta);
        Mockito.when(meta.uuid()).thenReturn("uuid-1234");

        IcebergTable base = IcebergTable.builder()
                .setId(1)
                .setSrTableName("t")
                .setCatalogName("cat")
                .setCatalogDBName("db")
                .setCatalogTableName("tbl")
                .setFullSchema(Lists.newArrayList())
                .setNativeTable(nativeTable)
                .build();
        LightWeightIcebergTable light = new LightWeightIcebergTable(base);
        Assertions.assertThrows(UnsupportedOperationException.class, light::getNativeTable);
    }
}
