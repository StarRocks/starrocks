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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.constraint.UniqueConstraint;
import com.starrocks.common.DdlException;
import com.starrocks.connector.iceberg.TableTestBase;
import com.starrocks.server.IcebergTableFactory;
import mockit.Mocked;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.catalog.Type.ARRAY_BIGINT;
import static com.starrocks.catalog.Type.INT;
import static com.starrocks.catalog.Type.STRING;
import static com.starrocks.server.ExternalTableFactory.RESOURCE;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class IcebergTableTest extends TableTestBase {

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
    public void testBasicFieldsCopied() {
        // prepare a full IcebergTable
        List<Column> schema = Lists.newArrayList(new Column("c1", INT));
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
