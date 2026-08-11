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

package com.starrocks.connector.bigquery;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.BigQueryTable;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.thrift.TTableType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

public class BigQueryTableTest {

    static BigQueryTable table;
    static BigQueryTable viewTable;

    @BeforeAll
    public static void setUp() {
        Schema schema = Schema.of(
                Field.of("id", StandardSQLTypeName.INT64),
                Field.of("name", StandardSQLTypeName.STRING),
                Field.of("amount", StandardSQLTypeName.NUMERIC)
        );
        List<Column> columns = BigQuerySchemaUtils.toStarRocksColumns(schema);
        table = new BigQueryTable("bq_catalog", "my_dataset", "my_table", columns, 1234567890L, false);
        viewTable = new BigQueryTable("bq_catalog", "my_dataset", "my_view", columns, 1234567890L, true);
    }

    @Test
    public void testTableType() {
        Assertions.assertEquals(Table.TableType.BIGQUERY, table.getType());
    }

    @Test
    public void testCatalogName() {
        Assertions.assertEquals("bq_catalog", table.getCatalogName());
    }

    @Test
    public void testDbName() {
        Assertions.assertEquals("my_dataset", table.getCatalogDBName());
    }

    @Test
    public void testTableName() {
        Assertions.assertEquals("my_table", table.getCatalogTableName());
        Assertions.assertEquals("my_table", table.getName());
    }

    @Test
    public void testFullSchema() {
        List<Column> cols = table.getFullSchema();
        Assertions.assertEquals(3, cols.size());
        Assertions.assertEquals("id", cols.get(0).getName());
        Assertions.assertEquals("name", cols.get(1).getName());
        Assertions.assertEquals("amount", cols.get(2).getName());
    }

    @Test
    public void testIsNotView() {
        Assertions.assertFalse(table.isView());
    }

    @Test
    public void testIsView() {
        Assertions.assertTrue(viewTable.isView());
    }

    @Test
    public void testIsUnPartitioned() {
        Assertions.assertTrue(table.isUnPartitioned());
    }

    @Test
    public void testNoPartitionColumns() {
        Assertions.assertTrue(table.getPartitionColumns().isEmpty());
        Assertions.assertTrue(table.getPartitionColumnNames().isEmpty());
    }

    @Test
    public void testUUID() {
        String uuid = table.getUUID();
        Assertions.assertNotNull(uuid);
        // UUID should contain catalog, db, table, and creation time
        Assertions.assertTrue(uuid.contains("bq_catalog"));
        Assertions.assertTrue(uuid.contains("my_dataset"));
        Assertions.assertTrue(uuid.contains("my_table"));
    }

    @Test
    public void testIsSupported() {
        Assertions.assertTrue(table.isSupported());
    }

    @Test
    public void testToThrift() {
        com.starrocks.thrift.TTableDescriptor tdesc = table.toThrift(ImmutableList.of());
        Assertions.assertEquals(TTableType.BIGQUERY_TABLE, tdesc.getTableType());
        Assertions.assertEquals("my_table", tdesc.getTableName());
        Assertions.assertEquals("my_dataset", tdesc.getDbName());
        // Schema columns should be serialized
        Assertions.assertEquals(3, tdesc.getHdfsTable().getColumns().size());
    }

    @Test
    public void testIsBigQueryTable() {
        Assertions.assertTrue(table.isBigQueryTable());
    }

    @Test
    public void testDataColumnNames() {
        List<String> names = table.getDataColumnNames();
        Assertions.assertEquals(3, names.size());
        Assertions.assertTrue(names.contains("id"));
        Assertions.assertTrue(names.contains("name"));
        Assertions.assertTrue(names.contains("amount"));
    }

    @Test
    public void testViewTableTableName() {
        Assertions.assertEquals("my_view", viewTable.getCatalogTableName());
        Assertions.assertEquals(Table.TableType.BIGQUERY, viewTable.getType());
    }
}
