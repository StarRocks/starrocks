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

package com.starrocks.connector.jdbc;

import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.catalog.Type;
import com.starrocks.thrift.TJDBCTable;
import com.starrocks.thrift.TTableDescriptor;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class JDBCTableTest {

    @Test
    public void testJDBCTableNameClass() {
        try {
            JDBCTableName jdbcTableName = new JDBCTableName("catalog", "db", "tbl");
            Assert.assertTrue(jdbcTableName.getCatalogName().equals("catalog"));
            Assert.assertTrue(jdbcTableName.getDatabaseName().equals("db"));
            Assert.assertTrue(jdbcTableName.getTableName().equals("tbl"));
            Assert.assertTrue(jdbcTableName.toString().contains("tbl"));
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.fail();
        }
    }

    @Test
    public void testJDBCPartitionClass() {
        try {
            Partition partition = new Partition("20230810", 1000L);
            Assert.assertTrue(partition.equals(partition));
            Assert.assertTrue(partition.hashCode() == Objects.hash("20230810", 1000L));
            Assert.assertTrue(partition.toString().contains("20230810"));
            Assert.assertTrue(partition.toJson().toString().contains("20230810"));
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.fail();
        }
    }

    @Test
    public void testJDBCDriverName() {
        try {
            Map<String, String> properties = ImmutableMap.of(
                    "driver_class", "org.postgresql.Driver",
                    "checksum", "bef0b2e1c6edcd8647c24bed31e1a4ac",
                    "driver_url",
                    "http://x.com/postgresql-42.3.3.jar",
                    "type", "jdbc",
                    "user", "postgres",
                    "password", "postgres",
                    "jdbc_uri", "jdbc:postgresql://172.26.194.237:5432/db_pg_select"
            );
            List<Column> schema = new ArrayList<>();
            schema.add(new Column("id", Type.INT));
            JDBCTable jdbcTable = new JDBCTable(10, "tbl", schema, "db", "jdbc_catalog", properties);
            TTableDescriptor tableDescriptor = jdbcTable.toThrift(null);
            TJDBCTable table = tableDescriptor.getJdbcTable();
            Assert.assertEquals(table.getJdbc_driver_name(), "jdbc_postgresql_172.26.194.237_5432_db_pg_select");
            Assert.assertEquals(table.getJdbc_driver_url(), "http://x.com/postgresql-42.3.3.jar");
            Assert.assertEquals(table.getJdbc_driver_checksum(), "bef0b2e1c6edcd8647c24bed31e1a4ac");
            Assert.assertEquals(table.getJdbc_driver_class(), "org.postgresql.Driver");
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.fail();
        }
    }
}
