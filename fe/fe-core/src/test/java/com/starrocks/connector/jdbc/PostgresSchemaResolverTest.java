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

import com.google.common.collect.Lists;
import com.mockrunner.mock.jdbc.MockResultSet;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.JDBCResource;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.catalog.Table;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PostgresSchemaResolverTest {
    @Mocked
    DriverManager driverManager;

    @Mocked
    Connection connection;

    private Map<String, String> properties;
    private MockResultSet dbResult;
    private MockResultSet tableResult;
    private MockResultSet columnResult;

    @Before
    public void setUp() throws SQLException {
        dbResult = new MockResultSet("catalog");
        dbResult.addColumn("TABLE_SCHEM", Arrays.asList("postgres", "template1", "test"));
        tableResult = new MockResultSet("tables");
        tableResult.addColumn("TABLE_NAME", Arrays.asList("tbl1", "tbl2", "tbl3"));
        columnResult = new MockResultSet("columns");
        columnResult.addColumn("DATA_TYPE", Arrays.asList(Types.BIT, Types.INTEGER, Types.INTEGER, Types.REAL, Types.DOUBLE,
                Types.NUMERIC, Types.CHAR, Types.VARCHAR, Types.VARCHAR, Types.DATE, Types.TIMESTAMP));
        columnResult.addColumn("TYPE_NAME", Arrays.asList("BOOL", "INTEGER", "SERIAL", "FLOAT4", "FLOAT8",
                "NUMERIC", "CHAR", "VARCHAR", "TEXT", "DATE", "TIMESTAMP"));
        columnResult.addColumn("COLUMN_SIZE", Arrays.asList(1, 10, 10, 8, 17, 10, 10, 10, 2147483647, 13, 29));
        columnResult.addColumn("DECIMAL_DIGITS", Arrays.asList(0, 0, 0, 8, 17, 2, 0, 0, 0, 0, 6));
        columnResult.addColumn("COLUMN_NAME", Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"));
        columnResult.addColumn("IS_NULLABLE", Arrays.asList("YES", "NO", "NO", "NO", "NO", "NO", "NO", "YES", "NO", "NO", "NO"));
        properties = new HashMap<>();
        properties.put(JDBCResource.DRIVER_CLASS, "org.postgresql.Driver");
        properties.put(JDBCResource.URI, "jdbc:postgresql://127.0.0.1:5432/t1");
        properties.put(JDBCResource.USER, "root");
        properties.put(JDBCResource.PASSWORD, "123456");
        properties.put(JDBCResource.CHECK_SUM, "xxxx");
        properties.put(JDBCResource.DRIVER_URL, "xxxx");
    }

    @Test
    public void testListDatabaseNames() throws SQLException {
        new Expectations() {
            {
                driverManager.getConnection(anyString, anyString, anyString);
                result = connection;
                minTimes = 0;

                connection.getMetaData().getSchemas();
                result = dbResult;
                minTimes = 0;
            }
        };
        try {
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog");
            List<String> result = jdbcMetadata.listDbNames();
            List<String> expectResult = Lists.newArrayList("postgres", "template1", "test");
            Assert.assertEquals(expectResult, result);
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testGetDb() throws SQLException {
        new Expectations() {
            {
                driverManager.getConnection(anyString, anyString, anyString);
                result = connection;
                minTimes = 0;

                connection.getMetaData().getSchemas();
                result = dbResult;
                minTimes = 0;
            }
        };
        try {
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog");
            Database db = jdbcMetadata.getDb("test");
            Assert.assertEquals("test", db.getOriginName());
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testListTableNames() throws SQLException {
        new Expectations() {
            {
                driverManager.getConnection(anyString, anyString, anyString);
                result = connection;
                minTimes = 0;

                connection.getCatalog();
                result = "t1";
                minTimes = 0;

                connection.getMetaData().getTables("t1", "test", null,
                        new String[] {"TABLE", "VIEW", "MATERIALIZED VIEW", "FOREIGN TABLE"});
                result = tableResult;
                minTimes = 0;
            }
        };
        try {
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog");
            List<String> result = jdbcMetadata.listTableNames("test");
            List<String> expectResult = Lists.newArrayList("tbl1", "tbl2", "tbl3");
            Assert.assertEquals(expectResult, result);
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testGetTable() throws SQLException {
        new Expectations() {
            {
                driverManager.getConnection(anyString, anyString, anyString);
                result = connection;
                minTimes = 0;

                connection.getCatalog();
                result = "t1";
                minTimes = 0;

                connection.getMetaData().getColumns("t1", "test", "tbl1", "%");
                result = columnResult;
                minTimes = 0;
            }
        };
        try {
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog");
            Table table = jdbcMetadata.getTable("test", "tbl1");
            Assert.assertTrue(table instanceof JDBCTable);
            Assert.assertEquals("catalog.test.tbl1", table.getUUID());
            Assert.assertEquals("tbl1", table.getName());
            Assert.assertNull(properties.get(JDBCTable.JDBC_TABLENAME));
            PostgresSchemaResolver postgresSchemaResolver = new PostgresSchemaResolver();
            ResultSet columnSet = postgresSchemaResolver.getColumns(connection, "test", "tbl1");
            List<Column> fullSchema = postgresSchemaResolver.convertToSRTable(columnSet);
            Table table1 = postgresSchemaResolver.getTable(1, "tbl1", fullSchema, "test", "catalog", properties);
            Assert.assertTrue(table1 instanceof JDBCTable);
            Assert.assertNull(properties.get(JDBCTable.JDBC_TABLENAME));
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.fail();
        }
    }

    @Test
    public void testGetPartitions() {
        PostgresSchemaResolver postgresSchemaResolver = new PostgresSchemaResolver();
        List<Partition> partitions = postgresSchemaResolver.getPartitions(null, new Table(1L, "tbl1",
                Table.TableType.JDBC, Lists.newArrayList()));
        Assert.assertEquals(partitions.size(), 1);
        Assert.assertEquals(partitions.get(0).getPartitionName(), "tbl1");
    }
}
