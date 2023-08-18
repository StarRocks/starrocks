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
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.common.jmockit.Deencapsulation;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.catalog.JDBCResource.DRIVER_CLASS;

public class JDBCMetadataTest {

    @Mocked
    DriverManager driverManager;

    @Mocked
    Connection connection;

    @Mocked
    JDBCTableIdCache jdbcTableIdCache;

    private Map<String, String> properties;
    private MockResultSet dbResult;
    private MockResultSet tableResult;
    private MockResultSet columnResult;

    @Before
    public void setUp() throws SQLException {
        dbResult = new MockResultSet("catalog");
        dbResult.addColumn("TABLE_CAT", Arrays.asList("information_schema", "mysql", "test"));
        tableResult = new MockResultSet("tables");
        tableResult.addColumn("TABLE_NAME", Arrays.asList("tbl1", "tbl2", "tbl3"));
        columnResult = new MockResultSet("columns");
        columnResult.addColumn("DATA_TYPE",
                Arrays.asList(Types.INTEGER, Types.DECIMAL, Types.CHAR, Types.VARCHAR, Types.TINYINT, Types.SMALLINT,
                        Types.INTEGER, Types.BIGINT, Types.TINYINT, Types.SMALLINT,
                        Types.INTEGER, Types.BIGINT));
        columnResult.addColumn("TYPE_NAME",
                Arrays.asList("INTEGER", "DECIMAL", "CHAR", "VARCHAR", "TINYINT UNSIGNED", "SMALLINT UNSIGNED",
                        "INTEGER UNSIGNED", "BIGINT UNSIGNED", "TINYINT", "SMALLINT",
                        "INTEGER", "BIGINT"));
        columnResult.addColumn("COLUMN_SIZE", Arrays.asList(4, 10, 10, 10, 1, 2, 4, 8, 1, 2, 4, 8));
        columnResult.addColumn("DECIMAL_DIGITS", Arrays.asList(0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
        columnResult.addColumn("COLUMN_NAME",
                Arrays.asList("a", "b", "c", "d", "e1", "e2", "e4", "e8", "f1", "f2", "f3", "f4"));
        columnResult.addColumn("IS_NULLABLE",
                Arrays.asList("YES", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO"));
        properties = new HashMap<>();
        properties.put(DRIVER_CLASS, "com.mysql.cj.jdbc.Driver");
        properties.put(JDBCResource.URI, "jdbc:mysql://127.0.0.1:3306");
        properties.put(JDBCResource.USER, "root");
        properties.put(JDBCResource.PASSWORD, "123456");
        properties.put(JDBCResource.CHECK_SUM, "xxxx");
        properties.put(JDBCResource.DRIVER_URL, "xxxx");


        new Expectations() {
            {
                driverManager.getConnection(anyString, anyString, anyString);
                result = connection;
                minTimes = 0;

                connection.getMetaData().getCatalogs();
                result = dbResult;
                minTimes = 0;

                connection.getMetaData().getTables("test", null, null,
                        new String[] {"TABLE", "VIEW"});
                result = tableResult;
                minTimes = 0;

                connection.getMetaData().getColumns("test", null, "tbl1", "%");
                result = columnResult;
                minTimes = 0;

                JDBCTableName jdbcTablekey = JDBCTableName.of("catalog", "test", "tbl1");

                Deencapsulation.invoke(jdbcTableIdCache, "containsTableId", jdbcTablekey);
                result = true;
                minTimes = 0;

                Deencapsulation.invoke(jdbcTableIdCache, "getTableId", jdbcTablekey);
                result = 100000;
                minTimes = 0;

            }
        };
    }

    @Test
    public void testListDatabaseNames() {
        try {
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog");
            List<String> result = jdbcMetadata.listDbNames();
            List<String> expectResult = Lists.newArrayList("test");
            Assert.assertEquals(expectResult, result);
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testGetDb() {
        try {
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog");
            Database db = jdbcMetadata.getDb("test");
            Assert.assertEquals("test", db.getOriginName());
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testListTableNames() {
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
    public void testGetTable() {
        try {
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog");
            Table table = jdbcMetadata.getTable("test", "tbl1");
            Assert.assertTrue(table instanceof JDBCTable);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.fail();
        }
    }

    @Test
    public void testColumnTypes() {
        JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog");
        Table table = jdbcMetadata.getTable("test", "tbl1");
        List<Column> columns = table.getColumns();
        Assert.assertEquals(columns.size(), columnResult.getRowCount());
        Assert.assertTrue(columns.get(0).getType().equals(ScalarType.createType(PrimitiveType.INT)));
        Assert.assertTrue(columns.get(1).getType().equals(ScalarType.createUnifiedDecimalType(10, 2)));
        Assert.assertTrue(columns.get(2).getType().equals(ScalarType.createCharType(10)));
        Assert.assertTrue(columns.get(3).getType().equals(ScalarType.createVarcharType(10)));
        Assert.assertTrue(columns.get(4).getType().equals(ScalarType.createType(PrimitiveType.SMALLINT)));
        Assert.assertTrue(columns.get(5).getType().equals(ScalarType.createType(PrimitiveType.INT)));
        Assert.assertTrue(columns.get(6).getType().equals(ScalarType.createType(PrimitiveType.BIGINT)));
        Assert.assertTrue(columns.get(7).getType().equals(ScalarType.createType(PrimitiveType.LARGEINT)));
        Assert.assertTrue(columns.get(8).getType().equals(ScalarType.createType(PrimitiveType.TINYINT)));
        Assert.assertTrue(columns.get(9).getType().equals(ScalarType.createType(PrimitiveType.SMALLINT)));
        Assert.assertTrue(columns.get(10).getType().equals(ScalarType.createType(PrimitiveType.INT)));
        Assert.assertTrue(columns.get(11).getType().equals(ScalarType.createType(PrimitiveType.BIGINT)));
    }

    @Test
    public void testGetTable2() {
        // user/password are optional fields for jdbc.
        properties.put(JDBCResource.USER, "");
        properties.put(JDBCResource.PASSWORD, "");
        JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog");
        Table table = jdbcMetadata.getTable("test", "tbl1");
        Assert.assertNotNull(table);
    }

    @Test
    public void testCacheTableId() {
        try {
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog");
            Table table1 = jdbcMetadata.getTable("test", "tbl1");
            Assert.assertTrue(table1.getId() == 100000);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.fail();
        }
    }

}
