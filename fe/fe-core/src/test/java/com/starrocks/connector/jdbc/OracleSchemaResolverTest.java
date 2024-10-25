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
import com.starrocks.catalog.Database;
import com.starrocks.catalog.JDBCResource;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.catalog.Table;
import com.zaxxer.hikari.HikariDataSource;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OracleSchemaResolverTest {
    @Mocked
    HikariDataSource dataSource;

    @Mocked
    Connection connection;

    private Map<String, String> properties;
    private MockResultSet dbResult;
    private MockResultSet tableResult;
    private MockResultSet columnResult;

    @Before
    public void setUp() throws SQLException {
        dbResult = new MockResultSet("catalog");
        dbResult.addColumn("TABLE_SCHEM", Arrays.asList("oracle", "template1", "test"));
        tableResult = new MockResultSet("tables");
        tableResult.addColumn("TABLE_NAME", Arrays.asList("tbl1", "tbl2", "tbl3"));
        columnResult = new MockResultSet("columns");
        columnResult.addColumn("DATA_TYPE", Arrays.asList(100, 101,
                3, Types.CHAR, Types.VARCHAR, Types.BLOB, Types.CLOB, Types.DATE, Types.TIMESTAMP, -101, -102, 23));
        columnResult.addColumn("TYPE_NAME", Arrays.asList("BINARY_FLOAT", "BINARY_DOUBLE",
                "NUMBER", "CHAR", "VARCHAR", "BLOB", "CLOB", "DATE", "TIMESTAMP",
                "TIMESTAMP(6) WITH LOCAL TIME ZONE", "TIMESTAMP(6) WITH TIME ZONE", "RAW"));
        columnResult.addColumn("COLUMN_SIZE", Arrays.asList(8, 16, 10, 10, 10, 4000, 4000, 8, 11, 11, 13, 2000));
        columnResult.addColumn("DECIMAL_DIGITS", Arrays.asList(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
        columnResult.addColumn("COLUMN_NAME", Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"));
        columnResult.addColumn("IS_NULLABLE", Arrays.asList("NO", "NO", "NO", "NO", "YES", "NO",
                "NO", "NO", "NO", "YES", "YES", "YES"));
        properties = new HashMap<>();
        properties.put(JDBCResource.DRIVER_CLASS, "oracle.jdbc.driver.OracleDriver");
        properties.put(JDBCResource.URI, "jdbc:oracle:thin:@127.0.0.1:1521:ORCL");
        properties.put(JDBCResource.USER, "root");
        properties.put(JDBCResource.PASSWORD, "123456");
        properties.put(JDBCResource.CHECK_SUM, "xxxx");
        properties.put(JDBCResource.DRIVER_URL, "xxxx");
    }

    @Test
    public void testListDatabaseNames() throws SQLException {
        new Expectations() {
            {
                dataSource.getConnection();
                result = connection;
                minTimes = 0;

                connection.getMetaData().getSchemas();
                result = dbResult;
                minTimes = 0;
            }
        };
        try {
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog", dataSource);
            List<String> result = jdbcMetadata.listDbNames();
            List<String> expectResult = Lists.newArrayList("oracle", "template1", "test");
            Assert.assertEquals(expectResult, result);
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testGetDb() throws SQLException {
        new Expectations() {
            {
                dataSource.getConnection();
                result = connection;
                minTimes = 0;

                connection.getMetaData().getSchemas();
                result = dbResult;
                minTimes = 0;
            }
        };
        try {
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog", dataSource);
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
                dataSource.getConnection();
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
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog", dataSource);
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
                dataSource.getConnection();
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
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog", dataSource);
            Table table = jdbcMetadata.getTable("test", "tbl1");
            Assert.assertTrue(table instanceof JDBCTable);
            Assert.assertEquals("catalog.test.tbl1", table.getUUID());
            Assert.assertEquals("tbl1", table.getName());
            Assert.assertNull(properties.get(JDBCTable.JDBC_TABLENAME));
            Assert.assertTrue(table.getColumn("a").getType().isFloat());
            Assert.assertTrue(table.getColumn("b").getType().isDouble());
            Assert.assertTrue(table.getColumn("c").getType().isDecimalV3());
            Assert.assertTrue(table.getColumn("d").getType().isStringType());
            Assert.assertTrue(table.getColumn("e").getType().isStringType());
            Assert.assertTrue(table.getColumn("f").getType().isBinaryType());
            Assert.assertTrue(table.getColumn("g").getType().isStringType());
            Assert.assertTrue(table.getColumn("h").getType().isDate());
            Assert.assertTrue(table.getColumn("i").getType().isStringType());
            Assert.assertTrue(table.getColumn("j").getType().isStringType());
            Assert.assertTrue(table.getColumn("k").getType().isStringType());
            Assert.assertTrue(table.getColumn("l").getType().isBinaryType());
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.fail();
        }
    }
}
