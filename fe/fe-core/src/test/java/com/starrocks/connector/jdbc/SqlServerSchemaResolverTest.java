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
import com.starrocks.qe.ConnectContext;
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

public class SqlServerSchemaResolverTest {

    @Mocked
    HikariDataSource dataSource;

    @Mocked
    Connection connection;

    private Map<String, String> properties;
    private MockResultSet dbResult;
    private MockResultSet tableResult;
    private MockResultSet columnResult;

    @Before
    public void setUp() {
        dbResult = new MockResultSet("catalog");
        dbResult.addColumn("TABLE_SCHEM", Arrays.asList("sqlserver", "template1", "test"));
        tableResult = new MockResultSet("tables");
        tableResult.addColumn("TABLE_NAME", Arrays.asList("tbl1", "tbl2", "tbl3"));
        columnResult = new MockResultSet("columns");
        columnResult.addColumn("DATA_TYPE",
                Arrays.asList(-155, Types.BIGINT, Types.BIT, Types.CHAR, Types.CHAR, Types.DATE, Types.DECIMAL,
                        Types.DECIMAL, Types.DECIMAL, Types.DOUBLE, Types.INTEGER, Types.INTEGER, Types.LONGNVARCHAR,
                        Types.LONGNVARCHAR,
                        Types.LONGVARCHAR, Types.NCHAR, Types.NUMERIC, Types.NVARCHAR, Types.REAL, Types.SMALLINT,
                        Types.TIME, Types.TIMESTAMP, Types.TIMESTAMP, Types.TIMESTAMP, Types.TINYINT, Types.VARCHAR,
                        Types.LONGVARBINARY, Types.BINARY, Types.VARBINARY));
        columnResult.addColumn("TYPE_NAME",
                Arrays.asList("datetimeoffset", "bigint", "bit", "char", "uniqueidentifier", "date", "decimal",
                        "smallmoney", "money", "float", "int identity", "int", "ntext", "xml",
                        "text", "nchar", "numerics", "nvarchar", "real", "smallint",
                        "time", "datetime2", "smalldatetime", "datetime", "tinyint", "varchar", "longvarbinary",
                        "binary", "varbinary"));
        columnResult.addColumn("COLUMN_SIZE",
                Arrays.asList(34, 19, 1, 10, 36, 10, 18,
                        10, 19, 53, 10, 10, 1073741823, 2147483647,
                        2147483647, 10, 18, 50, 24, 5,
                        16, 27, 16, 23, 3, 50, 0, 10, 100));
        columnResult.addColumn("DECIMAL_DIGITS",
                Arrays.asList(7, 0, 0, 0, 0, 0, 4,
                        4, 4, 0, 0, 0, 0, 0,
                        0, 0, 0, 0, 0, 0,
                        7, 7, 0, 3, 0, 0, 0, 0, 0));
        columnResult.addColumn("COLUMN_NAME",
                Arrays.asList("a", "b", "c", "d", "e", "f", "g",
                        "h", "i", "j", "k", "l", "m", "n",
                        "o", "p", "q", "r", "s", "t",
                        "u", "v", "w", "x", "y", "z", "aa", "bb", "cc"));
        columnResult.addColumn("IS_NULLABLE",
                Arrays.asList("YES", "YES", "YES", "YES", "YES", "YES", "YES",
                        "YES", "YES", "YES", "NO", "YES", "YES", "YES",
                        "YES", "YES", "YES", "YES", "YES", "YES",
                        "YES", "YES", "YES", "YES", "YES", "YES", "YES", "YES", "YES"));
        properties = new HashMap<>();
        properties.put(JDBCResource.DRIVER_CLASS, "com.microsoft.sqlserver.jdbc.SQLServerDriver");
        properties.put(JDBCResource.URI, "jdbc:sqlserver://127.0.0.1:1433;databaseName=MyDatabase;");
        properties.put(JDBCResource.USER, "sa");
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
            List<String> result = jdbcMetadata.listDbNames(new ConnectContext());
            List<String> expectResult = Lists.newArrayList("sqlserver", "template1", "test");
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
            Database db = jdbcMetadata.getDb(new ConnectContext(), "test");
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
                        new String[] {"TABLE", "VIEW"});
                result = tableResult;
                minTimes = 0;
            }
        };
        try {
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog", dataSource);
            List<String> result = jdbcMetadata.listTableNames(new ConnectContext(), "test");
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
            Table table = jdbcMetadata.getTable(new ConnectContext(), "test", "tbl1");
            Assert.assertTrue(table instanceof JDBCTable);
            Assert.assertEquals("catalog.test.tbl1", table.getUUID());
            Assert.assertEquals("tbl1", table.getName());
            Assert.assertNull(properties.get(JDBCTable.JDBC_TABLENAME));
            Assert.assertTrue(table.getColumn("a").getType().isVarchar());
            Assert.assertTrue(table.getColumn("b").getType().isBigint());
            Assert.assertTrue(table.getColumn("c").getType().isBoolean());
            Assert.assertTrue(table.getColumn("d").getType().isChar());
            Assert.assertTrue(table.getColumn("e").getType().isChar());
            Assert.assertTrue(table.getColumn("f").getType().isDate());
            Assert.assertTrue(table.getColumn("g").getType().isDecimalV3());
            Assert.assertTrue(table.getColumn("h").getType().isDecimalV3());
            Assert.assertTrue(table.getColumn("i").getType().isDecimalV3());
            Assert.assertTrue(table.getColumn("j").getType().isDouble());
            Assert.assertTrue(table.getColumn("k").getType().isInt());
            Assert.assertTrue(table.getColumn("l").getType().isInt());
            Assert.assertTrue(table.getColumn("m").getType().isVarchar());
            Assert.assertTrue(table.getColumn("n").getType().isVarchar());
            Assert.assertTrue(table.getColumn("o").getType().isVarchar());
            Assert.assertTrue(table.getColumn("p").getType().isChar());
            Assert.assertTrue(table.getColumn("q").getType().isDecimalV3());
            Assert.assertTrue(table.getColumn("r").getType().isVarchar());
            Assert.assertTrue(table.getColumn("s").getType().isFloat());
            Assert.assertTrue(table.getColumn("t").getType().isSmallint());
            Assert.assertTrue(table.getColumn("u").getType().isTime());
            Assert.assertTrue(table.getColumn("v").getType().isDatetime());
            Assert.assertTrue(table.getColumn("w").getType().isDatetime());
            Assert.assertTrue(table.getColumn("x").getType().isDatetime());
            Assert.assertTrue(table.getColumn("y").getType().isSmallint());
            Assert.assertTrue(table.getColumn("z").getType().isVarchar());
            Assert.assertTrue(table.getColumn("aa").getType().isBinaryType());
            Assert.assertTrue(table.getColumn("bb").getType().isBinaryType());
            Assert.assertTrue(table.getColumn("cc").getType().isBinaryType());
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.fail();
        }
    }

}
