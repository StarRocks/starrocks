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
import com.starrocks.qe.ConnectContext;
import com.zaxxer.hikari.HikariDataSource;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.catalog.JDBCResource.DRIVER_CLASS;
import static com.starrocks.connector.jdbc.ClickhouseSchemaResolver.SUPPORTED_TABLE_TYPES;

public class ClickhouseSchemaResolverTest {

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
        dbResult.addColumn("TABLE_SCHEM", Arrays.asList("clickhouse", "template1", "test"));
        tableResult = new MockResultSet("tables");
        tableResult.addColumn("TABLE_NAME", Arrays.asList("tbl1", "tbl2", "tbl3"));
        columnResult = new MockResultSet("columns");
        columnResult.addColumn("DATA_TYPE",
                Arrays.asList(Types.TINYINT, Types.SMALLINT, Types.SMALLINT, Types.INTEGER, Types.INTEGER, Types.BIGINT,
                        Types.BIGINT, Types.NUMERIC, Types.NUMERIC, Types.NUMERIC, Types.NUMERIC, Types.NUMERIC,
                        Types.FLOAT,
                        Types.DOUBLE, Types.BOOLEAN, Types.DATE, Types.TIMESTAMP, Types.VARCHAR, Types.VARCHAR,
                        Types.DECIMAL
                ));
        columnResult.addColumn("TYPE_NAME", Arrays.asList("Int8", "UInt8", "Int16", "UInt16", "Int32", "Int64",
                "UInt32", "UInt64", "Int128", "UInt128", "Int256", "UInt256", "Float32", "Float64", "Bool", "Date",
                "DateTime",
                "String", "Nullable(String)", "Decimal(9,9)"));
        columnResult.addColumn("COLUMN_SIZE",
                Arrays.asList(3, 3, 5, 5, 10, 19, 10, 20, 39, 39, 77, 78, 12, 22, 1, 10, 29, 0, 0, 9));
        columnResult.addColumn("DECIMAL_DIGITS",
                Arrays.asList(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, null, 0, 0, null, null, 9));
        columnResult.addColumn("COLUMN_NAME", Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l",
                "m", "n", "o", "p", "q", "r", "s", "t"));
        columnResult.addColumn("IS_NULLABLE",
                Arrays.asList("NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO",
                        "NO", "NO", "NO", "YES", "NO"));
        properties = new HashMap<>();
        properties.put(DRIVER_CLASS, "com.clickhouse.jdbc.ClickHouseDriver");
        properties.put(JDBCResource.URI, "jdbc:clickhouse://127.0.0.1:8123");
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
            List<String> result = jdbcMetadata.listDbNames(new ConnectContext());
            List<String> expectResult = Lists.newArrayList("clickhouse", "template1", "test");
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
                        SUPPORTED_TABLE_TYPES.toArray(new String[SUPPORTED_TABLE_TYPES.size()]));
                result = tableResult;
                minTimes = 0;
            }
        };
        try {
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "t1", dataSource);
            List<String> result = jdbcMetadata.listTableNames(new ConnectContext(), "test");
            List<String> expectResult = Lists.newArrayList("tbl1", "tbl2", "tbl3");
            Assert.assertEquals(expectResult, result);
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testGetTables() throws SQLException {
        new Expectations() {
            {
                dataSource.getConnection();
                result = connection;
                minTimes = 0;

                connection.getCatalog();
                result = "catalog";
                minTimes = 0;

                connection.getMetaData().getTables("catalog", "test", null,
                        SUPPORTED_TABLE_TYPES.toArray(new String[SUPPORTED_TABLE_TYPES.size()]));
                result = tableResult;
                minTimes = 0;
            }
        };

        JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog", dataSource);
        List<String> result = jdbcMetadata.listTableNames(new ConnectContext(), "test");
        List<String> expectResult = Lists.newArrayList("tbl1", "tbl2", "tbl3");
        Assert.assertEquals(expectResult, result);

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
            ClickhouseSchemaResolver clickhouseSchemaResolver = new ClickhouseSchemaResolver(properties);
            ResultSet columnSet = clickhouseSchemaResolver.getColumns(connection, "test", "tbl1");
            List<Column> fullSchema = clickhouseSchemaResolver.convertToSRTable(columnSet);
            Table table1 = clickhouseSchemaResolver.getTable(1, "tbl1", fullSchema, "test", "catalog", properties);
            Assert.assertTrue(table1 instanceof JDBCTable);
            Assert.assertNull(properties.get(JDBCTable.JDBC_TABLENAME));
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.fail();
        }
    }

    @Test
    public void testListSchemas() throws SQLException {
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
        JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog", dataSource);
        List<String> result = jdbcMetadata.listDbNames(new ConnectContext());
        List<String> expectResult = Lists.newArrayList("clickhouse", "template1", "test");
        Assert.assertEquals(expectResult, result);
    }
}
