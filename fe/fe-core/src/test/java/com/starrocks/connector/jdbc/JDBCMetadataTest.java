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
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.zaxxer.hikari.HikariDataSource;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.catalog.JDBCResource.DRIVER_CLASS;

public class JDBCMetadataTest {

    @Mocked
    HikariDataSource dataSource;

    @Mocked
    Connection connection;

    private Map<String, String> properties;
    private MockResultSet dbResult;
    private MockResultSet tableResult;
    private MockResultSet columnResult;
    @Mocked
    PreparedStatement preparedStatement;
    private MockResultSet partitionsResult;
    MockResultSet partitionsInfoTablesResult;

    @BeforeEach
    public void setUp() throws SQLException {
        dbResult = new MockResultSet("catalog");
        dbResult.addColumn("TABLE_CAT", Arrays.asList("information_schema", "mysql", "test"));
        tableResult = new MockResultSet("tables");
        tableResult.addColumn("TABLE_NAME", Arrays.asList("tbl1", "tbl2", "tbl3"));
        columnResult = new MockResultSet("columns");
        columnResult.addColumn("DATA_TYPE",
                Arrays.asList(Types.INTEGER, Types.DECIMAL, Types.CHAR, Types.VARCHAR, Types.TINYINT, Types.SMALLINT,
                        Types.INTEGER, Types.BIGINT, Types.TINYINT, Types.SMALLINT,
                        Types.INTEGER, Types.BIGINT, Types.DATE, Types.TIME, Types.TIMESTAMP));
        columnResult.addColumn("TYPE_NAME",
                Arrays.asList("INTEGER", "DECIMAL", "CHAR", "VARCHAR", "TINYINT UNSIGNED", "SMALLINT UNSIGNED",
                        "INTEGER UNSIGNED", "BIGINT UNSIGNED", "TINYINT", "SMALLINT",
                        "INTEGER", "BIGINT", "DATE", "TIME", "TIMESTAMP"));
        columnResult.addColumn("COLUMN_SIZE", Arrays.asList(4, 10, 10, 10, 1, 2, 4, 8, 1, 2, 4, 8, 8, 8, 8));
        columnResult.addColumn("DECIMAL_DIGITS", Arrays.asList(0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
        columnResult.addColumn("COLUMN_NAME",
                Arrays.asList("a", "b", "c", "d", "e1", "e2", "e4", "e8", "f1", "f2", "f3", "f4", "g1", "g2", "g3"));
        columnResult.addColumn("IS_NULLABLE",
                Arrays.asList("YES", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO"));
        properties = new HashMap<>();
        properties.put(DRIVER_CLASS, "org.mariadb.jdbc.Driver");
        properties.put(JDBCResource.URI, "jdbc:mariadb://127.0.0.1:3306");
        properties.put(JDBCResource.USER, "root");
        properties.put(JDBCResource.PASSWORD, "123456");
        properties.put(JDBCResource.CHECK_SUM, "xxxx");
        properties.put(JDBCResource.DRIVER_URL, "xxxx");

        partitionsResult = new MockResultSet("partitions");
        partitionsResult.addColumn("NAME", Arrays.asList("'20230810'"));
        partitionsResult.addColumn("PARTITION_EXPRESSION", Arrays.asList("`d`"));
        partitionsResult.addColumn("MODIFIED_TIME", Arrays.asList("2023-08-01"));

        new Expectations() {
            {
                dataSource.getConnection();
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

            }
        };
    }

    @Test
    public void testListDatabaseNames() {
        try {
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog", dataSource);
            dbResult.beforeFirst();
            List<String> result = jdbcMetadata.listDbNames(new ConnectContext());
            List<String> expectResult = Lists.newArrayList("test");
            Assertions.assertEquals(expectResult, result);
        } catch (Exception e) {
            Assertions.fail();
        }
    }

    @Test
    public void testGetDb() {
        try {
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog", dataSource);
            dbResult.beforeFirst();
            Database db = jdbcMetadata.getDb(new ConnectContext(), "test");
            Assertions.assertEquals("test", db.getOriginName());
        } catch (Exception e) {
            Assertions.fail();
        }
    }

    @Test
    public void testListTableNames() {
        try {
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog", dataSource);
            List<String> result = jdbcMetadata.listTableNames(new ConnectContext(), "test");
            List<String> expectResult = Lists.newArrayList("tbl1", "tbl2", "tbl3");
            Assertions.assertEquals(expectResult, result);
        } catch (Exception e) {
            Assertions.fail();
        }
    }

    @Test
    public void testGetTableWithoutPartition() throws SQLException {
        new Expectations() {
            {
                preparedStatement.executeQuery();
                result = null;
                minTimes = 0;
            }
        };
        try {
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog", dataSource);
            Table table = jdbcMetadata.getTable(new ConnectContext(), "test", "tbl1");
            Assertions.assertTrue(table instanceof JDBCTable);
            Assertions.assertTrue(table.getPartitionColumns().isEmpty());
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assertions.fail();
        }
    }

    @Test
    public void testGetTableWithPartition() throws SQLException {
        new Expectations() {
            {
                preparedStatement.executeQuery();
                result = partitionsResult;
                minTimes = 0;

                partitionsInfoTablesResult = new MockResultSet("partitions");
                partitionsInfoTablesResult.addColumn("TABLE_NAME", Arrays.asList("partitions"));
                connection.getMetaData().getTables(anyString, null, null, null);
                result = partitionsInfoTablesResult;
                minTimes = 0;
            }
        };
        try {
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog", dataSource);
            Table table = jdbcMetadata.getTable(new ConnectContext(), "test", "tbl1");
            Assertions.assertTrue(table instanceof JDBCTable);
            Assertions.assertFalse(table.getPartitionColumns().isEmpty());
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assertions.fail();
        }
    }

    @Test
    public void testColumnTypes() {
        JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog", dataSource);
        Table table = jdbcMetadata.getTable(new ConnectContext(), "test", "tbl1");
        List<Column> columns = table.getColumns();
        Assertions.assertEquals(columns.size(), columnResult.getRowCount());
        Assertions.assertTrue(columns.get(0).getType().equals(ScalarType.createType(PrimitiveType.INT)));
        Assertions.assertTrue(columns.get(1).getType().equals(ScalarType.createUnifiedDecimalType(10, 2)));
        Assertions.assertTrue(columns.get(2).getType().equals(ScalarType.createCharType(10)));
        Assertions.assertTrue(columns.get(3).getType().equals(ScalarType.createVarcharType(10)));
        Assertions.assertTrue(columns.get(4).getType().equals(ScalarType.createType(PrimitiveType.SMALLINT)));
        Assertions.assertTrue(columns.get(5).getType().equals(ScalarType.createType(PrimitiveType.INT)));
        Assertions.assertTrue(columns.get(6).getType().equals(ScalarType.createType(PrimitiveType.BIGINT)));
        Assertions.assertTrue(columns.get(7).getType().equals(ScalarType.createType(PrimitiveType.LARGEINT)));
        Assertions.assertTrue(columns.get(8).getType().equals(ScalarType.createType(PrimitiveType.TINYINT)));
        Assertions.assertTrue(columns.get(9).getType().equals(ScalarType.createType(PrimitiveType.SMALLINT)));
        Assertions.assertTrue(columns.get(10).getType().equals(ScalarType.createType(PrimitiveType.INT)));
        Assertions.assertTrue(columns.get(11).getType().equals(ScalarType.createType(PrimitiveType.BIGINT)));
        Assertions.assertTrue(columns.get(12).getType().equals(ScalarType.createType(PrimitiveType.DATE)));
        Assertions.assertTrue(columns.get(13).getType().equals(ScalarType.createType(PrimitiveType.TIME)));
        Assertions.assertTrue(columns.get(14).getType().equals(ScalarType.createType(PrimitiveType.DATETIME)));
    }

    @Test
    public void testGetTable2() {
        // user/password are optional fields for jdbc.
        properties.put(JDBCResource.USER, "");
        properties.put(JDBCResource.PASSWORD, "");
        JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog", dataSource);
        Table table = jdbcMetadata.getTable(new ConnectContext(), "test", "tbl1");
        Assertions.assertNotNull(table);
    }

    @Test
    public void testCacheTableId() {
        try {
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog", dataSource);
            Table table1 = jdbcMetadata.getTable(new ConnectContext(), "test", "tbl1");
            columnResult.beforeFirst();
            Table table2 = jdbcMetadata.getTable(new ConnectContext(), "test", "tbl1");
            Assertions.assertTrue(table1.getId() == table2.getId());
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assertions.fail();
        }
    }

    @Test
    public void testGetJdbcUrl() {
        properties.put(JDBCResource.URI, "jdbc:mysql://127.0.0.1:3306");
        JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog");
        Assertions.assertEquals("jdbc:mariadb://127.0.0.1:3306", jdbcMetadata.getJdbcUrl());
        properties.put(JDBCResource.URI, "jdbc:mysql://abc.mysql.com:3306");
        jdbcMetadata = new JDBCMetadata(properties, "catalog");
        Assertions.assertEquals("jdbc:mariadb://abc.mysql.com:3306", jdbcMetadata.getJdbcUrl());
    }

    @Test
    public void testCreateHikariDataSource() {
        properties = new HashMap<>();
        properties.put(DRIVER_CLASS, "org.mariadb.jdbc.Driver");
        properties.put(JDBCResource.URI, "jdbc:mariadb://127.0.0.1:3306");
        properties.put(JDBCResource.USER, "root");
        properties.put(JDBCResource.PASSWORD, "123456");
        properties.put(JDBCResource.CHECK_SUM, "xxxx");
        properties.put(JDBCResource.DRIVER_URL, "xxxx");
        new JDBCMetadata(properties, "catalog");
    }
}
