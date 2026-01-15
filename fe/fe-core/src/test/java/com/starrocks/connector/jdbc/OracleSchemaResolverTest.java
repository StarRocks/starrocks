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
import com.starrocks.connector.ConnectorMetadatRequestContext;
import com.starrocks.qe.ConnectContext;
import com.starrocks.type.VarcharType;
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

public class OracleSchemaResolverTest {
    @Mocked
    HikariDataSource dataSource;

    @Mocked
    Connection connection;

    @Mocked
    PreparedStatement preparedStatement;

    private Map<String, String> properties;
    private MockResultSet dbResult;
    private MockResultSet tableResult;
    private MockResultSet columnResult;
    private MockResultSet partitionsResult;

    @BeforeEach
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

        partitionsResult = new MockResultSet("partitions");
        partitionsResult.addColumn("NAME", Arrays.asList("'20230810'"));
        partitionsResult.addColumn("COLUMN_NAME", Arrays.asList("`d`"));
        partitionsResult.addColumn("MODIFIED_TIME", Arrays.asList("2023-08-01 00:00:00"));

        new Expectations() {
            {
                dataSource.getConnection();
                result = connection;
                minTimes = 0;

                preparedStatement.executeQuery();
                result = partitionsResult;
                minTimes = 0;
            }
        };
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
            List<String> expectResult = Lists.newArrayList("oracle", "template1", "test");
            Assertions.assertEquals(expectResult, result);
        } catch (Exception e) {
            Assertions.fail();
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
            Assertions.assertEquals("test", db.getOriginName());
        } catch (Exception e) {
            Assertions.fail();
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
            List<String> result = jdbcMetadata.listTableNames(new ConnectContext(), "test");
            List<String> expectResult = Lists.newArrayList("tbl1", "tbl2", "tbl3");
            Assertions.assertEquals(expectResult, result);
        } catch (Exception e) {
            Assertions.fail();
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
            Assertions.assertTrue(table instanceof JDBCTable);
            Assertions.assertEquals("catalog.test.tbl1", table.getUUID());
            Assertions.assertEquals("tbl1", table.getName());
            Assertions.assertNull(properties.get(JDBCTable.JDBC_TABLENAME));
            Assertions.assertTrue(table.getColumn("a").getType().isFloat());
            Assertions.assertTrue(table.getColumn("b").getType().isDouble());
            Assertions.assertTrue(table.getColumn("c").getType().isDecimalV3());
            Assertions.assertTrue(table.getColumn("d").getType().isStringType());
            Assertions.assertTrue(table.getColumn("e").getType().isStringType());
            Assertions.assertTrue(table.getColumn("f").getType().isBinaryType());
            Assertions.assertTrue(table.getColumn("g").getType().isStringType());
            Assertions.assertTrue(table.getColumn("h").getType().isDate());
            Assertions.assertTrue(table.getColumn("i").getType().isStringType());
            Assertions.assertTrue(table.getColumn("j").getType().isStringType());
            Assertions.assertTrue(table.getColumn("k").getType().isStringType());
            Assertions.assertTrue(table.getColumn("l").getType().isBinaryType());
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assertions.fail();
        }
    }

    @Test
    public void testGetPartitions() {
        try {
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog", dataSource);
            JDBCTable jdbcTable = new JDBCTable(100000, "tbl1", Arrays.asList(new Column("d", VarcharType.VARCHAR)),
                    Arrays.asList(new Column("d", VarcharType.VARCHAR)), "test", "catalog", properties);
            Integer size = jdbcMetadata.getPartitions(jdbcTable, Arrays.asList("20230810")).size();
            Assertions.assertTrue(size > 0);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testListPartitionNames() {
        try {
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog", dataSource);
            List<String> partitionNames = jdbcMetadata.listPartitionNames("test", "tbl1",
                    ConnectorMetadatRequestContext.DEFAULT);
            Assertions.assertFalse(partitionNames.isEmpty());
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testListPartitionColumns() {
        try {
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog", dataSource);
            List<Column> partitionCols = jdbcMetadata.listPartitionColumns("test", "tbl1",
                    Arrays.asList(new Column("`d`", VarcharType.VARCHAR)));
            Integer size = partitionCols.size();
            Assertions.assertTrue(size > 0);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testMysqlInvalidPartition1() {
        try {
            MockResultSet invalidPartition = new MockResultSet("partitions");
            invalidPartition.addColumn("NAME", Arrays.asList("'20230810'"));
            invalidPartition.addColumn("PARTITION_EXPRESSION", Arrays.asList("`d`"));
            invalidPartition.addColumn("MODIFIED_TIME", Arrays.asList("2023-08-01"));

            new Expectations() {
                {
                    preparedStatement.executeQuery();
                    result = invalidPartition;
                    minTimes = 0;
                }
            };
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog", dataSource);
            List<Column> columns = Arrays.asList(new Column("d", VarcharType.VARCHAR));
            JDBCTable jdbcTable = new JDBCTable(100000, "tbl1", columns, Lists.newArrayList(),
                    "test", "catalog", properties);
            jdbcMetadata.getPartitions(jdbcTable, Arrays.asList("20230810")).size();
            // different mysql source may have different partition information, so we can ignore partition information parse
        } catch (Exception e) {
            Assertions.fail();
        }
    }

    @Test
    public void testMysqlInvalidPartition2() {
        try {
            MockResultSet invalidPartition = new MockResultSet("partitions");
            invalidPartition.addColumn("NAME", Arrays.asList("'20230810'"));
            invalidPartition.addColumn("PARTITION_EXPRESSION", Arrays.asList("`d`"));
            invalidPartition.addColumn("MODIFIED_TIME", Arrays.asList("NULL"));

            new Expectations() {
                {
                    preparedStatement.executeQuery();
                    result = invalidPartition;
                    minTimes = 0;
                }
            };
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog", dataSource);
            List<Column> columns = Arrays.asList(new Column("d", VarcharType.VARCHAR));
            JDBCTable jdbcTable = new JDBCTable(100000, "tbl1", columns, Lists.newArrayList(),
                    "test", "catalog", properties);
            jdbcMetadata.getPartitions(jdbcTable, Arrays.asList("20230810")).size();
            // different mysql source may have different partition information, so we can ignore partition information parse
        } catch (Exception e) {
            Assertions.fail();
        }
    }
}
