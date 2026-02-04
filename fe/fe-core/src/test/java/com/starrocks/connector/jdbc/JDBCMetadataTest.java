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
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.VarbinaryType;
import com.zaxxer.hikari.HikariDataSource;
import mockit.Delegate;
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
import java.util.concurrent.Executor;

import static com.starrocks.catalog.JDBCResource.DRIVER_CLASS;

public class JDBCMetadataTest {

    @Mocked
    HikariDataSource dataSource;

    @Mocked
    Connection connection;
    @Mocked
    PreparedStatement preparedStatement;
    MockResultSet partitionsInfoTablesResult;
    private Map<String, String> properties;
    private MockResultSet dbResult;
    private MockResultSet tableResult;
    private MockResultSet columnResult;
    private MockResultSet partitionsResult;

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
                        Types.INTEGER, Types.BIGINT, Types.DATE, Types.TIME, Types.TIMESTAMP,
                        Types.BINARY, Types.VARBINARY));
        columnResult.addColumn("TYPE_NAME",
                Arrays.asList("INTEGER", "DECIMAL", "CHAR", "VARCHAR", "TINYINT UNSIGNED", "SMALLINT UNSIGNED",
                        "INTEGER UNSIGNED", "BIGINT UNSIGNED", "TINYINT", "SMALLINT",
                        "INTEGER", "BIGINT", "DATE", "TIME", "TIMESTAMP", "BINARY", "VARBINARY"));
        columnResult.addColumn("COLUMN_SIZE", Arrays.asList(4, 10, 10, 10, 1, 2, 4, 8, 1, 2, 4, 8, 8, 8, 8, 16, 16));
        columnResult.addColumn("DECIMAL_DIGITS", Arrays.asList(0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
        columnResult.addColumn("COLUMN_NAME",
                Arrays.asList("a", "b", "c", "d", "e1", "e2", "e4", "e8", "f1", "f2", "f3", "f4", "g1", "g2", "g3", "h1", "h2"));
        columnResult.addColumn("IS_NULLABLE",
                Arrays.asList("YES", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO",
                        "NO"));
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
        Assertions.assertTrue(columns.get(0).getType().equals(IntegerType.INT));
        Assertions.assertTrue(columns.get(1).getType().equals(TypeFactory.createUnifiedDecimalType(10, 2)));
        Assertions.assertTrue(columns.get(2).getType().equals(TypeFactory.createCharType(10)));
        Assertions.assertTrue(columns.get(3).getType().equals(TypeFactory.createVarcharType(10)));
        Assertions.assertTrue(columns.get(4).getType().equals(IntegerType.SMALLINT));
        Assertions.assertTrue(columns.get(5).getType().equals(IntegerType.INT));
        Assertions.assertTrue(columns.get(6).getType().equals(IntegerType.BIGINT));
        Assertions.assertTrue(columns.get(7).getType().equals(IntegerType.LARGEINT));
        Assertions.assertTrue(columns.get(8).getType().equals(IntegerType.TINYINT));
        Assertions.assertTrue(columns.get(9).getType().equals(IntegerType.SMALLINT));
        Assertions.assertTrue(columns.get(10).getType().equals(IntegerType.INT));
        Assertions.assertTrue(columns.get(11).getType().equals(IntegerType.BIGINT));
        Assertions.assertTrue(columns.get(12).getType().equals(DateType.DATE));
        Assertions.assertTrue(columns.get(13).getType().equals(DateType.TIME));
        Assertions.assertTrue(columns.get(14).getType().equals(DateType.DATETIME));
        Assertions.assertTrue(columns.get(15).getType().equals(VarbinaryType.VARBINARY));
        Assertions.assertTrue(columns.get(16).getType().equals(VarbinaryType.VARBINARY));
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
    public void testDbCacheHit() {
        // Test that second getDb call uses cache (getCatalogs only called once)
        try {
            // Enable cache for this test and create new JDBCMetadata instance
            Map<String, String> cachedProperties = new HashMap<>(properties);
            cachedProperties.put("jdbc_meta_cache_enable", "true");

            JDBCMetadata jdbcMetadata = new JDBCMetadata(cachedProperties, "catalog", dataSource);

            new Expectations() {
                {
                    dataSource.getConnection();
                    result = connection;
                    minTimes = 0;

                    // getCatalogs should only be called ONCE due to caching
                    connection.getMetaData().getCatalogs();
                    result = dbResult;
                    minTimes = 1;
                    maxTimes = 1;

                    connection.getMetaData().getTables("test", null, null,
                            new String[] {"TABLE", "VIEW"});
                    result = tableResult;
                    minTimes = 0;

                    connection.getMetaData().getColumns("test", null, "tbl1", "%");
                    result = columnResult;
                    minTimes = 0;
                }
            };

            dbResult.beforeFirst();
            Database db1 = jdbcMetadata.getDb(new ConnectContext(), "test");
            Assertions.assertNotNull(db1);
            Assertions.assertEquals("test", db1.getOriginName());

            // Second call should use cache, getCatalogs won't be called again
            Database db2 = jdbcMetadata.getDb(new ConnectContext(), "test");
            Assertions.assertNotNull(db2);
            Assertions.assertEquals("test", db2.getOriginName());
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assertions.fail();
        }
    }

    @Test
    public void testDbCacheMissForNonExistentDb() {
        // Test getDb returns null for non-existent database
        try {
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog", dataSource);
            dbResult.beforeFirst();
            Database db = jdbcMetadata.getDb(new ConnectContext(), "nonexistent");
            Assertions.assertNull(db);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assertions.fail();
        }
    }

    @Test
    public void testDbCacheEnabledReturnsNullForNonExistentDb() throws SQLException {
        // Test that getDb returns null (not NPE) for non-existent database when cache is enabled
        // This is the bug fix test - previously it would throw NullPointerException due to
        // Objects.requireNonNull() in JDBCMetaCache.get() when the lambda returned null

        MockResultSet emptySchemaResult = new MockResultSet("schemas");
        emptySchemaResult.addColumn("TABLE_SCHEM", Arrays.asList("information_schema"));

        new Expectations() {
            {
                dataSource.getConnection();
                result = connection;
                minTimes = 0;

                // Mock getSchemas to return empty result (simulating non-existent database)
                connection.getMetaData().getSchemas();
                result = emptySchemaResult;
                minTimes = 0;
            }
        };

        // Enable cache
        Map<String, String> cachedProperties = new HashMap<>(properties);
        cachedProperties.put("jdbc_meta_cache_enable", "true");

        JDBCMetadata jdbcMetadata = new JDBCMetadata(cachedProperties, "catalog", dataSource);

        // First call - should return null without throwing NPE
        Database db1 = jdbcMetadata.getDb(new ConnectContext(), "nonexistent_db");
        Assertions.assertNull(db1, "First call should return null for non-existent database");

        // Second call - should also return null (not cached, and no NPE)
        Database db2 = jdbcMetadata.getDb(new ConnectContext(), "nonexistent_db");
        Assertions.assertNull(db2, "Second call should also return null for non-existent database");
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

    @Test
    public void testGetConnectionSetsNetworkTimeout() throws SQLException {
        long originalTimeoutMs = Config.jdbc_network_timeout_ms;
        try {
            // Verify timeout value is correctly passed (Optimization 1)
            Config.jdbc_network_timeout_ms = 12345;
            new Expectations() {
                {
                    connection.setNetworkTimeout(
                            (java.util.concurrent.ExecutorService) any,
                            12345);  // Verify config value is passed correctly in milliseconds
                    minTimes = 1;  // Fail test if setNetworkTimeout is not called

                    connection.getMetaData().getCatalogs();
                    result = dbResult;
                    minTimes = 0;
                }
            };

            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog", dataSource);
            dbResult.beforeFirst();
            List<String> result = jdbcMetadata.listDbNames(new ConnectContext());
            // Assert the operation completes successfully
            Assertions.assertEquals(Lists.newArrayList("test"), result);

        } finally {
            Config.jdbc_network_timeout_ms = originalTimeoutMs;
        }
    }

    @Test
    public void testNetworkTimeoutBoundaryConditions() throws SQLException {
        long originalTimeoutMs = Config.jdbc_network_timeout_ms;
        try {
            // Test boundary conditions (Optimization 2)

            // Boundary 1: timeout = 1ms (smallest positive value)
            Config.jdbc_network_timeout_ms = 1;
            new Expectations() {
                {
                    connection.setNetworkTimeout((Executor) any, 1);
                    minTimes = 1;
                    connection.getMetaData().getCatalogs();
                    result = dbResult;
                }
            };
            JDBCMetadata jdbcMetadata1 = new JDBCMetadata(properties, "catalog", dataSource);
            dbResult.beforeFirst();
            List<String> result1 = jdbcMetadata1.listDbNames(new ConnectContext());
            Assertions.assertEquals(Lists.newArrayList("test"), result1, "boundary: timeout=1ms");

            // Boundary 2: timeout = Integer.MAX_VALUE
            Config.jdbc_network_timeout_ms = Integer.MAX_VALUE;
            new Expectations() {
                {
                    connection.setNetworkTimeout((Executor) any, Integer.MAX_VALUE);
                    minTimes = 1;
                    connection.getMetaData().getCatalogs();
                    result = dbResult;
                }
            };
            JDBCMetadata jdbcMetadata2 = new JDBCMetadata(properties, "catalog", dataSource);
            dbResult.beforeFirst();
            List<String> result2 = jdbcMetadata2.listDbNames(new ConnectContext());
            Assertions.assertEquals(Lists.newArrayList("test"), result2, "boundary: timeout=MAX_VALUE");

            // Boundary 3: timeout = -1, setNetworkTimeout should NOT be called (code has >= 0 check)
            Config.jdbc_network_timeout_ms = -1;
            new Expectations() {
                {
                    // When timeout = -1, setNetworkTimeout should not be called
                    connection.setNetworkTimeout((Executor) any, anyInt);
                    maxTimes = 0;  // Fail test if setNetworkTimeout is called
                    connection.getMetaData().getCatalogs();
                    result = dbResult;
                }
            };
            JDBCMetadata jdbcMetadata3 = new JDBCMetadata(properties, "catalog", dataSource);
            dbResult.beforeFirst();
            List<String> result3 = jdbcMetadata3.listDbNames(new ConnectContext());
            Assertions.assertEquals(Lists.newArrayList("test"), result3,
                    "boundary: timeout=-1 (should NOT call setNetworkTimeout)");

        } finally {
            Config.jdbc_network_timeout_ms = originalTimeoutMs;
        }
    }

    @Test
    public void testExecutorServiceReuse() throws SQLException {
        long originalTimeoutMs = Config.jdbc_network_timeout_ms;
        try {
            // Verify ExecutorService reuse (Optimization 5)
            Config.jdbc_network_timeout_ms = 5000;

            // Collect all Executor instances passed to setNetworkTimeout
            final java.util.Set<Executor> executors = new java.util.HashSet<>();

            new Expectations() {
                {
                    // Record all Executor instances
                    connection.setNetworkTimeout((Executor) any, 5000);
                    minTimes = 3;  // Expect 3 calls
                    result = new Delegate() {
                        void setNetworkTimeout(Executor executor, long milliseconds) {
                            executors.add(executor);
                        }
                    };

                    connection.getMetaData().getCatalogs();
                    result = dbResult;
                    minTimes = 0;
                }
            };

            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog", dataSource);

            // Call getConnection multiple times
            dbResult.beforeFirst();
            jdbcMetadata.listDbNames(new ConnectContext());

            dbResult.beforeFirst();
            jdbcMetadata.listDbNames(new ConnectContext());

            dbResult.beforeFirst();
            jdbcMetadata.listDbNames(new ConnectContext());

            // Verify: 3 calls should use the same Executor instance (shared static Executor)
            // This avoids creating new Executor each time, saving resources
            Assertions.assertEquals(1, executors.size(),
                    "Should reuse the same ExecutorService instance for multiple calls");
            Assertions.assertNotNull(executors.iterator().next(),
                    "Executor should be the shared static instance");

        } finally {
            Config.jdbc_network_timeout_ms = originalTimeoutMs;
        }
    }

    @Test
    public void testGetTableResultSetClosed() throws SQLException {
        // Test that ResultSet is properly closed after getTable() to prevent cursor leaks
        // This is a regression test for ORA-01000: maximum open cursors exceeded

        // Create a mock ResultSet that tracks whether close() was called
        final boolean[] closed = {false};
        MockResultSet trackableColumnResult = new MockResultSet("columns") {
            @Override
            public void close() throws SQLException {
                closed[0] = true;
                super.close();
            }
        };
        trackableColumnResult.addColumn("DATA_TYPE",
                Arrays.asList(Types.INTEGER, Types.VARCHAR));
        trackableColumnResult.addColumn("TYPE_NAME",
                Arrays.asList("INTEGER", "VARCHAR"));
        trackableColumnResult.addColumn("COLUMN_SIZE", Arrays.asList(4, 100));
        trackableColumnResult.addColumn("DECIMAL_DIGITS", Arrays.asList(0, 0));
        trackableColumnResult.addColumn("COLUMN_NAME", Arrays.asList("id", "name"));
        trackableColumnResult.addColumn("IS_NULLABLE", Arrays.asList("NO", "YES"));

        new Expectations() {
            {
                dataSource.getConnection();
                result = connection;
                minTimes = 0;

                connection.getMetaData().getColumns("test", null, "trackable_tbl", "%");
                result = trackableColumnResult;
                minTimes = 1;
            }
        };

        JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog", dataSource);

        // Call getTable which should properly close the ResultSet
        Table table = jdbcMetadata.getTable(new ConnectContext(), "test", "trackable_tbl");

        // Verify table was retrieved successfully
        Assertions.assertNotNull(table, "Table should be retrieved successfully");
        Assertions.assertTrue(table instanceof JDBCTable, "Table should be JDBCTable");

        // Most importantly: verify ResultSet was closed
        // This prevents cursor leaks (ORA-01000 in Oracle)
        Assertions.assertTrue(closed[0], "ResultSet must be closed after getTable() to prevent cursor leaks");
    }
}
