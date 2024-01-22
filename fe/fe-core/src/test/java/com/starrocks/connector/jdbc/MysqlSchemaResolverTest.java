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
import com.starrocks.catalog.JDBCResource;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.catalog.Type;
import com.starrocks.common.DdlException;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.UtFrameUtils;
import com.zaxxer.hikari.HikariDataSource;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.starrocks.catalog.JDBCResource.DRIVER_CLASS;

public class MysqlSchemaResolverTest {

    private static ConnectContext connectContext;

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Mocked
    HikariDataSource dataSource;

    @Mocked
    Connection connection;

    @Mocked
    PreparedStatement preparedStatement;

    private Map<String, String> properties;
    private MockResultSet dbResult;
    private MockResultSet tableResult;
    private MockResultSet partitionsResult;
    private Map<JDBCTableName, Integer> tableIdCache;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
    }

    @Before
    public void setUp() throws SQLException {
        partitionsResult = new MockResultSet("partitions");
        partitionsResult.addColumn("NAME", Arrays.asList("'20230810'"));
        partitionsResult.addColumn("PARTITION_EXPRESSION", Arrays.asList("`d`"));
        partitionsResult.addColumn("MODIFIED_TIME", Arrays.asList("2023-08-01 00:00:00"));

        properties = new HashMap<>();
        properties.put(DRIVER_CLASS, "com.mysql.cj.jdbc.Driver");
        properties.put(JDBCResource.URI, "jdbc:mysql://127.0.0.1:3306");
        properties.put(JDBCResource.USER, "root");
        properties.put(JDBCResource.PASSWORD, "123456");
        properties.put(JDBCResource.CHECK_SUM, "xxxx");
        properties.put(JDBCResource.DRIVER_URL, "xxxx");
        tableIdCache = new ConcurrentHashMap<>();
        tableIdCache.put(JDBCTableName.of("catalog", "test", "tbl1"), 100000);

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
    public void testCheckPartitionWithoutPartitionsTable() {
        try {
            JDBCSchemaResolver schemaResolver = new MysqlSchemaResolver();
            Assert.assertFalse(schemaResolver.checkAndSetSupportPartitionInformation(connection));
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.fail();
        }
    }

    @Test
    public void testCheckPartitionWithPartitionsTable() throws SQLException {
        new Expectations() {
            {
                String catalogSchema = "information_schema";

                dbResult = new MockResultSet("catalog");
                dbResult.addColumn("TABLE_CAT", Arrays.asList(catalogSchema));

                connection.getMetaData().getCatalogs();
                result = dbResult;
                minTimes = 0;

                MockResultSet piResult = new MockResultSet("partitions");
                piResult.addColumn("TABLE_NAME", Arrays.asList("partitions"));
                connection.getMetaData().getTables(anyString, null, null, null);
                result = piResult;
                minTimes = 0;
            }
        };
        try {
            JDBCSchemaResolver schemaResolver = new MysqlSchemaResolver();
            Assert.assertTrue(schemaResolver.checkAndSetSupportPartitionInformation(connection));
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.fail();
        }
    }

    @Test
    public void testListPartitionNames() {
        try {
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog", dataSource);
            List<String> partitionNames = jdbcMetadata.listPartitionNames("test", "tbl1");
            Assert.assertFalse(partitionNames.isEmpty());
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.fail();
        }
    }

    @Test
    public void testListPartitionNamesWithCache() {
        try {
            JDBCCacheTestUtil.openCacheEnable(connectContext);
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog", dataSource);
            List<String> partitionNames = jdbcMetadata.listPartitionNames("test", "tbl1");
            Assert.assertFalse(partitionNames.isEmpty());
            List<String> partitionNamesWithCache = jdbcMetadata.listPartitionNames("test", "tbl1");
            Assert.assertFalse(partitionNamesWithCache.isEmpty());
            JDBCCacheTestUtil.closeCacheEnable(connectContext);
            Map<String, String> properties = new HashMap<>();
            jdbcMetadata.refreshCache(properties);
            List<String> partitionNamesWithOutCache = jdbcMetadata.listPartitionNames("test", "tbl1");
            Assert.assertTrue(partitionNamesWithOutCache.isEmpty());
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.fail();
        }
    }

    @Test
    public void testListPartitionNamesRsNull() {
        try {
            new Expectations() {
                {
                    preparedStatement.executeQuery();
                    result = null;
                    minTimes = 0;
                }
            };
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog", dataSource);
            List<String> partitionNames = jdbcMetadata.listPartitionNames("test", "tbl1");
            Assert.assertTrue(partitionNames.size() == 0);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.fail();
        }
    }

    @Test
    public void testListPartitionColumns() {
        try {
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog", dataSource);
            Integer size = jdbcMetadata.listPartitionColumns("test", "tbl1",
                    Arrays.asList(new Column("d", Type.VARCHAR))).size();
            Assert.assertTrue(size > 0);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.fail();
        }
    }

    @Test
    public void testListPartitionColumnsRsNull() {
        try {
            new Expectations() {
                {
                    preparedStatement.executeQuery();
                    result = null;
                    minTimes = 0;
                }
            };
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog", dataSource);
            Integer size = jdbcMetadata.listPartitionColumns("test", "tbl1",
                    Arrays.asList(new Column("d", Type.VARCHAR))).size();
            Assert.assertTrue(size == 0);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.fail();
        }
    }

    @Test
    public void testGetPartitions() {
        try {
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog", dataSource);
            JDBCTable jdbcTable = new JDBCTable(100000, "tbl1", Arrays.asList(new Column("d", Type.VARCHAR)),
                    Arrays.asList(new Column("d", Type.VARCHAR)), "test", "catalog", properties);
            Integer size = jdbcMetadata.getPartitions(jdbcTable, Arrays.asList("20230810")).size();
            Assert.assertTrue(size > 0);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.fail();
        }
    }

    @Test
    public void testGetPartitionsWithCache() {
        try {
            JDBCCacheTestUtil.openCacheEnable(connectContext);
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog", dataSource);
            JDBCTable jdbcTable = new JDBCTable(100000, "tbl1", Arrays.asList(new Column("d", Type.VARCHAR)),
                    Arrays.asList(new Column("d", Type.VARCHAR)), "test", "catalog", properties);
            int size = jdbcMetadata.getPartitions(jdbcTable, Arrays.asList("20230810")).size();
            Assert.assertTrue(size > 0);
            int sizeWithCache = jdbcMetadata.getPartitions(jdbcTable, Arrays.asList("20230810")).size();
            Assert.assertTrue(sizeWithCache > 0);
            JDBCCacheTestUtil.closeCacheEnable(connectContext);
            Map<String, String> properties = new HashMap<>();
            jdbcMetadata.refreshCache(properties);
            int sizeWithOutCache = jdbcMetadata.getPartitions(jdbcTable, Arrays.asList("20230810")).size();
            Assert.assertEquals(0, sizeWithOutCache);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.fail();
        }
    }

    @Test
    public void testGetPartitions_NonPartitioned() throws DdlException {
        JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog", dataSource);
        List<Column> columns = Arrays.asList(new Column("d", Type.VARCHAR));
        JDBCTable jdbcTable = new JDBCTable(100000, "tbl1", columns, Lists.newArrayList(),
                "test", "catalog", properties);
        int size = jdbcMetadata.getPartitions(jdbcTable, Arrays.asList("20230810")).size();
        Assert.assertEquals(1, size);
        List<String> partitionNames = PartitionUtil.getPartitionNames(jdbcTable);
        Assert.assertEquals(Arrays.asList("tbl1"), partitionNames);
    }

    @Test
    public void testGetPartitionsRsNull() {
        try {
            new Expectations() {
                {
                    preparedStatement.executeQuery();
                    result = null;
                    minTimes = 0;
                }
            };
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog", dataSource);
            JDBCTable jdbcTable = new JDBCTable(100000, "tbl1", Arrays.asList(new Column("d", Type.VARCHAR)),
                    Arrays.asList(new Column("d", Type.VARCHAR)), "test", "catalog", properties);
            Integer size = jdbcMetadata.getPartitions(jdbcTable, Arrays.asList("20230810")).size();
            Assert.assertTrue(size == 0);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.fail();
        }
    }

    @Test
    public void testMysqlInvalidPartition() {
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
            List<Column> columns = Arrays.asList(new Column("d", Type.VARCHAR));
            JDBCTable jdbcTable = new JDBCTable(100000, "tbl1", columns, Lists.newArrayList(),
                    "test", "catalog", properties);
            jdbcMetadata.getPartitions(jdbcTable, Arrays.asList("20230810")).size();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Timestamp format must be yyyy-mm-dd hh:mm:ss"));
        }
    }
}
