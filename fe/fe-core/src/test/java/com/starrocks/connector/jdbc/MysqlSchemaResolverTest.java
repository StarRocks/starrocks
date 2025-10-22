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
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.Type;
import com.starrocks.common.DdlException;
import com.starrocks.connector.ConnectorMetadatRequestContext;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.UtFrameUtils;
import com.zaxxer.hikari.HikariDataSource;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
    }

    @BeforeEach
    public void setUp() throws SQLException {
        partitionsResult = new MockResultSet("partitions");
        partitionsResult.addColumn("NAME", Arrays.asList("'20230810'"));
        partitionsResult.addColumn("PARTITION_EXPRESSION", Arrays.asList("`d`"));
        partitionsResult.addColumn("MODIFIED_TIME", Arrays.asList("2023-08-01 00:00:00"));

        properties = new HashMap<>();
        properties.put(DRIVER_CLASS, "org.mariadb.jdbc.Driver");
        properties.put(JDBCResource.URI, "jdbc:mariadb://127.0.0.1:3306");
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
            Assertions.assertFalse(schemaResolver.checkAndSetSupportPartitionInformation(connection));
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
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
            Assertions.assertTrue(schemaResolver.checkAndSetSupportPartitionInformation(connection));
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testListPartitionNames() {
        try {
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog", dataSource);
            List<String> partitionNames = jdbcMetadata.listPartitionNames("test", "tbl1", ConnectorMetadatRequestContext.DEFAULT);
            Assertions.assertFalse(partitionNames.isEmpty());
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testListPartitionNamesWithCache() {
        try {
            JDBCCacheTestUtil.openCacheEnable(connectContext);
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog", dataSource);
            List<String> partitionNames = jdbcMetadata.listPartitionNames("test", "tbl1",
                    ConnectorMetadatRequestContext.DEFAULT);
            Assertions.assertFalse(partitionNames.isEmpty());
            List<String> partitionNamesWithCache =
                    jdbcMetadata.listPartitionNames("test", "tbl1", ConnectorMetadatRequestContext.DEFAULT);
            Assertions.assertFalse(partitionNamesWithCache.isEmpty());
            JDBCCacheTestUtil.closeCacheEnable(connectContext);
            Map<String, String> properties = new HashMap<>();
            jdbcMetadata.refreshCache(properties);
            List<String> partitionNamesWithOutCache =
                    jdbcMetadata.listPartitionNames("test", "tbl1", ConnectorMetadatRequestContext.DEFAULT);
            Assertions.assertTrue(partitionNamesWithOutCache.isEmpty());
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
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
            List<String> partitionNames = jdbcMetadata.listPartitionNames("test", "tbl1", ConnectorMetadatRequestContext.DEFAULT);
            Assertions.assertTrue(partitionNames.size() == 0);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testListPartitionColumns() {
        try {
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog", dataSource);
            Integer size = jdbcMetadata.listPartitionColumns("test", "tbl1",
                    Arrays.asList(new Column("d", Type.VARCHAR))).size();
            Assertions.assertTrue(size > 0);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
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
            Assertions.assertTrue(size == 0);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testGetPartitions() {
        try {
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog", dataSource);
            JDBCTable jdbcTable = new JDBCTable(100000, "tbl1", Arrays.asList(new Column("d", Type.VARCHAR)),
                    Arrays.asList(new Column("d", Type.VARCHAR)), "test", "catalog", properties);
            Integer size = jdbcMetadata.getPartitions(jdbcTable, Arrays.asList("20230810")).size();
            Assertions.assertTrue(size > 0);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
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
            Assertions.assertTrue(size > 0);
            int sizeWithCache = jdbcMetadata.getPartitions(jdbcTable, Arrays.asList("20230810")).size();
            Assertions.assertTrue(sizeWithCache > 0);
            JDBCCacheTestUtil.closeCacheEnable(connectContext);
            Map<String, String> properties = new HashMap<>();
            jdbcMetadata.refreshCache(properties);
            int sizeWithOutCache = jdbcMetadata.getPartitions(jdbcTable, Arrays.asList("20230810")).size();
            Assertions.assertEquals(0, sizeWithOutCache);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testGetPartitions_NonPartitioned() throws DdlException {
        JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog", dataSource);
        List<Column> columns = Arrays.asList(new Column("d", Type.VARCHAR));
        JDBCTable jdbcTable = new JDBCTable(100000, "tbl1", columns, Lists.newArrayList(),
                "test", "catalog", properties);
        int size = jdbcMetadata.getPartitions(jdbcTable, Arrays.asList("20230810")).size();
        Assertions.assertEquals(1, size);
        List<String> partitionNames = PartitionUtil.getPartitionNames(jdbcTable);
        Assertions.assertEquals(Arrays.asList("tbl1"), partitionNames);
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
            Assertions.assertTrue(size == 0);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testGetPartitionsRsNonRecord() {
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
            Integer size = jdbcMetadata.getPartitions(jdbcTable, Arrays.asList("tbl1")).size();
            Assertions.assertTrue(size == 1);

        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assertions.fail();
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
            List<Column> columns = Arrays.asList(new Column("d", Type.VARCHAR));
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
            List<Column> columns = Arrays.asList(new Column("d", Type.VARCHAR));
            JDBCTable jdbcTable = new JDBCTable(100000, "tbl1", columns, Lists.newArrayList(),
                    "test", "catalog", properties);
            jdbcMetadata.getPartitions(jdbcTable, Arrays.asList("20230810")).size();
        } catch (Exception e) {
            Assertions.fail();
        }
    }

    @Test
    public void testConvertNumericTypes() {
        MysqlSchemaResolver resolver = new MysqlSchemaResolver();
        
        Type type = resolver.convertColumnType(java.sql.Types.BIT, "BIT", 1, 0);
        Assertions.assertEquals(PrimitiveType.BOOLEAN, type.getPrimitiveType());
        
        type = resolver.convertColumnType(java.sql.Types.BIT, "BIT", 5, 0);
        Assertions.assertEquals(PrimitiveType.BOOLEAN, type.getPrimitiveType());
        
        type = resolver.convertColumnType(java.sql.Types.BIT, "BIT", 64, 0);
        Assertions.assertEquals(PrimitiveType.BOOLEAN, type.getPrimitiveType());
        
        type = resolver.convertColumnType(java.sql.Types.BOOLEAN, "BIT", 5, 0);
        Assertions.assertEquals(PrimitiveType.BOOLEAN, type.getPrimitiveType());
        
        type = resolver.convertColumnType(java.sql.Types.BOOLEAN, "BIT", 64, 0);
        Assertions.assertEquals(PrimitiveType.BOOLEAN, type.getPrimitiveType());
        
        type = resolver.convertColumnType(java.sql.Types.TINYINT, "TINYINT", 3, 0);
        Assertions.assertEquals(PrimitiveType.TINYINT, type.getPrimitiveType());
        
        type = resolver.convertColumnType(java.sql.Types.TINYINT, "TINYINT UNSIGNED", 3, 0);
        Assertions.assertEquals(PrimitiveType.SMALLINT, type.getPrimitiveType());
        
        type = resolver.convertColumnType(java.sql.Types.SMALLINT, "SMALLINT", 5, 0);
        Assertions.assertEquals(PrimitiveType.SMALLINT, type.getPrimitiveType());
        
        type = resolver.convertColumnType(java.sql.Types.SMALLINT, "SMALLINT UNSIGNED", 5, 0);
        Assertions.assertEquals(PrimitiveType.INT, type.getPrimitiveType());
        
        type = resolver.convertColumnType(java.sql.Types.INTEGER, "MEDIUMINT", 7, 0);
        Assertions.assertEquals(PrimitiveType.INT, type.getPrimitiveType());
        
        type = resolver.convertColumnType(java.sql.Types.INTEGER, "MEDIUMINT UNSIGNED", 8, 0);
        Assertions.assertEquals(PrimitiveType.INT, type.getPrimitiveType());
        
        type = resolver.convertColumnType(java.sql.Types.INTEGER, "INT", 10, 0);
        Assertions.assertEquals(PrimitiveType.INT, type.getPrimitiveType());
        
        type = resolver.convertColumnType(java.sql.Types.INTEGER, "INT UNSIGNED", 10, 0);
        Assertions.assertEquals(PrimitiveType.BIGINT, type.getPrimitiveType());
        
        type = resolver.convertColumnType(java.sql.Types.BIGINT, "BIGINT", 19, 0);
        Assertions.assertEquals(PrimitiveType.BIGINT, type.getPrimitiveType());
        
        type = resolver.convertColumnType(java.sql.Types.BIGINT, "BIGINT UNSIGNED", 20, 0);
        Assertions.assertEquals(PrimitiveType.LARGEINT, type.getPrimitiveType());
        
        type = resolver.convertColumnType(java.sql.Types.FLOAT, "FLOAT", 12, 0);
        Assertions.assertEquals(PrimitiveType.FLOAT, type.getPrimitiveType());
        
        type = resolver.convertColumnType(java.sql.Types.DOUBLE, "DOUBLE", 22, 0);
        Assertions.assertEquals(PrimitiveType.DOUBLE, type.getPrimitiveType());
        
        type = resolver.convertColumnType(java.sql.Types.DECIMAL, "DECIMAL", 10, 2);
        Assertions.assertTrue(type.getPrimitiveType().isDecimalV3Type());
    }

    @Test
    public void testConvertStringTypes() {
        MysqlSchemaResolver resolver = new MysqlSchemaResolver();
        
        Type type = resolver.convertColumnType(java.sql.Types.CHAR, "CHAR", 10, 0);
        Assertions.assertEquals(PrimitiveType.CHAR, type.getPrimitiveType());
        
        type = resolver.convertColumnType(java.sql.Types.VARCHAR, "VARCHAR", 255, 0);
        Assertions.assertEquals(PrimitiveType.VARCHAR, type.getPrimitiveType());
        
        type = resolver.convertColumnType(java.sql.Types.BINARY, "BINARY", 10, 0);
        Assertions.assertEquals(PrimitiveType.VARBINARY, type.getPrimitiveType());
        
        type = resolver.convertColumnType(java.sql.Types.VARBINARY, "VARBINARY", 255, 0);
        Assertions.assertEquals(PrimitiveType.VARBINARY, type.getPrimitiveType());
        
        type = resolver.convertColumnType(java.sql.Types.LONGVARBINARY, "TINYBLOB", 255, 0);
        Assertions.assertEquals(PrimitiveType.VARBINARY, type.getPrimitiveType());
        
        type = resolver.convertColumnType(java.sql.Types.LONGVARBINARY, "BLOB", 65535, 0);
        Assertions.assertEquals(PrimitiveType.VARBINARY, type.getPrimitiveType());
        
        type = resolver.convertColumnType(java.sql.Types.LONGVARBINARY, "MEDIUMBLOB", 16777215, 0);
        Assertions.assertEquals(PrimitiveType.VARBINARY, type.getPrimitiveType());
        
        type = resolver.convertColumnType(java.sql.Types.LONGVARBINARY, "LONGBLOB", 2147483647, 0);
        Assertions.assertEquals(PrimitiveType.VARBINARY, type.getPrimitiveType());
        
        type = resolver.convertColumnType(java.sql.Types.LONGVARCHAR, "TINYTEXT", 255, 0);
        Assertions.assertEquals(PrimitiveType.VARCHAR, type.getPrimitiveType());
        
        type = resolver.convertColumnType(java.sql.Types.LONGVARCHAR, "TEXT", 65535, 0);
        Assertions.assertEquals(PrimitiveType.VARCHAR, type.getPrimitiveType());
        
        type = resolver.convertColumnType(java.sql.Types.LONGVARCHAR, "MEDIUMTEXT", 16777215, 0);
        Assertions.assertEquals(PrimitiveType.VARCHAR, type.getPrimitiveType());
        
        type = resolver.convertColumnType(java.sql.Types.LONGVARCHAR, "LONGTEXT", 2147483647, 0);
        Assertions.assertEquals(PrimitiveType.VARCHAR, type.getPrimitiveType());
        
        type = resolver.convertColumnType(java.sql.Types.CHAR, "ENUM", 5, 0);
        Assertions.assertEquals(PrimitiveType.VARCHAR, type.getPrimitiveType());
        
        type = resolver.convertColumnType(java.sql.Types.CHAR, "SET", 21, 0);
        Assertions.assertEquals(PrimitiveType.VARCHAR, type.getPrimitiveType());
    }

    @Test
    public void testConvertDateTimeTypes() {
        MysqlSchemaResolver resolver = new MysqlSchemaResolver();
        
        Type type = resolver.convertColumnType(java.sql.Types.DATE, "DATE", 10, 0);
        Assertions.assertEquals(PrimitiveType.DATE, type.getPrimitiveType());
        
        type = resolver.convertColumnType(java.sql.Types.TIME, "TIME", 10, 0);
        Assertions.assertEquals(PrimitiveType.TIME, type.getPrimitiveType());
        
        type = resolver.convertColumnType(java.sql.Types.TIME, "TIME", 16, 6);
        Assertions.assertEquals(PrimitiveType.TIME, type.getPrimitiveType());
        
        type = resolver.convertColumnType(java.sql.Types.TIMESTAMP, "DATETIME", 19, 0);
        Assertions.assertEquals(PrimitiveType.DATETIME, type.getPrimitiveType());
        
        type = resolver.convertColumnType(java.sql.Types.TIMESTAMP, "DATETIME", 26, 6);
        Assertions.assertEquals(PrimitiveType.DATETIME, type.getPrimitiveType());
        
        type = resolver.convertColumnType(java.sql.Types.TIMESTAMP, "TIMESTAMP", 19, 0);
        Assertions.assertEquals(PrimitiveType.DATETIME, type.getPrimitiveType());
        
        type = resolver.convertColumnType(java.sql.Types.TIMESTAMP, "TIMESTAMP", 26, 6);
        Assertions.assertEquals(PrimitiveType.DATETIME, type.getPrimitiveType());
        
        type = resolver.convertColumnType(java.sql.Types.DATE, "YEAR", 4, 0);
        Assertions.assertEquals(PrimitiveType.DATE, type.getPrimitiveType());
    }

    @Test
    public void testConvertJsonType() {
        MysqlSchemaResolver resolver = new MysqlSchemaResolver();
        
        Type type = resolver.convertColumnType(java.sql.Types.LONGVARCHAR, "JSON", 1073741824, 0);
        Assertions.assertEquals(PrimitiveType.VARCHAR, type.getPrimitiveType());
        
        type = resolver.convertColumnType(java.sql.Types.OTHER, "JSON", 1073741824, 0);
        Assertions.assertEquals(PrimitiveType.VARCHAR, type.getPrimitiveType());
    }

    @Test
    public void testConvertSpatialTypes() {
        MysqlSchemaResolver resolver = new MysqlSchemaResolver();
        
        Type type = resolver.convertColumnType(java.sql.Types.LONGVARBINARY, "GEOMETRY", 2147483647, 0);
        Assertions.assertEquals(PrimitiveType.VARBINARY, type.getPrimitiveType());
        
        type = resolver.convertColumnType(java.sql.Types.LONGVARBINARY, "POINT", 2147483647, 0);
        Assertions.assertEquals(PrimitiveType.VARBINARY, type.getPrimitiveType());
        
        type = resolver.convertColumnType(java.sql.Types.LONGVARBINARY, "LINESTRING", 2147483647, 0);
        Assertions.assertEquals(PrimitiveType.VARBINARY, type.getPrimitiveType());
        
        type = resolver.convertColumnType(java.sql.Types.LONGVARBINARY, "POLYGON", 2147483647, 0);
        Assertions.assertEquals(PrimitiveType.VARBINARY, type.getPrimitiveType());
        
        type = resolver.convertColumnType(java.sql.Types.LONGVARBINARY, "MULTIPOINT", 2147483647, 0);
        Assertions.assertEquals(PrimitiveType.VARBINARY, type.getPrimitiveType());
        
        type = resolver.convertColumnType(java.sql.Types.LONGVARBINARY, "MULTILINESTRING", 2147483647, 0);
        Assertions.assertEquals(PrimitiveType.VARBINARY, type.getPrimitiveType());
        
        type = resolver.convertColumnType(java.sql.Types.LONGVARBINARY, "MULTIPOLYGON", 2147483647, 0);
        Assertions.assertEquals(PrimitiveType.VARBINARY, type.getPrimitiveType());
        
        type = resolver.convertColumnType(java.sql.Types.LONGVARBINARY, "GEOMETRYCOLLECTION", 2147483647, 0);
        Assertions.assertEquals(PrimitiveType.VARBINARY, type.getPrimitiveType());
        
        type = resolver.convertColumnType(java.sql.Types.LONGVARCHAR, "GEOMETRYCOLLECTION", 1073741824, 0);
        Assertions.assertEquals(PrimitiveType.VARBINARY, type.getPrimitiveType());
        
        type = resolver.convertColumnType(java.sql.Types.OTHER, "GEOMETRY", 2147483647, 0);
        Assertions.assertEquals(PrimitiveType.VARBINARY, type.getPrimitiveType());
        
        type = resolver.convertColumnType(java.sql.Types.OTHER, "POINT", 2147483647, 0);
        Assertions.assertEquals(PrimitiveType.VARBINARY, type.getPrimitiveType());
        
        type = resolver.convertColumnType(java.sql.Types.OTHER, "GEOMETRYCOLLECTION", 1073741824, 0);
        Assertions.assertEquals(PrimitiveType.VARBINARY, type.getPrimitiveType());
    }
}
