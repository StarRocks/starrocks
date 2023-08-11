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

import com.mockrunner.mock.jdbc.MockResultSet;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.JDBCResource;
import com.starrocks.connector.PartitionInfo;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.catalog.JDBCResource.DRIVER_CLASS;


public class MysqlSchemaResolverTest {

    @Mocked
    DriverManager driverManager;

    @Mocked
    Connection connection;

    @Mocked
    PreparedStatement preparedStatement;

    private Map<String, String> properties;
    private MockResultSet dbResult;
    private MockResultSet tableResult;
    private MockResultSet columnResult;
    private MockResultSet partitionsResult;

    @Before
    public void setUp() throws SQLException {
        dbResult = new MockResultSet("catalog");
        dbResult.addColumn("TABLE_CAT", Arrays.asList("information_schema", "mysql", "test"));
        tableResult = new MockResultSet("tables");
        tableResult.addColumn("TABLE_NAME", Arrays.asList("tbl1", "tbl2", "tbl3"));
        columnResult = new MockResultSet("columns");
        columnResult.addColumn("DATA_TYPE", Arrays.asList(Types.INTEGER, Types.DECIMAL, Types.CHAR, Types.VARCHAR));
        columnResult.addColumn("TYPE_NAME", Arrays.asList("INTEGER", "DECIMAL", "CHAR", "VARCHAR"));
        columnResult.addColumn("COLUMN_SIZE", Arrays.asList(4, 10, 10, 10));
        columnResult.addColumn("DECIMAL_DIGITS", Arrays.asList(0, 2, 0, 0));
        columnResult.addColumn("COLUMN_NAME", Arrays.asList("a", "b", "c", "d"));
        columnResult.addColumn("IS_NULLABLE", Arrays.asList("YES", "NO", "NO", "NO"));
        partitionsResult = new MockResultSet("partitions");
        partitionsResult.addColumn("PARTITION_DESCRIPTION", Arrays.asList("'20230810'"));
        partitionsResult.addColumn("PARTITION_EXPRESSION", Arrays.asList("`d`"));
        partitionsResult.addColumn("CREATE_TIME", Arrays.asList(new Date(1691596800L)));
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

                preparedStatement.executeQuery();
                result = partitionsResult;
                minTimes = 0;
            }
        };
    }

    @Test
    public void testListPartitionNames() {
        try {
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog");
            List<String> partitionNames = jdbcMetadata.listPartitionNames("test", "tbl1");
            Assert.assertTrue(partitionNames.size() > 0);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.fail();
        }
    }

    @Test
    public void testListPartitionColumns() {
        try {
            new Expectations() {
                {
                    connection.getMetaData().getColumns("test", null, "tbl1", "%");
                    result = columnResult;
                    minTimes = 0;
                }
            };

            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog");

            JDBCSchemaResolver schemaResolver = new MysqlSchemaResolver();
            List<String> cols = schemaResolver.listPartitionColumns(connection, "test", "tbl1");
            System.out.println("schemaResolver testListPartitionColumns : " + cols);
            System.out.println("schemaResolver testListPartitionColumns fullSchema: " +
                    jdbcMetadata.getTable("test", "tbl1").getFullSchema());

            List<Column> partitionColumns = jdbcMetadata.listPartitionColumns("test", "tbl1",
                    jdbcMetadata.getTable("test", "tbl1").getFullSchema());
            System.out.println("schemaResolver testListPartitionColumns partitionColumns : " + partitionColumns);
            Assert.assertTrue(partitionColumns.size() > 0);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.fail();
        }
    }

    @Test
    public void testGetPartitions() {
        try {
            new Expectations() {
                {
                    connection.getMetaData().getColumns("test", null, "tbl1", "%");
                    result = columnResult;
                    minTimes = 0;
                }
            };

            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog");

            JDBCSchemaResolver schemaResolver = new MysqlSchemaResolver();
            List<Partition> partitionlls = schemaResolver.getPartitions(connection, jdbcMetadata.getTable("test", "tbl1"));
            System.out.println("schemaResolver testGetPartitions : " + partitionlls);

            List<PartitionInfo> partitions = jdbcMetadata.getPartitions(
                    jdbcMetadata.getTable("test", "tbl1"), Arrays.asList("20230810"));
            System.out.println("testGetPartitions:" + partitions);
            Assert.assertTrue(partitions.size() > 0);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assert.fail();
        }
    }

}
