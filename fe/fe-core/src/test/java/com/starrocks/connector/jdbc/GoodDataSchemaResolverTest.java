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
import com.starrocks.catalog.Table;
import com.starrocks.qe.ConnectContext;
import com.zaxxer.hikari.HikariDataSource;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GoodDataSchemaResolverTest {
    @Mocked
    HikariDataSource dataSource;

    @Mocked
    Connection connection;

    private Map<String, String> properties;
    private MockResultSet tableResult;
    private MockResultSet columnResult;

    @BeforeEach
    public void setUp() throws SQLException {
        tableResult = new MockResultSet("tables");
        tableResult.addColumn("TABLE_NAME", Arrays.asList("tbl1", "tbl2", "tbl3"));
        
        columnResult = new MockResultSet("columns");
        // Test GoodData types - all report IS_NULLABLE = "NO" but may contain NULLs
        columnResult.addColumn("DATA_TYPE", Arrays.asList(
                Types.VARCHAR,     // varchar
                Types.VARCHAR,     // varchar
                Types.BIGINT,      // integer
                Types.DOUBLE,      // double
                Types.TIMESTAMP,   // timestamptz
                Types.DATE,        // date
                Types.BOOLEAN,     // boolean
                Types.DECIMAL      // numeric
        ));
        columnResult.addColumn("TYPE_NAME", Arrays.asList(
                "Varchar",
                "Varchar",
                "Integer",
                "Double",
                "TimestampTz",
                "Date",
                "Boolean",
                "Numeric"
        ));
        columnResult.addColumn("COLUMN_SIZE", Arrays.asList(512, 512, 19, 17, 26, 10, 1, 18));
        columnResult.addColumn("DECIMAL_DIGITS", Arrays.asList(0, 0, 0, 17, 6, 0, 0, 2));
        columnResult.addColumn("COLUMN_NAME", Arrays.asList(
                "a__abandontype", "a__termreason", "a__index", "f__score",
                "x__timestamp", "d__date", "a__deleted", "f__amount"));
        // GoodData driver incorrectly reports ALL columns as NOT NULL
        columnResult.addColumn("IS_NULLABLE", Arrays.asList(
                "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO"));
        columnResult.addColumn("REMARKS", Arrays.asList(
                "", "", "", "", "", "", "", ""));
        
        properties = new HashMap<>();
        properties.put(JDBCResource.DRIVER_CLASS, "com.gooddata.datawarehouse.jdbc.driver.DatawarehouseDriver");
        properties.put(JDBCResource.URI, "jdbc:gdc:datawarehouse://example.com:443/gdc/datawarehouse/instances/test");
        properties.put(JDBCResource.USER, "user@example.com");
        properties.put(JDBCResource.PASSWORD, "password");
        properties.put(JDBCResource.CHECK_SUM, "xxxx");
        properties.put(JDBCResource.DRIVER_URL, "xxxx");
    }

    @Test
    public void testListTableNames() throws SQLException {
        new Expectations() {
            {
                dataSource.getConnection();
                result = connection;
                minTimes = 0;

                connection.getCatalog();
                result = "test";
                minTimes = 0;

                connection.getMetaData().getTables("test", "schema1", null, new String[] {"TABLE", "VIEW"});
                result = tableResult;
                minTimes = 0;
            }
        };
        try {
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog", dataSource);
            List<String> result = jdbcMetadata.listTableNames(new ConnectContext(), "schema1");
            List<String> expectResult = Lists.newArrayList("tbl1", "tbl2", "tbl3");
            Assertions.assertEquals(expectResult, result);
        } catch (Exception e) {
            Assertions.fail();
        }
    }

    @Test
    public void testGetTableAllColumnsNullable() throws SQLException {
        new Expectations() {
            {
                dataSource.getConnection();
                result = connection;
                minTimes = 0;

                connection.getCatalog();
                result = "test";
                minTimes = 0;

                connection.getMetaData().getColumns("test", "schema1", "tbl1", "%");
                result = columnResult;
                minTimes = 0;
            }
        };
        try {
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog", dataSource);
            Table table = jdbcMetadata.getTable(new ConnectContext(), "schema1", "tbl1");
            Assertions.assertTrue(table instanceof JDBCTable);
            Assertions.assertEquals("catalog.schema1.tbl1", table.getUUID());
            Assertions.assertEquals("tbl1", table.getName());
            Assertions.assertEquals(8, table.getColumns().size());
            
            // CRITICAL TEST: Verify ALL columns are marked as nullable
            // even though IS_NULLABLE metadata says "NO" for all columns
            for (Column column : table.getColumns()) {
                Assertions.assertTrue(column.isAllowNull(),
                        "Column " + column.getName() + " should be nullable but is marked as NOT NULL");
            }
            
            // Verify specific columns that are known to contain NULLs in actual data
            Column abandonTypeCol = table.getColumn("a__abandontype");
            Assertions.assertNotNull(abandonTypeCol);
            Assertions.assertTrue(abandonTypeCol.isAllowNull(),
                    "a__abandontype should be nullable (contains 96.75% NULLs in real data)");
            
            Column termReasonCol = table.getColumn("a__termreason");
            Assertions.assertNotNull(termReasonCol);
            Assertions.assertTrue(termReasonCol.isAllowNull(),
                    "a__termreason should be nullable (contains 35.9% NULLs in real data)");
            
            // Verify column types (inherited from Vertica type conversion)
            Assertions.assertTrue(table.getColumn("a__abandontype").getType().isStringType());
            // GoodData Integer type maps to BIGINT (inherited from Vertica)
            Assertions.assertEquals(PrimitiveType.BIGINT, table.getColumn("a__index").getType().getPrimitiveType());
            Assertions.assertEquals(PrimitiveType.DOUBLE, table.getColumn("f__score").getType().getPrimitiveType());
            Assertions.assertEquals(PrimitiveType.DATETIME, table.getColumn("x__timestamp").getType().getPrimitiveType());
            Assertions.assertEquals(PrimitiveType.DATE, table.getColumn("d__date").getType().getPrimitiveType());
            Assertions.assertEquals(PrimitiveType.BOOLEAN, table.getColumn("a__deleted").getType().getPrimitiveType());
            Assertions.assertTrue(table.getColumn("f__amount").getType().isDecimalOfAnyVersion());
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assertions.fail();
        }
    }

    @Test
    public void testConvertColumnTypeInheritsFromVertica() {
        GoodDataSchemaResolver resolver = new GoodDataSchemaResolver();
        
        // Test that we inherit Vertica's type conversion
        // Vertica INT (Types.INTEGER) maps to BIGINT (64-bit)
        com.starrocks.catalog.Type intType = resolver.convertColumnType(Types.INTEGER, "int", 19, 0);
        Assertions.assertEquals(PrimitiveType.BIGINT, intType.getPrimitiveType());
        
        // Test Varchar type
        com.starrocks.catalog.Type varcharType = resolver.convertColumnType(Types.VARCHAR, "varchar", 512, 0);
        Assertions.assertTrue(varcharType.isStringType());
        
        // Test TimestampTz (Vertica-specific)
        com.starrocks.catalog.Type tsType = resolver.convertColumnType(Types.TIMESTAMP, "timestamptz", 26, 6);
        Assertions.assertEquals(PrimitiveType.DATETIME, tsType.getPrimitiveType());
        
        // Test Date
        com.starrocks.catalog.Type dateType = resolver.convertColumnType(Types.DATE, "date", 10, 0);
        Assertions.assertEquals(PrimitiveType.DATE, dateType.getPrimitiveType());
        
        // Test Boolean
        com.starrocks.catalog.Type boolType = resolver.convertColumnType(Types.BOOLEAN, "boolean", 1, 0);
        Assertions.assertEquals(PrimitiveType.BOOLEAN, boolType.getPrimitiveType());
        
        // Test Numeric/Decimal
        com.starrocks.catalog.Type decimalType = resolver.convertColumnType(Types.DECIMAL, "numeric", 18, 2);
        Assertions.assertTrue(decimalType.isDecimalOfAnyVersion());
        
        // Test Double
        com.starrocks.catalog.Type doubleType = resolver.convertColumnType(Types.DOUBLE, "double precision", 17, 17);
        Assertions.assertEquals(PrimitiveType.DOUBLE, doubleType.getPrimitiveType());
        
        // Test Vertica-specific UUID type (inherited)
        com.starrocks.catalog.Type uuidType = resolver.convertColumnType(Types.VARCHAR, "uuid", 36, 0);
        Assertions.assertTrue(uuidType.isStringType());
        Assertions.assertEquals(36, ((com.starrocks.catalog.ScalarType) uuidType).getLength());
    }

    @Test
    public void testGetPartitions() {
        GoodDataSchemaResolver resolver = new GoodDataSchemaResolver();
        List<Partition> partitions = resolver.getPartitions(null, new Table(1L, "tbl1",
                Table.TableType.JDBC, Lists.newArrayList()));
        Assertions.assertEquals(1, partitions.size());
        Assertions.assertEquals("tbl1", partitions.get(0).getPartitionName());
    }
}
