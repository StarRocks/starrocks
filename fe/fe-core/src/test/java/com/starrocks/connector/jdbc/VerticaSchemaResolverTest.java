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
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
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

public class VerticaSchemaResolverTest {
    @Mocked
    HikariDataSource dataSource;

    @Mocked
    Connection connection;

    private Map<String, String> properties;
    private MockResultSet dbResult;
    private MockResultSet tableResult;
    private MockResultSet columnResult;

    @BeforeEach
    public void setUp() throws SQLException {
        // Include both user schemas and internal schemas to test filtering
        dbResult = new MockResultSet("catalog");
        dbResult.addColumn("TABLE_SCHEM", Arrays.asList(
                "public", "myschema", "v_catalog", "v_monitor", "v_internal", "v_internal_tables", "v_txtindex"));
        tableResult = new MockResultSet("tables");
        tableResult.addColumn("TABLE_NAME", Arrays.asList("tbl1", "tbl2", "tbl3"));
        columnResult = new MockResultSet("columns");
        // Test Vertica-specific types
        columnResult.addColumn("DATA_TYPE", Arrays.asList(
                Types.BIGINT,      // int/integer maps to BIGINT (Vertica INT is 64-bit)
                Types.BIGINT,      // bigint
                Types.SMALLINT,    // smallint
                Types.DOUBLE,      // double precision
                Types.NUMERIC,     // numeric/decimal
                Types.VARCHAR,     // varchar
                Types.BOOLEAN,     // boolean
                Types.DATE,        // date
                Types.TIMESTAMP,   // timestamp
                Types.TIMESTAMP,   // timestamptz
                Types.BINARY,      // varbinary
                Types.VARCHAR      // uuid
        ));
        columnResult.addColumn("TYPE_NAME", Arrays.asList(
                "int",
                "bigint",
                "smallint",
                "double precision",
                "numeric",
                "varchar",
                "boolean",
                "date",
                "timestamp",
                "timestamptz",
                "varbinary",
                "uuid"
        ));
        columnResult.addColumn("COLUMN_SIZE", Arrays.asList(19, 19, 5, 17, 18, 100, 1, 13, 29, 29, 100, 36));
        columnResult.addColumn("DECIMAL_DIGITS", Arrays.asList(0, 0, 0, 17, 2, 0, 0, 0, 6, 6, 0, 0));
        columnResult.addColumn("COLUMN_NAME", Arrays.asList(
                "int_col", "bigint_col", "smallint_col", "double_col", "numeric_col",
                "varchar_col", "bool_col", "date_col", "ts_col", "tstz_col", "binary_col", "uuid_col"));
        columnResult.addColumn("IS_NULLABLE", Arrays.asList(
                "YES", "NO", "NO", "NO", "NO", "YES", "NO", "NO", "NO", "NO", "YES", "NO"));
        properties = new HashMap<>();
        properties.put(JDBCResource.DRIVER_CLASS, "com.vertica.jdbc.Driver");
        properties.put(JDBCResource.URI, "jdbc:vertica://127.0.0.1:5433/testdb");
        properties.put(JDBCResource.USER, "dbadmin");
        properties.put(JDBCResource.PASSWORD, "password");
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
            // Should only contain user schemas, not internal schemas
            Assertions.assertTrue(result.contains("public"));
            Assertions.assertTrue(result.contains("myschema"));
            Assertions.assertFalse(result.contains("v_catalog"));
            Assertions.assertFalse(result.contains("v_monitor"));
            Assertions.assertFalse(result.contains("v_internal"));
            Assertions.assertFalse(result.contains("v_internal_tables"));
            Assertions.assertFalse(result.contains("v_txtindex"));
            Assertions.assertEquals(2, result.size());
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
            Database db = jdbcMetadata.getDb(new ConnectContext(), "public");
            Assertions.assertEquals("public", db.getOriginName());
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
                result = "testdb";
                minTimes = 0;

                connection.getMetaData().getTables("testdb", "public", null,
                        new String[] {"TABLE", "VIEW", "SYSTEM TABLE", "FLEX TABLE"});
                result = tableResult;
                minTimes = 0;
            }
        };
        try {
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog", dataSource);
            List<String> result = jdbcMetadata.listTableNames(new ConnectContext(), "public");
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
                result = "testdb";
                minTimes = 0;

                connection.getMetaData().getColumns("testdb", "public", "tbl1", "%");
                result = columnResult;
                minTimes = 0;
            }
        };
        try {
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog", dataSource);
            Table table = jdbcMetadata.getTable(new ConnectContext(), "public", "tbl1");
            Assertions.assertTrue(table instanceof JDBCTable);
            Assertions.assertEquals("catalog.public.tbl1", table.getUUID());
            Assertions.assertEquals("tbl1", table.getName());
            Assertions.assertEquals(12, table.getColumns().size());
            
            // Verify Vertica INT maps to BIGINT (64-bit)
            Assertions.assertEquals(PrimitiveType.BIGINT, table.getColumn("int_col").getType().getPrimitiveType());
            Assertions.assertEquals(PrimitiveType.BIGINT, table.getColumn("bigint_col").getType().getPrimitiveType());
            Assertions.assertEquals(PrimitiveType.SMALLINT, table.getColumn("smallint_col").getType().getPrimitiveType());
            Assertions.assertEquals(PrimitiveType.DOUBLE, table.getColumn("double_col").getType().getPrimitiveType());
            Assertions.assertTrue(table.getColumn("numeric_col").getType().isDecimalOfAnyVersion());
            Assertions.assertTrue(table.getColumn("varchar_col").getType().isStringType());
            Assertions.assertEquals(PrimitiveType.BOOLEAN, table.getColumn("bool_col").getType().getPrimitiveType());
            Assertions.assertEquals(PrimitiveType.DATE, table.getColumn("date_col").getType().getPrimitiveType());
            Assertions.assertEquals(PrimitiveType.DATETIME, table.getColumn("ts_col").getType().getPrimitiveType());
            Assertions.assertEquals(PrimitiveType.DATETIME, table.getColumn("tstz_col").getType().getPrimitiveType());
            Assertions.assertTrue(table.getColumn("binary_col").getType().isBinaryType());
            // UUID maps to VARCHAR(36)
            Assertions.assertTrue(table.getColumn("uuid_col").getType().isStringType());
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assertions.fail();
        }
    }

    @Test
    public void testConvertColumnTypeVerticaSpecific() {
        VerticaSchemaResolver resolver = new VerticaSchemaResolver();
        
        // Test Vertica INT is 64-bit (maps to BIGINT)
        Type intType = resolver.convertColumnType(Types.INTEGER, "int", 10, 0);
        Assertions.assertEquals(PrimitiveType.BIGINT, intType.getPrimitiveType());
        
        Type integerType = resolver.convertColumnType(Types.INTEGER, "integer", 10, 0);
        Assertions.assertEquals(PrimitiveType.BIGINT, integerType.getPrimitiveType());
        
        // Test timestamptz maps to DATETIME
        Type tstzType = resolver.convertColumnType(Types.TIMESTAMP, "timestamptz", 29, 6);
        Assertions.assertEquals(PrimitiveType.DATETIME, tstzType.getPrimitiveType());
        
        // Test interval maps to VARCHAR (no native interval type in StarRocks)
        Type intervalType = resolver.convertColumnType(Types.OTHER, "interval", 0, 0);
        Assertions.assertTrue(intervalType.isStringType());
        
        Type intervalDayType = resolver.convertColumnType(Types.OTHER, "interval day to second", 0, 0);
        Assertions.assertTrue(intervalDayType.isStringType());
        
        // Test bytea maps to VARBINARY
        Type byteaType = resolver.convertColumnType(Types.BINARY, "bytea", 100, 0);
        Assertions.assertTrue(byteaType.isBinaryType());
        
        // Test raw maps to VARBINARY
        Type rawType = resolver.convertColumnType(Types.BINARY, "raw", 100, 0);
        Assertions.assertTrue(rawType.isBinaryType());
        
        // Test long varchar maps to VARCHAR with max length
        Type longVarcharType = resolver.convertColumnType(Types.LONGVARCHAR, "long varchar", 0, 0);
        Assertions.assertTrue(longVarcharType.isStringType());
        Assertions.assertEquals(ScalarType.getOlapMaxVarcharLength(), 
                ((ScalarType) longVarcharType).getLength());
        
        // Test UUID maps to VARCHAR(36)
        Type uuidType = resolver.convertColumnType(Types.VARCHAR, "uuid", 36, 0);
        Assertions.assertTrue(uuidType.isStringType());
        Assertions.assertEquals(36, ((ScalarType) uuidType).getLength());
        
        // Test decimal with precision
        Type decimalType = resolver.convertColumnType(Types.NUMERIC, "numeric", 18, 2);
        Assertions.assertTrue(decimalType.isDecimalOfAnyVersion());
        
        // Test money type maps to decimal
        Type moneyType = resolver.convertColumnType(Types.NUMERIC, "money", 18, 2);
        Assertions.assertTrue(moneyType.isDecimalOfAnyVersion());
        
        // Test double precision
        Type doubleType = resolver.convertColumnType(Types.DOUBLE, "double precision", 17, 17);
        Assertions.assertEquals(PrimitiveType.DOUBLE, doubleType.getPrimitiveType());
        
        // Test float8 (alias for double)
        Type float8Type = resolver.convertColumnType(Types.DOUBLE, "float8", 17, 17);
        Assertions.assertEquals(PrimitiveType.DOUBLE, float8Type.getPrimitiveType());
    }

    @Test
    public void testInternalSchemaFiltering() throws SQLException {
        // Create a result set with only internal schemas
        MockResultSet internalOnlyResult = new MockResultSet("catalog");
        internalOnlyResult.addColumn("TABLE_SCHEM", Arrays.asList(
                "v_catalog", "v_monitor", "v_internal", "v_internal_tables"));
        
        new Expectations() {
            {
                dataSource.getConnection();
                result = connection;
                minTimes = 0;

                connection.getMetaData().getSchemas();
                result = internalOnlyResult;
                minTimes = 0;
            }
        };
        try {
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog", dataSource);
            List<String> result = jdbcMetadata.listDbNames(new ConnectContext());
            // All internal schemas should be filtered out
            Assertions.assertEquals(0, result.size());
        } catch (Exception e) {
            Assertions.fail();
        }
    }

    @Test
    public void testGetPartitions() {
        VerticaSchemaResolver verticaSchemaResolver = new VerticaSchemaResolver();
        List<Partition> partitions = verticaSchemaResolver.getPartitions(null, new Table(1L, "tbl1",
                Table.TableType.JDBC, Lists.newArrayList()));
        Assertions.assertEquals(1, partitions.size());
        Assertions.assertEquals("tbl1", partitions.get(0).getPartitionName());
    }

    @Test
    public void testTableNameQuoting() throws Exception {
        VerticaSchemaResolver resolver = new VerticaSchemaResolver();
        Table table = resolver.getTable(1L, "test_table", Lists.newArrayList(), "public", "catalog", properties);
        Assertions.assertTrue(table instanceof JDBCTable);
        JDBCTable jdbcTable = (JDBCTable) table;
        // Verify that the table name is quoted with double quotes (PostgreSQL style)
        Assertions.assertEquals("\"public\".\"test_table\"", jdbcTable.getCatalogTableName());
    }
}
