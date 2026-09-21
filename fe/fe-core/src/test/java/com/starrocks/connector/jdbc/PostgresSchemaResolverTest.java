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
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.TypeFactory;
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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class PostgresSchemaResolverTest {
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

    @BeforeEach
    public void setUp() throws SQLException {
        dbResult = new MockResultSet("catalog");
        dbResult.addColumn("TABLE_SCHEM", Arrays.asList("postgres", "template1", "test"));
        tableResult = new MockResultSet("tables");
        tableResult.addColumn("TABLE_NAME", Arrays.asList("tbl1", "tbl2", "tbl3"));
        columnResult = new MockResultSet("columns");
        columnResult.addColumn("DATA_TYPE", Arrays.asList(Types.BIT, Types.INTEGER, Types.INTEGER, Types.REAL, Types.DOUBLE,
                Types.NUMERIC, Types.CHAR, Types.VARCHAR, Types.VARCHAR, Types.DATE, Types.TIMESTAMP, Types.VARBINARY,
                Types.TIME, Types.TIME_WITH_TIMEZONE, Types.OTHER, Types.OTHER));
        columnResult.addColumn("TYPE_NAME", Arrays.asList("BOOL", "INTEGER", "SERIAL", "FLOAT4", "FLOAT8",
                "NUMERIC", "CHAR", "VARCHAR", "TEXT", "DATE", "TIMESTAMP", "UUID",
                "TIME", "TIMETZ", "JSON", "JSONB"));
        columnResult.addColumn("COLUMN_SIZE", Arrays.asList(1, 10, 10, 8, 17, 10, 10, 10, 2147483647, 13, 29, 36,
                15, 21, 2147483647, 2147483647));
        columnResult.addColumn("DECIMAL_DIGITS", Arrays.asList(0, 0, 0, 8, 17, 2, 0, 0, 0, 0, 6, 0,
                0, 0, 0, 0));
        columnResult.addColumn("COLUMN_NAME", Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l",
                "m", "n", "o", "p"));
        columnResult.addColumn("IS_NULLABLE", Arrays.asList("YES", "NO", "NO", "NO", "NO", "NO", "NO", "YES", "NO", "NO",
                "NO", "NO", "YES", "YES", "YES", "YES"));
        columnResult.addColumn("REMARKS", Arrays.asList("comment-a", null, null, null, null, null, null, null, null, null,
                null, null));
        properties = new HashMap<>();
        properties.put(JDBCResource.DRIVER_CLASS, "org.postgresql.Driver");
        properties.put(JDBCResource.URI, "jdbc:postgresql://127.0.0.1:5432/t1");
        properties.put(JDBCResource.USER, "root");
        properties.put(JDBCResource.PASSWORD, "123456");
        properties.put(JDBCResource.CHECK_SUM, "xxxx");
        properties.put(JDBCResource.DRIVER_URL, "xxxx");
    }

    @Test
    public void testUnboundedNumericMapping() {
        PostgresSchemaResolver resolver = new PostgresSchemaResolver();
        for (int jdbcType : new int[] {Types.NUMERIC, Types.DECIMAL}) {
            Assertions.assertEquals(TypeFactory.createUnifiedDecimalType(38, 18),
                    resolver.convertColumnType(jdbcType, "numeric", 0, 0));
        }
        Assertions.assertEquals(TypeFactory.createUnifiedDecimalType(12, 2),
                resolver.convertColumnType(Types.NUMERIC, "numeric", 12, 2));
        Assertions.assertEquals(TypeFactory.createUnifiedDecimalType(38, 18),
                resolver.convertColumnType(Types.NUMERIC, "numeric", 38, 18));
    }

    @Test
    public void testUnboundedNumericAlwaysUsesDecimal128() {
        boolean original = Config.enable_decimal_v3;
        try {
            Config.enable_decimal_v3 = false;
            Assertions.assertEquals(TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 18),
                    new PostgresSchemaResolver().convertColumnType(Types.NUMERIC, "numeric", 0, 0));
        } finally {
            Config.enable_decimal_v3 = original;
        }
    }

    @Test
    public void testOnlyUnboundedColumnIsMarked() throws SQLException {
        MockResultSet resultSet = new MockResultSet("numeric");
        resultSet.addColumn("DATA_TYPE", List.of(Types.NUMERIC, Types.NUMERIC, Types.VARCHAR));
        resultSet.addColumn("TYPE_NAME", List.of("numeric", "numeric", "text"));
        resultSet.addColumn("COLUMN_SIZE", List.of(0, 38, 0));
        resultSet.addColumn("DECIMAL_DIGITS", List.of(0, 18, 0));
        resultSet.addColumn("COLUMN_NAME", List.of("Amount", "bounded", "str"));
        resultSet.addColumn("IS_NULLABLE", List.of("YES", "YES", "YES"));
        Set<String> strict = new HashSet<>();
        new PostgresSchemaResolver().convertToSRTable(resultSet, new HashMap<>(), null, strict);
        Assertions.assertEquals(Set.of("\"Amount\""), strict);
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
            List<String> expectResult = Lists.newArrayList("postgres", "template1", "test");
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
            Assertions.assertEquals(16, table.getColumns().size());
            Assertions.assertTrue(table.getColumn("h").getType().isStringType());
            Assertions.assertTrue(table.getColumn("l").getType().isBinaryType());
            Assertions.assertEquals("comment-a", table.getColumn("a").getComment());
            Assertions.assertEquals("", table.getColumn("b").getComment());
            Assertions.assertTrue(table.getColumn("m").getType().isTime());
            Assertions.assertTrue(table.getColumn("n").getType().isTime());
            Assertions.assertTrue(table.getColumn("o").getType().isJsonType());
            Assertions.assertTrue(table.getColumn("p").getType().isJsonType());
            Assertions.assertEquals("TIMESTAMP", ((JDBCTable) table).getOriginalJdbcColumnTypeNames().get("k"));
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assertions.fail();
        }
    }

    @Test
    public void testGetPartitions() {
        PostgresSchemaResolver postgresSchemaResolver = new PostgresSchemaResolver();
        List<Partition> partitions = postgresSchemaResolver.getPartitions(null, new Table(1L, "tbl1",
                Table.TableType.JDBC, Lists.newArrayList()));
        Assertions.assertEquals(partitions.size(), 1);
        Assertions.assertEquals(partitions.get(0).getPartitionName(), "tbl1");
    }

    @Test
    public void testPreserveTimestampTypeNamesAndQuotedColumnNames() throws SQLException {
        MockResultSet columns = new MockResultSet("temporal_columns");
        columns.addColumn("COLUMN_NAME", List.of("createdAt", "createdat"));
        columns.addColumn("DATA_TYPE", List.of(Types.TIMESTAMP, Types.TIMESTAMP));
        columns.addColumn("TYPE_NAME", List.of("timestamp", "timestamptz"));
        columns.addColumn("COLUMN_SIZE", List.of(29, 35));
        columns.addColumn("DECIMAL_DIGITS", List.of(6, 6));
        columns.addColumn("IS_NULLABLE", List.of("YES", "YES"));
        Map<String, String> typeNames = new HashMap<>();
        List<Column> schema = new PostgresSchemaResolver().convertToSRTable(
                columns, new HashMap<>(), typeNames, null);
        Assertions.assertEquals(Map.of("\"createdAt\"", "timestamp", "createdat", "timestamptz"), typeNames);
        Assertions.assertEquals("\"createdAt\"", schema.get(0).getName());
        Assertions.assertEquals("createdat", schema.get(1).getName());
        Assertions.assertTrue(schema.stream().allMatch(column -> column.getType().isDatetime()));
    }

    @Test
    public void testConvertOtherTypeForTimeAndJson() {
        PostgresSchemaResolver postgresSchemaResolver = new PostgresSchemaResolver();
        Assertions.assertTrue(postgresSchemaResolver.convertColumnType(Types.OTHER, "time", 0, 0).isTime());
        Assertions.assertTrue(
                postgresSchemaResolver.convertColumnType(Types.OTHER, "time without time zone", 0, 0).isTime());
        Assertions.assertTrue(
                postgresSchemaResolver.convertColumnType(Types.OTHER, "timetz", 0, 0).isTime());
        Assertions.assertTrue(
                postgresSchemaResolver.convertColumnType(Types.OTHER, "time with time zone", 0, 0).isTime());
        Assertions.assertTrue(
                postgresSchemaResolver.convertColumnType(Types.OTHER, "timestamptz", 0, 0).isDatetime());
        Assertions.assertTrue(
                postgresSchemaResolver.convertColumnType(Types.OTHER, "timestamp with time zone", 0, 0).isDatetime());
        Assertions.assertTrue(postgresSchemaResolver.convertColumnType(Types.OTHER, "json", 0, 0).isJsonType());
        Assertions.assertTrue(postgresSchemaResolver.convertColumnType(Types.OTHER, "jsonb", 0, 0).isJsonType());
    }

    @Test
    public void testConvertWithTimezoneTypeName() {
        PostgresSchemaResolver postgresSchemaResolver = new PostgresSchemaResolver();
        Assertions.assertTrue(postgresSchemaResolver.convertColumnType(Types.TIME, "time", 0, 0).isTime());
        Assertions.assertTrue(postgresSchemaResolver.convertColumnType(Types.TIME, "timetz", 0, 0).isTime());
        Assertions.assertTrue(postgresSchemaResolver.convertColumnType(
                Types.TIME, "time with time zone", 0, 0).isTime());

        Assertions.assertTrue(postgresSchemaResolver.convertColumnType(
                Types.TIMESTAMP, "timestamp", 0, 0).isDatetime());
        Assertions.assertTrue(postgresSchemaResolver.convertColumnType(
                Types.TIMESTAMP, "timestamptz", 0, 0).isDatetime());
        Assertions.assertTrue(postgresSchemaResolver.convertColumnType(
                Types.TIMESTAMP, "timestamp with time zone", 0, 0).isDatetime());
        Assertions.assertTrue(postgresSchemaResolver.convertColumnType(
                Types.TIMESTAMP_WITH_TIMEZONE, "timestamp with time zone", 0, 0).isDatetime());
    }

    // -------------------------------------------------------------------------
    // getTableRowCount tests
    // -------------------------------------------------------------------------

    @Test
    public void testGetTableRowCountReturnsCount() throws SQLException {
        PostgresSchemaResolver resolver = new PostgresSchemaResolver();
        MockResultSet rs = new MockResultSet("row_count");
        rs.addColumn("reltuples", Arrays.asList(5_000_000L));

        new Expectations() {
            {
                connection.prepareStatement(anyString);
                result = preparedStatement;
                minTimes = 1;

                preparedStatement.executeQuery();
                result = rs;
                minTimes = 1;
            }
        };

        long count = resolver.getTableRowCount(connection, "public", "orders");
        Assertions.assertEquals(5_000_000L, count);
    }

    @Test
    public void testGetTableRowCountReturnsNegativeOneWhenEmpty() throws SQLException {
        PostgresSchemaResolver resolver = new PostgresSchemaResolver();
        MockResultSet rs = new MockResultSet("row_count");
        rs.addColumn("reltuples", Arrays.asList());

        new Expectations() {
            {
                connection.prepareStatement(anyString);
                result = preparedStatement;
                minTimes = 1;

                preparedStatement.executeQuery();
                result = rs;
                minTimes = 1;
            }
        };

        long count = resolver.getTableRowCount(connection, "public", "orders");
        Assertions.assertEquals(-1L, count, "Should return -1 when table is not found in pg_class");
    }

    @Test
    public void testGetTableRowCountReturnsNegativeOneWhenNull() throws SQLException {
        PostgresSchemaResolver resolver = new PostgresSchemaResolver();
        MockResultSet rs = new MockResultSet("row_count");
        rs.addColumn("reltuples", Arrays.asList((Object) null));

        new Expectations() {
            {
                connection.prepareStatement(anyString);
                result = preparedStatement;
                minTimes = 1;

                preparedStatement.executeQuery();
                result = rs;
                minTimes = 1;
            }
        };

        long count = resolver.getTableRowCount(connection, "public", "orders");
        Assertions.assertEquals(-1L, count, "Should return -1 when reltuples is NULL");
    }
    @Test
    public void testPostgresArrayElementMapping() {
        PostgresSchemaResolver resolver = new PostgresSchemaResolver();
        Map<String, com.starrocks.type.PrimitiveType> supported = new LinkedHashMap<>();
        supported.put("_bool", com.starrocks.type.PrimitiveType.BOOLEAN);
        supported.put("_int2", com.starrocks.type.PrimitiveType.SMALLINT);
        supported.put("_int4", com.starrocks.type.PrimitiveType.INT);
        supported.put("_int8", com.starrocks.type.PrimitiveType.BIGINT);
        supported.put("_float4", com.starrocks.type.PrimitiveType.FLOAT);
        supported.put("_float8", com.starrocks.type.PrimitiveType.DOUBLE);
        supported.put("_date", com.starrocks.type.PrimitiveType.DATE);
        supported.put("_timestamp", com.starrocks.type.PrimitiveType.DATETIME);
        // char(n)[] maps to ARRAY<VARCHAR>, not ARRAY<CHAR>: the BE array writer has no CHAR branch.
        supported.put("_bpchar", com.starrocks.type.PrimitiveType.VARCHAR);
        supported.put("_text", com.starrocks.type.PrimitiveType.VARCHAR);
        supported.put("_varchar", com.starrocks.type.PrimitiveType.VARCHAR);

        for (Map.Entry<String, com.starrocks.type.PrimitiveType> entry : supported.entrySet()) {
            for (String typeName : List.of(entry.getKey(), entry.getKey().toUpperCase(Locale.ROOT))) {
                com.starrocks.type.Type type = resolver.convertColumnType(Types.ARRAY, typeName, 0, 0);
                Assertions.assertTrue(type.isArrayType(), typeName);
                Assertions.assertEquals(entry.getValue(),
                        ((com.starrocks.type.ArrayType) type).getItemType().getPrimitiveType(), typeName);
            }
        }
    }

    @Test
    public void testPostgresArrayTimestampWithTimezoneStaysUnknown() {
        PostgresSchemaResolver resolver = new PostgresSchemaResolver();
        // The driver returns java.sql.Timestamp[] for _timestamptz exactly as it does for
        // _timestamp, so this name is the only thing that separates them. Reading one into
        // DATETIME collapses the two instants of a daylight-saving fall-back onto one wall
        // clock, which no later stage can undo -- it has to stay unmapped here.
        for (String typeName : List.of("_timestamptz", "_TIMESTAMPTZ")) {
            Assertions.assertEquals(com.starrocks.type.PrimitiveType.UNKNOWN_TYPE,
                    resolver.convertColumnType(Types.ARRAY, typeName, 29, 6).getPrimitiveType(), typeName);
        }
    }

    @Test
    public void testPostgresUnsupportedArrayElementsStayUnknown() {
        PostgresSchemaResolver resolver = new PostgresSchemaResolver();
        for (String typeName : Arrays.asList("_numeric", "_uuid", "_time", "_timetz", "_bytea", "_json",
                "_jsonb", "_int4range", "text", "text[]", "_", null)) {
            Assertions.assertEquals(com.starrocks.type.PrimitiveType.UNKNOWN_TYPE,
                    resolver.convertColumnType(Types.ARRAY, typeName, 10, 2).getPrimitiveType(), typeName);
        }
    }
}
