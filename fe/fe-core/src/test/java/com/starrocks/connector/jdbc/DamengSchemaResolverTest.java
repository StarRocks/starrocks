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
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.type.ScalarType;
import com.starrocks.type.Type;
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

public class DamengSchemaResolverTest {
    private static final String DAMENG_TIMESTAMPTZ_TO_DATETIME_KEY = "dameng.timestamptz.to-datetime";

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
    private MockResultSet partitionColumnsResult;

    // Column metadata observed on a real DM8 instance (driver 8.1.3.140)
    @BeforeEach
    public void setUp() throws SQLException {
        dbResult = new MockResultSet("catalog");
        dbResult.addColumn("TABLE_SCHEM",
                Arrays.asList("SRTEST", "SYS", "SYSAUDITOR", "SYSSSO", "CTISYS", "SYSDBA"));
        tableResult = new MockResultSet("tables");
        tableResult.addColumn("TABLE_NAME", Arrays.asList("tbl1", "tbl2", "tbl3"));

        // DM type codes from DatabaseMetaData#getColumns:
        // a TINYINT(-6) b SMALLINT(5) c INTEGER(4) d BIGINT(-5) e DECIMAL(10,2)
        // f DECIMAL without precision (size 0) g FLOAT(6) h DOUBLE(8) i REAL(7) j BIT(-7)
        // k CHAR(10) l VARCHAR(100) m TEXT(-1) n CLOB(2005) o BINARY(10)
        // p VARBINARY(100) q BLOB(2004) r DATE(91) s TIME(92) t TIMESTAMP(93)
        // u DATETIME WITH TIME ZONE(95) v TIME WITH TIME ZONE(94) w INTERVAL DAY TO SECOND(110)
        columnResult = new MockResultSet("columns");
        columnResult.addColumn("DATA_TYPE", Arrays.asList(-6, 5, 4, -5, 3, 3, 6, 8, 7, -7,
                Types.CHAR, Types.VARCHAR, -1, 2005, -2, -3, 2004, 91, 92, 93, 95, 94, 110));
        columnResult.addColumn("TYPE_NAME", Arrays.asList("TINYINT", "SMALLINT", "INTEGER", "BIGINT",
                "DECIMAL", "DECIMAL", "FLOAT", "DOUBLE", "REAL", "BIT",
                "CHAR", "VARCHAR", "TEXT", "CLOB", "BINARY", "VARBINARY", "BLOB",
                "DATE", "TIME", "TIMESTAMP", "DATETIME WITH TIME ZONE", "TIME WITH TIME ZONE",
                "INTERVAL DAY TO SECOND"));
        columnResult.addColumn("COLUMN_SIZE", Arrays.asList(3, 5, 10, 19, 10, 0, 53, 53, 24, 1,
                10, 100, 2147483647, 2147483647, 10, 100, 2147483647, 13, 15, 36, 43, 22, 25));
        columnResult.addColumn("DECIMAL_DIGITS", Arrays.asList(0, 0, 0, 0, 2, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 6, 6, 0, 6));
        columnResult.addColumn("COLUMN_NAME", Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i", "j",
                "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w"));
        columnResult.addColumn("IS_NULLABLE", Arrays.asList("NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO",
                "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "NO", "YES", "YES", "YES"));

        properties = new HashMap<>();
        properties.put(JDBCResource.DRIVER_CLASS, "dm.jdbc.driver.DmDriver");
        properties.put(JDBCResource.URI, "jdbc:dm://127.0.0.1:5236");
        properties.put(JDBCResource.USER, "SYSDBA");
        properties.put(JDBCResource.PASSWORD, "123456");
        properties.put(JDBCResource.CHECK_SUM, "xxxx");
        properties.put(JDBCResource.DRIVER_URL, "xxxx");

        partitionColumnsResult = new MockResultSet("partitionColumns");
        partitionColumnsResult.addColumn("OWNER", Arrays.asList("SRTEST"));
        partitionColumnsResult.addColumn("NAME", Arrays.asList("tbl1"));
        partitionColumnsResult.addColumn("COLUMN_NAME", Arrays.asList("DT"));
        partitionColumnsResult.addColumn("SEQ", Arrays.asList(1));

        partitionsResult = new MockResultSet("partitions");
        partitionsResult.addColumn("NAME", Arrays.asList("'20250101'"));
        partitionsResult.addColumn("MODIFIED_TIME", Arrays.asList("2025-08-01 00:00:00"));

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
    public void testListDatabaseNamesFiltersSystemSchemas() throws SQLException {
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
            List<String> expectResult = Lists.newArrayList("SRTEST", "SYSDBA");
            Assertions.assertEquals(expectResult, result);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
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
            Database db = jdbcMetadata.getDb(new ConnectContext(), "SRTEST");
            Assertions.assertEquals("SRTEST", db.getOriginName());
            Assertions.assertNull(jdbcMetadata.getDb(new ConnectContext(), "SYS"));
            Assertions.assertNull(jdbcMetadata.getDb(new ConnectContext(), "not_exist"));
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
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

                connection.getMetaData().getTables("t1", "SRTEST", null,
                        new String[] {"TABLE", "VIEW", "MATERIALIZED VIEW", "FOREIGN TABLE"});
                result = tableResult;
                minTimes = 0;
            }
        };
        try {
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog", dataSource);
            List<String> result = jdbcMetadata.listTableNames(new ConnectContext(), "SRTEST");
            List<String> expectResult = Lists.newArrayList("tbl1", "tbl2", "tbl3");
            Assertions.assertEquals(expectResult, result);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
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

                connection.getMetaData().getColumns("t1", "SRTEST", "tbl1", "%");
                result = columnResult;
                minTimes = 0;

                // first executeQuery is the partition-support probe run in the
                // JDBCMetadata constructor, later ones come from listPartitionColumns
                MockResultSet probeResult = new MockResultSet("probe");
                probeResult.addColumn("COLUMN_NAME", Arrays.asList("OWNER"));
                preparedStatement.executeQuery();
                result = probeResult;
                result = partitionColumnsResult;
                minTimes = 0;
            }
        };
        try {
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog", dataSource);
            Table table = jdbcMetadata.getTable(new ConnectContext(), "SRTEST", "tbl1");
            Assertions.assertTrue(table instanceof JDBCTable);
            Assertions.assertEquals("catalog.SRTEST.tbl1", table.getUUID());
            Assertions.assertEquals("tbl1", table.getName());
            Assertions.assertEquals("\"SRTEST\".\"tbl1\"", ((JDBCTable) table).getCatalogTableName());
            Assertions.assertTrue(table.getColumn("a").getType().isTinyint());
            Assertions.assertTrue(table.getColumn("b").getType().isSmallint());
            Assertions.assertTrue(table.getColumn("c").getType().isInt());
            Assertions.assertTrue(table.getColumn("d").getType().isBigint());
            Assertions.assertTrue(table.getColumn("e").getType().isDecimalV3());
            Assertions.assertTrue(table.getColumn("f").getType().isDecimalV3());
            Assertions.assertTrue(table.getColumn("g").getType().isDouble());
            Assertions.assertTrue(table.getColumn("h").getType().isDouble());
            Assertions.assertTrue(table.getColumn("i").getType().isFloat());
            Assertions.assertTrue(table.getColumn("j").getType().isBoolean());
            Assertions.assertTrue(table.getColumn("k").getType().isChar());
            Assertions.assertTrue(table.getColumn("l").getType().isStringType());
            Assertions.assertTrue(table.getColumn("m").getType().isStringType());
            Assertions.assertTrue(table.getColumn("n").getType().isStringType());
            Assertions.assertTrue(table.getColumn("o").getType().isBinaryType());
            Assertions.assertTrue(table.getColumn("p").getType().isBinaryType());
            Assertions.assertTrue(table.getColumn("q").getType().isBinaryType());
            Assertions.assertTrue(table.getColumn("r").getType().isDate());
            Assertions.assertTrue(table.getColumn("s").getType().isTime());
            Assertions.assertTrue(table.getColumn("t").getType().isDatetime());
            Assertions.assertTrue(table.getColumn("u").getType().isStringType());
            Assertions.assertTrue(table.getColumn("v").getType().isStringType());
            Assertions.assertTrue(table.getColumn("w").getType().isStringType());
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testGetTableWithTimestamptzSwitch() throws SQLException {
        new Expectations() {
            {
                dataSource.getConnection();
                result = connection;
                minTimes = 0;

                connection.getCatalog();
                result = "t1";
                minTimes = 0;

                connection.getMetaData().getColumns("t1", "SRTEST", "tbl1", "%");
                result = columnResult;
                minTimes = 0;

                MockResultSet probeResult = new MockResultSet("probe");
                probeResult.addColumn("COLUMN_NAME", Arrays.asList("OWNER"));
                preparedStatement.executeQuery();
                result = probeResult;
                result = partitionColumnsResult;
                minTimes = 0;
            }
        };

        Map<String, String> propertiesWithSwitch = new HashMap<>(properties);
        propertiesWithSwitch.put(DAMENG_TIMESTAMPTZ_TO_DATETIME_KEY, "true");
        try {
            JDBCMetadata jdbcMetadata = new JDBCMetadata(propertiesWithSwitch, "catalog", dataSource);
            Table table = jdbcMetadata.getTable(new ConnectContext(), "SRTEST", "tbl1");
            Assertions.assertTrue(table.getColumn("u").getType().isDatetime());
            // the switch only affects TIMESTAMP WITH TIME ZONE
            Assertions.assertTrue(table.getColumn("t").getType().isDatetime());
            Assertions.assertTrue(table.getColumn("r").getType().isDate());
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testDecimalPrecisionAndScale() {
        DamengSchemaResolver resolver = new DamengSchemaResolver();

        // DM reports decimal columns as Types.DECIMAL (3); Types.NUMERIC (2) is kept
        // for drivers that report it that way
        for (int code : new int[] {Types.DECIMAL, Types.NUMERIC}) {
            Assertions.assertTrue(resolver.convertColumnType(code, "DECIMAL", 10, 2).isDecimalV3(),
                    "type code " + code + " should map to a decimal type");
        }

        ScalarType defined = (ScalarType) resolver.convertColumnType(Types.DECIMAL, "DECIMAL", 10, 2);
        Assertions.assertEquals(10, defined.decimalPrecision());
        Assertions.assertEquals(2, defined.decimalScale());

        // DM reports precision 0 for NUMBER/DECIMAL declared without precision and scale
        ScalarType undefined = (ScalarType) resolver.convertColumnType(Types.NUMERIC, "DECIMAL", 0, 0);
        Assertions.assertEquals(38, undefined.decimalPrecision());
        Assertions.assertEquals(6, undefined.decimalScale());

        ScalarType oracleStyle = (ScalarType) resolver.convertColumnType(Types.NUMERIC, "NUMBER", 0, -127);
        Assertions.assertEquals(38, oracleStyle.decimalPrecision());
        Assertions.assertEquals(6, oracleStyle.decimalScale());

        ScalarType negativeScale = (ScalarType) resolver.convertColumnType(Types.NUMERIC, "NUMBER", 5, -2);
        Assertions.assertEquals(7, negativeScale.decimalPrecision());
        Assertions.assertEquals(0, negativeScale.decimalScale());

        ScalarType scaleLargerThanPrecision =
                (ScalarType) resolver.convertColumnType(Types.NUMERIC, "NUMBER", 1, 5);
        Assertions.assertEquals(5, scaleLargerThanPrecision.decimalPrecision());
        Assertions.assertEquals(5, scaleLargerThanPrecision.decimalScale());
    }

    @Test
    public void testInvalidNumberDefaultScaleFallsBackToSix() {
        Map<String, String> propertiesWithInvalidScale = new HashMap<>(properties);
        propertiesWithInvalidScale.put(DamengSchemaResolver.DAMENG_NUMBER_DEFAULT_SCALE, "invalid");
        DamengSchemaResolver resolver = new DamengSchemaResolver(propertiesWithInvalidScale);

        ScalarType type = (ScalarType) resolver.convertColumnType(Types.NUMERIC, "DECIMAL", 0, 0);
        Assertions.assertEquals(38, type.decimalPrecision());
        Assertions.assertEquals(6, type.decimalScale());
    }

    @Test
    public void testCustomNumberDefaultScale() {
        Map<String, String> propertiesWithCustomScale = new HashMap<>(properties);
        propertiesWithCustomScale.put(DamengSchemaResolver.DAMENG_NUMBER_DEFAULT_SCALE, "10");
        DamengSchemaResolver resolver = new DamengSchemaResolver(propertiesWithCustomScale);

        ScalarType type = (ScalarType) resolver.convertColumnType(Types.NUMERIC, "DECIMAL", 0, 0);
        Assertions.assertEquals(38, type.decimalPrecision());
        Assertions.assertEquals(10, type.decimalScale());
    }

    @Test
    public void testExceedingMaxPrecisionThrowsConnectorException() {
        DamengSchemaResolver resolver = new DamengSchemaResolver();
        StarRocksConnectorException ex = Assertions.assertThrows(StarRocksConnectorException.class,
                () -> resolver.convertColumnType(Types.NUMERIC, "DECIMAL", 39, 0));
        Assertions.assertTrue(ex.getMessage().contains(DamengSchemaResolver.DAMENG_NUMBER_DEFAULT_SCALE));
    }

    @Test
    public void testTimestampWithTimezoneTypeCodes() {
        DamengSchemaResolver defaultResolver = new DamengSchemaResolver();

        // 95 is what DM reports from DatabaseMetaData#getColumns for TIMESTAMP WITH TIME ZONE
        Assertions.assertTrue(defaultResolver.convertColumnType(95, "DATETIME WITH TIME ZONE", 43, 6)
                .isStringType());
        // 2014 (Types.TIMESTAMP_WITH_TIMEZONE) is what DM reports from ResultSetMetaData
        Assertions.assertTrue(defaultResolver.convertColumnType(Types.TIMESTAMP_WITH_TIMEZONE,
                "TIMESTAMP WITH TIME ZONE", 26, 6).isStringType());

        Map<String, String> propertiesWithSwitch = new HashMap<>(properties);
        propertiesWithSwitch.put(DAMENG_TIMESTAMPTZ_TO_DATETIME_KEY, "true");
        DamengSchemaResolver resolverWithSwitch = new DamengSchemaResolver(propertiesWithSwitch);
        Assertions.assertTrue(resolverWithSwitch.convertColumnType(95, "DATETIME WITH TIME ZONE", 43, 6)
                .isDatetime());
        Assertions.assertTrue(resolverWithSwitch.convertColumnType(Types.TIMESTAMP_WITH_TIMEZONE,
                "TIMESTAMP WITH TIME ZONE", 26, 6).isDatetime());
    }

    @Test
    public void testTypesWithoutStarRocksCounterpartMapToString() {
        DamengSchemaResolver resolver = new DamengSchemaResolver();

        // TIME WITH TIME ZONE: DM reports 94, the standard code is 2013
        Assertions.assertTrue(resolver.convertColumnType(94, "TIME WITH TIME ZONE", 22, 0).isStringType());
        Assertions.assertTrue(resolver.convertColumnType(Types.TIME_WITH_TIMEZONE,
                "TIME WITH TIME ZONE", 22, 0).isStringType());
        // INTERVAL DAY TO SECOND: 110 from getColumns, 1111 (Types.OTHER) from ResultSetMetaData
        Assertions.assertTrue(resolver.convertColumnType(110, "INTERVAL DAY TO SECOND", 25, 6).isStringType());
        Assertions.assertTrue(resolver.convertColumnType(Types.OTHER, "INTERVAL DAY TO SECOND", 25, 6)
                .isStringType());
    }

    @Test
    public void testHugeBinaryFallsBackToMaxVarbinary() {
        DamengSchemaResolver resolver = new DamengSchemaResolver();

        // DM reports COLUMN_SIZE = 2147483647 for BLOB
        Type blob = resolver.convertColumnType(Types.BLOB, "BLOB", 2147483647, 0);
        Assertions.assertTrue(blob.isBinaryType());
        Assertions.assertEquals(com.starrocks.type.TypeFactory.CATALOG_MAX_VARCHAR_LENGTH,
                ((ScalarType) blob).getLength());
    }

    @Test
    public void testListPartitionNames() throws SQLException {
        MockResultSet partitionNamesResult = new MockResultSet("partitionNames");
        partitionNamesResult.addColumn("PARTITION_NAME", Arrays.asList("P3", "P1", "P2"));
        new Expectations() {
            {
                connection.prepareStatement(anyString);
                result = preparedStatement;
                minTimes = 0;

                preparedStatement.executeQuery();
                result = partitionNamesResult;
                minTimes = 0;
            }
        };
        DamengSchemaResolver resolver = new DamengSchemaResolver();
        List<String> result = resolver.listPartitionNames(connection, "SRTEST", "PART_T");
        Assertions.assertEquals(Lists.newArrayList("P3", "P1", "P2"), result);
    }

    @Test
    public void testListPartitionColumnsReadsPositionally() throws SQLException {
        // DM's ALL_PART_KEY_COLUMNS reports OWNER/table/column all under the column label "NAME";
        // the resolver reads the column name by position (3rd column).
        MockResultSet partitionColumnsResult = new MockResultSet("partitionColumns");
        partitionColumnsResult.addColumn("OWNER", Arrays.asList("SRTEST"));
        partitionColumnsResult.addColumn("NAME", Arrays.asList("PART_T"));
        partitionColumnsResult.addColumn("NAME2", Arrays.asList("DT"));
        partitionColumnsResult.addColumn("SEQ", Arrays.asList(1));
        new Expectations() {
            {
                connection.prepareStatement(anyString);
                result = preparedStatement;
                minTimes = 0;

                preparedStatement.executeQuery();
                result = partitionColumnsResult;
                minTimes = 0;
            }
        };
        DamengSchemaResolver resolver = new DamengSchemaResolver();
        List<String> result = resolver.listPartitionColumns(connection, "srtest", "part_t");
        Assertions.assertEquals(Lists.newArrayList("DT"), result);
    }

    @Test
    public void testGetPartitionsForPartitionedTable() throws SQLException, com.starrocks.common.DdlException {
        Map<String, String> tableProperties = new HashMap<>(properties);
        List<Column> partitionColumns = Lists.newArrayList(
                new Column("DT", com.starrocks.type.TypeFactory.createType(com.starrocks.type.PrimitiveType.DATE), true, null));
        DamengSchemaResolver resolver = new DamengSchemaResolver();
        Table table = resolver.getTable(1, "PART_T", Lists.newArrayList(
                new Column("ID", com.starrocks.type.TypeFactory.createType(com.starrocks.type.PrimitiveType.INT), true, null)),
                partitionColumns, "SRTEST", "catalog", tableProperties);
        Assertions.assertFalse(table.isUnPartitioned());

        MockResultSet partitionsResult2 = new MockResultSet("partitions2");
        partitionsResult2.addColumn("PARTITION_NAME", Arrays.asList("P1", "P2"));
        partitionsResult2.addColumn("LAST_ANALYZED", Arrays.asList("2025-08-01 00:00:00", "2025-08-02 00:00:00"));
        new Expectations() {
            {
                connection.prepareStatement(anyString);
                result = preparedStatement;
                minTimes = 0;

                preparedStatement.executeQuery();
                result = partitionsResult2;
                minTimes = 0;
            }
        };
        List<Partition> partitions = resolver.getPartitions(connection, table);
        Assertions.assertEquals(2, partitions.size());
        Assertions.assertEquals("P1", partitions.get(0).getPartitionName());
        Assertions.assertEquals("P2", partitions.get(1).getPartitionName());
    }

    @Test
    public void testGetPartitionsForUnpartitionedTable() throws SQLException, com.starrocks.common.DdlException {
        DamengSchemaResolver resolver = new DamengSchemaResolver();
        Table table = resolver.getTable(1, "TBL1", Lists.newArrayList(
                new Column("ID", com.starrocks.type.TypeFactory.createType(com.starrocks.type.PrimitiveType.INT), true, null)),
                "SRTEST", "catalog", new HashMap<>(properties));
        Assertions.assertTrue(table.isUnPartitioned());

        MockResultSet emptyResult = new MockResultSet("nonPartitions");
        new Expectations() {
            {
                connection.prepareStatement(anyString);
                result = preparedStatement;
                minTimes = 0;

                preparedStatement.executeQuery();
                result = emptyResult;
                minTimes = 0;
            }
        };
        List<Partition> partitions = resolver.getPartitions(connection, table);
        Assertions.assertEquals(1, partitions.size());
        Assertions.assertEquals("TBL1", partitions.get(0).getPartitionName());
    }

    @Test
    public void testGetTableRowCount() throws SQLException {
        MockResultSet rowCountResult = new MockResultSet("rowCount");
        rowCountResult.addColumn("NUM_ROWS", Arrays.asList((Long) 42L));
        new Expectations() {
            {
                connection.prepareStatement(anyString);
                result = preparedStatement;
                minTimes = 0;

                preparedStatement.executeQuery();
                result = rowCountResult;
                minTimes = 0;
            }
        };
        DamengSchemaResolver resolver = new DamengSchemaResolver();
        Assertions.assertEquals(42L, resolver.getTableRowCount(connection, "SRTEST", "TBL1"));
    }

    @Test
    public void testGetTableRowCountReturnsMinusOneWhenNull() throws SQLException {
        MockResultSet rowCountResult = new MockResultSet("rowCountNull");
        rowCountResult.addColumn("NUM_ROWS", Arrays.asList((Object) null));
        new Expectations() {
            {
                connection.prepareStatement(anyString);
                result = preparedStatement;
                minTimes = 0;

                preparedStatement.executeQuery();
                result = rowCountResult;
                minTimes = 0;
            }
        };
        DamengSchemaResolver resolver = new DamengSchemaResolver();
        Assertions.assertEquals(-1L, resolver.getTableRowCount(connection, "SRTEST", "TBL1"));
    }

    @Test
    public void testCheckAndSetSupportPartitionInformation() throws SQLException {
        MockResultSet probeResult = new MockResultSet("probe");
        probeResult.addColumn("COLUMN_NAME", Arrays.asList("OWNER"));
        new Expectations() {
            {
                connection.prepareStatement(anyString);
                result = preparedStatement;
                minTimes = 0;

                preparedStatement.executeQuery();
                result = probeResult;
                minTimes = 0;
            }
        };
        DamengSchemaResolver resolver = new DamengSchemaResolver();
        Assertions.assertTrue(resolver.checkAndSetSupportPartitionInformation(connection));
        Assertions.assertTrue(resolver.isSupportPartitionInformation());
    }

    @Test
    public void testCheckAndSetSupportPartitionInformationFallsBackWhenViewMissing() throws SQLException {
        new Expectations() {
            {
                connection.prepareStatement(anyString);
                result = new SQLException("invalid table or view name");
                minTimes = 0;
            }
        };
        DamengSchemaResolver resolver = new DamengSchemaResolver();
        Assertions.assertFalse(resolver.checkAndSetSupportPartitionInformation(connection));
        Assertions.assertFalse(resolver.isSupportPartitionInformation());
    }

    @Test
    public void testResolverAutoDetectedFromDriverClass() {
        try {
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "catalog", dataSource);
            Assertions.assertTrue(jdbcMetadata.schemaResolver instanceof DamengSchemaResolver,
                    "dm.jdbc.driver.DmDriver should auto-detect DamengSchemaResolver");
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testResolverSelectedFromProperty() {
        for (String resolverType : new String[] {"dameng", "dm", "DAMENG"}) {
            try {
                Map<String, String> testProperties = new HashMap<>(properties);
                testProperties.put(JDBCResource.SCHEMA_RESOLVER, resolverType);
                JDBCMetadata jdbcMetadata = new JDBCMetadata(testProperties, "catalog", dataSource);
                Assertions.assertTrue(jdbcMetadata.schemaResolver instanceof DamengSchemaResolver,
                        resolverType + " should create DamengSchemaResolver");
            } catch (Exception e) {
                Assertions.fail(e.getMessage());
            }
        }
    }

    @Test
    public void testUnknownResolverTypeReportsDamengAsSupported() {
        Map<String, String> testProperties = new HashMap<>(properties);
        testProperties.put(JDBCResource.SCHEMA_RESOLVER, "not_a_database");
        StarRocksConnectorException ex = Assertions.assertThrows(StarRocksConnectorException.class,
                () -> new JDBCMetadata(testProperties, "catalog", dataSource));
        Assertions.assertTrue(ex.getMessage().contains("dameng"));
    }
}
