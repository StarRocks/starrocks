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
import com.starrocks.catalog.JDBCResource;
import com.starrocks.catalog.JDBCTable;
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

public class BigQuerySchemaResolverTest {

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
        dbResult = new MockResultSet("catalog");
        dbResult.addColumn("TABLE_SCHEM", Arrays.asList("my_dataset", "another_dataset"));

        tableResult = new MockResultSet("tables");
        tableResult.addColumn("TABLE_NAME", Arrays.asList("orders", "customers", "events"));

        columnResult = new MockResultSet("columns");
        //                                        0             1             2             3             4
        columnResult.addColumn("DATA_TYPE", Arrays.asList(
                Types.BIGINT,    // INT64
                Types.DOUBLE,    // FLOAT64
                Types.BOOLEAN,   // BOOL (some driver versions return BOOLEAN)
                Types.BIT,       // BOOL (Simba driver may return BIT)
                Types.NUMERIC,   // NUMERIC(38,9)
                Types.NUMERIC,   // BIGNUMERIC (precision > 38, use typeName to detect)
                Types.VARCHAR,   // STRING
                Types.BINARY,    // BYTES
                Types.DATE,      // DATE
                Types.TIME,      // TIME
                Types.TIMESTAMP, // DATETIME (civil time)
                Types.TIMESTAMP, // TIMESTAMP (UTC)
                Types.STRUCT,    // STRUCT/RECORD
                Types.ARRAY,     // ARRAY
                Types.OTHER,     // JSON
                Types.OTHER      // GEOGRAPHY
        ));
        columnResult.addColumn("TYPE_NAME", Arrays.asList(
                "INT64", "FLOAT64", "BOOL", "BOOL", "NUMERIC", "BIGNUMERIC",
                "STRING", "BYTES", "DATE", "TIME", "DATETIME", "TIMESTAMP",
                "STRUCT", "ARRAY", "JSON", "GEOGRAPHY"
        ));
        columnResult.addColumn("COLUMN_SIZE", Arrays.asList(
                19, 17, 1, 1, 38, 76,
                0, 0, 10, 8, 29, 29,
                0, 0, 0, 0
        ));
        columnResult.addColumn("DECIMAL_DIGITS", Arrays.asList(
                0, 0, 0, 0, 9, 38,
                0, 0, 0, 0, 0, 0,
                0, 0, 0, 0
        ));
        columnResult.addColumn("COLUMN_NAME", Arrays.asList(
                "id", "price", "active_bool", "active_bit", "amount", "big_amount",
                "name", "payload", "event_date", "event_time", "event_datetime", "event_timestamp",
                "details", "tags", "metadata", "location"
        ));
        columnResult.addColumn("IS_NULLABLE", Arrays.asList(
                "NO", "YES", "YES", "YES", "YES", "YES",
                "YES", "YES", "YES", "YES", "YES", "YES",
                "YES", "YES", "YES", "YES"
        ));
        columnResult.addColumn("REMARKS", Arrays.asList(
                "pk", null, null, null, null, null,
                null, null, null, null, null, null,
                null, null, null, null
        ));

        properties = new HashMap<>();
        properties.put(JDBCResource.DRIVER_CLASS, "com.simba.googlebigquery.jdbc.Driver");
        properties.put(JDBCResource.URI,
                "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;ProjectId=my-project;OAuthType=3");
        properties.put(JDBCResource.USER, "");
        properties.put(JDBCResource.PASSWORD, "");
        properties.put(JDBCResource.CHECK_SUM, "xxxx");
        properties.put(JDBCResource.DRIVER_URL, "xxxx");
    }

    // -------------------------------------------------------------------------
    // Type mapping unit tests (no connection needed)
    // -------------------------------------------------------------------------

    @Test
    public void testIntegerTypes() {
        BigQuerySchemaResolver r = new BigQuerySchemaResolver();
        Assertions.assertTrue(r.convertColumnType(Types.BIGINT, "INT64", 19, 0).isBigint());
        Assertions.assertTrue(r.convertColumnType(Types.INTEGER, "INT32", 10, 0).isInt());
        Assertions.assertTrue(r.convertColumnType(Types.SMALLINT, "INT16", 5, 0).isSmallint());
        Assertions.assertTrue(r.convertColumnType(Types.TINYINT, "INT8", 3, 0).isTinyint());
    }

    @Test
    public void testFloatTypes() {
        BigQuerySchemaResolver r = new BigQuerySchemaResolver();
        Assertions.assertTrue(r.convertColumnType(Types.DOUBLE, "FLOAT64", 17, 0).isDouble());
        Assertions.assertTrue(r.convertColumnType(Types.FLOAT, "FLOAT", 8, 0).isFloat());
    }

    @Test
    public void testBooleanTypes() {
        BigQuerySchemaResolver r = new BigQuerySchemaResolver();
        // Simba driver may return BIT or BOOLEAN for BigQuery BOOL
        Assertions.assertTrue(r.convertColumnType(Types.BOOLEAN, "BOOL", 1, 0).isBoolean());
        Assertions.assertTrue(r.convertColumnType(Types.BIT, "BOOL", 1, 0).isBoolean());
    }

    @Test
    public void testNumericTypes() {
        BigQuerySchemaResolver r = new BigQuerySchemaResolver();
        // NUMERIC(38,9) — standard BigQuery NUMERIC
        Assertions.assertTrue(r.convertColumnType(Types.NUMERIC, "NUMERIC", 38, 9).isDecimalV3());
        // columnSize == 0 → unknown precision → VARCHAR fallback
        Assertions.assertTrue(r.convertColumnType(Types.NUMERIC, "NUMERIC", 0, 0).isStringType());
        // precision > 38 → VARCHAR fallback
        Assertions.assertTrue(r.convertColumnType(Types.NUMERIC, "NUMERIC", 76, 38).isStringType());
    }

    @Test
    public void testBigNumeric() {
        BigQuerySchemaResolver r = new BigQuerySchemaResolver();
        // BIGNUMERIC detected by typeName regardless of JDBC type code
        Assertions.assertTrue(r.convertColumnType(Types.NUMERIC, "BIGNUMERIC", 76, 38).isStringType());
        Assertions.assertTrue(r.convertColumnType(Types.NUMERIC, "BIGNUMERIC(76, 38)", 76, 38).isStringType());
        Assertions.assertTrue(r.convertColumnType(Types.NUMERIC, "bignumeric", 76, 38).isStringType());
    }

    @Test
    public void testStringType() {
        BigQuerySchemaResolver r = new BigQuerySchemaResolver();
        // BigQuery STRING → max-length VARCHAR
        Assertions.assertTrue(r.convertColumnType(Types.VARCHAR, "STRING", 0, 0).isStringType());
        Assertions.assertTrue(r.convertColumnType(Types.LONGVARCHAR, "STRING", 0, 0).isStringType());
    }

    @Test
    public void testBytesType() {
        BigQuerySchemaResolver r = new BigQuerySchemaResolver();
        Assertions.assertTrue(r.convertColumnType(Types.BINARY, "BYTES", 0, 0).isBinaryType());
        Assertions.assertTrue(r.convertColumnType(Types.VARBINARY, "BYTES", 0, 0).isBinaryType());
    }

    @Test
    public void testTemporalTypes() {
        BigQuerySchemaResolver r = new BigQuerySchemaResolver();
        Assertions.assertTrue(r.convertColumnType(Types.DATE, "DATE", 10, 0).isDate());
        Assertions.assertTrue(r.convertColumnType(Types.TIME, "TIME", 8, 0).isTime());
        // Both DATETIME and TIMESTAMP → DATETIME in StarRocks
        Assertions.assertTrue(r.convertColumnType(Types.TIMESTAMP, "DATETIME", 29, 0).isDatetime());
        Assertions.assertTrue(r.convertColumnType(Types.TIMESTAMP, "TIMESTAMP", 29, 0).isDatetime());
        Assertions.assertTrue(r.convertColumnType(Types.TIMESTAMP_WITH_TIMEZONE, "TIMESTAMP", 29, 0).isDatetime());
    }

    @Test
    public void testComplexTypes() {
        BigQuerySchemaResolver r = new BigQuerySchemaResolver();
        // STRUCT and ARRAY → VARCHAR (serialised representation)
        Assertions.assertTrue(r.convertColumnType(Types.STRUCT, "STRUCT", 0, 0).isStringType());
        Assertions.assertTrue(r.convertColumnType(Types.ARRAY, "ARRAY", 0, 0).isStringType());
    }

    @Test
    public void testJsonType() {
        BigQuerySchemaResolver r = new BigQuerySchemaResolver();
        Assertions.assertTrue(r.convertColumnType(Types.OTHER, "JSON", 0, 0).isJsonType());
        Assertions.assertTrue(r.convertColumnType(Types.OTHER, "json", 0, 0).isJsonType());
    }

    @Test
    public void testGeographyAndUnknown() {
        BigQuerySchemaResolver r = new BigQuerySchemaResolver();
        // GEOGRAPHY and anything else → VARCHAR
        Assertions.assertTrue(r.convertColumnType(Types.OTHER, "GEOGRAPHY", 0, 0).isStringType());
        Assertions.assertTrue(r.convertColumnType(Types.OTHER, "INTERVAL", 0, 0).isStringType());
        Assertions.assertTrue(r.convertColumnType(Types.OTHER, null, 0, 0).isStringType());
        Assertions.assertTrue(r.convertColumnType(999, "UNKNOWN", 0, 0).isStringType());
    }

    // -------------------------------------------------------------------------
    // Integration-style tests via JDBCMetadata (mocked connection)
    // -------------------------------------------------------------------------

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
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "bq_catalog", dataSource);
            List<String> result = jdbcMetadata.listDbNames(new ConnectContext());
            List<String> expected = Lists.newArrayList("my_dataset", "another_dataset");
            Assertions.assertEquals(expected, result);
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
                result = "my-project";
                minTimes = 0;

                connection.getMetaData().getTables("my-project", "my_dataset", null,
                        new String[] {"TABLE", "VIEW", "EXTERNAL"});
                result = tableResult;
                minTimes = 0;
            }
        };
        try {
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "bq_catalog", dataSource);
            List<String> result = jdbcMetadata.listTableNames(new ConnectContext(), "my_dataset");
            List<String> expected = Lists.newArrayList("orders", "customers", "events");
            Assertions.assertEquals(expected, result);
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
                result = "my-project";
                minTimes = 0;

                connection.getMetaData().getColumns("my-project", "my_dataset", "orders", "%");
                result = columnResult;
                minTimes = 0;
            }
        };
        try {
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "bq_catalog", dataSource);
            Table table = jdbcMetadata.getTable(new ConnectContext(), "my_dataset", "orders");
            Assertions.assertTrue(table instanceof JDBCTable);
            Assertions.assertEquals(16, table.getColumns().size());

            // INT64 → BIGINT
            Assertions.assertTrue(table.getColumn("id").getType().isBigint());
            // FLOAT64 → DOUBLE
            Assertions.assertTrue(table.getColumn("price").getType().isDouble());
            // BOOL (BOOLEAN) → BOOLEAN
            Assertions.assertTrue(table.getColumn("active_bool").getType().isBoolean());
            // BOOL (BIT) → BOOLEAN
            Assertions.assertTrue(table.getColumn("active_bit").getType().isBoolean());
            // NUMERIC(38,9) → DECIMAL
            Assertions.assertTrue(table.getColumn("amount").getType().isDecimalV3());
            // BIGNUMERIC → VARCHAR
            Assertions.assertTrue(table.getColumn("big_amount").getType().isStringType());
            // STRING → VARCHAR
            Assertions.assertTrue(table.getColumn("name").getType().isStringType());
            // BYTES → VARBINARY
            Assertions.assertTrue(table.getColumn("payload").getType().isBinaryType());
            // DATE → DATE
            Assertions.assertTrue(table.getColumn("event_date").getType().isDate());
            // TIME → TIME
            Assertions.assertTrue(table.getColumn("event_time").getType().isTime());
            // DATETIME → DATETIME
            Assertions.assertTrue(table.getColumn("event_datetime").getType().isDatetime());
            // TIMESTAMP → DATETIME
            Assertions.assertTrue(table.getColumn("event_timestamp").getType().isDatetime());
            // STRUCT → VARCHAR
            Assertions.assertTrue(table.getColumn("details").getType().isStringType());
            // ARRAY → VARCHAR
            Assertions.assertTrue(table.getColumn("tags").getType().isStringType());
            // JSON → JSON
            Assertions.assertTrue(table.getColumn("metadata").getType().isJsonType());
            // GEOGRAPHY → VARCHAR
            Assertions.assertTrue(table.getColumn("location").getType().isStringType());

            // Table name should use backtick quoting for BigQuery SQL
            JDBCTable jdbcTable = (JDBCTable) table;
            Assertions.assertEquals("`my_dataset.orders`", jdbcTable.getConnectInfo(JDBCTable.JDBC_TABLENAME));
        } catch (Exception e) {
            System.out.println(e.getMessage());
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
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "bq_catalog", dataSource);
            // my_dataset exists in dbResult
            com.starrocks.catalog.Database db = jdbcMetadata.getDb(new ConnectContext(), "my_dataset");
            Assertions.assertEquals("my_dataset", db.getOriginName());
            // non-existent dataset returns null
            com.starrocks.catalog.Database missing = jdbcMetadata.getDb(new ConnectContext(), "no_such_dataset");
            Assertions.assertNull(missing);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testInformationSchemaFilteredFromDatasetList() throws SQLException {
        MockResultSet resultWithInternalSchemas = new MockResultSet("catalog");
        resultWithInternalSchemas.addColumn("TABLE_SCHEM",
                Arrays.asList("my_dataset", "INFORMATION_SCHEMA", "information_schema", "another_dataset"));

        new Expectations() {
            {
                dataSource.getConnection();
                result = connection;
                minTimes = 0;

                connection.getMetaData().getSchemas();
                result = resultWithInternalSchemas;
                minTimes = 0;
            }
        };
        try {
            JDBCMetadata jdbcMetadata = new JDBCMetadata(properties, "bq_catalog", dataSource);
            List<String> result = jdbcMetadata.listDbNames(new ConnectContext());
            Assertions.assertFalse(result.contains("INFORMATION_SCHEMA"));
            Assertions.assertFalse(result.contains("information_schema"));
            Assertions.assertTrue(result.contains("my_dataset"));
            Assertions.assertTrue(result.contains("another_dataset"));
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testSimbaDriverClassAutoDetection() throws SQLException {
        // com.simba.googlebigquery.jdbc.Driver contains "googlebigquery" → auto-detects BigQuery
        Map<String, String> simbaProps = new HashMap<>(properties);
        simbaProps.put(JDBCResource.DRIVER_CLASS, "com.simba.googlebigquery.jdbc.Driver");
        try {
            JDBCMetadata jdbcMetadata = new JDBCMetadata(simbaProps, "bq_catalog", dataSource);
            Assertions.assertNotNull(jdbcMetadata);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testSimba42DriverClassAutoDetection() throws SQLException {
        // com.simba.googlebigquery.jdbc42.Driver contains "bigquery" → auto-detects BigQuery
        Map<String, String> simba42Props = new HashMap<>(properties);
        simba42Props.put(JDBCResource.DRIVER_CLASS, "com.simba.googlebigquery.jdbc42.Driver");
        try {
            JDBCMetadata jdbcMetadata = new JDBCMetadata(simba42Props, "bq_catalog", dataSource);
            Assertions.assertNotNull(jdbcMetadata);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testSchemaResolverPropertyBigquery() {
        // Explicit schema_resolver=bigquery property overrides driver-class auto-detection.
        Map<String, String> props = new HashMap<>(properties);
        props.put(JDBCResource.SCHEMA_RESOLVER, "bigquery");
        try {
            JDBCMetadata jdbcMetadata = new JDBCMetadata(props, "bq_catalog", dataSource);
            Assertions.assertNotNull(jdbcMetadata);
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testGetPartitions() {
        BigQuerySchemaResolver resolver = new BigQuerySchemaResolver();
        List<Partition> partitions = resolver.getPartitions(null,
                new Table(1L, "orders", Table.TableType.JDBC, Lists.newArrayList()));
        Assertions.assertEquals(1, partitions.size());
        Assertions.assertEquals("orders", partitions.get(0).getPartitionName());
    }
}
