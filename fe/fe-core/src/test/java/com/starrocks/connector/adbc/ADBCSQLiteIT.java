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

package com.starrocks.connector.adbc;

import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.driver.jni.JniDriverFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test exercising the full ADBC catalog lifecycle against a real SQLite
 * ADBC driver (libadbc_driver_sqlite.so).
 *
 * <p>Requires {@code -Dadbc.sqlite.driver.path=/path/to/libadbc_driver_sqlite.so} to run.
 * Skipped silently when the system property is not set.</p>
 *
 * <p>Uses a temp-file SQLite database (not :memory:) so that data seeded in setUp
 * is visible across all connections opened by ADBCMetadata.</p>
 */
@EnabledIfSystemProperty(named = "adbc.sqlite.driver.path", matches = ".+")
public class ADBCSQLiteIT {

    private String driverPath;
    private File tempDbFile;
    private String dbUri;

    @BeforeEach
    public void setUp() throws Exception {
        driverPath = System.getProperty("adbc.sqlite.driver.path");

        // Create a temp file for the SQLite database
        tempDbFile = File.createTempFile("adbc_test_", ".db");
        dbUri = tempDbFile.getAbsolutePath();

        // Seed the database with test data using a direct JniDriver connection
        BufferAllocator seedAllocator = new RootAllocator();
        try {
            AdbcDriver driver = new JniDriverFactory().getDriver(seedAllocator);
            Map<String, Object> params = new HashMap<>();
            params.put("jni.driver", driverPath);
            params.put("uri", dbUri);
            AdbcDatabase seedDb = driver.open(params);

            // Create and populate test table
            try (AdbcConnection conn = seedDb.connect();
                    AdbcStatement stmt = conn.createStatement()) {
                stmt.setSqlQuery(
                        "CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT, value REAL)");
                try (AdbcStatement.QueryResult qr = stmt.executeQuery()) {
                    // drain empty result (always use executeQuery, not executeUpdate)
                }
            }
            try (AdbcConnection conn = seedDb.connect();
                    AdbcStatement stmt = conn.createStatement()) {
                stmt.setSqlQuery("INSERT INTO test_table VALUES (1, 'alice', 3.14)");
                try (AdbcStatement.QueryResult qr = stmt.executeQuery()) {
                    // drain empty result
                }
            }
            try (AdbcConnection conn = seedDb.connect();
                    AdbcStatement stmt = conn.createStatement()) {
                stmt.setSqlQuery("INSERT INTO test_table VALUES (2, 'bob', 2.71)");
                try (AdbcStatement.QueryResult qr = stmt.executeQuery()) {
                    // drain empty result
                }
            }

            // Close the seed database -- data is persisted to the temp file
            seedDb.close();
        } finally {
            seedAllocator.close();
        }
    }

    @AfterEach
    public void tearDown() {
        if (tempDbFile != null && tempDbFile.exists()) {
            tempDbFile.delete();
        }
    }

    @Test
    public void testListDbNamesReturnMain() throws Exception {
        BufferAllocator alloc = new RootAllocator();
        try {
            AdbcDriver driver = new JniDriverFactory().getDriver(alloc);
            Map<String, Object> params = new HashMap<>();
            params.put("jni.driver", driverPath);
            params.put("uri", dbUri);
            AdbcDatabase db = driver.open(params);

            Map<String, String> props = new HashMap<>();
            props.put("type", "adbc");
            props.put("driver_url", driverPath);
            props.put("uri", dbUri);

            ADBCMetadata metadata = new ADBCMetadata(props, "sqlite_test", db);
            List<String> dbNames = metadata.listDbNames(null);

            assertNotNull(dbNames);
            assertFalse(dbNames.isEmpty());
            assertTrue(dbNames.contains("main"), "Expected 'main' schema, got: " + dbNames);

            metadata.shutdown();
        } finally {
            alloc.close();
        }
    }

    @Test
    public void testListTableNamesReturnsTestTable() throws Exception {
        BufferAllocator alloc = new RootAllocator();
        try {
            AdbcDriver driver = new JniDriverFactory().getDriver(alloc);
            Map<String, Object> params = new HashMap<>();
            params.put("jni.driver", driverPath);
            params.put("uri", dbUri);
            AdbcDatabase db = driver.open(params);

            Map<String, String> props = new HashMap<>();
            props.put("type", "adbc");
            props.put("driver_url", driverPath);
            props.put("uri", dbUri);

            ADBCMetadata metadata = new ADBCMetadata(props, "sqlite_test", db);
            List<String> tableNames = metadata.listTableNames(null, "main");

            assertNotNull(tableNames);
            assertTrue(tableNames.contains("test_table"),
                    "Expected 'test_table', got: " + tableNames);

            metadata.shutdown();
        } finally {
            alloc.close();
        }
    }

    @Test
    public void testGetTableReturnsColumnsWithCorrectTypes() throws Exception {
        BufferAllocator alloc = new RootAllocator();
        try {
            AdbcDriver driver = new JniDriverFactory().getDriver(alloc);
            Map<String, Object> params = new HashMap<>();
            params.put("jni.driver", driverPath);
            params.put("uri", dbUri);
            AdbcDatabase db = driver.open(params);

            Map<String, String> props = new HashMap<>();
            props.put("type", "adbc");
            props.put("driver_url", driverPath);
            props.put("uri", dbUri);

            ADBCMetadata metadata = new ADBCMetadata(props, "sqlite_test", db);
            Table table = metadata.getTable(null, "main", "test_table");

            assertNotNull(table, "getTable returned null for test_table");
            assertTrue(table.getFullSchema().size() >= 3,
                    "Expected >= 3 columns, got: " + table.getFullSchema().size());
            // Verify column names
            List<String> colNames = table.getFullSchema().stream()
                    .map(c -> c.getName()).collect(Collectors.toList());
            assertTrue(colNames.contains("id"), "Missing column 'id'. Columns: " + colNames);
            assertTrue(colNames.contains("name"), "Missing column 'name'. Columns: " + colNames);
            assertTrue(colNames.contains("value"), "Missing column 'value'. Columns: " + colNames);

            metadata.shutdown();
        } finally {
            alloc.close();
        }
    }

    @Test
    public void testConnectorEndToEnd() {
        Map<String, String> props = new HashMap<>();
        props.put("type", "adbc");
        props.put("driver_url", driverPath);
        props.put("uri", dbUri);

        ConnectorContext context = new ConnectorContext(
                "sqlite_e2e_test", "adbc", props);
        ADBCConnector connector = new ADBCConnector(context);

        try {
            ConnectorMetadata metadata = connector.getMetadata();
            assertNotNull(metadata, "getMetadata() returned null");

            // Validate full lifecycle through ADBCConnector -> ADBCMetadata
            List<String> dbNames = metadata.listDbNames(null);
            assertNotNull(dbNames);
            assertTrue(dbNames.contains("main"),
                    "Expected 'main' schema via connector, got: " + dbNames);

            List<String> tableNames = metadata.listTableNames(null, "main");
            assertNotNull(tableNames);
            assertTrue(tableNames.contains("test_table"),
                    "Expected 'test_table' via connector, got: " + tableNames);

            Table table = metadata.getTable(null, "main", "test_table");
            assertNotNull(table, "getTable via connector returned null");
            assertTrue(table.getFullSchema().size() >= 3,
                    "Expected >= 3 columns via connector");
        } finally {
            connector.shutdown();
        }
    }

    @Test
    public void testMetadataShutdownClosesCleanly() throws Exception {
        BufferAllocator alloc = new RootAllocator();
        try {
            AdbcDriver driver = new JniDriverFactory().getDriver(alloc);
            Map<String, Object> params = new HashMap<>();
            params.put("jni.driver", driverPath);
            params.put("uri", dbUri);
            AdbcDatabase db = driver.open(params);

            Map<String, String> props = new HashMap<>();
            props.put("type", "adbc");
            props.put("driver_url", driverPath);
            props.put("uri", dbUri);

            ADBCMetadata metadata = new ADBCMetadata(props, "shutdown_test", db);
            assertDoesNotThrow(() -> metadata.shutdown());
        } finally {
            alloc.close();
            // arrow.memory.debug.allocator=true will catch any leak here
        }
    }
}
