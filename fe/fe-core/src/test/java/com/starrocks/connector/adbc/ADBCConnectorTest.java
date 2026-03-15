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

import com.google.common.collect.Lists;
import com.starrocks.catalog.ADBCTable;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ADBCConnectorTest {

    private ConnectorContext createContext(Map<String, String> properties) {
        return new ConnectorContext("test_catalog", "adbc", properties);
    }

    @Test
    public void testMissingDriverThrows() {
        Map<String, String> props = new HashMap<>();
        props.put(ADBCConnector.PROP_URL, "grpc://localhost:31337");

        assertThrows(StarRocksConnectorException.class, () -> {
            new ADBCConnector(createContext(props));
        });
    }

    @Test
    public void testMissingUrlThrows() {
        Map<String, String> props = new HashMap<>();
        props.put(ADBCConnector.PROP_DRIVER, "flight_sql");

        assertThrows(StarRocksConnectorException.class, () -> {
            new ADBCConnector(createContext(props));
        });
    }

    @Test
    public void testValidPropsDoNotThrow() {
        Map<String, String> props = new HashMap<>();
        props.put(ADBCConnector.PROP_DRIVER, "flight_sql");
        props.put(ADBCConnector.PROP_URL, "grpc://localhost:31337");

        ADBCConnector connector = new ADBCConnector(createContext(props));
        assertNotNull(connector);
    }

    @Test
    public void testGetMetadataReturnsNonNull() {
        Map<String, String> props = new HashMap<>();
        props.put(ADBCConnector.PROP_DRIVER, "flight_sql");
        props.put(ADBCConnector.PROP_URL, "grpc://localhost:31337");

        ADBCConnector connector = new ADBCConnector(createContext(props));
        ConnectorMetadata metadata = connector.getMetadata();
        assertNotNull(metadata);
    }

    @Test
    public void testShutdownDoesNotThrow() {
        Map<String, String> props = new HashMap<>();
        props.put(ADBCConnector.PROP_DRIVER, "flight_sql");
        props.put(ADBCConnector.PROP_URL, "grpc://localhost:31337");

        ADBCConnector connector = new ADBCConnector(createContext(props));
        connector.shutdown();
    }

    // ADBCTableName tests

    @Test
    public void testTableNameEquality() {
        ADBCTableName a = ADBCTableName.of("cat", "db", "tbl");
        ADBCTableName b = ADBCTableName.of("cat", "db", "tbl");
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    public void testTableNameInequality() {
        ADBCTableName a = ADBCTableName.of("cat", "db", "tbl1");
        ADBCTableName b = ADBCTableName.of("cat", "db", "tbl2");
        assertFalse(a.equals(b));
    }

    @Test
    public void testTableNameGetters() {
        ADBCTableName name = ADBCTableName.of("cat", "db", "tbl");
        assertEquals("cat", name.getCatalogName());
        assertEquals("db", name.getDatabaseName());
        assertEquals("tbl", name.getTableName());
    }

    // ADBCSchemaResolver tests

    @Test
    public void testSchemaResolverIsAbstract() {
        // ADBCSchemaResolver cannot be instantiated directly
        assertTrue(java.lang.reflect.Modifier.isAbstract(ADBCSchemaResolver.class.getModifiers()));
    }

    // ADBCTable tests

    @Test
    public void testADBCTableType() {
        List<Column> columns = Lists.newArrayList(
                new Column("id", IntegerType.INT, true, "")
        );
        Map<String, String> props = new HashMap<>();
        ADBCTable table = new ADBCTable(1L, "test_table", columns, "mydb", "mycat", props);
        assertEquals(Table.TableType.ADBC, table.getType());
        assertTrue(table.isADBCTable());
    }

    @Test
    public void testADBCTableGetters() {
        List<Column> columns = Lists.newArrayList(
                new Column("id", IntegerType.INT, true, "")
        );
        Map<String, String> props = new HashMap<>();
        props.put("key", "value");
        ADBCTable table = new ADBCTable(1L, "test_table", columns, "mydb", "mycat", props);
        assertEquals("mycat", table.getCatalogName());
        assertEquals("mydb", table.getDbName());
        assertEquals("mycat.mydb", table.getCatalogDBName());
        assertEquals(props, table.getProperties());
    }

    @Test
    public void testADBCTableIsSupported() {
        List<Column> columns = Lists.newArrayList(
                new Column("id", IntegerType.INT, true, "")
        );
        ADBCTable table = new ADBCTable(1L, "t", columns, "db", "cat", new HashMap<>());
        assertTrue(table.isSupported());
    }

    // Driver and URI validation tests

    @Test
    public void testUnsupportedDriverThrows() {
        Map<String, String> props = new HashMap<>();
        props.put(ADBCConnector.PROP_DRIVER, "asd");
        props.put(ADBCConnector.PROP_URL, "grpc://localhost:31337");

        StarRocksConnectorException ex = assertThrows(StarRocksConnectorException.class, () -> {
            new ADBCConnector(createContext(props));
        });
        assertTrue(ex.getMessage().contains("Unsupported ADBC driver"),
                "Expected 'Unsupported ADBC driver' in: " + ex.getMessage());
    }

    @Test
    public void testInvalidUriSchemeThrows() {
        Map<String, String> props = new HashMap<>();
        props.put(ADBCConnector.PROP_DRIVER, "flight_sql");
        props.put(ADBCConnector.PROP_URL, "http://localhost:31337");

        StarRocksConnectorException ex = assertThrows(StarRocksConnectorException.class, () -> {
            new ADBCConnector(createContext(props));
        });
        assertTrue(ex.getMessage().contains("Invalid ADBC URI scheme"),
                "Expected 'Invalid ADBC URI scheme' in: " + ex.getMessage());
    }

    @Test
    public void testDriverCaseInsensitive() {
        Map<String, String> props = new HashMap<>();
        props.put(ADBCConnector.PROP_DRIVER, "Flight_SQL");
        props.put(ADBCConnector.PROP_URL, "grpc://localhost:31337");

        ADBCConnector connector = new ADBCConnector(createContext(props));
        assertNotNull(connector);
    }

    // TLS validation tests

    @Test
    public void testTlsNonexistentCaCertFileThrows() {
        Map<String, String> props = new HashMap<>();
        props.put(ADBCConnector.PROP_DRIVER, "flight_sql");
        props.put(ADBCConnector.PROP_URL, "grpc+tls://localhost:443");
        props.put(ADBCConnector.PROP_TLS_CA_CERT_FILE, "/nonexistent/ca.pem");

        StarRocksConnectorException ex = assertThrows(StarRocksConnectorException.class, () -> {
            new ADBCConnector(createContext(props));
        });
        assertTrue(ex.getMessage().contains("does not exist"), "Expected 'does not exist' in: " + ex.getMessage());
    }

    @Test
    public void testTlsMtlsMissingKeyThrows() {
        Map<String, String> props = new HashMap<>();
        props.put(ADBCConnector.PROP_DRIVER, "flight_sql");
        props.put(ADBCConnector.PROP_URL, "grpc+tls://localhost:443");
        props.put(ADBCConnector.PROP_TLS_CLIENT_CERT_FILE, "/tmp/some.pem");

        StarRocksConnectorException ex = assertThrows(StarRocksConnectorException.class, () -> {
            new ADBCConnector(createContext(props));
        });
        assertTrue(ex.getMessage().contains("must both be provided"), "Expected 'must both be provided' in: " + ex.getMessage());
    }

    @Test
    public void testTlsMtlsMissingCertThrows() {
        Map<String, String> props = new HashMap<>();
        props.put(ADBCConnector.PROP_DRIVER, "flight_sql");
        props.put(ADBCConnector.PROP_URL, "grpc+tls://localhost:443");
        props.put(ADBCConnector.PROP_TLS_CLIENT_KEY_FILE, "/tmp/some.key");

        StarRocksConnectorException ex = assertThrows(StarRocksConnectorException.class, () -> {
            new ADBCConnector(createContext(props));
        });
        assertTrue(ex.getMessage().contains("must both be provided"), "Expected 'must both be provided' in: " + ex.getMessage());
    }

    @Test
    public void testNonTlsUriWithCertPropsDoesNotThrow() {
        Map<String, String> props = new HashMap<>();
        props.put(ADBCConnector.PROP_DRIVER, "flight_sql");
        props.put(ADBCConnector.PROP_URL, "grpc://localhost:31337");
        props.put(ADBCConnector.PROP_TLS_CA_CERT_FILE, "/some/path");

        // Should NOT throw -- certs are just ignored with warning for non-TLS URI
        ADBCConnector connector = new ADBCConnector(createContext(props));
        assertNotNull(connector);
    }

    @Test
    public void testTlsVerifyFalseDoesNotThrow() {
        Map<String, String> props = new HashMap<>();
        props.put(ADBCConnector.PROP_DRIVER, "flight_sql");
        props.put(ADBCConnector.PROP_URL, "grpc+tls://localhost:443");
        props.put(ADBCConnector.PROP_TLS_VERIFY, "false");

        // Should NOT throw -- insecure mode with no cert files
        ADBCConnector connector = new ADBCConnector(createContext(props));
        assertNotNull(connector);
    }

    @Test
    public void testTlsUriCaseInsensitive() {
        Map<String, String> props = new HashMap<>();
        props.put(ADBCConnector.PROP_DRIVER, "flight_sql");
        props.put(ADBCConnector.PROP_URL, "GRPC+TLS://localhost:443");
        props.put(ADBCConnector.PROP_TLS_CA_CERT_FILE, "/nonexistent/ca.pem");

        // Should detect TLS and validate the cert file (which doesn't exist)
        StarRocksConnectorException ex = assertThrows(StarRocksConnectorException.class, () -> {
            new ADBCConnector(createContext(props));
        });
        assertTrue(ex.getMessage().contains("does not exist"), "Expected 'does not exist' in: " + ex.getMessage());
    }
}
