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
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ADBCConnectorTest {

    private ConnectorContext createContext(Map<String, String> properties) {
        return new ConnectorContext("test_catalog", "adbc", properties);
    }

    // --- Property validation tests (new schema) ---

    @Test
    public void testBothDriverUrlAndDriverNameThrows() {
        Map<String, String> props = new HashMap<>();
        props.put("type", "adbc");
        props.put("driver_url", "/some/path");
        props.put("driver_name", "sqlite");

        StarRocksConnectorException ex = assertThrows(StarRocksConnectorException.class, () -> {
            new ADBCConnector(createContext(props));
        });
        assertTrue(ex.getMessage().contains("mutually exclusive"),
                "Expected 'mutually exclusive' in: " + ex.getMessage());
    }

    @Test
    public void testNeitherDriverUrlNorDriverNameThrows() {
        Map<String, String> props = new HashMap<>();
        props.put("type", "adbc");

        StarRocksConnectorException ex = assertThrows(StarRocksConnectorException.class, () -> {
            new ADBCConnector(createContext(props));
        });
        assertTrue(ex.getMessage().contains("one of 'driver_url' or 'driver_name' is required"),
                "Expected 'one of driver_url or driver_name is required' in: " + ex.getMessage());
    }

    @Test
    public void testUnknownTopLevelKeyThrows() {
        Map<String, String> props = new HashMap<>();
        props.put("type", "adbc");
        props.put("driver_url", "/some/path");
        props.put("bogus_key", "val");

        StarRocksConnectorException ex = assertThrows(StarRocksConnectorException.class, () -> {
            new ADBCConnector(createContext(props));
        });
        assertTrue(ex.getMessage().contains("unknown property 'bogus_key'"),
                "Expected 'unknown property bogus_key' in: " + ex.getMessage());
    }

    @Test
    public void testAdbcPrefixedKeyPassesValidation() {
        // adbc.* keys must pass property validation regardless of driver loading outcome.
        Map<String, String> props = new HashMap<>();
        props.put("type", "adbc");
        props.put("driver_url", "/some/path");
        props.put("adbc.flight.sql.rpc_timeout", "30");

        ADBCConnector.validateProperties(props);
    }

    @Test
    public void testLegacyCatalogRejectedWithClearMessage() {
        // Legacy v1 catalog shape: adbc.driver + adbc.url but no driver_url/driver_name.
        // The legacy property schema was removed in commit 58e940d41a; constructing such
        // a catalog must now fail fast with a clear message, not silently degrade.
        Map<String, String> props = new HashMap<>();
        props.put("adbc.driver", "flight_sql");
        props.put("adbc.url", "grpc://localhost:8815");

        StarRocksConnectorException ex = assertThrows(StarRocksConnectorException.class, () -> {
            new ADBCConnector(createContext(props));
        });
        assertTrue(ex.getMessage().contains("'driver_url' or 'driver_name' is required"),
                "Expected driver_url/driver_name guidance in: " + ex.getMessage());
    }

    @Test
    public void testKnownTopLevelKeysAccepted() {
        // All known top-level keys must pass property validation.
        Map<String, String> props = new HashMap<>();
        props.put("type", "adbc");
        props.put("driver_url", "/some/path");
        props.put("uri", ":memory:");
        props.put("username", "admin");
        props.put("password", "secret");
        props.put("path", "/data");
        props.put("driver_entrypoint", "my_init");

        ADBCConnector.validateProperties(props);
    }

    // --- ADBCTableName tests (unchanged) ---

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

    // --- ADBCTable tests (unchanged) ---

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
}
