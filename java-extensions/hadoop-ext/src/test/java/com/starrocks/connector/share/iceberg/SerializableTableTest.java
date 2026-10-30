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

package com.starrocks.connector.share.iceberg;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SerializableTableTest {

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get())
    );

    @Test
    public void testConstructionSucceedsWithUnresolvableLocationProvider() {
        // Regression test for https://github.com/StarRocks/starrocks/issues/73471
        // SerializableTable should not eagerly load the LocationProvider at construction time.
        // Custom LocationProvider JARs are write-path only and not available in FE classpath.
        Map<String, String> props = new HashMap<>();
        props.put("write.location-provider.impl", "com.nonexistent.CustomLocationProvider");

        Table table = createTable(props);
        InMemoryFileIO fileIO = new InMemoryFileIO();

        // Should not throw ClassNotFoundException even though the provider class doesn't exist
        SerializableTable serializable = assertDoesNotThrow(
                () -> new SerializableTable(table, fileIO),
                "SerializableTable construction must not trigger LocationProvider loading");

        assertNotNull(serializable.schema());
        assertNotNull(serializable.spec());
        assertNotNull(serializable.refs());
    }

    @Test
    public void testLocationProviderThrowsOnAccess() {
        // When explicitly accessed, locationProvider() should throw because the class doesn't exist.
        Map<String, String> props = new HashMap<>();
        props.put("write.location-provider.impl", "com.nonexistent.CustomLocationProvider");

        Table table = createTable(props);
        SerializableTable serializable = new SerializableTable(table, new InMemoryFileIO());

        assertThrows(IllegalArgumentException.class, serializable::locationProvider,
                "locationProvider() should throw when the impl class is not in classpath");
    }

    @Test
    public void testConstructionSucceedsWithNoCustomLocationProvider() {
        Table table = createTable(new HashMap<>());
        SerializableTable serializable = assertDoesNotThrow(
                () -> new SerializableTable(table, new InMemoryFileIO()));

        assertNotNull(serializable.schema());
        assertNotNull(serializable.locationProvider());
    }

    private Table createTable(Map<String, String> properties) {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", new HashMap<>());
        catalog.createNamespace(Namespace.of("db"));
        TableIdentifier id = TableIdentifier.of("db", "tbl");
        return catalog.createTable(id, SCHEMA, PartitionSpec.unpartitioned(), properties);
    }
}
