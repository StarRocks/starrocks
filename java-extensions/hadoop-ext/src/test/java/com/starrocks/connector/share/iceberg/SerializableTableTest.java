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

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopConfigurable;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializableSupplier;
import org.apache.iceberg.util.SerializationUtil;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
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

    @Test
    public void testIoNormalizesAzurePathAfterRoundTrip() {
        Table table = createTable(new HashMap<>());
        Configuration conf = new Configuration(false);
        conf.set("k", "v");
        SerializableTable deserialized = SerializationUtil.deserializeFromBase64(
                SerializationUtil.serializeToBase64(new SerializableTable(table, new LocationOnlyFileIO(conf))));

        FileIO io = deserialized.io();
        assertInstanceOf(AzurePathNormalizingFileIO.class, io);
        // the Hadoop conf is still serialized through the wrapped IO
        LocationOnlyFileIO wrapped =
                assertInstanceOf(LocationOnlyFileIO.class, ((AzurePathNormalizingFileIO) io).getDelegate());
        assertInstanceOf(SerializableTable.SerializableConfSupplier.class, wrapped.confSupplier);
        assertEquals("v", wrapped.getConf().get("k"));
        // storage is accessed through wasbs, while location() keeps the caller's path
        io.newInputFile("abfss://c@acct.blob.core.windows.net/m/a.avro");
        io.newInputFile("abfss://c@acct.dfs.core.windows.net/m/a.avro");
        assertEquals(List.of("wasbs://c@acct.blob.core.windows.net/m/a.avro",
                "abfss://c@acct.dfs.core.windows.net/m/a.avro"), wrapped.accessed);
        assertEquals("abfss://c@acct.blob.core.windows.net/m/a.avro",
                io.newInputFile("abfss://c@acct.blob.core.windows.net/m/a.avro").location());
    }

    @Test
    public void testLazyTableLoadsThroughNormalizingIo() {
        Table table = createTable(new HashMap<>());
        SerializableTable deserialized = SerializationUtil.deserializeFromBase64(
                SerializationUtil.serializeToBase64(new SerializableTable(table, new InMemoryFileIO())));

        // loads the metadata json through io(), then hands the same IO to the table operations
        assertEquals(table.schemas().keySet(), deserialized.schemas().keySet());
        assertInstanceOf(AzurePathNormalizingFileIO.class, deserialized.operations().io());
    }

    private Table createTable(Map<String, String> properties) {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", new HashMap<>());
        catalog.createNamespace(Namespace.of("db"));
        TableIdentifier id = TableIdentifier.of("db", "tbl");
        return catalog.createTable(id, SCHEMA, PartitionSpec.unpartitioned(), properties);
    }

    static class LocationOnlyFileIO implements FileIO, HadoopConfigurable {
        private SerializableSupplier<Configuration> confSupplier;
        private final List<String> accessed = new ArrayList<>();

        LocationOnlyFileIO(Configuration conf) {
            this.confSupplier = () -> conf;
        }

        @Override
        public InputFile newInputFile(String path) {
            accessed.add(path);
            return new InputFile() {
                @Override
                public long getLength() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public SeekableInputStream newStream() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public String location() {
                    return path;
                }

                @Override
                public boolean exists() {
                    return false;
                }
            };
        }

        @Override
        public OutputFile newOutputFile(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteFile(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void serializeConfWith(
                Function<Configuration, SerializableSupplier<Configuration>> confSerializer) {
            this.confSupplier = confSerializer.apply(confSupplier.get());
        }

        @Override
        public void setConf(Configuration conf) {
            this.confSupplier = () -> conf;
        }

        @Override
        public Configuration getConf() {
            return confSupplier.get();
        }
    }
}
