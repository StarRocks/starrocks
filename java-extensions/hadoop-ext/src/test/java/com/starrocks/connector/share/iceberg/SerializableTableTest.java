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
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
<<<<<<< HEAD
=======
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
>>>>>>> 0489f28 ([Enhancement][RB-225] Share the deserialized Iceberg table across metadata scan tasks (#83094))

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
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

<<<<<<< HEAD
=======
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

    @Test
    public void testConcurrentMetadataAccessReadsFileOnce() throws Exception {
        // The JNI metadata scanner shares one deserialized table across scan tasks, so the lazily loaded
        // metadata is read concurrently: it must be loaded once and published fully built.
        Table source = createTable(new HashMap<>());
        AtomicInteger reads = new AtomicInteger();
        FileIO countingIO = new FileIO() {
            @Override
            public InputFile newInputFile(String path) {
                reads.incrementAndGet();
                // Widen the window in which concurrent loads overlap.
                try {
                    Thread.sleep(20);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                return source.io().newInputFile(path);
            }

            @Override
            public OutputFile newOutputFile(String path) {
                return source.io().newOutputFile(path);
            }

            @Override
            public void deleteFile(String path) {
                source.io().deleteFile(path);
            }
        };
        SerializableTable table = new SerializableTable(source, countingIO);
        int threads = 16;
        CyclicBarrier gate = new CyclicBarrier(threads);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            List<Future<TableMetadata>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    gate.await();
                    return table.operations().current();
                }));
            }
            TableMetadata first = futures.get(0).get(10, TimeUnit.SECONDS);
            assertNotNull(first);
            for (Future<TableMetadata> future : futures) {
                assertSame(first, future.get(10, TimeUnit.SECONDS));
            }
            assertEquals(1, reads.get());
        } finally {
            pool.shutdownNow();
        }
    }

>>>>>>> 0489f28 ([Enhancement][RB-225] Share the deserialized Iceberg table across metadata scan tasks (#83094))
    private Table createTable(Map<String, String> properties) {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", new HashMap<>());
        catalog.createNamespace(Namespace.of("db"));
        TableIdentifier id = TableIdentifier.of("db", "tbl");
        return catalog.createTable(id, SCHEMA, PartitionSpec.unpartitioned(), properties);
    }
}
