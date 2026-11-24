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

package com.starrocks.connector.iceberg;

import com.starrocks.catalog.IcebergTable;
import com.starrocks.common.Config;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.connector.ConnectorMetadataCache;
import com.starrocks.connector.ConnectorMetadataCache.Kind;
import com.starrocks.connector.ConnectorMetadataCache.ScopedCache;
import com.starrocks.connector.ConnectorMetadataCache.Stats;
import com.starrocks.connector.ConnectorMetadataCache.Store;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ConnectorMetadataCacheIcebergTest {

    private static DataFile file(String path) {
        return DataFiles.builder(PartitionSpec.unpartitioned()).withPath(path).withFileSizeInBytes(10)
                .withRecordCount(1).withFormat(FileFormat.PARQUET).build();
    }

    private static Store newStore() {
        return ConnectorMetadataCache.newPrivate(1L << 30);
    }

    @Test
    public void testScopesDoNotSeeEachOther() {
        Store store = newStore();
        String a = ConnectorMetadataCache.register("c1", 3600);
        String b = ConnectorMetadataCache.register("c2", 3600);
        try {
            ScopedCache<String, Set<DataFile>> filesA = new ScopedCache<>(store, a, Kind.DATA_FILES);
            ScopedCache<String, Set<DataFile>> filesB = new ScopedCache<>(store, b, Kind.DATA_FILES);
            ScopedCache<String, Set<DataFile>> deletesA = new ScopedCache<>(store, a, Kind.DELETE_FILES);

            Set<DataFile> f1 = Set.of(file("f1"));
            Set<DataFile> f2 = Set.of(file("f2"));
            filesA.put("s3://bucket/m1.avro", f1);
            // the same manifest path in another catalog, or as another kind, is another entry
            Assertions.assertNull(filesB.getIfPresent("s3://bucket/m1.avro"));
            Assertions.assertNull(deletesA.getIfPresent("s3://bucket/m1.avro"));
            filesB.put("s3://bucket/m1.avro", f2);
            Assertions.assertSame(f1, filesA.getIfPresent("s3://bucket/m1.avro"));
            Assertions.assertSame(f2, filesB.getIfPresent("s3://bucket/m1.avro"));

            filesA.invalidateAll(List.of("s3://bucket/m1.avro"));
            Assertions.assertNull(filesA.getIfPresent("s3://bucket/m1.avro"));
            Assertions.assertSame(f2, filesB.getIfPresent("s3://bucket/m1.avro"));
        } finally {
            ConnectorMetadataCache.unregister(a, Set.of(store));
            ConnectorMetadataCache.unregister(b, Set.of(store));
        }
    }

    // A dropped catalog's entries go at once, and a catalog created again under the same name starts empty.
    @Test
    public void testUnregisterDropsTheScope() {
        Store store = newStore();
        String scope = ConnectorMetadataCache.register("c3", 3600);
        ScopedCache<String, Set<DataFile>> files = new ScopedCache<>(store, scope, Kind.DATA_FILES);
        files.put("s3://bucket/m2.avro", Set.of(file("f1")));
        store.put(scope, Kind.PARTITIONS, "t1", "partitions");
        Assertions.assertEquals(1, files.estimatedSize());

        ConnectorMetadataCache.unregister(scope, Set.of(store));
        Assertions.assertNull(files.getIfPresent("s3://bucket/m2.avro"));
        Assertions.assertNull(store.getIfPresent(scope, Kind.PARTITIONS, "t1"));

        String again = ConnectorMetadataCache.register("c3", 3600);
        try {
            Assertions.assertNotEquals(scope, again);
            Assertions.assertNull(new ScopedCache<String, Set<DataFile>>(store, again, Kind.DATA_FILES)
                    .getIfPresent("s3://bucket/m2.avro"));
        } finally {
            ConnectorMetadataCache.unregister(again, Set.of(store));
        }
    }

    // A scope's TTL applies to its own entries only.
    @Test
    public void testTtlIsPerScope() {
        Store store = newStore();
        String shortLived = ConnectorMetadataCache.register("c4", 0);
        String longLived = ConnectorMetadataCache.register("c5", 3600);
        try {
            store.put(shortLived, Kind.PARTITIONS, "t1", "p");
            store.put(longLived, Kind.PARTITIONS, "t1", "p");
            store.cache().cleanUp();
            Assertions.assertNull(store.getIfPresent(shortLived, Kind.PARTITIONS, "t1"));
            Assertions.assertEquals("p", store.getIfPresent(longLived, Kind.PARTITIONS, "t1"));
        } finally {
            ConnectorMetadataCache.unregister(shortLived, Set.of(store));
            ConnectorMetadataCache.unregister(longLived, Set.of(store));
        }
    }

    // One budget for all catalogs: a shared store is bounded by its kind's global ratio of the heap, and a scope
    // reports only what its own entries weigh.
    @Test
    public void testOneBudgetAndPerScopeWeights() throws Exception {
        // the catalog registers the weighers of the kinds it caches
        Class.forName(CachingIcebergCatalog.class.getName());
        double old = Config.iceberg_data_file_cache_global_memory_ratio;
        Config.iceberg_data_file_cache_global_memory_ratio = 0.1;
        Store store = ConnectorMetadataCache.shared(Kind.DATA_FILES);
        Config.iceberg_data_file_cache_global_memory_ratio = old;
        Assertions.assertEquals(Math.round(Runtime.getRuntime().maxMemory() * 0.1), store.maximum());
        String a = ConnectorMetadataCache.register("c6", 3600);
        String b = ConnectorMetadataCache.register("c7", 3600);
        try {
            new ScopedCache<String, Set<DataFile>>(store, a, Kind.DATA_FILES).put("m1", Set.of(file("f1"), file("f2")));
            new ScopedCache<String, Set<DataFile>>(store, b, Kind.DATA_FILES).put("m1", Set.of(file("f1")));
            long weightA = store.weightedSize(a, EnumSet.of(Kind.DATA_FILES));
            long weightB = store.weightedSize(b, EnumSet.of(Kind.DATA_FILES));
            Assertions.assertTrue(weightA > 0, "weight " + weightA);
            Assertions.assertTrue(weightB > 0, "weight " + weightB);
            Assertions.assertTrue(store.totalWeightedSize() >= weightA + weightB);
            Assertions.assertEquals(0, store.weightedSize(a, EnumSet.of(Kind.DELETE_FILES)));
        } finally {
            ConnectorMetadataCache.unregister(a, Set.of(store));
            ConnectorMetadataCache.unregister(b, Set.of(store));
        }
    }

    // The per-scope statistics: one walk computes every scope's count, items and weight. A shared store reuses the
    // walk briefly, so a tracker pass over N catalogs walks it once; a private store is walked each time.
    @Test
    public void testStatsPerScope() throws Exception {
        Class.forName(CachingIcebergCatalog.class.getName());
        Store store = newStore();
        String a = ConnectorMetadataCache.register("c8", 3600);
        try {
            new ScopedCache<String, Set<DataFile>>(store, a, Kind.DATA_FILES).put("m1", Set.of(file("f1"), file("f2")));
            new ScopedCache<String, Set<DataFile>>(store, a, Kind.DATA_FILES).put("m2", Set.of(file("f3")));
            Stats stats = store.stats(a, Kind.DATA_FILES);
            Assertions.assertEquals(2, stats.entries());
            Assertions.assertEquals(3, stats.items());
            Assertions.assertEquals(store.weightedSize(a, EnumSet.of(Kind.DATA_FILES)), stats.weight());
            Assertions.assertEquals(0, store.stats(a, Kind.DELETE_FILES).entries());
            new ScopedCache<String, Set<DataFile>>(store, a, Kind.DATA_FILES).put("m3", Set.of(file("f4")));
            Assertions.assertEquals(3, store.stats(a, Kind.DATA_FILES).entries());
        } finally {
            ConnectorMetadataCache.unregister(a, Set.of(store));
        }
    }

    // Compatibility: with the global ratios at 0 (the default) each catalog keeps a private store per kind, sized by
    // its ratio properties as before (0 turns a kind off). A kind whose global ratio is above 0 goes to the store all
    // catalogs share, and the catalog's ratio property of that kind is ignored.
    @Test
    public void testPrivateByDefaultSharedWhenGlobalRatioSet() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CachingIcebergCatalog defaults = newCatalog(Map.of(), executor);
        CachingIcebergCatalog explicit = newCatalog(Map.of(
                IcebergCatalogProperties.ICEBERG_DATA_FILE_CACHE_MEMORY_SIZE_RATIO, "0.05",
                IcebergCatalogProperties.ICEBERG_DELETE_FILE_CACHE_MEMORY_SIZE_RATIO, "0"), executor);
        double old = Config.iceberg_partition_cache_global_memory_ratio;
        Config.iceberg_partition_cache_global_memory_ratio = 0.05;
        CachingIcebergCatalog sharedA = newCatalog(Map.of(), executor);
        CachingIcebergCatalog sharedB = newCatalog(Map.of(
                IcebergCatalogProperties.ICEBERG_PARTITION_CACHE_MEMORY_SIZE_RATIO, "0"), executor);
        Config.iceberg_partition_cache_global_memory_ratio = old;
        try {
            String[] fields = {"dataFileStore", "deleteFileStore", "deleteFileIndexStore", "partitionStore"};
            for (String field : fields) {
                Store store = Deencapsulation.getField(defaults, field);
                Assertions.assertEquals(Math.round(Runtime.getRuntime().maxMemory() * 0.1), store.maximum(), field);
                Assertions.assertNotSame(store, Deencapsulation.getField(explicit, field), field);
            }
            Store data = Deencapsulation.getField(explicit, "dataFileStore");
            Store deletes = Deencapsulation.getField(explicit, "deleteFileStore");
            Store deleteIndex = Deencapsulation.getField(explicit, "deleteFileIndexStore");
            Assertions.assertEquals(Math.round(Runtime.getRuntime().maxMemory() * 0.05), data.maximum());
            Assertions.assertEquals(0, deletes.maximum());
            Assertions.assertEquals(0, deleteIndex.maximum());
            Assertions.assertNotSame(deletes, deleteIndex);

            Store partitions = Deencapsulation.getField(sharedA, "partitionStore");
            Assertions.assertSame(partitions, Deencapsulation.getField(sharedB, "partitionStore"));
            Assertions.assertEquals(Math.round(Runtime.getRuntime().maxMemory() * 0.05), partitions.maximum());
            Assertions.assertNotSame(Deencapsulation.getField(sharedA, "dataFileStore"),
                    Deencapsulation.getField(sharedB, "dataFileStore"));
        } finally {
            defaults.shutdown();
            explicit.shutdown();
            sharedA.shutdown();
            sharedB.shutdown();
            executor.shutdownNow();
        }
    }

    // Invalidating a table's partition maps while one is still loading must not miss it: the load would otherwise
    // publish the pre-invalidation map with a fresh TTL.
    @Test
    public void testInvalidateWaitsForInFlightPartitionLoad() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(3);
        IcebergCatalog delegate = Mockito.mock(IcebergCatalog.class);
        Table table = Mockito.mock(Table.class);
        Mockito.when(delegate.getTable(Mockito.any(), Mockito.eq("db"), Mockito.eq("t"))).thenReturn(table);
        CountDownLatch loading = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Mockito.when(delegate.getPartitions(Mockito.any(), Mockito.anyLong(), Mockito.any())).thenAnswer(invocation -> {
            loading.countDown();
            release.await();
            return new HashMap<String, Partition>();
        });
        CachingIcebergCatalog catalog = newCatalog(Map.of(), executor, delegate);
        try {
            IcebergTable icebergTable = IcebergTable.builder().setCatalogDBName("db").setSrTableName("t")
                    .setCatalogTableName("t").setNativeTable(table).build();
            Future<?> load = executor.submit(() -> catalog.getPartitions(icebergTable, 1L, null));
            Assertions.assertTrue(loading.await(30, TimeUnit.SECONDS));
            Future<?> invalidate = executor.submit(() -> catalog.invalidatePartitionCache("db", "t"));
            Thread.sleep(100);
            release.countDown();
            load.get(30, TimeUnit.SECONDS);
            invalidate.get(30, TimeUnit.SECONDS);

            Store store = Deencapsulation.getField(catalog, "partitionStore");
            String scope = Deencapsulation.getField(catalog, "cacheScope");
            Assertions.assertNull(store.getIfPresent(scope, Kind.PARTITIONS,
                    new CachingIcebergCatalog.IcebergTableName("db", "t", 1L)));
        } finally {
            catalog.shutdown();
            executor.shutdownNow();
        }
    }

    // Dropping a table's partition maps drops every snapshot's map, and nothing else of the catalog.
    @Test
    public void testInvalidateDropsEverySnapshot() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        IcebergCatalog delegate = Mockito.mock(IcebergCatalog.class);
        Table table = Mockito.mock(Table.class);
        Mockito.when(delegate.getTable(Mockito.any(), Mockito.anyString(), Mockito.anyString())).thenReturn(table);
        Mockito.when(delegate.getPartitions(Mockito.any(), Mockito.anyLong(), Mockito.any()))
                .thenAnswer(invocation -> new HashMap<String, Partition>());
        CachingIcebergCatalog catalog = newCatalog(Map.of(), executor, delegate);
        try {
            IcebergTable t = IcebergTable.builder().setCatalogDBName("db").setSrTableName("t")
                    .setCatalogTableName("t").setNativeTable(table).build();
            IcebergTable other = IcebergTable.builder().setCatalogDBName("db").setSrTableName("t2")
                    .setCatalogTableName("t2").setNativeTable(table).build();
            catalog.getPartitions(t, 1L, null);
            catalog.getPartitions(t, 2L, null);
            catalog.getPartitions(other, 1L, null);
            catalog.invalidatePartitionCache("DB", "T");

            Store store = Deencapsulation.getField(catalog, "partitionStore");
            String scope = Deencapsulation.getField(catalog, "cacheScope");
            Assertions.assertNull(store.getIfPresent(scope, Kind.PARTITIONS,
                    new CachingIcebergCatalog.IcebergTableName("db", "t", 1L)));
            Assertions.assertNull(store.getIfPresent(scope, Kind.PARTITIONS,
                    new CachingIcebergCatalog.IcebergTableName("db", "t", 2L)));
            Assertions.assertNotNull(store.getIfPresent(scope, Kind.PARTITIONS,
                    new CachingIcebergCatalog.IcebergTableName("db", "t2", 1L)));
            Mockito.verify(delegate, Mockito.times(3)).getPartitions(Mockito.any(), Mockito.anyLong(), Mockito.any());
        } finally {
            catalog.shutdown();
            executor.shutdownNow();
        }
    }

    private static CachingIcebergCatalog newCatalog(Map<String, String> extra, ExecutorService executor) {
        return newCatalog(extra, executor, Mockito.mock(IcebergCatalog.class));
    }

    private static CachingIcebergCatalog newCatalog(Map<String, String> extra, ExecutorService executor,
                                                    IcebergCatalog delegate) {
        Map<String, String> properties = new HashMap<>();
        properties.put("iceberg.catalog.type", "hive");
        properties.put("hive.metastore.uris", "thrift://127.0.0.1:9083");
        properties.putAll(extra);
        return new CachingIcebergCatalog("c", delegate, new IcebergCatalogProperties(properties), executor);
    }
}
