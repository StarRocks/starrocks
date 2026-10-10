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

import com.github.benmanes.caffeine.cache.Cache;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.LazyConnector;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.starrocks.connector.iceberg.IcebergCatalogProperties.ICEBERG_CATALOG_TYPE;
import static com.starrocks.connector.iceberg.IcebergCatalogProperties.ICEBERG_DATA_FILE_CACHE_MEMORY_SIZE_RATIO;
import static com.starrocks.connector.iceberg.IcebergCatalogProperties.ICEBERG_DELETE_FILE_CACHE_MEMORY_SIZE_RATIO;
import static com.starrocks.connector.iceberg.IcebergCatalogProperties.ICEBERG_PARTITION_CACHE_MEMORY_SIZE_RATIO;
import static com.starrocks.connector.iceberg.IcebergCatalogProperties.ICEBERG_TABLE_CACHE_MEMORY_SIZE_RATIO;
import static org.mockito.Mockito.mock;

public class IcebergCacheMemoryUpdateTest {
    private static final List<String> LIMITS = List.of(ICEBERG_TABLE_CACHE_MEMORY_SIZE_RATIO,
            ICEBERG_PARTITION_CACHE_MEMORY_SIZE_RATIO, ICEBERG_DATA_FILE_CACHE_MEMORY_SIZE_RATIO,
            ICEBERG_DELETE_FILE_CACHE_MEMORY_SIZE_RATIO);

    @Test
    public void testResizePreservesEntriesAndEvictsAtZero() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Map<String, String> properties = new HashMap<>(Map.of(ICEBERG_CATALOG_TYPE, "hive"));
            CachingIcebergCatalog catalog = new CachingIcebergCatalog("resize", mock(IcebergCatalog.class),
                    new IcebergCatalogProperties(properties), executor);
            Cache<Object, Object> tables = Deencapsulation.getField(catalog, "tables");
            Cache<Object, Object> partitions = Deencapsulation.getField(catalog, "partitionCache");
            Cache<Object, Object> data = Deencapsulation.getField(catalog, "dataFileCache");
            Cache<Object, Object> deletes = Deencapsulation.getField(catalog, "deleteFileCache");
            Object tableKey = new CachingIcebergCatalog.IcebergTableName("db", "table");
            Table table = mock(Table.class);
            tables.put(tableKey, table);
            partitions.put(tableKey, Map.of());
            data.put("manifest", Set.of());
            deletes.put("delete-manifest", Set.of());
            List<Cache<Object, Object>> caches = List.of(tables, partitions, data, deletes);
            LIMITS.forEach(key -> properties.put(key, "0.2"));
            catalog.updateCacheMemoryLimits(new IcebergCatalogProperties(properties));
            for (Cache<Object, Object> cache : caches) {
                cache.cleanUp();
                Assertions.assertEquals(Math.round(Runtime.getRuntime().maxMemory() * 0.2),
                        cache.policy().eviction().orElseThrow().getMaximum());
                Assertions.assertEquals(1, cache.estimatedSize());
            }
            Assertions.assertSame(table, tables.getIfPresent(tableKey));
            LIMITS.forEach(key -> properties.put(key, "0"));
            catalog.updateCacheMemoryLimits(new IcebergCatalogProperties(properties));
            for (Cache<Object, Object> cache : caches) {
                cache.cleanUp();
                Assertions.assertEquals(0, cache.estimatedSize());
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testPrepareIsSideEffectFreeAndRejectsMixedUpdates() {
        // Resource-mapping connectors do not initialize the remote catalog in their constructor.
        IcebergConnector connector = new IcebergConnector(new ConnectorContext(
                "resource_mapping_inside_catalog_iceberg_resize", "iceberg", Map.of(ICEBERG_CATALOG_TYPE, "hive")));
        try {
            Runnable update = connector.preparePropertyUpdate(Map.of(LIMITS.get(0), "0.2"));
            IcebergCatalogProperties before = Deencapsulation.getField(connector, "icebergCatalogProperties");
            Assertions.assertEquals(0.1, before.getIcebergTableCacheMemoryUsageRatio());
            update.run();
            IcebergCatalogProperties after = Deencapsulation.getField(connector, "icebergCatalogProperties");
            Assertions.assertEquals(0.2, after.getIcebergTableCacheMemoryUsageRatio());
            Assertions.assertEquals(0.1, after.getIcebergDataFileCacheMemoryUsageRatio());
            Assertions.assertNull(connector.preparePropertyUpdate(Map.of(LIMITS.get(0), "0.3", "aws.s3.region", "x")));
            Assertions.assertNull(connector.preparePropertyUpdate(Map.of()));
            for (String invalid : List.of("-1", "1.01", "NaN", "Infinity", "invalid")) {
                Assertions.assertThrows(StarRocksConnectorException.class,
                        () -> connector.preparePropertyUpdate(Map.of(LIMITS.get(0), "0.4", LIMITS.get(1), invalid)));
                Assertions.assertEquals(0.2, after.getIcebergTableCacheMemoryUsageRatio());
            }
        } finally {
            connector.shutdown();
        }
    }

    @Test
    public void testInitializationBetweenPrepareAndApply() {
        ConnectorContext context = new ConnectorContext("resource_mapping_inside_catalog_iceberg_resize", "iceberg",
                Map.of(ICEBERG_CATALOG_TYPE, "hive"));
        LazyConnector lazy = new LazyConnector(context);
        Runnable update = lazy.preparePropertyUpdate(Map.of(LIMITS.get(0), "0.2"));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        IcebergConnector connector = new IcebergConnector(context);
        try {
            CachingIcebergCatalog cache = new CachingIcebergCatalog("resize", mock(IcebergCatalog.class),
                    new IcebergCatalogProperties(context.getProperties()), executor);
            Deencapsulation.setField(connector, "icebergNativeCatalog", cache);
            // Simulate another query initializing the delegate before the journal callback runs.
            Deencapsulation.setField(lazy, "delegate", connector);
            update.run();
            Assertions.assertSame(cache, connector.getNativeCatalog());
            Cache<?, ?> tables = Deencapsulation.getField(cache, "tables");
            Assertions.assertEquals(Math.round(Runtime.getRuntime().maxMemory() * 0.2),
                    tables.policy().eviction().orElseThrow().getMaximum());
        } finally {
            connector.shutdown();
            executor.shutdownNow();
        }
    }

    @Test
    public void testUninitializedLazyConnectorRemainsLazy() {
        LazyConnector connector = new LazyConnector(new ConnectorContext("lazy_resize", "iceberg",
                Map.of(ICEBERG_CATALOG_TYPE, "glue")));
        connector.preparePropertyUpdate(Map.of(LIMITS.get(0), "0.3")).run();
        Assertions.assertNull(Deencapsulation.getField(connector, "delegate"));
        ConnectorContext context = Deencapsulation.getField(connector, "context");
        Assertions.assertEquals("0.3", context.getProperties().get(LIMITS.get(0)));
        Assertions.assertNull(connector.preparePropertyUpdate(Map.of("aws.s3.region", "x")));
    }
}
