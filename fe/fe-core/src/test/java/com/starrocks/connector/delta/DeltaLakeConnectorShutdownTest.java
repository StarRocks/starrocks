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

package com.starrocks.connector.delta;

import com.google.common.cache.Cache;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadataCache;
import io.delta.kernel.utils.FileStatus;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DeltaLakeConnectorShutdownTest {
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testShutdownCancelsPrefetchAndReleasesCacheScopes(boolean tableCacheEnabled) throws Exception {
        DeltaLakeConnector connector = new DeltaLakeConnector(new ConnectorContext("delta_shutdown", "deltalake", Map.of(
                "type", "deltalake", "hive.metastore.type", "hive", "hive.metastore.uris", "thrift://localhost:9083",
                DeltaLakeCatalogProperties.ENABLE_DELTA_LAKE_TABLE_CACHE, Boolean.toString(tableCacheEnabled))));
        DeltaLakeMetadataFactory factory = Deencapsulation.getField(connector, "metadataFactory");
        ThreadPoolExecutor executor = Deencapsulation.getField(factory, "prefetchExecutor");
        IDeltaLakeMetastore metastore = Deencapsulation.getField(connector, "metastore");
        if (metastore instanceof CachingDeltaLakeMetastore caching) {
            metastore = caching.delegate;
        }
        String jsonScope = Deencapsulation.getField(metastore, "jsonScope");
        String checkpointScope = Deencapsulation.getField(metastore, "checkpointScope");
        Field scopes = ConnectorMetadataCache.class.getDeclaredField("SCOPE_TTL_SEC");
        scopes.setAccessible(true);
        Map<?, ?> registeredScopes = (Map<?, ?>) scopes.get(null);
        Cache<DeltaLakeFileStatus, List<?>> jsonCache = Deencapsulation.getField(metastore, "jsonCache");
        DeltaLakeFileStatus key = DeltaLakeFileStatus.of(FileStatus.of("s3://bucket/log.json", 1, 1));
        CountDownLatch started = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(2);
        AtomicBoolean queuedTaskRan = new AtomicBoolean();
        try {
            jsonCache.put(key, List.of());
            assertNotNull(jsonCache.getIfPresent(key));
            assertTrue(registeredScopes.containsKey(jsonScope));
            assertTrue(registeredScopes.containsKey(checkpointScope));
            // Occupy both workers so the third future is queued when the catalog is dropped.
            for (int i = 0; i < 2; i++) {
                executor.submit(() -> {
                    started.countDown();
                    try {
                        release.await();
                    } catch (InterruptedException e) {
                        interrupted.countDown();
                        Thread.currentThread().interrupt();
                    }
                });
            }
            assertTrue(started.await(5, TimeUnit.SECONDS));
            Future<?> queued = executor.submit(() -> queuedTaskRan.set(true));
            connector.shutdown();
            assertTrue(executor.isShutdown());
            assertTrue(queued.isCancelled());
            assertThrows(CancellationException.class, () -> queued.get(5, TimeUnit.SECONDS));
            assertTrue(interrupted.await(5, TimeUnit.SECONDS));
            assertFalse(queuedTaskRan.get());
            assertNull(jsonCache.getIfPresent(key));
            assertFalse(registeredScopes.containsKey(jsonScope));
            assertFalse(registeredScopes.containsKey(checkpointScope));
        } finally {
            release.countDown();
            connector.shutdown();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }
}
