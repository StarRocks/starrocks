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

package com.starrocks.connector;

import com.google.common.util.concurrent.UncheckedExecutionException;
import com.starrocks.connector.ConnectorMetadataCache.Kind;
import com.starrocks.connector.ConnectorMetadataCache.ScopedGuavaCache;
import com.starrocks.connector.ConnectorMetadataCache.Store;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ConnectorMetadataCacheTest {

    // A miss loads once per key at a time, outside the store's compute, and a failure is reported as Guava does.
    @Test
    public void testGuavaViewLoadsOnceAndReportsFailures() throws Exception {
        Store store = ConnectorMetadataCache.newPrivate(1L << 30);
        String scope = ConnectorMetadataCache.register("d1", 3600);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            ScopedGuavaCache<String, List<String>> cache = new ScopedGuavaCache<>(store, scope, Kind.DELTA_JSON_LOGS);
            AtomicInteger loads = new AtomicInteger();
            CountDownLatch loading = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            Future<List<String>> first = executor.submit(() -> cache.get("f1", () -> {
                loads.incrementAndGet();
                loading.countDown();
                release.await();
                return List.of("row");
            }));
            Assertions.assertTrue(loading.await(30, TimeUnit.SECONDS));
            Future<List<String>> second = executor.submit(() -> cache.get("f1", () -> {
                loads.incrementAndGet();
                return List.of("other");
            }));
            // another key is not held up by the load in flight
            Assertions.assertEquals(List.of("x"), cache.get("f2", () -> List.of("x")));
            release.countDown();
            Assertions.assertEquals(List.of("row"), first.get(30, TimeUnit.SECONDS));
            Assertions.assertEquals(List.of("row"), second.get(30, TimeUnit.SECONDS));
            Assertions.assertEquals(1, loads.get());
            Assertions.assertEquals(2, cache.size());

            ExecutionException checked = Assertions.assertThrows(ExecutionException.class,
                    () -> cache.get("f3", () -> {
                        throw new IOException("boom");
                    }));
            Assertions.assertInstanceOf(IOException.class, checked.getCause());
            Assertions.assertThrows(UncheckedExecutionException.class, () -> cache.get("f3", () -> {
                throw new IllegalStateException("boom");
            }));
            Assertions.assertNull(cache.getIfPresent("f3"));

            cache.invalidateAll();
            Assertions.assertEquals(0, cache.size());
        } finally {
            ConnectorMetadataCache.unregister(scope, Set.of(store));
            executor.shutdownNow();
        }
    }

    // Callers waiting for a load share its failure instead of each loading again, the failure is not cached, and a
    // later call loads again.
    @Test
    public void testWaitersShareALoadFailure() throws Exception {
        Store store = ConnectorMetadataCache.newPrivate(1L << 30);
        String scope = ConnectorMetadataCache.register("d2", 3600);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            ScopedGuavaCache<String, List<String>> cache = new ScopedGuavaCache<>(store, scope, Kind.DELTA_JSON_LOGS);
            AtomicInteger loads = new AtomicInteger();
            CountDownLatch loading = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            Future<List<String>> first = executor.submit(() -> cache.get("f1", () -> {
                loads.incrementAndGet();
                loading.countDown();
                release.await();
                throw new IOException("remote failure");
            }));
            Assertions.assertTrue(loading.await(30, TimeUnit.SECONDS));
            Future<List<String>> second = executor.submit(() -> cache.get("f1", () -> {
                loads.incrementAndGet();
                return List.of("row");
            }));
            Thread.sleep(100);
            release.countDown();
            for (Future<List<String>> f : List.of(first, second)) {
                ExecutionException e = Assertions.assertThrows(ExecutionException.class,
                        () -> f.get(30, TimeUnit.SECONDS));
                Assertions.assertInstanceOf(ExecutionException.class, e.getCause());
                Assertions.assertInstanceOf(IOException.class, e.getCause().getCause());
            }
            Assertions.assertEquals(1, loads.get());
            Assertions.assertNull(cache.getIfPresent("f1"));
            Assertions.assertEquals(List.of("row"), cache.get("f1", () -> {
                loads.incrementAndGet();
                return List.of("row");
            }));
            Assertions.assertEquals(2, loads.get());
        } finally {
            ConnectorMetadataCache.unregister(scope, Set.of(store));
            executor.shutdownNow();
        }
    }

    // Invalidating a key while it loads drops the load: it does not publish afterwards.
    @Test
    public void testInvalidateDropsALoadInFlight() throws Exception {
        Store store = ConnectorMetadataCache.newPrivate(1L << 30);
        String scope = ConnectorMetadataCache.register("d3", 3600);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            ScopedGuavaCache<String, List<String>> cache = new ScopedGuavaCache<>(store, scope, Kind.DELTA_JSON_LOGS);
            CountDownLatch loading = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            Future<List<String>> load = executor.submit(() -> cache.get("f1", () -> {
                loading.countDown();
                release.await();
                return List.of("stale");
            }));
            Assertions.assertTrue(loading.await(30, TimeUnit.SECONDS));
            cache.invalidate("f1");
            release.countDown();
            Assertions.assertEquals(List.of("stale"), load.get(30, TimeUnit.SECONDS));
            store.cache().cleanUp();
            Assertions.assertNull(store.getIfPresent(scope, Kind.DELTA_JSON_LOGS, "f1"));
        } finally {
            ConnectorMetadataCache.unregister(scope, Set.of(store));
            executor.shutdownNow();
        }
    }
}
