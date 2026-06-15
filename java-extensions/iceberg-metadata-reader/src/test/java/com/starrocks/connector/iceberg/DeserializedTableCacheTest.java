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

import com.starrocks.connector.share.iceberg.SerializableTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializationUtil;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

public class DeserializedTableCacheTest {

    private static final int NO_EVICTION = 1024;

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get())
    );

    // #2 single-flight: many threads racing on the same key must trigger exactly one load and all
    // share the one resulting instance. The barrier + delayed loader widen the miss window so a
    // broken single-flight would actually be observed (multiple loads / distinct instances).
    @Test
    public void testSingleFlightDeserializesOncePerKey() throws Exception {
        DeserializedTableCache cache = new DeserializedTableCache(600, NO_EVICTION);
        int threads = 64;
        AtomicInteger loadCount = new AtomicInteger();
        CyclicBarrier gate = new CyclicBarrier(threads);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            List<Future<Table>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    gate.await();
                    return cache.get("same-payload", () -> {
                        loadCount.incrementAndGet();
                        Thread.sleep(20);
                        return makeTable();
                    });
                }));
            }
            Table first = futures.get(0).get(10, TimeUnit.SECONDS);
            for (Future<Table> future : futures) {
                assertSame(first, future.get(10, TimeUnit.SECONDS), "all callers must share one instance");
            }
            assertEquals(1, loadCount.get(), "loader must run exactly once per key");
        } finally {
            pool.shutdownNow();
        }
    }

    // #3 isolation: distinct payloads must not collide on the hashed key; a cache hit must not re-run
    // the loader and must return the instance stored for that exact payload.
    @Test
    public void testDistinctPayloadsAreIsolated() {
        DeserializedTableCache cache = new DeserializedTableCache(600, NO_EVICTION);
        Table a = cache.get("payload-A", DeserializedTableCacheTest::makeTable);
        Table b = cache.get("payload-B", DeserializedTableCacheTest::makeTable);
        assertNotSame(a, b);

        Table aHit = cache.get("payload-A", () -> {
            throw new AssertionError("loader must not run on a cache hit");
        });
        assertSame(a, aHit);
    }

    // #4 eviction: with maxEntries=1 a second distinct payload evicts the first; the next access to
    // the first reloads it (loader runs again) rather than returning a corrupt or stale handle.
    @Test
    public void testEntryReloadsAfterSizeEviction() {
        DeserializedTableCache cache = new DeserializedTableCache(600, 1);
        AtomicInteger keepLoads = new AtomicInteger();

        cache.get("keep", () -> {
            keepLoads.incrementAndGet();
            return makeTable();
        });
        // maxEntries=1, so inserting another distinct payload evicts "keep" (LRU)
        cache.get("other", DeserializedTableCacheTest::makeTable);
        cache.get("keep", () -> {
            keepLoads.incrementAndGet();
            return makeTable();
        });

        assertEquals(2, keepLoads.get(), "evicted entry must be reloaded on next access");
    }

    // public get() path: a real serialized payload is deserialized once and then served from cache.
    @Test
    public void testGetDeserializesAndCachesRealPayload() {
        String payload = SerializationUtil.serializeToBase64(makeTable());
        DeserializedTableCache cache = new DeserializedTableCache(600, NO_EVICTION);
        Table first = cache.get(payload);
        assertNotNull(first.schema());
        assertSame(first, cache.get(payload), "second get must hit the cache");
    }

    private static Table makeTable() {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", new HashMap<>());
        catalog.createNamespace(Namespace.of("db"));
        Table table = catalog.createTable(TableIdentifier.of("db", "tbl"), SCHEMA, PartitionSpec.unpartitioned());
        return new SerializableTable(table, new InMemoryFileIO());
    }
}
