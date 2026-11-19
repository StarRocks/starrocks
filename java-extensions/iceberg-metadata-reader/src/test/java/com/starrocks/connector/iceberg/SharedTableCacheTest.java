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

import com.github.benmanes.caffeine.cache.Scheduler;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SharedTableCacheTest {

    private static final class CountingIO implements FileIO {
        final AtomicInteger closes = new AtomicInteger();

        @Override
        public InputFile newInputFile(String path) {
            throw new UnsupportedOperationException();
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
        public void close() {
            closes.incrementAndGet();
        }
    }

    private static Table newTable(String name, FileIO io) {
        return (Table) Proxy.newProxyInstance(Table.class.getClassLoader(), new Class<?>[] {Table.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "io":
                            return io;
                        case "name":
                        case "toString":
                            return name;
                        case "hashCode":
                            return System.identityHashCode(proxy);
                        case "equals":
                            return proxy == args[0];
                        default:
                            throw new UnsupportedOperationException(method.getName());
                    }
                });
    }

    /** Loader that builds one table, with its own io, per call, and remembers them. */
    private static final class Loader implements Function<String, Table> {
        final List<CountingIO> ios = new ArrayList<>();

        @Override
        public synchronized Table apply(String payload) {
            CountingIO io = new CountingIO();
            ios.add(io);
            return newTable(payload, io);
        }
    }

    private static final class FakeTicker implements com.github.benmanes.caffeine.cache.Ticker {
        final AtomicLong nanos = new AtomicLong();

        @Override
        public long read() {
            return nanos.get();
        }
    }

    private static SharedTableCache newCache(long capacity, Function<String, Table> loader, FakeTicker ticker) {
        return new SharedTableCache(capacity, loader, ticker, Runnable::run, Scheduler.disabledScheduler());
    }

    @Test
    public void testSamePayloadLoadsOnceAndSharesTable() {
        Loader loader = new Loader();
        SharedTableCache cache = newCache(4, loader, new FakeTicker());
        try (SharedTableCache.Lease a = cache.acquire("p1"); SharedTableCache.Lease b = cache.acquire("p1")) {
            assertSame(a.table(), b.table());
            assertEquals(1, cache.loadCount());
        }
        try (SharedTableCache.Lease c = cache.acquire("p1")) {
            assertEquals(1, cache.loadCount());
        }
        assertEquals(0, loader.ios.get(0).closes.get(), "a cached table keeps its io open");
    }

    @Test
    public void testDifferentPayloadsGetDifferentTables() {
        Loader loader = new Loader();
        SharedTableCache cache = newCache(4, loader, new FakeTicker());
        try (SharedTableCache.Lease a = cache.acquire("p1"); SharedTableCache.Lease b = cache.acquire("p2")) {
            assertNotSame(a.table(), b.table());
            assertEquals("p1", a.table().name());
            assertEquals("p2", b.table().name());
            assertEquals(2, cache.loadCount());
        }
    }

    @Test
    public void testReleasingOneLeaseKeepsIOForOthers() {
        Loader loader = new Loader();
        SharedTableCache cache = newCache(4, loader, new FakeTicker());
        SharedTableCache.Lease a = cache.acquire("p1");
        SharedTableCache.Lease b = cache.acquire("p1");
        a.close();
        a.close();
        assertEquals(0, loader.ios.get(0).closes.get());
        assertSame(loader.ios.get(0), b.table().io());
        b.close();
        assertEquals(0, loader.ios.get(0).closes.get());
    }

    @Test
    public void testEvictedEntryClosesIOAfterLastLeaseOnlyOnce() {
        Loader loader = new Loader();
        SharedTableCache cache = newCache(4, loader, new FakeTicker());
        SharedTableCache.Lease a = cache.acquire("p1");
        SharedTableCache.Lease b = cache.acquire("p1");
        cache.invalidateAll();
        CountingIO io = loader.ios.get(0);
        assertEquals(0, io.closes.get(), "an evicted table still in use keeps its io open");
        a.close();
        assertEquals(0, io.closes.get());
        b.close();
        assertEquals(1, io.closes.get());
        b.close();
        assertEquals(1, io.closes.get());

        // The next task of the same payload loads a fresh table instead of the closed one.
        try (SharedTableCache.Lease c = cache.acquire("p1")) {
            assertEquals(2, cache.loadCount());
            assertSame(loader.ios.get(1), c.table().io());
        }
    }

    @Test
    public void testEvictedIdleEntryClosesIOImmediately() {
        Loader loader = new Loader();
        SharedTableCache cache = newCache(4, loader, new FakeTicker());
        cache.acquire("p1").close();
        cache.invalidateAll();
        assertEquals(1, loader.ios.get(0).closes.get());
    }

    @Test
    public void testCapacityEvictsAndCanShrink() {
        Loader loader = new Loader();
        SharedTableCache cache = newCache(2, loader, new FakeTicker());
        for (String payload : new String[] {"p1", "p2", "p3"}) {
            cache.acquire(payload).close();
        }
        assertEquals(2, cache.size());
        assertEquals(1, loader.ios.stream().mapToInt(io -> io.closes.get()).sum());

        cache.setCapacity(1);
        assertEquals(1, cache.size());
        assertEquals(2, loader.ios.stream().mapToInt(io -> io.closes.get()).sum());
    }

    @Test
    public void testIdleEntryExpires() {
        Loader loader = new Loader();
        FakeTicker ticker = new FakeTicker();
        SharedTableCache cache = newCache(4, loader, ticker);
        cache.acquire("p1").close();
        ticker.nanos.addAndGet(SharedTableCache.EXPIRE_AFTER_ACCESS.toNanos() + 1);
        assertEquals(0, cache.size());
        assertEquals(1, loader.ios.get(0).closes.get());
    }

    @Test
    public void testConcurrentAcquireLoadsOnce() throws Exception {
        int threads = 16;
        CountDownLatch loading = new CountDownLatch(1);
        Loader inner = new Loader();
        Function<String, Table> slowLoader = payload -> {
            loading.countDown();
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return inner.apply(payload);
        };
        SharedTableCache cache = newCache(4, slowLoader, new FakeTicker());
        CyclicBarrier gate = new CyclicBarrier(threads);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            List<Future<Table>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    gate.await();
                    try (SharedTableCache.Lease lease = cache.acquire("p1")) {
                        return lease.table();
                    }
                }));
            }
            Table first = futures.get(0).get(10, TimeUnit.SECONDS);
            for (Future<Table> future : futures) {
                assertSame(first, future.get(10, TimeUnit.SECONDS));
            }
            assertEquals(1, cache.loadCount());
            assertEquals(0, inner.ios.get(0).closes.get());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    public void testFailedLoadIsNotCached() {
        AtomicInteger calls = new AtomicInteger();
        Loader inner = new Loader();
        SharedTableCache cache = newCache(4, payload -> {
            if (calls.incrementAndGet() == 1) {
                throw new IllegalStateException("bad payload");
            }
            return inner.apply(payload);
        }, new FakeTicker());
        assertThrows(IllegalStateException.class, () -> cache.acquire("p1"));
        try (SharedTableCache.Lease lease = cache.acquire("p1")) {
            assertEquals("p1", lease.table().name());
        }
    }

    @Test
    public void testSha256MatchesUtf8Digest() throws Exception {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 200_000; i++) {
            sb.append((char) ('A' + i % 26));
        }
        for (String payload : new String[] {"", "abc", sb.toString()}) {
            byte[] expected = MessageDigest.getInstance("SHA-256").digest(payload.getBytes(StandardCharsets.UTF_8));
            assertEquals(HexFormat.of().formatHex(expected), SharedTableCache.sha256(payload));
        }
    }
}
