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
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.github.benmanes.caffeine.cache.Ticker;
import com.starrocks.utils.loader.ThreadContextClassLoader;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static org.apache.iceberg.util.SerializationUtil.deserializeFromBase64;

/**
 * Shares the deserialized Iceberg {@link Table} across the metadata scan tasks on one node.
 *
 * <p>Every scan task of one query gets the same {@code serialized_table} payload. Deserializing it per task costs
 * O(scan_threads * table_metadata) heap. The cache keys the table by the SHA-256 of the payload, so the tasks
 * share one table, and loads it once (single-flight). A cryptographic digest is used because a colliding key
 * would hand a task another table's metadata, credentials included.
 *
 * <p>The shared table brings a shared {@link FileIO} (an S3FileIO close shuts its S3 client), so no scanner
 * closes it. Each scanner holds a {@link Lease}; the cache closes the io once the entry has left the cache
 * and its last lease is released, exactly once.
 *
 * <p>The capacity bounds the cached entries, not the tables in use: a leased entry that is evicted lives on
 * until its leases are released. Idle entries expire after {@link #EXPIRE_AFTER_ACCESS} so a finished query
 * does not pin its metadata. Equal keys mean equal payloads, credentials included, so a cache hit never
 * gives a task other credentials than the ones the FE sent it.
 */
final class SharedTableCache {
    private static final Logger LOG = LogManager.getLogger(SharedTableCache.class);

    static final Duration EXPIRE_AFTER_ACCESS = Duration.ofSeconds(60);
    private static final int DIGEST_CHUNK_CHARS = 64 * 1024;

    /** A scanner's hold on a table. {@link #close()} releases it; for a shared table it never closes the io. */
    interface Lease extends AutoCloseable {
        Table table();

        @Override
        void close();
    }

    private static volatile SharedTableCache instance;

    private final Cache<String, Entry> cache;
    private final Function<String, Table> loader;
    private final ClassLoader classLoader;
    private final AtomicLong loads = new AtomicLong();
    private volatile long capacity;

    SharedTableCache(long capacity, Function<String, Table> loader, Ticker ticker, Executor executor,
                     Scheduler scheduler) {
        this.capacity = capacity;
        this.loader = loader;
        this.classLoader = SharedTableCache.class.getClassLoader();
        this.cache = Caffeine.newBuilder()
                .maximumSize(capacity)
                .expireAfterAccess(EXPIRE_AFTER_ACCESS)
                .ticker(ticker)
                .executor(executor)
                .scheduler(scheduler)
                .removalListener((String key, Entry entry, RemovalCause cause) -> {
                    if (entry != null) {
                        entry.markRemoved();
                    }
                })
                .build();
    }

    /**
     * Acquires the table of {@code serializedTable}. With {@code capacity <= 0} the caller gets a private table
     * whose io the lease closes, as before the cache existed.
     */
    static Lease acquire(String serializedTable, long capacity) {
        if (capacity <= 0) {
            return new PrivateLease(deserializeFromBase64(serializedTable));
        }
        return getInstance(capacity).acquire(serializedTable);
    }

    private static SharedTableCache getInstance(long capacity) {
        SharedTableCache cache = instance;
        if (cache == null) {
            synchronized (SharedTableCache.class) {
                cache = instance;
                if (cache == null) {
                    // Removal closes a FileIO under the reader's class loader. ForkJoinPool.commonPool() (Caffeine's
                    // default) may run innocuous workers that refuse setContextClassLoader, so run maintenance on
                    // the thread that triggers it: a scanner thread or the scheduler's delay thread.
                    cache = new SharedTableCache(capacity, payload -> deserializeFromBase64(payload),
                            Ticker.systemTicker(), Runnable::run, Scheduler.systemScheduler());
                    instance = cache;
                }
            }
        }
        cache.setCapacity(capacity);
        return cache;
    }

    Lease acquire(String serializedTable) {
        String key = sha256(serializedTable);
        while (true) {
            Entry entry = cache.get(key, k -> {
                loads.incrementAndGet();
                long start = System.nanoTime();
                Table table = loader.apply(serializedTable);
                LOG.info("Deserialized the shared iceberg table {} for metadata scans, payload {} chars, {} ms, key {}",
                        table.name(), serializedTable.length(), (System.nanoTime() - start) / 1_000_000, k);
                return new Entry(table);
            });
            // The entry may have been evicted and its io closed between the lookup and here; look up again.
            if (entry.tryAcquire()) {
                return new SharedLease(entry);
            }
        }
    }

    // The BE config is mutable, so every scanner passes the current value. The check and both updates run
    // under one lock: scanners carrying an old and a new value must not leave the recorded capacity and the
    // eviction maximum apart. The unlocked read keeps the common, unchanged case lock-free.
    void setCapacity(long newCapacity) {
        if (newCapacity == capacity) {
            return;
        }
        synchronized (this) {
            if (newCapacity != capacity) {
                capacity = newCapacity;
                cache.policy().eviction().ifPresent(eviction -> eviction.setMaximum(newCapacity));
            }
        }
    }

    long loadCount() {
        return loads.get();
    }

    long size() {
        cache.cleanUp();
        return cache.estimatedSize();
    }

    void invalidateAll() {
        cache.invalidateAll();
        cache.cleanUp();
    }

    static String sha256(String payload) {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
        // The payload is base64 (ASCII) and can be tens of MB: digest it in chunks instead of copying it whole.
        byte[] buffer = new byte[Math.min(DIGEST_CHUNK_CHARS, payload.length())];
        for (int start = 0; start < payload.length(); start += buffer.length) {
            int end = Math.min(start + buffer.length, payload.length());
            for (int i = start; i < end; i++) {
                buffer[i - start] = (byte) payload.charAt(i);
            }
            digest.update(buffer, 0, end - start);
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private void closeIO(Table table) {
        FileIO io = table.io();
        if (io == null) {
            return;
        }
        // Removal can run on a cache maintenance thread: close under the reader's class loader.
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            io.close();
        } catch (Exception e) {
            LOG.warn("Failed to close the FileIO of the shared iceberg table {}", table.name(), e);
        }
    }

    /** One cached table, its lease count, and whether it has left the cache. */
    private final class Entry {
        private final Table table;
        private int leases;
        private boolean removed;
        private boolean closed;

        Entry(Table table) {
            this.table = table;
        }

        boolean tryAcquire() {
            synchronized (this) {
                if (closed) {
                    return false;
                }
                leases++;
                return true;
            }
        }

        void release() {
            synchronized (this) {
                leases--;
                if (!removed || leases > 0) {
                    return;
                }
                closed = true;
            }
            closeIO(table);
        }

        void markRemoved() {
            synchronized (this) {
                removed = true;
                if (leases > 0 || closed) {
                    return;
                }
                closed = true;
            }
            closeIO(table);
        }
    }

    private static final class SharedLease implements Lease {
        private Entry entry;

        SharedLease(Entry entry) {
            this.entry = entry;
        }

        @Override
        public Table table() {
            return entry.table;
        }

        @Override
        public void close() {
            if (entry != null) {
                entry.release();
                entry = null;
            }
        }
    }

    private static final class PrivateLease implements Lease {
        private final Table table;
        private boolean closed;

        PrivateLease(Table table) {
            this.table = table;
        }

        @Override
        public Table table() {
            return table;
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                if (table.io() != null) {
                    table.io().close();
                }
            }
        }
    }
}
