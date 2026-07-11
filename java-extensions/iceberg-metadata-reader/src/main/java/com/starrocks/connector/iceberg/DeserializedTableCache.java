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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import org.apache.iceberg.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.iceberg.util.SerializationUtil.deserializeFromBase64;

/**
 * Shares the deserialized Iceberg {@link Table} across scan tasks of the same table on one node.
 *
 * <p>Without this cache every metadata scan task deserializes its own private copy of the full
 * table from the base64 {@code serialized_table} payload, so heap cost is
 * O(scan_thread_pool_size * table_metadata_size). The cache keys by the payload so identical
 * payloads (same table, same snapshot, same credentials) resolve to a single shared instance,
 * reducing that to roughly O(distinct_concurrent_tables * table_metadata_size).
 *
 * <p>Keying: a SHA-256 over the payload string. The payload is the only thing available before
 * deserialization, and a cryptographic digest is required because a colliding key would silently
 * return a different table's metadata. A cheap {@code hashCode} is not acceptable here.
 *
 * <p>Bounding: by entry count, not by serialized size. One entry holds one table, and all scan
 * tasks of the same table on a node share that single payload, so the entry count equals the
 * number of distinct tables scanned concurrently — a handful, not the scan-pool size. A byte
 * weight cannot be used honestly: the dominant retained cost is the lazily materialized full
 * {@code TableMetadata} (populated only when a scanner calls {@code snapshot()}/{@code history()}),
 * which is absent from the payload and unknown at insertion time. {@code maximumSize} therefore
 * caps worst-case retention at maxEntries * per-table-metadata, the only honest bound available.
 *
 * <p>When concurrent distinct tables exceed {@code maxEntries}, an active table can be size-evicted
 * and re-deserialized by its later tasks (correctness is unaffected, but the duplicate copy and the
 * repeated parse are wasted). That is logged so the operator can raise the bound; see the
 * size-eviction warning below.
 *
 * <p>Expiry uses {@code expireAfterWrite} (not {@code expireAfterAccess}) because the table carries
 * a {@code FileIO} bound to credentials that can expire; the TTL matches the FileIO cache.
 */
public class DeserializedTableCache {
    private static final Logger LOG = LoggerFactory.getLogger(DeserializedTableCache.class);

    // Throttle: warn on the first size-eviction (early signal) and then once per this many.
    private static final long EVICTION_LOG_INTERVAL = 100;

    private final Cache<String, Table> cache;
    private final int maxEntries;
    private final AtomicLong sizeEvictions = new AtomicLong();

    public DeserializedTableCache(long expireSeconds, int maxEntries) {
        this.maxEntries = maxEntries;
        this.cache = CacheBuilder.newBuilder()
                // single segment so maximumSize is an exact bound rather than an approximate
                // per-segment split; safe here because the cache is cold-path (a few distinct
                // tables) and loads run off-lock
                .concurrencyLevel(1)
                .expireAfterWrite(expireSeconds, TimeUnit.SECONDS)
                .maximumSize(maxEntries)
                .removalListener((RemovalListener<String, Table>) notification -> {
                    if (notification.getCause() == RemovalCause.SIZE) {
                        onSizeEviction();
                    }
                })
                .build();
    }

    public Table get(String serializedTable) {
        return get(serializedTable, () -> deserializeFromBase64(serializedTable));
    }

    // Single-flight: exactly one loader call runs per key; concurrent callers of the same key block
    // on it and share the result. The loader seam lets tests count and delay deserialization.
    Table get(String payload, Callable<Table> loader) {
        String cacheKey = sha256Hex(payload);
        try {
            return cache.get(cacheKey, () -> {
                LOG.debug("DeserializedTableCache miss for key {}", cacheKey);
                return loader.call();
            });
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new RuntimeException("Failed to deserialize iceberg table", cause);
        }
    }

    private void onSizeEviction() {
        long count = sizeEvictions.incrementAndGet();
        if (count == 1 || count % EVICTION_LOG_INTERVAL == 0) {
            LOG.warn("DeserializedTableCache evicted {} entries under size pressure (maxEntries={}). "
                            + "Concurrent distinct iceberg metadata tables exceed the cache, so some are being "
                            + "re-deserialized; raise iceberg_metadata_table_cache_capacity if this keeps growing.",
                    count, maxEntries);
        }
    }

    private static String sha256Hex(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(bytes.length * 2);
            for (byte b : bytes) {
                sb.append(Character.forDigit((b >> 4) & 0xf, 16));
                sb.append(Character.forDigit(b & 0xf, 16));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }
}
