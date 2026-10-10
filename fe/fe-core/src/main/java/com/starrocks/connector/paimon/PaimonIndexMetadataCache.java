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

package com.starrocks.connector.paimon;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.starrocks.connector.index.ConnectorIndexMetadata;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/** Connector-scoped cache for immutable, snapshot-bound index metadata. */
final class PaimonIndexMetadataCache {
    private static final long FAILURE_LOG_INTERVAL_MS = Duration.ofMinutes(1).toMillis();
    private final Cache<PaimonIndexMetadataCacheKey, ConnectorIndexMetadata> cache;
    // Query metadata instances share this connector-scoped limiter, just like the cache.
    private final AtomicLong lastFailureLogTimeMs = new AtomicLong(Long.MIN_VALUE);

    PaimonIndexMetadataCache(Duration ttl) {
        this(ttl, 1000L);
    }

    PaimonIndexMetadataCache(Duration ttl, long maximumSize) {
        if (maximumSize <= 0) {
            throw new IllegalArgumentException("maximumSize must be positive");
        }
        this.cache = Caffeine.newBuilder()
                .maximumSize(maximumSize)
                .expireAfterWrite(ttl)
                .build();
    }

    ConnectorIndexMetadata get(PaimonIndexMetadataCacheKey key, Supplier<ConnectorIndexMetadata> loader) {
        return cache.get(key, ignored -> loader.get());
    }

    boolean shouldLogFailure(long nowMs) {
        long previous = lastFailureLogTimeMs.get();
        if (previous != Long.MIN_VALUE && nowMs >= previous
                && nowMs - previous < FAILURE_LOG_INTERVAL_MS) {
            return false;
        }
        return lastFailureLogTimeMs.compareAndSet(previous, nowMs);
    }

    void invalidateTable(String catalogName, String databaseName, String tableName) {
        cache.asMap().keySet().removeIf(key -> key.belongsTo(catalogName, databaseName, tableName));
    }

    void invalidateAll() {
        cache.invalidateAll();
    }
}
