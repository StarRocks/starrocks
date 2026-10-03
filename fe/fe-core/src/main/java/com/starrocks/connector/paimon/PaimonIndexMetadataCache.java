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
import java.util.function.Supplier;

/** Connector-scoped cache for immutable, snapshot-bound index metadata. */
final class PaimonIndexMetadataCache {
    private final Cache<PaimonIndexMetadataCacheKey, ConnectorIndexMetadata> cache;

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

    void invalidateTable(String catalogName, String databaseName, String tableName) {
        cache.asMap().keySet().removeIf(key -> key.belongsTo(catalogName, databaseName, tableName));
    }

    void invalidateAll() {
        cache.invalidateAll();
    }
}
