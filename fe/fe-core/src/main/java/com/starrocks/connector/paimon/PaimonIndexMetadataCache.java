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
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DataTypes;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/** Connector-scoped cache for immutable, snapshot-bound index metadata. */
final class PaimonIndexMetadataCache {
    private static final long FAILURE_LOG_INTERVAL_MS = Duration.ofMinutes(1).toMillis();
    private final Cache<PaimonIndexMetadataCacheKey, ConnectorIndexMetadata> cache;
    private final Cache<SchemaKey, Set<Integer>> stableScalarFields;
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
        this.stableScalarFields = Caffeine.newBuilder()
                .maximumSize(maximumSize)
                .expireAfterWrite(ttl)
                .build();
    }

    private record SchemaKey(String catalog, String database, String table, String location,
                             String branch, long schemaId, long schemaTimeMillis) {
    }

    Set<Integer> getStableScalarFields(String catalog, String database, String table, FileStoreTable nativeTable) {
        TableSchema current = nativeTable.schema();
        SchemaKey key = new SchemaKey(catalog, database, table, nativeTable.location().toString(),
                org.apache.paimon.CoreOptions.branch(nativeTable.options()), current.id(), current.timeMillis());
        return stableScalarFields.get(key, ignored -> loadStableScalarFields(nativeTable, current));
    }

    // Paimon 2.0 BTree files do not record their key type/schema. Coverage alone is insufficient:
    // an index may have survived a type change before the snapshot that currently references it.
    // Conservatively require an unchanged encoding since each field's introduction. Rebuilt
    // indexes after a type change remain ineligible until file-level provenance is available.
    private static Set<Integer> loadStableScalarFields(FileStoreTable table, TableSchema current) {
        Map<Integer, DataType> encodings = new HashMap<>();
        current.fields().forEach(field -> encodings.put(field.id(), scalarEncoding(field.type())));
        Set<Integer> stable = new HashSet<>(encodings.keySet());
        for (long id = current.id() - 1; id >= 0 && !encodings.isEmpty(); id--) {
            TableSchema previous = table.schemaManager().schema(id);
            if (previous == null || previous.id() != id) {
                throw new IllegalStateException("Missing Paimon schema history at " + id);
            }
            var fields = previous.idToFieldMap();
            encodings.entrySet().removeIf(entry -> {
                var field = fields.get(entry.getKey());
                if (field == null) {
                    return true; // Stable IDs are never reused: the field did not exist yet.
                }
                if (!entry.getValue().equals(scalarEncoding(field.type()))) {
                    stable.remove(entry.getKey());
                    return true;
                }
                return false;
            });
        }
        return Set.copyOf(stable);
    }

    private static DataType scalarEncoding(DataType type) {
        if (type.getTypeRoot() == DataTypeRoot.CHAR || type.getTypeRoot() == DataTypeRoot.VARCHAR) {
            return DataTypes.STRING();
        }
        return type.copy(true);
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
        stableScalarFields.asMap().keySet().removeIf(key -> key.catalog().equals(catalogName)
                && key.database().equals(databaseName) && key.table().equals(tableName));
    }

    void invalidateAll() {
        cache.invalidateAll();
        stableScalarFields.invalidateAll();
    }
}
