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

package com.starrocks.connector.metastore;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.connector.DatabaseTableName;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.cache.CacheLoader.asyncReloading;
import static java.util.concurrent.TimeUnit.SECONDS;

public abstract class CachingMetastore {
    private static final Logger LOG = LogManager.getLogger(CachingMetastore.class);

    protected static final long NEVER_CACHE = 0;
    protected static final long NEVER_EVICT = -1;
    protected static final long NEVER_REFRESH = -1;

    // Used to synchronize the refreshTable process
    public final Map<DatabaseTableName, String> tableNameLockMap;
    protected LoadingCache<String, List<String>> databaseNamesCache;
    protected LoadingCache<String, List<String>> tableNamesCache;
    protected LoadingCache<String, Database> databaseCache;
    protected LoadingCache<DatabaseTableName, Table> tableCache;

    public CachingMetastore(Executor executor, long expireAfterWriteSec, long refreshIntervalSec, long maxSize) {
        databaseNamesCache = newCacheBuilder(NEVER_CACHE, NEVER_CACHE, NEVER_CACHE)
                .build(asyncReloading(CacheLoader.from(this::loadAllDatabaseNames), executor));
        tableNamesCache = newCacheBuilder(NEVER_CACHE, NEVER_CACHE, NEVER_CACHE)
                .build(asyncReloading(CacheLoader.from(this::loadAllTableNames), executor));
        databaseCache = newCacheBuilder(expireAfterWriteSec, refreshIntervalSec, maxSize)
                .build(asyncReloading(CacheLoader.from(this::loadDb), executor));
        tableCache = newCacheBuilder(expireAfterWriteSec, refreshIntervalSec, maxSize)
                .build(asyncReloading(CacheLoader.from(this::loadTable), executor));

        this.tableNameLockMap = Maps.newConcurrentMap();
    }

    protected static CacheBuilder<Object, Object> newCacheBuilder(long expiresAfterWriteSec, long refreshSec, long maximumSize) {
        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
        if (expiresAfterWriteSec >= 0) {
            cacheBuilder.expireAfterWrite(expiresAfterWriteSec, SECONDS);
        }

        if (refreshSec > 0 && expiresAfterWriteSec > refreshSec) {
            cacheBuilder.refreshAfterWrite(refreshSec, SECONDS);
        }

        cacheBuilder.maximumSize(maximumSize);
        return cacheBuilder;
    }

    protected abstract List<String> loadAllDatabaseNames();

    protected abstract List<String> loadAllTableNames(String dbName);

    protected abstract Database loadDb(String dbName);

    protected abstract Table loadTable(DatabaseTableName databaseTableName);

    protected static <K, V> V get(LoadingCache<K, V> cache, K key) {
        try {
            return cache.getUnchecked(key);
        } catch (UncheckedExecutionException e) {
            LOG.error("Error occurred when loading cache", e);
            throwIfInstanceOf(e.getCause(), StarRocksConnectorException.class);
            throw e;
        }
    }

    public boolean isTablePresent(DatabaseTableName tableName) {
        return tableCache.getIfPresent(tableName) != null;
    }

    public synchronized void invalidateAll() {
        databaseNamesCache.invalidateAll();
        databaseCache.invalidateAll();
        tableNamesCache.invalidateAll();
        tableCache.invalidateAll();
    }

    public synchronized void invalidateTable(String dbName, String tableName) {
        DatabaseTableName databaseTableName = DatabaseTableName.of(dbName, tableName);
        tableCache.invalidate(databaseTableName);
    }
}
