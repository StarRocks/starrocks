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

package com.starrocks.connector.jdbc;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.cache.CacheLoader.asyncReloading;
import static java.util.concurrent.TimeUnit.SECONDS;

public class CachingJDBCMetadata implements ConnectorMetadata {
    private static final Logger LOG = LogManager.getLogger(CachingJDBCMetadata.class);

    private static final long NEVER_REFRESH = -1;

    private final JDBCMetadata jdbcMetadata;

    private final LoadingCache<String, List<String>> databaseNamesCache;

    private final LoadingCache<String, List<String>> tableNamesCache;

    private final LoadingCache<String, Database> databaseCache;

    private final LoadingCache<JDBCTableName, Table> tableCache;

    public CachingJDBCMetadata(JDBCMetadata jdbcMetadata, Executor executor, long expireAfterWriteSec,
                               long refreshIntervalSec, long maxSize) {
        this.jdbcMetadata = jdbcMetadata;

        databaseNamesCache = newCacheBuilder(expireAfterWriteSec, NEVER_REFRESH, maxSize)
                .build(asyncReloading(CacheLoader.from(this::loadDbNames), executor));

        tableNamesCache = newCacheBuilder(expireAfterWriteSec, refreshIntervalSec, maxSize)
                .build(asyncReloading(CacheLoader.from(this::loadTableNames), executor));

        databaseCache = newCacheBuilder(expireAfterWriteSec, refreshIntervalSec, maxSize)
                .build(asyncReloading(CacheLoader.from(this::loadDb), executor));

        tableCache = newCacheBuilder(expireAfterWriteSec, refreshIntervalSec, maxSize)
                .build(asyncReloading(CacheLoader.from(this::loadTable), executor));
    }

    @Override
    public List<String> listDbNames() {
        return get(databaseNamesCache, "");
    }

    private List<String> loadDbNames() {
        return jdbcMetadata.listDbNames();
    }

    @Override
    public Database getDb(String name) {
        return get(databaseCache, name);
    }

    private Database loadDb(String name) {
        return jdbcMetadata.getDb(name);
    }

    @Override
    public List<String> listTableNames(String dbName) {
        return get(tableNamesCache, dbName);
    }

    private List<String> loadTableNames(String dbName) {
        return jdbcMetadata.listTableNames(dbName);
    }

    @Override
    public Table getTable(String dbName, String tblName) {
        return get(tableCache, JDBCTableName.of(dbName, tblName));
    }

    private Table loadTable(JDBCTableName tableName) {
        return jdbcMetadata.getTable(tableName.getDatabaseName(), tableName.getTableName());
    }

    @Override
    public List<String> listPartitionNames(String databaseName, String tableName) {
        return jdbcMetadata.listPartitionNames(databaseName, tableName);
    }

    @Override
    public List<PartitionInfo> getPartitions(Table table, List<String> partitionNames) {
        return jdbcMetadata.getPartitions(table, partitionNames);
    }

    private static <K, V> V get(LoadingCache<K, V> cache, K key) {
        try {
            return cache.getUnchecked(key);
        } catch (UncheckedExecutionException e) {
            LOG.error("Error occurred when loading cache", e);
            throwIfInstanceOf(e.getCause(), StarRocksConnectorException.class);
            throw e;
        }
    }

    private static CacheBuilder<Object, Object> newCacheBuilder(long expiresAfterWriteSec, long refreshSec,
                                                                long maximumSize) {
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

    static class JDBCTableName {
        private final String databaseName;
        private final String tableName;

        public JDBCTableName(String databaseName, String tableName) {
            this.databaseName = databaseName;
            this.tableName = tableName;
        }

        public static JDBCTableName of(String databaseName, String tableName) {
            return new JDBCTableName(databaseName, tableName);
        }

        public String getDatabaseName() {
            return databaseName;
        }

        public String getTableName() {
            return tableName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            JDBCTableName that = (JDBCTableName) o;
            return Objects.equals(databaseName, that.databaseName) &&
                    Objects.equals(tableName, that.tableName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(databaseName, tableName);
        }

        @Override
        public String toString() {
            return "JDBCTableName{" +
                    "databaseName='" + databaseName + '\'' +
                    ", tableName='" + tableName + '\'' +
                    '}';
        }
    }

}
