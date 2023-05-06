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

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.starrocks.catalog.Database;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.connector.ConnectorColumnStatisticLoader;
import com.starrocks.connector.ConnectorTableColumnKey;
import com.starrocks.connector.ConnectorTableColumnStat;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class CachingIcebergCatalog implements IcebergCatalog {
    private static final Logger LOG = LogManager.getLogger(CachingIcebergCatalog.class);
    private final int icebergMetaCacheSize = 5000;
    private final long icebergMetaCacheTtls = 3600L * 24L;
    private IcebergCatalog delegate;
    private final AsyncLoadingCache<ConnectorTableColumnKey, Optional<ConnectorTableColumnStat>> cacheTableStats;

    public CachingIcebergCatalog(IcebergCatalog icebergCatalog, Map<String, String> properties) {
        this.delegate = icebergCatalog;
        int size = Integer.parseInt(properties.getOrDefault("iceberg_meta_cache_size",
                String.valueOf(icebergMetaCacheSize)));
        long ttl = Long.parseLong(properties.getOrDefault("iceberg_meta_cache_ttl_s",
                String.valueOf(icebergMetaCacheTtls)));
        cacheTableStats = Caffeine.newBuilder().maximumSize(size)
                .expireAfterAccess(ttl, TimeUnit.SECONDS)
                .buildAsync(new ConnectorColumnStatisticLoader());
    }

    @Override
    public IcebergCatalogType getIcebergCatalogType() {
        return delegate.getIcebergCatalogType();
    }

    @Override
    public List<String> listAllDatabases() {
        return delegate.listAllDatabases();
    }

    @Override
    public void createDb(String dbName, Map<String, String> properties) {
        delegate.createDb(dbName, properties);
    }

    @Override
    public void dropDb(String dbName) throws MetaNotFoundException {
        delegate.dropDb(dbName);
    }

    @Override
    public Database getDB(String dbName) {
        return delegate.getDB(dbName);
    }

    @Override
    public List<String> listTables(String dbName) {
        return delegate.listTables(dbName);
    }

    @Override
    public boolean createTable(String dbName, String tableName, Schema schema, PartitionSpec partitionSpec,
                               String location, Map<String, String> properties) {
        return delegate.createTable(dbName, tableName, schema, partitionSpec, location, properties);
    }

    @Override
    public boolean dropTable(String dbName, String tableName, boolean purge) {
        return delegate.dropTable(dbName, tableName, purge);
    }

    @Override
    public Table getTable(String dbName, String tableName) throws StarRocksConnectorException {
        return delegate.getTable(dbName, tableName);
    }

    @Override
    public void deleteUncommittedDataFiles(List<String> fileLocations) {
        delegate.deleteUncommittedDataFiles(fileLocations);
    }

    public List<Optional<ConnectorTableColumnStat>> getColumnStatistics(String tableUUID, List<String> columns) {
        List<ConnectorTableColumnKey> keys = new ArrayList<>();
        for (String column : columns) {
            keys.add(new ConnectorTableColumnKey(tableUUID, column));
        }
        try {
            CompletableFuture<Map<ConnectorTableColumnKey, Optional<ConnectorTableColumnStat>>> result =
                    cacheTableStats.getAll(keys);
            if (result.isDone()) {
                List<Optional<ConnectorTableColumnStat>> columnStatistics = new ArrayList<>();
                Map<ConnectorTableColumnKey, Optional<ConnectorTableColumnStat>> realResult = result.get();
                for (String column : columns) {
                    Optional<ConnectorTableColumnStat> columnStatistic =
                            realResult.getOrDefault(new ConnectorTableColumnKey(tableUUID, column), Optional.empty());
                    columnStatistics.add(columnStatistic);
                }
                return columnStatistics;
            } else {
                return getDefaultColumnStatisticList(columns);
            }
        } catch (Exception e) {
            LOG.warn("Get column statistics error of tableUUID : {}, Columns : {}, errMsg: {}",
                    tableUUID, columns.toString(), e.getMessage());
            return getDefaultColumnStatisticList(columns);
        }
    }

    private List<Optional<ConnectorTableColumnStat>> getDefaultColumnStatisticList(List<String> columns) {
        List<Optional<ConnectorTableColumnStat>> columnStatisticList = new ArrayList<>();
        for (int i = 0; i < columns.size(); ++i) {
            columnStatisticList.add(Optional.empty());
        }
        return columnStatisticList;
    }

    public void invalidateCachedTableStat(String tableUUID, List<String> columns) {
        if (columns == null) {
            return;
        }
        List<ConnectorTableColumnKey> allKeys = new ArrayList<>();
        for (String column : columns) {
            ConnectorTableColumnKey key = new ConnectorTableColumnKey(tableUUID, column);
            allKeys.add(key);
        }
        cacheTableStats.synchronous().invalidateAll(allKeys);
    }
}
