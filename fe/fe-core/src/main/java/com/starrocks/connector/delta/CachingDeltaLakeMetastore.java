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

package com.starrocks.connector.delta;


import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.connector.DatabaseTableName;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.metastore.CachingMetastore;
import com.starrocks.connector.metastore.IMetastore;
import com.starrocks.connector.metastore.MetastoreTable;
import com.starrocks.mysql.MysqlCommand;
import com.starrocks.qe.ConnectContext;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;

public class CachingDeltaLakeMetastore extends CachingMetastore implements IMetastore {
    private static final Logger LOG = LogManager.getLogger(CachingDeltaLakeMetastore.class);

    public final IMetastore delegate;
    private final Map<DatabaseTableName, Long> lastAccessTimeMap;

    public CachingDeltaLakeMetastore(IMetastore metastore, Executor executor, long expireAfterWriteSec,
                                     long refreshIntervalSec, long maxSize) {
        super(executor, expireAfterWriteSec, refreshIntervalSec, maxSize);
        this.delegate = metastore;
        this.lastAccessTimeMap = Maps.newConcurrentMap();
    }

    public static CachingDeltaLakeMetastore createQueryLevelInstance(IMetastore metastore, long perQueryCacheMaxSize) {
        return new CachingDeltaLakeMetastore(
                metastore,
                newDirectExecutorService(),
                NEVER_EVICT,
                NEVER_REFRESH,
                perQueryCacheMaxSize);
    }

    public static CachingDeltaLakeMetastore createCatalogLevelInstance(IMetastore metastore, Executor executor,
                                                                  long expireAfterWrite, long refreshInterval,
                                                                  long maxSize) {
        return new CachingDeltaLakeMetastore(metastore, executor, expireAfterWrite, refreshInterval, maxSize);
    }

    @Override
    public List<String> getAllDatabaseNames() {
        return get(databaseNamesCache, "");
    }

    @Override
    public List<String> loadAllDatabaseNames() {
        return delegate.getAllDatabaseNames();
    }

    @Override
    public List<String> getAllTableNames(String dbName) {
        return get(tableNamesCache, dbName);
    }

    @Override
    public List<String> loadAllTableNames(String dbName) {
        return delegate.getAllTableNames(dbName);
    }

    @Override
    public Database getDb(String dbName) {
        return get(databaseCache, dbName);
    }

    @Override
    public Database loadDb(String dbName) {
        return delegate.getDb(dbName);
    }

    @Override
    public Table loadTable(DatabaseTableName databaseTableName) {
        return delegate.getTable(databaseTableName.getDatabaseName(), databaseTableName.getTableName());
    }

    @Override
    public MetastoreTable getMetastoreTable(String dbName, String tableName) {
        return delegate.getMetastoreTable(dbName, tableName);
    }

    @Override
    public Table getTable(String dbName, String tableName) {
        if (ConnectContext.get() != null && ConnectContext.get().getCommand() == MysqlCommand.COM_QUERY) {
            DatabaseTableName databaseTableName = DatabaseTableName.of(dbName, tableName);
            lastAccessTimeMap.put(databaseTableName, System.currentTimeMillis());
        }
        return get(tableCache, DatabaseTableName.of(dbName, tableName));
    }

    @Override
    public List<String> getPartitionKeys(String dbName, String tableName) {
        // todo(Youngwb): cache partition keys
        return delegate.getPartitionKeys(dbName, tableName);
    }

    @Override
    public boolean tableExists(String dbName, String tableName) {
        return delegate.tableExists(dbName, tableName);
    }

    public void refreshTable(String dbName, String tblName, boolean onlyCachedPartitions) {
        DatabaseTableName databaseTableName = DatabaseTableName.of(dbName, tblName);
        tableNameLockMap.putIfAbsent(databaseTableName, dbName + "_" + tblName + "_lock");
        synchronized (tableNameLockMap.get(databaseTableName)) {
            Table newDeltaLakeTable;
            try {
                newDeltaLakeTable = loadTable(databaseTableName);
            } catch (StarRocksConnectorException e) {
                Throwable cause = e.getCause();
                if (cause instanceof InvocationTargetException &&
                        ((InvocationTargetException) cause).getTargetException() instanceof NoSuchObjectException) {
                    invalidateTable(dbName, tblName);
                    throw new StarRocksConnectorException(e.getMessage() + ", invalidated cache.");
                } else {
                    throw e;
                }
            }

            tableCache.put(databaseTableName, newDeltaLakeTable);
        }
    }

    public Set<DatabaseTableName> getCachedTableNames() {
        return lastAccessTimeMap.keySet();
    }

    public void refreshTableBackground(String dbName, String tblName) {
        DatabaseTableName databaseTableName = DatabaseTableName.of(dbName, tblName);
        if (lastAccessTimeMap.containsKey(databaseTableName)) {
            long lastAccessTime = lastAccessTimeMap.get(databaseTableName);
            long intervalSec = (System.currentTimeMillis() - lastAccessTime) / 1000;
            long refreshIntervalSinceLastAccess = Config.background_refresh_metadata_time_secs_since_last_access_secs;
            if (refreshIntervalSinceLastAccess >= 0 && intervalSec > refreshIntervalSinceLastAccess) {
                invalidateTable(dbName, tblName);
                lastAccessTimeMap.remove(databaseTableName);
                LOG.info("{}.{} skip refresh because of the last access time is {}", dbName, tblName,
                        LocalDateTime.ofInstant(Instant.ofEpochMilli(lastAccessTime), ZoneId.systemDefault()));
                return;
            }
        }
        refreshTable(dbName, tblName, true);
        Set<DatabaseTableName> cachedTableName = tableCache.asMap().keySet();
        lastAccessTimeMap.keySet().retainAll(cachedTableName);
        LOG.info("Refresh table {}.{} in background", dbName, tblName);
    }
}
