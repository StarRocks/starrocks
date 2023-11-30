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
import com.starrocks.catalog.Database;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.concurrent.TimeUnit.SECONDS;

public class CachingIcebergCatalog implements IcebergCatalog {
    private static final Logger LOG = LogManager.getLogger(CachingIcebergCatalog.class);
    private final IcebergCatalog delegate;
    private final Cache<TableIdentifier, Table> tables;
    private final Map<String, Database> databases = new ConcurrentHashMap<>();
    private final Map<TableIdentifier, List<String>> partitionNames = new ConcurrentHashMap<>();

    public CachingIcebergCatalog(IcebergCatalog delegate, long ttlSec) {
        this.delegate = delegate;
        this.tables = CacheBuilder.newBuilder().expireAfterWrite(ttlSec, SECONDS).maximumSize(100000).build();
    }

    @Override
    public IcebergCatalogType getIcebergCatalogType() {
        return delegate.getIcebergCatalogType();
    }

    @Override
    public List<String> listAllDatabases() {
        return delegate.listAllDatabases();
    }

    public void createDb(String dbName, Map<String, String> properties) {
        delegate.createDb(dbName, properties);
    }

    public void dropDb(String dbName) throws MetaNotFoundException {
        delegate.dropDb(dbName);
        databases.remove(dbName);
    }

    @Override
    public Database getDB(String dbName) {
        if (databases.containsKey(dbName)) {
            return databases.get(dbName);
        }
        Database db;
        try {
            db = delegate.getDB(dbName);
        } catch (NoSuchNamespaceException e) {
            LOG.error("Database {} not found", dbName, e);
            return null;
        }

        databases.put(dbName, db);
        return db;
    }

    @Override
    public List<String> listTables(String dbName) {
        return delegate.listTables(dbName);
    }

    @Override
    public Table getTable(String dbName, String tableName) throws StarRocksConnectorException {
        TableIdentifier identifier = TableIdentifier.of(dbName, tableName);
        if (tables.getIfPresent(identifier) != null) {
            return tables.getIfPresent(identifier);
        }

        try {
            Table icebergTable = delegate.getTable(dbName, tableName);
            tables.put(identifier, icebergTable);
            return icebergTable;

        } catch (StarRocksConnectorException | NoSuchTableException e) {
            LOG.error("Failed to get iceberg table {}", identifier, e);
            return null;
        }
    }

    @Override
    public boolean createTable(String dbName,
                               String tableName,
                               Schema schema,
                               PartitionSpec partitionSpec,
                               String location,
                               Map<String, String> properties) {
        return delegate.createTable(dbName, tableName, schema, partitionSpec, location, properties);
    }

    @Override
    public boolean dropTable(String dbName, String tableName, boolean purge) {
        boolean dropped = delegate.dropTable(dbName, tableName, purge);
        tables.invalidate(TableIdentifier.of(dbName, tableName));
        return dropped;
    }

    @Override
    public List<String> listPartitionNames(String dbName, String tableName) {
        TableIdentifier identifier = TableIdentifier.of(dbName, tableName);
        if (partitionNames.containsKey(identifier)) {
            return partitionNames.get(identifier);
        } else {
            List<String> partitionNames = delegate.listPartitionNames(dbName, tableName);
            this.partitionNames.put(identifier, partitionNames);
            return partitionNames;
        }
    }

    @Override
    public void deleteUncommittedDataFiles(List<String> fileLocations) {
        delegate.deleteUncommittedDataFiles(fileLocations);
    }
}
