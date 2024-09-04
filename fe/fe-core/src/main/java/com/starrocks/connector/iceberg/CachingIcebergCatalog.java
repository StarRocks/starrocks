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
import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.common.MetaNotFoundException;
<<<<<<< HEAD
=======
import com.starrocks.common.Pair;
import com.starrocks.connector.ConnectorViewDefinition;
import com.starrocks.connector.PlanMode;
>>>>>>> f0cb5e97c8 ([Enhancement] Optimize memory tracker (#49841))
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import static java.util.concurrent.TimeUnit.SECONDS;

public class CachingIcebergCatalog implements IcebergCatalog {
    private static final Logger LOG = LogManager.getLogger(CachingIcebergCatalog.class);
<<<<<<< HEAD
=======
    public static final long NEVER_CACHE = 0;
    public static final long DEFAULT_CACHE_NUM = 100000;
    private static final int MEMORY_META_SAMPLES = 10;
    private static final int MEMORY_FILE_SAMPLES = 100;
    private final String catalogName;
>>>>>>> f0cb5e97c8 ([Enhancement] Optimize memory tracker (#49841))
    private final IcebergCatalog delegate;
    private final Cache<IcebergTableName, Table> tables;
    private final Map<String, Database> databases = new ConcurrentHashMap<>();
    private final Map<IcebergTableName, List<String>> partitionNames = new ConcurrentHashMap<>();
    private final ExecutorService backgroundExecutor;

    public CachingIcebergCatalog(IcebergCatalog delegate, long ttlSec, ExecutorService executorService) {
        this.delegate = delegate;
        this.tables = CacheBuilder.newBuilder().expireAfterWrite(ttlSec, SECONDS).maximumSize(100000).build();
        this.backgroundExecutor = executorService;
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
        Database db = delegate.getDB(dbName);
        databases.put(dbName, db);
        return db;
    }

    @Override
    public List<String> listTables(String dbName) {
        return delegate.listTables(dbName);
    }

    @Override
    public Table getTable(String dbName, String tableName) throws StarRocksConnectorException {
        IcebergTableName icebergTableName = new IcebergTableName(dbName, tableName);
        if (tables.getIfPresent(icebergTableName) != null) {
            return tables.getIfPresent(icebergTableName);
        }

        Table icebergTable = delegate.getTable(dbName, tableName);
        tables.put(icebergTableName, icebergTable);
        return icebergTable;
    }

    @Override
    public boolean tableExists(String dbName, String tableName) throws StarRocksConnectorException {
        return delegate.tableExists(dbName, tableName);
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
        tables.invalidate(new IcebergTableName(dbName, tableName));
        return dropped;
    }

    @Override
    public List<String> listPartitionNames(String dbName, String tableName, ExecutorService executorService) {
        IcebergTableName icebergTableName = new IcebergTableName(dbName, tableName);
        if (partitionNames.containsKey(icebergTableName)) {
            return partitionNames.get(icebergTableName);
        } else {
            List<String> partitionNames = delegate.listPartitionNames(dbName, tableName, executorService);
            this.partitionNames.put(icebergTableName, partitionNames);
            return partitionNames;
        }
    }

    @Override
    public void deleteUncommittedDataFiles(List<String> fileLocations) {
        delegate.deleteUncommittedDataFiles(fileLocations);
    }

    @Override
    public synchronized void refreshTable(String dbName, String tableName, ExecutorService executorService) {
        IcebergTableName icebergTableName = new IcebergTableName(dbName, tableName);
        if (tables.getIfPresent(icebergTableName) == null) {
            partitionNames.remove(icebergTableName);
        } else {
            BaseTable currentTable = (BaseTable) getTable(dbName, tableName);
            BaseTable updateTable = (BaseTable) delegate.getTable(dbName, tableName);
            if (updateTable == null) {
                invalidateCache(icebergTableName);
                return;
            }
            TableOperations currentOps = currentTable.operations();
            TableOperations updateOps = updateTable.operations();
            if (currentOps == null || updateOps == null) {
                invalidateCache(icebergTableName);
                return;
            }

            TableMetadata currentPointer = currentOps.current();
            TableMetadata updatePointer = updateOps.current();
            if (currentPointer == null || updatePointer == null) {
                invalidateCache(icebergTableName);
                return;
            }

            String currentLocation = currentOps.current().metadataFileLocation();
            String updateLocation = updateOps.current().metadataFileLocation();
            if (currentLocation == null || updateLocation == null) {
                invalidateCache(icebergTableName);
                return;
            }
            if (!currentLocation.equals(updateLocation)) {
                LOG.info("Refresh iceberg caching catalog table {}.{} from {} to {}",
                        dbName, tableName, currentLocation, updateLocation);
                tables.put(icebergTableName, updateTable);
                partitionNames.put(icebergTableName, delegate.listPartitionNames(dbName, tableName, executorService));
            }
        }
    }

    public void refreshCatalog() {
        List<IcebergTableName> identifiers = Lists.newArrayList(tables.asMap().keySet());
        for (IcebergTableName identifier : identifiers) {
            try {
                refreshTable(identifier.dbName, identifier.tableName, backgroundExecutor);
            } catch (Exception e) {
                LOG.warn("refresh {}.{} metadata cache failed, msg : ", identifier.dbName, identifier.tableName, e);
                invalidateCache(identifier);
            }
        }
    }

    public void invalidateCacheWithoutTable(CachingIcebergCatalog.IcebergTableName icebergTableName) {
        partitionNames.remove(icebergTableName);
    }

    public void invalidateCache(CachingIcebergCatalog.IcebergTableName icebergTableName) {
        tables.invalidate(icebergTableName);
        partitionNames.remove(icebergTableName);
    }

    static class IcebergTableName {
        private final String dbName;
        private final String tableName;

        public IcebergTableName(String dbName, String tableName) {
            this.dbName = dbName;
            this.tableName = tableName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            IcebergTableName that = (IcebergTableName) o;
            return dbName.equalsIgnoreCase(that.dbName) && tableName.equalsIgnoreCase(that.tableName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dbName.toLowerCase(Locale.ROOT), tableName.toLowerCase(Locale.ROOT));
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("IcebergTableName{");
            sb.append("dbName='").append(dbName).append('\'');
            sb.append(", tableName='").append(tableName).append('\'');
            sb.append('}');
            return sb.toString();
        }
    }
<<<<<<< HEAD
=======

    @Override
    public List<Pair<List<Object>, Long>> getSamples() {
        Pair<List<Object>, Long> dbSamples = Pair.create(databases.asMap().values()
                .stream()
                .limit(MEMORY_META_SAMPLES)
                .collect(Collectors.toList()),
                databases.size());

        Pair<List<Object>, Long> tableSamples = Pair.create(tables.asMap().values()
                .stream()
                .limit(MEMORY_META_SAMPLES)
                .collect(Collectors.toList()),
                tables.size());

        List<Object> partitions = partitionNames.asMap().values()
                .stream()
                .flatMap(List::stream)
                .limit(MEMORY_FILE_SAMPLES)
                .collect(Collectors.toList());
        long partitionTotal = partitionNames.asMap().values()
                .stream()
                .mapToLong(List::size)
                .sum();
        Pair<List<Object>, Long> partitionSamples = Pair.create(partitions, partitionTotal);

        List<Object> dataFiles = dataFileCache.asMap().values()
                .stream().flatMap(Set::stream)
                .limit(MEMORY_FILE_SAMPLES)
                .collect(Collectors.toList());
        long dataFilesTotal = dataFileCache.asMap().values()
                .stream()
                .mapToLong(Set::size)
                .sum();
        Pair<List<Object>, Long> dataFileSamples = Pair.create(dataFiles, dataFilesTotal);

        List<Object> deleteFiles = deleteFileCache.asMap().values()
                .stream().flatMap(Set::stream)
                .limit(MEMORY_FILE_SAMPLES)
                .collect(Collectors.toList());
        long deleteFilesTotal = deleteFileCache.asMap().values()
                .stream()
                .mapToLong(Set::size)
                .sum();
        Pair<List<Object>, Long> deleteFileSamples = Pair.create(deleteFiles, deleteFilesTotal);

        return Lists.newArrayList(dbSamples, tableSamples, partitionSamples, dataFileSamples, deleteFileSamples);
    }

    @Override
    public Map<String, Long> estimateCount() {
        Map<String, Long> counter = new HashMap<>();
        counter.put("Database", databases.size());
        counter.put("Table", tables.size());
        counter.put("PartitionNames", partitionNames.asMap().values()
                .stream()
                .mapToLong(List::size)
                .sum());
        counter.put("ManifestOfDataFile",  dataFileCache.asMap().values()
                .stream()
                .mapToLong(Set::size)
                .sum());
        counter.put("ManifestOfDeleteFile", deleteFileCache.asMap().values()
                .stream()
                .mapToLong(Set::size)
                .sum());
        return counter;
    }
>>>>>>> f0cb5e97c8 ([Enhancement] Optimize memory tracker (#49841))
}
