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
import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.connector.ConnectorViewDefinition;
import com.starrocks.connector.PlanMode;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.mysql.MysqlCommand;
import com.starrocks.qe.ConnectContext;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StarRocksIcebergTableScan;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.view.View;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.util.SizeEstimator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static com.starrocks.connector.PartitionUtil.convertIcebergPartitionToPartitionName;
import static java.util.concurrent.TimeUnit.SECONDS;

public class CachingIcebergCatalog implements IcebergCatalog {
    private static final Logger LOG = LogManager.getLogger(CachingIcebergCatalog.class);
    public static final long NEVER_CACHE = 0;
    public static final long DEFAULT_CACHE_NUM = 100000;
    private final String catalogName;
    private final IcebergCatalog delegate;
    private final Cache<IcebergTableName, Table> tables;
    private final Cache<IcebergTableName, List<String>> partitionNames;
    private final Cache<String, Database> databases;
    private final ExecutorService backgroundExecutor;

    private final IcebergCatalogProperties icebergProperties;
    private final Cache<String, Set<DataFile>> dataFileCache;
    private final Cache<String, Set<DeleteFile>> deleteFileCache;
    private final Map<IcebergTableName, Long> tableLatestAccessTime = new ConcurrentHashMap<>();
    private final Map<IcebergTableName, Long> tableLatestRefreshTime = new ConcurrentHashMap<>();
    private final Map<IcebergTableName, Long> tableLatestSnapshotTime = new ConcurrentHashMap<>();

    public CachingIcebergCatalog(String catalogName, IcebergCatalog delegate, IcebergCatalogProperties icebergProperties,
                                 ExecutorService executorService) {
        this.catalogName = catalogName;
        this.delegate = delegate;
        this.icebergProperties = icebergProperties;
        boolean enableCache = icebergProperties.isEnableIcebergMetadataCache();
        this.databases = newCacheBuilder(icebergProperties.getIcebergTableCacheTtlSec(),
                enableCache ? DEFAULT_CACHE_NUM : NEVER_CACHE).build();
        this.tables = newCacheBuilder(icebergProperties.getIcebergTableCacheTtlSec(),
                enableCache ? DEFAULT_CACHE_NUM : NEVER_CACHE).build();
        this.partitionNames = newCacheBuilder(icebergProperties.getIcebergTableCacheTtlSec(),
                enableCache ? DEFAULT_CACHE_NUM : NEVER_CACHE).build();
        this.dataFileCache = enableCache ?
                newCacheBuilder(
                        icebergProperties.getIcebergTableCacheTtlSec(), icebergProperties.getIcebergManifestCacheMaxNum()).build()
                : null;
        this.deleteFileCache = enableCache ?
                newCacheBuilder(
                        icebergProperties.getIcebergTableCacheTtlSec(), icebergProperties.getIcebergManifestCacheMaxNum()).build()
                : null;
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
        databases.invalidate(dbName);
    }

    @Override
    public Database getDB(String dbName) {
        if (databases.asMap().containsKey(dbName)) {
            return databases.getIfPresent(dbName);
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

        if (ConnectContext.get().getCommand() == MysqlCommand.COM_QUERY) {
            tableLatestAccessTime.put(icebergTableName, System.currentTimeMillis());
        }

        if (tables.getIfPresent(icebergTableName) != null) {
            Table icebergTable = tables.getIfPresent(icebergTableName);
            // prolong table cache
            tables.put(icebergTableName, icebergTable);
            return icebergTable;
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
    public void renameTable(String dbName, String tblName, String newTblName) throws StarRocksConnectorException {
        delegate.renameTable(dbName, tblName, newTblName);
        invalidateCache(new IcebergTableName(dbName, tblName));
    }

    @Override
    public boolean createView(ConnectorViewDefinition connectorViewDefinition, boolean replace) {
        return delegate.createView(connectorViewDefinition, replace);
    }

    public boolean dropView(String dbName, String viewName) {
        return delegate.dropView(dbName, viewName);
    }

    public View getView(String dbName, String viewName) {
        return delegate.getView(dbName, viewName);
    }

    @Override
    public List<String> listPartitionNames(String dbName, String tableName, long snapshotId, ExecutorService executorService) {
        IcebergTableName icebergTableName = new IcebergTableName(dbName, tableName, snapshotId);
        if (partitionNames.asMap().containsKey(icebergTableName)) {
            return partitionNames.getIfPresent(icebergTableName);
        } else {
            org.apache.iceberg.Table icebergTable = delegate.getTable(dbName, tableName);
            List<String> partitionNames = Lists.newArrayList();

            if (icebergTable.specs().values().stream().allMatch(PartitionSpec::isUnpartitioned)) {
                return partitionNames;
            }

            if (snapshotId == -1) {
                if (icebergTable.currentSnapshot() == null) {
                    return partitionNames;
                } else {
                    snapshotId = icebergTable.currentSnapshot().snapshotId();
                }
            }

            partitionNames = listPartitionNamesWithSnapshotId(icebergTable, dbName, tableName, snapshotId, executorService);
            this.partitionNames.put(icebergTableName, partitionNames);
            return partitionNames;
        }
    }

    private List<String> listPartitionNamesWithSnapshotId(
            Table table, String dbName, String tableName, long snapshotId, ExecutorService executorService) {
        Set<String> partitionNames = Sets.newHashSet();
        StarRocksIcebergTableScanContext scanContext = new StarRocksIcebergTableScanContext(
                catalogName, dbName, tableName, PlanMode.LOCAL);
        scanContext.setOnlyReadCache(true);
        TableScan tableScan = getTableScan(table, scanContext)
                .planWith(executorService)
                .useSnapshot(snapshotId);

        try (CloseableIterable<FileScanTask> fileScanTaskIterable = tableScan.planFiles();
                CloseableIterator<FileScanTask> fileScanTaskIterator = fileScanTaskIterable.iterator()) {

            while (fileScanTaskIterator.hasNext()) {
                FileScanTask scanTask = fileScanTaskIterator.next();
                StructLike partition = scanTask.file().partition();
                partitionNames.add(convertIcebergPartitionToPartitionName(scanTask.spec(), partition));
            }
        } catch (IOException e) {
            throw new StarRocksConnectorException(String.format("Failed to list iceberg partition names %s.%s",
                    dbName, tableName), e);
        }

        return new ArrayList<>(partitionNames);
    }

    @Override
    public void deleteUncommittedDataFiles(List<String> fileLocations) {
        delegate.deleteUncommittedDataFiles(fileLocations);
    }

    @Override
    public synchronized void refreshTable(String dbName, String tableName, ExecutorService executorService) {
        Long latestRefreshTime = tableLatestRefreshTime.computeIfAbsent(new IcebergTableName(dbName, tableName), ignore -> -1L);
        IcebergTableName icebergTableName = new IcebergTableName(dbName, tableName);
        if (tables.getIfPresent(icebergTableName) == null) {
            partitionNames.invalidate(icebergTableName);
        } else {
            BaseTable currentTable = (BaseTable) tables.getIfPresent(icebergTableName);
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

            Long currentSnapshotId = currentTable.currentSnapshot().snapshotId();
            Long updateSnapshotId = updateTable.currentSnapshot().snapshotId();

            // if latest refresh is -1, it means the table has never been refreshed
            if (!currentSnapshotId.equals(updateSnapshotId) || latestRefreshTime == -1) {
                LOG.info("Refresh iceberg caching catalog table {}.{} from {} to {}",
                        dbName, tableName, currentLocation, updateLocation);
                refreshTable(updateTable, currentSnapshotId, dbName, tableName, executorService);
                LOG.info("Finished to refresh iceberg table {}.{}", dbName, tableName);
            } else {
                LOG.info("Not refreshing iceberg table {}.{} because snapshot is the same",
                        dbName, tableName);
                tableLatestRefreshTime.put(new IcebergTableName(dbName, tableName), System.currentTimeMillis());
            }
        }
    }

    private void refreshTable(BaseTable updatedTable, long baseSnapshotId,
                              String dbName, String tableName, ExecutorService executorService) {
        Long updatedSnapshotId = updatedTable.currentSnapshot().snapshotId();
        Long updatedSnapshotTime = updatedTable.currentSnapshot().timestampMillis();
        IcebergTableName baseIcebergTableName = new IcebergTableName(dbName, tableName, baseSnapshotId);
        IcebergTableName updatedIcebergTableName = new IcebergTableName(dbName, tableName, updatedSnapshotId);
        Long latestRefreshTime = tableLatestRefreshTime.computeIfAbsent(new IcebergTableName(dbName, tableName), ignore -> -1L);

        List<String> updatedPartitionNames = updatedTable.spec().isPartitioned() ?
                listPartitionNamesWithSnapshotId(updatedTable, dbName, tableName, updatedSnapshotId, executorService) :
                new ArrayList<>();

        synchronized (this) {
            partitionNames.put(updatedIcebergTableName, updatedPartitionNames);
            tables.put(updatedIcebergTableName, updatedTable);
            partitionNames.invalidate(baseIcebergTableName);
            tables.invalidate(baseIcebergTableName);
        }

        TableMetadata updatedTableMetadata = updatedTable.operations().current();
        List<ManifestFile> manifestFiles = updatedTable.currentSnapshot().dataManifests(updatedTable.io()).stream()
                .filter(f -> updatedTableMetadata.snapshot(f.snapshotId()) != null)
                .filter(f -> updatedTableMetadata.snapshot(f.snapshotId()).timestampMillis() > latestRefreshTime)
                .collect(Collectors.toList());

        boolean alreadyCached = manifestFiles.stream().allMatch(f -> dataFileCache.getIfPresent(f.path()) != null);

        if (manifestFiles.isEmpty() || alreadyCached) {
            LOG.debug("Not caching manifests on the table {}.{}: {}",
                    dbName, tableName, alreadyCached ? "all manifests already cached" : "no manifests to cache");
            if (alreadyCached) {
                tableLatestRefreshTime.put(new IcebergTableName(dbName, tableName), System.currentTimeMillis());
                tableLatestSnapshotTime.put(new IcebergTableName(dbName, tableName), updatedSnapshotTime);
            }
            return;
        }
        StarRocksIcebergTableScanContext scanContext = new StarRocksIcebergTableScanContext(
                catalogName, dbName, tableName, PlanMode.LOCAL);
        StarRocksIcebergTableScan tableScan = (StarRocksIcebergTableScan) getTableScan(updatedTable, scanContext)
                .planWith(executorService)
                .useSnapshot(updatedSnapshotId);
        tableScan.refreshDataFileCache(manifestFiles);

        LOG.debug("Refreshed LatestSnapshotTime for table {}.{} is {}", dbName, tableName, updatedSnapshotTime);
        tableLatestRefreshTime.put(new IcebergTableName(dbName, tableName), System.currentTimeMillis());
        tableLatestSnapshotTime.put(new IcebergTableName(dbName, tableName), updatedSnapshotTime);
    }

    // This is called every background_refresh_metadata_interval_millis
    public void refreshCatalog() {
        List<IcebergTableName> identifiers = Lists.newArrayList(tables.asMap().keySet());
        for (IcebergTableName identifier : identifiers) {
            try {
                IcebergTableName icebergTableName = new IcebergTableName(identifier.dbName, identifier.tableName);
                Long latestAccessTime = tableLatestAccessTime.get(icebergTableName);
                Long latestRefreshTime = tableLatestRefreshTime.get(icebergTableName);
                Long latestSnapshotTime = tableLatestSnapshotTime.get(icebergTableName);
                Long metaCacheTtlSec = icebergProperties.getIcebergMetaCacheTtlSec();
                Long tableCacheTtlSec = icebergProperties.getIcebergTableCacheTtlSec();

                // refresh tables with expired manifest time
                // don't refresh tables that were not accessed by queries
                if ((latestAccessTime != null &&
                        (System.currentTimeMillis() - latestAccessTime) / 1000 < tableCacheTtlSec) &&
                        (latestSnapshotTime == null ||
                        (System.currentTimeMillis() - latestSnapshotTime) / 1000 > metaCacheTtlSec) &&
                        (latestRefreshTime == null ||
                        (System.currentTimeMillis() - latestRefreshTime) / 1000 > metaCacheTtlSec)) {
                    LOG.info("Iceberg table {}.{} eligible for refresh", identifier.dbName, identifier.tableName);
                    LOG.debug("{}.{} latestSnapshotTime: {}", identifier.dbName, identifier.tableName, latestSnapshotTime);
                    LOG.debug("{}.{} latestRefreshTime: {}", identifier.dbName, identifier.tableName, latestRefreshTime);
                    refreshTable(identifier.dbName, identifier.tableName, backgroundExecutor);
                }
            } catch (Exception e) {
                LOG.warn("refresh {}.{} metadata cache failed, msg : ", identifier.dbName, identifier.tableName, e);
                invalidateCache(identifier);
            }
        }
    }

    public void invalidateCacheWithoutTable(IcebergTableName icebergTableName) {
        partitionNames.invalidate(icebergTableName);
    }

    public void invalidateCache(IcebergTableName icebergTableName) {
        tables.invalidate(icebergTableName);
        partitionNames.invalidate(icebergTableName);
    }

    @Override
    public StarRocksIcebergTableScan getTableScan(Table table, StarRocksIcebergTableScanContext scanContext) {
        scanContext.setLocalParallelism(icebergProperties.getIcebergJobPlanningThreadNum());
        scanContext.setLocalPlanningMaxSlotSize(icebergProperties.getLocalPlanningMaxSlotBytes());
        scanContext.setDataFileCache(dataFileCache);
        scanContext.setDeleteFileCache(deleteFileCache);
        scanContext.setDataFileCacheWithMetrics(icebergProperties.isIcebergManifestCacheWithColumnStatistics());
        scanContext.setEnableCacheDataFileIdentifierColumnMetrics(
                icebergProperties.enableCacheDataFileIdentifierColumnStatistics());

        return delegate.getTableScan(table, scanContext);
    }

    private CacheBuilder<Object, Object> newCacheBuilder(long expiresAfterWriteSec, long maximumSize) {
        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
        if (expiresAfterWriteSec >= 0) {
            cacheBuilder.expireAfterWrite(expiresAfterWriteSec, SECONDS);
        }

        cacheBuilder.maximumSize(maximumSize);
        return cacheBuilder;
    }

    public static class IcebergTableName {
        private final String dbName;
        private final String tableName;
        private long snapshotId = -1;

        public IcebergTableName(String dbName, String tableName) {
            this(dbName, tableName, -1);
        }

        public IcebergTableName(String dbName, String tableName, long snapshotId) {
            this.dbName = dbName;
            this.tableName = tableName;
            this.snapshotId = snapshotId;
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
            return dbName.equalsIgnoreCase(that.dbName) && tableName.equalsIgnoreCase(that.tableName) &&
                    (snapshotId == -1 || snapshotId == that.snapshotId);
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

    @Override
    public long estimateSize() {
        return SizeEstimator.estimate(databases) +
                SizeEstimator.estimate(tables) +
                SizeEstimator.estimate(partitionNames) +
                SizeEstimator.estimate(dataFileCache) +
                SizeEstimator.estimate(deleteFileCache);

    }

    @Override
    public Map<String, Long> estimateCount() {
        Map<String, Long> counter = new HashMap<>();
        counter.put("Database", databases.size());
        counter.put("Table", tables.size());
        counter.put("PartitionNames", partitionNames.size());
        counter.put("ManifestOfDataFile", dataFileCache.size());
        counter.put("ManifestOfDeleteFile", deleteFileCache.size());
        return counter;
    }
}
