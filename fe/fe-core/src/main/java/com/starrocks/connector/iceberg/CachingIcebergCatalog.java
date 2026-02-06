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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Weigher;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.common.Config;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.connector.ConnectorMetadatRequestContext;
import com.starrocks.connector.ConnectorViewDefinition;
import com.starrocks.connector.PlanMode;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.rest.IcebergRESTCatalog;
import com.starrocks.memory.estimate.Estimator;
import com.starrocks.mysql.MysqlCommand;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StarRocksIcebergTableScan;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.view.View;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.Strings;
import org.apache.spark.util.SizeEstimator;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.SECONDS;

public class CachingIcebergCatalog implements IcebergCatalog {
    private static final Logger LOG = LogManager.getLogger(CachingIcebergCatalog.class);
    public static final long NEVER_CACHE = 0;
    public static final long DEFAULT_CACHE_NUM = 100000;
    private static final int MEMORY_SNAPSHOT_SIZE = 2048; // approx memory size of one snapshot object without manifests
    private static final int MEMORY_MANIFEST_SIZE = 1024; // approx memory size of one manifest object in snapshot
    private static final int MEMORY_DATA_FILE_SIZE = 1536; // approx memory size of one data file in manifest
    private static final int MEMORY_PARTITION_DATA_SIZE = 768; // approx memory size of one partition data in data file
    private static final ThreadLocal<ConnectContext> TABLE_LOAD_CONTEXT = new ThreadLocal<>();
    private final String catalogName;
    private final IcebergCatalog delegate;
    private final com.github.benmanes.caffeine.cache.LoadingCache<IcebergTableName, Table> tables;
    private final com.github.benmanes.caffeine.cache.Cache<String, Database> databases;
    private final ExecutorService backgroundExecutor;

    private final IcebergCatalogProperties icebergProperties;
    private final com.github.benmanes.caffeine.cache.Cache<String, Set<DataFile>> dataFileCache;
    private final com.github.benmanes.caffeine.cache.Cache<String, Set<DeleteFile>> deleteFileCache;
    private final Map<IcebergTableName, Set<String>> metaFileCacheMap = new ConcurrentHashMap<>(); // table -> metadata file paths
    private final Map<IcebergTableName, Long> tableLatestAccessTime = new ConcurrentHashMap<>();
    private final Map<IcebergTableName, Long> tableLatestRefreshTime = new ConcurrentHashMap<>();

    private final com.github.benmanes.caffeine.cache.LoadingCache<IcebergTableName, Map<String, Partition>> partitionCache;

    public CachingIcebergCatalog(String catalogName, IcebergCatalog delegate, IcebergCatalogProperties icebergProperties,
                                 ExecutorService executorService) {
        this.catalogName = catalogName;
        this.delegate = delegate;
        this.icebergProperties = icebergProperties;
        boolean enableCache = icebergProperties.isEnableIcebergMetadataCache();
        long tableCacheSize = Math.round(Runtime.getRuntime().maxMemory() *
                icebergProperties.getIcebergTableCacheMemoryUsageRatio());
        this.databases = newCacheBuilderWithMaximumSize(
                icebergProperties.getIcebergMetaCacheTtlSec(),
                NEVER_CACHE,
                enableCache ? DEFAULT_CACHE_NUM : NEVER_CACHE).build();
        this.tables = newCacheBuilder(
                icebergProperties.getIcebergMetaCacheTtlSec(),
                icebergProperties.getIcebergTableCacheRefreshIntervalSec())
                .executor(executorService)
                .maximumWeight(tableCacheSize)
                .weigher((Weigher<IcebergTableName, Table>) (IcebergTableName key, Table table) -> {
                    long size = SizeEstimator.estimate(key);
                    if (table != null) {
                        size += 1L * countSnapshotsSafe(table) * MEMORY_SNAPSHOT_SIZE;
                        if (((BaseTable) table).operations().current().currentSnapshot() != null) {
                            size += 1L * (((BaseTable) table).operations().current().currentSnapshot()
                                    .allManifests(((BaseTable) table).operations().io()).size() * MEMORY_MANIFEST_SIZE);
                        }
                    }
                    return (size > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) size;
                })
                .removalListener((IcebergTableName key, Table value, RemovalCause cause) -> {
                    if (key != null) {
                        LOG.debug("iceberg table cache removal: {}.{}, cause={}, evicted={}",
                                key.dbName, key.tableName,
                                cause, cause.wasEvicted());
                    }
                })
                .build(new com.github.benmanes.caffeine.cache.CacheLoader<IcebergTableName, Table>() {
                    @Override
                    public Table load(IcebergTableName key) throws Exception {
                        LOG.debug("Loading iceberg table {}.{} from remote catalog",
                                key.dbName, key.tableName);
                        ConnectContext context = TABLE_LOAD_CONTEXT.get();
                        return delegate.getTable(context != null ? context : new ConnectContext(),
                                key.dbName, key.tableName);
                    }

                    @Override
                    public Table reload(IcebergTableName key, Table oldValue) {
                        try {
                            BaseTable updateTable = 
                                    (BaseTable) delegate.getTable(new ConnectContext(), key.dbName, key.tableName);
                            TableOperations newOps = updateTable.operations();
                            TableOperations oldOps = ((BaseTable) oldValue).operations();
                            if (oldOps.current().metadataFileLocation().equals(newOps.current().metadataFileLocation())) {
                                return oldValue;
                            }
                            return updateTable;
                        } catch (Exception e) {
                            LOG.warn("refresh table {}.{} failed", key.dbName, key.tableName, e);
                            return oldValue;
                        }
                    }
                });
        this.partitionCache = newCacheBuilderWithMaximumSize(
                icebergProperties.getIcebergMetaCacheTtlSec(), NEVER_CACHE,
                enableCache ? DEFAULT_CACHE_NUM : NEVER_CACHE).build(
                    new com.github.benmanes.caffeine.cache.CacheLoader<IcebergTableName, Map<String, Partition>>() {
                        @Override
                        public Map<String, Partition> load(IcebergTableName key) throws Exception {
                            Table nativeTable = getTable(new ConnectContext(), key.dbName, key.tableName);
                            IcebergTable icebergTable =
                                    IcebergTable.builder()
                                            .setCatalogDBName(key.dbName)
                                            .setSrTableName(key.tableName)
                                            .setCatalogTableName(key.tableName)
                                            .setNativeTable(nativeTable).build();
                            return delegate.getPartitions(icebergTable, key.snapshotId, null);
                        }
                    });
        long dataFileCacheSize = Math.round(Runtime.getRuntime().maxMemory() *
                icebergProperties.getIcebergDataFileCacheMemoryUsageRatio());
        long deleteFileCacheSize = Math.round(Runtime.getRuntime().maxMemory() *
                icebergProperties.getIcebergDeleteFileCacheMemoryUsageRatio());

        this.dataFileCache = enableCache ? Caffeine.newBuilder()
                .executor(executorService)
                .expireAfterWrite(icebergProperties.getIcebergMetaCacheTtlSec(), SECONDS)
                .weigher((Weigher<String, Set<DataFile>>) (String key, Set<DataFile> files) -> {
                    long size = SizeEstimator.estimate(key);
                    if (!files.isEmpty()) {
                        size += 1L * estimateDataFileSizeInBytes(files.iterator().next()) * files.size();
                    }
                    return (size > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) size;
                })
                .maximumWeight(dataFileCacheSize)
                .removalListener((String key, Set<DataFile> value, RemovalCause cause) -> {
                    LOG.debug(String.format("Key=%s, Value.size=%d, Cause=%s",
                            key,
                            value != null ? value.size() : 0,
                            cause));
                })
                .build() : null;
        this.deleteFileCache = enableCache ? Caffeine.newBuilder()
                .executor(executorService)
                .expireAfterWrite(icebergProperties.getIcebergMetaCacheTtlSec(), SECONDS)
                .weigher((Weigher<String, Set<DeleteFile>>) (String key, Set<DeleteFile> files) -> {
                    long size = SizeEstimator.estimate(key);
                    if (!files.isEmpty()) {
                        size += 1L * estimateDataFileSizeInBytes(files.iterator().next()) * files.size();
                    }
                    return (size > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) size;
                })
                .maximumWeight(deleteFileCacheSize)
                .removalListener((String key, Set<DeleteFile> value, RemovalCause cause) -> {
                    LOG.debug(String.format("Key=%s, Value.size=%d, Cause=%s",
                            key,
                            value != null ? value.size() : 0,
                            cause));
                })
                .build() : null;

        this.backgroundExecutor = executorService;
    }

    @Override
    public IcebergCatalogType getIcebergCatalogType() {
        return delegate.getIcebergCatalogType();
    }

    @Override
    public List<String> listAllDatabases(ConnectContext connectContext) {
        return delegate.listAllDatabases(connectContext);
    }

    public void createDB(ConnectContext connectContext, String dbName, Map<String, String> properties) {
        delegate.createDB(connectContext, dbName, properties);
    }

    public void dropDB(ConnectContext connectContext, String dbName) throws MetaNotFoundException {
        delegate.dropDB(connectContext, dbName);
        databases.invalidate(dbName);
    }

    @Override
    public Database getDB(ConnectContext connectContext, String dbName) {
        if (databases.asMap().containsKey(dbName)) {
            return databases.getIfPresent(dbName);
        }
        Database db = delegate.getDB(connectContext, dbName);
        databases.put(dbName, db);
        return db;
    }

    @Override
    public List<String> listTables(ConnectContext connectContext, String dbName) {
        return delegate.listTables(connectContext, dbName);
    }

    @Override
    public Table getTable(ConnectContext connectContext, String dbName, String tableName) throws StarRocksConnectorException {
        IcebergTableName icebergTableName = new IcebergTableName(dbName, tableName);

        if (ConnectContext.get() == null || ConnectContext.get().getCommand() == MysqlCommand.COM_QUERY) {
            tableLatestAccessTime.put(icebergTableName, System.currentTimeMillis());
        }

        // do not cache if jwt or oauth2 is not used OR if it is not a REST Catalog.
        boolean cacheAllowed = icebergProperties.isEnableIcebergTableCache() && 
                (Strings.isNullOrEmpty(connectContext.getAuthToken()) || !(delegate instanceof IcebergRESTCatalog));
        if (!cacheAllowed) {
            return delegate.getTable(connectContext, dbName, tableName);
        }
        try {
            TABLE_LOAD_CONTEXT.set(connectContext);
            return tables.get(icebergTableName);
        } catch (NoSuchTableException e) {
            throw e;
        } catch (Exception e) {
            Throwable ce = ExceptionUtils.getRootCause(e);
            String errMsg = ce != null ? ce.getMessage() : e.getMessage();
            throw new StarRocksConnectorException(String.format("Failed to get iceberg table %s.%s.%s. %s",
                    catalogName, dbName, tableName, errMsg), e);
        } finally {
            TABLE_LOAD_CONTEXT.remove();
        }
    }

    @Override
    public boolean tableExists(ConnectContext connectContext, String dbName, String tableName)
            throws StarRocksConnectorException {
        return delegate.tableExists(connectContext, dbName, tableName);
    }

    @Override
    public boolean createTable(ConnectContext connectContext,
                               String dbName,
                               String tableName,
                               Schema schema,
                               PartitionSpec partitionSpec,
                               String location,
                               SortOrder sortOrder,
                               Map<String, String> properties) {
        return delegate.createTable(connectContext, dbName, tableName, schema, partitionSpec, location, sortOrder, properties);
    }

    @Override
    public boolean dropTable(ConnectContext connectContext, String dbName, String tableName, boolean purge) {
        boolean dropped = delegate.dropTable(connectContext, dbName, tableName, purge);
        invalidateCache(new IcebergTableName(dbName, tableName));
        return dropped;
    }

    @Override
    public void renameTable(ConnectContext connectContext, String dbName, String tblName, String newTblName)
            throws StarRocksConnectorException {
        delegate.renameTable(connectContext, dbName, tblName, newTblName);
        invalidateCache(new IcebergTableName(dbName, tblName));
    }

    @Override
    public boolean createView(ConnectContext connectContext,
                              String catalogName,
                              ConnectorViewDefinition connectorViewDefinition,
                              boolean replace) {
        return delegate.createView(connectContext, catalogName, connectorViewDefinition, replace);
    }

    @Override
    public boolean alterView(ConnectContext connectContext, View currentView, ConnectorViewDefinition connectorViewDefinition) {
        return delegate.alterView(connectContext, currentView, connectorViewDefinition);
    }

    @Override
    public boolean dropView(ConnectContext connectContext, String dbName, String viewName) {
        return delegate.dropView(connectContext, dbName, viewName);
    }

    public View getView(ConnectContext connectContext, String dbName, String viewName) {
        return delegate.getView(connectContext, dbName, viewName);
    }

    @Override
    public Map<String, Partition> getPartitions(IcebergTable icebergTable, long snapshotId,
                                                ExecutorService executorService) {
        IcebergTableName key =
                new IcebergTableName(icebergTable.getCatalogDBName(), icebergTable.getCatalogTableName(), snapshotId);
        return partitionCache.get(key);
    }

    @Override
    public List<String> listPartitionNames(IcebergTable icebergTable, ConnectorMetadatRequestContext requestContext,
                                           ExecutorService executorService) {
        SessionVariable sv = ConnectContext.getSessionVariableOrDefault();
        // optimization for query mv rewrite, we can optionally return null to bypass it.
        // if we don't have cache right now, which means it probably takes time to load it during query,
        // so we can do load in background while return null to bypass this synchronous process.
        if (requestContext.isQueryMVRewrite() && sv.isEnableConnectorAsyncListPartitions()) {
            long snapshotId = requestContext.getSnapshotId();
            IcebergTableName key =
                    new IcebergTableName(icebergTable.getCatalogDBName(), icebergTable.getCatalogTableName(), snapshotId);
            Map<String, Partition> cacheValue = partitionCache.getIfPresent(key);
            if (cacheValue == null) {
                backgroundExecutor.submit(() -> partitionCache.refresh(key));
                return null;
            }
        }
        return IcebergCatalog.super.listPartitionNames(icebergTable, requestContext, executorService);
    }

    @Override
    public void deleteUncommittedDataFiles(List<String> fileLocations) {
        delegate.deleteUncommittedDataFiles(fileLocations);
    }

    @Override
    public synchronized void refreshTable(String dbName, String tableName, ConnectContext ctx, ExecutorService executorService) {
        IcebergTableName icebergTableName = new IcebergTableName(dbName, tableName);
        Table cachedTable = tables.getIfPresent(icebergTableName);
        if (cachedTable == null) {
            partitionCache.invalidate(icebergTableName);
        } else {
            BaseTable currentTable = (BaseTable) tables.getIfPresent(icebergTableName);
            BaseTable updateTable = (BaseTable) delegate.getTable(ctx, dbName, tableName);
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
                refreshTable(currentTable, updateTable, dbName, tableName, ctx, executorService);
                LOG.info("Finished to refresh iceberg table {}.{}", dbName, tableName);
            }
        }
    }

    private void refreshTable(BaseTable currentTable, BaseTable updatedTable,
                              String dbName, String tableName, ConnectContext ctx, ExecutorService executorService) {
        long baseSnapshotId = currentTable.currentSnapshot().snapshotId();
        long updatedSnapshotId = updatedTable.currentSnapshot().snapshotId();
        IcebergTableName baseIcebergTableName = new IcebergTableName(dbName, tableName, baseSnapshotId);
        IcebergTableName updatedIcebergTableName = new IcebergTableName(dbName, tableName, updatedSnapshotId);
        IcebergTableName keyWithoutSnap = new IcebergTableName(dbName, tableName);
        long latestRefreshTime = tableLatestRefreshTime.computeIfAbsent(new IcebergTableName(dbName, tableName), ignore -> -1L);

        // update tables before refresh partition cache
        // so when refreshing partition cache, `getTables` can return the latest one.
        // another way to fix is to call `delegate.getTables` when refreshing partition cache.
        synchronized (this) {
            tables.put(keyWithoutSnap, updatedTable);
        }

        partitionCache.invalidate(baseIcebergTableName);
        partitionCache.get(updatedIcebergTableName);

        TableMetadata updatedTableMetadata = updatedTable.operations().current();
        List<ManifestFile> manifestFiles = updatedTable.currentSnapshot().dataManifests(updatedTable.io()).stream()
                .filter(f -> updatedTableMetadata.snapshot(f.snapshotId()) != null)
                .filter(f -> updatedTableMetadata.snapshot(f.snapshotId()).timestampMillis() > latestRefreshTime)
                .filter(f -> dataFileCache.getIfPresent(f.path()) == null)
                .filter(f -> f.hasAddedFiles() || f.hasExistingFiles())
                .filter(f -> f.length() >= icebergProperties.getRefreshIcebergManifestMinLength())
                .collect(Collectors.toList());

        if (manifestFiles.isEmpty()) {
            tableLatestRefreshTime.put(new IcebergTableName(dbName, tableName), System.currentTimeMillis());
            return;
        }

        StarRocksIcebergTableScanContext scanContext = new StarRocksIcebergTableScanContext(
                catalogName, dbName, tableName, PlanMode.LOCAL);
        StarRocksIcebergTableScan tableScan = (StarRocksIcebergTableScan) getTableScan(updatedTable, scanContext)
                .planWith(executorService)
                .useSnapshot(updatedSnapshotId);
        tableScan.refreshDataFileCache(manifestFiles);

        tableLatestRefreshTime.put(new IcebergTableName(dbName, tableName), System.currentTimeMillis());
        LOG.info("Refreshed {} iceberg manifests on the table [{}.{}]", manifestFiles.size(), dbName, tableName);
    }

    public void refreshCatalog() {
        List<IcebergTableName> identifiers = Lists.newArrayList(tables.asMap().keySet());
        for (IcebergTableName identifier : identifiers) {
            try {
                Long latestAccessTime = tableLatestAccessTime.get(identifier);
                if (latestAccessTime == null || (System.currentTimeMillis() - latestAccessTime) / 1000 >
                        Config.background_refresh_metadata_time_secs_since_last_access_secs) {
                    invalidateCache(identifier);
                    continue;
                }

                refreshTable(identifier.dbName, identifier.tableName, new ConnectContext(), backgroundExecutor);
            } catch (Exception e) {
                LOG.warn("refresh {}.{} metadata cache failed, msg : ", identifier.dbName,
                        identifier.tableName, e);
                invalidateCache(identifier);
            }
        }
    }

    @Override
    public void invalidatePartitionCache(String dbName, String tableName) {
        // will invalidate all snapshots of this table
        IcebergTableName key = new IcebergTableName(dbName, tableName);
        partitionCache.invalidate(key);
    }

    @Override
    public void invalidateCache(String dbName, String tableName) {
        IcebergTableName key = new IcebergTableName(dbName, tableName);
        invalidateCache(key);

    }

    private void invalidateCache(IcebergTableName key) {
        tables.invalidate(key);
        // will invalidate all snapshots of this table
        partitionCache.invalidate(key);
        Set<String> paths = metaFileCacheMap.get(key);
        if (paths != null && !paths.isEmpty()) {
            dataFileCache.invalidateAll(paths);
            deleteFileCache.invalidateAll(paths);
            paths.clear();
        }
    }

    @Override
    public StarRocksIcebergTableScan getTableScan(Table table, StarRocksIcebergTableScanContext scanContext) {
        scanContext.setDataFileCache(dataFileCache);
        scanContext.setDeleteFileCache(deleteFileCache);
        scanContext.setMetaFileCacheMap(metaFileCacheMap);
        scanContext.setDataFileCacheWithMetrics(icebergProperties.isIcebergManifestCacheWithColumnStatistics());
        scanContext.setEnableCacheDataFileIdentifierColumnMetrics(
                icebergProperties.enableCacheDataFileIdentifierColumnStatistics());

        return delegate.getTableScan(table, scanContext);
    }

    private Caffeine<Object, Object> newCacheBuilder(long expiresAfterWriteSec, long refreshInterval) {
        Caffeine<Object, Object> cacheBuilder = Caffeine.newBuilder();
        if (expiresAfterWriteSec >= 0) {
            cacheBuilder.expireAfterWrite(expiresAfterWriteSec, SECONDS);
        }

        if (refreshInterval > 0) {
            cacheBuilder.refreshAfterWrite(refreshInterval, SECONDS);
        }

        return cacheBuilder;
    }

    private Caffeine<Object, Object> newCacheBuilderWithMaximumSize(long expiresAfterWriteSec, long refreshInterval,
                                                                    long maximumSize) {
        return newCacheBuilder(expiresAfterWriteSec, refreshInterval).maximumSize(maximumSize);
    }

    public static class IcebergTableName {
        private final String dbName;
        private final String tableName;
        // if as cache key for `getTable`, ignoreSnapshotId = true
        // otherwise it's false
        private boolean ignoreSnapshotId = false;
        // -1 mean it's an empty table without any snapshot.
        private long snapshotId = -1;

        public IcebergTableName(String dbName, String tableName) {
            this(dbName, tableName, -1);
            this.ignoreSnapshotId = true;
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
                    (ignoreSnapshotId || snapshotId == that.snapshotId);
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

    private List<List<String>> getAllCachedPartitionNames() {
        List<List<String>> ans = new ArrayList<>();
        for (Map<String, Partition> kv : partitionCache.asMap().values()) {
            ans.add(new ArrayList<>(kv.keySet()));
        }
        return ans;
    }

    public static long estimatePartitionDataInBytes(PartitionData pd) {
        if (pd == null) {
            return 0L;
        }
        long size = MEMORY_PARTITION_DATA_SIZE;
        size += 64L * pd.size(); // estimate an object as 64Byte in partition data objects
        return size;
    }
    /**
     * Roughly estimates the payload size (in bytes) of the statistics carried on a content file.
     */
    public static long estimateDataFileSizeInBytes(ContentFile<?> file) {
        if (file == null) {
            return 0L;
        }

        long size = MEMORY_DATA_FILE_SIZE;

        size += sizeOfLongMap(file.columnSizes());
        size += sizeOfLongMap(file.valueCounts());
        size += sizeOfLongMap(file.nullValueCounts());
        size += sizeOfLongMap(file.nanValueCounts());
        size += sizeOfByteBufferMap(file.lowerBounds());
        size += sizeOfByteBufferMap(file.upperBounds());
        if (file.keyMetadata() != null) {
            size += file.keyMetadata().remaining();
        }
        if (file.partition() instanceof PartitionData) {
            size += estimatePartitionDataInBytes((PartitionData) file.partition());
        }
        return size;
    }

    private static long sizeOfLongMap(Map<Integer, Long> map) {
        if (map == null || map.isEmpty()) {
            return 0L;
        }

        return 1L * map.size() * (Integer.BYTES + Long.BYTES);
    }

    private static long sizeOfByteBufferMap(Map<Integer, ByteBuffer> map) {
        if (map == null || map.isEmpty()) {
            return 0L;
        }

        long size = 0L;
        for (Map.Entry<Integer, ByteBuffer> entry : map.entrySet()) {
            size += Integer.BYTES; // key
            ByteBuffer buffer = entry.getValue();
            if (buffer != null) {
                size += buffer.remaining();
            }
        }
        return size;
    }

    public Map<String, Long> estimateCount() {
        Map<String, Long> counter = new HashMap<>();
        List<List<String>> partitionNames = getAllCachedPartitionNames();
        counter.put("Database", databases.estimatedSize());
        counter.put("Table", tables.estimatedSize());
        counter.put("TableSnapshot", tables.asMap().values()
                .stream()
                .mapToLong(this::countSnapshotsSafe)
                .sum());
        counter.put("PartitionNames", partitionNames
                .stream()
                .mapToLong(List::size)
                .sum());
        counter.put("ManifestOfDataFile", dataFileCache.asMap().values()
                .stream()
                .mapToLong(Set::size)
                .sum());
        counter.put("ManifestOfDeleteFile", deleteFileCache.asMap().values()
                .stream()
                .mapToLong(Set::size)
                .sum());
        return counter;
    }

    @Override
    public long estimateSize() {
        return Estimator.estimate(tables.asMap(), 10) +
                Estimator.estimate(databases.asMap(), 10) +
                estimateDataFileCacheSize() +
                estimateDeleteFileCacheSize() + Estimator.estimate(metaFileCacheMap, 10);
    }

    private long estimateDataFileCacheSize() {
        int cnt = dataFileCache.asMap().size();
        long size = Estimator.estimate(dataFileCache.asMap().keySet(), cnt);
        for (Set<DataFile> files : dataFileCache.asMap().values()) {
            if (files.isEmpty()) {
                continue;
            }
            StructLike like = files.iterator().next().partition();
            if (like instanceof PartitionData partitionData) {
                // all files using the same PartitionData schema, so ignore it in estimation
                org.apache.avro.Schema schema = partitionData.getSchema();
                Set<Class<?>> ignoreClass = new HashSet<>();
                ignoreClass.add(schema.getClass());
                size += Estimator.estimate(files, 10, ignoreClass);
                size += Estimator.estimate(schema, 10);
            } else {
                size += Estimator.estimate(files, 10);
            }
        }
        return size;
    }

    private long estimateDeleteFileCacheSize() {
        int cnt = deleteFileCache.asMap().size();
        // Estimate size of keys
        long size = Estimator.estimate(deleteFileCache.asMap().keySet(), cnt);

        // Count total delete files across all cache entries
        long totalDeleteFiles = deleteFileCache.asMap().values().stream()
                .mapToLong(Set::size)
                .sum();

        if (totalDeleteFiles == 0) {
            return size;
        }

        // Sample up to 100 DeleteFiles evenly distributed
        int sampleTarget = (int) Math.min(100, totalDeleteFiles);
        long step = totalDeleteFiles / sampleTarget;
        List<DeleteFile> samples = new ArrayList<>(sampleTarget);
        long index = 0;
        long nextSampleIndex = 0;

        outer:
        for (Set<DeleteFile> files : deleteFileCache.asMap().values()) {
            for (DeleteFile file : files) {
                if (index == nextSampleIndex) {
                    samples.add(file);
                    nextSampleIndex += step;
                    if (samples.size() >= sampleTarget) {
                        break outer;
                    }
                }
                index++;
            }
        }

        // Estimate all samples at once, then calculate average and extrapolate
        if (!samples.isEmpty()) {
            long sampleTotalSize = Estimator.estimate(samples, samples.size());
            long avgSize = sampleTotalSize / samples.size();
            size += avgSize * totalDeleteFiles;
        }

        return size;
    }

    private long countSnapshotsSafe(Table table) {
        if (!(table instanceof BaseTable)) {
            return 0;
        }
        BaseTable baseTable = (BaseTable) table;
        TableOperations ops = baseTable.operations();
        if (ops == null || ops.current() == null || ops.current().snapshots() == null) {
            return 0;
        }
        return ops.current().snapshots().size();
    }
}
