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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.common.Config;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.connector.ConnectorMetadataCache;
import com.starrocks.connector.ConnectorMetadataCache.Kind;
import com.starrocks.connector.ConnectorMetadataRequestContext;
import com.starrocks.connector.ConnectorViewDefinition;
import com.starrocks.connector.PlanMode;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.rest.IcebergRESTCatalog;
import com.starrocks.memory.estimate.Estimator;
import com.starrocks.mysql.MysqlCommand;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.DeleteFileIndexCache;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StarRocksIcebergTableScan;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.View;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.Strings;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.SECONDS;

public class CachingIcebergCatalog implements IcebergCatalog {
    private static final Logger LOG = LogManager.getLogger(CachingIcebergCatalog.class);
    public static final long NEVER_CACHE = 0;
    public static final long DEFAULT_CACHE_NUM = 100000;
    // Only emit the partition-load INFO line when a refresh exceeds this many partitions, so the log
    // remains useful for diagnosing slow loads on large tables without flooding for normal-sized ones.
    private static final int PARTITION_LOAD_LOG_THRESHOLD = 10000;
    // REST tables embed short-lived vended credentials (~1h) in their FileIO; cap the table-cache TTL so an
    // idle entry can't outlive its token. Provider-agnostic: bounds cache lifetime, no per-cloud expiry parse.
    private static final long REST_TABLE_CACHE_MAX_TTL_SEC = 3000;
    // The (package-private) class of an avro record schema, for leaving shared avro schemas out of weights.
    private static final Set<Class<?>> AVRO_RECORD_SCHEMA = Set.of(org.apache.avro.Schema.createRecord(
            "r", null, null, false, Collections.emptyList()).getClass());
    private static final ThreadLocal<ConnectContext> TABLE_LOAD_CONTEXT = new ThreadLocal<>();
    private final String catalogName;
    private final IcebergCatalog delegate;
    private final com.github.benmanes.caffeine.cache.LoadingCache<IcebergTableName, Table> tables;
    // When each table object in the cache was loaded from the catalog, by identity: a reload that fails keeps the
    // old object and resets the entry's write time, so the entry's age does not tell how old its credentials are.
    private final com.github.benmanes.caffeine.cache.Cache<Table, Long> tableLoadTimes =
            Caffeine.newBuilder().weakKeys().build();
    private final com.github.benmanes.caffeine.cache.Cache<String, Database> databases;
    private final ExecutorService backgroundExecutor;

    private final IcebergCatalogProperties icebergProperties;
    private final com.github.benmanes.caffeine.cache.Cache<String, Set<DataFile>> dataFileCache;
    private final com.github.benmanes.caffeine.cache.Cache<String, Set<DeleteFile>> deleteFileCache;
    private final DeleteFileIndexCache deleteFileIndexCache;
    private final Map<IcebergTableName, Set<String>> metaFileCacheMap = new ConcurrentHashMap<>(); // table -> metadata file paths
    private final Map<IcebergTableName, Long> tableLatestAccessTime = new ConcurrentHashMap<>();
    private final Map<IcebergTableName, Long> tableLatestRefreshTime = new ConcurrentHashMap<>();
    // Per-table lock strings: avoids catalog-wide serialization during concurrent refresh of different tables.
    private final ConcurrentHashMap<String, String> tableRefreshLockMap = new ConcurrentHashMap<>();

    // this catalog's scope in the metadata caches of manifest files, delete file indexes and partition maps; see
    // ConnectorMetadataCache. Each kind is in a private store sized by the catalog's ratio property, as before,
    // unless the kind's global FE ratio is above 0 and so puts it in the store all Iceberg catalogs share.
    private final String cacheScope;
    private final ConnectorMetadataCache.Store dataFileStore;
    private final ConnectorMetadataCache.Store deleteFileStore;
    private final ConnectorMetadataCache.Store deleteFileIndexStore;
    private final ConnectorMetadataCache.Store partitionStore;
    // Per-table locks of the partition maps: a load (miss, load, put) and dropping the table's maps run under it, so
    // an invalidation never misses a load in flight. The load runs outside the store's compute, so a slow partition
    // scan does not hold the store's internal locks that other tables and catalogs need.
    // Weakly interned, so a table's lock lives only while a thread holds or waits for it.
    private final Interner<String> partitionLocks = Interners.newWeakInterner();
    // partition maps being warmed up in the background, so concurrent misses submit one load, not one per query
    private final Set<IcebergTableName> warmingPartitions = ConcurrentHashMap.newKeySet();

    static {
        ConnectorMetadataCache.registerWeigher(Kind.DATA_FILES, CachingIcebergCatalog::weighContentFiles);
        ConnectorMetadataCache.registerWeigher(Kind.DELETE_FILES, CachingIcebergCatalog::weighContentFiles);
        ConnectorMetadataCache.registerWeigher(Kind.PARTITIONS, CachingIcebergCatalog::weighPartitionEntry);
    }

    public CachingIcebergCatalog(String catalogName, IcebergCatalog delegate, IcebergCatalogProperties icebergProperties,
                                 ExecutorService executorService) {
        this.catalogName = catalogName;
        this.delegate = delegate;
        this.icebergProperties = icebergProperties;
        Preconditions.checkArgument(icebergProperties.isEnableIcebergMetadataCache(),
                "CachingIcebergCatalog should only be instantiated when metadata cache is enabled");
        long tableCacheSize = Math.round(Runtime.getRuntime().maxMemory() *
                icebergProperties.getIcebergTableCacheMemoryUsageRatio());
        this.databases = newCacheBuilderWithMaximumSize(
                icebergProperties.getIcebergMetaCacheTtlSec(),
                NEVER_CACHE, DEFAULT_CACHE_NUM).build();
        long tableCacheTtlSec = icebergProperties.getIcebergMetaCacheTtlSec();
        if (delegate instanceof IcebergRESTCatalog) {
            tableCacheTtlSec = Math.min(tableCacheTtlSec, REST_TABLE_CACHE_MAX_TTL_SEC);
        }
        this.tables = newCacheBuilder(
                tableCacheTtlSec,
                icebergProperties.getIcebergTableCacheRefreshIntervalSec())
                .executor(executorService)
                .maximumWeight(tableCacheSize)
                .weigher((Weigher<IcebergTableName, Table>) this::weighTableEntry)
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
                        return recordLoadTime(delegate.getTable(context != null ? context : new ConnectContext(),
                                key.dbName, key.tableName));
                    }

                    @Override
                    public Table reload(IcebergTableName key, Table oldValue) {
                        try {
                            return recordLoadTime(delegate.getTable(new ConnectContext(), key.dbName, key.tableName));
                        } catch (Exception e) {
                            LOG.warn("refresh table {}.{} failed", key.dbName, key.tableName, e);
                            return oldValue;
                        }
                    }
                });
        this.cacheScope = ConnectorMetadataCache.register(catalogName, icebergProperties.getIcebergMetaCacheTtlSec());
        this.dataFileStore = storeOf(Kind.DATA_FILES, icebergProperties.getIcebergDataFileCacheMemoryUsageRatio());
        // the index has the same budget as the delete files, in a store of its own, as before
        this.deleteFileStore =
                storeOf(Kind.DELETE_FILES, icebergProperties.getIcebergDeleteFileCacheMemoryUsageRatio());
        this.deleteFileIndexStore =
                storeOf(Kind.DELETE_FILE_INDEX, icebergProperties.getIcebergDeleteFileCacheMemoryUsageRatio());
        this.partitionStore = storeOf(Kind.PARTITIONS, icebergProperties.getIcebergPartitionCacheMemoryUsageRatio());
        this.dataFileCache = new ConnectorMetadataCache.ScopedCache<>(dataFileStore, cacheScope, Kind.DATA_FILES);
        this.deleteFileCache =
                new ConnectorMetadataCache.ScopedCache<>(deleteFileStore, cacheScope, Kind.DELETE_FILES);
        // an index references the cached delete files, and is weighed by what it would keep alive after the delete
        // file cache evicts them
        this.deleteFileIndexCache = new DeleteFileIndexCache(deleteFileIndexStore, cacheScope,
                CachingIcebergCatalog::estimateContentFilesSize);

        this.backgroundExecutor = executorService;
    }

    @Override
    public IcebergCatalogType getIcebergCatalogType() {
        return delegate.getIcebergCatalogType();
    }

    @Override
    public Map<String, String> getCatalogProperties() {
        return delegate.getCatalogProperties();
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
        Database db = databases.getIfPresent(dbName);
        if (db != null) {
            return db;
        }

        db = delegate.getDB(connectContext, dbName);
        databases.put(dbName, db);
        return db;
    }

    @Override
    public List<String> listTables(ConnectContext connectContext, String dbName) {
        return delegate.listTables(connectContext, dbName);
    }

    @Override
    public List<String> listViews(ConnectContext connectContext, String dbName) {
        return delegate.listViews(connectContext, dbName);
    }

    @Override
    public Table getTable(ConnectContext connectContext, String dbName, String tableName) throws StarRocksConnectorException {
        IcebergTableName icebergTableName = new IcebergTableName(dbName, tableName);

        if (!tableCacheAllowed(connectContext)) {
            return delegate.getTable(connectContext, dbName, tableName);
        }

        if (ConnectContext.get() == null || ConnectContext.get().getCommand() == MysqlCommand.COM_QUERY) {
            tableLatestAccessTime.put(icebergTableName, System.currentTimeMillis());
        }
        try {
            if (shouldOnlyReadCache(connectContext)) {
                Table cachedTable = tables.getIfPresent(icebergTableName);
                return cachedTable != null ? cachedTable : delegate.getTable(connectContext, dbName, tableName);
            } else {
                TABLE_LOAD_CONTEXT.set(connectContext);
                return tables.get(icebergTableName);
            }
        } catch (NoSuchTableException e) {
            throw e;
        } catch (Exception e) {
            throw new StarRocksConnectorException(
                    String.format("Failed to get iceberg table %s.%s.%s", catalogName, dbName, tableName), e);
        } finally {
            TABLE_LOAD_CONTEXT.remove();
        }
    }

    // do not cache if jwt or oauth2 is used AND it is a REST Catalog.
    private boolean tableCacheAllowed(ConnectContext connectContext) {
        return icebergProperties.isEnableIcebergTableCache() &&
                (Strings.isNullOrEmpty(connectContext.getAuthToken()) || !(delegate instanceof IcebergRESTCatalog));
    }

    private Table recordLoadTime(Table table) {
        if (table != null) {
            tableLoadTimes.put(table, System.currentTimeMillis());
        }
        return table;
    }

    @Override
    public OptionalLong tableLoadedAtMs(ConnectContext connectContext, String dbName, String tableName, Table table,
                                        long nowMs) {
        if (connectContext != null && !tableCacheAllowed(connectContext)) {
            return OptionalLong.of(nowMs);
        }
        Long loadedAt = tableLoadTimes.getIfPresent(table);
        return loadedAt == null ? OptionalLong.empty() : OptionalLong.of(loadedAt);
    }

    @Override
    public Set<String> statsColumnsKeptByManifestCache(Table table, Schema schema, Set<String> columnNames) {
        if (columnNames.isEmpty() || !icebergProperties.isIcebergManifestCacheWithColumnStatistics()) {
            return Set.of();
        }
        Map<Integer, String> namesById = new HashMap<>();
        for (String name : columnNames) {
            Types.NestedField field = schema.findField(name);
            if (field != null) {
                namesById.put(field.fieldId(), name);
            }
        }
        if (namesById.isEmpty()) {
            return Set.of();
        }
        int limit = Math.max(icebergProperties.getIcebergManifestCacheStatisticsMaxColumns(), 0);
        Set<String> kept = new HashSet<>();
        for (Integer id : StarRocksIcebergTableScan.statsColumnIdsKeptByCache(table, schema, limit, namesById.keySet())) {
            kept.add(namesById.get(id));
        }
        return kept;
    }

    @Override
    public Table getTableUncached(ConnectContext connectContext, String dbName, String tableName)
            throws StarRocksConnectorException {
        return delegate.getTableUncached(connectContext, dbName, tableName);
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

    @Override
    public boolean registerTable(ConnectContext context, String dbName, String tableName, String metadataFileLocation) {
        return delegate.registerTable(context, dbName, tableName, metadataFileLocation);
    }

    public View getView(ConnectContext connectContext, String dbName, String viewName) {
        return delegate.getView(connectContext, dbName, viewName);
    }

    private Map<String, Partition> loadPartitions(IcebergTableName key) {
        ConnectContext context = new ConnectContext();
        context.setOnlyReadIcebergCache(true);
        Table nativeTable = getTable(context, key.dbName, key.tableName);
        IcebergTable icebergTable =
                IcebergTable.builder()
                        .setCatalogDBName(key.dbName)
                        .setSrTableName(key.tableName)
                        .setCatalogTableName(key.tableName)
                        .setNativeTable(nativeTable).build();
        Map<String, Partition> partitions =
                delegate.getPartitions(icebergTable, key.snapshotId, null);
        if (partitions.size() > PARTITION_LOAD_LOG_THRESHOLD) {
            // -1 is used by callers as "use current snapshot" (see IcebergCatalog#getPartitions);
            // resolve it here so the summary and logged snapshot id reflect the snapshot actually scanned.
            Snapshot snapshot = key.snapshotId == -1
                    ? nativeTable.currentSnapshot() : nativeTable.snapshot(key.snapshotId);
            long loggedSnapshotId = snapshot != null ? snapshot.snapshotId() : key.snapshotId;
            Map<String, String> summary =
                    (snapshot != null && snapshot.summary() != null)
                            ? snapshot.summary() : Collections.emptyMap();
            LOG.info("Loaded large iceberg partition set: catalog={}, table={}.{}, snapshot={}, "
                            + "partitions={}, dataFiles={}, deleteFiles={}, specs={}, "
                            + "partitionFields={}",
                    catalogName, key.dbName, key.tableName, loggedSnapshotId,
                    partitions.size(),
                    summary.getOrDefault(SnapshotSummary.TOTAL_DATA_FILES_PROP, "?"),
                    summary.getOrDefault(SnapshotSummary.TOTAL_DELETE_FILES_PROP, "?"),
                    nativeTable.specs().size(),
                    nativeTable.spec().fields().size());
        }
        return partitions;
    }

    private static ConnectorMetadataCache.Store storeOf(Kind kind, double catalogRatio) {
        return ConnectorMetadataCache.storeOf(kind, catalogRatio);
    }

    // the distinct stores this catalog's entries are in
    private static Set<ConnectorMetadataCache.Store> distinct(ConnectorMetadataCache.Store... stores) {
        Set<ConnectorMetadataCache.Store> result = Collections.newSetFromMap(new IdentityHashMap<>());
        result.addAll(Arrays.asList(stores));
        return result;
    }

    private Set<ConnectorMetadataCache.Store> stores() {
        return distinct(dataFileStore, deleteFileStore, deleteFileIndexStore, partitionStore);
    }

    private String partitionLock(IcebergTableName table) {
        return partitionLocks.intern(
                table.dbName.toLowerCase(Locale.ROOT) + "." + table.tableName.toLowerCase(Locale.ROOT));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Partition> getCachedPartitions(IcebergTableName key) {
        Object cached = partitionStore.getIfPresent(cacheScope, Kind.PARTITIONS, key);
        if (cached != null) {
            return (Map<String, Partition>) cached;
        }
        synchronized (partitionLock(key)) {
            cached = partitionStore.getIfPresent(cacheScope, Kind.PARTITIONS, key);
            if (cached == null) {
                cached = loadPartitions(key);
                partitionStore.put(cacheScope, Kind.PARTITIONS, key, cached);
            }
            return (Map<String, Partition>) cached;
        }
    }

    // Drops the cached partition maps of every snapshot of the table, after a load in flight has published its map.
    private void invalidatePartitions(IcebergTableName table) {
        synchronized (partitionLock(table)) {
            partitionStore.invalidateIf(cacheScope, Kind.PARTITIONS, key -> {
                IcebergTableName cachedKey = (IcebergTableName) key;
                return cachedKey.dbName.equalsIgnoreCase(table.dbName)
                        && cachedKey.tableName.equalsIgnoreCase(table.tableName);
            });
        }
    }

    @Override
    public Map<String, Partition> getPartitions(IcebergTable icebergTable, long snapshotId,
                                                ExecutorService executorService) {
        IcebergTableName key =
                new IcebergTableName(icebergTable.getCatalogDBName(), icebergTable.getCatalogTableName(), snapshotId);
        return getCachedPartitions(key);
    }

    @Override
    public List<String> listPartitionNames(IcebergTable icebergTable, ConnectorMetadataRequestContext requestContext,
                                           ExecutorService executorService) {
        SessionVariable sv = ConnectContext.getSessionVariableOrDefault();
        // optimization for query mv rewrite, we can optionally return null to bypass it.
        // if we don't have cache right now, which means it probably takes time to load it during query,
        // so we can do load in background while return null to bypass this synchronous process.
        if (requestContext.isQueryMVRewrite() && sv.isEnableConnectorAsyncListPartitions()) {
            long snapshotId = requestContext.getSnapshotId();
            IcebergTableName key =
                    new IcebergTableName(icebergTable.getCatalogDBName(), icebergTable.getCatalogTableName(), snapshotId);
            Object cacheValue = partitionStore.getIfPresent(cacheScope, Kind.PARTITIONS, key);
            if (cacheValue == null) {
                try {
                    if (warmingPartitions.add(key)) {
                        try {
                            backgroundExecutor.submit(() -> {
                                try {
                                    getCachedPartitions(key);
                                } catch (Exception e) {
                                    // nobody reads the future, so report the failure here
                                    LOG.warn("Failed to load iceberg partitions of {}.{}.{} at snapshot {} in background",
                                            catalogName, key.dbName, key.tableName, key.snapshotId, e);
                                } finally {
                                    warmingPartitions.remove(key);
                                }
                            });
                        } catch (RejectedExecutionException e) {
                            warmingPartitions.remove(key);
                            throw e;
                        }
                    }
                    return null;
                } catch (RejectedExecutionException e) {
                    // The connector was shut down while this query was in flight; load synchronously instead.
                }
            }
        }
        return IcebergCatalog.super.listPartitionNames(icebergTable, requestContext, executorService);
    }

    @Override
    public void deleteUncommittedDataFiles(List<String> fileLocations) {
        delegate.deleteUncommittedDataFiles(fileLocations);
    }

    @Override
    public void refreshTable(String dbName, String tableName, ConnectContext ctx, ExecutorService executorService) {
        String lockKey = dbName.toLowerCase(Locale.ROOT) + "." + tableName.toLowerCase(Locale.ROOT);
        tableRefreshLockMap.putIfAbsent(lockKey, lockKey);
        String lock = tableRefreshLockMap.get(lockKey);
        synchronized (lock) {
            refreshTableUnderLock(dbName, tableName, ctx, executorService);
        }
    }

    private void refreshTableUnderLock(String dbName, String tableName, ConnectContext ctx, ExecutorService executorService) {
        IcebergTableName icebergTableName = new IcebergTableName(dbName, tableName);
        Table cachedTable = tables.getIfPresent(icebergTableName);
        if (cachedTable == null) {
            invalidatePartitions(icebergTableName);
        } else {
            BaseTable currentTable = (BaseTable) cachedTable;
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
                if (refreshTable(currentTable, updateTable, dbName, tableName, ctx, executorService)) {
                    LOG.info("Finished to refresh iceberg table {}.{}", dbName, tableName);
                }
            } else {
                // Metadata unchanged keeps the partition/file caches valid; still swap in the reloaded
                // table so the cache stops serving the old (expiring) vended FileIO token.
                replaceCachedTable(icebergTableName, currentTable, updateTable);
            }
        }
    }

    /**
     * Swap the reloaded table into the cache only if the entry still holds the table this refresh started
     * from. `updatedTable` was loaded from the remote catalog before this call, so if the entry was
     * invalidated meanwhile (e.g. by a commit on this FE) or replaced by a concurrent load, writing it back
     * could resurrect metadata older than what the cache already moved past.
     * On a lost race the entry is invalidated instead: a concurrently loaded table may itself be older than
     * `updatedTable`, and the next access then loads metadata at least as new as what this refresh saw.
     */
    private boolean replaceCachedTable(IcebergTableName key, Table currentTable, Table updatedTable) {
        if (tables.asMap().replace(key, currentTable, updatedTable)) {
            return true;
        }
        tables.invalidate(key);
        LOG.info("Skip refreshing iceberg caching catalog table {}.{}: cache entry was invalidated or " +
                "replaced concurrently, invalidate it to reload on next access", key.dbName, key.tableName);
        return false;
    }

    private boolean refreshTable(BaseTable currentTable, BaseTable updatedTable,
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
        if (!replaceCachedTable(keyWithoutSnap, currentTable, updatedTable)) {
            return false;
        }

        synchronized (partitionLock(baseIcebergTableName)) {
            partitionStore.invalidate(cacheScope, Kind.PARTITIONS, baseIcebergTableName);
        }
        getCachedPartitions(updatedIcebergTableName);

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
            return true;
        }

        StarRocksIcebergTableScanContext scanContext = new StarRocksIcebergTableScanContext(
                catalogName, dbName, tableName, PlanMode.LOCAL);
        StarRocksIcebergTableScan tableScan = (StarRocksIcebergTableScan) getTableScan(updatedTable, scanContext)
                .planWith(executorService)
                .useSnapshot(updatedSnapshotId);
        tableScan.refreshDataFileCache(manifestFiles);

        tableLatestRefreshTime.put(new IcebergTableName(dbName, tableName), System.currentTimeMillis());
        LOG.info("Refreshed {} iceberg manifests on the table [{}.{}]", manifestFiles.size(), dbName, tableName);
        return true;
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
        invalidatePartitions(key);
    }

    @Override
    public void invalidateTableCache(String dbName, String tableName) {
        IcebergTableName key = new IcebergTableName(dbName, tableName);
        tables.invalidate(key);
    }

    @Override
    public void shutdown() {
        ConnectorMetadataCache.unregister(cacheScope, stores());
        delegate.shutdown();
    }

    @Override
    public void invalidateCache(String dbName, String tableName) {
        IcebergTableName key = new IcebergTableName(dbName, tableName);
        invalidateCache(key);
    }

    private void invalidateCache(IcebergTableName key) {
        tables.invalidate(key);
        // will invalidate all snapshots of this table
        invalidatePartitions(key);
        deleteFileIndexCache.invalidate(key);
        tableLatestAccessTime.remove(key);
        tableLatestRefreshTime.remove(key);

        Set<String> paths = metaFileCacheMap.remove(key);
        if (paths != null && !paths.isEmpty()) {
            dataFileCache.invalidateAll(paths);
            deleteFileCache.invalidateAll(paths);
        }
    }

    @VisibleForTesting
    DeleteFileIndexCache getDeleteFileIndexCache() {
        return deleteFileIndexCache;
    }

    @Override
    public StarRocksIcebergTableScan getTableScan(Table table, StarRocksIcebergTableScanContext scanContext) {
        scanContext.setDataFileCache(dataFileCache);
        scanContext.setDeleteFileCache(deleteFileCache);
        scanContext.setDeleteFileIndexCache(deleteFileIndexCache);
        scanContext.setMetaFileCacheMap(metaFileCacheMap);
        scanContext.setDataFileCacheWithMetrics(icebergProperties.isIcebergManifestCacheWithColumnStatistics());
        scanContext.setDataFileCacheStatsColumnLimit(icebergProperties.getIcebergManifestCacheStatisticsMaxColumns());
        scanContext.setEnableCacheDataFileIdentifierColumnMetrics(
                icebergProperties.enableCacheDataFileIdentifierColumnStatistics());
        scanContext.setOnlyReadCache(shouldOnlyReadCache(scanContext.getConnectContext()));
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

    // Returns the cached data files of this manifest whose locations are in `locations`, or null unless the
    // manifest is cached completely (its live file count is known and matches the cached entry).
    public Map<String, DataFile> findCachedDataFiles(ManifestFile manifest, Set<String> locations) {
        Set<DataFile> files = dataFileCache.getIfPresent(manifest.path());
        Integer added = manifest.addedFilesCount();
        Integer existing = manifest.existingFilesCount();
        if (files == null || added == null || existing == null || files.size() != added + existing) {
            return null;
        }
        Map<String, DataFile> found = new HashMap<>();
        for (DataFile file : files) {
            if (locations.contains(file.location())) {
                if (file.specId() != manifest.partitionSpecId()) {
                    return null;
                }
                found.put(file.location(), file);
            }
        }
        return found;
    }

    // We still allow reads from caches, but skip populating or refreshing them when this flag is set.
    private boolean shouldOnlyReadCache(ConnectContext context) {
        return context != null && context.isOnlyReadIcebergCache();
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

    private static long weighContentFiles(Object key, Object files) {
        return Estimator.estimate(key) + estimateContentFilesSize((Set<? extends ContentFile<?>>) files);
    }


    private int weighTableEntry(IcebergTableName key, Table table) {
        long size = Estimator.estimate(key) + weighPrivateTableState(table);
        if (size > Integer.MAX_VALUE) {
            LOG.warn("Table cache entry size for key {} is too large: {} bytes", key, size);
            return Integer.MAX_VALUE;
        }
        return (int) size;
    }

    // Charge an entry only for the metadata it exclusively owns, which is what evicting it actually
    // frees. Walking the whole table instead reaches the TableOperations, and from there the objects
    // the catalog shares across all of its tables -- the Hadoop configuration, the metastore client
    // pool, the FileIO, the metrics reporter. Those exist once per catalog, so charging them to every
    // entry makes the ledger grow with the entry count while the heap does not: the cache then
    // declares itself full while holding a fraction of maximumWeight and evicts continuously.
    private long weighPrivateTableState(Table table) {
        if (table == null) {
            return 0;
        }
        if (table instanceof BaseTable) {
            TableOperations ops = ((BaseTable) table).operations();
            TableMetadata metadata = ops != null ? ops.current() : null;
            if (metadata != null) {
                // The current snapshot caches its manifest list once the table is planned. When it is the newest
                // snapshot it is the last element of metadata.snapshots(), which the sampled walk of that list does
                // not reach, so charge it first to count its manifests in full. Otherwise (a rollback, or a newer
                // commit on a branch) it may sit on a sampled position, where charging it first would make the
                // sample extrapolate its zero; leave it to the walk then. A v1 table has no sequence numbers to
                // tell. Every manifest of every table points to the same avro schema of the manifest list, which
                // no entry owns.
                Snapshot current = metadata.currentSnapshot();
                if (current != null && metadata.formatVersion() >= 2 &&
                        current.sequenceNumber() == metadata.lastSequenceNumber()) {
                    return Estimator.estimateAll(AVRO_RECORD_SCHEMA, current, metadata);
                }
                return Estimator.estimateAll(AVRO_RECORD_SCHEMA, metadata);
            }
        }
        // Nothing we can attribute to this entry alone; charge its own footprint rather than a whole
        // shared object graph. Deliberately never throws: the weigher runs on Caffeine's write path.
        return Estimator.shallow(table);
    }

    // Each entry holds a whole table's partition map, which is unbounded in bytes (a table can have
    // millions of partitions). Sample the partition values so weighing stays cheap on large maps.
    private static long weighPartitionEntry(Object key, Object partitions) {
        return Estimator.estimate(key) + Estimator.estimate(partitions, 3);
    }

    @SuppressWarnings("unchecked")
    private static long estimateContentFilesSize(Set<? extends ContentFile<?>> files) {
        if (files == null || files.isEmpty()) {
            return 0L;
        }
        ContentFile<?> first = files.iterator().next();
        // Compact statistics are shared by every file of the manifest and skipped by the per-file sample.
        long sharedStatsSize = first instanceof DataFileWrapper wrapper && wrapper.columnStats() != null
                ? wrapper.columnStats().retainedBytes() : 0L;
        StructLike like = first.partition();
        if (like instanceof PartitionData partitionData) {
            org.apache.avro.Schema schema = partitionData.getSchema();
            Set<Class<?>> ignoreClass = new HashSet<>();
            ignoreClass.add(schema.getClass());
            return Estimator.estimate(files, 3, ignoreClass) + Estimator.estimate(schema, 3) + sharedStatsSize;
        } else {
            return Estimator.estimate(files, 3) + sharedStatsSize;
        }
    }

    public Map<String, Long> estimateCount() {
        Map<String, Long> counter = new HashMap<>();
        counter.put("Database", databases.estimatedSize());
        counter.put("Table", tables.estimatedSize());
        counter.put("TableSnapshot", tables.asMap().values()
                .stream()
                .mapToLong(this::countSnapshotsSafe)
                .sum());
        counter.put("PartitionNames", stats(partitionStore, Kind.PARTITIONS).items());
        counter.put("ManifestOfDataFile", stats(dataFileStore, Kind.DATA_FILES).items());
        counter.put("ManifestOfDeleteFile", stats(deleteFileStore, Kind.DELETE_FILES).items());
        return counter;
    }

    private ConnectorMetadataCache.Stats stats(ConnectorMetadataCache.Store store, Kind kind) {
        return store.stats(cacheScope, kind);
    }

    // The partition, data file and delete file caches report their weigher ledger, so sys.fe_memory_usage
    // shows exactly what eviction is bounded by; their values do not change after they are weighed. A cached
    // table does: planning loads its current snapshot's manifest list after the load weighed it, so the table
    // term weighs the entries again, with the weigher's own private-state rule (walking whole tables reached
    // the catalog-shared objects and reported several times the real size).
    @Override
    public long estimateSize() {
        return estimateTableCacheSize() +
                stats(partitionStore, Kind.PARTITIONS).weight() +
                stats(dataFileStore, Kind.DATA_FILES).weight() +
                stats(deleteFileStore, Kind.DELETE_FILES).weight() +
                Estimator.estimate(databases.asMap(), 10) +
                Estimator.estimate(metaFileCacheMap, 10) +
                Estimator.estimate(tableLatestAccessTime, 10) +
                Estimator.estimate(tableLatestRefreshTime, 10) +
                Estimator.estimate(tableRefreshLockMap, 10);
    }

    private long estimateTableCacheSize() {
        long size = 0;
        for (Map.Entry<IcebergTableName, Table> entry : tables.asMap().entrySet()) {
            size += Estimator.estimate(entry.getKey()) + weighPrivateTableState(entry.getValue());
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
