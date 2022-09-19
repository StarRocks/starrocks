// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveMetaStoreTableInfo;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.HudiTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.external.HiveMetaStoreTableUtils;
import com.starrocks.external.ObjectStorageUtils;
import com.starrocks.server.GlobalStateMgr;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import static com.google.common.cache.CacheLoader.asyncReloading;
import static com.starrocks.external.HiveMetaStoreTableUtils.getAllColumnNames;
import static com.starrocks.external.HiveMetaStoreTableUtils.getPartitionColumns;
import static java.util.concurrent.TimeUnit.SECONDS;

public class HiveMetaCache {
    private static final Logger LOG = LogManager.getLogger(HiveMetaCache.class);
    private static final long MAX_TABLE_CACHE_SIZE = 1000L;
    private static final long MAX_PARTITION_CACHE_SIZE = MAX_TABLE_CACHE_SIZE * 1000L;

    // Pulling the value of the latest state every time when getting databaseNames or tableNames
    private static final long MAX_NAMES_CACHE_SIZE = 0L;
    private final HiveMetaClient client;
    private final String resourceName;


    // HivePartitionKeysKey => ImmutableMap<PartitionKey -> PartitionId>
    // for unPartitioned table, partition map is: ImmutableMap<>.of(new PartitionKey(), PartitionId)
    LoadingCache<HivePartitionKeysKey, ImmutableMap<PartitionKey, Long>> partitionKeysCache;
    // HivePartitionKey => Partitions
    LoadingCache<HivePartitionKey, HivePartition> partitionsCache;

    // statistic cache
    // HiveTableKey => HiveTableStatistic
    LoadingCache<HiveTableKey, HiveTableStats> tableStatsCache;
    // HivePartitionKey => PartitionStatistic
    LoadingCache<HivePartitionKey, HivePartitionStats> partitionStatsCache;

    // HiveTableColumnsKey => ImmutableMap<ColumnName -> HiveColumnStats>
    LoadingCache<HiveTableColumnsKey, ImmutableMap<String, HiveColumnStats>> tableColumnStatsCache;

    // HiveTableName => Table
    LoadingCache<HiveTableName, Table> tableCache;
    LoadingCache<String, Database> databaseCache;

    LoadingCache<String, List<String>> databaseNamesCache;
    LoadingCache<String, List<String>> tableNamesCache;


    public HiveMetaCache(HiveMetaClient hiveMetaClient, Executor executor) {
        this(hiveMetaClient, executor, null);
    }

    public HiveMetaCache(HiveMetaClient hiveMetaClient, Executor executor, String resourceName) {
        this.client = hiveMetaClient;
        this.resourceName = resourceName;
        init(executor);
    }

    private void init(Executor executor) {
        partitionKeysCache = newCacheBuilder(MAX_TABLE_CACHE_SIZE)
                .build(asyncReloading(new CacheLoader<HivePartitionKeysKey, ImmutableMap<PartitionKey, Long>>() {
                    @Override
                    public ImmutableMap<PartitionKey, Long> load(HivePartitionKeysKey key) throws Exception {
                        return loadPartitionKeys(key);
                    }
                }, executor));

        partitionsCache = newCacheBuilder(MAX_PARTITION_CACHE_SIZE)
                .build(asyncReloading(new CacheLoader<HivePartitionKey, HivePartition>() {
                    @Override
                    public HivePartition load(HivePartitionKey key) throws Exception {
                        return loadPartition(key);
                    }
                }, executor));

        tableStatsCache = newCacheBuilder(MAX_TABLE_CACHE_SIZE)
                .build(asyncReloading(new CacheLoader<HiveTableKey, HiveTableStats>() {
                    @Override
                    public HiveTableStats load(HiveTableKey key) throws Exception {
                        return loadTableStats(key);
                    }
                }, executor));

        partitionStatsCache = newCacheBuilder(MAX_PARTITION_CACHE_SIZE)
                .build(asyncReloading(new CacheLoader<HivePartitionKey, HivePartitionStats>() {
                    @Override
                    public HivePartitionStats load(HivePartitionKey key) throws Exception {
                        return loadPartitionStats(key);
                    }
                }, executor));

        tableColumnStatsCache = newCacheBuilder(MAX_TABLE_CACHE_SIZE)
                .build(asyncReloading(new CacheLoader<HiveTableColumnsKey, ImmutableMap<String, HiveColumnStats>>() {
                    @Override
                    public ImmutableMap<String, HiveColumnStats> load(HiveTableColumnsKey key) throws Exception {
                        return loadTableColumnStats(key);
                    }
                }, executor));

        databaseNamesCache = newCacheBuilder(MAX_NAMES_CACHE_SIZE)
                .build(asyncReloading(new CacheLoader<String, List<String>>() {
                    @Override
                        public List<String> load(String key) throws Exception {
                        return loadAllDatabaseNames();
                    }
                }, executor));

        tableNamesCache = newCacheBuilder(MAX_NAMES_CACHE_SIZE)
                .build(asyncReloading(new CacheLoader<String, List<String>>() {
                    @Override
                    public List<String> load(String key) throws Exception {
                        return loadAllTableNames(key);
                    }
                }, executor));

        tableCache = newCacheBuilder(MAX_TABLE_CACHE_SIZE)
                .build(asyncReloading(new CacheLoader<HiveTableName, Table>() {
                    @Override
                    public Table load(HiveTableName key) throws Exception {
                        return loadTable(key);
                    }
                }, executor));

        databaseCache = newCacheBuilder(MAX_TABLE_CACHE_SIZE)
                .build(asyncReloading(new CacheLoader<String, Database>() {
                    @Override
                    public Database load(String key) throws Exception {
                        return loadDatabase(key);
                    }
                }, executor));
    }

    /**
     * Currently we only support either refreshAfterWrite or automatic refresh by events.
     */
    private static CacheBuilder<Object, Object> newCacheBuilder(long maximumSize) {
        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
        cacheBuilder.expireAfterWrite(Config.hive_meta_cache_ttl_s, SECONDS);
        if (!Config.enable_hms_events_incremental_sync &&
                Config.hive_meta_cache_ttl_s > Config.hive_meta_cache_refresh_interval_s) {
            cacheBuilder.refreshAfterWrite(Config.hive_meta_cache_refresh_interval_s, SECONDS);
        }
        cacheBuilder.maximumSize(maximumSize);
        return cacheBuilder;
    }

    private ImmutableMap<PartitionKey, Long> loadPartitionKeys(HivePartitionKeysKey key) throws DdlException {
        Map<PartitionKey, Long> partitionKeys = client.getPartitionKeys(key.getDatabaseName(),
                key.getTableName(),
                key.getPartitionColumns(),
                key.getTableType() == Table.TableType.HUDI);
        return ImmutableMap.copyOf(partitionKeys);
    }

    private HivePartition loadPartition(HivePartitionKey key) throws DdlException {
        if (key.getTableType() == Table.TableType.HUDI) {
            return client.getHudiPartition(key.getDatabaseName(), key.getTableName(), key.getPartitionValues());
        } else {
            return client.getPartition(key.getDatabaseName(), key.getTableName(), key.getPartitionValues());
        }
    }

    private HiveTableStats loadTableStats(HiveTableKey key) throws DdlException {
        return client.getTableStats(key.getDatabaseName(), key.getTableName());
    }

    private HivePartitionStats loadPartitionStats(HivePartitionKey key) throws Exception {
        HivePartitionStats partitionStats =
                client.getPartitionStats(key.getDatabaseName(), key.getTableName(), key.getPartitionValues());
        HivePartition partition = partitionsCache.get(key);
        long totalFileBytes = 0;
        for (HdfsFileDesc fileDesc : partition.getFiles()) {
            totalFileBytes += fileDesc.getLength();
        }
        partitionStats.setTotalFileBytes(totalFileBytes);
        return partitionStats;
    }

    private ImmutableMap<String, HiveColumnStats> loadTableColumnStats(HiveTableColumnsKey key) throws Exception {
        if (key.getPartitionColumns().size() > 0) {
            List<PartitionKey> partitionKeys = new ArrayList<>(partitionKeysCache
                    .get(new HivePartitionKeysKey(key.getDatabaseName(),
                            key.getTableName(),
                            key.getTableType(),
                            key.getPartitionColumns())).keySet());
            return ImmutableMap.copyOf(client.getTableLevelColumnStatsForPartTable(key.getDatabaseName(),
                    key.getTableName(),
                    partitionKeys,
                    key.getPartitionColumns(),
                    key.getColumnNames(),
                    key.getTableType() == Table.TableType.HUDI));
        } else {
            return ImmutableMap.copyOf(client.getTableLevelColumnStatsForUnpartTable(key.getDatabaseName(),
                    key.getTableName(),
                    key.getColumnNames()));
        }
    }

    public ImmutableMap<PartitionKey, Long> getPartitionKeys(HiveMetaStoreTableInfo hmsTable) throws DdlException {
        List<Column> partColumns = getPartitionColumns(hmsTable);
        try {
            return partitionKeysCache.get(new HivePartitionKeysKey(hmsTable.getDb(),
                    hmsTable.getTable(), hmsTable.getTableType(), partColumns));
        } catch (ExecutionException e) {
            LOG.warn("get partition keys failed", e);
            throw new DdlException("get partition keys failed: " + e.getMessage());
        }
    }

    public HivePartition getPartition(HiveMetaStoreTableInfo hmsTable,
                                      PartitionKey partitionKey) throws DdlException {
        List<String> partitionValues = Utils.getPartitionValues(partitionKey,
                hmsTable.getTableType() == Table.TableType.HUDI);
        try {
            return partitionsCache.get(new HivePartitionKey(hmsTable.getDb(), hmsTable.getTable(),
                    hmsTable.getTableType(), partitionValues));
        } catch (ExecutionException e) {
            throw new DdlException("get partition detail failed: " + e.getMessage());
        }
    }

    public HiveTableStats getTableStats(String dbName, String tableName) throws DdlException {
        try {
            return tableStatsCache.get(new HiveTableKey(dbName, tableName));
        } catch (ExecutionException e) {
            throw new DdlException("get table stats failed: " + e.getMessage());
        }
    }

    public HivePartitionStats getPartitionStats(HiveMetaStoreTableInfo hmsTable,
                                                PartitionKey partitionKey) throws DdlException {
        List<String> partValues =
                Utils.getPartitionValues(partitionKey, hmsTable.getTableType() == Table.TableType.HUDI);
        HivePartitionKey key =
                new HivePartitionKey(hmsTable.getDb(), hmsTable.getTable(), hmsTable.getTableType(), partValues);
        try {
            return partitionStatsCache.get(key);
        } catch (ExecutionException e) {
            throw new DdlException("get table partition stats failed: " + e.getMessage());
        }
    }

    // NOTE: always using all column names in HiveTable as request param, this will get the best cache effect.
    // set all partitions keys to partitionKeys param, if table is partition table
    public ImmutableMap<String, HiveColumnStats> getTableLevelColumnStats(HiveMetaStoreTableInfo hmsTable)
            throws DdlException {
        List<Column> partColumns = getPartitionColumns(hmsTable);
        // NOTE: Using allColumns as param to get column stats, we will get the best cache effect.
        List<String> allColumnNames = new ArrayList<>(hmsTable.getNameToColumn().keySet());
        HiveTableColumnsKey key = new HiveTableColumnsKey(hmsTable.getDb(),
                hmsTable.getTable(), partColumns, allColumnNames, hmsTable.getTableType());
        try {
            return tableColumnStatsCache.get(key);
        } catch (ExecutionException e) {
            throw new DdlException("get table level column stats failed: " + e.getMessage());
        }
    }

    public List<String> getAllDatabaseNames() throws DdlException {
        try {
            return databaseNamesCache.get("");
        } catch (ExecutionException e) {
            throw new DdlException("Failed to get all databases name on " + resourceName);
        }
    }

    private List<String> loadAllDatabaseNames() throws DdlException {
        return client.getAllDatabaseNames();
    }

    public List<String> getAllTableNames(String dbName) throws DdlException {
        try {
            return tableNamesCache.get(dbName);
        } catch (ExecutionException e) {
            throw new DdlException("Failed to get all tables name on database: " + dbName);
        }
    }

    private List<String> loadAllTableNames(String dbName) throws DdlException {
        return client.getAllTableNames(dbName);
    }

    public Table getTableFromCache(HiveTableName hiveTableName) {
        return tableCache.getIfPresent(hiveTableName);
    }

    public Table getTable(HiveTableName hiveTableName) {
        try {
            return tableCache.get(hiveTableName);
        } catch (Exception e) {
            LOG.error("Failed to get table {}", hiveTableName, e);
            return null;
        }
    }

    // load table according to hiveTableName, however the table could be hiveTable or hudiTable
    // we should check table type by input format from hms
    private Table loadTable(HiveTableName hiveTableName) throws TException, DdlException {
        org.apache.hadoop.hive.metastore.api.Table hiveTable = client.getTable(hiveTableName);
        Table table = null;
        if (HudiTable.fromInputFormat(hiveTable.getSd().getInputFormat()) != HudiTable.HudiTableType.UNKNOWN) {
            table = HiveMetaStoreTableUtils.convertHudiConnTableToSRTable(hiveTable, resourceName);
        } else {
            table = HiveMetaStoreTableUtils.convertHiveConnTableToSRTable(hiveTable, resourceName);
        }
        tableColumnStatsCache.invalidate(new HiveTableColumnsKey(hiveTableName.getDatabaseName(),
                hiveTableName.getTableName(), null, null, table.getType()));

        return table;
    }

    public Database getDb(String dbName) {
        try {
            return databaseCache.get(dbName);
        } catch (Exception e) {
            LOG.error("Failed to get database {}", dbName, e);
            return null;
        }
    }

    private Database loadDatabase(String dbName) throws TException {
        // Check whether the db exists, if not, an exception will be thrown here
        org.apache.hadoop.hive.metastore.api.Database db = client.getDb(dbName);
        if (db == null || db.getName() == null) {
            throw new TException("Hive db " + dbName + " doesn't exist");
        }
        return HiveMetaStoreTableUtils.convertToSRDatabase(dbName);
    }

    public void alterTableByEvent(HiveTableKey tableKey, HivePartitionKey hivePartitionKey,
                                  StorageDescriptor sd, Map<String, String> params) throws Exception {
        HiveTableStats tableStats = new HiveTableStats(Utils.getRowCount(params), Utils.getTotalSize(params));
        tableStatsCache.put(tableKey, tableStats);
        alterPartitionByEvent(hivePartitionKey, sd, params);
    }

    public synchronized void addPartitionKeyByEvent(HivePartitionKeysKey hivePartitionKeysKey,
                                                    PartitionKey partitionKey, HivePartitionKey hivePartitionKey) {
        ImmutableMap<PartitionKey, Long> cachedPartitions = partitionKeysCache.getIfPresent(hivePartitionKeysKey);
        if (cachedPartitions == null) {
            return;
        }
        Map<PartitionKey, Long> partitions = Maps.newHashMap(cachedPartitions);
        partitions.putIfAbsent(partitionKey, client.nextPartitionId());
        partitionKeysCache.put(hivePartitionKeysKey, ImmutableMap.copyOf(partitions));
        partitionsCache.invalidate(hivePartitionKey);
        partitionStatsCache.invalidate(hivePartitionKey);
    }

    private HivePartition getPartitionByEvent(StorageDescriptor sd) throws Exception {
        RemoteFileInputFormat format = RemoteFileInputFormat.fromHdfsInputFormatClass(sd.getInputFormat());
        String path = ObjectStorageUtils.formatObjectStoragePath(sd.getLocation());
        boolean isSplittable = ObjectStorageUtils.isObjectStorage(path) ||
                RemoteFileInputFormat.isSplittable(sd.getInputFormat());
        List<HdfsFileDesc> fileDescs = client.getHdfsFileDescs(path, isSplittable, sd);
        return new HivePartition(format, ImmutableList.copyOf(fileDescs), path);
    }

    public void alterPartitionByEvent(HivePartitionKey hivePartitionKey,
                                      StorageDescriptor sd, Map<String, String> params) throws Exception {
        HivePartition updatedHivePartition = getPartitionByEvent(sd);
        partitionsCache.put(hivePartitionKey, updatedHivePartition);

        HivePartitionStats partitionStats = new HivePartitionStats(Utils.getRowCount(params));
        long totalFileBytes = 0;
        for (HdfsFileDesc fileDesc : updatedHivePartition.getFiles()) {
            totalFileBytes += fileDesc.getLength();
        }
        partitionStats.setTotalFileBytes(totalFileBytes);
        partitionStatsCache.put(hivePartitionKey, partitionStats);
    }

    public synchronized void dropPartitionKeyByEvent(HivePartitionKeysKey hivePartitionKeysKey,
                                                     PartitionKey partitionKey, HivePartitionKey hivePartitionKey) {
        ImmutableMap<PartitionKey, Long> cachedPartitions = partitionKeysCache.getIfPresent(hivePartitionKeysKey);
        if (cachedPartitions == null) {
            return;
        }

        Map<PartitionKey, Long> partitions = Maps.newHashMap(cachedPartitions);
        partitions.remove(partitionKey);
        partitionKeysCache.put(hivePartitionKeysKey, ImmutableMap.copyOf(partitions));
        partitionsCache.invalidate(hivePartitionKey);
        partitionStatsCache.invalidate(hivePartitionKey);
    }

    public boolean tableExistInCache(HiveTableKey tableKey) {
        boolean exist = tableStatsCache.getIfPresent(tableKey) != null;
        if (!HiveMetaStoreTableUtils.isInternalCatalog(resourceName)) {
            boolean connectorTableExist = getTableFromCache(
                    HiveTableName.of(tableKey.getDatabaseName(), tableKey.getTableName())) != null;
            return exist && connectorTableExist;
        }
        return exist;
    }

    public boolean partitionExistInCache(HivePartitionKey partitionKey) {
        boolean exist = partitionsCache.getIfPresent(partitionKey) != null;
        if (!HiveMetaStoreTableUtils.isInternalCatalog(resourceName)) {
            boolean connectorTableExist = getTableFromCache(
                    HiveTableName.of(partitionKey.getDatabaseName(), partitionKey.getTableName())) != null;
            return exist && connectorTableExist;
        }
        return exist;
    }

    public void refreshConnectorTable(String db, String name) throws TException, DdlException, ExecutionException {
        HiveTableName hiveTableName = HiveTableName.of(db, name);
        refreshConnectorTableSchema(hiveTableName);
        if (tableCache.get(hiveTableName) instanceof HiveTable) {
            HiveTable hiveTable = (HiveTable) tableCache.get(hiveTableName);
            refreshTable(hiveTable.getHmsTableInfo());
        } else if (tableCache.get(hiveTableName) instanceof HudiTable) {
            HudiTable hudiTable = (HudiTable) tableCache.get(hiveTableName);
            refreshTable(hudiTable.getHmsTableInfo());
        }
    }

    public void refreshConnectorTableSchema(HiveTableName hiveTableName) throws TException, DdlException {
        tableCache.put(hiveTableName, loadTable(hiveTableName));
    }

    public void refreshTable(HiveMetaStoreTableInfo hmsTable)
            throws DdlException {
        String dbName = hmsTable.getDb();
        String tableName = hmsTable.getTable();
        Table.TableType tableType = hmsTable.getTableType();
        List<Column> partColumns = getPartitionColumns(hmsTable);
        List<String> columnNames = getAllColumnNames(hmsTable);
        HivePartitionKeysKey hivePartitionKeysKey = new HivePartitionKeysKey(dbName, tableName, tableType, partColumns);
        HiveTableKey hiveTableKey = HiveTableKey.gen(dbName, tableName);
        HiveTableColumnsKey hiveTableColumnsKey =
                new HiveTableColumnsKey(dbName, tableName, partColumns, columnNames, tableType);
        GlobalStateMgr.getCurrentState().getMetastoreEventsProcessor().getEventProcessorLock().writeLock().lock();
        try {
            ImmutableMap<PartitionKey, Long> partitionKeys = loadPartitionKeys(hivePartitionKeysKey);
            partitionKeysCache.put(hivePartitionKeysKey, partitionKeys);
            tableStatsCache.put(hiveTableKey, loadTableStats(hiveTableKey));
            tableColumnStatsCache.put(hiveTableColumnsKey, loadTableColumnStats(hiveTableColumnsKey));

            // for unpartition table, refresh the partition info, because there is only one partition
            if (partColumns.size() <= 0) {
                HivePartitionKey hivePartitionKey =
                        new HivePartitionKey(dbName, tableName, tableType, new ArrayList<>());
                partitionsCache.put(hivePartitionKey, loadPartition(hivePartitionKey));
                partitionStatsCache.put(hivePartitionKey, loadPartitionStats(hivePartitionKey));
            }
        } catch (Exception e) {
            LOG.warn("refresh table cache failed", e);
            throw new DdlException("refresh table cache failed: " + e.getMessage());
        } finally {
            GlobalStateMgr.getCurrentState().getMetastoreEventsProcessor().getEventProcessorLock().writeLock().unlock();
        }
    }

    public void refreshPartition(HiveMetaStoreTableInfo hmsTable, List<String> partNames) throws DdlException {
        GlobalStateMgr.getCurrentState().getMetastoreEventsProcessor().getEventProcessorLock().writeLock().lock();
        try {
            for (String partName : partNames) {
                List<String> partValues = client.partitionNameToVals(partName);
                HivePartitionKey key = new HivePartitionKey(hmsTable.getDb(), hmsTable.getTable(),
                        hmsTable.getTableType(), partValues);
                partitionsCache.put(key, loadPartition(key));
                partitionStatsCache.put(key, loadPartitionStats(key));
            }
        } catch (Exception e) {
            LOG.warn("refresh partition cache failed", e);
            throw new DdlException("refresh partition cached failed: " + e.getMessage());
        } finally {
            GlobalStateMgr.getCurrentState().getMetastoreEventsProcessor().getEventProcessorLock().writeLock().unlock();
        }
    }

    public void refreshColumnStats(HiveMetaStoreTableInfo hmsTable)
            throws DdlException {
        List<Column> partColumns = getPartitionColumns(hmsTable);
        List<String> columnNames = getAllColumnNames(hmsTable);
        try {
            HiveTableColumnsKey hiveTableColumnsKey =
                    new HiveTableColumnsKey(hmsTable.getDb(), hmsTable.getTable(),
                            partColumns, columnNames, hmsTable.getTableType());
            tableColumnStatsCache.put(hiveTableColumnsKey, loadTableColumnStats(hiveTableColumnsKey));
        } catch (Exception e) {
            throw new DdlException("refresh table column statistic cached failed: " + e.getMessage());
        }
    }

    public void clearCache(HiveMetaStoreTableInfo hmsTable) {
        String dbName = hmsTable.getDb();
        String tableName = hmsTable.getTable();
        Table.TableType tableType = hmsTable.getTableType();

        HivePartitionKeysKey hivePartitionKeysKey = new HivePartitionKeysKey(dbName, tableName, tableType, null);
        ImmutableMap<PartitionKey, Long> partitionKeys = partitionKeysCache.getIfPresent(hivePartitionKeysKey);
        partitionKeysCache.invalidate(hivePartitionKeysKey);
        tableStatsCache.invalidate(HiveTableKey.gen(dbName, tableName));
        tableColumnStatsCache.invalidate(new HiveTableColumnsKey(dbName, tableName, null, null, tableType));
        if (partitionKeys != null) {
            for (Map.Entry<PartitionKey, Long> entry : partitionKeys.entrySet()) {
                HivePartitionKey pKey =
                        new HivePartitionKey(dbName, tableName, tableType,
                                Utils.getPartitionValues(entry.getKey(), tableType == Table.TableType.HUDI));
                partitionsCache.invalidate(pKey);
                partitionStatsCache.invalidate(pKey);
            }
        }

        if (!HiveMetaStoreTableUtils.isInternalCatalog(resourceName)) {
            if (partitionKeys != null) {
                List<HivePartitionKey> residualToRemove = partitionsCache.asMap().keySet().stream()
                        .filter(key -> key.approximateMatchTable(dbName, tableName))
                        .collect(Collectors.toList());
                partitionsCache.invalidateAll(residualToRemove);
            }
            tableCache.invalidate(HiveTableName.of(hmsTable.getDb(), hmsTable.getTable()));
        }
    }

    public String getResourceName() {
        return resourceName;
    }
}