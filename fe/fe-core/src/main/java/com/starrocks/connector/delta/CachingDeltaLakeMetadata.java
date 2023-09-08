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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.connector.delta.cache.DeltaLakeMetadateDiskCacheDao;
import com.starrocks.connector.delta.cache.DeltaLakeMetadateDiskCacheFactory;
import com.starrocks.connector.delta.cache.DeltaLakeTableName;
import com.starrocks.connector.delta.cache.DeltaTableMetadata;
import com.starrocks.connector.hive.CachingHiveMetastore;
import com.starrocks.connector.hive.HiveMetastore;
import com.starrocks.connector.hive.HivePartitionStats;
import com.starrocks.connector.hive.IHiveMetastore;
import com.starrocks.connector.hive.Partition;
import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.internal.util.DeltaJsonUtil;
import io.delta.standalone.internal.util.DeltaPartitionUtil;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class CachingDeltaLakeMetadata extends CachingHiveMetastore {
    private static final Logger LOG = LogManager.getLogger(CachingDeltaLakeMetadata.class);

    private static final ScheduledExecutorService TIMER_CHECK_DELETE_EXPIREDCACHE = Executors
            .newScheduledThreadPool(1, new ThreadFactoryBuilder()
                    .setNameFormat("disk-cached-expired-delete-%d").build());

    // Whether the cache needs to check the metadata size, which is greater than a certain threshold cache.
    // The cache strategy is to automatically cache the first query,
    // and the cache can also be refreshed through the refresh table command.
    // Regularly detect and clean up expired files.
    public static final String METADATA_CACHE_DISK_ROOT_PATH = Config.delta_metadata_cache_disk_path;

    protected Cache<DeltaLakeTableName, DeltaTableMetadata> cacheTableMetadata;

    //All partition information is cached in the json file
    protected Cache<DeltaLakeTableName, Set<Map<String, String>>> cacheTablePartitions;

    protected Cache<DeltaLakeTableName, DeltaLakeMetadateDiskCacheDao> cacheDeltaLocalCacheDao;

    protected Cache<DeltaLakeTableName, Long> snapshotVersionCheckTime;

    //no cache delta metadata statistics
    private final boolean deltaCacheFilterDataskippingStats = true;

    public CachingDeltaLakeMetadata(IHiveMetastore metastore, Executor executor, long expireAfterWriteSec,
                                    long refreshIntervalSec, long maxSize, boolean enableListNamesCache) {
        super(metastore, executor, expireAfterWriteSec, refreshIntervalSec, maxSize, enableListNamesCache);

        //By default, it will expire without access for 24 hours, delete the cache
        long expiredTime = Config.delta_cache_expired_time_seconds;

        cacheTableMetadata = newCacheBuilder(expiredTime, maxSize)
                .build();

        cacheDeltaLocalCacheDao = newCacheBuilder(expiredTime, Config.delta_db_dao_cache_size)
                .removalListener(remove -> {
                    DeltaLakeMetadateDiskCacheDao dao = (DeltaLakeMetadateDiskCacheDao) remove.getValue();
                    dao.closeConnection();
                })
                .build();

        cacheTablePartitions = newCacheBuilder(expiredTime, Config.delta_db_dao_cache_size)
                .build();

        snapshotVersionCheckTime = newCacheBuilder(Config.delta_snapshot_version_check_seconds, maxSize)
                .build();

        long deltaTimerCheckExpiredCacheSeconds = Config.delta_timer_check_expired_cache_seconds;

        TIMER_CHECK_DELETE_EXPIREDCACHE.scheduleWithFixedDelay(
                this::periodCheckAndDeleteExpiredCache,
                deltaTimerCheckExpiredCacheSeconds,
                deltaTimerCheckExpiredCacheSeconds,
                TimeUnit.SECONDS);

    }

    /**
     * Period clean up expired cache files
     */
    private void periodCheckAndDeleteExpiredCache() {
        String catalogName = ((HiveMetastore) metastore).getCatalogName();
        String catalogCachePath = METADATA_CACHE_DISK_ROOT_PATH + "/" + catalogName;
        File rootPath = new File(catalogCachePath);
        if (!rootPath.exists()) {
            return;
        }

        cacheTableMetadata.cleanUp();
        cacheTablePartitions.cleanUp();
        cacheDeltaLocalCacheDao.cleanUp();
        snapshotVersionCheckTime.cleanUp();

        periodCheckAndDeleteCacheFile(rootPath);
    }

    private void periodCheckAndDeleteCacheFile(File rootPath) {
        File[] files = rootPath.listFiles();
        String jeStatFile = "LOG";
        Map<File, Long[]> pathInfoMap = new HashMap<>();
        long totalSpaceUsed = 0;

        for (File file : files) {
            Path path = Paths.get(file.getPath(), jeStatFile);
            try {
                if (!Files.exists(path)) {
                    continue;
                }
                long lastModifiedTime = Files.getLastModifiedTime(path).toMillis();
                if ((System.currentTimeMillis() - lastModifiedTime) >
                        Config.delta_cache_expired_time_seconds * 1000) {
                    clearLocalCache(getTblFromFileName(file.getName()));
                    FileUtils.deleteDirectory(file);
                    LOG.info("clear cache of table:{},delete cache path:{}",
                            getTblFromFileName(file.getName()),
                            file.getPath());
                } else {
                    long size = FileUtils.sizeOfDirectory(file);
                    totalSpaceUsed += size;
                    Long[] longs = {
                            lastModifiedTime,
                            size
                    };
                    pathInfoMap.put(file, longs);
                }

            } catch (IOException ex) {
                LOG.error("delete directory fail:{}", file.getPath(), ex);
            }
        }


        if (totalSpaceUsed > Config.delta_metadata_disk_cache_capacity) {
            List<Map.Entry<File, Long[]>> list = new ArrayList<>(pathInfoMap.entrySet());
            list.sort((o1, o2) -> {
                Long aLong = o1.getValue()[0];
                Long bLong = o2.getValue()[0];
                return Math.toIntExact(aLong - bLong);
            });
            for (Map.Entry<File, Long[]> pathEntry : list) {
                long size = pathEntry.getValue()[1];
                try {
                    DeltaLakeTableName tbl = getTblFromFileName(pathEntry.getKey().getName());
                    clearLocalCache(tbl);
                    FileUtils.deleteDirectory(pathEntry.getKey());
                    LOG.info("clear cache of table:{} ,delete cache path:{}", tbl, pathEntry.getKey());
                } catch (IOException e) {
                    LOG.error("delete directory fail:{}", pathEntry.getKey().getPath(), e);
                }
                totalSpaceUsed -= size;

                if (totalSpaceUsed < Config.delta_metadata_disk_cache_capacity) {
                    break;
                }
            }
        }
    }

    private DeltaLakeTableName getTblFromFileName(String path) {
        try {
            String[] split = path.split("@");
            String catalog = ((HiveMetastore) metastore).getCatalogName();
            String database = split[0];
            String tableName = split[1];
            Table table = getTable(database, tableName);
            String dataPath = ((HiveTable) table).getTableLocation();
            return new DeltaLakeTableName(catalog, database, tableName, dataPath);
        } catch (Exception e) {
            LOG.warn(path, e.getMessage());
            return null;
        }
    }

    private DeltaLakeMetadateDiskCacheDao getMetadataDiskCacheDao(DeltaLakeTableName tbl) {
        DeltaLakeMetadateDiskCacheDao deltaCacheBdbDao = cacheDeltaLocalCacheDao.getIfPresent(tbl);
        if (deltaCacheBdbDao == null) {
            synchronized (this) {
                deltaCacheBdbDao = cacheDeltaLocalCacheDao.getIfPresent(tbl);
                if (deltaCacheBdbDao == null) {
                    deltaCacheBdbDao = DeltaLakeMetadateDiskCacheFactory
                            .createDeltaCacheBdbDao(tbl, METADATA_CACHE_DISK_ROOT_PATH);
                    cacheDeltaLocalCacheDao.put(tbl, deltaCacheBdbDao);
                }
            }
        }
        return deltaCacheBdbDao;
    }

    public DeltaTableMetadata loaderCacheTable(DeltaLakeTableName tbl) {

        return getMetadataDiskCacheDao(tbl).getMetadata();
    }

    private static CacheBuilder<Object, Object> newCacheBuilder(long expiresAfterWriteSec,
                                                                long maximumSize) {
        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();

        if (expiresAfterWriteSec >= 0) {
            cacheBuilder.expireAfterWrite(expiresAfterWriteSec, TimeUnit.SECONDS);
        }
        cacheBuilder.maximumSize(maximumSize);
        return cacheBuilder;
    }

    /**
     * Updating the local cache needs to be locked.
     * @param snapshot snapshot
     * @param tbl      tbl
     */
    public void refreshTable(Snapshot snapshot, DeltaLakeTableName tbl) throws Exception {
        try {
            long startTime = System.currentTimeMillis();
            process(snapshot, tbl);
            LOG.info("refresh table:{}'s metadata take total time:{} ms", tbl,
                    (System.currentTimeMillis() - startTime));
        } catch (Exception e) {
            LOG.error("caching " + tbl.toString() + " error", e);
            clearLocalCache(tbl);
            throw new Exception("caching " + tbl + " error:" + e.getMessage());
        }
    }

    private void clearLocalCache(DeltaLakeTableName tbl) {
        cacheTableMetadata.invalidate(tbl);
        cacheTablePartitions.invalidate(tbl);
        cacheDeltaLocalCacheDao.invalidate(tbl);
        snapshotVersionCheckTime.invalidate(tbl);
    }

    /**
     * Persist first and then update the memory cache
     */
    private void process(Snapshot snapshot, DeltaLakeTableName tbl) throws RocksDBException {
        Metadata metadata = snapshot.getMetadata();
        long updateVersion = snapshot.getVersion();
        List<AddFile> allFiles = snapshot.getAllFiles();
        List<String> partitionColumns = metadata.getPartitionColumns();

        String metadataJToJson = DeltaJsonUtil.convertMetadataJToJson(metadata);
        DeltaTableMetadata tableMetadata = new DeltaTableMetadata(metadataJToJson, updateVersion);

        //--- persistent partition group by addfile
        Set<Map<String, String>> allPartionValues = Collections.newSetFromMap(new HashMap<>());
        List<Pair<String, AddFile>> partitionedAddFiles = allFiles.stream().map(addFile -> {
            // TODO SR currently does not support non-numeric cost calculations,
            //  and the delta-connector does not implement push-down calculations based on statistics.
            //  temporarily remove minValues first, maxValues, nullCount statistics, to reduce the local cache size
            if (deltaCacheFilterDataskippingStats) {
                try {
                    long numRecords = 0L;
                    if (null != addFile.getStats()) {
                        numRecords = DeltaJsonUtil.mapper
                                .readTree(addFile.getStats())
                                .get("numRecords").asLong();
                    }
                    String stats = "stats:{\"numRecords\":" + numRecords + "}";
                    addFile = new AddFile.Builder(addFile.getPath(), addFile.getPartitionValues(),
                            addFile.getSize(),
                            addFile.getModificationTime(),
                            addFile.isDataChange())
                            .stats(stats)
                            .tags(addFile.getTags())
                            .build();
                } catch (JsonProcessingException e) {
                    LOG.error("AddFile json parser error,{}", e.getMessage());
                    throw new StarRocksDeltaLakeException(e.getMessage());
                }
            }
            Map<String, String> partitionValues = addFile.getPartitionValues();
            allPartionValues.add(partitionValues);
            StringBuffer sb = new StringBuffer();
            String partitionValue;
            for (String partitionColumn : partitionColumns) {
                if (partitionValues.containsKey(partitionColumn) && null != partitionValues.get(partitionColumn)) {
                    partitionValue = partitionValues.get(partitionColumn);
                } else {
                    partitionValue = DeltaPartitionUtil.DElTA_DEFAULT_PARTITION_VALUE;
                }
                sb.append(partitionColumn)
                        .append("=")
                        .append(partitionValue)
                        .append("/");

            }
            sb.deleteCharAt(sb.length() - 1);
            return Pair.create(sb.toString(), addFile);
        }).collect(Collectors.toList());


        Map<String, Set<String>> map = partitionedAddFiles.parallelStream().map(x -> {
            String addFileJson = DeltaJsonUtil.convertAddFileJToJson(x.second);
            return Pair.create(x.first, addFileJson);
        }).collect(Collectors.groupingByConcurrent(pair -> pair.first, ConcurrentHashMap::new,
                Collectors.mapping(pair -> pair.second, Collectors.toSet())));

        //持久化元数据
        getMetadataDiskCacheDao(tbl).persistAllData(tableMetadata, map);
        cacheTablePartitions.put(tbl, allPartionValues);
        snapshotVersionCheckTime.put(tbl, tableMetadata.getSnapshotVersion());
        cacheTableMetadata.put(tbl, tableMetadata);
    }

    public void putCacheTableMetadata(@NotNull Snapshot snapshot, DeltaLakeTableName tbl) {
        Metadata metadata = snapshot.getMetadata();
        long updateVersion = snapshot.getVersion();
        String metadataJToJson = DeltaJsonUtil.convertMetadataJToJson(metadata);
        DeltaTableMetadata tableMetadata = new DeltaTableMetadata(metadataJToJson, updateVersion);
        cacheTableMetadata.put(tbl, tableMetadata);
    }

    /**
     * Query all the data of this table from bdbje
     */
    public List<AddFile> getAllFiles(DeltaLakeTableName tbl) throws Exception {

        Set<Map<String, String>> allPartitions = getAllPartitions(tbl);
        List<String> partitionColumns = getMetadata(tbl).getPartitionColumns();
        Set<String> collected = DeltaPartitionUtil.toHivePartitionFormat(partitionColumns, allPartitions);
        return getPushedAddFiles(tbl, collected);
    }


    public Long getCacheSnapshotVersion(DeltaLakeTableName tbl) {

        try {
            DeltaTableMetadata tableMetadata = getDeltaTableMetadata(tbl);
            if (null != tableMetadata) {
                return tableMetadata.getSnapshotVersion();
            }
        } catch (Exception e) {
            LOG.warn("get table:" + tbl.getTblName() + "'s snaphot version error {}",
                    e.getMessage());
        }
        return null;
    }

    public Long getLastSnapshotVersion(DeltaLakeTableName tbl,
                                       Configuration configuration) throws IOException {
        Long ifPresent = snapshotVersionCheckTime.getIfPresent(tbl);
        if (null == ifPresent) {
            long lastSnapshotVersion = DeltaUtils.lastSnapshotVersion(configuration, tbl.getDataPath());
            snapshotVersionCheckTime.put(tbl, lastSnapshotVersion);
            return lastSnapshotVersion;
        }
        return ifPresent;
    }

    public Metadata getMetadata(DeltaLakeTableName tbl) throws Exception {
        DeltaTableMetadata tblMetadata = getDeltaTableMetadata(tbl);
        if (null == tblMetadata) {
            return null;
        }
        String metadata = tblMetadata.getMetadata();
        return DeltaJsonUtil.convertJsonToMetadataJ(metadata);
    }

    private DeltaTableMetadata getDeltaTableMetadata(DeltaLakeTableName tbl) {
        DeltaTableMetadata ifPresent = cacheTableMetadata.getIfPresent(tbl);
        if (null == ifPresent) {
            //从bdb中载入
            ifPresent = loaderCacheTable(tbl);
        }
        return ifPresent;
    }


    public Set<Map<String, String>> getAllPartitions(DeltaLakeTableName tbl) throws Exception {

        return cacheTablePartitions
                .get(tbl, () -> {
                    Set<String> allPartition = getMetadataDiskCacheDao(tbl).getAllPartition();
                    return DeltaPartitionUtil.revertPartitions(allPartition);
                });
    }

    public List<AddFile> getPushedAddFiles(DeltaLakeTableName tbl, Set<String> pushedPartitionValues) {

        return getMetadataDiskCacheDao(tbl).getAddFilesByPartitions(pushedPartitionValues);
    }


    @Override
    public List<String> getAllDatabaseNames() {
        return super.getAllDatabaseNames();
    }

    @Override
    public List<String> getAllTableNames(String dbName) {
        return super.getAllTableNames(dbName);
    }

    @Override
    public Database getDb(String dbName) {
        return super.getDb(dbName);
    }

    @Override
    public Table getTable(String dbName, String tableName) {
        return super.getTable(dbName, tableName);
    }

    @Override
    public Partition getPartition(String dbName, String tableName, List<String> partitionValues) {
        return super.getPartition(dbName, tableName, partitionValues);
    }

    @Override
    public Map<String, Partition> getPartitionsByNames(String dbName, String tableName, List<String> partitionNames) {
        return super.getPartitionsByNames(dbName, tableName, partitionNames);
    }

    @Override
    public HivePartitionStats getTableStatistics(String dbName, String tableName) {
        return super.getTableStatistics(dbName, tableName);
    }

    @Override
    public Map<String, HivePartitionStats> getPartitionStatistics(Table table, List<String> partitions) {
        return super.getPartitionStatistics(table, partitions);
    }


    @Override
    public synchronized void invalidateAll() {
        super.invalidateAll();
        //clear all cache
        cacheTableMetadata.invalidateAll();
        cacheTablePartitions.invalidateAll();
        cacheDeltaLocalCacheDao.invalidateAll();
        snapshotVersionCheckTime.invalidateAll();


        String catalogName = ((HiveMetastore) metastore).getCatalogName();
        String catalogCachePath = METADATA_CACHE_DISK_ROOT_PATH + "/" + catalogName;
        File file = new File(catalogCachePath);
        if (file.exists()) {
            try {
                FileUtils.deleteDirectory(file);
                LOG.info("drop catalog: {}, delete cache path:{} succeed",
                        catalogName,
                        catalogCachePath);
            } catch (IOException e) {
                LOG.warn("drop catalog: {}, delete cache path:{} failed:{}",
                        catalogName,
                        catalogCachePath, e.getMessage());
            }

        }

    }

    public static CachingDeltaLakeMetadata createCatalogLevelInstance(IHiveMetastore metastore, Executor executor,
                                                                      long expireAfterWrite, long refreshInterval,
                                                                      long maxSize, boolean enableListNamesCache) {
        return new CachingDeltaLakeMetadata(metastore, executor,
                expireAfterWrite, refreshInterval,
                maxSize, enableListNamesCache);
    }

    public void invalidateDao(DeltaLakeTableName deltaTbl) {
        cacheDeltaLocalCacheDao.invalidate(deltaTbl);
    }
}