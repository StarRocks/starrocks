// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hive;

import com.starrocks.connector.CachingRemoteFileConf;
import com.starrocks.connector.CachingRemoteFileIO;
import com.starrocks.connector.RemoteFileIO;
import com.starrocks.connector.RemoteFileOperations;

import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static com.starrocks.connector.hive.CachingHiveMetastore.createQueryLevelInstance;

public class HiveMetadataFactory {
    private final String catalogName;
    private final IHiveMetastore metastore;
    private final RemoteFileIO remoteFileIO;
    private final long perQueryMetastoreMaxNum;
    private final long perQueryCacheRemotePathMaxNum;
    private final ExecutorService pullRemoteFileExecutor;
    private final boolean isRecursive;
    private final boolean enableHmsEventsIncrementalSync;

    public HiveMetadataFactory(String catalogName,
                               IHiveMetastore metastore,
                               RemoteFileIO remoteFileIO,
                               CachingHiveMetastoreConf hmsConf,
                               CachingRemoteFileConf fileConf,
                               ExecutorService pullRemoteFileExecutor,
                               boolean isRecursive,
                               boolean enableHmsEventsIncrementalSync) {
        this.catalogName = catalogName;
        this.metastore = metastore;
        this.remoteFileIO = remoteFileIO;
        this.perQueryMetastoreMaxNum = hmsConf.getPerQueryCacheMaxNum();
        this.perQueryCacheRemotePathMaxNum = fileConf.getPerQueryCacheMaxSize();
        this.pullRemoteFileExecutor = pullRemoteFileExecutor;
        this.isRecursive = isRecursive;
        this.enableHmsEventsIncrementalSync = enableHmsEventsIncrementalSync;
    }

    public HiveMetadata create() {
        HiveMetastoreOperations hiveMetastoreOperations = new HiveMetastoreOperations(
                createQueryLevelInstance(metastore, perQueryMetastoreMaxNum), metastore instanceof CachingHiveMetastore);
        RemoteFileOperations remoteFileOperations = new RemoteFileOperations(
                CachingRemoteFileIO.createQueryLevelInstance(remoteFileIO, perQueryCacheRemotePathMaxNum),
                pullRemoteFileExecutor,
                isRecursive,
                remoteFileIO instanceof CachingRemoteFileIO);
        HiveStatisticsProvider statisticsProvider = new HiveStatisticsProvider(hiveMetastoreOperations, remoteFileOperations);

        Optional<CacheUpdateProcessor> cacheUpdateProcessor = getCacheUpdateProcessor();
        return new HiveMetadata(catalogName, hiveMetastoreOperations,
                remoteFileOperations, statisticsProvider, cacheUpdateProcessor);
    }

    public synchronized Optional<CacheUpdateProcessor> getCacheUpdateProcessor() {
        Optional<CacheUpdateProcessor> cacheUpdateProcessor;
        if (remoteFileIO instanceof CachingRemoteFileIO || metastore instanceof CachingHiveMetastore) {
            cacheUpdateProcessor = Optional.of(new CacheUpdateProcessor(
                    catalogName, metastore, remoteFileIO, pullRemoteFileExecutor,
                    isRecursive, enableHmsEventsIncrementalSync));
        } else {
            cacheUpdateProcessor = Optional.empty();
        }

        return cacheUpdateProcessor;
    }
}
