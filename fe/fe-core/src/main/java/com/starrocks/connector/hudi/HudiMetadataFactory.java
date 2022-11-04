// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hudi;

import com.starrocks.connector.CachingRemoteFileConf;
import com.starrocks.connector.CachingRemoteFileIO;
import com.starrocks.connector.RemoteFileIO;
import com.starrocks.connector.RemoteFileOperations;
import com.starrocks.connector.hive.CacheUpdateProcessor;
import com.starrocks.connector.hive.CachingHiveMetastore;
import com.starrocks.connector.hive.CachingHiveMetastoreConf;
import com.starrocks.connector.hive.HiveMetastoreOperations;
import com.starrocks.connector.hive.HiveStatisticsProvider;
import com.starrocks.connector.hive.IHiveMetastore;

import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static com.starrocks.connector.hive.CachingHiveMetastore.createQueryLevelInstance;

public class HudiMetadataFactory {
    private final String catalogName;
    private final IHiveMetastore metastore;
    private final RemoteFileIO remoteFileIO;
    private final long perQueryMetastoreMaxNum;
    private final long perQueryCacheRemotePathMaxNum;
    private final ExecutorService pullRemoteFileExecutor;
    private final boolean isRecursive;

    public HudiMetadataFactory(String catalogName,
                               IHiveMetastore metastore,
                               RemoteFileIO remoteFileIO,
                               CachingHiveMetastoreConf hmsConf,
                               CachingRemoteFileConf fileConf,
                               ExecutorService pullRemoteFileExecutor,
                               boolean isRecursive) {
        this.catalogName = catalogName;
        this.metastore = metastore;
        this.remoteFileIO = remoteFileIO;
        this.perQueryMetastoreMaxNum = hmsConf.getPerQueryCacheMaxNum();
        this.perQueryCacheRemotePathMaxNum = fileConf.getPerQueryCacheMaxSize();
        this.pullRemoteFileExecutor = pullRemoteFileExecutor;
        this.isRecursive = isRecursive;
    }

    public HudiMetadata create() {
        HiveMetastoreOperations hiveMetastoreOperations = new HiveMetastoreOperations(
                createQueryLevelInstance(metastore, perQueryMetastoreMaxNum), metastore instanceof CachingHiveMetastore);
        RemoteFileOperations remoteFileOperations = new RemoteFileOperations(
                CachingRemoteFileIO.createQueryLevelInstance(remoteFileIO, perQueryCacheRemotePathMaxNum),
                pullRemoteFileExecutor,
                isRecursive,
                remoteFileIO instanceof CachingRemoteFileIO);
        HiveStatisticsProvider statisticsProvider = new HiveStatisticsProvider(hiveMetastoreOperations, remoteFileOperations);
        Optional<CacheUpdateProcessor> cacheUpdateProcessor = getCacheUpdateProcessor();

        return new HudiMetadata(catalogName, hiveMetastoreOperations,
                remoteFileOperations, statisticsProvider, cacheUpdateProcessor);
    }

    public synchronized Optional<CacheUpdateProcessor> getCacheUpdateProcessor() {
        Optional<CacheUpdateProcessor> cacheUpdateProcessor;
        if (remoteFileIO instanceof CachingRemoteFileIO || metastore instanceof CachingHiveMetastore) {
            cacheUpdateProcessor = Optional.of(new CacheUpdateProcessor(
                    catalogName, metastore, remoteFileIO, pullRemoteFileExecutor,
                    isRecursive, false));
        } else {
            cacheUpdateProcessor = Optional.empty();
        }

        return cacheUpdateProcessor;
    }
}
