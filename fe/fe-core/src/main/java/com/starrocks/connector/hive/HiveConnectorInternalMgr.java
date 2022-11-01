// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hive;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.starrocks.common.Config;
import com.starrocks.connector.CachingRemoteFileConf;
import com.starrocks.connector.CachingRemoteFileIO;
import com.starrocks.connector.ReentrantExecutor;
import com.starrocks.connector.RemoteFileIO;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HiveConnectorInternalMgr {
    private final String catalogName;
    private final Map<String, String> properties;
    private final boolean enableMetastoreCache;
    private final CachingHiveMetastoreConf hmsConf;

    private final boolean enableRemoteFileCache;
    private final CachingRemoteFileConf remoteFileConf;

    private ExecutorService refreshHiveMetastoreExecutor;
    private ExecutorService refreshRemoteFileExecutor;
    private ExecutorService pullRemoteFileExecutor;

    private final boolean isRecursive;
    private final int loadRemoteFileMetadataThreadNum;

    public HiveConnectorInternalMgr(String catalogName, Map<String, String> properties) {
        this.catalogName = catalogName;
        this.properties = properties;
        this.enableMetastoreCache = Boolean.parseBoolean(properties.getOrDefault("enable_metastore_cache", "true"));
        this.hmsConf = new CachingHiveMetastoreConf(properties);

        this.enableRemoteFileCache = Boolean.parseBoolean(properties.getOrDefault("enable_remote_file_cache", "true"));
        this.remoteFileConf = new CachingRemoteFileConf(properties);

        this.isRecursive = Boolean.parseBoolean(properties.getOrDefault("hive_recursive_directories", "false"));
        this.loadRemoteFileMetadataThreadNum = Integer.parseInt(properties.getOrDefault("remote_file_load_thread_num",
                String.valueOf(Config.remote_file_metadata_load_concurrency)));
    }

    public void shutdown() {
        if (enableMetastoreCache && refreshHiveMetastoreExecutor != null) {
            refreshHiveMetastoreExecutor.shutdown();
        }
        if (enableRemoteFileCache && refreshRemoteFileExecutor != null) {
            refreshRemoteFileExecutor.shutdown();
        }
        if (pullRemoteFileExecutor != null) {
            pullRemoteFileExecutor.shutdown();
        }
    }

    public IHiveMetastore createHiveMetastore() {
        // TODO(stephen): Abstract the creator class to construct hive meta client
        HiveMetaClient metaClient = HiveMetaClient.createHiveMetaClient(properties);
        IHiveMetastore hiveMetastore = new HiveMetastore(metaClient, catalogName);
        IHiveMetastore baseHiveMetastore;
        if (!enableMetastoreCache) {
            baseHiveMetastore = hiveMetastore;
        } else {
            refreshHiveMetastoreExecutor = Executors.newCachedThreadPool(
                    new ThreadFactoryBuilder().setNameFormat("hive-metastore-refresh-%d").build());
            baseHiveMetastore = CachingHiveMetastore.createCatalogLevelInstance(
                    hiveMetastore,
                    new ReentrantExecutor(refreshHiveMetastoreExecutor, hmsConf.getCacheRefreshThreadMaxNum()),
                    hmsConf.getCacheTtlSec(),
                    hmsConf.getCacheRefreshIntervalSec(),
                    hmsConf.getCacheMaxNum(),
                    hmsConf.enableListNamesCache());
        }

        return baseHiveMetastore;
    }

    public RemoteFileIO createRemoteFileIO() {
        // TODO(stephen): Abstract the creator class to construct RemoteFiloIO
        Configuration configuration = new Configuration();
        RemoteFileIO remoteFileIO = new HiveRemoteFileIO(configuration);

        RemoteFileIO baseRemoteFileIO;
        if (!enableRemoteFileCache) {
            baseRemoteFileIO = remoteFileIO;
        } else {
            refreshRemoteFileExecutor = Executors.newCachedThreadPool(
                    new ThreadFactoryBuilder().setNameFormat("hive-remote-files-refresh-%d").build());
            baseRemoteFileIO = CachingRemoteFileIO.createCatalogLevelInstance(
                    remoteFileIO,
                    new ReentrantExecutor(refreshRemoteFileExecutor, remoteFileConf.getPerQueryCacheMaxSize()),
                    remoteFileConf.getCacheTtlSec(),
                    remoteFileConf.getCacheRefreshIntervalSec(),
                    remoteFileConf.getCacheMaxSize());
        }

        return baseRemoteFileIO;
    }

    public ExecutorService getPullRemoteFileExecutor() {
        if (pullRemoteFileExecutor == null) {
            pullRemoteFileExecutor = Executors.newFixedThreadPool(loadRemoteFileMetadataThreadNum,
                    new ThreadFactoryBuilder().setNameFormat("pull-hive-remote-files-%d").build());
        }

        return pullRemoteFileExecutor;
    }

    public boolean isSearchRecursive() {
        return isRecursive;
    }

    public CachingHiveMetastoreConf getHiveMetastoreConf() {
        return hmsConf;
    }

    public CachingRemoteFileConf getRemoteFileConf() {
        return remoteFileConf;
    }
}
