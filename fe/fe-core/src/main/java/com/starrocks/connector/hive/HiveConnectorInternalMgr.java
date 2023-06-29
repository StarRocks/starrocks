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


package com.starrocks.connector.hive;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.starrocks.common.Config;
import com.starrocks.connector.CachingRemoteFileConf;
import com.starrocks.connector.CachingRemoteFileIO;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.ReentrantExecutor;
import com.starrocks.connector.RemoteFileIO;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.starrocks.connector.CachingRemoteFileIO.NEVER_REFRESH;

public class HiveConnectorInternalMgr {
    private final String catalogName;
    private final HdfsEnvironment hdfsEnvironment;
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
    private final boolean enableHmsEventsIncrementalSync;

    private final boolean enableBackgroundRefreshHiveMetadata;

    public HiveConnectorInternalMgr(String catalogName, Map<String, String> properties, HdfsEnvironment hdfsEnvironment) {
        this.catalogName = catalogName;
        this.properties = properties;
        this.hdfsEnvironment = hdfsEnvironment;
        this.enableMetastoreCache = Boolean.parseBoolean(properties.getOrDefault("enable_metastore_cache", "true"));
        this.hmsConf = new CachingHiveMetastoreConf(properties, "hive");

        this.enableRemoteFileCache = Boolean.parseBoolean(properties.getOrDefault("enable_remote_file_cache", "true"));
        this.remoteFileConf = new CachingRemoteFileConf(properties);

        this.isRecursive = Boolean.parseBoolean(properties.getOrDefault("enable_recursive_listing", "true"));
        this.loadRemoteFileMetadataThreadNum = Integer.parseInt(properties.getOrDefault("remote_file_load_thread_num",
                String.valueOf(Config.remote_file_metadata_load_concurrency)));
        this.enableHmsEventsIncrementalSync = Boolean.parseBoolean(properties.getOrDefault("enable_hms_events_incremental_sync",
                String.valueOf(Config.enable_hms_events_incremental_sync)));

        this.enableBackgroundRefreshHiveMetadata = Boolean.parseBoolean(properties.getOrDefault(
                "enable_background_refresh_connector_metadata", "true"));
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
                    enableHmsEventsIncrementalSync ? NEVER_REFRESH : hmsConf.getCacheRefreshIntervalSec(),
                    hmsConf.getCacheMaxNum(),
                    hmsConf.enableListNamesCache());
        }

        return baseHiveMetastore;
    }

    public RemoteFileIO createRemoteFileIO() {
        // TODO(stephen): Abstract the creator class to construct RemoteFiloIO

        RemoteFileIO remoteFileIO = new HiveRemoteFileIO(hdfsEnvironment.getConfiguration());

        RemoteFileIO baseRemoteFileIO;
        if (!enableRemoteFileCache) {
            baseRemoteFileIO = remoteFileIO;
        } else {
            refreshRemoteFileExecutor = Executors.newCachedThreadPool(
                    new ThreadFactoryBuilder().setNameFormat("hive-remote-files-refresh-%d").build());
            baseRemoteFileIO = CachingRemoteFileIO.createCatalogLevelInstance(
                    remoteFileIO,
                    new ReentrantExecutor(refreshRemoteFileExecutor, remoteFileConf.getRefreshMaxThreadNum()),
                    remoteFileConf.getCacheTtlSec(),
                    enableHmsEventsIncrementalSync ? NEVER_REFRESH : remoteFileConf.getCacheRefreshIntervalSec(),
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

    public boolean enableHmsEventsIncrementalSync() {
        return enableHmsEventsIncrementalSync;
    }

    public HdfsEnvironment getHdfsEnvironment() {
        return hdfsEnvironment;
    }

    public boolean isEnableBackgroundRefreshHiveMetadata() {
        return enableBackgroundRefreshHiveMetadata;
    }
}
