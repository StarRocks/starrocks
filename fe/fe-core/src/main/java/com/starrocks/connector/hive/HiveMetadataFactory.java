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

import com.starrocks.connector.CachingRemoteFileConf;
import com.starrocks.connector.CachingRemoteFileIO;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.MetastoreType;
import com.starrocks.connector.RemoteFileIO;
import com.starrocks.connector.RemoteFileOperations;

import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import static com.starrocks.connector.hive.CachingHiveMetastore.createQueryLevelInstance;

public class HiveMetadataFactory {
    private final String catalogName;
    private final IHiveMetastore metastore;
    private final RemoteFileIO remoteFileIO;
    private final long perQueryMetastoreMaxNum;
    private final long perQueryCacheRemotePathMaxNum;
    private final ExecutorService pullRemoteFileExecutor;
    private final Executor updateRemoteFilesExecutor;
    private final Executor updateStatisticsExecutor;
    private final Executor refreshOthersFeExecutor;
    private final boolean isRecursive;
    private final boolean enableHmsEventsIncrementalSync;
    private final HdfsEnvironment hdfsEnvironment;
    private final MetastoreType metastoreType;

    public HiveMetadataFactory(String catalogName,
                               IHiveMetastore metastore,
                               RemoteFileIO remoteFileIO,
                               CachingHiveMetastoreConf hmsConf,
                               CachingRemoteFileConf fileConf,
                               ExecutorService pullRemoteFileExecutor,
                               Executor updateRemoteFilesExecutor,
                               Executor updateStatisticsExecutor,
                               Executor refreshOthersFeExecutor,
                               boolean isRecursive,
                               boolean enableHmsEventsIncrementalSync,
                               HdfsEnvironment hdfsEnvironment,
                               MetastoreType metastoreType) {
        this.catalogName = catalogName;
        this.metastore = metastore;
        this.remoteFileIO = remoteFileIO;
        this.perQueryMetastoreMaxNum = hmsConf.getPerQueryCacheMaxNum();
        this.perQueryCacheRemotePathMaxNum = fileConf.getPerQueryCacheMaxSize();
        this.pullRemoteFileExecutor = pullRemoteFileExecutor;
        this.updateRemoteFilesExecutor = updateRemoteFilesExecutor;
        this.updateStatisticsExecutor = updateStatisticsExecutor;
        this.refreshOthersFeExecutor = refreshOthersFeExecutor;
        this.isRecursive = isRecursive;
        this.enableHmsEventsIncrementalSync = enableHmsEventsIncrementalSync;
        this.hdfsEnvironment = hdfsEnvironment;
        this.metastoreType = metastoreType;
    }

    public HiveMetadata create() {
        HiveMetastoreOperations hiveMetastoreOperations = new HiveMetastoreOperations(
                createQueryLevelInstance(metastore, perQueryMetastoreMaxNum),
                metastore instanceof CachingHiveMetastore,
                hdfsEnvironment.getConfiguration(), metastoreType, catalogName);
        RemoteFileOperations remoteFileOperations = new RemoteFileOperations(
                CachingRemoteFileIO.createQueryLevelInstance(remoteFileIO, perQueryCacheRemotePathMaxNum),
                pullRemoteFileExecutor,
                updateRemoteFilesExecutor,
                isRecursive,
                remoteFileIO instanceof CachingRemoteFileIO,
                hdfsEnvironment.getConfiguration());
        HiveStatisticsProvider statisticsProvider = new HiveStatisticsProvider(hiveMetastoreOperations, remoteFileOperations);

        Optional<CacheUpdateProcessor> cacheUpdateProcessor = getCacheUpdateProcessor();
        return new HiveMetadata(catalogName, hdfsEnvironment, hiveMetastoreOperations, remoteFileOperations,
                statisticsProvider, cacheUpdateProcessor, updateStatisticsExecutor, refreshOthersFeExecutor);
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
