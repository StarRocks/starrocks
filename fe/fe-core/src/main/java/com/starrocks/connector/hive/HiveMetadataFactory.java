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
<<<<<<< HEAD
=======
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.MetastoreType;
>>>>>>> c60edea929 ([Refactor] Move `getCloudConfiguration` to `ConnectorMetadata` from `Connector` (#30476))
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
<<<<<<< HEAD
=======
    private final HdfsEnvironment hdfsEnvironment;
    private final MetastoreType metastoreType;
>>>>>>> c60edea929 ([Refactor] Move `getCloudConfiguration` to `ConnectorMetadata` from `Connector` (#30476))

    public HiveMetadataFactory(String catalogName,
                               IHiveMetastore metastore,
                               RemoteFileIO remoteFileIO,
                               CachingHiveMetastoreConf hmsConf,
                               CachingRemoteFileConf fileConf,
                               ExecutorService pullRemoteFileExecutor,
                               boolean isRecursive,
<<<<<<< HEAD
                               boolean enableHmsEventsIncrementalSync) {
=======
                               boolean enableHmsEventsIncrementalSync,
                               HdfsEnvironment hdfsEnvironment,
                               MetastoreType metastoreType) {
>>>>>>> c60edea929 ([Refactor] Move `getCloudConfiguration` to `ConnectorMetadata` from `Connector` (#30476))
        this.catalogName = catalogName;
        this.metastore = metastore;
        this.remoteFileIO = remoteFileIO;
        this.perQueryMetastoreMaxNum = hmsConf.getPerQueryCacheMaxNum();
        this.perQueryCacheRemotePathMaxNum = fileConf.getPerQueryCacheMaxSize();
        this.pullRemoteFileExecutor = pullRemoteFileExecutor;
        this.isRecursive = isRecursive;
        this.enableHmsEventsIncrementalSync = enableHmsEventsIncrementalSync;
<<<<<<< HEAD
=======
        this.hdfsEnvironment = hdfsEnvironment;
        this.metastoreType = metastoreType;
>>>>>>> c60edea929 ([Refactor] Move `getCloudConfiguration` to `ConnectorMetadata` from `Connector` (#30476))
    }

    public HiveMetadata create() {
        HiveMetastoreOperations hiveMetastoreOperations = new HiveMetastoreOperations(
<<<<<<< HEAD
                createQueryLevelInstance(metastore, perQueryMetastoreMaxNum), metastore instanceof CachingHiveMetastore);
=======
                createQueryLevelInstance(metastore, perQueryMetastoreMaxNum),
                metastore instanceof CachingHiveMetastore,
                hdfsEnvironment.getConfiguration(), metastoreType, catalogName);
>>>>>>> c60edea929 ([Refactor] Move `getCloudConfiguration` to `ConnectorMetadata` from `Connector` (#30476))
        RemoteFileOperations remoteFileOperations = new RemoteFileOperations(
                CachingRemoteFileIO.createQueryLevelInstance(remoteFileIO, perQueryCacheRemotePathMaxNum),
                pullRemoteFileExecutor,
                isRecursive,
<<<<<<< HEAD
                remoteFileIO instanceof CachingRemoteFileIO);
        HiveStatisticsProvider statisticsProvider = new HiveStatisticsProvider(hiveMetastoreOperations, remoteFileOperations);

        Optional<CacheUpdateProcessor> cacheUpdateProcessor = getCacheUpdateProcessor();
        return new HiveMetadata(catalogName, hiveMetastoreOperations,
                remoteFileOperations, statisticsProvider, cacheUpdateProcessor);
=======
                remoteFileIO instanceof CachingRemoteFileIO,
                hdfsEnvironment.getConfiguration());
        HiveStatisticsProvider statisticsProvider = new HiveStatisticsProvider(hiveMetastoreOperations, remoteFileOperations);

        Optional<CacheUpdateProcessor> cacheUpdateProcessor = getCacheUpdateProcessor();
        return new HiveMetadata(catalogName, hdfsEnvironment, hiveMetastoreOperations, remoteFileOperations,
                statisticsProvider, cacheUpdateProcessor, updateStatisticsExecutor);
>>>>>>> c60edea929 ([Refactor] Move `getCloudConfiguration` to `ConnectorMetadata` from `Connector` (#30476))
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
