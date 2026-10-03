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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.starrocks.connector.ConnectorProperties;
import com.starrocks.connector.ConnectorType;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.MetastoreType;
import com.starrocks.connector.hive.CachingHiveMetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.starrocks.connector.delta.CachingDeltaLakeMetastore.createQueryLevelInstance;
import static com.starrocks.connector.delta.DeltaLakeConnector.HIVE_METASTORE_URIS;

public class DeltaLakeMetadataFactory {
    private final String catalogName;
    private final ThreadPoolExecutor prefetchExecutor = new ThreadPoolExecutor(2, 2, 60, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(128),
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("delta-scan-prefetch-%d").build(),
            new ThreadPoolExecutor.AbortPolicy());
    protected final IDeltaLakeMetastore metastore;
    protected final long perQueryMetastoreMaxNum;
    private final HdfsEnvironment hdfsEnvironment;
    protected final ConnectorProperties connectorProperties;
    protected final MetastoreType metastoreType;

    public DeltaLakeMetadataFactory(String catalogName, IDeltaLakeMetastore metastore, CachingHiveMetastoreConf hmsConf,
                                    Map<String, String> properties, HdfsEnvironment hdfsEnvironment,
                                    MetastoreType metastoreType) {
        this.catalogName = catalogName;
        this.metastore = metastore;
        this.perQueryMetastoreMaxNum = hmsConf.getPerQueryCacheMaxNum();
        this.hdfsEnvironment = hdfsEnvironment;
        this.connectorProperties = new ConnectorProperties(ConnectorType.DELTALAKE, properties);
        if (properties.containsKey(HIVE_METASTORE_URIS)) {
            this.hdfsEnvironment.getConfiguration().set(MetastoreConf.ConfVars.THRIFT_URIS.getHiveName(),
                    properties.get(HIVE_METASTORE_URIS));
        }
        this.metastoreType = metastoreType;
    }

    protected CachingDeltaLakeMetastore createQueryLevelCacheMetastore() {
        return createQueryLevelInstance(metastore, perQueryMetastoreMaxNum);
    }

    public DeltaLakeMetadata create() {
        CachingDeltaLakeMetastore queryLevelCacheMetastore = createQueryLevelCacheMetastore();
        DeltaMetastoreOperations metastoreOperations = new DeltaMetastoreOperations(queryLevelCacheMetastore,
                metastore instanceof CachingDeltaLakeMetastore, metastoreType);

        Optional<DeltaLakeCacheUpdateProcessor> cacheUpdateProcessor = getCacheUpdateProcessor();
        return new DeltaLakeMetadata(hdfsEnvironment, catalogName, metastoreOperations,
                cacheUpdateProcessor.orElse(null), connectorProperties, prefetchExecutor);
    }

    public synchronized Optional<DeltaLakeCacheUpdateProcessor> getCacheUpdateProcessor() {
        Optional<DeltaLakeCacheUpdateProcessor> cacheUpdateProcessor;
        if (metastore instanceof CachingDeltaLakeMetastore) {
            cacheUpdateProcessor = Optional.of(new DeltaLakeCacheUpdateProcessor((CachingDeltaLakeMetastore) metastore));
        } else {
            cacheUpdateProcessor = Optional.empty();
        }

        return cacheUpdateProcessor;
    }

    public void shutdown() {
        // shutdownNow removes queued futures without canceling them; wake any waiting query consumers.
        for (Runnable task : prefetchExecutor.shutdownNow()) {
            if (task instanceof Future<?> future) {
                future.cancel(true);
            }
        }
        metastoreCacheInvalidateCache();
    }

    public void metastoreCacheInvalidateCache() {
        if (metastore instanceof CachingDeltaLakeMetastore) {
            ((CachingDeltaLakeMetastore) metastore).invalidateAll();
        } else {
            ((DeltaLakeMetastore) metastore).invalidateAll();
        }
    }
}
