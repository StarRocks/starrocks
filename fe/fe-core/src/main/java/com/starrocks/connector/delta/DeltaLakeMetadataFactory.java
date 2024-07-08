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

import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.MetastoreType;
import com.starrocks.connector.hive.CachingHiveMetastoreConf;
import com.starrocks.connector.metastore.IMetastore;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;

import java.util.Map;
import java.util.Optional;

import static com.starrocks.connector.delta.CachingDeltaLakeMetastore.createQueryLevelInstance;
import static com.starrocks.connector.delta.DeltaLakeConnector.HIVE_METASTORE_URIS;

public class DeltaLakeMetadataFactory {
    private final String catalogName;
    protected final IMetastore metastore;
    protected final long perQueryMetastoreMaxNum;
    private final HdfsEnvironment hdfsEnvironment;
    protected final MetastoreType metastoreType;

    public DeltaLakeMetadataFactory(String catalogName, IMetastore metastore, CachingHiveMetastoreConf hmsConf,
                                    Map<String, String> properties, HdfsEnvironment hdfsEnvironment,
                                    MetastoreType metastoreType) {
        this.catalogName = catalogName;
        this.metastore = metastore;
        this.perQueryMetastoreMaxNum = hmsConf.getPerQueryCacheMaxNum();
        this.hdfsEnvironment = hdfsEnvironment;
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
                cacheUpdateProcessor.orElse(null));
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
}
