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


package com.starrocks.connector.iceberg.glue;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.thrift.TException;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class CachedGlueClientPool implements ClientPool<IMetaStoreClient, TException> {
    private static Cache<String, GlueClientPool> clientPoolCache;

    private final Configuration conf;
    private final String catalogName;
    private final int clientPoolSize;
    private final long evictionInterval;

    public CachedGlueClientPool(String catalogName, Configuration conf, Map<String, String> properties) {
        this.catalogName = catalogName;
        this.conf = conf;
        this.clientPoolSize = PropertyUtil.propertyAsInt(properties,
                CatalogProperties.CLIENT_POOL_SIZE,
                CatalogProperties.CLIENT_POOL_SIZE_DEFAULT);
        this.evictionInterval = PropertyUtil.propertyAsLong(properties,
                CatalogProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS,
                CatalogProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS_DEFAULT);
        init();
    }

    GlueClientPool clientPool() {
        return clientPoolCache.get(catalogName, k -> new GlueClientPool(clientPoolSize, conf));
    }

    private synchronized void init() {
        if (clientPoolCache == null) {
            clientPoolCache = Caffeine.newBuilder().expireAfterAccess(evictionInterval, TimeUnit.MILLISECONDS)
                    .removalListener((key, value, cause) -> ((GlueClientPool) value).close())
                    .build();
        }
    }

    @Override
    public <R> R run(Action<R, IMetaStoreClient, TException> action) throws TException, InterruptedException {
        return clientPool().run(action);
    }

    @Override
    public <R> R run(Action<R, IMetaStoreClient, TException> action, boolean retry) throws TException, InterruptedException {
        return clientPool().run(action, retry);
    }
}
