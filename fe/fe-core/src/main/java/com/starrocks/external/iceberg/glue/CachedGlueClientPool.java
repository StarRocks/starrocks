// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.iceberg.glue;

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
}
