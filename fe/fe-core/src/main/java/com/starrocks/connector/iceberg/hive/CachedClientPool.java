// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.iceberg.hive;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.thrift.TException;

import java.util.Map;
import java.util.concurrent.TimeUnit;

// TODO(@caneGuy) refactor with HiveRepository
public class CachedClientPool implements ClientPool<IMetaStoreClient, TException> {

    private static Cache<String, HiveClientPool> clientPoolCache;

    private final Configuration conf;
    private final String metastoreUri;
    private final int clientPoolSize;
    private final long evictionInterval;

    public CachedClientPool(Configuration conf, Map<String, String> properties) {
        this.conf = conf;
        this.metastoreUri = conf.get(HiveConf.ConfVars.METASTOREURIS.varname, "");
        this.clientPoolSize = PropertyUtil.propertyAsInt(properties,
                CatalogProperties.CLIENT_POOL_SIZE,
                CatalogProperties.CLIENT_POOL_SIZE_DEFAULT);
        this.evictionInterval = PropertyUtil.propertyAsLong(properties,
                CatalogProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS,
                CatalogProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS_DEFAULT);
        init();
    }

    HiveClientPool clientPool() {
        return clientPoolCache.get(metastoreUri, k -> new HiveClientPool(clientPoolSize, conf));
    }

    private synchronized void init() {
        if (clientPoolCache == null) {
            clientPoolCache = Caffeine.newBuilder().expireAfterAccess(evictionInterval, TimeUnit.MILLISECONDS)
                    .removalListener((key, value, cause) -> ((HiveClientPool) value).close())
                    .build();
        }
    }

    @Override
    public <R> R run(Action<R, IMetaStoreClient, TException> action) throws TException, InterruptedException {
        return clientPool().run(action);
    }
}
