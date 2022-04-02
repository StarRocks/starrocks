// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.external.iceberg.cost;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.IcebergResource;
import com.starrocks.catalog.Resource;
import com.starrocks.common.Config;
import com.starrocks.external.iceberg.IcebergCatalog;
import com.starrocks.external.iceberg.IcebergTableKey;
import com.starrocks.external.iceberg.IcebergUtil;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.ArrayList;
import java.util.concurrent.Executor;

import static com.google.common.cache.CacheLoader.asyncReloading;
import static java.util.concurrent.TimeUnit.SECONDS;

public class IcebergMetaCache {
    private static final long MAX_TABLE_CACHE_SIZE = 1000L;

    // statistic cache
    // IcebergTableKey => Statistics
    LoadingCache<IcebergTableKey, IcebergFileStats> tableStatsCache;

    public IcebergMetaCache(Executor executor) {
        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
        cacheBuilder.expireAfterWrite(Config.iceberg_meta_cache_ttl_s, SECONDS);
        cacheBuilder.maximumSize(MAX_TABLE_CACHE_SIZE);
        tableStatsCache = cacheBuilder.build(asyncReloading(
                new CacheLoader<IcebergTableKey, IcebergFileStats>() {
                    @Override
                    public IcebergFileStats load(IcebergTableKey key) throws Exception {
                        Resource resource = Catalog.getCurrentCatalog().getResourceMgr()
                                .getResource(key.getResourceName());
                        IcebergResource icebergResource = (IcebergResource) resource;
                        TableIdentifier identifier = IcebergUtil.getIcebergTableIdentifier(
                                key.getDatabaseName(), key.getTableName());
                        IcebergCatalog catalog = IcebergUtil.getIcebergCatalog(
                                icebergResource.getCatalogType(), icebergResource.getHiveMetastoreURIs());
                        org.apache.iceberg.Table icebergTable = catalog.loadTable(identifier);
                        IcebergTableStatisticCalculator calculator = new IcebergTableStatisticCalculator(icebergTable);
                        IcebergFileStats icebergFileStats = calculator.generateIcebergFileStats(
                                new ArrayList<>(), icebergTable.schema().columns()
                        );
                        return icebergFileStats;
                    }
                }, executor));
    }
}
