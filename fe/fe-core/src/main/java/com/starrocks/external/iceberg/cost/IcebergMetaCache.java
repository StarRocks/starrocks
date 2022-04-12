// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.external.iceberg.cost;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergResource;
import com.starrocks.catalog.Resource;
import com.starrocks.common.Config;
import com.starrocks.external.iceberg.IcebergCatalog;
import com.starrocks.external.iceberg.IcebergTableKey;
import com.starrocks.external.iceberg.IcebergUtil;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import static com.google.common.cache.CacheLoader.asyncReloading;
import static java.util.concurrent.TimeUnit.SECONDS;

public class IcebergMetaCache {
    private static final Logger LOG = LogManager.getLogger(IcebergMetaCache.class);

    private static final long MAX_TABLE_CACHE_SIZE = 1000L;

    // statistic cache
    // IcebergTableKey => IcebergFileStats
    @VisibleForTesting
    protected LoadingCache<IcebergTableKey, IcebergFileStats> tableStatsCache;

    public IcebergMetaCache(Executor executor) {
        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
        cacheBuilder.expireAfterWrite(Config.iceberg_meta_cache_ttl_s, SECONDS);
        cacheBuilder.maximumSize(MAX_TABLE_CACHE_SIZE);
        tableStatsCache = cacheBuilder.build(asyncReloading(
                new CacheLoader<IcebergTableKey, IcebergFileStats>() {
                    @Override
                    public IcebergFileStats load(IcebergTableKey key) throws Exception {
                        Table table = getTable(key);
                        return getIcebergFileStats(table, getCalculator(table));
                    }
                }, executor));
    }

    public Statistics getTableStatistics(IcebergTableKey key,
                                         Map<ColumnRefOperator, Column> colRefToColumnMetaMap) {
        IcebergFileStats icebergFileStats;
        try {
            icebergFileStats = tableStatsCache.get(key);
        } catch (ExecutionException e) {
            LOG.warn("Get iceberg file stats from cache with failure!", e);
            Table table = getTable(key);
            icebergFileStats = getIcebergFileStats(table, getCalculator(table));
        }
        Table table = getTable(key);
        IcebergTableStatisticCalculator calculator = getCalculator(table);
        List<Types.NestedField> columns = table.schema().columns();
        return calculator.makeTableStatistics(icebergFileStats, columns, colRefToColumnMetaMap);
    }

    public List<ColumnStatistic> getColumnStatistics(IcebergTableKey key,
                                                     Map<ColumnRefOperator, Column> colRefToColumnMetaMap) {
        IcebergFileStats icebergFileStats;
        try {
            icebergFileStats = tableStatsCache.get(key);
        } catch (ExecutionException e) {
            LOG.warn("Get iceberg file stats from cache with failure!", e);
            Table table = getTable(key);
            icebergFileStats = getIcebergFileStats(table, getCalculator(table));
        }
        Table table = getTable(key);
        IcebergTableStatisticCalculator calculator = getCalculator(table);
        List<Types.NestedField> columns = table.schema().columns();
        return calculator.makeColumnStatistics(icebergFileStats, columns, colRefToColumnMetaMap);
    }

    @VisibleForTesting
    protected IcebergFileStats getIcebergFileStats(Table table,
                                                IcebergTableStatisticCalculator calculator) {
        IcebergFileStats icebergFileStats = calculator.generateIcebergFileStats(
                new ArrayList<>(), table.schema().columns()
        );
        return icebergFileStats;
    }

    @VisibleForTesting
    protected IcebergTableStatisticCalculator getCalculator(Table table) {
        IcebergTableStatisticCalculator calculator = new IcebergTableStatisticCalculator(table);
        return calculator;
    }

    @VisibleForTesting
    protected org.apache.iceberg.Table getTable(IcebergTableKey key) {
        Resource resource = Catalog.getCurrentCatalog().getResourceMgr()
                .getResource(key.getResourceName());
        IcebergResource icebergResource = (IcebergResource) resource;
        TableIdentifier identifier = IcebergUtil.getIcebergTableIdentifier(
                key.getDatabaseName(), key.getTableName());
        IcebergCatalog catalog = IcebergUtil.getIcebergCatalog(
                icebergResource.getCatalogType(), icebergResource.getHiveMetastoreURIs());
        Table icebergTable = catalog.loadTable(identifier);
        return icebergTable;
    }
}
