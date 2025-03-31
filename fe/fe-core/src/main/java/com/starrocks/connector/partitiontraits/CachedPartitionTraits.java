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

package com.starrocks.connector.partitiontraits;

import com.github.benmanes.caffeine.cache.Cache;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ThrowingSupplier;
import com.starrocks.connector.ConnectorPartitionTraits;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.sql.common.PCell;
import com.starrocks.sql.optimizer.QueryMaterializationContext;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * CachedPartitionTraits is a wrapper class for ConnectorPartitionTraits, which is used to cache the partition info once it has
 * been fetched from the connector.
 *
 * NOTE: This class will cache partition info during the query connect context which it's unexpected if the table partition
 * changes during the query execution. But it can be used in mv refresh since mv refresh will snapshot the base table's partition
 * info before the refresh.
 */
public class CachedPartitionTraits extends DefaultTraits {
    private final Cache<Object, Object> cache;
    private final ConnectorPartitionTraits delegate;
    private final QueryMaterializationContext.QueryCacheStats stats;
    private final MaterializedView mv;

    public CachedPartitionTraits(Cache<Object, Object> cache,
                                 ConnectorPartitionTraits delegate,
                                 QueryMaterializationContext.QueryCacheStats stats,
                                 MaterializedView mv) {
        Objects.requireNonNull(cache);
        Objects.requireNonNull(delegate);
        Objects.requireNonNull(stats);
        this.cache = cache;
        this.delegate = delegate;
        this.table = delegate.getTable();
        this.stats = stats;
        this.mv = mv;
    }

    /**
     * Construct the cache key: cache_{prefix}_{tableId}
     * @param prefix use the prefix to distinguish different cache keys, please check the unique usage of this method
     * @return the cache key
     */
    private String buildCacheKey(String prefix) {
        return String.format("cache_%s_%s_%s", prefix, (mv == null) ? "0" : mv.getId(), table.getId());
    }

    /**
     * Get the cache value by key, if the value is not in cache, use the supplier to get the value and put it into cache
     * @param prefix the cache key prefix
     * @param supplier the supplier to get the value if the key is not in the cache
     * @param defaultSupplier  the default supplier to initialize the cache value
     * @return the cache value
     * @param <T> the cache result type
     */
    private <T> T getCache(String prefix, Supplier<T> supplier, Supplier<T> defaultSupplier) {
        String cacheKey = buildCacheKey(prefix);
        T result = (T) cache.get(cacheKey, k -> supplier.get());
        stats.incr(cacheKey);
        return Optional.ofNullable(result).map(x -> (T) x).orElse(defaultSupplier.get());
    }

    /**
     * it's another version to getCache which the input supplier can be a throwing supplier
     */
    private <T> T getCacheWithException(String key, ThrowingSupplier<T> supplier, Supplier<T> defaultSupplier) {
        String cacheKey = buildCacheKey(key);
        T result = (T) cache.get(cacheKey, k -> {
            try {
                return supplier.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        stats.incr(cacheKey);
        return Optional.ofNullable(result).map(x -> (T) x).orElse(defaultSupplier.get());
    }

    @Override
    public PartitionKey createEmptyKey() {
        return delegate.createEmptyKey();
    }

    @Override
    public String getCatalogDBName() {
        return delegate.getCatalogDBName();
    }

    @Override
    public boolean isSupportPCTRefresh() {
        return delegate.isSupportPCTRefresh();
    }

    @Override
    public PartitionKey createPartitionKeyWithType(List<String> values, List<Type> types) throws AnalysisException {
        return delegate.createPartitionKeyWithType(values, types);
    }

    @Override
    public PartitionKey createPartitionKey(List<String> partitionValues, List<Column> partitionColumns)
            throws AnalysisException {
        return delegate.createPartitionKey(partitionValues, partitionColumns);
    }

    @Override
    public String getTableName() {
        return delegate.getTableName();
    }

    @Override
    public List<String> getPartitionNames() {
        return getCacheWithException("partitionNames", delegate::getPartitionNames, () -> Lists.newArrayList());
    }

    @Override
    public List<Column> getPartitionColumns() {
        return delegate.getPartitionColumns();
    }

    @Override
    public Map<String, Range<PartitionKey>> getPartitionKeyRange(Column partitionColumn, Expr partitionExpr)
            throws AnalysisException {
        return getCacheWithException("getPartitionKeyRange",
                () -> delegate.getPartitionKeyRange(partitionColumn, partitionExpr), () -> Maps.newHashMap());
    }

    @Override
    public Map<String, PCell> getPartitionCells(List<Column> partitionColumns) throws AnalysisException {
        return getCacheWithException("getPartitionList",
                () -> delegate.getPartitionCells(partitionColumns), () -> Maps.newHashMap());
    }

    @Override
    public Map<String, PartitionInfo> getPartitionNameWithPartitionInfo() {
        return getCache("getPartitionNameWithPartitionInfo1",
                () -> delegate.getPartitionNameWithPartitionInfo(), () -> Maps.newHashMap());
    }

    @Override
    public Map<String, PartitionInfo> getPartitionNameWithPartitionInfo(List<String> partitionNames) {
        // no cache since partition names are not stable.
        return delegate.getPartitionNameWithPartitionInfo(partitionNames);
    }

    @Override
    public List<PartitionInfo> getPartitions(List<String> names) {
        // no cache since partition names are not stable.
        return delegate.getPartitions(names);
    }

    @Override
    public Optional<Long> maxPartitionRefreshTs() {
        return getCache("maxPartitionRefreshTs", () -> delegate.maxPartitionRefreshTs(), () -> Optional.empty());
    }

    @Override
    public Set<String> getUpdatedPartitionNames(List<BaseTableInfo> baseTables,
                                                MaterializedView.AsyncRefreshContext context) {
        return getCache("getUpdatedPartitionNames", () -> delegate.getUpdatedPartitionNames(baseTables, context),
                () -> Sets.newHashSet());
    }
}
