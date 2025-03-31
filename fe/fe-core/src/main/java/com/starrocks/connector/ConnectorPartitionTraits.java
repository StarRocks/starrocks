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

package com.starrocks.connector;

import com.github.benmanes.caffeine.cache.Cache;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.connector.partitiontraits.CachedPartitionTraits;
import com.starrocks.connector.partitiontraits.DeltaLakePartitionTraits;
import com.starrocks.connector.partitiontraits.HivePartitionTraits;
import com.starrocks.connector.partitiontraits.HudiPartitionTraits;
import com.starrocks.connector.partitiontraits.IcebergPartitionTraits;
import com.starrocks.connector.partitiontraits.JDBCPartitionTraits;
import com.starrocks.connector.partitiontraits.KuduPartitionTraits;
import com.starrocks.connector.partitiontraits.OdpsPartitionTraits;
import com.starrocks.connector.partitiontraits.OlapPartitionTraits;
import com.starrocks.connector.partitiontraits.PaimonPartitionTraits;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.common.PCell;
import com.starrocks.sql.optimizer.QueryMaterializationContext;
import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Abstract the partition-related interfaces for different connectors, including Iceberg/Hive/....
 */
public abstract class ConnectorPartitionTraits {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectorPartitionTraits.class);

    private static final Map<Table.TableType, Supplier<ConnectorPartitionTraits>> TRAITS_TABLE =
            ImmutableMap.<Table.TableType, Supplier<ConnectorPartitionTraits>>builder()
                    // Consider all native tables as OLAP
                    .put(Table.TableType.OLAP, OlapPartitionTraits::new)
                    .put(Table.TableType.MATERIALIZED_VIEW, OlapPartitionTraits::new)
                    .put(Table.TableType.CLOUD_NATIVE, OlapPartitionTraits::new)
                    .put(Table.TableType.CLOUD_NATIVE_MATERIALIZED_VIEW, OlapPartitionTraits::new)

                    // external tables
                    .put(Table.TableType.HIVE, HivePartitionTraits::new)
                    .put(Table.TableType.HUDI, HudiPartitionTraits::new)
                    .put(Table.TableType.ICEBERG, IcebergPartitionTraits::new)
                    .put(Table.TableType.PAIMON, PaimonPartitionTraits::new)
                    .put(Table.TableType.ODPS, OdpsPartitionTraits::new)
                    .put(Table.TableType.KUDU, KuduPartitionTraits::new)
                    .put(Table.TableType.JDBC, JDBCPartitionTraits::new)
                    .put(Table.TableType.DELTALAKE, DeltaLakePartitionTraits::new)
                    .build();

    protected Table table;

    protected boolean queryMVRewrite = false;

    public static boolean isSupported(Table.TableType tableType) {
        return TRAITS_TABLE.containsKey(tableType);
    }

    public static boolean isSupportPCTRefresh(Table.TableType tableType) {
        if (!isSupported(tableType)) {
            return false;
        }
        return TRAITS_TABLE.get(tableType).get().isSupportPCTRefresh();
    }

    public static ConnectorPartitionTraits build(Table.TableType tableType) {
        return Preconditions.checkNotNull(TRAITS_TABLE.get(tableType),
                "traits not supported: " + tableType).get();
    }

    /**
     * Build the partition traits for the table, if the current thread has a ConnectContext, use the cache if possible.
     * @param ctx the connect context
     * @param table the table to build partition traits
     * @return the partition traits
     */
    public static ConnectorPartitionTraits buildWithCache(ConnectContext ctx, MaterializedView mv, Table table) {
        ConnectorPartitionTraits delegate = buildWithoutCache(table);
        if (Config.enable_mv_query_context_cache && ctx != null && ctx.getQueryMVContext() != null) {
            QueryMaterializationContext queryMVContext = ctx.getQueryMVContext();
            Cache<Object, Object> cache = queryMVContext.getMvQueryContextCache();
            if (cache == null || queryMVContext.getQueryCacheStats() == null) {
                return delegate;
            }
            return new CachedPartitionTraits(cache, delegate, queryMVContext.getQueryCacheStats(), mv);
        } else {
            return delegate;
        }
    }

    /**
     * Build the partition traits for the table, if the current thread has a ConnectContext, use the cache if possible.
     * @param table the table to build partition traits
     * @return the partition traits
     */
    public static ConnectorPartitionTraits build(MaterializedView mv, Table table) {
        ConnectContext ctx = ConnectContext.get();
        return buildWithCache(ctx, mv, table);
    }

    public static ConnectorPartitionTraits build(Table table) {
        ConnectContext ctx = ConnectContext.get();
        return buildWithCache(ctx, null, table);
    }

    private static ConnectorPartitionTraits buildWithoutCache(Table table) {
        ConnectorPartitionTraits res = build(table.getType());
        res.table = table;
        return res;
    }

    public Table getTable() {
        return this.table;
    }

    public String getTableName() {
        return table.getName();
    }

    /**
     * Whether this table support partition-granular refresh as ref-table
     */
    public abstract boolean isSupportPCTRefresh();

    /**
     * Build a partition key for the table, some of them have specific representations for null values
     */
    public abstract PartitionKey createEmptyKey();

    public String getCatalogDBName() {
        return table.getCatalogDBName();
    }

    /**
     * `createPartitionKeyWithType` is deprecated, use `createPartitionKey` instead.
     * partition values should take care time zone for Iceberg table which is handled by `createPartitionKey`.
     */
    @Deprecated
    public abstract PartitionKey createPartitionKeyWithType(List<String> values, List<Type> types) throws AnalysisException;

    public abstract PartitionKey createPartitionKey(List<String> partitionValues, List<Column> partitionColumns)
            throws AnalysisException;
    /**
     * Get all partitions' name
     */
    public abstract List<String> getPartitionNames();

    /**
     * Get partition columns
     */
    public abstract List<Column> getPartitionColumns();

    /**
     * Get partition range map with the specified partition column and expression
     *
     * @apiNote it must be a range-partitioned table
     */
    public abstract Map<String, Range<PartitionKey>> getPartitionKeyRange(Column partitionColumn, Expr partitionExpr)
            throws AnalysisException;

    /**
     * Get the list-map with specified partition column and expression
     *
     * @apiNote it must be a list-partitioned table
     */
    public abstract Map<String, PCell> getPartitionCells(List<Column> partitionColumns) throws AnalysisException;

    public abstract Map<String, PartitionInfo> getPartitionNameWithPartitionInfo();

    public abstract Map<String, PartitionInfo> getPartitionNameWithPartitionInfo(List<String> partitionNames);

    public List<PartitionInfo> getPartitions(List<String> names) {
        throw new NotImplementedException("getPartitions is not implemented for this table type: " + table.getType());
    }

    /**
     * The max of refresh ts for all partitions
     */
    public abstract Optional<Long> maxPartitionRefreshTs();

    /**
     * Get updated partitions based on current snapshot, to implement incremental refresh
     */
    public abstract Set<String> getUpdatedPartitionNames(List<BaseTableInfo> baseTables,
                                                         MaterializedView.AsyncRefreshContext context);

    /**
     * Get updated partitions based on updated time, return partition names if the partition is updated after the checkTime.
     * For external table, we get partition update time from other system, there may be a time
     * inconsistency between the two systems, so we add extraSeconds to make sure partition update
     * time is later than check time
     * @param checkTime the time to check
     * @param extraSeconds partition updated time would add extraSeconds to check whether it is after checkTime
     */
    public abstract Set<String> getUpdatedPartitionNames(LocalDateTime checkTime, int extraSeconds);

    /**
     * Get the last update time of the table,
     * For external table, we get partition update time from other system, there may be a time
     * inconsistency between the two systems, so we add extraSeconds
     */
    public abstract LocalDateTime getTableLastUpdateTime(int extraSeconds);

    public void setQueryMVRewrite(boolean value) {
        queryMVRewrite = value;
    }

    public boolean isQueryMVRewrite() {
        return queryMVRewrite;
    }
}
