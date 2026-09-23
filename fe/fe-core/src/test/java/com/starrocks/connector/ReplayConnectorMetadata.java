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

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import org.apache.commons.collections4.map.CaseInsensitiveMap;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Base for the mock {@link ConnectorMetadata}s that serve external-catalog (iceberg/hive/...) tables during
 * ReplayFromDump. A query dump records such a table only as its declared schema (plus, for newer dumps, its
 * catalog name and, for partitioned tables, its partition list) -- the backing metastore/object store is NOT
 * part of the dump, and reaching out to one is exactly what makes a dump un-replayable off-cluster. Each
 * engine subclass synthesizes a {@link Table} from the declared schema (keeping the real column types) and
 * registers it here; this base then serves it, its captured row count, and its captured per-column
 * statistics (falling back to {@link ColumnStatistic#unknown()} when the dump carried none, exactly as an
 * un-analyzed external table would).
 */
public abstract class ReplayConnectorMetadata implements ConnectorMetadata {
    protected final String catalogName;
    // dbName -> tableName -> table info
    protected final Map<String, Map<String, ReplayTableInfo>> tables = new CaseInsensitiveMap<>();
    protected final AtomicLong idGen = new AtomicLong(1L);

    protected ReplayConnectorMetadata(String catalogName) {
        this.catalogName = catalogName;
    }

    // Store a table an engine subclass has built from the dump, along with its captured row count, per-column
    // statistics, and partition names (any of which may be empty/null when the dump did not capture them).
    protected void register(String dbName, String tableName, Table table, long rowCount,
                            Map<String, ColumnStatistic> columnStats, List<String> partitionNames) {
        tables.computeIfAbsent(dbName, k -> new CaseInsensitiveMap<>())
                .put(tableName, new ReplayTableInfo(table, rowCount, columnStats, partitionNames));
    }

    protected ReplayTableInfo lookup(Table table) {
        return lookup(table.getCatalogDBName(), table.getCatalogTableName());
    }

    protected ReplayTableInfo lookup(String dbName, String tableName) {
        Map<String, ReplayTableInfo> dbTables = tables.get(dbName);
        return dbTables == null ? null : dbTables.get(tableName);
    }

    @Override
    public Database getDb(ConnectContext context, String dbName) {
        return new Database(idGen.getAndIncrement(), dbName);
    }

    @Override
    public Table getTable(ConnectContext context, String dbName, String tblName) {
        ReplayTableInfo info = lookup(dbName, tblName);
        return info == null ? null : info.table;
    }

    @Override
    public List<String> listPartitionNames(String dbName, String tableName,
                                            ConnectorMetadataRequestContext requestContext) {
        ReplayTableInfo info = lookup(dbName, tableName);
        return (info == null || info.partitionNames == null)
                ? Lists.newArrayList() : Lists.newArrayList(info.partitionNames);
    }

    @Override
    public Statistics getTableStatistics(OptimizerContext session, Table table,
                                         Map<ColumnRefOperator, Column> columns, List<PartitionKey> partitionKeys,
                                         ScalarOperator predicate, long limit, TvrVersionRange version) {
        ReplayTableInfo info = lookup(table);
        Statistics.Builder builder = Statistics.builder();
        builder.setOutputRowCount(info == null ? 1 : info.rowCount);
        for (ColumnRefOperator col : columns.keySet()) {
            ColumnStatistic cs = info == null ? null : info.columnStats.get(col.getName());
            builder.addColumnStatistic(col, cs == null ? ColumnStatistic.unknown() : cs);
        }
        return builder.build();
    }

    /** Everything the replay needs to serve a single external table, built from the dump. */
    private static class ReplayTableInfo {
        private final Table table;
        private final long rowCount;
        // column name (case-insensitive) -> statistic captured in the dump; empty when none was captured.
        private final Map<String, ColumnStatistic> columnStats;
        // partition names captured in the dump (metastore form "col=val/col2=val2"); null/empty when the
        // table is unpartitioned or the dump did not capture them.
        private final List<String> partitionNames;

        private ReplayTableInfo(Table table, long rowCount, Map<String, ColumnStatistic> columnStats,
                                  List<String> partitionNames) {
            this.table = table;
            this.rowCount = rowCount;
            this.columnStats = columnStats == null ? new CaseInsensitiveMap<>() : new CaseInsensitiveMap<>(columnStats);
            this.partitionNames = partitionNames;
        }
    }
}
