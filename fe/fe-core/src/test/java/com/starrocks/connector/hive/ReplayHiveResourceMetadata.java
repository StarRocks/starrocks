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

package com.starrocks.connector.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorMetadataRequestContext;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.RemoteFileInfoDefaultSource;
import com.starrocks.connector.RemoteFileInfoSource;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import org.apache.commons.collections4.map.CaseInsensitiveMap;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static com.starrocks.catalog.Table.TableType.HIVE;

/**
 * A {@link ConnectorMetadata} that serves modern hive-catalog external tables during ReplayFromDump.
 *
 * <p>A query dump captures such a table only as a {@code CREATE EXTERNAL TABLE ... ENGINE=HIVE
 * ("resource"=...)} statement in {@code table_meta} plus an {@code hms_table} entry; the backing hive
 * metastore is NOT part of the dump, and reaching out to a real metastore is exactly what makes a dump
 * un-replayable off-cluster. This metadata synthesizes a {@link HiveTable} from the declared column
 * schema (keeping the real column types, unlike the legacy resource-mapping replay which coerced every
 * column to STRING) so the table can be planned entirely offline.
 *
 * <p>{@link #getTableStatistics} returns the captured row count and, for any column the dump carried a
 * statistic for, that statistic -- otherwise {@link ColumnStatistic#unknown()}, mirroring what a hive
 * catalog hands back when column statistics were never collected. Remote-file listing is mocked with a
 * single empty split so the scan can be built without touching storage.
 */
public class ReplayHiveResourceMetadata implements ConnectorMetadata {
    private static final List<RemoteFileInfo> MOCKED_FILES =
            ImmutableList.of(new RemoteFileInfo(null, ImmutableList.of(), null));

    private final String catalogName;
    // dbName -> tableName -> table info
    private final Map<String, Map<String, HiveTableInfo>> tables = new CaseInsensitiveMap<>();
    private final AtomicLong idGen = new AtomicLong(1L);

    public ReplayHiveResourceMetadata(String catalogName) {
        this.catalogName = catalogName;
    }

    public void registerTable(String dbName, String tableName, List<Column> columns,
                              List<String> dataColumnNames, List<String> partitionColumnNames,
                              long rowCount, Map<String, ColumnStatistic> columnStats) {
        long tableId = GlobalStateMgr.getCurrentState().getNextId();
        HiveTable table = HiveTable.builder()
                .setId(tableId)
                .setTableName(tableName)
                .setCatalogName(catalogName)
                .setResourceName(catalogName)
                .setHiveDbName(dbName)
                .setHiveTableName(tableName)
                .setPartitionColumnNames(partitionColumnNames)
                .setDataColumnNames(dataColumnNames)
                .setFullSchema(columns)
                .setTableLocation("")
                .setCreateTime(0L)
                .build();
        HiveTableInfo info = new HiveTableInfo(table, rowCount,
                columnStats == null ? new CaseInsensitiveMap<>() : new CaseInsensitiveMap<>(columnStats));
        tables.computeIfAbsent(dbName, k -> new CaseInsensitiveMap<>()).put(tableName, info);
    }

    private HiveTableInfo lookup(Table table) {
        Map<String, HiveTableInfo> dbTables = tables.get(table.getCatalogDBName());
        return dbTables == null ? null : dbTables.get(table.getCatalogTableName());
    }

    @Override
    public Table.TableType getTableType() {
        return HIVE;
    }

    @Override
    public Database getDb(ConnectContext context, String dbName) {
        return new Database(idGen.getAndIncrement(), dbName);
    }

    @Override
    public Table getTable(ConnectContext context, String dbName, String tblName) {
        Map<String, HiveTableInfo> dbTables = tables.get(dbName);
        HiveTableInfo info = dbTables == null ? null : dbTables.get(tblName);
        return info == null ? null : info.table;
    }

    @Override
    public List<String> listPartitionNames(String dbName, String tableName,
                                            ConnectorMetadataRequestContext requestContext) {
        return Lists.newArrayList();
    }

    @Override
    public Statistics getTableStatistics(OptimizerContext session, Table table,
                                         Map<ColumnRefOperator, Column> columns, List<PartitionKey> partitionKeys,
                                         ScalarOperator predicate, long limit, TvrVersionRange version) {
        HiveTableInfo info = lookup(table);
        Statistics.Builder builder = Statistics.builder();
        builder.setOutputRowCount(info == null ? 1 : info.rowCount);
        for (ColumnRefOperator col : columns.keySet()) {
            ColumnStatistic cs = info == null ? null : info.columnStats.get(col.getName());
            builder.addColumnStatistic(col, cs == null ? ColumnStatistic.unknown() : cs);
        }
        return builder.build();
    }

    @Override
    public List<RemoteFileInfo> getRemoteFiles(Table table, GetRemoteFilesParams params) {
        return Lists.newArrayList(MOCKED_FILES);
    }

    @Override
    public RemoteFileInfoSource getRemoteFilesAsync(Table table, GetRemoteFilesParams params) {
        return new RemoteFileInfoDefaultSource(Lists.newArrayList(MOCKED_FILES));
    }

    private static class HiveTableInfo {
        private final HiveTable table;
        private final long rowCount;
        private final Map<String, ColumnStatistic> columnStats;

        private HiveTableInfo(HiveTable table, long rowCount, Map<String, ColumnStatistic> columnStats) {
            this.table = table;
            this.rowCount = rowCount;
            this.columnStats = columnStats;
        }
    }
}
