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

package com.starrocks.connector.iceberg;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorMetadatRequestContext;
import com.starrocks.connector.ConnectorTableVersion;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.TableVersionRange;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static com.starrocks.catalog.Table.TableType.ICEBERG;

/**
 * A {@link ConnectorMetadata} that serves old-style resource-mapping iceberg external tables during
 * ReplayFromDump. A captured query dump records such tables only as {@code CREATE EXTERNAL TABLE ...
 * ENGINE=ICEBERG ("resource"=...)} statements in {@code table_meta}; the backing iceberg resource and
 * its metastore metadata are NOT part of the dump, so the normal create path cannot resolve them.
 *
 * <p>This metadata synthesizes a native iceberg table from the declared column schema so the table can
 * be created and planned. Crucially {@link #getTableStatistics} returns ONLY the row count and NO
 * per-column statistics, faithfully mirroring an empty {@code column_statistics} section in the dump --
 * that is exactly the state that reproduces missing-statistic planner bugs.
 */
public class ReplayIcebergResourceMetadata implements ConnectorMetadata {
    private final String catalogName;
    // dbName -> tableName -> table
    private final Map<String, Map<String, MockIcebergTable>> tables = new CaseInsensitiveMap<>();
    private final Map<String, Map<String, Long>> rowCounts = new CaseInsensitiveMap<>();
    private final AtomicLong idGen = new AtomicLong(1L);

    public ReplayIcebergResourceMetadata(String catalogName) {
        this.catalogName = catalogName;
    }

    public void registerTable(String resourceName, String dbName, String tableName, List<Column> columns, long rowCount)
            throws IOException {
        Schema schema = IcebergApiConverter.toIcebergApiSchema(columns);
        PartitionSpec spec = PartitionSpec.builderFor(schema).build();
        File baseDir = Files.createTempDirectory("replay_iceberg_").toFile();
        TestTables.TestTable nativeTable = TestTables.create(
                new File(baseDir, dbName + "_" + tableName), tableName, schema, spec, 1);
        long tableId = GlobalStateMgr.getCurrentState().getNextId();
        MockIcebergTable table = new MockIcebergTable(tableId, tableName, catalogName, resourceName,
                dbName, tableName, columns, nativeTable, Maps.newHashMap(), "");
        tables.computeIfAbsent(dbName, k -> new CaseInsensitiveMap<>()).put(tableName, table);
        rowCounts.computeIfAbsent(dbName, k -> new CaseInsensitiveMap<>()).put(tableName, rowCount);
    }

    @Override
    public Table.TableType getTableType() {
        return ICEBERG;
    }

    @Override
    public Database getDb(ConnectContext context, String dbName) {
        return new Database(idGen.getAndIncrement(), dbName);
    }

    @Override
    public Table getTable(ConnectContext context, String dbName, String tblName) {
        Map<String, MockIcebergTable> dbTables = tables.get(dbName);
        return dbTables == null ? null : dbTables.get(tblName);
    }

    @Override
    public List<String> listPartitionNames(String dbName, String tableName,
                                            ConnectorMetadatRequestContext requestContext) {
        return Lists.newArrayList();
    }

    @Override
    public List<PartitionInfo> getPartitions(Table table, List<String> partitionNames) {
        return Lists.newArrayList(new Partition(100L));
    }

    @Override
    public TableVersionRange getTableVersionRange(String dbName, Table table,
                                                  Optional<ConnectorTableVersion> startVersion,
                                                  Optional<ConnectorTableVersion> endVersion) {
        return TableVersionRange.withEnd(Optional.of(1L));
    }

    @Override
    public Statistics getTableStatistics(OptimizerContext session, Table table,
                                         Map<ColumnRefOperator, Column> columns, List<PartitionKey> partitionKeys,
                                         ScalarOperator predicate, long limit, TableVersionRange version) {
        // Intentionally return row count only -- NO per-column statistics. This mirrors the empty
        // column_statistics captured in the dump and is what triggers the planner bug under replay.
        IcebergTable icebergTable = (IcebergTable) table;
        long rc = rowCounts.getOrDefault(icebergTable.getCatalogDBName(), Collections.emptyMap())
                .getOrDefault(icebergTable.getCatalogTableName(), 1L);
        Statistics.Builder builder = Statistics.builder();
        builder.setOutputRowCount(rc);
        // The captured dump has no column statistics, mirroring an iceberg table whose catalog returns
        // none. Real IcebergMetadata.getTableStatistics still hands back an UNKNOWN ColumnStatistic for
        // every requested scan column -- so the scan output is complete here. Any column that goes
        // missing downstream is dropped by a later optimizer step, not by the scan.
        for (ColumnRefOperator col : columns.keySet()) {
            builder.addColumnStatistic(col, ColumnStatistic.unknown());
        }
        return builder.build();
    }
}
