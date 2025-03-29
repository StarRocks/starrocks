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

package com.starrocks.connector.statistics;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.statistic.ColumnStatsMeta;
import com.starrocks.statistic.ExternalBasicStatsMeta;
import com.starrocks.statistic.StatsConstants;
import io.trino.hive.$internal.org.apache.commons.lang3.tuple.ImmutableTriple;
import io.trino.hive.$internal.org.apache.commons.lang3.tuple.Triple;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class StatisticsUtils {
    public static Table getTableByUUID(ConnectContext context, String tableUUID) {
        String[] splits = tableUUID.split("\\.");

        Preconditions.checkState(splits.length == 4);
        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(context, splits[0], splits[1], splits[2]);
        if (table == null) {
            throw new SemanticException("Table [%s.%s.%s] is not existed", splits[0], splits[1], splits[2]);
        }
        if (table.getUUID().equals(tableUUID)) {
            return table;
        } else {
            throw new SemanticException("Table [%s.%s.%s] is not existed", splits[0], splits[1], splits[2]);
        }
    }

    public static Triple<String, Database, Table> getTableTripleByUUID(ConnectContext context, String tableUUID) {
        String[] splits = tableUUID.split("\\.");

        Preconditions.checkState(splits.length == 4);
        Database db = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(context, splits[0], splits[1]);
        if (db == null) {
            throw new SemanticException("Database [%s.%s] is not existed", splits[0], splits[1]);
        }

        Table table = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(context, splits[0], splits[1], splits[2]);
        if (table == null) {
            throw new SemanticException("Table [%s.%s.%s] is not existed", splits[0], splits[1], splits[2]);
        }
        if (!table.getUUID().equals(tableUUID)) {
            throw new SemanticException("Table [%s.%s.%s] is not existed", splits[0], splits[1], splits[2]);
        }

        return ImmutableTriple.of(splits[0], db, table);
    }

    public static List<String> getTableNameByUUID(String tableUUID) {
        String[] splits = tableUUID.split("\\.");
        Preconditions.checkState(splits.length >= 3);
        return ImmutableList.of(splits[0], splits[1], splits[2]);
    }

    public static Statistics buildDefaultStatistics(Set<ColumnRefOperator> columns) {
        Statistics.Builder statisticsBuilder = Statistics.builder();
        statisticsBuilder.setOutputRowCount(1);
        statisticsBuilder.addColumnStatistics(
                columns.stream().collect(Collectors.toMap(column -> column, column -> ColumnStatistic.unknown())));
        return statisticsBuilder.build();
    }

    public static ConnectorTableColumnStats estimateColumnStatistics(Table table, String columnName,
                                                                     ConnectorTableColumnStats connectorTableColumnStats) {
        Triple<String, Database, Table> tableIdentifier = getTableTripleByUUID(new ConnectContext(), table.getUUID());
        ExternalBasicStatsMeta externalBasicStatsMeta = GlobalStateMgr.getCurrentState().getAnalyzeMgr().
                getExternalTableBasicStatsMeta(tableIdentifier.getLeft(), tableIdentifier.getMiddle().getFullName(),
                        tableIdentifier.getRight().getName());

        if (externalBasicStatsMeta == null) {
            return connectorTableColumnStats;
        }

        Map<String, ColumnStatsMeta> columnStatsMetaMap = externalBasicStatsMeta.getColumnStatsMetaMap();
        if (!columnStatsMetaMap.containsKey(columnName)) {
            return connectorTableColumnStats;
        }

        ColumnStatsMeta columnStatsMeta = columnStatsMetaMap.get(columnName);
        if (columnStatsMeta.getType() == StatsConstants.AnalyzeType.FULL) {
            return connectorTableColumnStats;
        }

        // the column statistics analyze type is sample , we need to estimate the table level column statistics
        int sampledPartitionSize = columnStatsMeta.getSampledPartitionsHashValue().size();
        int totalPartitionSize = columnStatsMeta.getAllPartitionSize();

        double avgPartitionRowCount = connectorTableColumnStats.getRowCount() * 1.0 / sampledPartitionSize;
        long totalRowCount = (long) avgPartitionRowCount * totalPartitionSize;

        return new ConnectorTableColumnStats(connectorTableColumnStats.getColumnStatistic(),
                totalRowCount, connectorTableColumnStats.getUpdateTime());
    }

}
