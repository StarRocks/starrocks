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

package com.starrocks.datacache.copilot;

import com.google.common.collect.ImmutableSet;
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.RecommendDataCacheSelectStmt;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.thrift.TStatisticData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

public class DataCacheCopilotSimpleRecommender implements DataCacheCopilotRecommender {

    private static final Logger LOG = LogManager.getLogger(DataCacheCopilotSimpleRecommender.class);

    private final RecommendDataCacheSelectStmt stmt;

    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("RECOMMEND_SQL", ScalarType.createVarchar(500)))
                    .addColumn(new Column("WEIGHT", ScalarType.createVarcharType(20)))
                    .build();

    public DataCacheCopilotSimpleRecommender(RecommendDataCacheSelectStmt stmt) {
        this.stmt = stmt;
    }

    @Override
    public ShowResultSet recommend() {
        // flush access logs to BE first
        DataCacheCopilotRepo repository = GlobalStateMgr.getCurrentState().getDataCacheCopilotRepo();
        repository.flushStorageToBE();

        String collectSQL = SQLBuilder.buildCollectSQL(stmt.getTarget(), stmt.getInterval(), stmt.getLimitElement());

        // get TStatisticData results from BE
        List<TStatisticData> tStatisticDataList = repository.collectCopilotStatistics(collectSQL);
        // convert TStatisticData -> StatisticsData, so that we can apply some transform algorithm
        List<StatisticsData> statisticDataList = StatisticsData.buildFromThrift(tStatisticDataList);
        // generate cache select sql based on StatisticsData
        List<List<String>> resultSQLs = new LinkedList<>();
        for (StatisticsData statisticsData : statisticDataList) {
            String buildSQL = SQLBuilder.buildCacheSelectSQL(statisticsData);
            List<String> sql = new ArrayList<>(2);
            sql.add(buildSQL);
            sql.add(String.valueOf(statisticsData.count));
            resultSQLs.add(sql);
        }
        return new ShowResultSet(META_DATA, resultSQLs);
    }

    private static class StatisticsData {
        private final String catalogName;
        private final String databaseName;
        private final String tableName;
        private final ImmutableSet<String> partitionNames;
        private final ImmutableSet<String> columnNames;
        private final long count;

        private StatisticsData(String catalogName, String databaseName, String tableName,
                               ImmutableSet<String> partitionNames,
                               ImmutableSet<String> columnNames, long count) {
            this.catalogName = catalogName;
            this.databaseName = databaseName;
            this.tableName = tableName;
            this.partitionNames = partitionNames;
            this.columnNames = columnNames;
            this.count = count;
        }

        public static List<StatisticsData> buildFromThrift(List<TStatisticData> tStatisticDataList) {
            List<StatisticsData> statisticsDataList = new ArrayList<>(tStatisticDataList.size());
            for (TStatisticData tStatisticData : tStatisticDataList) {
                ImmutableSet.Builder<String> partitionNames = ImmutableSet.builder();
                if (!tStatisticData.getPartitionName().isBlank()) {
                    // partitionName empty means there is no predicate on partition column
                    partitionNames.add(tStatisticData.getPartitionName().split(","));
                }

                ImmutableSet.Builder<String> columnNames = ImmutableSet.builder();
                columnNames.add(tStatisticData.getColumnName().split(","));

                statisticsDataList.add(
                        new StatisticsData(tStatisticData.getCatalogName(), tStatisticData.getDatabaseName(),
                                tStatisticData.getTableName(), partitionNames.build(), columnNames.build(),
                                tStatisticData.getRowCount()));
            }
            return statisticsDataList;
        }
    }

    private static class SQLBuilder {
        private static final String COLLECT_SQL_TEMPLATE =
                "SELECT CAST($copilotVersion AS INT), `$catalogName`, `$databaseName`, `$tableName`, " +
                        "GROUP_CONCAT(`$partitionName` SEPARATOR ',') AS `partition_names`, " +
                        "GROUP_CONCAT(`$columnName` SEPARATOR ',') AS `column_names`, `$count` " +
                        "FROM `$internalCatalog`.`$statisticsDb`.`$copilotTable` $whereCondition " +
                        "GROUP BY `$catalogName`, `$databaseName`, `$tableName`, `$accessTime`, `$count` " +
                        "ORDER BY `$count` DESC $limitCondition";
        private static final String CACHE_SELECT_TEMPLATE =
                "CACHE SELECT $columnNames FROM `$catalogName`.`$databaseName`.`$tableName` $whereCondition";
        private static final VelocityEngine DEFAULT_VELOCITY_ENGINE = new VelocityEngine();

        public static String buildCollectSQL(Optional<QualifiedName> qualifiedName, long interval,
                                             LimitElement limitElement) {
            VelocityContext context = new VelocityContext();
            context.put("copilotVersion", StatsConstants.STATISTICS_DATACACHE_COPILOT_VERSION);
            context.put("catalogName", DataCacheCopilotConstants.CATALOG_NAME);
            context.put("databaseName", DataCacheCopilotConstants.DATABASE_NAME);
            context.put("tableName", DataCacheCopilotConstants.TABLE_NAME);
            context.put("partitionName", DataCacheCopilotConstants.PARTITION_NAME);
            context.put("columnName", DataCacheCopilotConstants.COLUMN_NAME);
            context.put("accessTime", DataCacheCopilotConstants.ACCESS_TIME_NAME);
            context.put("count", DataCacheCopilotConstants.COUNT_NAME);
            context.put("internalCatalog", InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
            context.put("statisticsDb", StatsConstants.STATISTICS_DB_NAME);
            context.put("copilotTable", DataCacheCopilotConstants.DATACACHE_COPILOT_STATISTICS_TABLE_NAME);
            context.put("whereCondition", buildWhereCondition(qualifiedName, interval));
            context.put("limitCondition", buildLimitCondition(limitElement));

            StringWriter sw = new StringWriter();
            DEFAULT_VELOCITY_ENGINE.evaluate(context, sw, "", COLLECT_SQL_TEMPLATE);
            return sw.toString();
        }

        private static String buildWhereCondition(Optional<QualifiedName> qualifiedName, long interval) {
            List<String> conditions = new LinkedList<>();

            if (interval > 0) {
                // `access_time` >= from_unixtime(xxx)
                conditions.add(
                        String.format("`" + DataCacheCopilotConstants.ACCESS_TIME_NAME + "` >= from_unixtime(%d)",
                                System.currentTimeMillis() / 1000 - interval));
            }

            if (qualifiedName.isPresent()) {
                List<String> parts = qualifiedName.get().getParts();
                for (int i = 0; i < parts.size(); i++) {
                    if (i == 0) {
                        // `catalog_name` = 'xxxx'
                        conditions.add(String.format("`" + DataCacheCopilotConstants.CATALOG_NAME + "` = '%s'",
                                parts.get(0)));
                    } else if (i == 1) {
                        // `database_name` = 'xxxx'
                        conditions.add(String.format("`" + DataCacheCopilotConstants.DATABASE_NAME + "` = '%s'",
                                parts.get(1)));
                    } else if (i == 2) {
                        // `table_name` = 'xxxx'
                        conditions.add(String.format("`" + DataCacheCopilotConstants.TABLE_NAME + "` = '%s'",
                                parts.get(2)));
                    } else {
                        break;
                    }
                }
            }

            StringBuilder sb = new StringBuilder();
            if (!conditions.isEmpty()) {
                sb.append("WHERE ");
                sb.append(String.join(" AND ", conditions));
            }
            return sb.toString();
        }

        private static String buildLimitCondition(LimitElement limitElement) {
            return limitElement.toSql();
        }

        public static String buildCacheSelectSQL(StatisticsData statisticsData) {
            try {
                Table table = MetaUtils.getTable(statisticsData.catalogName, statisticsData.databaseName,
                        statisticsData.tableName);

                VelocityContext context = new VelocityContext();
                context.put("columnNames", buildColumnNames(table, statisticsData.columnNames));
                context.put("catalogName", statisticsData.catalogName);
                context.put("databaseName", statisticsData.databaseName);
                context.put("tableName", statisticsData.tableName);
                context.put("whereCondition", buildWhereCondition(table, statisticsData.partitionNames));
                StringWriter sw = new StringWriter();
                DEFAULT_VELOCITY_ENGINE.evaluate(context, sw, "", CACHE_SELECT_TEMPLATE);
                return sw.toString();
            } catch (Exception e) {
                LOG.warn("Failed to generate cache select sql", e);
                return String.format("Generate cache select failed, the reason is %s", e.getMessage());
            }
        }

        private static String buildColumnNames(Table table, ImmutableSet<String> columns) {
            List<String> list = new ArrayList<>(columns.size());
            for (String column : columns) {
                // because group_concat() will truncate column_names, so here we have to validate column is valid
                if (table.getColumn(column) == null) {
                    LOG.warn("Handle column name {} failed, because of truncated", column);
                    break;
                }
                list.add(String.format("`%s`", column));
            }
            return String.join(", ", list);
        }

        private static String buildWhereCondition(Table table, ImmutableSet<String> partitionNames) {
            if (partitionNames.isEmpty()) {
                return "";
            }
            List<Column> partitionColumns = PartitionUtil.getPartitionColumns(table);
            List<String> orPredicates = new ArrayList<>(partitionNames.size());
            for (String partitionName : partitionNames) {
                // because group_concat() will truncate partition_names, so here we have to check it.
                try {
                    List<String> partitionValues = PartitionUtil.toPartitionValues(partitionName);
                    PartitionKey partitionKey =
                            PartitionUtil.createPartitionKey(partitionValues, partitionColumns, table);
                    List<LiteralExpr> keys = partitionKey.getKeys();
                    List<String> predicates = new ArrayList<>(keys.size());
                    for (int i = 0; i < keys.size(); i++) {
                        predicates.add(String.format("`%s` = %s", partitionColumns.get(i).getName(),
                                keys.get(i).getStringValue()));
                    }
                    orPredicates.add(String.join(" AND ", predicates));
                } catch (Exception e) {
                    LOG.warn("Handle partition name {} failed, because of truncated", partitionName);
                    break;
                }
            }
            if (orPredicates.isEmpty()) {
                return "";
            } else {
                return "WHERE " + String.join(" OR ", orPredicates);
            }
        }
    }
}
