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

package com.starrocks.statistic;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.hive.HiveMetaClient;
import com.starrocks.connector.iceberg.IcebergApiConverter;
import com.starrocks.connector.iceberg.IcebergPartitionUtils;
import com.starrocks.connector.paimon.PaimonMetadata;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.OriginStatement;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.thrift.TStatisticData;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.velocity.VelocityContext;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.statistic.StatsConstants.EXTERNAL_MULTI_COLUMN_STATISTICS_TABLE_NAME;

/**
 * This class implements multi-column statistics collection for external tables.
 */
public class ExternalMultiColumnStatisticsCollectJob extends ExternalFullStatisticsCollectJob {
    private static final Logger LOG = LogManager.getLogger(ExternalMultiColumnStatisticsCollectJob.class);

    // Template for building multi-column NDV statistics SQL
    private static final String MULTI_COLUMN_NDV_TEMPLATE =
            "SELECT " +
            "  '$tableUUID' AS table_uuid, " +
            "  '$columnGroup' AS column_group, " +
            "  '$catalogName' AS catalog_name, " +
            "  '$dbName' AS db_name, " +
            "  '$tableName' AS table_name, " +
            "  '$columnNamesStr' AS column_names, " +
            "  CAST(ndv($combinedColumnKey) AS BIGINT) AS ndv, " +
            "  '$updateTime' AS update_time " +
            "FROM `$catalogName`.`$dbName`.`$tableName` " +
            "WHERE $partitionPredicate";

    public ExternalMultiColumnStatisticsCollectJob(String catalogName, Database db, Table table,
                                                  List<String> partitionNames,
                                                  List<String> columnNames, List<Type> columnTypes,
                                                  StatsConstants.AnalyzeType type,
                                                  StatsConstants.ScheduleType scheduleType,
                                                  Map<String, String> properties,
                                                  List<StatsConstants.StatisticsType> statisticsTypes,
                                                  List<List<String>> columnGroups) {
        super(catalogName, db, table, partitionNames, columnNames, columnTypes, type, scheduleType, properties);
        this.statisticsTypes = statisticsTypes;
        this.columnGroups = columnGroups;
    }

    @Override
    public String getName() {
        return "ExternalMultiColumnStats";
    }

    @Override
    public void collect(ConnectContext context, AnalyzeStatus analyzeStatus) throws Exception {
        LOG.info("Start collecting external multi-column statistics for table: {}.{}.{}, column groups: {}",
                getCatalogName(), db.getFullName(), table.getName(), columnGroups);

        int parallelism = Math.max(1, context.getSessionVariable().getStatisticCollectParallelism());
        List<List<String>> collectSQLList = buildMultiColumnCollectSQLList(parallelism);
        long totalCollectSQL = collectSQLList.size();
        long finishedSQLNum = 0;

        // Run SQL queries and process results
        for (List<String> sqlUnion : collectSQLList) {
            if (sqlUnion.size() < parallelism) {
                context.getSessionVariable().setPipelineDop(parallelism / sqlUnion.size());
            } else {
                context.getSessionVariable().setPipelineDop(1);
            }

            String sql = Joiner.on(" UNION ALL ").join(sqlUnion);
            List<TStatisticData> dataList = StatisticExecutor.executeStatisticsSQL(sql, context);

            // Process results and add to sqlBuffer and rowsBuffer
            for (TStatisticData data : dataList) {
                String sqlValue = String.format("('%s', '%s', '%s', '%s', '%s', '%s', %d, '%s')",
                        data.getTableUuid(),
                        data.getColumnGroup(),
                        data.getCatalogName(),
                        data.getDbName(),
                        data.getTableName(),
                        data.getColumnNames(),
                        data.getNdv(),
                        data.getUpdateTime());
                sqlBuffer.add(sqlValue);

                // Create expressions for values relation
                List<Expr> row = Lists.newArrayList();
                row.add(new StringLiteral(data.getTableUuid()));
                row.add(new StringLiteral(data.getColumnGroup()));
                row.add(new StringLiteral(data.getCatalogName()));
                row.add(new StringLiteral(data.getDbName()));
                row.add(new StringLiteral(data.getTableName()));
                row.add(new StringLiteral(data.getColumnNames()));
                row.add(new IntLiteral(data.getNdv(), Type.BIGINT));
                row.add(new StringLiteral(data.getUpdateTime()));
                rowsBuffer.add(row);
            }

            finishedSQLNum++;
            analyzeStatus.setProgress(finishedSQLNum * 100 / totalCollectSQL);
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().replayAddAnalyzeStatus(analyzeStatus);
        }

        // Insert collected data into the statistics table
        if (!sqlBuffer.isEmpty()) {
            flushInsertStatisticsData(context, true);
        }

        LOG.info("Finished collecting external multi-column statistics for table: {}.{}.{}",
                getCatalogName(), db.getFullName(), table.getName());
    }

    protected List<List<String>> buildMultiColumnCollectSQLList(int parallelism) {
        List<String> totalQuerySQL = Lists.newArrayList();
        List<String> partitions = getPartitionNames();

        if (partitions == null || partitions.isEmpty()) {
            // Handle unpartitioned table or all partitions
            for (List<String> columnGroup : columnGroups) {
                if (columnGroup.size() < 2) {
                    continue; // Skip single column groups
                }
                totalQuerySQL.add(buildMultiColumnNDVStatisticsSQL(table, null, columnGroup));
            }
        } else {
            // Handle specific partitions
            for (String partitionName : partitions) {
                for (List<String> columnGroup : columnGroups) {
                    if (columnGroup.size() < 2) {
                        continue; // Skip single column groups
                    }
                    totalQuerySQL.add(buildMultiColumnNDVStatisticsSQL(table, partitionName, columnGroup));
                }
            }
        }

        return Lists.partition(totalQuerySQL, parallelism);
    }

    private String buildMultiColumnNDVStatisticsSQL(Table table, String partitionName, List<String> columnGroup) {
        VelocityContext context = new VelocityContext();

        // Prepare column group string
        String columnGroupStr = StringUtils.join(columnGroup, ",");

        // Prepare column names string with proper quoting
        String columnNamesStr = columnGroup.stream()
                .map(name -> "\"" + name + "\"")
                .collect(Collectors.joining(", "));

        // Build combined column key using murmur_hash for the columns
        List<String> columnCoalesce = columnGroup.stream()
                .map(name -> "coalesce(" + StatisticUtils.quoting(table, name) + ", '')")
                .collect(Collectors.toList());
        String combinedColumnKey = "murmur_hash3_32(" + String.join(", ", columnCoalesce) + ")";

        String tableUUID = StatisticUtils.getUuid(getCatalogName(), db.getFullName(), table.getName());

        // Set context values
        context.put("tableUUID", tableUUID);
        context.put("columnGroup", columnGroupStr);
        context.put("columnNamesStr", columnNamesStr);
        context.put("combinedColumnKey", combinedColumnKey);
        context.put("catalogName", getCatalogName());
        context.put("dbName", db.getOriginName());
        context.put("tableName", table.getName());
        context.put("updateTime", LocalDateTime.now().toString());

        // Set partition predicate
        if (table.isUnPartitioned() || partitionName == null) {
            context.put("partitionPredicate", "1=1");
        } else {
            String nullValue;
            if (table.isIcebergTable()) {
                nullValue = IcebergApiConverter.PARTITION_NULL_VALUE;
            } else if (table.isPaimonTable()) {
                nullValue = PaimonMetadata.PAIMON_PARTITION_NULL_VALUE;
            } else {
                nullValue = HiveMetaClient.PARTITION_NULL_VALUE;
            }

            List<String> partitionColumnNames = table.getPartitionColumnNames();
            List<String> partitionValues = PartitionUtil.toPartitionValues(partitionName);
            List<String> partitionPredicate = Lists.newArrayList();

            for (int i = 0; i < partitionColumnNames.size(); i++) {
                String partitionColumnName = partitionColumnNames.get(i);
                String partitionValue = partitionValues.get(i);

                if (partitionValue.equals(nullValue)) {
                    partitionPredicate.add(StatisticUtils.quoting(partitionColumnName) + " IS NULL");
                } else if (isSupportedPartitionTransform(partitionColumnName)) {
                    partitionPredicate.add(IcebergPartitionUtils.convertPartitionFieldToPredicate(
                            (IcebergTable) table, partitionColumnName, partitionValue));
                } else {
                    partitionPredicate.add(StatisticUtils.quoting(partitionColumnName) +
                            " = '" + partitionValue + "'");
                }
            }

            context.put("partitionPredicate", Joiner.on(" AND ").join(partitionPredicate));
        }

        // Build the SQL
        return build(context, MULTI_COLUMN_NDV_TEMPLATE);
    }

    @Override
    protected StatementBase createInsertStmt() {
        List<String> targetColumnNames = StatisticUtils.buildStatsColumnDef(EXTERNAL_MULTI_COLUMN_STATISTICS_TABLE_NAME).stream()
                .map(ColumnDef::getName)
                .collect(Collectors.toList());

        String sql = "INSERT INTO _statistics_.external_multi_column_statistics(" + String.join(", ", targetColumnNames) +
                ") values " + String.join(", ", sqlBuffer) + ";";
        QueryStatement qs = new QueryStatement(new ValuesRelation(rowsBuffer, targetColumnNames));
        InsertStmt insert = new InsertStmt(new TableName("_statistics_", "external_multi_column_statistics"), qs);
        insert.setTargetColumnNames(targetColumnNames);
        insert.setOrigStmt(new OriginStatement(sql, 0));
        return insert;
    }

    // Helper method to build the SQL using velocity engine
    private String build(VelocityContext context, String template) {
        java.io.StringWriter sw = new java.io.StringWriter();
        DEFAULT_VELOCITY_ENGINE.evaluate(context, sw, "", template);
        return sw.toString();
    }
}
