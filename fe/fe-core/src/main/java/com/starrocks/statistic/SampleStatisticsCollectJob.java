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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.MetaUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.velocity.VelocityContext;

import java.io.StringWriter;
import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class SampleStatisticsCollectJob extends StatisticsCollectJob {

    private static final String INSERT_SELECT_WITH_TEMPLATE =
            "WITH base_cte_table as (SELECT $columnNames from `$dbName`.`$tableName` $hints) ";

    private static final String INSERT_SELECT_METRIC_SAMPLE_TEMPLATE =
            "SELECT $tableId, '$columnNameStr', $dbId, '$dbNameStr.$tableNameStr', '$dbNameStr', COUNT(1) * $ratio, "
                    + "$dataSize * $ratio, 0, 0, '', '', NOW() "
                    + "FROM (SELECT 1 as column_key FROM `base_cte_table` ) as t ";

    private static final String INSERT_SELECT_TYPE_SAMPLE_TEMPLATE =
            "SELECT $tableId, '$columnNameStr', $dbId, '$dbNameStr.$tableNameStr', '$dbNameStr', "
                    + "       IFNULL(SUM(t1.count), 0) * $ratio, "
                    + "       $dataSize * $ratio, $countDistinctFunction, "
                    + "       IFNULL(SUM(IF(t1.`column_key` IS NULL, t1.count, 0)), 0) * $ratio, "
                    + "       $maxFunction, $minFunction, NOW() "
                    + "FROM ( "
                    + "    SELECT t0.`column_key`, COUNT(1) as count "
                    + "    FROM (SELECT $columnName as column_key FROM `base_cte_table`) as t0 "
                    + "    GROUP BY t0.column_key "
                    + ") as t1 ";

    protected static final String INSERT_STATISTIC_TEMPLATE =
            "INSERT INTO " + StatsConstants.SAMPLE_STATISTICS_TABLE_NAME;

    public SampleStatisticsCollectJob(Database db, Table table, List<String> columnNames,
                                      StatsConstants.AnalyzeType type, StatsConstants.ScheduleType scheduleType,
                                      Map<String, String> properties) {
        super(db, table, columnNames, type, scheduleType, properties);
    }

    public SampleStatisticsCollectJob(Database db, Table table, List<String> columnNames, List<Type> columnTypes,
                                      StatsConstants.AnalyzeType type, StatsConstants.ScheduleType scheduleType,
                                      Map<String, String> properties) {
        super(db, table, columnNames, columnTypes, type, scheduleType, properties);
    }

    protected int splitColumns(long rowCount) {
        long splitSize;
        if (rowCount == 0) {
            splitSize = columnNames.size();
        } else {
            splitSize = Config.statistic_collect_max_row_count_per_query / rowCount + 1;
            if (splitSize > columnNames.size()) {
                splitSize = columnNames.size();
            }
        }
        // Supports a maximum of 64 tasks for a union,
        // preventing unexpected situations caused by too many tasks being executed at one time
        return (int) Math.min(64, splitSize);
    }

    @Override
    public void collect(ConnectContext context, AnalyzeStatus analyzeStatus) throws Exception {
        long sampleRowCount = Long.parseLong(properties.getOrDefault(StatsConstants.STATISTIC_SAMPLE_COLLECT_ROWS,
                String.valueOf(Config.statistic_sample_collect_rows)));

        int splitSize = splitColumns(sampleRowCount);
        List<List<String>> collectSQLList = Lists.partition(columnNames, splitSize);
        List<List<Type>> collectTypeList = Lists.partition(columnTypes, splitSize);
        long finishedSQLNum = 0;
        long totalCollectSQL = collectSQLList.size();

        for (int i = 0; i < collectSQLList.size(); i++) {
            String sql = buildSampleInsertSQL(db.getId(), table.getId(), collectSQLList.get(i), collectTypeList.get(i),
                    sampleRowCount);
            collectStatisticSync(sql, context);

            finishedSQLNum++;
            analyzeStatus.setProgress(finishedSQLNum * 100 / totalCollectSQL);
            GlobalStateMgr.getCurrentAnalyzeMgr().addAnalyzeStatus(analyzeStatus);
        }
    }

    private String getDataSize(Type columnType) {
        if (columnType.getPrimitiveType().isCharFamily()) {
            return "IFNULL(SUM(CHAR_LENGTH(`column_key`)), 0)";
        }

        long typeSize = columnType.getTypeSize();

        if (columnType.canStatistic()) {
            return "IFNULL(SUM(t1.count), 0) * " + typeSize;
        }
        return "COUNT(1) * " + typeSize;
    }

    protected String buildSampleInsertSQL(Long dbId, Long tableId, List<String> columnNames, List<Type> columnTypes,
                                          long rows) {
        Table table = MetaUtils.getTable(dbId, tableId);

        long hitRows = 1;
        long totalRows = 0;
        long totalTablet = 0;
        Set<String> randomTablets = Sets.newHashSet();
        rows = Math.max(rows, 1);

        // calculate the number of tablets by each partition
        // simpleTabletNums = simpleRows / partitionNums / (actualPartitionRows / actualTabletNums)
        long avgRowsPerPartition = rows / Math.max(table.getPartitions().size(), 1);

        for (Partition p : table.getPartitions()) {
            List<Long> ids = p.getBaseIndex().getTabletIdsInOrder();

            if (ids.isEmpty()) {
                continue;
            }

            if (p.getBaseIndex().getRowCount() < (avgRowsPerPartition / 2)) {
                continue;
            }

            long avgRowsPerTablet = Math.max(p.getBaseIndex().getRowCount() / ids.size(), 1);
            long tabletCounts = Math.max(avgRowsPerPartition / avgRowsPerTablet, 1);
            tabletCounts = Math.min(tabletCounts, ids.size());

            for (int i = 0; i < tabletCounts; i++) {
                randomTablets.add(String.valueOf(ids.get(i)));
            }

            hitRows += avgRowsPerTablet * tabletCounts;
            totalRows += p.getBaseIndex().getRowCount();
            totalTablet += ids.size();
        }

        long ratio = Math.max(totalRows / Math.min(hitRows, rows), 1);
        // all hit, direct full
        String hintTablets;
        if (randomTablets.size() == totalTablet) {
            hintTablets = " LIMIT " + rows;
        } else {
            hintTablets = " Tablet(" + String.join(", ", randomTablets) + ")" + " LIMIT " + rows;
        }

        StringBuilder builder = new StringBuilder(INSERT_STATISTIC_TEMPLATE).append(" ");

        Set<String> lowerDistributeColumns =
                table.getDistributionColumnNames().stream().map(String::toLowerCase).collect(Collectors.toSet());

        {
            VelocityContext cteContext = new VelocityContext();
            cteContext.put("dbName", db.getFullName());
            cteContext.put("tableName", table.getName());
            cteContext.put("hints", hintTablets);
            List<String> collectNames = Lists.newArrayList();
            for (int i = 0; i < columnNames.size(); i++) {
                Type columnType = columnTypes.get(i);
                if (columnType.canStatistic()) {
                    collectNames.add(StatisticUtils.quoting(columnNames.get(i)));
                }
            }
            if (collectNames.isEmpty()) {
                collectNames.add(StatisticUtils.quoting(columnNames.get(0)));
            }
            cteContext.put("columnNames", String.join(", ", collectNames));
            StringWriter sw = new StringWriter();
            DEFAULT_VELOCITY_ENGINE.evaluate(cteContext, sw, "", INSERT_SELECT_WITH_TEMPLATE);
            builder.append(sw).append(" ");
        }

        for (int i = 0; i < columnNames.size(); i++) {
            VelocityContext context = new VelocityContext();
            String quoteColumnName =  StatisticUtils.quoting(columnNames.get(i));
            String columnNameStr = StringEscapeUtils.escapeSql(columnNames.get(i));
            Type columnType = columnTypes.get(i);

            context.put("dbId", dbId);
            context.put("tableId", tableId);
            context.put("columnName", quoteColumnName);
            context.put("columnNameStr", columnNameStr);
            context.put("dbName", db.getFullName());
            context.put("dbNameStr", StringEscapeUtils.escapeSql(db.getFullName()));
            context.put("tableName", table.getName());
            context.put("tableNameStr", StringEscapeUtils.escapeSql(table.getName()));
            context.put("dataSize", getDataSize(columnType));
            context.put("ratio", ratio);
            context.put("hints", hintTablets);
            context.put("maxFunction", getMinMaxFunction(columnType, "t1.`column_key`", true));
            context.put("minFunction", getMinMaxFunction(columnType, "t1.`column_key`", false));

            // countDistinctFunction
            if (lowerDistributeColumns.size() == 1 && lowerDistributeColumns.contains(quoteColumnName.toLowerCase())) {
                context.put("countDistinctFunction", "COUNT(1) * " + ratio);
            } else {
                // From PostgreSQL: n*d / (n - f1 + f1*n/N)
                // (https://github.com/postgres/postgres/blob/master/src/backend/commands/analyze.c)
                // and paper: ESTIMATING THE NUMBER OF CLASSES IN A FINITE POPULATION
                // (http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.93.8637&rep=rep1&type=pdf)
                // sample_row * count_distinct / ( sample_row - once_count + once_count * sample_row / total_row)
                String sampleRows = "SUM(t1.count)";
                String onceCount = "SUM(IF(t1.count = 1, 1, 0))";
                String countDistinct = "COUNT(1)";

                String fn = MessageFormat.format("{0} * {1} / ({0} - {2} + {2} * {0} / {3})", sampleRows,
                        countDistinct, onceCount, String.valueOf(totalRows));
                context.put("countDistinctFunction", "IFNULL(" + fn + ", COUNT(1))");
            }

            StringWriter sw = new StringWriter();

            if (!columnType.canStatistic()) {
                DEFAULT_VELOCITY_ENGINE.evaluate(context, sw, "", INSERT_SELECT_METRIC_SAMPLE_TEMPLATE);
            } else {
                DEFAULT_VELOCITY_ENGINE.evaluate(context, sw, "", INSERT_SELECT_TYPE_SAMPLE_TEMPLATE);
            }

            builder.append(sw);
            builder.append(" UNION ALL ");
        }

        return builder.substring(0, builder.length() - "UNION ALL ".length());
    }
}
