// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.statistic;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import org.apache.velocity.VelocityContext;

import java.io.StringWriter;
import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class SampleStatisticsCollectJob extends StatisticsCollectJob {

    private static final String INSERT_SELECT_METRIC_SAMPLE_TEMPLATE =
            "SELECT $tableId, '$columnName', $dbId, '$tableName', '$dbName', COUNT(1) * $ratio, "
                    + "$dataSize * $ratio, 0, 0, '', '', NOW() "
                    + "FROM (SELECT `$columnName` FROM $tableName $hints ) as t";

    private static final String INSERT_SELECT_TYPE_SAMPLE_TEMPLATE =
            "SELECT $tableId, '$columnName', $dbId, '$tableName', '$dbName', IFNULL(SUM(t1.count), 0) * $ratio, "
                    + "       $dataSize * $ratio, $countDistinctFunction, "
                    + "       IFNULL(SUM(IF(t1.`$columnName` IS NULL, t1.count, 0)), 0) * $ratio, "
                    + "       IFNULL(MAX(t1.`$columnName`), ''), IFNULL(MIN(t1.`$columnName`), ''), NOW() "
                    + "FROM ( "
                    + "    SELECT t0.`$columnName`, COUNT(1) as count "
                    + "    FROM (SELECT `$columnName` FROM $tableName $hints) as t0 "
                    + "    GROUP BY t0.`$columnName` "
                    + ") as t1";

    protected static final String INSERT_STATISTIC_TEMPLATE =
            "INSERT INTO " + StatsConstants.SAMPLE_STATISTICS_TABLE_NAME;

    public SampleStatisticsCollectJob(Database db, Table table, List<String> columns,
                                      StatsConstants.AnalyzeType type, StatsConstants.ScheduleType scheduleType,
                                      Map<String, String> properties) {
        super(db, table, columns, type, scheduleType, properties);
    }

    @Override
    public void collect(ConnectContext context, AnalyzeStatus analyzeStatus) throws Exception {
        long sampleRowCount = Long.parseLong(properties.getOrDefault(StatsConstants.STATISTIC_SAMPLE_COLLECT_ROWS,
                String.valueOf(Config.statistic_sample_collect_rows)));

        List<List<String>> collectSQLList = Lists.partition(columns, splitColumns(sampleRowCount));
        long finishedSQLNum = 0;
        long totalCollectSQL = collectSQLList.size();

        for (List<String> splitColItem : collectSQLList) {
            String sql = buildSampleInsertSQL(db.getId(), table.getId(), splitColItem, sampleRowCount);
            collectStatisticSync(sql, context);

            finishedSQLNum++;
            analyzeStatus.setProgress(finishedSQLNum * 100 / totalCollectSQL);
            GlobalStateMgr.getCurrentAnalyzeMgr().addAnalyzeStatus(analyzeStatus);
        }
    }

    private String buildSampleInsertSQL(Long dbId, Long tableId, List<String> columnNames, long rows) {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        Table table = db.getTable(tableId);

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

        for (String name : columnNames) {
            VelocityContext context = new VelocityContext();
            Column column = table.getColumn(name);

            context.put("dbId", dbId);
            context.put("tableId", tableId);
            context.put("columnName", name);
            context.put("dbName", db.getFullName());
            context.put("tableName", db.getOriginName() + "." + table.getName());
            context.put("dataSize", getDataSize(column, true));
            context.put("ratio", ratio);
            context.put("hints", hintTablets);

            // countDistinctFunction
            if (lowerDistributeColumns.size() == 1 && lowerDistributeColumns.contains(name.toLowerCase())) {
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

            if (!column.getType().canStatistic()) {
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
