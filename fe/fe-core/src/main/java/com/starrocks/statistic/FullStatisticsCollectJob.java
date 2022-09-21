// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.statistic;

import com.google.common.base.Joiner;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import org.apache.velocity.VelocityContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FullStatisticsCollectJob extends StatisticsCollectJob {

    private static final String COLLECT_FULL_STATISTIC_TEMPLATE =
            "SELECT $tableId, $partitionId, '$columnName', $dbId," +
                    " '$dbName.$tableName', '$partitionName'," +
                    " COUNT(1), $dataSize, $countDistinctFunction, $countNullFunction, $maxFunction, $minFunction, NOW() "
                    + "FROM $dbName.$tableName partition $partitionName";

    private final List<Long> partitionIdList;

    public FullStatisticsCollectJob(Database db, OlapTable table, List<Long> partitionIdList, List<String> columns,
                                    StatsConstants.AnalyzeType type, StatsConstants.ScheduleType scheduleType,
                                    Map<String, String> properties) {
        super(db, table, columns, type, scheduleType, properties);
        this.partitionIdList = partitionIdList;
    }

    public List<Long> getPartitionIdList() {
        return partitionIdList;
    }

    @Override
    public void collect(ConnectContext context, AnalyzeStatus analyzeStatus) throws Exception {
        List<String> collectSQLList = buildCollectSQLList();
        long finishedSQLNum = 0;
        long totalCollectSQL = collectSQLList.size();

        for (String sql : collectSQLList) {
            collectStatisticSync(sql, context);
            finishedSQLNum++;
            analyzeStatus.setProgress(finishedSQLNum * 100 / totalCollectSQL);
            GlobalStateMgr.getCurrentAnalyzeMgr().addAnalyzeStatus(analyzeStatus);
        }
    }

    /*
     * Split tasks at the partition and column levels,
     * and the number of rows to scan is the number of rows in the partition
     * where the column is located.
     * The number of rows is accumulated in turn until the maximum number of rows is accumulated.
     * Use UNION ALL connection between multiple tasks and collect them in one query
     */
    public List<String> buildCollectSQLList() {
        List<String> collectSQLList = new ArrayList<>();
        List<String> sqlInUnion = new ArrayList<>();
        long scanRowCount = 0;
        for (Long partitionId : partitionIdList) {
            Partition partition = table.getPartition(partitionId);
            for (String columnName : columns) {
                sqlInUnion.add(buildCollectFullStatisticSQL(db, table, partition, columnName));
                scanRowCount += partition.getRowCount();
                if (scanRowCount >= Config.statistic_collect_max_row_count_per_query
                        /*
                         * Because the previous code has filtered empty partitions, the partition row count of 0 here means.
                         * 1. The load has just been completed, and the number of lines has not been collected.
                         * 2. Just restarted, the number of lines has not been collected
                         *
                         * In order to avoid that a single query is too large because the number of rows is 0,
                         * we no longer accumulate unions for this special case.
                         */
                        || partition.getRowCount() == 0) {
                    collectSQLList.add("INSERT INTO column_statistics " + Joiner.on(" UNION ALL ").join(sqlInUnion));
                    scanRowCount = 0;
                    sqlInUnion.clear();
                }
            }
        }

        if (!sqlInUnion.isEmpty()) {
            collectSQLList.add("INSERT INTO column_statistics " + Joiner.on(" UNION ALL ").join(sqlInUnion));
        }

        return collectSQLList;
    }

    private String buildCollectFullStatisticSQL(Database database, Table table, Partition partition, String columnNames) {
        StringBuilder builder = new StringBuilder();
        VelocityContext context = new VelocityContext();
        Column column = table.getColumn(columnNames);

        context.put("dbId", database.getId());
        context.put("tableId", table.getId());
        context.put("partitionId", partition.getId());
        context.put("columnName", columnNames);
        context.put("dbName", database.getOriginName());
        context.put("tableName", table.getName());
        context.put("partitionName", partition.getName());
        context.put("dataSize", getDataSize(column, false));

        if (!column.getType().canStatistic()) {
            context.put("countDistinctFunction", "hll_empty()");
            context.put("countNullFunction", "0");
            context.put("maxFunction", "''");
            context.put("minFunction", "''");
        } else {
            context.put("countDistinctFunction", "IFNULL(hll_raw(`" + columnNames + "`), hll_empty())");
            context.put("countNullFunction", "COUNT(1) - COUNT(`" + columnNames + "`)");
            context.put("maxFunction", "IFNULL(MAX(`" + columnNames + "`), '')");
            context.put("minFunction", "IFNULL(MIN(`" + columnNames + "`), '')");
        }

        builder.append(build(context, COLLECT_FULL_STATISTIC_TEMPLATE));
        return builder.toString();
    }
}
