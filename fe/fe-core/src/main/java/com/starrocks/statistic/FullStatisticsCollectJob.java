// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.statistic;

import com.google.common.base.Joiner;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
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

    public FullStatisticsCollectJob(Database db, Table table, List<Long> partitionIdList, List<String> columns,
                                    StatsConstants.AnalyzeType type, StatsConstants.ScheduleType scheduleType,
                                    Map<String, String> properties) {
        super(db, table, columns, type, scheduleType, properties);
        this.partitionIdList = partitionIdList;
    }

    public List<Long> getPartitionIdList() {
        return partitionIdList;
    }

    @Override
    public void collect(ConnectContext context) throws Exception {
        for (String sql : buildCollectSQLList()) {
            collectStatisticSync(sql, context);
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
                if (scanRowCount >= Config.statistic_collect_max_row_count_per_query) {
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
            context.put("countDistinctFunction", "IFNULL(hll_union(hll_hash(`" + columnNames + "`)), hll_empty())");
            context.put("countNullFunction", "COUNT(1) - COUNT(`" + columnNames + "`)");
            context.put("maxFunction", "IFNULL(MAX(`" + columnNames + "`), '')");
            context.put("minFunction", "IFNULL(MIN(`" + columnNames + "`), '')");
        }

        builder.append(build(context, COLLECT_FULL_STATISTIC_TEMPLATE));
        return builder.toString();
    }
}
