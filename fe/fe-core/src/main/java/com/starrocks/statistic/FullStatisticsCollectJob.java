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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TStatisticData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.velocity.VelocityContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FullStatisticsCollectJob extends StatisticsCollectJob {
    private static final Logger LOG = LogManager.getLogger(FullStatisticsCollectJob.class);

    private static final String COLLECT_FULL_STATISTIC_TEMPLATE =
            "SELECT $tableId, $partitionId, '$columnName', $dbId," +
                    " '$dbName.$tableName', '$partitionName'," +
                    " COUNT(1), $dataSize, $countDistinctFunction, $countNullFunction, $maxFunction, $minFunction, NOW() "
                    + "FROM `$dbName`.`$tableName` partition `$partitionName`";

    private static final String BATCH_FULL_STATISTIC_TEMPLATE = "SELECT cast($version as INT)" +
            ", cast($partitionId as BIGINT)" + // BIGINT
            ", '$columnName'" + // VARCHAR
            ", cast(COUNT(1) as BIGINT)" + // BIGINT
            ", cast($dataSize as BIGINT)" + // BIGINT
            ", $hllFunction" + // VARCHAR
            ", cast($countNullFunction as BIGINT)" + // BIGINT
            ", $maxFunction" + // VARCHAR
            ", $minFunction " + // VARCHAR
            " FROM `$dbName`.`$tableName` partition `$partitionName`";

    private static final String BATCH_INSERT_INTO_STATISTICS_TEMPLATE = "INSERT INTO column_statistics values " +
            "($tableId, $partitionId, '$columnName', $dbId, '$dbName.$tableName', '$partitionName'," +
            " $count, $dataSize, hll_deserialize('$hll'), $countNull," +
            " $maxFunction, $minFunction, NOW())";

    private final List<Long> partitionIdList;

    private final StringBuilder buffer = new StringBuilder();

    public FullStatisticsCollectJob(Database db, Table table, List<Long> partitionIdList, List<String> columns,
                                    StatsConstants.AnalyzeType type, StatsConstants.ScheduleType scheduleType,
                                    Map<String, String> properties) {
        super(db, table, columns, type, scheduleType, properties);
        this.partitionIdList = partitionIdList;
    }

    @Override
    public void collect(ConnectContext context, AnalyzeStatus analyzeStatus) throws Exception {
        long finishedSQLNum = 0;
        int parallelism = Math.max(1, context.getSessionVariable().getStatisticCollectParallelism());
        List<List<String>> collectSQLList = buildCollectSQLList(parallelism);
        long totalCollectSQL = collectSQLList.size();

        // First, the collection task is divided into several small tasks according to the column name and partition,
        // and then the multiple small tasks are aggregated into several tasks
        // that will actually be run according to the configured parallelism, and are connected by union all
        // Because each union will run independently, if the number of unions is greater than the degree of parallelism,
        // dop will be set to 1 to meet the requirements of the degree of parallelism.
        // If the number of unions is less than the degree of parallelism,
        // dop should be adjusted appropriately to use enough cpu cores
        for (List<String> sqlUnion : collectSQLList) {
            if (sqlUnion.size() < parallelism) {
                context.getSessionVariable().setPipelineDop(parallelism / sqlUnion.size());
            } else {
                context.getSessionVariable().setPipelineDop(1);
            }

            String sql = Joiner.on(" UNION ALL ").join(sqlUnion);

            collectStatisticsData(sql, context);
            finishedSQLNum++;
            analyzeStatus.setProgress(finishedSQLNum * 100 / totalCollectSQL);
            GlobalStateMgr.getCurrentAnalyzeMgr().addAnalyzeStatus(analyzeStatus);
        }

        syncInsertStatisticsData(context, true);
    }

    // INSERT INTO column_statistics values
    // ($tableId, $partitionId, '$columnName', $dbId, '$dbName.$tableName', '$partitionName',
    //  $count, $dataSize, hll_deserialize('$hll'), $countNull, $maxFunction, $minFunction, NOW());
    private void collectStatisticsData(String sql, ConnectContext context) throws Exception {
        LOG.debug("statistics collect sql : " + sql);
        StatisticExecutor executor = new StatisticExecutor();
        List<TStatisticData> dataList = executor.executeStatisticDQL(context, sql);

        List<String> params = Lists.newArrayList();
        for (TStatisticData data : dataList) {
            params.add(String.valueOf(table.getId()));
            params.add(String.valueOf(data.getPartitionId()));
            params.add("'" + data.getColumnName() + "'");
            params.add(String.valueOf(db.getId()));
            params.add("'" + db.getOriginName() + "." + table.getName() + "'");
            params.add(String.valueOf(data.getRowCount()));
            params.add(String.valueOf(data.getDataSize()));
            params.add("hll_deserialize('" + data.getHll() + "'");
            params.add(String.valueOf(data.getNullCount()));
            params.add(data.getMax());
            params.add(data.getMin());
            params.add("now()");
        }
        if (buffer.length() > 1) {
            buffer.append(", ");
        }
        buffer.append("(").append(String.join(", ", params)).append(")");

        syncInsertStatisticsData(context, false);
    }

    private void syncInsertStatisticsData(ConnectContext context, boolean force) throws Exception {
        if (buffer.length() < Config.statistics_full_collect_buffer && !force) {
            return;
        }

        String sql = "INSERT INTO column_statistics values " + buffer;
        collectStatisticSync(sql, context);
    }

    /*
     * Split tasks at the partition and column levels,
     * and the number of rows to scan is the number of rows in the partition
     * where the column is located.
     * The number of rows is accumulated in turn until the maximum number of rows is accumulated.
     * Use UNION ALL connection between multiple tasks and collect them in one query
     */
    protected List<List<String>> buildCollectSQLList(int parallelism) {
        List<String> totalQuerySQL = new ArrayList<>();
        for (Long partitionId : partitionIdList) {
            Partition partition = table.getPartition(partitionId);
            for (String columnName : columns) {
                totalQuerySQL.add(buildBatchCollectFullStatisticSQL(table, partition, columnName));
            }
        }

        return Lists.partition(totalQuerySQL, parallelism);
    }

    private String getDataSize(Column column) {
        if (column.getPrimitiveType().isCharFamily()) {
            return "IFNULL(SUM(CHAR_LENGTH(" + StatisticUtils.quoting(column.getName()) + ")), 0)";
        }
        long typeSize = column.getType().getTypeSize();
        return "COUNT(1) * " + typeSize;
    }

    private String buildBatchCollectFullStatisticSQL(Table table, Partition partition, String columnName) {
        StringBuilder builder = new StringBuilder();
        VelocityContext context = new VelocityContext();
        Column column = table.getColumn(columnName);

        context.put("version", StatsConstants.STATISTIC_BATCH_VERSION);
        context.put("partitionId", partition.getId());
        context.put("columnName", columnName);
        context.put("dataSize", getDataSize(column));

        if (!column.getType().canStatistic()) {
            context.put("hllFunction", "hll_serialize(hll_empty())");
            context.put("countNullFunction", "0");
            context.put("maxFunction", "''");
            context.put("minFunction", "''");
        } else {
            context.put("hllFunction", "hll_serialize(IFNULL(hll_raw(`" + columnName + "`), hll_empty()))");
            context.put("countNullFunction", "COUNT(1) - COUNT(`" + columnName + "`)");
            context.put("maxFunction", getMinMaxFunction(column, StatisticUtils.quoting(columnName), true));
            context.put("minFunction", getMinMaxFunction(column, StatisticUtils.quoting(columnName), false));
        }

        builder.append(build(context, BATCH_FULL_STATISTIC_TEMPLATE));
        return builder.toString();
    }

    // private String buildCollectFullStatisticSQL(Database database, Table table, Partition partition,
    //                                             String columnName) {
    //     StringBuilder builder = new StringBuilder();
    //     VelocityContext context = new VelocityContext();
    //     Column column = table.getColumn(columnName);
    //
    //     context.put("dbId", database.getId());
    //     context.put("tableId", table.getId());
    //     context.put("partitionId", partition.getId());
    //     context.put("columnName", columnName);
    //     context.put("dbName", database.getOriginName());
    //     context.put("tableName", table.getName());
    //     context.put("partitionName", partition.getName());
    //     context.put("dataSize", getDataSize(column));
    //
    //     if (!column.getType().canStatistic()) {
    //         context.put("countDistinctFunction", "hll_empty()");
    //         context.put("countNullFunction", "0");
    //         context.put("maxFunction", "''");
    //         context.put("minFunction", "''");
    //     } else {
    //         context.put("countDistinctFunction", "IFNULL(hll_raw(`" + columnName + "`), hll_empty())");
    //         context.put("countNullFunction", "COUNT(1) - COUNT(`" + columnName + "`)");
    //         context.put("maxFunction", getMinMaxFunction(column, StatisticUtils.quoting(columnName), true));
    //         context.put("minFunction", getMinMaxFunction(column, StatisticUtils.quoting(columnName), false));
    //     }
    //
    //     builder.append(build(context, COLLECT_FULL_STATISTIC_TEMPLATE));
    //     return builder.toString();
    // }
}
