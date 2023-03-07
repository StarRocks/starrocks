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
                    + "FROM `$dbName`.`$tableName` partition `$partitionName`";

    private final List<Long> partitionIdList;

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

            String sql = "INSERT INTO column_statistics " + Joiner.on(" UNION ALL ").join(sqlUnion);
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
    public List<List<String>> buildCollectSQLList(int parallelism) {
        List<String> totalQuerySQL = new ArrayList<>();
        for (Long partitionId : partitionIdList) {
            Partition partition = table.getPartition(partitionId);
            for (String columnName : columns) {
                totalQuerySQL.add(buildCollectFullStatisticSQL(db, table, partition, columnName));
            }
        }

        return Lists.partition(totalQuerySQL, parallelism);
    }

    private String buildCollectFullStatisticSQL(Database database, Table table, Partition partition, String columnName) {
        StringBuilder builder = new StringBuilder();
        VelocityContext context = new VelocityContext();
        Column column = table.getColumn(columnName);

        context.put("dbId", database.getId());
        context.put("tableId", table.getId());
        context.put("partitionId", partition.getId());
        context.put("columnName", columnName);
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
            context.put("countDistinctFunction", "IFNULL(hll_raw(`" + columnName + "`), hll_empty())");
            context.put("countNullFunction", "COUNT(1) - COUNT(`" + columnName + "`)");
            context.put("maxFunction", "IFNULL(MAX(`" + columnName + "`), '')");
            context.put("minFunction", "IFNULL(MIN(`" + columnName + "`), '')");
        }

        builder.append(build(context, COLLECT_FULL_STATISTIC_TEMPLATE));
        return builder.toString();
    }
}
