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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TStatisticData;
import org.apache.velocity.VelocityContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.statistic.StatsConstants.HISTOGRAM_STATISTICS_TABLE_NAME;

public class HistogramStatisticsCollectJob extends StatisticsCollectJob {
    private static final String COLLECT_HISTOGRAM_STATISTIC_TEMPLATE =
            "SELECT $tableId, '$columnName', $dbId, '$dbName.$tableName'," +
                    " histogram(`$columnName`, cast($bucketNum as int), cast($sampleRatio as double)), " +
                    " $mcv," +
                    " NOW()" +
                    " FROM (SELECT `$columnName` FROM `$dbName`.`$tableName` where rand() <= $sampleRatio" +
                    " and `$columnName` is not null $MCVExclude" +
                    " ORDER BY `$columnName` LIMIT $totalRows) t";

    private static final String COLLECT_MCV_STATISTIC_TEMPLATE =
            "select cast(version as INT), cast(db_id as BIGINT), cast(table_id as BIGINT), " +
                    "cast(column_key as varchar), cast(column_value as varchar) from (" +
                    "select " + StatsConstants.STATISTIC_HISTOGRAM_VERSION + " as version, " +
                    "$dbId as db_id, " +
                    "$tableId as table_id, " +
                    "`$columnName` as column_key, " +
                    "count(`$columnName`) as column_value " +
                    "from `$dbName`.`$tableName` where `$columnName` is not null " +
                    "group by `$columnName` " +
                    "order by count(`$columnName`) desc limit $topN ) t";

    public HistogramStatisticsCollectJob(Database db, Table table, List<String> columns,
                                         StatsConstants.AnalyzeType type, StatsConstants.ScheduleType scheduleType,
                                         Map<String, String> properties) {
        super(db, table, columns, type, scheduleType, properties);
    }

    @Override
    public void collect(ConnectContext context, AnalyzeStatus analyzeStatus) throws Exception {
        context.getSessionVariable().setNewPlanerAggStage(1);

        double sampleRatio = Double.parseDouble(properties.get(StatsConstants.HISTOGRAM_SAMPLE_RATIO));
        long bucketNum = Long.parseLong(properties.get(StatsConstants.HISTOGRAM_BUCKET_NUM));
        long mcvSize = Long.parseLong(properties.get(StatsConstants.HISTOGRAM_MCV_SIZE));

        long finishedSQLNum = 0;
        long totalCollectSQL = columns.size();

        for (String column : columns) {
            String sql = buildCollectMCV(db, table, mcvSize, column);
            StatisticExecutor statisticExecutor = new StatisticExecutor();
            List<TStatisticData> mcv = statisticExecutor.queryMCV(context, sql);

            Map<String, String> mostCommonValues = new HashMap<>();
            for (TStatisticData tStatisticData : mcv) {
                mostCommonValues.put(tStatisticData.columnName, tStatisticData.histogram);
            }

            sql = buildCollectHistogram(db, table, sampleRatio, bucketNum, mostCommonValues, column);
            collectStatisticSync(sql, context);

            finishedSQLNum++;
            analyzeStatus.setProgress(finishedSQLNum * 100 / totalCollectSQL);
            GlobalStateMgr.getCurrentAnalyzeMgr().addAnalyzeStatus(analyzeStatus);
        }
    }

    private String buildCollectMCV(Database database, Table table, Long topN, String columnName) {
        VelocityContext context = new VelocityContext();
        context.put("tableId", table.getId());
        context.put("columnName", columnName);
        context.put("dbId", database.getId());

        context.put("dbName", database.getOriginName());
        context.put("tableName", table.getName());
        context.put("topN", topN);

        return build(context, COLLECT_MCV_STATISTIC_TEMPLATE);
    }

    private String buildCollectHistogram(Database database, Table table, double sampleRatio,
                                         Long bucketNum, Map<String, String> mostCommonValues, String columnName) {
        StringBuilder builder = new StringBuilder("INSERT INTO ").append(HISTOGRAM_STATISTICS_TABLE_NAME).append(" ");

        VelocityContext context = new VelocityContext();
        context.put("tableId", table.getId());
        context.put("columnName", columnName);
        context.put("dbId", database.getId());
        context.put("dbName", database.getOriginName());
        context.put("tableName", table.getName());

        context.put("bucketNum", bucketNum);
        context.put("sampleRatio", sampleRatio);
        context.put("totalRows", Config.histogram_max_sample_row_count);

        Column column = table.getColumn(columnName);

        List<String> mcvList = new ArrayList<>();
        for (Map.Entry<String, String> entry : mostCommonValues.entrySet()) {
            mcvList.add("[\"" + entry.getKey() + "\",\"" + entry.getValue() + "\"]");
        }

        if (mostCommonValues.isEmpty()) {
            context.put("mcv", "NULL");
        } else {
            context.put("mcv", "'[" + Joiner.on(",").join(mcvList) + "]'");
        }

        if (!mostCommonValues.isEmpty()) {
            if (column.getType().getPrimitiveType().isDateType() || column.getType().getPrimitiveType().isCharFamily()) {
                context.put("MCVExclude", " and " + StatisticUtils.quoting(columnName) + " not in (\"" +
                        Joiner.on("\",\"").join(mostCommonValues.keySet()) + "\")");
            } else {
                context.put("MCVExclude", " and " + StatisticUtils.quoting(columnName) + " not in (" +
                        Joiner.on(",").join(mostCommonValues.keySet()) + ")");
            }
        } else {
            context.put("MCVExclude", "");
        }

        builder.append(build(context, COLLECT_HISTOGRAM_STATISTIC_TEMPLATE));
        return builder.toString();
    }
}
