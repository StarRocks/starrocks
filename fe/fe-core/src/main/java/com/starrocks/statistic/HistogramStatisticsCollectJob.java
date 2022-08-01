// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.statistic;

import com.google.common.base.Joiner;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.util.DateUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.thrift.TStatisticData;
import org.apache.velocity.VelocityContext;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.statistic.StatsConstants.HISTOGRAM_STATISTICS_TABLE_NAME;

public class HistogramStatisticsCollectJob extends StatisticsCollectJob {
    private static final String COLLECT_HISTOGRAM_STATISTIC_TEMPLATE =
            "SELECT $tableId, '$columnName', $dbId, '$dbName.$tableName'," +
                    " histogram($columnName, $bucketNum, $sampleRatio), " +
                    " $mcv," +
                    " NOW()" +
                    " FROM (SELECT $columnName FROM $dbName.$tableName where rand() <= $sampleRatio" +
                    " and $columnName is not null $topNExclude" +
                    " ORDER BY $columnName LIMIT $totalRows) t";

    private static final String COLLECT_MCV_STATISTIC_TEMPLATE =
            "select cast(version as INT), cast(db_id as BIGINT), cast(table_id as BIGINT), " +
                    "cast(column_key as varchar), cast(column_value as varchar) from (" +
                    "select " + StatsConstants.STATISTIC_HISTOGRAM_VERSION + " as version, " +
                    "$dbId as db_id, " +
                    "$tableId as table_id, " +
                    "`$columnName` as column_key, " +
                    "count(`$columnName`) as column_value " +
                    "from $dbName.$tableName " +
                    "group by `$columnName` " +
                    "order by count(`$columnName`) desc limit $topN ) t";

    public HistogramStatisticsCollectJob(Database db, OlapTable table, List<String> columns,
                                         StatsConstants.AnalyzeType type, StatsConstants.ScheduleType scheduleType,
                                         Map<String, String> properties) {
        super(db, table, columns, type, scheduleType, properties);
    }

    @Override
    public void collect() throws Exception {
        ConnectContext context = StatisticUtils.buildConnectContext();
        context.getSessionVariable().setNewPlanerAggStage(1);

        double sampleRatio = Double.parseDouble(properties.get(StatsConstants.HISTOGRAM_SAMPLE_RATIO));
        long bucketNum = Long.parseLong(properties.get(StatsConstants.HISTOGRAM_BUCKET_NUM));
        long topN = Long.parseLong(properties.get(StatsConstants.HISTOGRAM_TOPN_SIZE));

        for (String column : columns) {
            String sql = buildCollectMCV(db, table, topN, column);
            StatisticExecutor statisticExecutor = new StatisticExecutor();
            List<TStatisticData> mcv = statisticExecutor.queryTopN(sql);

            Map<String, String> mostCommonValues = new HashMap<>();
            for (TStatisticData tStatisticData : mcv) {
                mostCommonValues.put(tStatisticData.columnName, tStatisticData.histogram);
            }

            sql = buildCollectHistogram(db, table, sampleRatio, bucketNum, mostCommonValues, column);
            collectStatisticSync(sql);
        }
    }

    private String buildCollectMCV(Database database, OlapTable table, Long topN, String columnName) {
        VelocityContext context = new VelocityContext();
        context.put("tableId", table.getId());
        context.put("columnName", columnName);
        context.put("dbId", database.getId());

        context.put("dbName", database.getOriginName());
        context.put("tableName", table.getName());
        context.put("topN", topN);

        return build(context, COLLECT_MCV_STATISTIC_TEMPLATE);
    }

    private String buildCollectHistogram(Database database, OlapTable table, double sampleRatio,
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
        context.put("totalRows", Long.MAX_VALUE);

        Column column = table.getColumn(columnName);

        List<String> mcvList = new ArrayList<>();
        for (Map.Entry<String, String> entry : mostCommonValues.entrySet()) {
            String key;
            if (column.getType().isDate()) {
                key = LocalDate.parse(entry.getKey(), DateUtils.DATE_FORMATTER).format(DateUtils.DATEKEY_FORMATTER);
            } else if (column.getType().isDatetime()) {
                key = LocalDate.parse(entry.getKey(), DateUtils.DATE_TIME_FORMATTER).format(DateUtils.SECOND_FORMATTER);
            } else {
                key = entry.getKey();
            }

            mcvList.add("[\"" + key + "\",\"" + entry.getValue() + "\"]");
        }

        if (mcvList.isEmpty()) {
            context.put("mcv", "NULL");
        } else {
            context.put("mcv", "'[" + Joiner.on(",").join(mcvList) + "]'");
        }

        if (!mostCommonValues.isEmpty()) {
            context.put("topNExclude", " and " + columnName + " not in (" +
                    Joiner.on(",").join(mostCommonValues.keySet()) + ")");
        } else {
            context.put("topNExclude", "");
        }

        builder.append(build(context, COLLECT_HISTOGRAM_STATISTIC_TEMPLATE));
        return builder.toString();
    }
}
