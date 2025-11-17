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
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.thrift.TStatisticData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.velocity.VelocityContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.statistic.StatsConstants.HISTOGRAM_STATISTICS_TABLE_NAME;

public class HistogramStatisticsCollectJob extends StatisticsCollectJob {
    private static final Logger LOG = LogManager.getLogger(HistogramStatisticsCollectJob.class);

    private static final String HISTOGRAM_FUNCTION_WITHOUT_NDV_TEMPLATE =
            "histogram(`column_key`, cast($bucketNum as int), cast($sampleRatio as double))";

    private static final String HISTOGRAM_FUNCTION_WITH_NDV_TEMPLATE =
            "histogram(`column_key`, cast($bucketNum as int), cast($sampleRatio as double), '$ndvEstimator')";

    private static final String COLLECT_HISTOGRAM_STATISTIC_TEMPLATE =
            "SELECT $tableId, '$columnNameStr', $dbId, '$dbName.$tableName'," +
                    " $histogramFunction, " +
                    " $mcv," +
                    " NOW()" +
                    " FROM (" +
                    "   SELECT $columnName as column_key " +
                    "   FROM `$dbName`.`$tableName` $sampleClause " +
                    "   WHERE $randFilter and $columnName is not null $MCVExclude" +
                    "   ORDER BY $columnName LIMIT $totalRows) t";

    private static final String COLLECT_HISTOGRAM_WITH_HLL_NDV_STATISTIC_TEMPLATE =
            "SELECT $tableId, '$columnNameStr', $dbId, '$dbName.$tableName'," +
                    " histogram_hll_ndv($columnName, '$buckets')," +
                    " $mcv," +
                    " NOW()" +
                    " FROM `$dbName`.`$tableName`;";

    private static final String COLLECT_BUCKETS_WITHOUT_NDV_STATISTIC_TEMPLATE =
            "SELECT cast(" + StatsConstants.STATISTIC_HISTOGRAM_VERSION + " as INT) as version," +
                    " cast($dbId  as BIGINT), cast($tableId as BIGINT), '$columnNameStr'," +
                    " $histogramFunction" +
                    " FROM (SELECT $columnName as column_key FROM `$dbName`.`$tableName` where rand() <= $sampleRatio" +
                    " and $columnName is not null $MCVExclude" +
                    " ORDER BY $columnName LIMIT $totalRows) t";

    private static final String COLLECT_MCV_STATISTIC_TEMPLATE =
            "select cast(version as INT), cast(db_id as BIGINT), cast(table_id as BIGINT), " +
                    "cast(column_key as varchar), cast(column_value as varchar) from (" +
                    "SELECT " +
                    StatsConstants.STATISTIC_HISTOGRAM_VERSION + " as version, " +
                    "   $dbId as db_id, " +
                    "   $tableId as table_id, " +
                    "   $columnName as column_key, " +
                    "   count($columnName) as column_value " +
                    "FROM `$dbName`.`$tableName` $sampleClause " +
                    "WHERE $columnName is not null " +
                    "GROUP BY $columnName " +
                    "ORDER BY count($columnName) desc limit $topN ) t";

    public HistogramStatisticsCollectJob(Database db, Table table, List<String> columnNames, List<Type> columnTypes,
                                         StatsConstants.ScheduleType scheduleType, Map<String, String> properties) {
        super(db, table, columnNames, columnTypes, StatsConstants.AnalyzeType.HISTOGRAM, scheduleType, properties);
    }

    @Override
    public void collect(ConnectContext context, AnalyzeStatus analyzeStatus) throws Exception {
        context.getSessionVariable().setNewPlanerAggStage(1);

        double sampleRatio = Double.parseDouble(properties.get(StatsConstants.HISTOGRAM_SAMPLE_RATIO));
        long bucketNum = Long.parseLong(properties.get(StatsConstants.HISTOGRAM_BUCKET_NUM));
        long mcvSize = Long.parseLong(properties.get(StatsConstants.HISTOGRAM_MCV_SIZE));
        StatsConstants.HistogramCollectBucketNdvMode ndvMode = getHistogramCollectBucketNdvMode();

        long finishedSQLNum = 0;
        long totalCollectSQL = columnNames.size();
        if (table.isTemporaryTable()) {
            context.setSessionId(((OlapTable) table).getSessionId());
        }
        for (int i = 0; i < columnNames.size(); i++) {
            String columnName = columnNames.get(i);
            Type columnType = columnTypes.get(i);
            String sql = buildCollectMCV(db, table, mcvSize, columnName, sampleRatio);
            StatisticExecutor statisticExecutor = new StatisticExecutor();
            List<TStatisticData> mcv = statisticExecutor.queryMCV(context, sql);

            Map<String, String> mostCommonValues = new HashMap<>();
            for (TStatisticData tStatisticData : mcv) {
                if (sampleRatio > 0.0 && sampleRatio < 1.0) {
                    long count = Long.parseLong(tStatisticData.histogram);
                    count = (long) (1.0 * count / sampleRatio);
                    mostCommonValues.put(tStatisticData.columnName, String.valueOf(count));
                } else {
                    mostCommonValues.put(tStatisticData.columnName, tStatisticData.histogram);
                }
            }

            if (ndvMode == StatsConstants.HistogramCollectBucketNdvMode.NONE) {
                sql = buildCollectHistogram(db, table, sampleRatio, bucketNum, mostCommonValues, columnName,
                        columnType, false);
            } else if (ndvMode == StatsConstants.HistogramCollectBucketNdvMode.SAMPLE) {
                sql = buildCollectHistogram(db, table, sampleRatio, bucketNum, mostCommonValues, columnName,
                        columnType, true);
            } else if (ndvMode == StatsConstants.HistogramCollectBucketNdvMode.HLL) {
                sql = buildCollectBucketsWithoutNdv(db, table, sampleRatio, bucketNum, mostCommonValues, columnName, columnType);
                List<TStatisticData> buckets = statisticExecutor.executeStatisticDQL(context, sql);
                sql = buildCollectHistogramWithHllNdv(db, table, mostCommonValues, buckets.get(0).histogram, columnName);
            }
            collectStatisticSync(sql, context);

            finishedSQLNum++;
            analyzeStatus.setProgress(finishedSQLNum * 100 / totalCollectSQL);
            GlobalStateMgr.getCurrentState().getAnalyzeMgr().addAnalyzeStatus(analyzeStatus);
        }
    }

    private StatsConstants.HistogramCollectBucketNdvMode getHistogramCollectBucketNdvMode() {
        String mode = properties.get(StatsConstants.HISTOGRAM_COLLECT_BUCKET_NDV_MODE);
        if (mode.equalsIgnoreCase("none")) {
            return StatsConstants.HistogramCollectBucketNdvMode.NONE;
        } else if (mode.equalsIgnoreCase("sample")) {
            return StatsConstants.HistogramCollectBucketNdvMode.SAMPLE;
        } else if (mode.equalsIgnoreCase("hll")) {
            return StatsConstants.HistogramCollectBucketNdvMode.HLL;
        } else {
            LOG.warn("Invalid histogram collect bucket ndv mode {}.", mode);
            return StatsConstants.HistogramCollectBucketNdvMode.NONE;
        }
    }

    private String buildCollectMCV(Database database, Table table, Long topN, String columnName, double sampleRatio) {
        VelocityContext context = new VelocityContext();
        context.put("tableId", table.getId());
        context.put("columnName", StatisticUtils.quoting(table, columnName));
        context.put("dbId", database.getId());

        context.put("dbName", database.getOriginName());
        context.put("tableName", table.getName());
        context.put("topN", topN);

        if (sampleRatio > 0.0 && sampleRatio < 1.0) {
            String sample = String.format("SAMPLE('percent'='%d')", (int) (sampleRatio * 100));
            context.put("sampleClause", sample);
        } else {
            context.put("sampleClause", "");
        }

        return build(context, COLLECT_MCV_STATISTIC_TEMPLATE);
    }

    private String buildInsertIntoHistogramStatistics(String query) {
        List<String> targetColumnNames = StatisticUtils.buildStatsColumnDef(HISTOGRAM_STATISTICS_TABLE_NAME).stream()
                .map(ColumnDef::getName)
                .collect(Collectors.toList());
        String columnNames = "(" + String.join(", ", targetColumnNames) + ")";
        return "INSERT INTO " +
                HISTOGRAM_STATISTICS_TABLE_NAME +
                columnNames +
                " " +
                query;
    }

    private VelocityContext buildBaseContext(Database database, Table table, String columnName) {
        String quoteColumName = StatisticUtils.quoting(table, columnName);

        VelocityContext context = new VelocityContext();
        context.put("tableId", table.getId());
        context.put("columnName", quoteColumName);
        context.put("columnNameStr", columnName);
        context.put("dbId", database.getId());
        context.put("dbName", database.getOriginName());
        context.put("tableName", table.getName());

        return context;
    }

    private void addMcvToContext(VelocityContext context, Map<String, String> mostCommonValues) {
        List<String> mcvList = new ArrayList<>();
        for (Map.Entry<String, String> entry : mostCommonValues.entrySet()) {
            mcvList.add("[\"" + entry.getKey() + "\",\"" + entry.getValue() + "\"]");
        }

        if (mostCommonValues.isEmpty()) {
            context.put("mcv", "NULL");
        } else {
            String mcvJson = "[" + Joiner.on(",").join(mcvList) + "]";
            String escapedMcvJson = mcvJson.replace("'", "''");
            context.put("mcv", "'" + escapedMcvJson + "'");
        }
    }

    private void addMcvExcludeToContext(VelocityContext context, Map<String, String> mostCommonValues, String columnName,
                                        Type columnType) {
        String quoteColumName = StatisticUtils.quoting(table, columnName);
        if (!mostCommonValues.isEmpty()) {
            if (columnType.getPrimitiveType().isDateType() || columnType.getPrimitiveType().isCharFamily()) {
                context.put("MCVExclude", " and " + quoteColumName + " not in (\"" +
                        Joiner.on("\",\"").join(mostCommonValues.keySet()) + "\")");
            } else {
                context.put("MCVExclude", " and " + quoteColumName + " not in (" +
                        Joiner.on(",").join(mostCommonValues.keySet()) + ")");
            }
        } else {
            context.put("MCVExclude", "");
        }
    }

    private String buildHistogramFunctionWithoutNdv(Database database, Table table, double sampleRatio, Long bucketNum,
                                          String columnName) {
        VelocityContext context = buildBaseContext(database, table, columnName);
        context.put("bucketNum", bucketNum);
        context.put("sampleRatio", sampleRatio);

        return build(context, HISTOGRAM_FUNCTION_WITHOUT_NDV_TEMPLATE);
    }

    private String buildHistogramFunction(Database database, Table table, double sampleRatio, Long bucketNum,
                                          String columnName, boolean withSampleNdv) {
        VelocityContext context = buildBaseContext(database, table, columnName);
        context.put("bucketNum", bucketNum);
        context.put("sampleRatio", sampleRatio);
        if (withSampleNdv) {
            context.put("ndvEstimator", Config.statistics_sample_ndv_estimator);
            return build(context, HISTOGRAM_FUNCTION_WITH_NDV_TEMPLATE);
        } else {
            return build(context, HISTOGRAM_FUNCTION_WITHOUT_NDV_TEMPLATE);
        }
    }

    private String buildCollectHistogram(Database database, Table table, double sampleRatio, Long bucketNum,
                                         Map<String, String> mostCommonValues, String columnName, Type columnType,
                                         boolean withSampleNdv) {
        VelocityContext context = buildBaseContext(database, table, columnName);
        context.put("histogramFunction", buildHistogramFunction(database, table, sampleRatio, bucketNum, columnName,
                withSampleNdv));
        context.put("totalRows", Config.histogram_max_sample_row_count);

        // TODO: use it by default and remove this switch
        if (Config.enable_use_table_sample_collect_statistics && sampleRatio > 0.0 && sampleRatio < 1.0) {
            String sampleClause = String.format("SAMPLE('percent'='%d')", (int) (sampleRatio * 100));
            context.put("sampleClause", sampleClause);
            context.put("randFilter", "TRUE");
        } else {
            String randFilter = String.format(" rand() <= %f", sampleRatio);
            context.put("randFilter", randFilter);
            context.put("sampleClause", "");
        }

        addMcvToContext(context, mostCommonValues);
        addMcvExcludeToContext(context, mostCommonValues, columnName, columnType);

        return buildInsertIntoHistogramStatistics(build(context, COLLECT_HISTOGRAM_STATISTIC_TEMPLATE));
    }

    private String buildCollectHistogramWithHllNdv(Database database, Table table, Map<String, String> mostCommonValues,
                                                String buckets, String columnName) {
        VelocityContext context = buildBaseContext(database, table, columnName);
        addMcvToContext(context, mostCommonValues);
        context.put("buckets", buckets);

        return buildInsertIntoHistogramStatistics(build(context, COLLECT_HISTOGRAM_WITH_HLL_NDV_STATISTIC_TEMPLATE));
    }

    private String buildCollectBucketsWithoutNdv(Database database, Table table, double sampleRatio,
                                      Long bucketNum, Map<String, String> mostCommonValues, String columnName,
                                      Type columnType) {
        VelocityContext context = buildBaseContext(database, table, columnName);
        context.put("histogramFunction", buildHistogramFunction(database, table, sampleRatio, bucketNum, columnName, false));
        context.put("sampleRatio", sampleRatio);
        context.put("totalRows", Config.histogram_max_sample_row_count);
        addMcvExcludeToContext(context, mostCommonValues, columnName, columnType);

        return build(context, COLLECT_BUCKETS_WITHOUT_NDV_STATISTIC_TEMPLATE);
    }

    @Override
    public String getName() {
        return "Histogram";
    }
}
