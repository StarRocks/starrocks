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

import com.google.common.annotations.VisibleForTesting;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.thrift.TStatisticData;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.velocity.VelocityContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.starrocks.statistic.HistogramStatisticsUtils.buildBaseContext;
import static com.starrocks.statistic.HistogramStatisticsUtils.buildBucketsLiteral;
import static com.starrocks.statistic.HistogramStatisticsUtils.buildBucketsSql;
import static com.starrocks.statistic.HistogramStatisticsUtils.buildDefaultBucketSql;
import static com.starrocks.statistic.HistogramStatisticsUtils.formatSamplePercent;
import static com.starrocks.statistic.HistogramStatisticsUtils.normalizeBucketsForHll;
import static com.starrocks.statistic.HistogramStatisticsUtils.putMcvExclude;
import static com.starrocks.statistic.HistogramStatisticsUtils.quoteSqlString;
import static com.starrocks.statistic.StatsConstants.HISTOGRAM_STATISTICS_TABLE_NAME;

/**
 * Histogram collection for a native (internal) table: the buckets come from the sampled
 * {@code histogram()} aggregate, and the rows carry the table and database ids.
 *
 * @see HistogramCollector
 */
final class NativeHistogramTraits extends HistogramCollectTraits {
    private static final String MCV_STATISTIC_TEMPLATE =
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

    private static final String HISTOGRAM_FUNCTION_WITHOUT_NDV_TEMPLATE =
            "histogram(`column_key`, cast($bucketNum as int), cast($sampleRatio as double))";

    private static final String HISTOGRAM_FUNCTION_WITH_NDV_TEMPLATE =
            "histogram(`column_key`, cast($bucketNum as int), cast($sampleRatio as double), '$ndvEstimator')";

    private static final String QUERY_HISTOGRAM_STATISTIC_TEMPLATE =
            "SELECT cast(" + StatsConstants.STATISTIC_HISTOGRAM_VERSION + " as INT)," +
                    " cast($dbId as BIGINT), cast($tableId as BIGINT), '$columnNameStr'," +
                    " $histogramFunction" +
                    " FROM (" +
                    "   SELECT $columnName as column_key " +
                    "   FROM `$dbName`.`$tableName` $sampleClause " +
                    "   WHERE $randFilter and $columnName is not null $MCVExclude" +
                    "   ORDER BY $columnName LIMIT $totalRows) t";

    private static final String QUERY_HISTOGRAM_WITH_HLL_NDV_STATISTIC_TEMPLATE =
            "SELECT cast(" + StatsConstants.STATISTIC_HISTOGRAM_VERSION + " as INT)," +
                    " cast($dbId as BIGINT), cast($tableId as BIGINT), '$columnNameStr'," +
                    " histogram_hll_ndv($columnName, '$buckets')" +
                    " FROM `$dbName`.`$tableName`;";

    private static final String QUERY_DEFAULT_BUCKET_STATISTIC_TEMPLATE =
            "SELECT cast(" + StatsConstants.STATISTIC_HISTOGRAM_VERSION + " as INT)," +
                    " cast($dbId as BIGINT), cast($tableId as BIGINT), '$columnNameStr'," +
                    " $bucketExpr" +
                    " FROM `$dbName`.`$tableName`$sampleClause$randFilter";

    private static final String BUCKET_BOUNDARIES_TEMPLATE =
            "SELECT cast(" + StatsConstants.STATISTIC_HISTOGRAM_VERSION + " as INT) as version," +
                    " cast($dbId  as BIGINT), cast($tableId as BIGINT), '$columnNameStr'," +
                    " $histogramFunction" +
                    " FROM (SELECT $columnName as column_key FROM `$dbName`.`$tableName` where rand() <= $sampleRatio" +
                    " and $columnName is not null $MCVExclude" +
                    " ORDER BY $columnName LIMIT $totalRows) t";

    NativeHistogramTraits(StatisticsCollectJob job, HistogramCollectParams params) {
        super(job, params);
    }

    @Override
    String statsTableName() {
        return HISTOGRAM_STATISTICS_TABLE_NAME;
    }

    @Override
    String statisticsDescription() {
        return "histogram";
    }

    @Override
    String buildMcvQuery(String columnName) {
        VelocityContext context = new VelocityContext();
        context.put("tableId", table.getId());
        context.put("columnName", StatisticUtils.quoting(table, columnName));
        context.put("dbId", db.getId());

        context.put("dbName", db.getOriginName());
        context.put("tableName", table.getName());
        context.put("topN", params.mcvSize());

        double sampleRatio = params.sampleRatio();
        if (sampleRatio > 0.0 && sampleRatio < 1.0) {
            context.put("sampleClause", String.format("SAMPLE('percent'='%s')", formatSamplePercent(sampleRatio)));
        } else {
            context.put("sampleClause", "");
        }

        return StatisticsCollectJob.build(context, MCV_STATISTIC_TEMPLATE);
    }

    @Override
    Map<String, String> buildMostCommonValues(List<TStatisticData> mcv) {
        return HistogramStatisticsUtils.buildMostCommonValues(mcv, params.sampleRatio());
    }

    @Override
    String buildBucketsQuery(ConnectContext context, AnalyzeStatus analyzeStatus, String columnName,
                             Type columnType, Map<String, String> mostCommonValues) throws Exception {
        if (StatisticsCollectJob.shouldSkipHistogramBuckets(columnType)) {
            return buildDefaultBucketSql(db, table, catalogName, columnName, mostCommonValues,
                    params.sampleRatio(), QUERY_DEFAULT_BUCKET_STATISTIC_TEMPLATE);
        }

        StatsConstants.HistogramCollectBucketNdvMode ndvMode = params.bucketNdvMode();
        if (ndvMode == StatsConstants.HistogramCollectBucketNdvMode.NONE) {
            return buildHistogramQuery(params.sampleRatio(), params.bucketNum(), mostCommonValues,
                    columnName, columnType, false);
        }

        if (ndvMode == StatsConstants.HistogramCollectBucketNdvMode.SAMPLE) {
            return buildHistogramQuery(params.sampleRatio(), params.bucketNum(), mostCommonValues,
                    columnName, columnType, true);
        }

        List<TStatisticData> buckets = job.queryStatisticSync(
                buildBucketBoundariesQuery(params.sampleRatio(), params.bucketNum(), mostCommonValues,
                        columnName, columnType),
                context, analyzeStatus);
        return buildHllNdvQuery(normalizeBucketsForHll(singleResult(buckets, columnName).histogram), columnName);
    }

    @Override
    List<Expr> buildInsertRow(String columnName, String buckets, String mcvJson) {
        List<Expr> row = new ArrayList<>();
        row.add(new IntLiteral(table.getId(), IntegerType.BIGINT));
        row.add(new StringLiteral(columnName));
        row.add(new IntLiteral(db.getId(), IntegerType.BIGINT));
        row.add(new StringLiteral(db.getOriginName() + "." + table.getName()));
        row.add(buildBucketsLiteral(buckets));
        row.add(mcvJson == null ? new NullLiteral() : new StringLiteral(mcvJson));
        row.add(StatisticsCollectJob.nowFn());
        return row;
    }

    @Override
    String buildInsertRowSql(String columnName, String buckets, String mcvJson) {
        List<String> values = new ArrayList<>();
        values.add(String.valueOf(table.getId()));
        values.add(quoteSqlString(columnName));
        values.add(String.valueOf(db.getId()));
        values.add(quoteSqlString(db.getOriginName() + "." + table.getName()));
        values.add(buildBucketsSql(buckets));
        values.add(mcvJson == null ? "NULL" : quoteSqlString(mcvJson));
        values.add("NOW()");
        return "(" + String.join(", ", values) + ")";
    }

    @VisibleForTesting
    String buildHistogramQuery(double sampleRatio, Long bucketNum, Map<String, String> mostCommonValues,
                               String columnName, Type columnType, boolean withSampleNdv) {
        VelocityContext context = buildBaseContext(db, table, catalogName, columnName);
        putMcvExclude(context, mostCommonValues, StatisticUtils.quoting(table, columnName), columnType);

        context.put("histogramFunction", buildHistogramFunction(sampleRatio, bucketNum, columnName, withSampleNdv));
        context.put("totalRows", Config.histogram_max_sample_row_count);
        addSampleClauseToContext(context, sampleRatio);

        return StatisticsCollectJob.build(context, QUERY_HISTOGRAM_STATISTIC_TEMPLATE);
    }

    @VisibleForTesting
    String buildHllNdvQuery(String buckets, String columnName) {
        VelocityContext context = buildBaseContext(db, table, catalogName, columnName);
        context.put("buckets", StringEscapeUtils.escapeSql(buckets));
        return StatisticsCollectJob.build(context, QUERY_HISTOGRAM_WITH_HLL_NDV_STATISTIC_TEMPLATE);
    }

    /** Query whose single row carries the bucket boundaries the HLL mode estimates NDV over. */
    @VisibleForTesting
    String buildBucketBoundariesQuery(double sampleRatio, Long bucketNum, Map<String, String> mostCommonValues,
                                      String columnName, Type columnType) {
        VelocityContext context = buildBaseContext(db, table, catalogName, columnName);
        context.put("histogramFunction", buildHistogramFunction(sampleRatio, bucketNum, columnName, false));
        context.put("sampleRatio", sampleRatio);
        context.put("totalRows", Config.histogram_max_sample_row_count);
        putMcvExclude(context, mostCommonValues, StatisticUtils.quoting(table, columnName), columnType);

        return StatisticsCollectJob.build(context, BUCKET_BOUNDARIES_TEMPLATE);
    }

    private String buildHistogramFunction(double sampleRatio, Long bucketNum, String columnName,
                                          boolean withSampleNdv) {
        VelocityContext context = buildBaseContext(db, table, catalogName, columnName);
        context.put("bucketNum", bucketNum);
        context.put("sampleRatio", sampleRatio);
        if (withSampleNdv) {
            context.put("ndvEstimator", Config.statistics_sample_ndv_estimator);
            return StatisticsCollectJob.build(context, HISTOGRAM_FUNCTION_WITH_NDV_TEMPLATE);
        } else {
            return StatisticsCollectJob.build(context, HISTOGRAM_FUNCTION_WITHOUT_NDV_TEMPLATE);
        }
    }

    // TODO: use table sample by default and remove this switch
    private static void addSampleClauseToContext(VelocityContext context, double sampleRatio) {
        if (Config.enable_use_table_sample_collect_statistics && sampleRatio > 0.0 && sampleRatio < 1.0) {
            context.put("sampleClause", String.format("SAMPLE('percent'='%s')", formatSamplePercent(sampleRatio)));
            context.put("randFilter", "TRUE");
        } else {
            context.put("sampleClause", "");
            context.put("randFilter", String.format(" rand() <= %f", sampleRatio));
        }
    }
}
