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

import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.thrift.TStatisticData;
import com.starrocks.type.Type;
import org.apache.velocity.VelocityContext;

import java.util.List;
import java.util.Map;

import static com.starrocks.statistic.HistogramStatisticsUtils.buildBaseContext;
import static com.starrocks.statistic.HistogramStatisticsUtils.buildDefaultBucketSql;
import static com.starrocks.statistic.HistogramStatisticsUtils.buildStatsTargetColumnListSql;
import static com.starrocks.statistic.HistogramStatisticsUtils.putMcv;
import static com.starrocks.statistic.HistogramStatisticsUtils.putMcvExclude;
import static com.starrocks.statistic.NativeHistogramSql.addSampleClauseToContext;
import static com.starrocks.statistic.NativeHistogramSql.buildBucketBoundariesQuery;
import static com.starrocks.statistic.NativeHistogramSql.buildHistogramFunction;
import static com.starrocks.statistic.StatsConstants.HISTOGRAM_STATISTICS_TABLE_NAME;

/**
 * The native half of the legacy strategy: every column becomes one INSERT ... SELECT that the
 * backend computes end to end.
 *
 * @see LegacyHistogramCollector
 */
final class HistogramLegacyTarget implements LegacyCollectTarget {
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

    // For char-family columns we skip the histogram() bucket aggregate, but we still need
    // Histogram.getTotalRows() to reflect the column's real cardinality. So instead of storing
    // NULL buckets we store a single placeholder bucket that represents "all values excluding
    // the MCVs".
    private static final String COLLECT_DEFAULT_BUCKET_STATISTIC_TEMPLATE =
            "SELECT $tableId, '$columnNameStr', $dbId, '$dbName.$tableName'," +
                    " $bucketExpr," +
                    " $mcv," +
                    " NOW()" +
                    " FROM `$dbName`.`$tableName`$sampleClause$randFilter";

    private final HistogramStatisticsCollectJob job;
    private final StatsConstants.HistogramCollectBucketNdvMode ndvMode;
    private final Database db;
    private final Table table;

    HistogramLegacyTarget(HistogramStatisticsCollectJob job,
                          StatsConstants.HistogramCollectBucketNdvMode ndvMode) {
        this.job = job;
        this.ndvMode = ndvMode;
        this.db = job.getDb();
        this.table = job.getTable();
    }

    @Override
    public StatisticsCollectJob job() {
        return job;
    }

    @Override
    public String buildMcvQuery(HistogramCollectParams params, String columnName) {
        return NativeHistogramSql.buildMcvQuery(db, table, params.mcvSize(), columnName, params.sampleRatio());
    }

    @Override
    public double mcvCountScaleRatio(HistogramCollectParams params) {
        return params.sampleRatio();
    }

    @Override
    public String buildLegacyCollectSql(ConnectContext context, StatisticExecutor executor,
                                        HistogramCollectParams params, String columnName, Type columnType,
                                        Map<String, String> mostCommonValues) throws Exception {
        if (StatisticsCollectJob.shouldSkipHistogramBuckets(columnType)) {
            return buildInsertIntoHistogramStatistics(buildDefaultBucketSql(db, table, job.getCatalogName(),
                    columnName, mostCommonValues, params.sampleRatio(), COLLECT_DEFAULT_BUCKET_STATISTIC_TEMPLATE));
        }

        if (ndvMode == StatsConstants.HistogramCollectBucketNdvMode.NONE) {
            return buildCollectHistogram(params.sampleRatio(), params.bucketNum(), mostCommonValues,
                    columnName, columnType, false);
        }

        if (ndvMode == StatsConstants.HistogramCollectBucketNdvMode.SAMPLE) {
            return buildCollectHistogram(params.sampleRatio(), params.bucketNum(), mostCommonValues,
                    columnName, columnType, true);
        }

        // HLL mode needs the bucket boundaries in hand before the INSERT can name them, so it runs an
        // intermediate query on the caller's executor first.
        String bucketQuery = buildBucketBoundariesQuery(db, table, job.getCatalogName(), params.sampleRatio(),
                params.bucketNum(), mostCommonValues, columnName, columnType);
        List<TStatisticData> buckets = executor.executeStatisticDQL(context, bucketQuery);
        return buildCollectHistogramWithHllNdv(mostCommonValues, buckets.get(0).histogram, columnName);
    }

    @Override
    public void afterColumnInserted(ConnectContext context, StatisticExecutor executor, String columnName) {
        // Internal statistics need no post-write cleanup.
    }

    String buildCollectHistogram(double sampleRatio, Long bucketNum, Map<String, String> mostCommonValues,
                                 String columnName, Type columnType, boolean withSampleNdv) {
        VelocityContext context = buildBaseContext(db, table, job.getCatalogName(), columnName);
        putMcv(context, mostCommonValues);
        putMcvExclude(context, mostCommonValues, StatisticUtils.quoting(table, columnName), columnType);

        context.put("histogramFunction", buildHistogramFunction(db, table, job.getCatalogName(), sampleRatio,
                bucketNum, columnName, withSampleNdv));
        context.put("totalRows", Config.histogram_max_sample_row_count);
        addSampleClauseToContext(context, sampleRatio);

        return buildInsertIntoHistogramStatistics(
                StatisticsCollectJob.build(context, COLLECT_HISTOGRAM_STATISTIC_TEMPLATE));
    }

    String buildCollectHistogramWithHllNdv(Map<String, String> mostCommonValues, String buckets, String columnName) {
        VelocityContext context = buildBaseContext(db, table, job.getCatalogName(), columnName);
        putMcv(context, mostCommonValues);
        context.put("buckets", buckets);

        return buildInsertIntoHistogramStatistics(
                StatisticsCollectJob.build(context, COLLECT_HISTOGRAM_WITH_HLL_NDV_STATISTIC_TEMPLATE));
    }

    private String buildInsertIntoHistogramStatistics(String query) {
        return "INSERT INTO " +
                HISTOGRAM_STATISTICS_TABLE_NAME +
                buildStatsTargetColumnListSql(HISTOGRAM_STATISTICS_TABLE_NAME) +
                " " +
                query;
    }
}
