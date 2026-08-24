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
import com.starrocks.common.DdlException;
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
import static com.starrocks.statistic.HistogramStatisticsUtils.normalizeBucketsForHll;
import static com.starrocks.statistic.HistogramStatisticsUtils.putMcvExclude;
import static com.starrocks.statistic.HistogramStatisticsUtils.quoteSqlString;
import static com.starrocks.statistic.NativeHistogramSql.addSampleClauseToContext;
import static com.starrocks.statistic.NativeHistogramSql.buildBucketBoundariesQuery;
import static com.starrocks.statistic.NativeHistogramSql.buildHistogramFunction;
import static com.starrocks.statistic.StatsConstants.HISTOGRAM_STATISTICS_TABLE_NAME;

/**
 * The native half of the batched strategy: every column's buckets are queried into the FE and
 * written back as one buffered INSERT ... VALUES.
 *
 * @see BatchedHistogramCollector
 */
final class HistogramBatchedTarget implements BatchedStatsCollectionUtils {
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

    private final HistogramStatisticsCollectJob job;
    private final StatsConstants.HistogramCollectBucketNdvMode ndvMode;
    private final Database db;
    private final Table table;

    HistogramBatchedTarget(HistogramStatisticsCollectJob job,
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
    public String statsTableName() {
        return HISTOGRAM_STATISTICS_TABLE_NAME;
    }

    @Override
    public String statisticsDescription() {
        return "histogram";
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
    public String buildSqlCmd(ConnectContext context, AnalyzeStatus analyzeStatus,
                                             HistogramCollectParams params, String columnName, Type columnType,
                                             Map<String, String> mostCommonValues) throws Exception {
        if (StatisticsCollectJob.shouldSkipHistogramBuckets(columnType)) {
            return buildDefaultBucketSql(db, table, job.getCatalogName(), columnName, mostCommonValues,
                    params.sampleRatio(), QUERY_DEFAULT_BUCKET_STATISTIC_TEMPLATE);
        }

        if (ndvMode == StatsConstants.HistogramCollectBucketNdvMode.NONE) {
            return buildQueryHistogram(params.sampleRatio(), params.bucketNum(), mostCommonValues,
                    columnName, columnType, false);
        }

        if (ndvMode == StatsConstants.HistogramCollectBucketNdvMode.SAMPLE) {
            return buildQueryHistogram(params.sampleRatio(), params.bucketNum(), mostCommonValues,
                    columnName, columnType, true);
        }

        List<TStatisticData> buckets = job.queryStatisticSync(
                buildBucketBoundariesQuery(db, table, job.getCatalogName(), params.sampleRatio(), params.bucketNum(),
                        mostCommonValues, columnName, columnType),
                context, analyzeStatus);
        return buildQueryHistogramWithHllNdv(
                normalizeBucketsForHll(getSingleHistogramResult(buckets, columnName).histogram), columnName);
    }

    @Override
    public void afterCollection(ConnectContext context, List<String> insertedColumns) {
        // Internal statistics need no post-write cleanup.
    }

    @Override
    public List<Expr> buildBatchInsertRow(String columnName, String buckets, String mcvJson) {
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
    public String buildBatchInsertRowSql(String columnName, String buckets, String mcvJson) {
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

    String buildQueryHistogram(double sampleRatio, Long bucketNum, Map<String, String> mostCommonValues,
                               String columnName, Type columnType, boolean withSampleNdv) {
        VelocityContext context = buildBaseContext(db, table, job.getCatalogName(), columnName);
        putMcvExclude(context, mostCommonValues, StatisticUtils.quoting(table, columnName), columnType);

        context.put("histogramFunction", buildHistogramFunction(db, table, job.getCatalogName(), sampleRatio,
                bucketNum, columnName, withSampleNdv));
        context.put("totalRows", Config.histogram_max_sample_row_count);
        addSampleClauseToContext(context, sampleRatio);

        return StatisticsCollectJob.build(context, QUERY_HISTOGRAM_STATISTIC_TEMPLATE);
    }

    private String buildQueryHistogramWithHllNdv(String buckets, String columnName) {
        VelocityContext context = buildBaseContext(db, table, job.getCatalogName(), columnName);
        context.put("buckets", StringEscapeUtils.escapeSql(buckets));
        return StatisticsCollectJob.build(context, QUERY_HISTOGRAM_WITH_HLL_NDV_STATISTIC_TEMPLATE);
    }

    private TStatisticData getSingleHistogramResult(List<TStatisticData> results, String columnName)
            throws DdlException {
        return HistogramStatisticsUtils.getSingleHistogramResult(results, columnName, statisticsDescription());
    }
}
