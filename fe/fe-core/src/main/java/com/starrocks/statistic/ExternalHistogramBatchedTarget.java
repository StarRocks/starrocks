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
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.type.Type;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.velocity.VelocityContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.starrocks.statistic.ExternalHistogramSql.UNSAMPLED_RATIO;
import static com.starrocks.statistic.HistogramStatisticsUtils.buildBaseContext;
import static com.starrocks.statistic.HistogramStatisticsUtils.buildBucketsLiteral;
import static com.starrocks.statistic.HistogramStatisticsUtils.buildBucketsSql;
import static com.starrocks.statistic.HistogramStatisticsUtils.buildDefaultBucketSql;
import static com.starrocks.statistic.HistogramStatisticsUtils.putMcvExclude;
import static com.starrocks.statistic.HistogramStatisticsUtils.quoteSqlString;
import static com.starrocks.statistic.StatsConstants.EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME;

/**
 * The external half of the batched strategy: every column's buckets are queried into the FE and
 * written back as one buffered INSERT ... VALUES, followed by a best-effort cleanup of the rows they
 * superseded.
 *
 * @see BatchedHistogramCollector
 */
final class ExternalHistogramBatchedTarget implements BatchedStatsCollectionUtils {
    private static final Logger LOG = LogManager.getLogger(ExternalHistogramBatchedTarget.class);

    private static final String QUERY_HISTOGRAM_STATISTIC_TEMPLATE =
            "SELECT cast(" + StatsConstants.STATISTIC_EXTERNAL_HISTOGRAM_VERSION + " as INT)," +
                    " '$columnNameStr'," +
                    " histogram(`column_key`, cast($bucketNum as int), cast($sampleRatio as double))" +
                    " FROM (SELECT $columnName as column_key FROM `$catalogName`.`$dbName`.`$tableName`" +
                    " where rand() <= $sampleRatio" +
                    " and $columnName is not null $MCVExclude" +
                    " ORDER BY $columnName LIMIT $totalRows) t";

    private static final String QUERY_DEFAULT_BUCKET_STATISTIC_TEMPLATE =
            "SELECT cast(" + StatsConstants.STATISTIC_EXTERNAL_HISTOGRAM_VERSION + " as INT)," +
                    " '$columnNameStr', $bucketExpr" +
                    " FROM `$catalogName`.`$dbName`.`$tableName`";

    private final ExternalHistogramStatisticsCollectJob job;
    private final Database db;
    private final Table table;
    private final String catalogName;

    ExternalHistogramBatchedTarget(ExternalHistogramStatisticsCollectJob job) {
        this.job = job;
        this.db = job.getDb();
        this.table = job.getTable();
        this.catalogName = job.getCatalogName();
    }

    @Override
    public StatisticsCollectJob job() {
        return job;
    }

    @Override
    public String statsTableName() {
        return EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME;
    }

    @Override
    public String statisticsDescription() {
        return "external histogram";
    }

    @Override
    public String buildMcvQuery(HistogramCollectParams params, String columnName) {
        return ExternalHistogramSql.buildMcvQuery(db, table, catalogName, params.mcvSize(), columnName);
    }

    @Override
    public double mcvCountScaleRatio(HistogramCollectParams params) {
        return UNSAMPLED_RATIO;
    }

    @Override
    public String buildSqlCmd(ConnectContext context, AnalyzeStatus analyzeStatus,
                                             HistogramCollectParams params, String columnName, Type columnType,
                                             Map<String, String> mostCommonValues) {
        return StatisticsCollectJob.shouldSkipHistogramBuckets(columnType)
                ? buildDefaultBucketSql(db, table, catalogName, columnName, mostCommonValues,
                UNSAMPLED_RATIO, QUERY_DEFAULT_BUCKET_STATISTIC_TEMPLATE)
                : buildQueryHistogram(params.sampleRatio(), params.bucketNum(), mostCommonValues,
                columnName, columnType);
    }

    @Override
    public void afterCollection(ConnectContext context, List<String> insertedColumns) {
        if (insertedColumns.isEmpty()) {
            return;
        }
        try {
            StatisticExecutor statisticExecutor = new StatisticExecutor();
            if (!statisticExecutor.dropExternalHistogramRawColumns(context, table.getUUID(), insertedColumns)) {
                LOG.warn("[ExternalStats] failed to clean up stale raw-keyed histogram rows | catalog={} db={} " +
                                "table={} columns={}",
                        catalogName, db.getOriginName(), table.getName(), insertedColumns);
            }
        } catch (Exception e) {
            LOG.warn("[ExternalStats] failed to clean up stale raw-keyed histogram rows | catalog={} db={} table={} " +
                            "columns={}",
                    catalogName, db.getOriginName(), table.getName(), insertedColumns, e);
        }
    }

    @Override
    public List<Expr> buildBatchInsertRow(String columnName, String buckets, String mcvJson) {
        List<Expr> row = new ArrayList<>();
        row.add(new StringLiteral(StatisticUtils.hashTableUuidForPkStorage(table.getUUID())));
        row.add(new StringLiteral(columnName));
        row.add(new StringLiteral(catalogName));
        row.add(new StringLiteral(db.getOriginName()));
        row.add(new StringLiteral(table.getName()));
        row.add(buildBucketsLiteral(buckets));
        row.add(mcvJson == null ? new NullLiteral() : new StringLiteral(mcvJson));
        row.add(StatisticsCollectJob.nowFn());
        return row;
    }

    @Override
    public String buildBatchInsertRowSql(String columnName, String buckets, String mcvJson) {
        List<String> values = new ArrayList<>();
        values.add(quoteSqlString(StatisticUtils.hashTableUuidForPkStorage(table.getUUID())));
        values.add(quoteSqlString(columnName));
        values.add(quoteSqlString(catalogName));
        values.add(quoteSqlString(db.getOriginName()));
        values.add(quoteSqlString(table.getName()));
        values.add(buildBucketsSql(buckets));
        values.add(mcvJson == null ? "NULL" : quoteSqlString(mcvJson));
        values.add("NOW()");
        return "(" + String.join(", ", values) + ")";
    }

    String buildQueryHistogram(double sampleRatio, Long bucketNum, Map<String, String> mostCommonValues,
                               String columnName, Type columnType) {
        VelocityContext context = buildBaseContext(db, table, catalogName, columnName);
        putMcvExclude(context, mostCommonValues, StatisticUtils.quoting(table, columnName), columnType);

        context.put("bucketNum", bucketNum);
        context.put("sampleRatio", sampleRatio);
        context.put("totalRows", Config.histogram_max_sample_row_count);
        return StatisticsCollectJob.build(context, QUERY_HISTOGRAM_STATISTIC_TEMPLATE);
    }
}
