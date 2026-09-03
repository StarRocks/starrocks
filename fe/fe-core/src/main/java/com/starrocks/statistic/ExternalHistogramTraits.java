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
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.thrift.TStatisticData;
import com.starrocks.type.Type;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.velocity.VelocityContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.starrocks.statistic.HistogramStatisticsUtils.buildBaseContext;
import static com.starrocks.statistic.HistogramStatisticsUtils.buildBucketsLiteral;
import static com.starrocks.statistic.HistogramStatisticsUtils.buildBucketsSql;
import static com.starrocks.statistic.HistogramStatisticsUtils.buildDefaultBucketSql;
import static com.starrocks.statistic.HistogramStatisticsUtils.putMcvExclude;
import static com.starrocks.statistic.HistogramStatisticsUtils.quoteSqlString;
import static com.starrocks.statistic.StatsConstants.EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME;

/**
 * Histogram collection for an external table: the rows are keyed by a hashed table UUID plus the
 * catalog/db/table names, and the write is followed by a best-effort cleanup of the raw-keyed rows
 * it superseded.
 *
 * @see HistogramCollector
 */
final class ExternalHistogramTraits extends HistogramCollectTraits {
    private static final Logger LOG = LogManager.getLogger(ExternalHistogramTraits.class);

    /**
     * The MCV and default-bucket queries scan the whole table - unlike the histogram query they
     * carry no sample clause - so their counts are already full-table counts and must not be scaled.
     */
    private static final double UNSAMPLED_RATIO = 1.0;

    private static final String MCV_STATISTIC_TEMPLATE =
            "select cast(version as INT), " +
                    "cast(column_key as varchar), cast(column_value as varchar) from (" +
                    "select " + StatsConstants.STATISTIC_EXTERNAL_HISTOGRAM_VERSION + " as version, " +
                    "$columnName as column_key, " +
                    "count($columnName) as column_value " +
                    "from `$catalogName`.`$dbName`.`$tableName` where $columnName is not null " +
                    "group by $columnName " +
                    "order by column_value desc limit $topN ) t";

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

    ExternalHistogramTraits(StatisticsCollectJob job, HistogramCollectParams params) {
        super(job, params);
    }

    @Override
    String statsTableName() {
        return EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME;
    }

    @Override
    String statisticsDescription() {
        return "external histogram";
    }

    @Override
    String buildMcvQuery(String columnName) {
        VelocityContext context = new VelocityContext();
        context.put("columnName", StatisticUtils.quoting(table, columnName));
        context.put("catalogName", catalogName);
        context.put("dbName", db.getOriginName());
        context.put("tableName", table.getName());
        context.put("topN", params.mcvSize());

        return StatisticsCollectJob.build(context, MCV_STATISTIC_TEMPLATE);
    }

    @Override
    Map<String, String> buildMostCommonValues(List<TStatisticData> mcv) {
        return HistogramStatisticsUtils.buildMostCommonValues(mcv, UNSAMPLED_RATIO);
    }

    @Override
    String buildBucketsQuery(ConnectContext context, AnalyzeStatus analyzeStatus, String columnName,
                             Type columnType, Map<String, String> mostCommonValues) {
        // Skipping the buckets leaves one tail bucket holding all values - sum(MCVs).
        return StatisticsCollectJob.shouldSkipHistogramBuckets(columnType)
                ? buildDefaultBucketSql(db, table, catalogName, columnName, mostCommonValues,
                        UNSAMPLED_RATIO, QUERY_DEFAULT_BUCKET_STATISTIC_TEMPLATE)
                : buildHistogramQuery(params.sampleRatio(), params.bucketNum(), mostCommonValues,
                        columnName, columnType);
    }

    @Override
    void afterCollection(ConnectContext context, List<String> insertedColumns) {
        if (insertedColumns.isEmpty()) {
            return;
        }
        // Best-effort: remove the stale raw-keyed rows the fresh hashed-keyed rows just superseded.
        // The read side no longer depends on this for correctness (it dedups by update_time), so
        // this is purely storage hygiene - failures are logged, not fatal.
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
    List<Expr> buildInsertRow(String columnName, String buckets, String mcvJson) {
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
    String buildInsertRowSql(String columnName, String buckets, String mcvJson) {
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

    @VisibleForTesting
    String buildHistogramQuery(double sampleRatio, Long bucketNum, Map<String, String> mostCommonValues,
                               String columnName, Type columnType) {
        VelocityContext context = buildBaseContext(db, table, catalogName, columnName);
        putMcvExclude(context, mostCommonValues, StatisticUtils.quoting(table, columnName), columnType);

        context.put("bucketNum", bucketNum);
        context.put("sampleRatio", sampleRatio);
        context.put("totalRows", Config.histogram_max_sample_row_count);
        return StatisticsCollectJob.build(context, QUERY_HISTOGRAM_STATISTIC_TEMPLATE);
    }
}
