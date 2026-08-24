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
import com.starrocks.type.Type;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.velocity.VelocityContext;

import java.util.Map;

import static com.starrocks.statistic.ExternalHistogramSql.UNSAMPLED_RATIO;
import static com.starrocks.statistic.HistogramStatisticsUtils.buildBaseContext;
import static com.starrocks.statistic.HistogramStatisticsUtils.buildDefaultBucketSql;
import static com.starrocks.statistic.HistogramStatisticsUtils.buildStatsTargetColumnListSql;
import static com.starrocks.statistic.HistogramStatisticsUtils.putMcv;
import static com.starrocks.statistic.HistogramStatisticsUtils.putMcvExclude;
import static com.starrocks.statistic.StatsConstants.EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME;

/**
 * The external half of the legacy strategy: every column becomes one INSERT ... SELECT, followed by
 * a best-effort cleanup of the row it superseded.
 *
 * @see LegacyHistogramCollector
 */
final class ExternalHistogramLegacyTarget implements LegacyCollectTarget {
    private static final Logger LOG = LogManager.getLogger(ExternalHistogramLegacyTarget.class);

    private static final String COLLECT_HISTOGRAM_STATISTIC_TEMPLATE =
            "SELECT '$tableUUID', '$columnNameStr', '$catalogName', '$dbName', '$tableName'," +
                    " histogram(`column_key`, cast($bucketNum as int), cast($sampleRatio as double)), " +
                    " $mcv," +
                    " NOW()" +
                    " FROM (SELECT $columnName as column_key FROM `$catalogName`.`$dbName`.`$tableName`" +
                    " where rand() <= $sampleRatio" +
                    " and $columnName is not null $MCVExclude" +
                    " ORDER BY $columnName LIMIT $totalRows) t";

    // For char-family columns we skip the histogram() bucket aggregate, but we still need
    // Histogram.getTotalRows() to reflect the column's real cardinality. So instead of storing
    // NULL buckets we store a single placeholder bucket that represents "all values excluding
    // the MCVs".
    private static final String COLLECT_DEFAULT_BUCKET_STATISTIC_TEMPLATE =
            "SELECT '$tableUUID', '$columnNameStr', '$catalogName', '$dbName', '$tableName'," +
                    " $bucketExpr, $mcv, NOW()" +
                    " FROM `$catalogName`.`$dbName`.`$tableName`";

    private final ExternalHistogramStatisticsCollectJob job;
    private final Database db;
    private final Table table;
    private final String catalogName;

    ExternalHistogramLegacyTarget(ExternalHistogramStatisticsCollectJob job) {
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
    public String buildMcvQuery(HistogramCollectParams params, String columnName) {
        return ExternalHistogramSql.buildMcvQuery(db, table, catalogName, params.mcvSize(), columnName);
    }

    @Override
    public double mcvCountScaleRatio(HistogramCollectParams params) {
        return UNSAMPLED_RATIO;
    }

    @Override
    public String buildLegacyCollectSql(ConnectContext context, StatisticExecutor executor,
                                        HistogramCollectParams params, String columnName, Type columnType,
                                        Map<String, String> mostCommonValues) {
        // Skipping the buckets leaves one tail bucket holding all values - sum(MCVs).
        return StatisticsCollectJob.shouldSkipHistogramBuckets(columnType)
                ? buildInsertIntoHistogramStatistics(buildDefaultBucketSql(db, table, catalogName, columnName,
                mostCommonValues, UNSAMPLED_RATIO, COLLECT_DEFAULT_BUCKET_STATISTIC_TEMPLATE))
                : buildCollectHistogram(params.sampleRatio(), params.bucketNum(), mostCommonValues,
                columnName, columnType);
    }

    @Override
    public void afterColumnInserted(ConnectContext context, StatisticExecutor executor, String columnName) {
        // Best-effort: remove the stale raw-keyed row this column's fresh hashed-keyed row just
        // superseded. The read side no longer depends on this for correctness (it dedups by
        // update_time), so this is purely storage hygiene - failures are logged, not fatal.
        if (!executor.dropExternalHistogramRawColumn(context, table.getUUID(), columnName)) {
            LOG.warn("[ExternalStats] failed to clean up stale raw-keyed histogram row | catalog={} db={} table={} " +
                    "column={}", catalogName, db.getOriginName(), table.getName(), columnName);
        }
    }

    String buildCollectHistogram(double sampleRatio, Long bucketNum, Map<String, String> mostCommonValues,
                                 String columnName, Type columnType) {
        VelocityContext context = buildBaseContext(db, table, catalogName, columnName);
        putMcv(context, mostCommonValues);
        putMcvExclude(context, mostCommonValues, StatisticUtils.quoting(table, columnName), columnType);

        context.put("bucketNum", bucketNum);
        context.put("sampleRatio", sampleRatio);
        context.put("totalRows", Config.histogram_max_sample_row_count);

        return buildInsertIntoHistogramStatistics(
                StatisticsCollectJob.build(context, COLLECT_HISTOGRAM_STATISTIC_TEMPLATE));
    }

    private String buildInsertIntoHistogramStatistics(String query) {
        return "INSERT INTO " +
                EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME +
                buildStatsTargetColumnListSql(EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME) +
                " " +
                query;
    }
}
