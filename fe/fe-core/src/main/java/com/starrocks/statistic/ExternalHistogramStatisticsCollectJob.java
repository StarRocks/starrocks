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

import static com.starrocks.statistic.HistogramStatisticsUtils.buildBaseContext;
import static com.starrocks.statistic.HistogramStatisticsUtils.buildBucketsLiteral;
import static com.starrocks.statistic.HistogramStatisticsUtils.buildBucketsSql;
import static com.starrocks.statistic.HistogramStatisticsUtils.buildDefaultBucketSql;
import static com.starrocks.statistic.HistogramStatisticsUtils.buildStatsTargetColumnNames;
import static com.starrocks.statistic.HistogramStatisticsUtils.putMcv;
import static com.starrocks.statistic.HistogramStatisticsUtils.putMcvExclude;
import static com.starrocks.statistic.HistogramStatisticsUtils.quoteSqlString;
import static com.starrocks.statistic.StatsConstants.EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME;

public class ExternalHistogramStatisticsCollectJob extends StatisticsCollectJob
        implements LegacyCollectTarget, BatchedCollectTarget {
    private static final Logger LOG = LogManager.getLogger(ExternalHistogramStatisticsCollectJob.class);

    private static final String COLLECT_HISTOGRAM_STATISTIC_TEMPLATE =
            "SELECT '$tableUUID', '$columnNameStr', '$catalogName', '$dbName', '$tableName'," +
                    " histogram(`column_key`, cast($bucketNum as int), cast($sampleRatio as double)), " +
                    " $mcv," +
                    " NOW()" +
                    " FROM (SELECT $columnName as column_key FROM `$catalogName`.`$dbName`.`$tableName`" +
                    " where rand() <= $sampleRatio" +
                    " and $columnName is not null $MCVExclude" +
                    " ORDER BY $columnName LIMIT $totalRows) t";

    private static final String QUERY_HISTOGRAM_STATISTIC_TEMPLATE =
            "SELECT cast(" + StatsConstants.STATISTIC_EXTERNAL_HISTOGRAM_VERSION + " as INT)," +
                    " '$columnNameStr'," +
                    " histogram(`column_key`, cast($bucketNum as int), cast($sampleRatio as double))" +
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

    private static final String QUERY_DEFAULT_BUCKET_STATISTIC_TEMPLATE =
            "SELECT cast(" + StatsConstants.STATISTIC_EXTERNAL_HISTOGRAM_VERSION + " as INT)," +
                    " '$columnNameStr', $bucketExpr" +
                    " FROM `$catalogName`.`$dbName`.`$tableName`";

    private static final String COLLECT_MCV_STATISTIC_TEMPLATE =
            "select cast(version as INT), " +
                    "cast(column_key as varchar), cast(column_value as varchar) from (" +
                    "select " + StatsConstants.STATISTIC_EXTERNAL_HISTOGRAM_VERSION + " as version, " +
                    "$columnName as column_key, " +
                    "count($columnName) as column_value " +
                    "from `$catalogName`.`$dbName`.`$tableName` where $columnName is not null " +
                    "group by $columnName " +
                    "order by column_value desc limit $topN ) t";

    // The MCV and default-bucket templates above scan the whole table - unlike the histogram
    // template, they carry no sample clause - so their counts are already full-table counts and the
    // shared helpers must not scale them.
    private static final double UNSAMPLED_RATIO = 1.0;

    private final String catalogName;

    public ExternalHistogramStatisticsCollectJob(String catalogName, Database db, Table table, List<String> columnNames,
                                                 List<Type> columnTypes, StatsConstants.AnalyzeType type,
                                                 StatsConstants.ScheduleType scheduleType,
                                                 Map<String, String> properties) {
        super(db, table, columnNames, columnTypes, type, scheduleType, properties);
        this.catalogName = catalogName;
    }

    @Override
    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public String getName() {
        return "ExternalHistogram";
    }

    @Override
    public void collect(ConnectContext context, AnalyzeStatus analyzeStatus) throws Exception {
        context.getSessionVariable().setNewPlanerAggStage(1);

        HistogramCollectParams params = new HistogramCollectParams(properties);
        if (Config.enable_batch_insert_histogram_statistics && columnNames.size() > 1) {
            new BatchedHistogramCollector(this, params).collect(context, analyzeStatus);
        } else {
            new LegacyHistogramCollector(this, params).collect(context, analyzeStatus);
        }
    }

    @Override
    public StatisticsCollectJob job() {
        return this;
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
        return buildCollectMCV(db, table, params.mcvSize(), columnName);
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
        return shouldSkipHistogramBuckets(columnType)
                ? buildInsertIntoHistogramStatistics(buildDefaultBucketSql(db, table, catalogName, columnName,
                mostCommonValues, UNSAMPLED_RATIO, COLLECT_DEFAULT_BUCKET_STATISTIC_TEMPLATE))
                : buildCollectHistogram(db, table, params.sampleRatio(), params.bucketNum(), mostCommonValues,
                columnName, columnType);
    }

    @Override
    public String buildBatchedHistogramQuery(ConnectContext context, AnalyzeStatus analyzeStatus,
                                             HistogramCollectParams params, String columnName, Type columnType,
                                             Map<String, String> mostCommonValues) {
        return shouldSkipHistogramBuckets(columnType)
                ? buildDefaultBucketSql(db, table, catalogName, columnName, mostCommonValues,
                UNSAMPLED_RATIO, QUERY_DEFAULT_BUCKET_STATISTIC_TEMPLATE)
                : buildQueryHistogram(db, table, params.sampleRatio(), params.bucketNum(), mostCommonValues,
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

    private String buildCollectMCV(Database database, Table table, Long topN, String columnName) {
        VelocityContext context = new VelocityContext();
        context.put("columnName", StatisticUtils.quoting(table, columnName));
        context.put("catalogName", catalogName);
        context.put("dbName", database.getOriginName());
        context.put("tableName", table.getName());
        context.put("topN", topN);

        return build(context, COLLECT_MCV_STATISTIC_TEMPLATE);
    }

    private String buildCollectHistogram(Database database, Table table, double sampleRatio,
                                         Long bucketNum, Map<String, String> mostCommonValues, String columnName,
                                         Type columnType) {
        VelocityContext context = buildBaseContext(database, table, catalogName, columnName);
        putMcv(context, mostCommonValues);
        putMcvExclude(context, mostCommonValues, StatisticUtils.quoting(table, columnName), columnType);

        context.put("bucketNum", bucketNum);
        context.put("sampleRatio", sampleRatio);
        context.put("totalRows", Config.histogram_max_sample_row_count);

        return buildInsertIntoHistogramStatistics(build(context, COLLECT_HISTOGRAM_STATISTIC_TEMPLATE));
    }

    private String buildQueryHistogram(Database database, Table table, double sampleRatio, Long bucketNum,
                                       Map<String, String> mostCommonValues, String columnName, Type columnType) {
        VelocityContext context = buildBaseContext(database, table, catalogName, columnName);
        putMcvExclude(context, mostCommonValues, StatisticUtils.quoting(table, columnName), columnType);

        context.put("bucketNum", bucketNum);
        context.put("sampleRatio", sampleRatio);
        context.put("totalRows", Config.histogram_max_sample_row_count);
        return build(context, QUERY_HISTOGRAM_STATISTIC_TEMPLATE);
    }

    private String buildInsertIntoHistogramStatistics(String query) {
        List<String> targetColumnNames = buildStatsTargetColumnNames(EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME);
        String columnNames = "(" + String.join(", ", targetColumnNames) + ")";
        return "INSERT INTO " +
                EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME +
                columnNames +
                " " +
                query;
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
        row.add(nowFn());
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

}
