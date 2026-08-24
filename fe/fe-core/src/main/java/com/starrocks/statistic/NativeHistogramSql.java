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
import com.starrocks.type.Type;
import org.apache.velocity.VelocityContext;

import java.util.Map;

import static com.starrocks.statistic.HistogramStatisticsUtils.buildBaseContext;
import static com.starrocks.statistic.HistogramStatisticsUtils.formatSamplePercent;
import static com.starrocks.statistic.HistogramStatisticsUtils.putMcvExclude;

/**
 * The native histogram SQL both collection strategies need: the most-common-values query, the
 * histogram() aggregate expression, and the bucket-boundary query the HLL mode derives its buckets
 * from. Strategy-specific SQL lives in {@link HistogramLegacyTarget} and {@link HistogramBatchedTarget}.
 */
final class NativeHistogramSql {
    private static final String HISTOGRAM_FUNCTION_WITHOUT_NDV_TEMPLATE =
            "histogram(`column_key`, cast($bucketNum as int), cast($sampleRatio as double))";

    private static final String HISTOGRAM_FUNCTION_WITH_NDV_TEMPLATE =
            "histogram(`column_key`, cast($bucketNum as int), cast($sampleRatio as double), '$ndvEstimator')";

    private static final String BUCKET_BOUNDARIES_TEMPLATE =
            "SELECT cast(" + StatsConstants.STATISTIC_HISTOGRAM_VERSION + " as INT) as version," +
                    " cast($dbId  as BIGINT), cast($tableId as BIGINT), '$columnNameStr'," +
                    " $histogramFunction" +
                    " FROM (SELECT $columnName as column_key FROM `$dbName`.`$tableName` where rand() <= $sampleRatio" +
                    " and $columnName is not null $MCVExclude" +
                    " ORDER BY $columnName LIMIT $totalRows) t";

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

    private NativeHistogramSql() {
    }

    static String buildMcvQuery(Database database, Table table, Long topN, String columnName, double sampleRatio) {
        VelocityContext context = new VelocityContext();
        context.put("tableId", table.getId());
        context.put("columnName", StatisticUtils.quoting(table, columnName));
        context.put("dbId", database.getId());

        context.put("dbName", database.getOriginName());
        context.put("tableName", table.getName());
        context.put("topN", topN);

        if (sampleRatio > 0.0 && sampleRatio < 1.0) {
            String sample = String.format("SAMPLE('percent'='%s')", formatSamplePercent(sampleRatio));
            context.put("sampleClause", sample);
        } else {
            context.put("sampleClause", "");
        }

        return StatisticsCollectJob.build(context, MCV_STATISTIC_TEMPLATE);
    }

    static String buildHistogramFunction(Database database, Table table, String catalogName, double sampleRatio,
                                         Long bucketNum, String columnName, boolean withSampleNdv) {
        VelocityContext context = buildBaseContext(database, table, catalogName, columnName);
        context.put("bucketNum", bucketNum);
        context.put("sampleRatio", sampleRatio);
        if (withSampleNdv) {
            context.put("ndvEstimator", Config.statistics_sample_ndv_estimator);
            return StatisticsCollectJob.build(context, HISTOGRAM_FUNCTION_WITH_NDV_TEMPLATE);
        } else {
            return StatisticsCollectJob.build(context, HISTOGRAM_FUNCTION_WITHOUT_NDV_TEMPLATE);
        }
    }

    /**
     * Query whose single row carries the bucket boundaries. Both strategies feed it to the HLL mode,
     * which needs the boundaries in hand before it can name them; only the way they execute it differs.
     */
    static String buildBucketBoundariesQuery(Database database, Table table, String catalogName, double sampleRatio,
                                             Long bucketNum, Map<String, String> mostCommonValues, String columnName,
                                             Type columnType) {
        VelocityContext context = buildBaseContext(database, table, catalogName, columnName);
        context.put("histogramFunction",
                buildHistogramFunction(database, table, catalogName, sampleRatio, bucketNum, columnName, false));
        context.put("sampleRatio", sampleRatio);
        context.put("totalRows", Config.histogram_max_sample_row_count);
        putMcvExclude(context, mostCommonValues, StatisticUtils.quoting(table, columnName), columnType);

        return StatisticsCollectJob.build(context, BUCKET_BOUNDARIES_TEMPLATE);
    }

    // TODO: use table sample by default and remove this switch
    static void addSampleClauseToContext(VelocityContext context, double sampleRatio) {
        if (Config.enable_use_table_sample_collect_statistics && sampleRatio > 0.0 && sampleRatio < 1.0) {
            context.put("sampleClause", String.format("SAMPLE('percent'='%s')", formatSamplePercent(sampleRatio)));
            context.put("randFilter", "TRUE");
        } else {
            context.put("sampleClause", "");
            context.put("randFilter", String.format(" rand() <= %f", sampleRatio));
        }
    }
}
