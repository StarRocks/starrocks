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
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.SqlUtils;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.OriginStatement;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TStatisticData;
import com.starrocks.type.Type;
import org.apache.commons.lang.StringUtils;
import org.apache.velocity.VelocityContext;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class HistogramStatisticsUtils {
    private HistogramStatisticsUtils() {
    }

    /**
     * Identity context for one analyzed column: every key describing "which catalog/db/table/column
     * is this". Deliberately a superset - each SQL template picks the keys it needs and ignores the
     * rest, which is what lets the native and external jobs share one context builder even though
     * their templates select different identifying columns.
     */
    static VelocityContext buildBaseContext(Database database, Table table, String catalogName, String columnName) {
        VelocityContext context = new VelocityContext();
        context.put("tableId", table.getId());
        context.put("tableUUID", StatisticUtils.hashTableUuidForPkStorage(table.getUUID()));
        context.put("columnName", StatisticUtils.quoting(table, columnName));
        context.put("columnNameStr", SqlUtils.escapeSqlString(columnName));
        context.put("dbId", database.getId());
        context.put("catalogName", catalogName);
        context.put("dbName", database.getOriginName());
        context.put("tableName", table.getName());
        return context;
    }

    /**
     * SQL for the placeholder-bucket form of a histogram, used when the histogram() aggregate is
     * skipped for a column. Serves both the INSERT and the plain-query shape of both jobs - the
     * caller supplies the template, and wraps the result in an INSERT prefix if it needs one.
     */
    static String buildDefaultBucketSql(Database database, Table table, String catalogName, String columnName,
                                        Map<String, String> mostCommonValues, double sampleRatio, String template) {
        VelocityContext context = buildBaseContext(database, table, catalogName, columnName);
        // The query templates reference neither of these; only the INSERT ones do.
        putMcv(context, mostCommonValues);
        putDefaultBucketSampleClause(context, sampleRatio);
        context.put("bucketExpr",
                buildDefaultBucketExpr(StatisticUtils.quoting(table, columnName), sampleRatio, mostCommonValues));

        return StatisticsCollectJob.build(context, template);
    }

    static void putMcv(VelocityContext context, Map<String, String> mostCommonValues) {
        String mcvJson = buildMcvJson(mostCommonValues);
        context.put("mcv", mcvJson == null ? "NULL" : quoteSqlString(mcvJson));
    }

    // The default-bucket templates splice $sampleClause$randFilter straight onto "FROM `db`.`table`"
    // and have no WHERE of their own, so the keyword travels inside the substituted value here. An
    // unsampled scan yields empty strings for both - the external templates never even reference them.
    private static void putDefaultBucketSampleClause(VelocityContext context, double sampleRatio) {
        if (!isSampled(sampleRatio)) {
            context.put("sampleClause", "");
            context.put("randFilter", "");
        } else if (Config.enable_use_table_sample_collect_statistics) {
            context.put("sampleClause", " SAMPLE('percent'='" + formatSamplePercent(sampleRatio) + "')");
            context.put("randFilter", "");
        } else {
            context.put("sampleClause", "");
            context.put("randFilter", " WHERE rand() <= " + formatSampleRatio(sampleRatio));
        }
    }

    /**
     * Turn the raw MCV query result into a column-value -> count map. Counts collected under a
     * sample are scaled back up to full-table counts; pass a ratio of 1.0 for an unsampled query.
     */
    static Map<String, String> buildMostCommonValues(List<TStatisticData> mcv, double sampleRatio) {
        Map<String, String> mostCommonValues = new HashMap<>();
        for (TStatisticData tStatisticData : mcv) {
            if (isSampled(sampleRatio)) {
                long count = Long.parseLong(tStatisticData.histogram);
                count = (long) (1.0 * count / sampleRatio);
                mostCommonValues.put(tStatisticData.columnName, String.valueOf(count));
            } else {
                mostCommonValues.put(tStatisticData.columnName, tStatisticData.histogram);
            }
        }
        return mostCommonValues;
    }

    /**
     * MCVs are stored separately from the buckets, so the bucket query has to exclude them to avoid
     * double counting. Renders that exclusion predicate into the $MCVExclude template slot.
     */
    static void putMcvExclude(VelocityContext context, Map<String, String> mostCommonValues,
                              String quotedColumnName, Type columnType) {
        if (mostCommonValues.isEmpty()) {
            context.put("MCVExclude", "");
            return;
        }

        if (columnType.getPrimitiveType().isDateType() || columnType.getPrimitiveType().isCharFamily()) {
            context.put("MCVExclude", " and " + quotedColumnName + " not in (\"" +
                    Joiner.on("\",\"").join(mostCommonValues.keySet()) + "\")");
        } else {
            context.put("MCVExclude", " and " + quotedColumnName + " not in (" +
                    Joiner.on(",").join(mostCommonValues.keySet()) + ")");
        }
    }

    /**
     * Bucket expression used when the histogram() aggregate is skipped for a column: a single
     * placeholder bucket holding "every value except the MCVs", so Histogram.getTotalRows() still
     * reflects the column's real cardinality instead of reading as empty.
     */
    static String buildDefaultBucketExpr(String quotedColumnName, double sampleRatio,
                                         Map<String, String> mostCommonValues) {
        String countExpr = "count(" + quotedColumnName + ")";
        if (isSampled(sampleRatio)) {
            countExpr += " / cast(" + formatSampleRatio(sampleRatio) + " as double)";
        }

        long mcvSum = mostCommonValues.values().stream().mapToLong(Long::parseLong).sum();
        String nonMcvExpr = "greatest(0, " + countExpr + " - " + mcvSum + ")";
        // The bigint cast keeps the sampled (divided, hence double) form from rendering as a
        // decimal; on the unsampled form it is a no-op, since count() is already a bigint.
        return "concat('[[\"Infinity\",\"Infinity\",', cast(cast(" + nonMcvExpr +
                " as bigint) as varchar), ',0]]')";
    }

    /**
     * Render a sample ratio as a plain decimal SQL literal: no trailing zeros, and no scientific
     * notation for sub-1% ratios (which the SQL parser cannot consume).
     */
    static String formatSampleRatio(double sampleRatio) {
        return BigDecimal.valueOf(sampleRatio).stripTrailingZeros().toPlainString();
    }

    /**
     * Convert a sample ratio in (0, 1) into a percent string in (0, 100) for the SAMPLE('percent'=...)
     * clause. Uses BigDecimal to avoid both binary float noise (e.g. 0.49999999999999994) and
     * truncation to 0 for sub-1% ratios on very large tables (which used to produce the illegal
     * SAMPLE('percent'='0')).
     */
    @VisibleForTesting
    static String formatSamplePercent(double sampleRatio) {
        BigDecimal percent = BigDecimal.valueOf(sampleRatio).multiply(BigDecimal.valueOf(100));
        // Drop trailing zeros so integral percents stay clean (e.g. "50" not "50.00"), and avoid
        // scientific notation that the SQL parser cannot consume.
        return percent.stripTrailingZeros().toPlainString();
    }

    private static boolean isSampled(double sampleRatio) {
        return sampleRatio > 0.0 && sampleRatio < 1.0;
    }

    static String buildMcvJson(Map<String, String> mostCommonValues) {
        if (mostCommonValues.isEmpty()) {
            return null;
        }

        JsonArray mcvArray = new JsonArray();
        for (Map.Entry<String, String> entry : mostCommonValues.entrySet()) {
            JsonArray mcvEntry = new JsonArray();
            mcvEntry.add(entry.getKey());
            mcvEntry.add(entry.getValue());
            mcvArray.add(mcvEntry);
        }
        return mcvArray.toString();
    }

    static TStatisticData getSingleHistogramResult(
            List<TStatisticData> results, String columnName, String resultDescription) throws DdlException {
        if (results.size() != 1) {
            throw new DdlException("Expected exactly one " + resultDescription + " result for column " + columnName +
                    ", but got " + results.size());
        }
        return results.get(0);
    }

    static Expr buildBucketsLiteral(String buckets) {
        return isEmptyBuckets(buckets) ? new NullLiteral() : new StringLiteral(buckets);
    }

    static String buildBucketsSql(String buckets) {
        return isEmptyBuckets(buckets) ? "NULL" : quoteSqlString(buckets);
    }

    static String normalizeBucketsForHll(String buckets) {
        return StringUtils.isEmpty(buckets) ? "[]" : buckets;
    }

    private static boolean isEmptyBuckets(String buckets) {
        return StringUtils.isEmpty(buckets) || "[]".equals(buckets);
    }

    static String quoteSqlString(String value) {
        return "'" + SqlUtils.escapeSqlString(value) + "'";
    }

    static long utf8Length(String value) {
        return value.getBytes(StandardCharsets.UTF_8).length;
    }

    static long batchInsertPrefixSize(String tableName) {
        return utf8Length(buildBatchInsertPrefix(tableName)) + 1;
    }

    static String buildBatchInsertPrefix(String tableName) {
        return "INSERT INTO " + StatsConstants.STATISTICS_DB_NAME + "." + tableName +
                buildStatsTargetColumnListSql(tableName) + " VALUES ";
    }

    static StatementBase createInsertStmt(String tableName, List<List<Expr>> rowsBuffer, String sql) {
        List<List<Expr>> rows = new ArrayList<>();
        for (List<Expr> row : rowsBuffer) {
            rows.add(ExprUtils.cloneList(row));
        }

        List<String> targetColumnNames = buildStatsTargetColumnNames(tableName);
        QueryStatement queryStatement = new QueryStatement(new ValuesRelation(rows, targetColumnNames));
        TableRef tableRef = new TableRef(
                QualifiedName.of(Lists.newArrayList(StatsConstants.STATISTICS_DB_NAME, tableName)),
                null, NodePosition.ZERO);
        InsertStmt insertStmt = new InsertStmt(tableRef, queryStatement);
        insertStmt.setTargetColumnNames(targetColumnNames);
        insertStmt.setOrigStmt(new OriginStatement(sql, 0));
        return insertStmt;
    }

    /** The parenthesised target column list of a statistics table, as it appears in an INSERT. */
    static String buildStatsTargetColumnListSql(String tableName) {
        return "(" + String.join(", ", buildStatsTargetColumnNames(tableName)) + ")";
    }

    static List<String> buildStatsTargetColumnNames(String tableName) {
        return StatisticUtils.buildStatsColumnDef(tableName).stream()
                .map(ColumnDef::getName)
                .collect(Collectors.toList());
    }
}
