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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.optimizer.statistics.HistogramUtils;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.thrift.TStatisticData;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.apache.velocity.VelocityContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static com.starrocks.statistic.StatsConstants.HISTOGRAM_STATISTICS_TABLE_NAME;

public class HistogramStatisticsUtilsTest {
    // Touches every key buildDefaultBucketSql populates: if one stops being set, Velocity leaks the
    // literal "$key" into the rendered SQL and the assertions below catch it.
    private static final String IDENTITY_TEMPLATE =
            "SELECT $tableId, '$columnNameStr', $dbId, '$catalogName', '$tableUUID', $bucketExpr, $mcv" +
                    " FROM `$dbName`.`$tableName`$sampleClause$randFilter";

    @Test
    public void testMcvJsonEscaping() {
        String key = "a\"b\\c'd";
        String value = "1\"2\\3'";
        String mcvJson = HistogramStatisticsUtils.buildMcvJson(ImmutableMap.of(key, value));

        JsonArray mcvArray = JsonParser.parseString(mcvJson).getAsJsonArray();
        Assertions.assertEquals(key, mcvArray.get(0).getAsJsonArray().get(0).getAsString());
        Assertions.assertEquals(value, mcvArray.get(0).getAsJsonArray().get(1).getAsString());

        StatementBase statement = SqlParser.parseSingleStatement(
                "select " + HistogramStatisticsUtils.quoteSqlString(mcvJson), SqlModeHelper.MODE_DEFAULT);
        SelectRelation selectRelation = (SelectRelation) ((QueryStatement) statement).getQueryRelation();
        StringLiteral literal = (StringLiteral) selectRelation.getSelectList().getItems().get(0).getExpr();
        Assertions.assertEquals(mcvJson, literal.getStringValue());

        String numericMcvJson = HistogramStatisticsUtils.buildMcvJson(ImmutableMap.of(key, "10"));
        Map<String, Long> convertedMcv =
                HistogramUtils.convertMCV("{\"buckets\":[],\"mcv\":" + numericMcvJson + "}");
        Assertions.assertEquals(10L, convertedMcv.get(key));
    }

    @Test
    public void testGetSingleHistogramResult() throws Exception {
        TStatisticData result = new TStatisticData();
        result.histogram = "[]";
        Assertions.assertSame(result, HistogramStatisticsUtils.getSingleHistogramResult(
                Lists.newArrayList(result), "v2", "histogram"));

        TStatisticData emptyResult = new TStatisticData();
        Assertions.assertSame(emptyResult, HistogramStatisticsUtils.getSingleHistogramResult(
                Lists.newArrayList(emptyResult), "v2", "external histogram"));
        Assertions.assertNull(emptyResult.histogram);

        DdlException missingHistogramException = Assertions.assertThrows(DdlException.class,
                () -> HistogramStatisticsUtils.getSingleHistogramResult(
                        Lists.newArrayList(), "v2", "histogram"));
        Assertions.assertEquals("Expected exactly one histogram result for column v2, but got 0",
                missingHistogramException.getMessage());
    }

    @Test
    public void testEmptyBucketsPreserveLegacyNull() {
        Assertions.assertInstanceOf(NullLiteral.class, HistogramStatisticsUtils.buildBucketsLiteral(null));
        Assertions.assertEquals("NULL", HistogramStatisticsUtils.buildBucketsSql(""));
        Assertions.assertInstanceOf(NullLiteral.class, HistogramStatisticsUtils.buildBucketsLiteral("[]"));
        Assertions.assertEquals("NULL", HistogramStatisticsUtils.buildBucketsSql("[]"));
        Assertions.assertEquals("[]", HistogramStatisticsUtils.normalizeBucketsForHll(null));

        String buckets = "[[\"1\",\"2\",\"3\",\"4\"]]";
        StringLiteral literal = (StringLiteral) HistogramStatisticsUtils.buildBucketsLiteral(buckets);
        Assertions.assertEquals(buckets, literal.getStringValue());
        Assertions.assertEquals(HistogramStatisticsUtils.quoteSqlString(buckets),
                HistogramStatisticsUtils.buildBucketsSql(buckets));
        Assertions.assertEquals(buckets, HistogramStatisticsUtils.normalizeBucketsForHll(buckets));
    }

    @Test
    public void testCreateInsertStmtClonesRows() {
        List<List<Expr>> rows = Lists.newArrayList();
        rows.add(Lists.newArrayList(new StringLiteral("value")));
        String sql = HistogramStatisticsUtils.buildBatchInsertPrefix(HISTOGRAM_STATISTICS_TABLE_NAME) + "('value');";

        InsertStmt first = (InsertStmt) HistogramStatisticsUtils.createInsertStmt(
                HISTOGRAM_STATISTICS_TABLE_NAME, rows, sql);
        InsertStmt second = (InsertStmt) HistogramStatisticsUtils.createInsertStmt(
                HISTOGRAM_STATISTICS_TABLE_NAME, rows, sql);
        ValuesRelation firstValues = (ValuesRelation) first.getQueryStatement().getQueryRelation();
        ValuesRelation secondValues = (ValuesRelation) second.getQueryStatement().getQueryRelation();

        Assertions.assertNotSame(first, second);
        Assertions.assertNotSame(rows.get(0).get(0), firstValues.getRows().get(0).get(0));
        Assertions.assertNotSame(firstValues.getRows().get(0).get(0), secondValues.getRows().get(0).get(0));
        Assertions.assertEquals(HistogramStatisticsUtils.buildStatsTargetColumnNames(HISTOGRAM_STATISTICS_TABLE_NAME),
                first.getTargetColumnNames());
        Assertions.assertEquals(sql, first.getOrigStmt().getOrigStmt());
    }

    @Test
    public void testMostCommonValuesScaleSampledCountsBackToFullTable() {
        List<TStatisticData> mcv = Lists.newArrayList(mcvRow("a", "10"), mcvRow("b", "7"));

        Map<String, String> sampled = HistogramStatisticsUtils.buildMostCommonValues(mcv, 0.1);
        Assertions.assertEquals(ImmutableMap.of("a", "100", "b", "70"), sampled);

        // A ratio of 1.0 means the query was unsampled, so the counts must pass through untouched.
        Map<String, String> unsampled = HistogramStatisticsUtils.buildMostCommonValues(mcv, 1.0);
        Assertions.assertEquals(ImmutableMap.of("a", "10", "b", "7"), unsampled);
        Assertions.assertEquals(unsampled, HistogramStatisticsUtils.buildMostCommonValues(mcv, 0.0));
    }

    @Test
    public void testDefaultBucketExprSubtractsMcvsFromCount() {
        Map<String, String> mcv = ImmutableMap.of("a", "10", "b", "7");

        Assertions.assertEquals(
                "concat('[[\"Infinity\",\"Infinity\",', " +
                        "cast(cast(greatest(0, count(`v7`) - 17) as bigint) as varchar), ',0]]')",
                HistogramStatisticsUtils.buildDefaultBucketExpr("`v7`", 1.0, mcv));

        // Under a sample the count is divided back up first, which is why the bigint cast is needed.
        Assertions.assertEquals(
                "concat('[[\"Infinity\",\"Infinity\",', " +
                        "cast(cast(greatest(0, count(`v7`) / cast(0.1 as double) - 17) as bigint) as varchar), " +
                        "',0]]')",
                HistogramStatisticsUtils.buildDefaultBucketExpr("`v7`", 0.1, mcv));

        Assertions.assertEquals(
                "concat('[[\"Infinity\",\"Infinity\",', " +
                        "cast(cast(greatest(0, count(`v7`) - 0) as bigint) as varchar), ',0]]')",
                HistogramStatisticsUtils.buildDefaultBucketExpr("`v7`", 1.0, ImmutableMap.of()));
    }

    @Test
    public void testSampleRatioRendersAsPlainDecimalLiteral() {
        Assertions.assertEquals("0.1", HistogramStatisticsUtils.formatSampleRatio(0.1));
        Assertions.assertEquals("0.5", HistogramStatisticsUtils.formatSampleRatio(0.50));
        // Sub-1% ratios must not come out in scientific notation - the SQL parser cannot consume it.
        Assertions.assertEquals("0.0000001", HistogramStatisticsUtils.formatSampleRatio(0.0000001));
    }

    @Test
    public void testMcvExcludeQuotesOnlyStringLikeValues() {
        Map<String, String> mcv = ImmutableMap.of("a", "10", "b", "7");

        Assertions.assertEquals(" and `v2` not in (a,b)", mcvExclude(mcv, IntegerType.BIGINT));
        Assertions.assertEquals(" and `v7` not in (\"a\",\"b\")", mcvExclude(mcv, VarcharType.VARCHAR, "`v7`"));
        Assertions.assertEquals(" and `v4` not in (\"a\",\"b\")", mcvExclude(mcv, DateType.DATE, "`v4`"));

        // No MCVs means nothing to exclude, and the slot still has to be filled for the template.
        Assertions.assertEquals("", mcvExclude(ImmutableMap.of(), IntegerType.BIGINT));
    }

    private static TStatisticData mcvRow(String columnValue, String count) {
        TStatisticData row = new TStatisticData();
        row.columnName = columnValue;
        row.histogram = count;
        return row;
    }

    private static String mcvExclude(Map<String, String> mostCommonValues, Type columnType) {
        return mcvExclude(mostCommonValues, columnType, "`v2`");
    }

    private static String mcvExclude(Map<String, String> mostCommonValues, Type columnType, String quotedColumnName) {
        VelocityContext context = new VelocityContext();
        HistogramStatisticsUtils.putMcvExclude(context, mostCommonValues, quotedColumnName, columnType);
        return (String) context.get("MCVExclude");
    }

    @Test
    public void testBaseContextCarriesEveryIdentityKey() {
        VelocityContext context = HistogramStatisticsUtils.buildBaseContext(
                testDb(), testTable(), "default_catalog", "v1");

        Assertions.assertEquals(2L, context.get("tableId"));
        Assertions.assertEquals(1L, context.get("dbId"));
        Assertions.assertEquals("default_catalog", context.get("catalogName"));
        Assertions.assertEquals(testDb().getOriginName(), context.get("dbName"));
        Assertions.assertEquals("t0", context.get("tableName"));
        Assertions.assertEquals("`v1`", context.get("columnName"));
        Assertions.assertEquals("v1", context.get("columnNameStr"));
        // Only the external templates read tableUUID, but an internal table must still resolve one:
        // Table.getUUID() falls back to the table id.
        Assertions.assertEquals(StatisticUtils.hashTableUuidForPkStorage("2"), context.get("tableUUID"));
    }

    @Test
    public void testDefaultBucketSqlLeavesAnUnsampledScanUnfiltered() {
        String sql = HistogramStatisticsUtils.buildDefaultBucketSql(
                testDb(), testTable(), "hive0", "v1", ImmutableMap.of("a", "10"), 1.0, IDENTITY_TEMPLATE);

        Assertions.assertEquals(
                "SELECT 2, 'v1', 1, 'hive0', '" + StatisticUtils.hashTableUuidForPkStorage("2") + "', " +
                        "concat('[[\"Infinity\",\"Infinity\",', " +
                        "cast(cast(greatest(0, count(`v1`) - 10) as bigint) as varchar), ',0]]'), " +
                        "'[[\"a\",\"10\"]]' FROM `test`.`t0`",
                sql);
    }

    @Test
    public void testDefaultBucketSqlScalesAndFiltersASampledScan() {
        boolean originalUseTableSample = Config.enable_use_table_sample_collect_statistics;
        try {
            Config.enable_use_table_sample_collect_statistics = true;
            Assertions.assertTrue(sampledDefaultBucketSql().endsWith("FROM `test`.`t0` SAMPLE('percent'='10')"),
                    sampledDefaultBucketSql());

            Config.enable_use_table_sample_collect_statistics = false;
            Assertions.assertTrue(sampledDefaultBucketSql().endsWith("FROM `test`.`t0` WHERE rand() <= 0.1"),
                    sampledDefaultBucketSql());

            // Either way the bucket count is divided back up to a full-table estimate.
            Assertions.assertTrue(
                    sampledDefaultBucketSql().contains("count(`v1`) / cast(0.1 as double) - 10"),
                    sampledDefaultBucketSql());
        } finally {
            Config.enable_use_table_sample_collect_statistics = originalUseTableSample;
        }
    }

    private static String sampledDefaultBucketSql() {
        return HistogramStatisticsUtils.buildDefaultBucketSql(
                testDb(), testTable(), "hive0", "v1", ImmutableMap.of("a", "10"), 0.1, IDENTITY_TEMPLATE);
    }

    private static Database testDb() {
        return new Database(1, "test");
    }

    private static OlapTable testTable() {
        OlapTable table = new OlapTable();
        table.setId(2);
        table.setName("t0");
        return table;
    }
}
