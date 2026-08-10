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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static com.starrocks.statistic.StatsConstants.HISTOGRAM_STATISTICS_TABLE_NAME;

public class HistogramStatisticsUtilsTest {
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
}
