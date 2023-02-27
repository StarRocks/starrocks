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

package com.starrocks.sql.analyzer;

import com.starrocks.analysis.StringLiteral;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.getConnectContext;

public class AnalyzeFunctionTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @Test
    public void testFunctionWithoutDb() {
        StarRocksAssert starRocksAssert = AnalyzeTestUtil.getStarRocksAssert();
        starRocksAssert.withoutUseDatabase();
        analyzeFail("select query_id()", "No matching function with signature: query_id()");
        starRocksAssert.useDatabase(AnalyzeTestUtil.getDbName());
    }

    @Test
    public void testSingle() {
        analyzeFail("select sum() from t0", "No matching function with signature: sum()");
        analyzeFail("select now(*) from t0");

        analyzeSuccess("SHOW FULL BUILTIN FUNCTIONS FROM `testDb1` LIKE '%year%'");
    }

    @Test
    public void testDateTrunc() {
        analyzeSuccess("select date_trunc(\"year\", ti) from tall");
        analyzeSuccess("select date_trunc(\"month\", ti) from tall");
        analyzeSuccess("select date_trunc(\"day\", ti) from tall");
        analyzeSuccess("select date_trunc(\"week\", ti) from tall");
        analyzeSuccess("select date_trunc(\"quarter\", ti) from tall");

        analyzeFail("select date_trunc(\"hour\", ti) from tall",
                "date_trunc function can't support argument other than year|quarter|month|week|day");
        analyzeFail("select date_trunc(\"minute\", ti) from tall",
                "date_trunc function can't support argument other than year|quarter|month|week|day");
        analyzeFail("select date_trunc(\"second\", ti) from tall",
                "date_trunc function can't support argument other than year|quarter|month|week|day");
        analyzeFail("select date_trunc(\"foo\", ti) from tall",
                "date_trunc function can't support argument other than year|quarter|month|week|day");

        analyzeSuccess("select date_trunc(\"year\", th) from tall");
        analyzeSuccess("select date_trunc(\"month\", th) from tall");
        analyzeSuccess("select date_trunc(\"day\", th) from tall");
        analyzeSuccess("select date_trunc(\"week\", th) from tall");
        analyzeSuccess("select date_trunc(\"quarter\", th) from tall");
        analyzeSuccess("select date_trunc(\"hour\", th) from tall");
        analyzeSuccess("select date_trunc(\"minute\", th) from tall");
        analyzeSuccess("select date_trunc(\"second\", th) from tall");

        analyzeFail("select date_trunc(\"foo\", th) from tall",
                "date_trunc function can't support argument other than year|quarter|month|week|day|hour|minute|second");

        analyzeFail("select date_trunc(ta, th) from tall",
                "date_trunc requires first parameter must be a string constant");
    }

    @Test
    public void testTimeSlice() {
        analyzeSuccess("select time_slice(th, interval 1 year) from tall");
        analyzeSuccess("select time_slice(th, interval 1 year, ceil) from tall");
        analyzeSuccess("select time_slice(th, interval 1 month) from tall");
        analyzeSuccess("select time_slice(th, interval 1 month, ceil) from tall");
        analyzeSuccess("select time_slice(th, interval 1 day) from tall");
        analyzeSuccess("select time_slice(th, interval 1 day, ceil) from tall");
        analyzeSuccess("select time_slice(th, interval 1 week) from tall");
        analyzeSuccess("select time_slice(th, interval 1 week, ceil) from tall");
        analyzeSuccess("select time_slice(th, interval 1 quarter) from tall");
        analyzeSuccess("select time_slice(th, interval 1 quarter, ceil) from tall");
        analyzeSuccess("select time_slice(th, interval 1 hour) from tall");
        analyzeSuccess("select time_slice(th, interval 1 hour, ceil) from tall");
        analyzeSuccess("select time_slice(th, interval 1 minute) from tall");
        analyzeSuccess("select time_slice(th, interval 1 minute, ceil) from tall");
        analyzeSuccess("select time_slice(th, interval 1 second) from tall");
        analyzeSuccess("select time_slice(th, interval 1 second, ceil) from tall");

        analyzeSuccess("select date_slice(ti, interval 1 year) from tall");
        analyzeSuccess("select date_slice(ti, interval 1 year, ceil) from tall");
        analyzeSuccess("select date_slice(ti, interval 1 month) from tall");
        analyzeSuccess("select date_slice(ti, interval 1 month, ceil) from tall");
        analyzeSuccess("select date_slice(ti, interval 1 day) from tall");
        analyzeSuccess("select date_slice(ti, interval 1 day, ceil) from tall");
        analyzeSuccess("select date_slice(ti, interval 1 week) from tall");
        analyzeSuccess("select date_slice(ti, interval 1 week, ceil) from tall");
        analyzeSuccess("select date_slice(ti, interval 1 quarter) from tall");
        analyzeSuccess("select date_slice(ti, interval 1 quarter, ceil) from tall");

        analyzeFail("select time_slice(ta, th) from tall",
                "time_slice requires second parameter must be a constant interval");

        analyzeFail("select time_slice(NULL, NULL) from tall",
                "time_slice requires second parameter must be a constant interval");

        analyzeFail("select time_slice(ta, -1) from tall",
                "time_slice requires second parameter must be greater than 0");

        analyzeFail("select time_slice(th, interval 1 second, FCEILK) from tall",
                "Incorrect type/value of arguments in expr 'time_slice'");

        analyzeFail("select time_slice('2023-12-31 03:12:04',interval -3.2 day)",
                "time_slice requires second parameter must be a constant interval");

        analyzeFail("select date_slice('2023-12-31',interval -3.2 day)",
                "date_slice requires second parameter must be a constant interval");
    }

    @Test
    public void testApproxCountDistinctWithMetricType() {
        analyzeFail("select approx_count_distinct(h1) from test_object");
        analyzeFail("select ndv(h1) from test_object");
        analyzeFail("select approx_count_distinct(b1) from test_object");
        analyzeFail("select ndv(b1) from test_object");
    }

    @Test
    public void testMatrixTypeCast() {
        analyzeFail("select trim(b1) from test_object");
        analyzeFail("select trim(h1) from test_object");
        analyzeFail("select trim(p1) from test_object");
    }

    @Test
    public void testToBitMap() {
        analyzeFail("select to_bitmap(b1) from test_object", "No matching function with signature: to_bitmap(bitmap)");
        analyzeFail("select to_bitmap(h1) from test_object", "No matching function with signature: to_bitmap(hll)");

        List<String> successColumns = Arrays.asList("ta", "tb", "tc", "td", "te", "tf", "tg", "th", "ti", "tj");
        for (String column : successColumns) {
            analyzeSuccess("select to_bitmap(" + column + ") from tall");
        }
    }

    @Test
    public void testTimestampArithmeticExpr() {
        QueryStatement queryStatement =
                (QueryStatement) analyzeSuccess("select date_add('2022-01-01', interval 2 day)");
        Assert.assertEquals("SELECT date_add('2022-01-01 00:00:00', INTERVAL 2 DAY)",
                AstToStringBuilder.toString(queryStatement.getQueryRelation()));

        queryStatement = (QueryStatement) analyzeSuccess("select date_add('2022-01-01 00:00:00', interval 2 day)");
        Assert.assertEquals("SELECT date_add('2022-01-01 00:00:00', INTERVAL 2 DAY)",
                AstToStringBuilder.toString(queryStatement.getQueryRelation()));

        queryStatement = (QueryStatement) analyzeSuccess("select date_add('2022-01-01', interval 2 minute)");
        Assert.assertEquals("SELECT date_add('2022-01-01 00:00:00', INTERVAL 2 MINUTE)",
                AstToStringBuilder.toString(queryStatement.getQueryRelation()));

        queryStatement = (QueryStatement) analyzeSuccess("select date_add('2022-01-01 00:00:00', interval 2 minute)");
        Assert.assertEquals("SELECT date_add('2022-01-01 00:00:00', INTERVAL 2 MINUTE)",
                AstToStringBuilder.toString(queryStatement.getQueryRelation()));

        queryStatement = (QueryStatement) analyzeSuccess("select timestampadd(day, 2, '2022-01-01')");
        Assert.assertEquals("SELECT timestampadd(DAY, 2, '2022-01-01 00:00:00')",
                AstToStringBuilder.toString(queryStatement.getQueryRelation()));

        queryStatement = (QueryStatement) analyzeSuccess("select date_add('2022-01-01', 2)");
        Assert.assertEquals("SELECT date_add('2022-01-01 00:00:00', INTERVAL 2 DAY)",
                AstToStringBuilder.toString(queryStatement.getQueryRelation()));

        queryStatement = (QueryStatement) analyzeSuccess("select date_add('2022-01-01', interval 2 year)");
        Assert.assertEquals("SELECT date_add('2022-01-01 00:00:00', INTERVAL 2 YEAR)",
                AstToStringBuilder.toString(queryStatement.getQueryRelation()));

        queryStatement = (QueryStatement) analyzeSuccess("select date_sub('2022-01-01', interval 2 year)");
        Assert.assertEquals("SELECT date_sub('2022-01-01 00:00:00', INTERVAL 2 YEAR)",
                AstToStringBuilder.toString(queryStatement.getQueryRelation()));

        queryStatement = (QueryStatement) analyzeSuccess("select subdate('2022-01-01', interval 2 year)");
        Assert.assertEquals("SELECT subdate('2022-01-01 00:00:00', INTERVAL 2 YEAR)",
                AstToStringBuilder.toString(queryStatement.getQueryRelation()));

        queryStatement = (QueryStatement) analyzeSuccess("select date_add('2022-01-01', interval 2 year)");
        Assert.assertEquals("SELECT date_add('2022-01-01 00:00:00', INTERVAL 2 YEAR)",
                AstToStringBuilder.toString(queryStatement.getQueryRelation()));

        queryStatement = (QueryStatement) analyzeSuccess("select timestampdiff(day, '2020-01-01', '2020-01-03')");
        Assert.assertEquals("SELECT timestampdiff(DAY, '2020-01-01 00:00:00', '2020-01-03 00:00:00')",
                AstToStringBuilder.toString(queryStatement.getQueryRelation()));

        queryStatement = (QueryStatement) analyzeSuccess(
                "select timestampdiff(day, '2020-01-01 00:00:00', '2020-01-03 00:00:00')");
        Assert.assertEquals("SELECT timestampdiff(DAY, '2020-01-01 00:00:00', '2020-01-03 00:00:00')",
                AstToStringBuilder.toString(queryStatement.getQueryRelation()));

        queryStatement = (QueryStatement) analyzeSuccess("select timestampdiff(minute, '2020-01-01', '2020-01-03')");
        Assert.assertEquals("SELECT timestampdiff(MINUTE, '2020-01-01 00:00:00', '2020-01-03 00:00:00')",
                AstToStringBuilder.toString(queryStatement.getQueryRelation()));

        queryStatement = (QueryStatement) analyzeSuccess(
                "select timestampdiff(minute, '2020-01-01 00:00:00', '2020-01-03 00:00:00')");
        Assert.assertEquals("SELECT timestampdiff(MINUTE, '2020-01-01 00:00:00', '2020-01-03 00:00:00')",
                AstToStringBuilder.toString(queryStatement.getQueryRelation()));
    }

    @Test
    public void testSpecialFunction() {
        analyzeSuccess("select char('A')");
        analyzeSuccess("select CURRENT_TIMESTAMP()");
        analyzeSuccess("select day('2022-01-01 00:00:00')");
        analyzeSuccess("select hour('2022-01-01 00:00:00')");
        analyzeSuccess("select minute('2022-01-01 00:00:00')");
        analyzeSuccess("select month('2022-01-01 00:00:00')");
        analyzeSuccess("select second('2022-01-01 00:00:00')");
        //analyzeSuccess("select week('2022-01-01 00:00:00')");
        analyzeSuccess("select year('2022-01-01 00:00:00')");
        analyzeSuccess("select quarter('2022-01-01 00:00:00')");

        analyzeSuccess("select password('root')");

        analyzeSuccess("select like(ta, ta) from tall");
        analyzeSuccess("select regexp(ta, ta) from tall");
        analyzeSuccess("select rlike(ta, ta) from tall");

        QueryStatement queryStatement = (QueryStatement) analyzeSuccess("select password('3wS_r7UHc')");
        Assert.assertTrue(queryStatement.getQueryRelation().getOutputExpression().get(0) instanceof StringLiteral);
        StringLiteral stringLiteral = (StringLiteral) queryStatement.getQueryRelation().getOutputExpression().get(0);
        Assert.assertEquals("*0B5CED987A45262765BB7DEE0EB00483E4AD82D0", stringLiteral.getValue());
    }

    @Test
    public void testODBCFunction() {
        analyzeSuccess("SELECT {fn ucase(ta)} FROM tall");
        analyzeSuccess("SELECT {fn ucase(`ta`)} FROM tall");
        analyzeSuccess("SELECT {fn UCASE(ucase(`ta`))} FROM tall");
        analyzeSuccess("select { fn extract(year from th)} from tall");
        analyzeFail("select {fn date_format(th, \"%Y\")} from tall",
                "Invalid odbc scalar function 'date_format(th, '%Y')'");
    }

    @Test
    public void testCreateFunction() throws Exception {
        UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer("CREATE FUNCTION f(INT, INT) RETURNS INT",
                getConnectContext());
        UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(
                "CREATE FUNCTION f(INT, INT, CHAR(10), BIGINT, ...) RETURNS INT",
                getConnectContext());
        UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(
                "CREATE AGGREGATE FUNCTION f(INT, INT) RETURNS INT INTERMEDIATE INT",
                getConnectContext());
        UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(
                "CREATE TABLE FUNCTION f(INT, INT) RETURNS INT",
                getConnectContext());
        UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(
                "CREATE FUNCTION f(INT, INT) RETURNS INT PROPERTIES (\"key\"=\"value\")",
                getConnectContext());
    }

    @Test
    public void testDropFunction() throws Exception {
        UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer("DROP FUNCTION f()", getConnectContext());
        UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer("DROP FUNCTION f(int)", getConnectContext());
        UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(
                "DROP FUNCTION f(int, ...)", getConnectContext());
        UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(
                "DROP FUNCTION db.f(int, char(2))", getConnectContext());
    }
}
