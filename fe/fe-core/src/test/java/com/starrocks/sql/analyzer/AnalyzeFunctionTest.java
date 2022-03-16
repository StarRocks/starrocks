// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzeFunctionTest {
    // use a unique dir so that it won't be conflict with other unit test which
    // may also start a Mocked Frontend
    private static String runningDir = "fe/mocked/AnalyzeFunction/" + UUID.randomUUID().toString() + "/";

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(runningDir);
        AnalyzeTestUtil.init();
    }

    @AfterClass
    public static void tearDown() {
        File file = new File(runningDir);
        file.delete();
    }

    @Test
    public void testSingle() {
        analyzeFail("select sum() from t0", "No matching function with signature: sum()");
        analyzeFail("select now(*) from t0", "Cannot pass '*' to scalar function.");
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
        QueryStatement queryStatement = (QueryStatement) analyzeSuccess("select date_add('2022-01-01', interval 2 day)");
        Assert.assertEquals("SELECT date_add('2022-01-01 00:00:00', INTERVAL 2 DAY) AS `date_add('2022-01-01', INTERVAL 2 DAY)`",
                AST2SQL.toString(queryStatement.getQueryRelation()));

        queryStatement = (QueryStatement) analyzeSuccess("select date_add('2022-01-01 00:00:00', interval 2 day)");
        Assert.assertEquals("SELECT date_add('2022-01-01 00:00:00', INTERVAL 2 DAY) AS `date_add('2022-01-01 00:00:00', INTERVAL 2 DAY)`",
                AST2SQL.toString(queryStatement.getQueryRelation()));

        queryStatement = (QueryStatement) analyzeSuccess("select date_add('2022-01-01', interval 2 minute)");
        Assert.assertEquals("SELECT date_add('2022-01-01 00:00:00', INTERVAL 2 MINUTE) AS `date_add('2022-01-01', INTERVAL 2 MINUTE)`",
                AST2SQL.toString(queryStatement.getQueryRelation()));

        queryStatement = (QueryStatement) analyzeSuccess("select date_add('2022-01-01 00:00:00', interval 2 minute)");
        Assert.assertEquals("SELECT date_add('2022-01-01 00:00:00', INTERVAL 2 MINUTE) AS `date_add('2022-01-01 00:00:00', INTERVAL 2 MINUTE)`",
                AST2SQL.toString(queryStatement.getQueryRelation()));

        queryStatement = (QueryStatement) analyzeSuccess("select timestampadd(day, 2, '2022-01-01')");
        Assert.assertEquals("SELECT TIMESTAMPADD(DAY, 2, '2022-01-01 00:00:00') AS `TIMESTAMPADD(DAY, 2, '2022-01-01')`",
                AST2SQL.toString(queryStatement.getQueryRelation()));

        queryStatement = (QueryStatement) analyzeSuccess("select date_add('2022-01-01', 2)");
        Assert.assertEquals("SELECT date_add('2022-01-01 00:00:00', INTERVAL 2 DAY) AS `date_add('2022-01-01', INTERVAL 2 DAY)`",
                AST2SQL.toString(queryStatement.getQueryRelation()));

        queryStatement = (QueryStatement) analyzeSuccess("select date_add('2022-01-01', interval 2 year)");
        Assert.assertEquals("SELECT date_add('2022-01-01 00:00:00', INTERVAL 2 YEAR) AS `date_add('2022-01-01', INTERVAL 2 YEAR)`",
                AST2SQL.toString(queryStatement.getQueryRelation()));

        queryStatement = (QueryStatement) analyzeSuccess("select date_sub('2022-01-01', interval 2 year)");
        Assert.assertEquals("SELECT date_sub('2022-01-01 00:00:00', INTERVAL 2 YEAR) AS `date_sub('2022-01-01', INTERVAL 2 YEAR)`",
                AST2SQL.toString(queryStatement.getQueryRelation()));

        queryStatement = (QueryStatement) analyzeSuccess("select subdate('2022-01-01', interval 2 year)");
        Assert.assertEquals("SELECT subdate('2022-01-01 00:00:00', INTERVAL 2 YEAR) AS `subdate('2022-01-01', INTERVAL 2 YEAR)`",
                AST2SQL.toString(queryStatement.getQueryRelation()));

        queryStatement = (QueryStatement) analyzeSuccess("select date_add('2022-01-01', interval 2 year)");
        Assert.assertEquals("SELECT date_add('2022-01-01 00:00:00', INTERVAL 2 YEAR) AS `date_add('2022-01-01', INTERVAL 2 YEAR)`",
                AST2SQL.toString(queryStatement.getQueryRelation()));

        queryStatement = (QueryStatement) analyzeSuccess("select timestampdiff(day, '2020-01-01', '2020-01-03')");
        Assert.assertEquals("SELECT TIMESTAMPDIFF(DAY, '2020-01-01 00:00:00', '2020-01-03 00:00:00') AS `TIMESTAMPDIFF(DAY, '2020-01-01', '2020-01-03')`",
                AST2SQL.toString(queryStatement.getQueryRelation()));

        queryStatement = (QueryStatement) analyzeSuccess("select timestampdiff(day, '2020-01-01 00:00:00', '2020-01-03 00:00:00')");
        Assert.assertEquals("SELECT TIMESTAMPDIFF(DAY, '2020-01-01 00:00:00', '2020-01-03 00:00:00') AS `TIMESTAMPDIFF(DAY, '2020-01-01 00:00:00', '2020-01-03 00:00:00')`",
                AST2SQL.toString(queryStatement.getQueryRelation()));

        queryStatement = (QueryStatement) analyzeSuccess("select timestampdiff(minute, '2020-01-01', '2020-01-03')");
        Assert.assertEquals("SELECT TIMESTAMPDIFF(MINUTE, '2020-01-01 00:00:00', '2020-01-03 00:00:00') AS `TIMESTAMPDIFF(MINUTE, '2020-01-01', '2020-01-03')`",
                AST2SQL.toString(queryStatement.getQueryRelation()));

        queryStatement = (QueryStatement) analyzeSuccess("select timestampdiff(minute, '2020-01-01 00:00:00', '2020-01-03 00:00:00')");
        Assert.assertEquals("SELECT TIMESTAMPDIFF(MINUTE, '2020-01-01 00:00:00', '2020-01-03 00:00:00') AS `TIMESTAMPDIFF(MINUTE, '2020-01-01 00:00:00', '2020-01-03 00:00:00')`",
                AST2SQL.toString(queryStatement.getQueryRelation()));
    }

    @Test
    public void testSpecialFunction() {
        analyzeSuccess("select char('A')");
        analyzeSuccess("select day('2022-01-01 00:00:00')");
        analyzeSuccess("select hour('2022-01-01 00:00:00')");
        analyzeSuccess("select minute('2022-01-01 00:00:00')");
        analyzeSuccess("select month('2022-01-01 00:00:00')");
        analyzeSuccess("select second('2022-01-01 00:00:00')");
        //analyzeSuccess("select week('2022-01-01 00:00:00')");
        analyzeSuccess("select year('2022-01-01 00:00:00')");
    }
}
