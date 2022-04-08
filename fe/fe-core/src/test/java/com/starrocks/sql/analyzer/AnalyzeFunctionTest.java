// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzeFunctionTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
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
}
