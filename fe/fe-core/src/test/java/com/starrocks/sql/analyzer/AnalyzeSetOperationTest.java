// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzeSetOperationTest {
    // use a unique dir so that it won't be conflict with other unit test which
    // may also start a Mocked Frontend
    private static String runningDir = "fe/mocked/AnalyzeSetOperation/" + UUID.randomUUID().toString() + "/";

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
    public void testColumnNumberUnequal() {
        analyzeSuccess("select v1,v2,v3 from t0 union select v4,v5,v6 from t1");

        analyzeFail("select v1,v2 from t0 union select v4,v5,v6 from t1");
        analyzeFail("select v1,v2 from t0 union all select v4,v5,v6 from t1");
        analyzeFail("select v1,v2 from t0 except select v4,v5,v6 from t1");
        analyzeFail("select v1,v2 from t0 intersect select v4,v5,v6 from t1");
        analyzeFail("select v1,v2 from t0 union select v5,v6 from t1 union select v7,v8,v9 from t2");
    }

    @Test
    public void testQualifier() {
        analyzeSuccess("select v1,v2,v3 from t0 union all select v4,v5,v6 from t1");
        analyzeSuccess("select v1,v2,v3 from t0 union distinct select v4,v5,v6 from t1");

        analyzeFail("select v1,v2,v3 from t0 except all select v4,v5,v6 from t1");
        analyzeSuccess("select v1,v2,v3 from t0 except distinct select v4,v5,v6 from t1");

        analyzeFail("select v1,v2,v3 from t0 intersect all select v4,v5,v6 from t1");
        analyzeSuccess("select v1,v2,v3 from t0 intersect distinct select v4,v5,v6 from t1");
    }

    @Test
    public void testOutput() {
        analyzeSuccess("select b1 from test_object union all select b1 from test_object");
        analyzeFail("select b1 from test_object union select b1 from test_object",
                "not support set operation");
        analyzeFail("select b1 from test_object except select b1 from test_object",
                "not support set operation");
        analyzeFail("select b1 from test_object intersect select b1 from test_object",
                "not support set operation");
    }

    @Test
    public void testValues() {
        analyzeFail("(SELECT 1 AS c1, 2 AS c2) UNION ALL SELECT * FROM (VALUES (10, 1006), (NULL)) tmp",
                "Values have unequal number of columns");
    }
}
