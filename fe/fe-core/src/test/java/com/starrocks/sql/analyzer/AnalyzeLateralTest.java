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

public class AnalyzeLateralTest {
    // use a unique dir so that it won't be conflict with other unit test which
    // may also start a Mocked Frontend
    private static String runningDir = "fe/mocked/AnalyzeLateralTest/" + UUID.randomUUID().toString() + "/";

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
    public void testUnnest() {
        analyzeSuccess("select * from tarray cross join unnest(v3)");
        analyzeSuccess("select * from tarray cross join lateral unnest(v3)");
        analyzeFail("select * from tarray left join unnest(v3) on true",
                "Not support lateral join except inner or cross");
        analyzeFail("select * from tarray left join lateral unnest(v3) on true",
                "Not support lateral join except inner or cross");
        analyzeSuccess("select * from tarray,unnest(v3)");
        analyzeSuccess("select * from tarray,lateral unnest(v3)");
        analyzeSuccess("select v1,unnest from tarray,unnest(v3)");
        analyzeSuccess("select v1,t.unnest from tarray, unnest(v3) t");
        analyzeSuccess("select v1,t.* from tarray, unnest(v3) t");

        analyzeFail("select * from tarray, unknow_table_function(v2)",
                "Unknown table function 'unknow_table_function(BIGINT)'");
        analyzeFail("select * from tarray,unnest(v2)", "Unknown table function 'unnest(BIGINT)'");
        analyzeFail("select * from tarray,unnest(foo)", "Column '`foo`' cannot be resolved");
        analyzeFail("select * from t0 cross join lateral t1", "Only support lateral join with UDTF");
        analyzeFail("select  unnest(split('1,2,3',','))", "Table function cannot be used in expression");
        analyzeFail("select a.* from unnest(split('1,2,3',',')) a",
                "Table function must be used with lateral join");
    }
}