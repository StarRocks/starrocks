// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzeNotUseDBTest {
    // use a unique dir so that it won't be conflict with other unit test which
    // may also start a Mocked Frontend
    private static String runningDir = "fe/mocked/AnalyzeSingle/" + UUID.randomUUID().toString() + "/";

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
    public void testNotUseDatabase() {
        AnalyzeTestUtil.getConnectContext().setDatabase("");
        analyzeSuccess("select count(*) from (select v1 from test.t0) t");
        analyzeSuccess("with t as (select v4 from test.t1) select count(*) from (select v1 from test.t0) a,t");

        analyzeSuccess("select * from test.t0 a join test.t1 b on a.v1 = b.v4");
        analyzeSuccess("select a.v2,b.v5 from test.t0 a join test.t1 b on a.v1 = b.v4");
    }
}
