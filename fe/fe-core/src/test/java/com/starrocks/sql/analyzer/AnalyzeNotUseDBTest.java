// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.starrocks.utframe.UtFrameUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzeNotUseDBTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @Test
    public void testNotUseDatabase() {
        AnalyzeTestUtil.getConnectContext().setDatabase("");
        analyzeSuccess("select count(*) from (select v1 from test.t0) t");
        analyzeSuccess("with t as (select v4 from test.t1) select count(*) from (select v1 from test.t0) a,t");

        analyzeSuccess("select * from test.t0 a join test.t1 b on a.v1 = b.v4");
        analyzeSuccess("select a.v2,b.v5 from test.t0 a join test.t1 b on a.v1 = b.v4");

        analyzeSuccess("select v1,unnest from test.tarray, unnest(v3)");
    }
}
