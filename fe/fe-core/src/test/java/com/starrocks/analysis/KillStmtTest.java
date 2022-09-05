// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.analysis;

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.BeforeClass;
import org.junit.Test;

public class KillStmtTest {
    private static StarRocksAssert starRocksAssert;
    private static ConnectContext ctx;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        starRocksAssert = new StarRocksAssert();
        starRocksAssert.withDatabase("db1").useDatabase("tbl1");
        ctx = new ConnectContext(null);
        ctx.setGlobalStateMgr(AccessTestUtil.fetchAdminCatalog());
    }

    @Test
    public void testNormal() {
        String sql_1 = "kill query 1";
        AnalyzeTestUtil.analyzeSuccess(sql_1);

        String sql_2 = "kill 2";
        AnalyzeTestUtil.analyzeSuccess(sql_2);

        String sql_3 = "kill connection 3";
        AnalyzeTestUtil.analyzeSuccess(sql_3);

        String sql_4 = "kill q 4";
        AnalyzeTestUtil.analyzeFail(sql_4);
    }
}