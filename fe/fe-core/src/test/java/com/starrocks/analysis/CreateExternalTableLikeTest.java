// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.analysis;

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.CreateTableLikeStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class CreateExternalTableLikeTest {
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
    public void testCreateTableLike() {
        String sql_1 = "CREATE EXTERNAL TABLE hive_test_like LIKE hive_test";
        com.starrocks.analysis.StatementBase statementBase = AnalyzeTestUtil.analyzeSuccess(sql_1);
        Assert.assertTrue(statementBase instanceof CreateTableLikeStmt);
    }
}
