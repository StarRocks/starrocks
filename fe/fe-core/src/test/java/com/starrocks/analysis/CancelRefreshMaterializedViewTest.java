// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.analysis;

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.CancelRefreshMaterializedViewStatement;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class CancelRefreshMaterializedViewTest {
    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
        connectContext = AnalyzeTestUtil.getConnectContext();
    }

    @Test
    public void testNormal() throws Exception {
        String refreshMvSql = "cancel refresh materialized view test1.mv1";
        CancelRefreshMaterializedViewStatement cancelRefresh =
                (CancelRefreshMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(refreshMvSql, connectContext);
        String dbName = cancelRefresh.getMvName().getDb();
        String mvName = cancelRefresh.getMvName().getTbl();
        Assert.assertEquals("test1", dbName);
        Assert.assertEquals("mv1", mvName);
    }
}
