// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.analysis;

import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.CatalogMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class UseCatalogStmtTest {
    private static ConnectContext ctx;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        StarRocksAssert starRocksAssert = new StarRocksAssert();
        starRocksAssert.withDatabase("db1").useDatabase("tbl1");
        String createCatalog = "create external catalog hive_catalog properties(" +
                "\"type\" = \"hive\", \"hive.metastore.uris\" = \"thrift://127.0.0.1:3333\")";
        starRocksAssert.withCatalog(createCatalog);
        ctx = new ConnectContext(null);
        ctx.setGlobalStateMgr(AccessTestUtil.fetchAdminCatalog());
    }

    @Test
    public void testParserAndAnalyzer() {
        String sql = "USE catalog hive_catalog";
        AnalyzeTestUtil.analyzeSuccess(sql);

        String sql_2 = "USE catalog default_catalog";
        AnalyzeTestUtil.analyzeSuccess(sql_2);

        String sql_3 = "USE xxxx default_catalog";
        AnalyzeTestUtil.analyzeFail(sql_3);
    }

    @Test
    public void testUse(@Mocked CatalogMgr catalogMgr) throws Exception {
        new Expectations() {
            {
                catalogMgr.catalogExists("hive_catalog");
                result = true;
                minTimes = 0;

                catalogMgr.catalogExists("default_catalog");
                result = true;
                minTimes = 0;
            }
        };

        ctx.setQueryId(UUIDUtil.genUUID());
        StmtExecutor executor = new StmtExecutor(ctx, "use catalog hive_catalog");
        executor.execute();

        Assert.assertEquals("hive_catalog", ctx.getCurrentCatalog());

        executor = new StmtExecutor(ctx, "use catalog default_catalog");
        executor.execute();

        Assert.assertEquals("default_catalog", ctx.getCurrentCatalog());

        executor = new StmtExecutor(ctx, "use xxx default_catalog");
        executor.execute();
        Assert.assertSame(ctx.getState().getStateType(), QueryState.MysqlStateType.ERR);

        executor = new StmtExecutor(ctx, "use catalog default_catalog xxx");
        executor.execute();
        Assert.assertSame(ctx.getState().getStateType(), QueryState.MysqlStateType.ERR);
    }
}
