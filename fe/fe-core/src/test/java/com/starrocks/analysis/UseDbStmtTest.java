// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.analysis;

import com.starrocks.catalog.Database;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class UseDbStmtTest {
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
    public void testParserAndAnalyzer() {
        String sql = "USE db1";
        AnalyzeTestUtil.analyzeSuccess(sql);

        String sql_2 = "USE default_catalog.db1";
        AnalyzeTestUtil.analyzeSuccess(sql_2);

        String sql_3 = "USE hive_catalog.hive_db";
        AnalyzeTestUtil.analyzeSuccess(sql_3);

        String sql_4 = "USE hive_catalog.hive_db.hive_table";
        AnalyzeTestUtil.analyzeFail(sql_4);
    }

    @Test
    public void testUse(@Mocked CatalogMgr catalogMgr, @Mocked MetadataMgr metadataMgr, @Mocked Auth auth) throws Exception {
        new Expectations() {
            {
                CatalogMgr.isInternalCatalog("default_catalog");
                result = true;

                catalogMgr.catalogExists("default_catalog");
                result = true;
                minTimes = 0;

                metadataMgr.getDb("default_catalog", "db");
                result = new Database();
                minTimes = 0;

                auth.checkDbPriv(ctx, "db", PrivPredicate.SHOW);
                result = true;
            }
        };

        ctx.setQueryId(UUIDUtil.genUUID());
        StmtExecutor executor = new StmtExecutor(ctx, "use default_catalog.db");
        executor.execute();

        Assert.assertEquals("default_catalog", ctx.getCurrentCatalog());
        Assert.assertEquals("db", ctx.getDatabase());
    }
}