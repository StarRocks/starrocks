// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.analysis;

import com.clearspring.analytics.util.Lists;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class ShowCreateViewStmtTest {

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        FeConstants.default_scheduler_interval_millisecond = 100;
        Config.dynamic_partition_enable = true;
        Config.dynamic_partition_check_interval_seconds = 1;
        UtFrameUtils.createMinStarRocksCluster();

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);

        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE test.tbl1\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values less than('2020-02-01'),\n" +
                        "    PARTITION p2 values less than('2020-03-01')\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String dropSQL = "drop table tbl1";
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        try {
            GlobalStateMgr.getCurrentState().dropTable(dropTableStmt);
        } catch (Exception ex) {

        }
    }

    @Test
    public void testShowCreateView() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String createViewSql = "create view test_view (k1 COMMENT \"dt\", k2, v1) COMMENT \"view comment\" " +
                "as select * from tbl1";
        CreateViewStmt createViewStmt = (CreateViewStmt) UtFrameUtils.parseStmtWithNewParser(createViewSql, ctx);
        GlobalStateMgr.getCurrentState().createView(createViewStmt);

        List<Table> views = GlobalStateMgr.getCurrentState().getDb(createViewStmt.getDbName()).getViews();
        List<String> res = Lists.newArrayList();
        GlobalStateMgr.getDdlStmt(createViewStmt.getDbName(), views.get(0), res,
                null, null, false, false);
        Assert.assertEquals("CREATE VIEW `test_view` (k1 COMMENT \"dt\", k2, v1) COMMENT \"view comment\" " +
                "AS SELECT `test`.`tbl1`.`k1`, `test`.`tbl1`.`k2`, `test`.`tbl1`.`v1`\n" +
                "FROM `test`.`tbl1`;", res.get(0));
    }

}
