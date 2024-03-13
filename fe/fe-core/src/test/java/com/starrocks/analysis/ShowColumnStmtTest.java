package com.starrocks.analysis;

import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.ShowColumnStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ShowColumnStmtTest {

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.alter_scheduler_interval_millisecond = 100;
        Config.dynamic_partition_enable = true;
        Config.dynamic_partition_check_interval_seconds = 1;
        Config.default_replication_num = 1;
        Config.enable_strict_storage_medium_check = false;
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);

        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("create table test_default\n" +
                        "(id varchar(255) default (uuid()))\n" +
                        "distributed by hash(id)\n" +
                        "PROPERTIES ( \"replication_num\" = \"1\" )")
                .withTable("create table test_only_metric_default (a int,b hll hll_union," +
                "c bitmap bitmap_union,d percentile percentile_union) distributed by hash(a);");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String dropSQL = "drop table test_default";
        try {
            DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
            GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);
        } catch (Exception ex) {

        }
    }

    @Test
    public void testShowDefaultValue() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String sql = "show full columns from test_default;";
        ShowColumnStmt showColumnStmt = (ShowColumnStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        ShowExecutor executor = new ShowExecutor(ctx, showColumnStmt);
        ShowResultSet resultSet = executor.execute();
        Assert.assertEquals("uuid()", resultSet.getResultRows().get(0).get(5));
    }

    @Test
    public void testOnlyMetricTypeDefaultValue() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String sql = "show full columns from test_only_metric_default;";
        ShowColumnStmt showColumnStmt = (ShowColumnStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        ShowExecutor executor = new ShowExecutor(ctx, showColumnStmt);
        ShowResultSet resultSet = executor.execute();
        // here must set null not \N
        Assert.assertNull(resultSet.getResultRows().get(0).get(5));
        Assert.assertNull(resultSet.getResultRows().get(1).get(5));
        Assert.assertNull(resultSet.getResultRows().get(2).get(5));
        Assert.assertNull(resultSet.getResultRows().get(3).get(5));
    }

}
