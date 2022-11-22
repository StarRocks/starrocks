// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.analysis;

import com.clearspring.analytics.util.Lists;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.ast.DescribeStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.ShowResourceGroupStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.system.BackendCoreStat;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static com.starrocks.sql.optimizer.statistics.CachedStatisticStorageTest.DEFAULT_CREATE_TABLE_TEMPLATE;

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

        starRocksAssert.withDatabase(StatsConstants.STATISTICS_DB_NAME)
                .useDatabase(StatsConstants.STATISTICS_DB_NAME)
                .withTable(DEFAULT_CREATE_TABLE_TEMPLATE);

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
                        "PROPERTIES('replication_num' = '1');")
                .withTable("CREATE TABLE if not exists t0(\n" +
                        "dt DATE NOT NULL,\n" +
                        "c1 VARCHAR NOT NULL,\n" +
                        "c2 VARCHAR  NOT NULL,\n" +
                        "c3 VARCHAR NOT NULL,\n" +
                        "c4 VARCHAR  NOT NULL\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(`dt`, `c1`, `c2`)\n" +
                        "COMMENT \"OLAP\"\n" +
                        "PARTITION BY RANGE(dt) (\n" +
                        "  START (\"2022-01-01\") END (\"2022-03-01\") EVERY (INTERVAL 1 day))\n" +
                        "DISTRIBUTED BY HASH(`c1`, `c2`) BUCKETS 2\n" +
                        "PROPERTIES(\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"in_memory\" = \"false\",\n" +
                        "\"storage_format\" = \"default\"\n" +
                        ");");
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

    @Test
    public void testViewOfThreeUnionAllWithConstNullOutput() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String createViewSql = "create view v2 as \n" +
                "select \n" +
                "\tt0.c1 as a,\n" +
                "\tNULL as b,\n" +
                "\tNULL as c,\n" +
                "\tNULL as d\n" +
                "from t0\n" +
                "UNION ALL\n" +
                "select \n" +
                "\tNULL as a,\n" +
                "\tt0.c2 as b,\n" +
                "\tNULL as c,\n" +
                "\tNULL as d\n" +
                "from t0\n" +
                "UNION ALL\n" +
                "select \n" +
                "\tNULL as a,\n" +
                "\tNULL as b,\n" +
                "\tt0.c3 as c,\n" +
                "\tt0.c4 as d\n" +
                "from t0";
        CreateViewStmt createViewStmt = (CreateViewStmt) UtFrameUtils.parseStmtWithNewParser(createViewSql, ctx);
        GlobalStateMgr.getCurrentState().createView(createViewStmt);

        String descViewSql = "describe v2";

        StatementBase statement =
                com.starrocks.sql.parser.SqlParser.parse(descViewSql, ctx.getSessionVariable()).get(0);
        Analyzer.analyze(statement, ctx);
        Assert.assertTrue(statement instanceof DescribeStmt);
        ShowExecutor showExecutor = new ShowExecutor(ctx, (DescribeStmt) statement);
        ShowResultSet rs = showExecutor.execute();
        Assert.assertTrue(rs.getResultRows().stream().allMatch(r -> r.get(1).toUpperCase().startsWith("VARCHAR")));
        String query = "select * from v2 union all select c1 as a, c2 as b, NULL as c, c4 as d from t0";
        String plan = UtFrameUtils.getVerboseFragmentPlan(ctx, query);
        plan = plan.replaceAll("\\[\\d+,\\s*", "")
                .replaceAll("VARCHAR\\(\\d+\\)", "VARCHAR")
                .replaceAll(",\\s*(true|false)]", "");
        String snippet = "  0:UNION\n" +
                "  |  output exprs:\n" +
                "  |      VARCHAR | VARCHAR | VARCHAR | VARCHAR\n" +
                "  |  child exprs:\n" +
                "  |      VARCHAR | VARCHAR | VARCHAR | VARCHAR\n" +
                "  |      VARCHAR | VARCHAR | VARCHAR | VARCHAR\n" +
                "  |  pass-through-operands: all";
        Assert.assertTrue(plan.contains(snippet));

        String dropViewSql = "drop view if exists v2";
        DropTableStmt dropViewStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropViewSql, ctx);
        GlobalStateMgr.getCurrentState().dropTable(dropViewStmt);
    }
}
