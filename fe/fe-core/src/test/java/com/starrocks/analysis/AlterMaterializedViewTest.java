// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.analysis;

import com.google.common.collect.Lists;
import com.starrocks.alter.AlterJobMgr;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.MVActiveChecker;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class AlterMaterializedViewTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    private static GlobalStateMgr currentState;

    @BeforeClass
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
        connectContext = AnalyzeTestUtil.getConnectContext();
        starRocksAssert = AnalyzeTestUtil.getStarRocksAssert();
        currentState = GlobalStateMgr.getCurrentState();
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv1\n" +
                "                DISTRIBUTED BY HASH(v1) BUCKETS 10\n" +
                "                PROPERTIES(\n" +
                "                    \"replication_num\" = \"1\"\n" +
                "                )\n" +
                "                as  select v1, count(v2) as count_c2, sum(v3) as sum_c3\n" +
                "                from t0 group by v1;\n");
    }

    @Before
    public void before() {
        starRocksAssert.getCtx().setThreadLocalInfo();
    }

    @Test
    public void testRename() throws Exception {
        String alterMvSql = "alter materialized view mv1 rename mv2;";
        AlterMaterializedViewStmt alterMvStmt =
                (AlterMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(alterMvSql, connectContext);
        TableName oldMvName = alterMvStmt.getMvName();
        String newMvName = alterMvStmt.getNewMvName();
        Assert.assertEquals("test", oldMvName.getDb());
        Assert.assertEquals("mv1", oldMvName.getTbl());
        Assert.assertEquals("mv2", newMvName);
    }

    @Test(expected = AnalysisException.class)
    public void testRenameSameName() throws Exception {
        String alterMvSql = "alter materialized view mv1 rename mv1;";
        UtFrameUtils.parseStmtWithNewParser(alterMvSql, connectContext);
    }

    @Test(expected = AnalysisException.class)
    public void testAlterSyncRefresh() throws Exception {
        String alterMvSql = "alter materialized view mv1 refresh sync";
        AlterMaterializedViewStmt alterMvStmt =
                (AlterMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(alterMvSql, connectContext);
        Assert.assertEquals(alterMvStmt.getRefreshSchemeDesc().getType(), MaterializedView.RefreshType.SYNC);
    }

    @Test
    public void testAlterRefreshScheme() throws Exception {
        List<String> refreshSchemes = Lists.newArrayList(
                "ASYNC START(\"2022-05-23 00:00:00\") EVERY(INTERVAL 1 HOUR)",
                "ASYNC",
                "ASYNC START(\"2022-05-23 01:02:03\") EVERY(INTERVAL 1 DAY)",
                "ASYNC EVERY(INTERVAL 1 DAY)",
                "ASYNC",
                "MANUAL",
                "ASYNC EVERY(INTERVAL 1 DAY)",
                "MANUAL",
                "ASYNC START(\"2022-05-23 01:02:03\") EVERY(INTERVAL 1 DAY)"
        );

        String mvName = "mv1";
        for (String refresh : refreshSchemes) {
            // alter
            String sql = String.format("alter materialized view %s refresh %s", mvName, refresh);
            starRocksAssert.ddl(sql);

            // verify
            MaterializedView mv = starRocksAssert.getMv("test", mvName);
            String showCreateStmt = mv.getMaterializedViewDdlStmt(false);
            Assert.assertTrue(String.format("alter to %s \nbut got \n%s", refresh, showCreateStmt),
                    showCreateStmt.contains(refresh));
        }
    }

    @Test
    public void testAlterMVProperties() throws Exception {
        {
            String alterMvSql = "alter materialized view mv1 set (\"session.query_timeout\" = \"10000\")";
            AlterMaterializedViewStmt stmt =
                    (AlterMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(alterMvSql, connectContext);
            currentState.alterMaterializedView(stmt);
        }

        {
            String alterMvSql = "alter materialized view mv1 set (\"query_timeout\" = \"10000\")";
            AlterMaterializedViewStmt stmt =
                    (AlterMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(alterMvSql, connectContext);
            Assert.assertThrows(DdlException.class, () -> currentState.alterMaterializedView(stmt));
        }
    }

    @Test
    public void testAlterMVRewriteStalenessProperties() throws Exception {
        {
            String alterMvSql = "alter materialized view mv1 set (\"mv_rewrite_staleness_second\" = \"60\")";
            AlterMaterializedViewStmt stmt =
                    (AlterMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(alterMvSql, connectContext);
            currentState.alterMaterializedView(stmt);
        }

        {
            String alterMvSql = "alter materialized view mv1 set (\"mv_rewrite_staleness_second\" = \"abc\")";
            AlterMaterializedViewStmt stmt =
                    (AlterMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(alterMvSql, connectContext);
            Assert.assertThrows(DdlException.class, () -> currentState.alterMaterializedView(stmt));
        }
    }

    /**
     * Reload procedure should work for hierarchical MV
     */
    @Test
    public void testMVOnMVReload() throws Exception {
        PlanTestBase.mockDml();
        MVActiveChecker checker = GlobalStateMgr.getCurrentState().getMvActiveChecker();
        checker.setStop();

        String createBaseTable = "create table treload_1 (c1 int) distributed by hash(c1) " +
                "properties('replication_num'='1')";
        starRocksAssert.withTable(createBaseTable);
        starRocksAssert.withMaterializedView("create materialized view mvreload_1 " +
                "distributed by hash(c1) refresh async " +
                "as select * from treload_1");
        starRocksAssert.withMaterializedView("create materialized view mvreload_2 " +
                "distributed by hash(c1) refresh async " +
                "as select * from treload_1");
        starRocksAssert.withMaterializedView("create materialized view mvreload_3 " +
                "distributed by hash(c1) refresh async " +
                "as select a.c1, b.c1 as bc1 from mvreload_1 a join mvreload_2 b");

        // drop base table would inactive all related MV
        starRocksAssert.dropTable("treload_1");
        Assert.assertThrows(DdlException.class, () ->
                starRocksAssert.refreshMV("refresh materialized view mvreload_3"));
        Assert.assertFalse(starRocksAssert.getMv("test", "mvreload_1").isActive());
        Assert.assertFalse(starRocksAssert.getMv("test", "mvreload_2").isActive());
        Assert.assertFalse(starRocksAssert.getMv("test", "mvreload_3").isActive());

        // create the table and run the AutoActive
        starRocksAssert.withTable(createBaseTable);
        checker.runForTest();
        checker.runForTest();
        Assert.assertTrue(starRocksAssert.getMv("test", "mvreload_1").isActive());
        Assert.assertTrue(starRocksAssert.getMv("test", "mvreload_2").isActive());
        Assert.assertTrue(starRocksAssert.getMv("test", "mvreload_3").isActive());

        // create the table and refresh
        starRocksAssert.dropTable("treload_1");
        starRocksAssert.withTable(createBaseTable);
        starRocksAssert.refreshMV("refresh materialized view mvreload_1");
        starRocksAssert.refreshMV("refresh materialized view mvreload_2");
        starRocksAssert.refreshMV("refresh materialized view mvreload_3");
        Assert.assertTrue(starRocksAssert.getMv("test", "mvreload_1").isActive());
        Assert.assertTrue(starRocksAssert.getMv("test", "mvreload_2").isActive());
        Assert.assertTrue(starRocksAssert.getMv("test", "mvreload_3").isActive());

        // create the table and manually active, top-down active
        starRocksAssert.dropTable("treload_1");
        starRocksAssert.withTable(createBaseTable);
        starRocksAssert.ddl("alter materialized view mvreload_1 active");
        starRocksAssert.ddl("alter materialized view mvreload_2 active");
        starRocksAssert.ddl("alter materialized view mvreload_3 active");
        Assert.assertTrue(starRocksAssert.getMv("test", "mvreload_1").isActive());
        Assert.assertTrue(starRocksAssert.getMv("test", "mvreload_2").isActive());
        Assert.assertTrue(starRocksAssert.getMv("test", "mvreload_3").isActive());

        // cleanup
        starRocksAssert.getCtx().setThreadLocalInfoIfNotExists();
        starRocksAssert.dropTable("treload_1");
        starRocksAssert.dropMaterializedView("mvreload_1");
        starRocksAssert.dropMaterializedView("mvreload_2");
        starRocksAssert.dropMaterializedView("mvreload_3");
        checker.start();
    }

    @Test
    public void testActiveChecker() throws Exception {
        Config.enable_mv_automatic_active_check = true;
        PlanTestBase.mockDml();
        MVActiveChecker checker = GlobalStateMgr.getCurrentState().getMvActiveChecker();
        checker.setStop();

        String baseTableName = "base_tbl_active";
        String createTableSql =
                "create table " + baseTableName + " ( k1 int, k2 int) distributed by hash(k1) " +
                        "properties('replication_num'='1')";
        starRocksAssert.withTable(createTableSql);
        starRocksAssert.withMaterializedView("create materialized view mv_active " +
                " distributed by hash(k1) refresh manual as select * from base_tbl_active");
        MaterializedView mv = (MaterializedView) starRocksAssert.getTable(connectContext.getDatabase(), "mv_active");
        Assert.assertTrue(mv.isActive());

        // drop the base table and try to activate it
        starRocksAssert.dropTable(baseTableName);
        Assert.assertFalse(mv.isActive());
        Assert.assertEquals("base-table dropped: base_tbl_active", mv.getInactiveReason());
        checker.runForTest();
        Assert.assertFalse(mv.isActive());
        Assert.assertEquals("base-table dropped: base_tbl_active", mv.getInactiveReason());

        // create the table again, and activate it
        connectContext.setThreadLocalInfo();
        starRocksAssert.withTable(createTableSql);
        checker.runForTest();
        Assert.assertTrue(mv.isActive());

        // activate before refresh
        connectContext.setThreadLocalInfo();
        starRocksAssert.dropTable(baseTableName);
        starRocksAssert.withTable(createTableSql);
        Assert.assertFalse(mv.isActive());
        Thread.sleep(1000);
        starRocksAssert.getCtx().executeSql("refresh materialized view " + mv.getName() + " with sync mode");
        Assert.assertTrue(mv.isActive());

        // manually set to inactive
        mv.setInactiveAndReason(AlterJobMgr.MANUAL_INACTIVE_MV_REASON);
        Assert.assertFalse(mv.isActive());
        checker.runForTest();
        Assert.assertFalse(mv.isActive());
        Assert.assertEquals(AlterJobMgr.MANUAL_INACTIVE_MV_REASON, mv.getInactiveReason());

        checker.start();
        Config.enable_mv_automatic_active_check = false;
    }

}
