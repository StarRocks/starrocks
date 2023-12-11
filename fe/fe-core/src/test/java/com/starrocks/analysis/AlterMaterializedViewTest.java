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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.alter.AlterJobMgr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.MVActiveChecker;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.sql.ast.RefreshSchemeClause;
import com.starrocks.sql.ast.TableRenameClause;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

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
        connectContext.setThreadLocalInfo();
    }

    @Test
    public void testRename() throws Exception {
        String alterMvSql = "alter materialized view mv1 rename mv2;";
        AlterMaterializedViewStmt alterMvStmt =
                (AlterMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(alterMvSql, connectContext);
        TableName oldMvName = alterMvStmt.getMvName();
        String newMvName = ((TableRenameClause) alterMvStmt.getAlterTableClause()).getNewTableName();
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
        RefreshSchemeClause refreshSchemeClause = (RefreshSchemeClause) alterMvStmt.getAlterTableClause();
        Assert.assertEquals(refreshSchemeClause.getType(), MaterializedView.RefreshType.SYNC);
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
            Assert.assertThrows(SemanticException.class, () -> currentState.alterMaterializedView(stmt));
        }
    }

    // TODO: consider to support alterjob for mv
    @Test
    public void testAlterMVColocateGroup() throws Exception {
        String alterMvSql = "alter materialized view mv1 set (\"colocate_with\" = \"group1\")";
        AlterMaterializedViewStmt stmt =
                (AlterMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(alterMvSql, connectContext);
        Assert.assertThrows(SemanticException.class, () -> currentState.alterMaterializedView(stmt));
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
            Assert.assertThrows(SemanticException.class, () -> currentState.alterMaterializedView(stmt));
        }
    }

    @Test
    public void testAlterMVOnView() throws Exception {
        final String mvName = "mv_on_view_1";
        starRocksAssert.withView("CREATE VIEW view1 as select v1, sum(v2) as k2 from t0 group by v1");
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + mvName +
                "                DISTRIBUTED BY HASH(v1) BUCKETS 10\n" +
                "                PROPERTIES(\n" +
                "                    \"replication_num\" = \"1\"\n" +
                "                )\n" +
                "                as select v1, k2 from view1");

        MaterializedView mv = (MaterializedView) starRocksAssert.getTable(connectContext.getDatabase(), mvName);
        List<String> columns = mv.getColumns().stream().map(Column::getName).sorted().collect(Collectors.toList());
        Assert.assertEquals(ImmutableList.of("k2", "v1"), columns);

        // alter the view to a different type, cause MV inactive
        connectContext.executeSql("alter view view1 as select v1, avg(v2) as k2 from t0 group by v1");
        Assert.assertFalse(mv.isActive());
        Assert.assertEquals("base view view1 changed", mv.getInactiveReason());

        // try to active the mv
        connectContext.executeSql(String.format("alter materialized view %s active", mvName));
        Assert.assertFalse(mv.isActive());
        Assert.assertEquals("mv schema changed: " +
                "[[`k2` bigint(20) NULL COMMENT \"\", `v1` bigint(20) NULL COMMENT \"\"]] " +
                "does not match " +
                "[[`k2` double NULL COMMENT \"\", `v1` bigint(20) NULL COMMENT \"\"]]", mv.getInactiveReason());

        // use a illegal view schema, should active the mv correctly
        connectContext.executeSql("alter view view1 as select v1, max(v2) as k2 from t0 group by v1");
        connectContext.executeSql(String.format("alter materialized view %s active", mvName));
        Assert.assertTrue(mv.isActive());
        Assert.assertNull(mv.getInactiveReason());
    }

    @Test
    public void testActiveChecker() throws Exception {
        PlanTestBase.mockDml();
        MVActiveChecker checker = GlobalStateMgr.getCurrentState().getMvActiveChecker();
        checker.setStop();

        String baseTableName = "base_tbl_active";
        String createTableSql =
                "create table " + baseTableName + " ( k1 int, k2 int) properties('replication_num'='1')";
        starRocksAssert.withTable(createTableSql);
        starRocksAssert.withMaterializedView("create materialized view mv_active " +
                " refresh manual as select * from base_tbl_active");
        MaterializedView mv = (MaterializedView) starRocksAssert.getTable(connectContext.getDatabase(), "mv_active");
        Assert.assertTrue(mv.isActive());

        // drop the base table and try to activate it
        starRocksAssert.dropTable(baseTableName);
        Assert.assertFalse(mv.isActive());
        Assert.assertEquals("base-table dropped: base_tbl_active", mv.getInactiveReason());
        checker.runForTest(true);
        Assert.assertFalse(mv.isActive());
        Assert.assertEquals("base-table dropped: base_tbl_active", mv.getInactiveReason());

        // create the table again, and activate it
        connectContext.setThreadLocalInfo();
        starRocksAssert.withTable(createTableSql);
        checker.runForTest(true);
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
        checker.runForTest(true);
        Assert.assertFalse(mv.isActive());
        Assert.assertEquals(AlterJobMgr.MANUAL_INACTIVE_MV_REASON, mv.getInactiveReason());

        checker.start();
        starRocksAssert.dropTable(baseTableName);
        starRocksAssert.dropMaterializedView(mv.getName());
    }

    @Test
    public void testActiveGracePeriod() throws Exception {
        PlanTestBase.mockDml();
        MVActiveChecker checker = GlobalStateMgr.getCurrentState().getMvActiveChecker();
        checker.setStop();

        String baseTableName = "base_tbl_active";
        String createTableSql =
                "create table " + baseTableName + " ( k1 int, k2 int) properties('replication_num'='1')";
        starRocksAssert.withTable(createTableSql);
        starRocksAssert.withMaterializedView("create materialized view mv_active " +
                " refresh manual as select * from base_tbl_active");
        MaterializedView mv = (MaterializedView) starRocksAssert.getTable(connectContext.getDatabase(), "mv_active");
        Assert.assertTrue(mv.isActive());

        // drop the base table and try to activate it
        starRocksAssert.dropTable(baseTableName);
        Assert.assertFalse(mv.isActive());
        Assert.assertEquals("base-table dropped: base_tbl_active", mv.getInactiveReason());
        checker.runForTest(false);
        for (int i = 0; i < 10; i++) {
            checker.runForTest(false);
            Assert.assertFalse(mv.isActive());
        }

        // create the table, but in grace period, could not activate it
        connectContext.setThreadLocalInfo();
        starRocksAssert.withTable(createTableSql);
        for (int i = 0; i < 10; i++) {
            checker.runForTest(false);
            Assert.assertFalse(mv.isActive());
        }

        // clear the grace period and active it again
        checker.runForTest(true);
        Assert.assertTrue(mv.isActive());

        checker.start();
        starRocksAssert.dropTable(baseTableName);
    }

    @Test
    public void testActiveCheckerBackoff() {
        MVActiveChecker.MvActiveInfo activeInfo = MVActiveChecker.MvActiveInfo.firstFailure();
        Assert.assertTrue(activeInfo.isInGracePeriod());

        LocalDateTime start = LocalDateTime.now(TimeUtils.getSystemTimeZone().toZoneId());
        for (int i = 0; i < 10; i++) {
            activeInfo.next();
        }
        Assert.assertTrue(activeInfo.isInGracePeriod());
        Duration d = Duration.between(start, activeInfo.getNextActive());
        Assert.assertEquals(d.toMinutes(), MVActiveChecker.MvActiveInfo.MAX_BACKOFF_MINUTES);
    }
}
