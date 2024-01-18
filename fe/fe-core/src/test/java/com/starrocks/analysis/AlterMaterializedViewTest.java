// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.analysis;

import com.google.common.collect.Lists;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
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
}
