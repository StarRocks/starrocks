// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.analysis;

import com.starrocks.alter.AlterJobMgr;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.AnalysisException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.sql.ast.AsyncRefreshSchemeDesc;
import com.starrocks.sql.ast.RefreshSchemeDesc;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class AlterMaterializedViewTest {
    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
        connectContext = AnalyzeTestUtil.getConnectContext();
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
    public void testAlterChangeRefresh() throws Exception {
        String alterMvSql = "alter materialized view mv1 refresh async start ('2222-05-23') every (interval 1 hour)";
        AlterMaterializedViewStmt alterMvStmt =
                (AlterMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(alterMvSql, connectContext);
        new AlterJobMgr().processAlterMaterializedView(alterMvStmt);

        alterMvSql = "alter materialized view mv1 refresh ASYNC";
        alterMvStmt = (AlterMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(alterMvSql, connectContext);
        new AlterJobMgr().processAlterMaterializedView(alterMvStmt);
        MaterializedView mv = (MaterializedView) currentState.getDb("test").getTable("mv1");
        String showCreateStmt = mv.getMaterializedViewDdlStmt(false);
        Assert.assertFalse(showCreateStmt.contains("EVERY(INTERVAL 1 HOUR)"));
    }

    @Test
    public void testAlterManualRefresh() throws Exception {
        String alterMvSql = "alter materialized view mv1 refresh manual";
        AlterMaterializedViewStmt alterMvStmt =
                (AlterMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(alterMvSql, connectContext);
        Assert.assertEquals(alterMvStmt.getRefreshSchemeDesc().getType(), MaterializedView.RefreshType.MANUAL);
    }

    @Test
    public void testAlterAsyncRefresh() throws Exception {
        String alterMvSql = "alter materialized view mv1 refresh async start ('2222-05-23') every (interval 1 hour)";
        AlterMaterializedViewStmt alterMvStmt =
                (AlterMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(alterMvSql, connectContext);
        final RefreshSchemeDesc asyncRefreshSchemeDesc = alterMvStmt.getRefreshSchemeDesc();
        assertTrue(asyncRefreshSchemeDesc instanceof AsyncRefreshSchemeDesc);
        Assert.assertEquals(asyncRefreshSchemeDesc.getType(), MaterializedView.RefreshType.ASYNC);
        assertNotNull(((AsyncRefreshSchemeDesc) asyncRefreshSchemeDesc).getStartTime());
        assertEquals(((IntLiteral) ((AsyncRefreshSchemeDesc) asyncRefreshSchemeDesc).getIntervalLiteral()
                .getValue()).getValue(), 1);
        assertEquals(((AsyncRefreshSchemeDesc) asyncRefreshSchemeDesc).getIntervalLiteral().getUnitIdentifier()
                .getDescription(), "HOUR");
    }

    @Test
    public void testAlterAsyncRefreshMonth() {
        String alterMvSql = "alter materialized view mv1 refresh async start ('2222-05-23') every (interval 1 MONTH)";
        Assert.assertThrows(IllegalArgumentException.class,
                () -> UtFrameUtils.parseStmtWithNewParser(alterMvSql, connectContext));
    }

    @Test
    public void testAlterAsyncRefreshNormal() throws Exception {
        String alterMvSql = "alter materialized view mv1 refresh async start ('2222-05-23') every (interval 1 DAY)";
        UtFrameUtils.parseStmtWithNewParser(alterMvSql, connectContext);
    }

    @Test
    public void testAlterAsyncRefreshLowercase() throws Exception {
        String alterMvSql = "alter materialized view mv1 refresh async start ('2222-05-23') every (interval 1 day)";
        UtFrameUtils.parseStmtWithNewParser(alterMvSql, connectContext);
    }
}
