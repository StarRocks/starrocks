// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.analysis;

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ShowCreateMaterializedViewStmtTest {
    private ConnectContext ctx;

    @Before
    public void setUp() {
    }

    @Test
    public void testNormal() throws Exception {
        ctx = UtFrameUtils.createDefaultCtx();
        ctx.setCluster("testCluster");
        ctx.setDatabase("testDb");
        ShowCreateTableStmt stmt = new ShowCreateTableStmt(new TableName("testDb", "testTbl"), false, true);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals("SHOW CREATE MATERIALIZED VIEW testCluster:testDb.testTbl", stmt.toString());
        Assert.assertEquals("testCluster:testDb", stmt.getDb());
        Assert.assertEquals("testTbl", stmt.getTable());
        Assert.assertEquals(2, ShowCreateTableStmt.getMaterializedViewMetaData().getColumnCount());
        Assert.assertEquals("Materialized View",
                ShowCreateTableStmt.getMaterializedViewMetaData().getColumn(0).getName());
        Assert.assertEquals("Create Materialized View",
                ShowCreateTableStmt.getMaterializedViewMetaData().getColumn(1).getName());
    }

    @Test(expected = SemanticException.class)
    public void testNoTbl() throws Exception {
        ctx = UtFrameUtils.createDefaultCtx();
        ShowCreateTableStmt stmt = new ShowCreateTableStmt(null);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.fail("No Exception throws.");
    }
}
