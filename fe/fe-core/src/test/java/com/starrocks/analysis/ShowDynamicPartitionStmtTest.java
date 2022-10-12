// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.analysis;

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.ShowDynamicPartitionStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ShowDynamicPartitionStmtTest {

    private ConnectContext ctx;

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testNormal() throws Exception {
        ctx = UtFrameUtils.createDefaultCtx();
        ctx.setDatabase("testDb");
        String showSQL = "SHOW DYNAMIC PARTITION TABLES FROM testDb";
        ShowDynamicPartitionStmt stmtFromSql =
                (ShowDynamicPartitionStmt) UtFrameUtils.parseStmtWithNewParser(showSQL, ctx);
        ShowDynamicPartitionStmt stmt = new ShowDynamicPartitionStmt("testDb");
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals(stmtFromSql.toSql(), stmt.toSql());
        Assert.assertEquals("testDb", stmt.getDb());

        String showWithoutDbSQL = "SHOW DYNAMIC PARTITION TABLES ";
        ShowDynamicPartitionStmt stmtWithoutDbFromSql =
                (ShowDynamicPartitionStmt) UtFrameUtils.parseStmtWithNewParser(showWithoutDbSQL, ctx);
        ShowDynamicPartitionStmt stmtWithoutIndicateDb = new ShowDynamicPartitionStmt(null);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmtWithoutIndicateDb, ctx);
        Assert.assertEquals(stmtWithoutDbFromSql.toSql(), stmtWithoutIndicateDb.toSql());
        Assert.assertEquals("testDb", stmtWithoutIndicateDb.getDb());

    }

    @Test(expected = SemanticException.class)
    public void testNoDb() throws Exception {
        ctx = UtFrameUtils.createDefaultCtx();
        ShowDynamicPartitionStmt stmtWithoutDb = new ShowDynamicPartitionStmt(null);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmtWithoutDb, ctx);
        Assert.fail("No Exception throws.");
    }

}