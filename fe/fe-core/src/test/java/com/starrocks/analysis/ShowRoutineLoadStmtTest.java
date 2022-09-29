// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class ShowRoutineLoadStmtTest {

    private static ConnectContext ctx;

    @BeforeClass
    public static void beforeClass() throws Exception {
        // create connect context
        ctx = UtFrameUtils.createDefaultCtx();
    }

    @Test
    public void testNormal() throws Exception {
        ctx = UtFrameUtils.createDefaultCtx();
        ctx.setDatabase("testDb");

        ShowRoutineLoadStmt stmt = new ShowRoutineLoadStmt(new LabelName("testDb","label"), false);

        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals("SHOW ROUTINE LOAD FOR `testDb`.`label`", stmt.toString());
        Assert.assertEquals("testDb", stmt.getDbFullName());
        Assert.assertFalse(stmt.isIncludeHistory());
        Assert.assertEquals(18, stmt.getMetaData().getColumnCount());
        Assert.assertEquals("Id", stmt.getMetaData().getColumn(0).getName());
    }

    @Test
    public void testFromDB() throws Exception {
        ctx = UtFrameUtils.createDefaultCtx();
        ctx.setDatabase("testDb");

        ShowRoutineLoadStmt stmt = new ShowRoutineLoadStmt(new LabelName("testDb",null), false);

        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals("SHOW ROUTINE LOAD FROM testDb", stmt.toString());
        Assert.assertEquals("testDb", stmt.getDbFullName());
    }

    @Test
    public void testBackquote() throws SecurityException, IllegalArgumentException {
        String sql = "SHOW ROUTINE LOAD FOR `rl_test` FROM `db_test` WHERE state = 'RUNNING' ORDER BY `CreateTime` desc";
        List<StatementBase> stmts = com.starrocks.sql.parser.SqlParser.parse(sql, ctx.getSessionVariable());

        ShowRoutineLoadStmt stmt = (ShowRoutineLoadStmt) stmts.get(0);
        Assert.assertEquals("db_test", stmt.getDbFullName());
        Assert.assertEquals("rl_test", stmt.getName());
    }
}
