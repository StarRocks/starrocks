// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ShowRoutineLoadStmtTest {

    private ConnectContext ctx;

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
}
