// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class PauseRoutineLoadStmtTest {

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

        PauseRoutineLoadStmt stmt = new PauseRoutineLoadStmt(new LabelName("testDb","label"));

        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals("PAUSE ROUTINE LOAD FOR testDb.label", stmt.toSql());
        Assert.assertEquals("testDb", stmt.getDbFullName());
    }

    @Test
    public void testBackquote() throws SecurityException, IllegalArgumentException {
        String sql = "PAUSE ROUTINE LOAD FOR `db_test`.`rl_test`";
        List<StatementBase> stmts = com.starrocks.sql.parser.SqlParser.parse(sql, ctx.getSessionVariable());

        PauseRoutineLoadStmt stmt = (PauseRoutineLoadStmt) stmts.get(0);
        Assert.assertEquals("db_test", stmt.getDbFullName());
        Assert.assertEquals("rl_test", stmt.getName());
    }
}
