// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.analysis;

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.ResumeRoutineLoadStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Test;

public class ResumeRoutineLoadStmtTest {

    private ConnectContext ctx;

    @Test
    public void testNormal() throws Exception {
        ctx = UtFrameUtils.createDefaultCtx();
        ctx.setDatabase("testDb");

        ResumeRoutineLoadStmt stmt = new ResumeRoutineLoadStmt(new LabelName("testDb","label"));

        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals("RESUME ROUTINE LOAD FOR testDb.label", stmt.toSql());
        Assert.assertEquals("testDb", stmt.getDbFullName());
    }

}
