// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.analysis;

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.StopRoutineLoadStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.Test;

public class StopRoutineLoadStmtTest {

    private ConnectContext ctx;

    @Test
    public void testNormal() throws Exception {
        ctx = UtFrameUtils.createDefaultCtx();
        ctx.setDatabase("testDb");

        StopRoutineLoadStmt stmt = new StopRoutineLoadStmt(new LabelName("testDb","label"));

        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals("STOP ROUTINE LOAD FOR testDb.label", stmt.toSql());
        Assert.assertEquals("testDb", stmt.getDbFullName());
    }

}
