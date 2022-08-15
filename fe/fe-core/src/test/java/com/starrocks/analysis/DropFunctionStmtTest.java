// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.


package com.starrocks.analysis;

import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DropFunctionStmtTest {
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
        compareAfterParse("DROP FUNCTION f()", "DROP FUNCTION testDb.f()");
        compareAfterParse("DROP FUNCTION f(int)", "DROP FUNCTION testDb.f(INT)");
        compareAfterParse("DROP FUNCTION f(int, ...)", "DROP FUNCTION testDb.f(INT, ...)");
        compareAfterParse("DROP FUNCTION db.f(int, char(2))", "DROP FUNCTION db.f(INT, CHAR(2))");
    }

    private void compareAfterParse(String originSql, String exceptedNormalizedSql) throws Exception {
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(originSql, ctx);
        Assert.assertEquals(exceptedNormalizedSql, stmt.toSql());
    }

}
