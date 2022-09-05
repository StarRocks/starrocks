// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.analysis;

import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CreateFunctionStmtTest {
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
        compareAfterParse("CREATE FUNCTION f(INT, INT) RETURNS INT");
        compareAfterParse("CREATE FUNCTION f(INT, INT, CHAR(10), BIGINT, ...) RETURNS INT");
        compareAfterParse("CREATE AGGREGATE FUNCTION f(INT, INT) RETURNS INT INTERMEDIATE INT");
        compareAfterParse("CREATE TABLE FUNCTION f(INT, INT) RETURNS INT");
        compareAfterParse("CREATE FUNCTION f(INT, INT) RETURNS INT PROPERTIES (\"key\"=\"value\")");
    }

    private void compareAfterParse(String originSql) throws Exception {
        compareAfterParse(originSql, originSql);
    }

    private void compareAfterParse(String originSql, String exceptedNormalizedSql) throws Exception {
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(originSql, ctx);
        Assert.assertEquals(exceptedNormalizedSql, stmt.toSql());
    }

}
