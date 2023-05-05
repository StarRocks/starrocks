// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.qe.ConnectContext;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

public class ShowProcedureStmtTest {
    @Mocked
    private ConnectContext ctx;

    @Test
    public void testNormal() throws Exception {

        ShowProcedureStmt stmt = (ShowProcedureStmt) com.starrocks.sql.parser.SqlParser.parse(
                "SHOW PROCEDURE STATUS", 32).get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);

        stmt = (ShowProcedureStmt) com.starrocks.sql.parser.SqlParser.parse(
                "SHOW PROCEDURE STATUS LIKE 'abc'", 32).get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals("abc", stmt.getPattern());

        stmt = (ShowProcedureStmt) com.starrocks.sql.parser.SqlParser.parse(
                "SHOW PROCEDURE STATUS where name='abc'", 32).get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);

        stmt = new ShowProcedureStmt("abc");
        Assert.assertNotNull(stmt.getPattern());
        Assert.assertEquals(11, stmt.getMetaData().getColumnCount());
        Assert.assertEquals("Db", stmt.getMetaData().getColumn(0).getName());

        // MySQLWorkbench use
        stmt = (ShowProcedureStmt) com.starrocks.sql.parser.SqlParser.parse(
                "SHOW FUNCTION STATUS where Db='abc'", 32).get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
    }
}
