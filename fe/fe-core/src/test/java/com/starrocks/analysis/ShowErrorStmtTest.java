package com.starrocks.analysis;

import com.starrocks.qe.ConnectContext;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

// SHOW ERRORS [LIMIT [offset,] row_count]
public class ShowErrorStmtTest {
    @Mocked
    private ConnectContext ctx;

    @Test
    public void testNormal() throws Exception {

        ShowErrorStmt stmt = (ShowErrorStmt) com.starrocks.sql.parser.SqlParser.parse(
                "SHOW ERRORS",32).get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals("SHOW ERRORS", stmt.toString());
        Assert.assertEquals(-1L, stmt.getLimitNum());
        Assert.assertTrue(stmt.isSupportNewPlanner());

        stmt = (ShowErrorStmt) com.starrocks.sql.parser.SqlParser.parse(
                "SHOW ERRORS LIMIT 10",32).get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals("SHOW ERRORS LIMIT 10", stmt.toString());
        Assert.assertEquals(10L, stmt.getLimitNum());

        Assert.assertNotNull( stmt.toString());

        Assert.assertEquals( 3, stmt.getMetaData().getColumnCount());
        Assert.assertEquals("Message", stmt.getMetaData().getColumn(2).getName());

    }

}
