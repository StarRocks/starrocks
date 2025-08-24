package com.starrocks.analysis;

import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultMetaFactory;
import com.starrocks.sql.ast.ShowWarningStmt;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

// SHOW WARNINGS [LIMIT [offset,] row_count]
public class ShowWarningStmtTest {
    @Mocked
    private ConnectContext ctx;

    @Test
    public void testNormal() throws Exception {
        ShowWarningStmt stmt = (ShowWarningStmt) com.starrocks.sql.parser.SqlParser.parse(
                "SHOW WARNINGS",32).get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assertions.assertEquals(-1L, stmt.getLimitNum());

        stmt = (ShowWarningStmt) com.starrocks.sql.parser.SqlParser.parse(
                "SHOW WARNINGS LIMIT 10",32).get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assertions.assertEquals(10L, stmt.getLimitNum());

        Assertions.assertEquals( 3, new ShowResultMetaFactory().getMetadata(stmt).getColumnCount());
        Assertions.assertEquals("Message", new ShowResultMetaFactory().getMetadata(stmt).getColumn(2).getName());

        // show Error cases
        ShowWarningStmt stmt_e = (ShowWarningStmt) UtFrameUtils.parseStmtWithNewParser("SHOW ERRORS limit 10", ctx);
        Assertions.assertEquals(10L, stmt_e.getLimitNum());
    }
}
