package com.starrocks.analysis;

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.ShowCollationStmt;
import com.starrocks.sql.parser.SqlParser;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ShowCollationStmtTest {
    @Mocked
    private ConnectContext ctx;

    @Test
    public void testShowCollation() {
        {
            ShowCollationStmt stmt = (ShowCollationStmt) SqlParser.parse("SHOW COLLATION", 32).get(0);
            Analyzer.analyze(stmt, ctx);
            Assertions.assertNull(stmt.getPattern());
            Assertions.assertNull(stmt.getWhere());
        }

        {
            ShowCollationStmt stmt = (ShowCollationStmt) SqlParser.parse("SHOW COLLATION LIKE 'abc'", 32).get(0);
            Analyzer.analyze(stmt, ctx);
            Assertions.assertEquals("abc", stmt.getPattern());
            Assertions.assertNull(stmt.getWhere());
        }

        {
            ShowCollationStmt stmt = (ShowCollationStmt) SqlParser.parse("SHOW COLLATION WHERE Sortlen>1", 32).get(0);
            Analyzer.analyze(stmt, ctx);
            Assertions.assertNull(stmt.getPattern());
            Assertions.assertEquals("Sortlen > 1", stmt.getWhere().toSql());
        }

        {
            ShowCollationStmt stmt = new ShowCollationStmt();
            Analyzer.analyze(stmt, ctx);
            Assertions.assertNull(stmt.getPattern());
            Assertions.assertNull(stmt.getWhere());
        }

    }
}