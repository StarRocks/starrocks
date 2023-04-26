package com.starrocks.analysis;

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.ShowCollationStmt;
import com.starrocks.sql.parser.SqlParser;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

public class ShowCollationStmtTest {
    @Mocked
    private ConnectContext ctx;

    @Test
    public void testShowCollation() {
        {
            ShowCollationStmt stmt = (ShowCollationStmt) SqlParser.parse("SHOW COLLATION", 32).get(0);
            Analyzer.analyze(stmt, ctx);
            Assert.assertNull(stmt.getPattern());
            Assert.assertNull(stmt.getWhere());
        }

        {
            ShowCollationStmt stmt = (ShowCollationStmt) SqlParser.parse("SHOW COLLATION LIKE 'abc'", 32).get(0);
            Analyzer.analyze(stmt, ctx);
            Assert.assertEquals("abc", stmt.getPattern());
            Assert.assertNull(stmt.getWhere());
        }

        {
            ShowCollationStmt stmt = (ShowCollationStmt) SqlParser.parse("SHOW COLLATION WHERE Sortlen>1", 32).get(0);
            Analyzer.analyze(stmt, ctx);
            Assert.assertNull(stmt.getPattern());
            Assert.assertEquals("Sortlen > 1", stmt.getWhere().toSql());
        }

        {
            ShowCollationStmt stmt = new ShowCollationStmt();
            Analyzer.analyze(stmt, ctx);
            Assert.assertNull(stmt.getPattern());
            Assert.assertNull(stmt.getWhere());
        }

    }
}