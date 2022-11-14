// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.analysis;

import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.ShowCharsetStmt;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.qe.ConnectContext;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

public class ShowCharsetStmtTest  {
    @Mocked
    private ConnectContext ctx;

    @Test
    public void testShowCharset() throws Exception {
        {
            ShowCharsetStmt stmt = (ShowCharsetStmt) SqlParser.parse("SHOW CHARSET", 32).get(0);
            Analyzer.analyze(stmt, ctx);
            Assert.assertNull(stmt.getPattern());
            Assert.assertNull(stmt.getWhere());
        }

        {
            ShowCharsetStmt stmt = (ShowCharsetStmt) SqlParser.parse("SHOW CHAR SET", 32).get(0);
            Analyzer.analyze(stmt, ctx);
            Assert.assertNull(stmt.getPattern());
            Assert.assertNull(stmt.getWhere());
        }

        {
            ShowCharsetStmt stmt = (ShowCharsetStmt) SqlParser.parse("SHOW CHARSET LIKE 'abc'", 32).get(0);
            Analyzer.analyze(stmt, ctx);
            Assert.assertEquals("abc", stmt.getPattern());
            Assert.assertNull(stmt.getWhere());
        }

        {
            ShowCharsetStmt stmt = (ShowCharsetStmt) SqlParser.parse("SHOW CHARSET WHERE Maxlen>1", 32).get(0);
            Analyzer.analyze(stmt, ctx);
            Assert.assertNull(stmt.getPattern());
            Assert.assertEquals("Maxlen > 1", stmt.getWhere().toSql());
        }

        {
            ShowCharsetStmt stmt = new ShowCharsetStmt();
            Analyzer.analyze(stmt, ctx);
            Assert.assertNull(stmt.getPattern());
            Assert.assertNull(stmt.getWhere());
        }
    }
}