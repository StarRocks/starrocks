// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.ast;

import com.starrocks.analysis.ShowOpenTableStmt;
import com.starrocks.qe.ConnectContext;
import mockit.Mocked;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * test for SHOW OPEN TABLES Statement
 * SHOW OPEN TABLES
 *     [{FROM | IN} db_name]
 *     [LIKE 'pattern' | WHERE expr]
 */
public class ShowOpenTableStmtTest {
    @Mocked
    private ConnectContext ctx;

    @Test
    public void testShowOpenTables() throws Exception{
        // normal
        ShowOpenTableStmt stmt = (ShowOpenTableStmt)  com.starrocks.sql.parser.SqlParser.parse("SHOW OPEN TABLES", 32).get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals("SHOW OPEN TABLES", stmt.toString());

        // with db
        stmt = (ShowOpenTableStmt)  com.starrocks.sql.parser.SqlParser.parse("SHOW OPEN TABLES FROM test", 32).get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals("SHOW OPEN TABLES FROM test", stmt.toString());

        // with like
        stmt = (ShowOpenTableStmt)  com.starrocks.sql.parser.SqlParser.parse("SHOW OPEN TABLES LIKE 'hello world'", 32).get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals("SHOW OPEN TABLES LIKE 'hello world'", stmt.toString());

        // with where
        stmt = (ShowOpenTableStmt)  com.starrocks.sql.parser.SqlParser.parse("SHOW OPEN TABLES where auth='abc'", 32).get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertNotNull( stmt.toString());

        // test set
        stmt = new ShowOpenTableStmt("test", "hello world", null);
        Assert.assertNotNull(stmt.getPattern());
        Assert.assertEquals("SHOW OPEN TABLES FROM test LIKE 'hello world'", stmt.toString());
        Assert.assertEquals(4, stmt.getMetaData().getColumnCount());
        Assert.assertEquals("Database", stmt.getMetaData().getColumn(0).getName());
        Assert.assertEquals("Name_locked", stmt.getMetaData().getColumn(3).getName());
    }

    @After
    public void cleanup() {
        ctx = null;
    }
}
