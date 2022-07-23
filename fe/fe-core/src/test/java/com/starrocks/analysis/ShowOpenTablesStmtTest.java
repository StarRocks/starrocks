// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.analysis;

import com.starrocks.qe.ConnectContext;
import mockit.Mocked;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/***
 * Test for SHOW OPEN TABLES Statement
 *  SHOW OPEN TABLES
 *      [{FROM | IN} db_name]
 *      [LIKE 'pattern' | WHERE expr]
 */
public class ShowOpenTablesStmtTest {
    @Mocked
    private ConnectContext ctx;
    @Test
    public void testShowOpenTables() {
        // common
        ShowOpenTableStmt stmt = (ShowOpenTableStmt)  com.starrocks.sql.parser.SqlParser.parse("SHOW OPEN TABLES", 32).get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals("SHOW OPEN TABLES", stmt.toString());
        // with Db
        stmt = (ShowOpenTableStmt)  com.starrocks.sql.parser.SqlParser.parse("SHOW OPEN TABLES FROM test", 32).get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertNotNull(stmt.getDbName());
        Assert.assertEquals("SHOW OPEN TABLES FROM test", stmt.toString());
        // with LIKE
        stmt = (ShowOpenTableStmt)  com.starrocks.sql.parser.SqlParser.parse("SHOW OPEN TABLES WHERE `Table` LIKE 'hello_world'", 32).get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals("SHOW OPEN TABLES WHERE 'LikePredicate{id=null, type=INVALID_TYPE, sel=0.1, #distinct=-1, scale=-1}'", stmt.toString());
        // with Where
        stmt = (ShowOpenTableStmt)  com.starrocks.sql.parser.SqlParser.parse("SHOW OPEN TABLES where in_use >=1", 32).get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertNotNull(stmt.getWhere());
        Assert.assertNotNull( stmt.toString());
        // Test set
        stmt = new ShowOpenTableStmt("test", "hello_world", null);
        Assert.assertNotNull(stmt.getPattern());
        Assert.assertEquals("SHOW OPEN TABLES FROM test LIKE 'hello_world'", stmt.toString());
        Assert.assertEquals(4, stmt.getMetaData().getColumnCount());
        Assert.assertEquals("Database", stmt.getMetaData().getColumn(0).getName());
        Assert.assertEquals("Name_locked", stmt.getMetaData().getColumn(3).getName());

        // compatible old
        stmt = new ShowOpenTableStmt();
        stmt.analyze(stmt.analyzer);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals("SHOW OPEN TABLES", stmt.toString());
        Assert.assertTrue(stmt.isSupportNewPlanner());
    }
    @After
    public void cleanup() {
        ctx = null;
    }
}