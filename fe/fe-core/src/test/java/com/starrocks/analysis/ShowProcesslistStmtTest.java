// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.analysis;

import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.utframe.UtFrameUtils;

import java.util.Locale;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.analyzer.Analyzer;

public class ShowProcesslistStmtTest {
    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        connectContext = UtFrameUtils.createDefaultCtx();
    }

    @Test
    public void testNormal() throws Exception {
        {
            ShowProcesslistStmt stmt = new ShowProcesslistStmt();
            Analyzer.analyze(stmt, connectContext);
            Assert.assertEquals("SHOW PROCESSLIST", stmt.toString());
            Assert.assertNull(stmt.getWhere());
        }
        {
            ShowProcesslistStmt stmt =
                    (ShowProcesslistStmt) SqlParser.parse("SHOW PROCESSLIST WHERE DB = 'test'", 32).get(0);
            Analyzer.analyze(stmt, connectContext);
            Assert.assertEquals("SHOW PROCESSLIST WHERE DB = 'test'", stmt.toString());
            Assert.assertEquals("DB = 'test'", stmt.getWhere().toSql());
        }

        {
            ShowProcesslistStmt stmt = new ShowProcesslistStmt(true);
            Analyzer.analyze(stmt, connectContext);
            Assert.assertEquals("SHOW FULL PROCESSLIST", stmt.toString());
            Assert.assertNull(stmt.getWhere());
        }
        {
            ShowProcesslistStmt stmt = new ShowProcesslistStmt(false);
            Analyzer.analyze(stmt, connectContext);
            Assert.assertEquals("SHOW PROCESSLIST", stmt.toString());
            Assert.assertNull(stmt.getWhere());
        }

    }

}
