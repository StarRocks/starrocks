// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import org.junit.Assert;
import org.junit.Test;


public class ShowRoutineLoadTaskStmtTest {
    
    @Test 
    public void testGetRedirectStatus() {
        ShowRoutineLoadTaskStmt loadStmt = new ShowRoutineLoadTaskStmt("", null);
        Assert.assertTrue(loadStmt.getRedirectStatus().equals(RedirectStatus.FORWARD_WITH_SYNC));
    }
<<<<<<< HEAD
=======

    @Test
    public void testParser() {
        String sql = "SHOW ROUTINE LOAD TASK WHERE JobName = \"test1\";";
        List<StatementBase> stmts = com.starrocks.sql.parser.SqlParser.parse(sql, 32);
        ShowRoutineLoadTaskStmt loadTaskStmt = (ShowRoutineLoadTaskStmt)stmts.get(0);
        ShowStmtAnalyzer.analyze(loadTaskStmt, connectContext);
        Assert.assertEquals("test1", loadTaskStmt.getJobName());
    }

    @Test
    public void testShowRoutineLoadTask() throws SecurityException, IllegalArgumentException {
        String sql = "SHOW ROUTINE LOAD TASK FROM `db_test` WHERE JobName = \"rl_test\"";
        List<StatementBase> stmts = com.starrocks.sql.parser.SqlParser.parse(sql, connectContext.getSessionVariable());

        ShowRoutineLoadTaskStmt stmt = (ShowRoutineLoadTaskStmt)stmts.get(0);
        ShowStmtAnalyzer.analyze(stmt, connectContext);
        Assert.assertEquals("db_test", stmt.getDbFullName());
        Assert.assertEquals("rl_test", stmt.getJobName());
    }
>>>>>>> 9f4a558ed ([BugFix] Routine load name with backquotes (#11552))
}
