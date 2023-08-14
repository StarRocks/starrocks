// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.starrocks.analysis;

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.ShowRoutineLoadStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class ShowRoutineLoadStmtTest {

    private static ConnectContext ctx;

    @BeforeClass
    public static void beforeClass() throws Exception {
        // create connect context
        ctx = UtFrameUtils.createDefaultCtx();
    }

    @Test
    public void testNormal() throws Exception {
        ctx = UtFrameUtils.createDefaultCtx();
        ctx.setDatabase("testDb");

        ShowRoutineLoadStmt stmt = new ShowRoutineLoadStmt(new LabelName("testDb", "label"), false);

        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals("label", stmt.getName());
        Assert.assertEquals("testDb", stmt.getDbFullName());
        Assert.assertFalse(stmt.isIncludeHistory());
        Assert.assertEquals(19, stmt.getMetaData().getColumnCount());
        Assert.assertEquals("Id", stmt.getMetaData().getColumn(0).getName());
    }

    @Test
    public void testFromDB() {
        ctx = UtFrameUtils.createDefaultCtx();
        ctx.setDatabase("testDb");

        ShowRoutineLoadStmt stmt = new ShowRoutineLoadStmt(new LabelName("testDb", null), false);

        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals("testDb", stmt.getDbFullName());
    }

    @Test
    public void testBackquote() throws SecurityException, IllegalArgumentException {
        String sql = "SHOW ROUTINE LOAD FOR `rl_test` FROM `db_test` WHERE state = 'RUNNING' ORDER BY `CreateTime` desc";
        List<StatementBase> stmts = com.starrocks.sql.parser.SqlParser.parse(sql, ctx.getSessionVariable());

        ShowRoutineLoadStmt stmt = (ShowRoutineLoadStmt) stmts.get(0);
        Assert.assertEquals("db_test", stmt.getDbFullName());
        Assert.assertEquals("rl_test", stmt.getName());
    }

    @Test
    public void testWithoutLabel() {
        String sql = "SHOW ROUTINE LOAD";
        List<StatementBase> stmts = com.starrocks.sql.parser.SqlParser.parse(sql, ctx.getSessionVariable());
        ShowRoutineLoadStmt stmt = (ShowRoutineLoadStmt) stmts.get(0);
        Assert.assertNull(stmt.getName());
        Assert.assertNull(stmt.getDbFullName());
    }

    @Test
    public void testWithDB() {
        String sql = "SHOW ROUTINE LOAD FROM db_test";
        List<StatementBase> stmts = com.starrocks.sql.parser.SqlParser.parse(sql, ctx.getSessionVariable());
        ShowRoutineLoadStmt stmt = (ShowRoutineLoadStmt) stmts.get(0);
        Assert.assertNull(stmt.getName());
        Assert.assertEquals("db_test", stmt.getDbFullName());
    }
}
