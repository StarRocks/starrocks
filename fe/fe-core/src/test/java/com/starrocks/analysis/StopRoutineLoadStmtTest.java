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
import com.starrocks.sql.ast.LabelName;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.StopRoutineLoadStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

public class StopRoutineLoadStmtTest {

    private static ConnectContext ctx;

    @BeforeAll
    public static void beforeClass() throws Exception {
        // create connect context
        ctx = UtFrameUtils.createDefaultCtx();
    }

    @Test
    public void testNormal() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ctx.setDatabase("testDb");

        StopRoutineLoadStmt stmt = new StopRoutineLoadStmt(new LabelName("testDb", "label"));

        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assertions.assertEquals("label", stmt.getName());
        Assertions.assertEquals("testDb", stmt.getDbFullName());
    }

    @Test
    public void testBackquote() throws SecurityException, IllegalArgumentException {
        String sql = "STOP ROUTINE LOAD FOR `db_test`.`rl_test`";
        List<StatementBase> stmts = com.starrocks.sql.parser.SqlParser.parse(sql, ctx.getSessionVariable());

        StopRoutineLoadStmt stmt = (StopRoutineLoadStmt) stmts.get(0);
        Assertions.assertEquals("db_test", stmt.getDbFullName());
        Assertions.assertEquals("rl_test", stmt.getName());
    }
}
