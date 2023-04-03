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


package com.starrocks.sql.ast;

import com.starrocks.qe.ConnectContext;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

public class ShowProcedureStmtTest {
    @Mocked
    private ConnectContext ctx;

    @Test
    public void testNormal() throws Exception {

        ShowProcedureStmt stmt = (ShowProcedureStmt) com.starrocks.sql.parser.SqlParser.parse(
                "SHOW PROCEDURE STATUS", 32).get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);

        stmt = (ShowProcedureStmt) com.starrocks.sql.parser.SqlParser.parse(
                "SHOW PROCEDURE STATUS LIKE 'abc'", 32).get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals("abc", stmt.getPattern());

        stmt = (ShowProcedureStmt) com.starrocks.sql.parser.SqlParser.parse(
                "SHOW PROCEDURE STATUS where name='abc'", 32).get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);

        stmt = new ShowProcedureStmt("abc", null);
        Assert.assertNotNull(stmt.getPattern());
        Assert.assertEquals(11, stmt.getMetaData().getColumnCount());
        Assert.assertEquals("Db", stmt.getMetaData().getColumn(0).getName());

        // MySQLWorkbench use
        stmt = (ShowProcedureStmt) com.starrocks.sql.parser.SqlParser.parse(
                "SHOW FUNCTION STATUS where Db='abc'", 32).get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
    }
}
