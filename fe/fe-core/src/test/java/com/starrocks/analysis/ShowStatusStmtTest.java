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
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.SetType;
import com.starrocks.sql.ast.ShowStatusStmt;
import com.starrocks.sql.parser.SqlParser;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ShowStatusStmtTest {

    @Mocked
    private ConnectContext ctx;

    @Test
    public void testNormal() throws Exception {
        {
            ShowStatusStmt stmt = (ShowStatusStmt) SqlParser.parse("SHOW STATUS", 32).get(0);
            Analyzer.analyze(stmt, ctx);
            Assertions.assertNull(stmt.getPattern());
            Assertions.assertNull(stmt.getWhere());
        }

        {
            ShowStatusStmt stmt = (ShowStatusStmt) SqlParser.parse("SHOW GLOBAL STATUS", 32).get(0);
            Analyzer.analyze(stmt, ctx);
            Assertions.assertEquals(SetType.GLOBAL, stmt.getType());
            Assertions.assertNull(stmt.getPattern());
            Assertions.assertNull(stmt.getWhere());
        }

        {
            ShowStatusStmt stmt = (ShowStatusStmt) SqlParser.parse("SHOW LOCAL STATUS", 32).get(0);
            Analyzer.analyze(stmt, ctx);
            Assertions.assertNull(stmt.getPattern());
            Assertions.assertNull(stmt.getWhere());
        }

        {
            ShowStatusStmt stmt = (ShowStatusStmt) SqlParser.parse("SHOW SESSION STATUS", 32).get(0);
            Analyzer.analyze(stmt, ctx);
            Assertions.assertEquals(SetType.SESSION, stmt.getType());
            Assertions.assertNull(stmt.getPattern());
            Assertions.assertNull(stmt.getWhere());
        }

        {
            ShowStatusStmt stmt = (ShowStatusStmt) SqlParser.parse("SHOW STATUS like 'abc'", 32).get(0);
            Analyzer.analyze(stmt, ctx);
            Assertions.assertNotNull(stmt.getPattern());
            Assertions.assertEquals("abc", stmt.getPattern());
            Assertions.assertNull(stmt.getWhere());
        }

        {
            ShowStatusStmt stmt = (ShowStatusStmt) SqlParser.parse("SHOW STATUS where abc=123", 32).get(0);
            Assertions.assertNull(stmt.getPattern());
            Assertions.assertEquals("abc = 123", stmt.getWhere().toSql());
        }

        {
            ShowStatusStmt stmt = new ShowStatusStmt();
            Assertions.assertNotNull(stmt.getType());
            Assertions.assertEquals(SetType.SESSION, stmt.getType());
            Assertions.assertNull(stmt.getPattern());
            Assertions.assertNull(stmt.getWhere());
        }
    }
}
