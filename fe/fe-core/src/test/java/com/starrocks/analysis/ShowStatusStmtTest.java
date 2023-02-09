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

import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.SetType;
import com.starrocks.sql.ast.ShowStatusStmt;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.qe.ConnectContext;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

public class ShowStatusStmtTest {

    @Mocked
    private ConnectContext ctx;

    @Test
    public void testNormal() throws Exception {
        {
            ShowStatusStmt stmt = (ShowStatusStmt) SqlParser.parse("SHOW STATUS", 32).get(0);
            Analyzer.analyze(stmt, ctx);
            Assert.assertEquals(SetType.SESSION, stmt.getType());
            Assert.assertNull(stmt.getPattern());
            Assert.assertNull(stmt.getWhere());
        }

        {
            ShowStatusStmt stmt = (ShowStatusStmt) SqlParser.parse("SHOW GLOBAL STATUS", 32).get(0);
            Analyzer.analyze(stmt, ctx);
            Assert.assertEquals(SetType.GLOBAL, stmt.getType());
            Assert.assertNull(stmt.getPattern());
            Assert.assertNull(stmt.getWhere());
        }

        {
            ShowStatusStmt stmt = (ShowStatusStmt) SqlParser.parse("SHOW LOCAL STATUS", 32).get(0);
            Analyzer.analyze(stmt, ctx);
            Assert.assertEquals(SetType.SESSION, stmt.getType());
            Assert.assertNull(stmt.getPattern());
            Assert.assertNull(stmt.getWhere());
        }

        {
            ShowStatusStmt stmt = (ShowStatusStmt) SqlParser.parse("SHOW SESSION STATUS", 32).get(0);
            Analyzer.analyze(stmt, ctx);
            Assert.assertEquals(SetType.SESSION, stmt.getType());
            Assert.assertNull(stmt.getPattern());
            Assert.assertNull(stmt.getWhere());
        }

        {
            ShowStatusStmt stmt = (ShowStatusStmt) SqlParser.parse("SHOW STATUS like 'abc'", 32).get(0);
            Analyzer.analyze(stmt, ctx);
            Assert.assertEquals(SetType.SESSION, stmt.getType());
            Assert.assertNotNull(stmt.getPattern());
            Assert.assertEquals("abc", stmt.getPattern());
            Assert.assertNull(stmt.getWhere());
        }

        {
            ShowStatusStmt stmt = (ShowStatusStmt) SqlParser.parse("SHOW STATUS where abc=123", 32).get(0);
            Assert.assertEquals(SetType.SESSION, stmt.getType());
            Assert.assertNull(stmt.getPattern());
            Assert.assertEquals("abc = 123", stmt.getWhere().toSql());
        }

        {
            ShowStatusStmt stmt = new ShowStatusStmt();
            Assert.assertNotNull(stmt.getType());
            Assert.assertEquals(SetType.SESSION, stmt.getType());
            Assert.assertNull(stmt.getPattern());
            Assert.assertNull(stmt.getWhere());
        }
    }
}
