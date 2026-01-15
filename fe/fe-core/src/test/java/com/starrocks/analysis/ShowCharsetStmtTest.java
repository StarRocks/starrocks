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
import com.starrocks.sql.ast.ShowCharsetStmt;
import com.starrocks.sql.ast.expression.ExprToSql;
import com.starrocks.sql.parser.SqlParser;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ShowCharsetStmtTest  {
    @Mocked
    private ConnectContext ctx;

    @Test
    public void testShowCharset() throws Exception {
        {
            ShowCharsetStmt stmt = (ShowCharsetStmt) SqlParser.parse("SHOW CHARSET", 32).get(0);
            Analyzer.analyze(stmt, ctx);
            Assertions.assertNull(stmt.getPattern());
            Assertions.assertNull(stmt.getWhere());
        }

        {
            ShowCharsetStmt stmt = (ShowCharsetStmt) SqlParser.parse("SHOW CHAR SET", 32).get(0);
            Analyzer.analyze(stmt, ctx);
            Assertions.assertNull(stmt.getPattern());
            Assertions.assertNull(stmt.getWhere());
        }

        {
            ShowCharsetStmt stmt = (ShowCharsetStmt) SqlParser.parse("SHOW CHARSET LIKE 'abc'", 32).get(0);
            Analyzer.analyze(stmt, ctx);
            Assertions.assertEquals("abc", stmt.getPattern());
            Assertions.assertNull(stmt.getWhere());
        }

        {
            ShowCharsetStmt stmt = (ShowCharsetStmt) SqlParser.parse("SHOW CHARSET WHERE Maxlen>1", 32).get(0);
            Analyzer.analyze(stmt, ctx);
            Assertions.assertNull(stmt.getPattern());
            Assertions.assertEquals("Maxlen > 1", ExprToSql.toSql(stmt.getWhere()));
        }

        {
            ShowCharsetStmt stmt = new ShowCharsetStmt();
            Analyzer.analyze(stmt, ctx);
            Assertions.assertNull(stmt.getPattern());
            Assertions.assertNull(stmt.getWhere());
        }
    }
}