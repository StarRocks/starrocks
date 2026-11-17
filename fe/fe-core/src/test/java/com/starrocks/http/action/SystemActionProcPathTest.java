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

package com.starrocks.http.action;

import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.ShowProcStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class SystemActionProcPathTest {

    @Test
    public void testShowProcStmtKeepsPathAsSingleLiteral() {
        String[] paths = {
                "/dbs/10001/tbl_a&b",
                "/dbs/10001/o'brien",
                "/dbs/10001/a\"b",
                "/dbs/10001/a\\b",
                "/dbs/10001/a\\nb",
                "/dbs/10001/a\\\"; SELECT 1; -- ",
        };
        for (String path : paths) {
            String sql = SystemAction.buildShowProcStmt(path);
            List<StatementBase> stmts = SqlParser.parse(sql, new SessionVariable());
            Assertions.assertEquals(1, stmts.size(), sql);
            Assertions.assertInstanceOf(ShowProcStmt.class, stmts.get(0), sql);
            Assertions.assertEquals(path, ((ShowProcStmt) stmts.get(0)).getPath(), sql);
        }
    }

    @Test
    public void testEncodeProcPathForUrl() {
        Assertions.assertEquals("/", SystemAction.encodeProcPathForUrl("/"));
        Assertions.assertEquals("//backends", SystemAction.encodeProcPathForUrl("//backends"));
        Assertions.assertEquals("/dbs/10001", SystemAction.encodeProcPathForUrl("/dbs/10001"));
        // Whitespace is not covered: ProcService.open treats it as the end of the path.
        Assertions.assertEquals("/catalog/hive/tbl_a%26b%2Bc%23d",
                SystemAction.encodeProcPathForUrl("/catalog/hive/tbl_a&b+c#d"));
        Assertions.assertEquals("/dbs/a%22%3E%3Cimg", SystemAction.encodeProcPathForUrl("/dbs/a\"><img"));
    }
}
