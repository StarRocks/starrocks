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

import com.starrocks.sql.ast.AlterSystemStmt;
import com.starrocks.sql.ast.FrontendClause;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.TransferLeaderClause;
import com.starrocks.sql.parser.SqlParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class TransferLeaderClauseTest {

    private TransferLeaderClause parse(String sql) {
        List<StatementBase> stmts = SqlParser.parse(sql, 0);
        Assertions.assertEquals(1, stmts.size());
        AlterSystemStmt alter = (AlterSystemStmt) stmts.get(0);
        // TransferLeaderClause is a FrontendClause, so the analyzer routes it through
        // visitFrontendClause to validate host:port.
        Assertions.assertTrue(alter.getAlterClause() instanceof FrontendClause);
        return (TransferLeaderClause) alter.getAlterClause();
    }

    @Test
    public void testParseTransferLeader() {
        TransferLeaderClause clause = parse("ALTER SYSTEM TRANSFER LEADER TO \"127.0.0.1:9010\"");
        Assertions.assertEquals("127.0.0.1:9010", clause.getHostPort());
        Assertions.assertFalse(clause.isForce());
    }

    @Test
    public void testParseTransferLeaderForce() {
        TransferLeaderClause clause = parse("ALTER SYSTEM TRANSFER LEADER TO \"127.0.0.1:9010\" FORCE");
        Assertions.assertEquals("127.0.0.1:9010", clause.getHostPort());
        Assertions.assertTrue(clause.isForce());
    }

    @Test
    public void testLeaderAndTransferRemainUsableAsIdentifiers() {
        // LEADER / TRANSFER are added as non-reserved keywords, so existing SQL using them as
        // identifiers (table/column names) must still parse.
        List<StatementBase> stmts = SqlParser.parse("SELECT leader, transfer FROM t_transfer", 0);
        Assertions.assertEquals(1, stmts.size());
    }
}
