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

package com.starrocks.sql.parser;

import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.AddRollupClause;
import com.starrocks.sql.ast.AlterTableStmt;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AddRollupOrderByParserTest {

    private static AlterTableStmt parse(String sql) {
        return (AlterTableStmt) SqlParser.parse(sql, new SessionVariable()).get(0);
    }

    @Test
    void testAddRollupWithOrderBy() {
        String sql = "ALTER TABLE t ADD ROLLUP r2 (k1, k2, v1) ORDER BY (k2, k1)";
        AlterTableStmt stmt = parse(sql);
        AddRollupClause clause = (AddRollupClause) stmt.getAlterClauseList().get(0);
        assertEquals(List.of("k2", "k1"), clause.getSortKeys());
    }

    @Test
    void testAddRollupWithoutOrderByHasEmptySortKeys() {
        String sql = "ALTER TABLE t ADD ROLLUP r2 (k1, k2, v1)";
        AddRollupClause clause = (AddRollupClause) parse(sql).getAlterClauseList().get(0);
        assertTrue(clause.getSortKeys().isEmpty());
    }

    @Test
    void testAddRollupOrderByToSqlRoundTrip() {
        String sql = "ALTER TABLE t ADD ROLLUP r2 (k1, k2, v1) ORDER BY (k2, k1)";
        AddRollupClause clause = (AddRollupClause) parse(sql).getAlterClauseList().get(0);
        String toSql = clause.toSql();
        assertTrue(toSql.contains("ORDER BY"), "toSql should include ORDER BY clause but was: " + toSql);
        assertTrue(toSql.contains("k2"), "toSql should contain sort key k2 but was: " + toSql);
        assertTrue(toSql.contains("k1"), "toSql should contain sort key k1 but was: " + toSql);
    }
}
