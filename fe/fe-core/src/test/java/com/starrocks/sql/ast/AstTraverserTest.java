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

import com.starrocks.sql.analyzer.AnalyzerUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * AstTraverser must reach every nested relation, because its consumers include security-sensitive
 * collectors: the security-policy marker (Ranger column masking / row-level filtering),
 * ColumnPrivilege's table collector, AuthorizerStmtVisitor and PlannerMetaLocker. A relation the
 * traverser does not reach is silently left out of those checks.
 */
public class AstTraverserTest {

    private static StatementBase parse(String sql) {
        return com.starrocks.sql.parser.SqlParser.parse(sql, 0L).get(0);
    }

    private static List<TableRelation> collectTableRelations(StatementBase stmt) {
        List<TableRelation> tableRelations = new ArrayList<>();
        new AstTraverser<Void, Void>() {
            @Override
            public Void visitTable(TableRelation node, Void context) {
                tableRelations.add(node);
                return null;
            }
        }.visit(stmt);
        return tableRelations;
    }

    private static void assertTablesReached(String sql, int expected) {
        Assertions.assertEquals(expected, collectTableRelations(parse(sql)).size(),
                "AstTraverser did not reach every table relation in: " + sql);
        // AnalyzerUtils uses its own AstTraverser subclass and is what ColumnPrivilege relies on.
        Assertions.assertEquals(expected, AnalyzerUtils.collectAllTableAndViewRelations(parse(sql)).size(),
                "collectAllTableAndViewRelations did not reach every table relation in: " + sql);
    }

    @Test
    public void testReachesRelationsInPlainQueries() {
        assertTablesReached("select * from db1.tbl1", 1);
        assertTablesReached("select * from db1.tbl1 a join db1.tbl2 b on a.k1 = b.k1", 2);
        assertTablesReached("select * from (select * from db1.tbl1) t", 1);
    }

    @Test
    public void testReachesRelationInsidePivot() {
        assertTablesReached("select * from db1.tbl1 PIVOT (max(k1) FOR k2 IN ('a'))", 1);
    }

    @Test
    public void testReachesRelationInsidePivotNestedInJoinAndSubquery() {
        assertTablesReached(
                "select * from (select * from db1.tbl1 PIVOT (max(k1) FOR k2 IN ('a'))) p "
                        + "join db1.tbl2 b on p.k2 = b.k2", 2);
    }

    @Test
    public void testReachesRelationInsidePreparedStatement() {
        assertTablesReached("PREPARE p1 FROM 'select * from db1.tbl1'", 1);
        assertTablesReached("PREPARE p2 FROM 'select * from db1.tbl1 a join db1.tbl2 b on a.k1 = b.k1'", 2);
    }

    @Test
    public void testReachesRelationInsidePivotWithinPreparedStatement() {
        assertTablesReached("PREPARE p3 FROM 'select * from db1.tbl1 PIVOT (max(k1) FOR k2 IN (1))'", 1);
    }
}
