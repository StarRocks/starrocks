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

import com.starrocks.authorization.SecurityPolicyRewriteRule;
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
        StatementBase statement = parse(sql);
        List<TableRelation> relations = collectTableRelations(statement);
        Assertions.assertEquals(expected, relations.size(),
                "AstTraverser did not reach every table relation in: " + sql);
        SecurityPolicyRewriteRule.markRelationsForRewrite(statement);
        Assertions.assertTrue(relations.stream().allMatch(Relation::isNeedRewrittenByPolicy),
                "Security-policy marker did not mark every table relation in: " + sql);
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
    public void testReachesRelationsInSelectListAndTableFunctionArguments() {
        assertTablesReached("select (select k1 from db1.tbl1) from db1.tbl2", 2);
        assertTablesReached("select * from unnest((select array_agg(k1) from db1.tbl1))", 1);
        assertTablesReached("select count(*) from db1.tbl1 "
                + "group by (select max(k1) from db1.tbl2)", 2);
        assertTablesReached("select * from (values ((select max(k1) from db1.tbl1))) v", 1);
    }

    @Test
    public void testReachesRelationsInParsedDmlSources() {
        // The write target is a TableRef, not a source Relation. Only the three source reads are
        // expected here.
        assertTablesReached("update db1.target set k1 = (select max(k1) from db1.tbl1) "
                + "from db1.tbl2 where k2 in (select k2 from db1.tbl3)", 3);
        assertTablesReached("delete from db1.target using db1.tbl1 "
                + "where k1 in (select k1 from db1.tbl2)", 2);
        assertTablesReached("merge into db1.target t using db1.tbl1 s on t.k1 = s.k1 "
                + "when matched then update set v1 = (select max(v1) from db1.tbl2)", 2);
    }

    @Test
    public void testAnalyzedDmlTraversesOnlySynthesizedQuery() {
        QueryStatement synthesized = (QueryStatement) parse("select * from db1.synthesized_source");

        UpdateStmt update = (UpdateStmt) parse("update db1.target set k1 = 1 from db1.raw_source");
        update.setQueryStatement(synthesized);
        Assertions.assertEquals(List.of("synthesized_source"), collectTableRelations(update).stream()
                .map(x -> x.getName().getTbl()).toList());

        DeleteStmt delete = (DeleteStmt) parse("delete from db1.target using db1.raw_source where k1 = 1");
        delete.setQueryStatement(synthesized);
        Assertions.assertEquals(List.of("synthesized_source"), collectTableRelations(delete).stream()
                .map(x -> x.getName().getTbl()).toList());

        MergeIntoStmt merge = (MergeIntoStmt) parse("merge into db1.target t using db1.raw_source s "
                + "on t.k1 = s.k1 when matched then delete");
        merge.setQueryStatement(synthesized);
        Assertions.assertEquals(List.of("synthesized_source"), collectTableRelations(merge).stream()
                .map(x -> x.getName().getTbl()).toList());
    }

    @Test
    public void testPrepareIsADeferredTraversalBoundary() {
        PrepareStmt prepareStmt = (PrepareStmt) parse("PREPARE p1 FROM 'select * from db1.tbl1'");
        Assertions.assertTrue(collectTableRelations(prepareStmt).isEmpty());
        SecurityPolicyRewriteRule.markRelationsForRewrite(prepareStmt);
        Assertions.assertFalse(collectTableRelations(prepareStmt.getInnerStmt()).get(0).isNeedRewrittenByPolicy());

        // PREPARE metadata and EXECUTE explicitly mark the inner executable statement.
        SecurityPolicyRewriteRule.markRelationsForRewrite(prepareStmt.getInnerStmt());
        List<TableRelation> innerRelations = collectTableRelations(prepareStmt.getInnerStmt());
        Assertions.assertEquals(1, innerRelations.size());
        Assertions.assertTrue(innerRelations.get(0).isNeedRewrittenByPolicy());
    }
}
