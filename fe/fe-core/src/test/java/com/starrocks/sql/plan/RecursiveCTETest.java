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

package com.starrocks.sql.plan;

import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.qe.recursivecte.RecursiveCTEAstCheck;
import com.starrocks.qe.recursivecte.RecursiveCTEExecutor;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.CreateTableAsSelectStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class RecursiveCTETest extends PlanTestBase {

    public String explainRecursiveCte(String sql) throws Exception {
        List<StatementBase> statements;
        try (Timer ignored = Tracers.watchScope("Parser")) {
            statements = SqlParser.parse(sql, connectContext.getSessionVariable());
        }
        StatementBase statementBase = statements.get(0);

        connectContext.getSessionVariable().setEnableRecursiveCTE(true);
        if (RecursiveCTEAstCheck.hasRecursiveCte(statementBase, connectContext)) {
            RecursiveCTEExecutor executor = new RecursiveCTEExecutor(connectContext);
            StatementBase sb = executor.splitOuterStmt(statementBase);
            return executor.explainCTE(sb);
        }
        return getCostExplain(sql);
    }

    @Test
    public void testSimpleRecursiveCTE() throws Exception {
        String sql = "with recursive cte as " +
                "(select v1 from t0 union all select v1 + 1 from cte where v1 < 10) " +
                "select * from cte";
        String plan = explainRecursiveCte(sql);
        assertContains(plan, "SELECT `test`.`t0`.`v1`\n"
                + "FROM `test`.`t0`");
        assertContains(plan, "Recursive Statement: SELECT `cte`.`v1` + 1 AS `v1 + 1`");
        assertContains(plan, "Outer Statement After Rewriting:\n"
                + "SELECT `cte`.`v1`");
    }

    @Test
    public void testRecursiveCTEWithColumnNames() throws Exception {
        String sql = "with recursive cte(id, parent_id) as " +
                "(select v1, v2 from t0 where v2 is null " +
                "union all " +
                "select t0.v1, t0.v2 from t0 join cte on t0.v2 = cte.id) " +
                "select * from cte";
        String plan = explainRecursiveCte(sql);
        assertContains(plan, "SELECT `test`.`t0`.`v1`, `test`.`t0`.`v2`\n"
                + "FROM `test`.`t0`\n"
                + "WHERE `test`.`t0`.`v2` IS NULL");
        assertContains(plan, ".`parent_id`\n"
                + "FROM `test`.`cte_");
        assertContains(plan, "Outer Statement After Rewriting:\n"
                + "SELECT `cte`.`id`, `cte`.`parent_id`");
    }

    @Test
    public void testRecursiveCteCtasReusesInferredColumns() throws Exception {
        String sql = "create table recursive_ctas as " +
                "with recursive cte(s) as " +
                "(select cast('a' as varchar(10)) " +
                "union all select cast(concat(s, 'a') as varchar(10)) from cte where length(s) < 2) " +
                "select s from cte";
        StatementBase statement = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        CreateTableAsSelectStmt ctas = (CreateTableAsSelectStmt) statement;
        connectContext.getSessionVariable().setEnableRecursiveCTE(true);

        RecursiveCTEExecutor executor = new RecursiveCTEExecutor(connectContext);
        executor.splitOuterStmt(statement);

        Assertions.assertEquals(1, ctas.getCreateTableStmt().getColumnDefs().size());
        Assertions.assertEquals("varchar(10)",
                ctas.getCreateTableStmt().getColumnDefs().get(0).getType().toSql().toLowerCase());
    }

    @Test
    public void testRecursiveCteCtasValidatesOuterStatementBeforeSplit() {
        String sql = "create table recursive_ctas(a, b) as " +
                "with recursive cte(n) as " +
                "(select 1 union all select n + 1 from cte where n < 3) " +
                "select n from cte";
        StatementBase statement = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        connectContext.getSessionVariable().setEnableRecursiveCTE(true);

        RecursiveCTEExecutor executor = new RecursiveCTEExecutor(connectContext);
        Assertions.assertThrows(SemanticException.class, () -> executor.splitOuterStmt(statement));
    }

    @Test
    public void testMultipleRecursiveCTE() throws Exception {
        String sql = "with recursive " +
                "cte1 as (select v1 from t0 union all select v1 + 1 from cte1 where v1 < 5), " +
                "cte2 as (select v4 from t1 union all select v4 + 1 from cte2 where v4 < 5) " +
                "select * from cte1, cte2";
        String plan = explainRecursiveCte(sql);
        assertContains(plan, "Recursive CTE Name: cte1\n"
                + "Temporary Table: cte1_");
        assertContains(plan, "Recursive CTE Name: cte2\n"
                + "Temporary Table: cte2_");
        assertContains(plan, "WHERE `cte1`.`v1` < 5");
        assertContains(plan, "WHERE `cte2`.`v4` < 5");
    }

    @Test
    public void testMultipleRecursiveCTE2() throws Exception {
        String sql = "with recursive " +
                "cte1 as (select v1 from t0   union all select v1 + 1 from cte1 where v1 < 5), " +
                "cte2 as (select v1 from cte1 union all select v1 + 1 from cte2 where v1 < 5) " +
                "select * from cte1, cte2";
        String plan = explainRecursiveCte(sql);
        assertContains(plan, "Recursive CTE Name: cte1\n"
                + "Temporary Table: cte1_");
        assertContains(plan, "Recursive CTE Name: cte2\n"
                + "Temporary Table: cte2_");
        assertContains(plan, "WHERE `cte1`.`v1` < 5");
        assertContains(plan, "WHERE `cte2`.`v1` < 5");
    }

    @Test
    public void testMultipleRecursiveCTE3() throws Exception {
        String sql = "with recursive " +
                "cte1 as (select v1 from t0 union all "
                + "select v1 + 1 from cte1 where v1 < 5 union all "
                + "select v1 + 2 from cte1 where v1 < 10)" +
                "select * from cte1";
        String plan = explainRecursiveCte(sql);
        assertContains(plan, "Recursive CTE Name: cte1\n"
                + "Temporary Table: cte1_");
        assertContains(plan, "WHERE `cte1`.`v1` < 5");
        assertContains(plan, "UNION ALL");
    }

    @Test
    public void testNestedRecursiveCTE() throws Exception {
        String sql = "with recursive cte1 as " +
                "(select v1 from t0 union all select v1 + 1 from cte1 where v1 < 10) " +
                "select * from (select * from cte1 where v1 > 5) t";
        String plan = explainRecursiveCte(sql);
        assertContains(plan, ".*, 0 AS `_cte_level_");
        assertContains(plan, "WHERE `cte1`.`v1` < 10");
        assertContains(plan, "WHERE `cte1`.`v1` > 5) `t`");
    }

    @Test
    public void testUnionRecursiveCTE() throws Exception {
        String sql = "with recursive cte as " +
                "(select v1 from t0 union select v1 + 1 from cte where v1 < 10) " +
                "select * from cte";
        String plan = explainRecursiveCte(sql);
        assertContains(plan, "SELECT `test`.`t0`.`v1`\n"
                + "FROM `test`.`t0`");
        assertContains(plan, "Recursive Statement: SELECT `cte`.`v1` + 1 AS `v1 + 1`");
        assertContains(plan, "Outer Statement After Rewriting:\n"
                + "SELECT `cte`.`v1`");
    }

    @Test
    public void testNestedRecursiveCteCrossScopeReferenceRejected() throws Exception {
        // A recursive CTE nested inside another one, whose recursive member references the OUTER
        // recursive CTE. The single-value recursive-CTE marker used to lose the outer name, so the
        // table collectors expanded the outer CTE without end (StackOverflowError / hang). It must be
        // rejected with a clear error, not blow the stack.
        String sql = "with recursive q as ("
                + " select v1, v2, v3 from t0"
                + " union all ("
                + "   with recursive x as ("
                + "     select v1, v2, v3 from t0"
                + "     union all ("
                + "       select v1, v2, v3 from q where v1 < 10"
                + "       union all"
                + "       select v1, v2, v3 from x where v2 < 10"
                + "     )"
                + "   )"
                + "   select v1, v2, v3 from x"
                + " )"
                + ") select v1 from q";
        connectContext.getSessionVariable().setEnableRecursiveCTE(true);
        SemanticException e = Assertions.assertThrows(SemanticException.class, () -> getFragmentPlan(sql));
        Assertions.assertTrue(e.getMessage().contains("Doesn't support multi-level recursive CTE"),
                e.getMessage());
    }

    @Test
    public void testNestedRecursiveCteEnableFalseAlsoRejected() throws Exception {
        // With enable_recursive_cte=false the RecursiveCTEAstCheck multi-level guard does not run
        // (it is gated on the session var), so the nested reference reaches QueryAnalyzer. It must
        // still be rejected there instead of overflowing the stack in the table collectors.
        String sql = "with recursive q as ("
                + " select v1, v2, v3 from t0"
                + " union all ("
                + "   with recursive x as ("
                + "     select v1, v2, v3 from t0"
                + "     union all ("
                + "       select v1, v2, v3 from q where v1 < 10"
                + "       union all"
                + "       select v1, v2, v3 from x where v2 < 10"
                + "     )"
                + "   )"
                + "   select v1, v2, v3 from x"
                + " )"
                + ") select v1 from q";
        connectContext.getSessionVariable().setEnableRecursiveCTE(false);
        Assertions.assertThrows(SemanticException.class, () -> getFragmentPlan(sql));
    }

    @Test
    public void testRecursiveCteReferencedOnlyInScalarSubqueryRejected() throws Exception {
        // The recursive member's FROM is a base table; the recursive CTE q is referenced only inside a
        // scalar subquery. That reference is resolved by the fresh QueryAnalyzer the subquery spawns,
        // whose own recursiveCteStack is empty, so the per-Visitor guard cannot see it. The
        // session-shared recursive-CTE path must catch it; before the fix it slipped through and the
        // table collectors expanded q forever ("Unknown error" / StackOverflowError).
        String sql = "with recursive q as ("
                + " select v1, v2, v3 from t0"
                + " union all"
                + " select v1, v2, v3 from t0 where v1 < (select max(v1) from q)"
                + ") select v1 from q";
        connectContext.getSessionVariable().setEnableRecursiveCTE(false);
        SemanticException e = Assertions.assertThrows(SemanticException.class, () -> getFragmentPlan(sql));
        Assertions.assertTrue(e.getMessage().contains("Doesn't support multi-level recursive CTE"),
                e.getMessage());
    }

    @Test
    public void testNestedRecursiveCteReferencedOnlyInSelectListSubqueryRejected() throws Exception {
        // The outer recursive CTE q is referenced only from a SELECT-list subquery inside the nested
        // recursive CTE x. Before AstTraverser visited the pre-analysis selectList, RecursiveCTEAstCheck
        // never saw this reference; it must now be rejected as a multi-level recursive CTE.
        String sql = "with recursive q as ("
                + " select v1, v2, v3 from t0"
                + " union all ("
                + "   with recursive x as ("
                + "     select v1, v2, v3 from t0"
                + "     union all"
                + "     select (select max(v1) from q), v2, v3 from x where v2 < 10"
                + "   )"
                + "   select v1, v2, v3 from x"
                + " )"
                + ") select v1 from q";
        connectContext.getSessionVariable().setEnableRecursiveCTE(true);
        SemanticException e = Assertions.assertThrows(SemanticException.class, () -> getFragmentPlan(sql));
        Assertions.assertTrue(e.getMessage().contains("Doesn't support multi-level recursive CTE"),
                e.getMessage());
    }

    @Test
    public void testRecursiveCteReferencedOnlyInInSubqueryRejected() throws Exception {
        // Same as above but the reference hides in an IN subquery instead of a scalar one.
        String sql = "with recursive q as ("
                + " select v1, v2, v3 from t0"
                + " union all"
                + " select v1, v2, v3 from t0 where v1 in (select v1 from q)"
                + ") select v1 from q";
        connectContext.getSessionVariable().setEnableRecursiveCTE(false);
        SemanticException e = Assertions.assertThrows(SemanticException.class, () -> getFragmentPlan(sql));
        Assertions.assertTrue(e.getMessage().contains("Doesn't support multi-level recursive CTE"),
                e.getMessage());
    }

    @Test
    public void testRecursiveMemberSubqueryOverOtherRelationNotRejected() throws Exception {
        // Negative control: a subquery inside the recursive member that does NOT reference the
        // recursive CTE (only a base table) must plan normally. The session-shared path only holds
        // the recursive CTE's own name, so an unrelated subquery is never rejected.
        String sql = "with recursive cte as ("
                + " select v1 from t0"
                + " union all"
                + " select v1 + 1 from cte where v1 < (select max(v1) from t0)"
                + ") select * from cte";
        String plan = explainRecursiveCte(sql);
        assertContains(plan, "Recursive Statement:");
    }

    @Test
    public void testRecursiveCTEInNormalProcess() {
        String sql = "with recursive cte as " +
                "(select v1 from t0 union select v1 + 1 from cte where v1 < 10) " +
                "select * from cte";
        Assertions.assertThrows(SemanticException.class, () -> getFragmentPlan(sql), "Recursive CTE is not supported.");
    }

    @Test
    public void testNormalCTE() throws Exception {
        String sql = "with recursive cte as " +
                "(select 100 as x union all select 123456.789 as y) " +
                "select * from cte";
        String plan = explainRecursiveCte(sql);
        assertContains(plan, "constant exprs: \n"
                + "         100\n"
                + "         123456.789");
    }

    @Test
    public void testRecursiveCTEWithMultiUnion() {
        String sql = "with recursive cte1 as " +
                "(select v1 from t0 union all "
                + "select v1 + 1 from cte1 where v1 < 10 union "
                + "select v1 + 10 from cte1 where v1 < 5)" +
                "select * from (select * from cte1 where v1 > 5) t";
        Assertions.assertThrows(SemanticException.class, () -> explainRecursiveCte(sql), "Unknown table");
    }

    @Test
    public void testRecursiveCTEWithNonRecursive() throws Exception {
        String sql = "with recursive cte2 as (select v4,v5,v6 from t1), "
                + "cte1 as " +
                "(select v1, v2, v3 from t0 "
                + "union all "
                + "select v1 + 1, v5 + 1, v6 + v3 from cte1, cte2 where v2 = v6 and v1 < 10)" +
                "select * from cte1, cte2 where v1 > 5";
        String plan = explainRecursiveCte(sql);
        assertContains(plan, "Start Statement: WITH `cte2` (`v4`, `v5`, `v6`)");
        assertContains(plan, "Recursive Statement: SELECT `cte1`.`v1` + 1 AS `v1 + 1`, `cte2`.`v5` + 1");
        assertContains(plan, "Outer Statement After Rewriting:\n"
                + "WITH RECURSIVE `cte2`");
    }

    private boolean hasRecursiveCte(String sql) {
        StatementBase statement = SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        connectContext.getSessionVariable().setEnableRecursiveCTE(true);
        return RecursiveCTEAstCheck.hasRecursiveCte(statement, connectContext);
    }

    @Test
    public void testOrdinaryNestedCteShadowingTableIsNotRecursive() throws Exception {
        // The inner CTE t0 is not visible in its own definition, so the t0 in the scalar subquery is the
        // base table t0. This is an ordinary nested CTE, not a (multi-level) recursive one.
        String sql = "with a as ("
                + " with t0 as (select (select count(*) from t0) as n)"
                + " select n from t0"
                + ") select n from a";
        Assertions.assertFalse(hasRecursiveCte(sql));
        connectContext.getSessionVariable().setEnableRecursiveCTE(true);
        getFragmentPlan(sql);
    }

    @Test
    public void testOrdinaryCteSelfNameIsNotRecursive() {
        // Without WITH RECURSIVE, or when the definition is not a UNION, the CTE name inside its own
        // definition refers to the base table.
        Assertions.assertFalse(hasRecursiveCte(
                "with t0 as (select v1 from t0 union all select v1 + 1 from t0) select * from t0"));
        Assertions.assertFalse(hasRecursiveCte(
                "with recursive t0 as (select v1 from t0 where v1 > 1) select * from t0"));
    }

    @Test
    public void testInnerCteShadowsOuterRecursiveCte() throws Exception {
        // The q inside the recursive member resolves to the inner ordinary CTE q, so the only
        // recursive reference is the one in FROM cte.
        String sql = "with recursive cte as ("
                + " select v1 from t0"
                + " union all"
                + " select v1 + 1 from cte where v1 < (with cte as (select 1 as v1) select max(v1) from cte)"
                + ") select * from cte";
        Assertions.assertTrue(hasRecursiveCte(sql));

        String onlyShadowed = "with recursive cte as ("
                + " select v1 from t0"
                + " union all"
                + " select * from (with cte as (select 1 as v1) select v1 from cte) t"
                + ") select * from cte";
        Assertions.assertFalse(hasRecursiveCte(onlyShadowed));
    }

    @Test
    public void testRecursiveCteDetectionUnchanged() {
        Assertions.assertTrue(hasRecursiveCte("with recursive cte as "
                + "(select v1 from t0 union all select v1 + 1 from cte where v1 < 10) select * from cte"));
        // a recursive CTE nested in an ordinary CTE is still a single-level recursive CTE
        Assertions.assertTrue(hasRecursiveCte("with a as ("
                + " with recursive cte as (select v1 from t0 union all select v1 + 1 from cte where v1 < 10)"
                + " select v1 from cte"
                + ") select v1 from a"));
        // a recursive CTE referenced only from the outer query is not recursive
        Assertions.assertFalse(hasRecursiveCte("with recursive cte as "
                + "(select v1 from t0 union all select v2 from t0) select * from cte"));
        // nested recursive CTEs are still rejected
        SemanticException e = Assertions.assertThrows(SemanticException.class, () -> hasRecursiveCte(
                "with recursive q as ("
                        + " select v1 from t0"
                        + " union all ("
                        + "   with recursive x as (select v1 from t0 union all select v1 from x where v1 < 10)"
                        + "   select v1 from x"
                        + " )"
                        + ") select v1 from q"));
        Assertions.assertTrue(e.getMessage().contains("Doesn't support multi-level recursive CTE"), e.getMessage());
    }

    @Test
    public void testRecursiveCteNotVisibleInItsAnchor() throws Exception {
        // As in QueryAnalyzer, a recursive CTE enters the scope only after its anchor, so the t0 in the
        // anchor (here inside the nested recursive CTE x) is the base table, not a multi-level reference.
        String sql = "with recursive t0(v1) as ("
                + " (with recursive x as (select v1 from t0 union all select 1) select v1 from x)"
                + " union all"
                + " select v1 + 1 from t0 where v1 < 3"
                + ") select * from t0";
        Assertions.assertTrue(hasRecursiveCte(sql));
        String plan = explainRecursiveCte(sql);
        assertContains(plan, "Recursive Statement:");
    }

    @Test
    public void testCteShadowingIsCaseSensitive() {
        // With case-sensitive table names, the ordinary CTE R does not shadow the recursive CTE r.
        String sql = "with recursive r(n) as ("
                + " select 1"
                + " union all"
                + " (with R as (select 42 as n) select n + 1 from r where n < 3)"
                + ") select * from r";
        Assertions.assertTrue(hasRecursiveCte(sql));
    }

    @Test
    public void testRecursiveCteInsideAnchorRejected() {
        // A real recursive CTE nested in the anchor of another recursive CTE is still multi-level,
        // whether or not the two share a name.
        for (String inner : new String[] {"q", "x"}) {
            String sql = "with recursive q(n) as ("
                    + " (with recursive " + inner + "(n) as ("
                    + "   select 1 union all select n + 1 from " + inner + " where n < 2"
                    + " ) select n from " + inner + ")"
                    + " union all"
                    + " select n + 1 from q where n < 3"
                    + ") select n from q";
            SemanticException e = Assertions.assertThrows(SemanticException.class, () -> hasRecursiveCte(sql));
            Assertions.assertTrue(e.getMessage().contains("Doesn't support multi-level recursive CTE"),
                    e.getMessage());
        }
    }

    @Test
    public void testCteOnParenthesizedQueryShadowsOuterRecursiveCte() {
        // WITH r AS (...) (SELECT ...) attaches the inner r to a parenthesized query; that r shadows the
        // outer recursive candidate r, so there is no recursive reference.
        String sql = "with recursive r(n) as ("
                + " select 1"
                + " union all"
                + " select n from (with r(n) as (select 2) (select n from r)) x"
                + ") select * from r";
        Assertions.assertFalse(hasRecursiveCte(sql));
    }

    @Test
    public void testSubqueryCteShadowingRecursiveCteNameIsNotRecursive() throws Exception {
        // The q inside the subquery is an ordinary CTE that shadows the recursive candidate q. The analyzer used
        // to match the recursive CTE by name and mark that inner q recursive, so planning failed with
        // "Recursive CTE is not supported" instead of reading it.
        String sql = "with recursive q(n) as ("
                + " select 1"
                + " union all"
                + " select n from (with q(n) as (select 2) (select n from q)) x"
                + ") select * from q";
        connectContext.getSessionVariable().setEnableRecursiveCTE(false);
        String plan = getFragmentPlan(sql);
        assertContains(plan, "UNION");
    }

    @Test
    public void testScalarSubqueryCteShadowingRecursiveCteNameIsNotRecursive() throws Exception {
        // The scalar subquery is analyzed by a fresh QueryAnalyzer, which sees the recursive CTE only through
        // the session-shared path. Its q is an ordinary CTE that shadows the recursive one, so it must not be
        // rejected as a multi-level recursive reference; the FROM q of the member is still recursive.
        String sql = "with recursive q(n) as ("
                + " select 1"
                + " union all"
                + " select n + 1 from q where n < (select max(n) from (with q(n) as (select 3) (select n from q)) x)"
                + ") select * from q";
        String plan = explainRecursiveCte(sql);
        assertContains(plan, "Recursive Statement:");
    }

    // The UNION that defines a recursive CTE may carry its own WITH clause. The CTE it defines (t0 below, over
    // the base table t1) is visible to every member and shadows the base table t0.
    private static final String UNION_OWN_WITH_RECURSIVE_SQL = "with recursive q as ("
            + " with t0 as (select v4 as v1 from t1)"
            + " select v1 from t0"
            + " union all"
            + " select v1 + 1 from q where v1 < 3"
            + ") select * from q";

    @Test
    public void testUnionOwnWithInRecursiveCteAnalyzed() throws Exception {
        // The analyzer used to analyze only the UNION members, never the UNION's own WITH clause: the anchor
        // read the base table t0, and the body of the inner t0 stayed unanalyzed, so the privilege check
        // failed on its null table with a NullPointerException instead of reporting the recursive CTE as
        // unsupported.
        connectContext.getSessionVariable().setEnableRecursiveCTE(false);
        StatementBase statement = UtFrameUtils.parseStmtWithNewParser(UNION_OWN_WITH_RECURSIVE_SQL, connectContext);
        Authorizer.check(statement, connectContext);
        SemanticException e = Assertions.assertThrows(SemanticException.class,
                () -> getFragmentPlan(UNION_OWN_WITH_RECURSIVE_SQL));
        Assertions.assertTrue(e.getMessage().contains("Recursive CTE is not supported"), e.getMessage());

        // A name that matches no table used to fail with "Unknown table" instead.
        String sql = "with recursive q as ("
                + " with w as (select v4 as v1 from t1)"
                + " select v1 from w"
                + " union all"
                + " select v1 + 1 from q where v1 < 3"
                + ") select * from q";
        e = Assertions.assertThrows(SemanticException.class, () -> getFragmentPlan(sql));
        Assertions.assertTrue(e.getMessage().contains("Recursive CTE is not supported"), e.getMessage());
    }

    @Test
    public void testUnionOwnWithInNonRecursiveCteUnderWithRecursive() throws Exception {
        // No self reference, so the definition is analyzed again as an ordinary CTE. The first pass used to bind
        // t0 to the base table and qualify its name, so the second pass kept reading the base table t0.
        connectContext.getSessionVariable().setEnableRecursiveCTE(false);
        String sql = "with recursive q as ("
                + " with t0 as (select v4 as v1 from t1)"
                + " select v1 from t0"
                + " union all"
                + " select v1 from t0"
                + ") select * from q";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "TABLE: t1");
        assertNotContains(plan, "TABLE: t0");
    }

    @Test
    public void testUnionOwnWithKeptBySplitRecursiveCte() throws Exception {
        // RecursiveCTEExecutor runs the anchor and the recursive member as statements of their own; both must
        // keep the UNION's own WITH clause, or the anchor reads the base table t0.
        String plan = explainRecursiveCte(UNION_OWN_WITH_RECURSIVE_SQL);
        assertContains(plan, "Start Statement: ");
        String start = plan.substring(plan.indexOf("Start Statement: "), plan.indexOf("Recursive Statement: "));
        assertContains(start, "WITH `t0`");
        assertContains(start, "FROM `test`.`t1`");
        assertNotContains(start, "FROM `test`.`t0`");
        String recursive = plan.substring(plan.indexOf("Recursive Statement: "));
        assertContains(recursive, "WITH `t0`");
    }

    @Test
    public void testUnionOwnWithShadowsRecursiveCteName() throws Exception {
        // A CTE of the UNION's own WITH clause named like the recursive CTE shadows it, so there is no
        // recursive reference.
        String sql = "with recursive q as ("
                + " with q as (select v4 as v1 from t1)"
                + " select v1 from q"
                + " union all"
                + " select v1 from q"
                + ") select * from q";
        Assertions.assertFalse(hasRecursiveCte(sql));
        connectContext.getSessionVariable().setEnableRecursiveCTE(false);
        String plan = getFragmentPlan(sql);
        assertContains(plan, "TABLE: t1");
    }
}
