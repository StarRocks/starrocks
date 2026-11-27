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

import com.starrocks.qe.SetExecutor;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class UserVariableAssignTest extends PlanTestBase {

    // Nothing here runs a query, so what a SELECT without FROM has assigned is never taken over or dropped.
    @AfterEach
    public void dropAssignments() {
        connectContext.resetUserVariableCopyInWrite();
    }

    private void assertFails(String sql, String message) {
        SemanticException e = Assertions.assertThrows(SemanticException.class, () -> getFragmentPlan(sql));
        assertContains(e.getMessage(), message);
    }

    @Test
    public void testAssignWithoutFrom() throws Exception {
        String plan = getFragmentPlan("select @a := 1, @b := @a + 1, @a, @b");
        assertContains(plan, "<slot 2> : 1\n" +
                "  |  <slot 3> : 2\n" +
                "  |  <slot 4> : 1\n" +
                "  |  <slot 5> : 2");

        // Making a plan assigns nothing. StmtExecutor takes the values over once the query has run.
        Assertions.assertNull(connectContext.getUserVariable("a"));
        Assertions.assertNotNull(connectContext.getUserVariableCopyInWrite("a"));

        assertFails("select if(1 = 2, @a := 5, 0)", "must be the outermost operator of a select item");
        assertFails("select @a := (select max(v1) from t0)", "can be folded into a constant");
    }

    @Test
    public void testSetStatement() throws Exception {
        SetStmt stmt = (SetStmt) UtFrameUtils.parseStmtWithNewParser("set @c := 5, @d = @c + 1", connectContext);
        new SetExecutor(connectContext, stmt).execute();
        assertContains(getFragmentPlan("select @c, @d"), "<slot 2> : 5\n" +
                "  |  <slot 3> : 6");
    }

    @Test
    public void testRowNumber() throws Exception {
        String sql = "select @rn := @rn + 1 as rn, v1 from t0, (select @rn := 0) r order by v1 desc";
        String plan = getFragmentPlan(sql);
        // The rows are numbered in the order of the ORDER BY clause, the counter is the running sum of its increments.
        assertContains(plan, "functions: [, row_number(), ]\n" +
                "  |  order by: 1: v1 DESC");
        assertContains(plan, "functions: [, sum(1), ]\n" +
                "  |  order by: 6: row_number() ASC\n" +
                "  |  window: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");
        assertContains(plan, "<slot 9> : 8: lag(add(0, 7: sum(1)), 1, 0) + 1");

        // The initialization and the query can also be separate statements. A variable that was never set is NULL.
        plan = getFragmentPlan("select @never_set := @never_set + 1 from t0");
        assertContains(plan, "lag(NULL + 5: sum(1), 1, NULL)");
    }

    @Test
    public void testOrderOfSubquery() throws Exception {
        // The ORDER BY of a subquery is normally dropped. It is kept if it decides how the variables evolve.
        String sql = "select * from (select @rn := @rn + 1 as rn, v1 from t0, (select @rn := 0) r order by v2) t " +
                "where rn <= 3";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "functions: [, row_number(), ]\n" +
                "  |  order by: 2: v2 ASC");
        assertContains(plan, "predicates: 9: expr <= 3");

        sql = "select @prev as prev, @prev := v1 as cur from (select * from t0 order by v3, v1) t";
        plan = getFragmentPlan(sql);
        assertContains(plan, "functions: [, row_number(), ]\n" +
                "  |  order by: 3: v3 ASC, 1: v1 ASC");
        assertContains(plan, "functions: [, lag(1: v1, 1, NULL), ]");

        // An ORDER BY that sorts by the variables can only be applied afterwards. Until then the rows keep the order
        // of the subquery they come from.
        sql = "select @rn := @rn + 1 as rn, t.* from (select * from t0 order by v2) t, (select @rn := 0) r " +
                "order by rn desc";
        plan = getFragmentPlan(sql);
        assertContains(plan, "functions: [, row_number(), ]\n" +
                "  |  order by: 2: v2 ASC");
        assertContains(plan, "order by: <slot 9> 9: expr DESC");
    }

    @Test
    public void testRankWithinGroup() throws Exception {
        String sql = "select if(@g = v2, @n := @n + 1, @n := 1) as n, @g := v2 as g, v1 " +
                "from t0, (select @n := 0, @g := null) i order by v2, v1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "functions: [, lag(2: v2, 1, NULL), ]");
        // Every assignment of 1 starts a new stretch of rows, within which the increments are summed up.
        assertContains(plan, "functions: [, sum(if(8: lag(2: v2, 1, null) = 2: v2, 1, 0)), ]\n" +
                "  |  partition by: 9: sum(if(8: lag(2: v2, 1, null) = 2: v2, 0, 1))\n" +
                "  |  order by: 7: row_number() ASC");
        assertContains(plan, "functions: [, first_value(if(8: lag(2: v2, 1, null) = 2: v2, NULL, 1)), ]");
    }

    @Test
    public void testOrderByAssignedColumn() throws Exception {
        // The alias hides the column of the same name, so this sorts by the select item "@g := v2".
        String sql = "select if(@g = v2, @n := @n + 1, @n := 1) as n, @g := v2 as v2, v1 " +
                "from t0, (select @n := 0, @g := null) i order by v2, v1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "functions: [, row_number(), ]\n" +
                "  |  order by: 2: v2 ASC, 1: v1 ASC");
    }

    @Test
    public void testOrderByExpression() throws Exception {
        String sql = "select @rn := @rn + 1 as rn, v1 from t0, (select @rn := 0) r order by v1 % 5, abs(v2) desc";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "functions: [, row_number(), ]\n" +
                "  |  order by: ");
        assertContains(plan, "1: v1 % 5", "abs(2: v2)");

        sql = "select @rn := @rn + 1 as rn, t.v1 from (select * from t0 order by v2) t join (select @rn := 0) r on true";
        plan = getFragmentPlan(sql);
        assertContains(plan, "functions: [, row_number(), ]\n" +
                "  |  order by: 2: v2 ASC");

        assertFails("select @rn := @rn + 1 as rn, v1 from t0, (select @rn := 0) r order by v1 limit 5 offset 3",
                "not supported together with OFFSET");
    }

    @Test
    public void testVariablesWithEqualUpdates() throws Exception {
        String sql = "select @a := @a + 1 as a, @b := @b + 1 as b, v1 from t0, (select @a := 0, @b := 10) i order by v1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "sum(1), 1, 0)", "sum(1), 1, 10)");
    }

    @Test
    public void testRunningTotal() throws Exception {
        String sql = "select @s := @s - v2 as total, v1 from t0, (select @s := 100) i order by v1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "functions: [, sum(0 - 2: v2), ]");
        // Unlike SUM(), the variable becomes NULL once a NULL has been added to it.
        assertContains(plan, "functions: [, sum(if(0 - 2: v2 IS NULL, 1, 0)), ]");

        sql = "select @s := @s + v2 - v3 + 1 as total from t0, (select @s := 0) i order by v1";
        assertContains(getFragmentPlan(sql), "functions: [, sum(2: v2 + 0 - 3: v3 + 1), ]");

        // A total for each group: IF restarts it, and v3 is added either way.
        sql = "select @s := if(@g = v2, @s, 0) + v3 as total, @g := v2 from t0, (select @s := 0, @g := -1) i " +
                "order by v2, v1";
        plan = getFragmentPlan(sql);
        assertContains(plan, "functions: [, sum(if(8: lag(2: v2, 1, -1) = 2: v2, 3: v3, 0)), ]");
        assertContains(plan, "functions: [, first_value(if(8: lag(2: v2, 1, -1) = 2: v2, NULL, 0 + 3: v3)), ]");

        sql = "select @m := if(v2 > 5, v1, @m) as m from t0, (select @m := -1) i order by v1";
        plan = getFragmentPlan(sql);
        assertContains(plan, "if(2: v2 > 5, 1: v1, 9: lag(if(7: sum(if(2: v2 > 5, 1, 0)) = 0, -1, " +
                "8: first_value(if(2: v2 > 5, 1: v1, null))), 1, -1))");
    }

    @Test
    public void testDates() throws Exception {
        // A date is a string once it is in a variable, as in MySQL.
        String sql = "select timestampdiff(day, @d, id_datetime), @d := id_datetime from test_all_type, " +
                "(select @d := '') i order by t1d";
        assertContains(getFragmentPlan(sql), "id_datetime AS VARCHAR");

        assertFails("select @d := if(t1d > 1, id_datetime, @d) from test_all_type, (select @d := 0) i order by t1d",
                "holds a number and cannot be assigned a date");
    }

    // The query Looker generates for a pivot table, see https://github.com/StarRocks/starrocks/issues/49563
    @Test
    public void testLookerPivot() throws Exception {
        String sql = "SELECT zz1.* FROM (\n" +
                "  SELECT @min_rank := IF(!_ct2, _rank, @min_rank) as m_rank, zz.* FROM (\n" +
                "    SELECT (@curType1 IS NOT NULL AND @curType1 = CAST(COALESCE(`d`,'') AS CHAR)) as _ct2,\n" +
                "           @curType1 := CAST(COALESCE(`d`,'') AS CHAR) as _ct1, xx.* FROM (\n" +
                "      SELECT * FROM (\n" +
                "        SELECT (CASE CAST(COALESCE(`p`,'') AS CHAR) WHEN @curType THEN @curRow := @curRow + 1\n" +
                "                ELSE @curRow := 1 AND @curType := CAST(COALESCE(`p`,'') AS CHAR) END) AS _rank, ww.*\n" +
                "        FROM (SELECT v2 AS `p`, v1 AS `d`, COALESCE(SUM(v3), 0) AS `m` FROM t0 GROUP BY 1, 2) as ww,\n" +
                "             (SELECT @min_rank := 1000000, @curType1 := NULL, @curRow := 0, @curType := '') r\n" +
                "        ORDER BY `p`, `d` DESC) xx1\n" +
                "      ORDER BY `d`, _rank) as xx\n" +
                "    ORDER BY `d`, _rank, _ct2) as zz\n" +
                "  ORDER BY `d`, _rank, _ct2) as zz1\n" +
                "WHERE m_rank <= 500 LIMIT 30000";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "functions: [, row_number(), ]\n" +
                "  |  order by: 2: v2 ASC, 1: v1 DESC");
        assertContains(plan, "functions: [, row_number(), ]\n" +
                "  |  order by: 1: v1 ASC, 17: case ASC\n");
        assertContains(plan, "functions: [, row_number(), ]\n" +
                "  |  order by: 1: v1 ASC, 17: case ASC, 20: expr ASC");
        assertContains(plan, "functions: [, lag(coalesce(CAST(2: v2 AS VARCHAR), ''), 1, ''), ]");
        assertContains(plan, "functions: [, lag(coalesce(CAST(1: v1 AS VARCHAR), ''), 1, NULL), ]");
        assertContains(plan, "functions: [, first_value(if(NOT (20: expr), 17: case, NULL)), ]");
    }

    @Test
    public void testUnsupported() {
        assertFails("select v1 from t0 where (@x := 1) = 1", "only supported in the select list");
        assertFails("select @x := @x + 1 from t0 where @x < 3", "cannot be used in WHERE");
        assertFails("select @x := @x + 1 as c from t0 order by @x", "cannot be used in ORDER BY");
        assertFails("select @x := @x + 1, count(*) from t0 group by v1", "@x := @x + 1");
        assertFails("select distinct @x := @x + 1 from t0", "not supported together with DISTINCT");
        assertFails("select @x := v1 from t0 having (@y := 1) = 1", "not supported together with HAVING");
        assertFails("select @x := @x + 1 as x from t0 having x > 1", "not supported together with HAVING");
        assertFails("select @x := t0.v1 from t0 join t1 on (@y := 1) = t0.v1", "only supported in the select list");
        assertFails("select @x := @x + 1, row_number() over (order by v1) from t0",
                "not supported together with window functions");
        assertFails("select @x := @x * 2 from t0", "Unsupported assignment to user variable @x");
        assertFails("select @x as prev, @x := v1 from (select v1 from t0 order by v2) t",
                "must return the expressions it is sorted by");
        assertFails("select @x := @y * 2, @y := @x + 1 from t0", "Unsupported assignment to user variable @y");
        assertFails("select @x := @x + 1, (select @x) from t0", "cannot contain a subquery");
        assertFails("select @x := @x + 1 from t0 where v1 in (select @x)", "cannot be used in WHERE");
        assertFails("select @x := 1, @x := [1, 2] from t0", "incompatible types");
    }

    @Test
    public void testToSql() throws Exception {
        String sql = "select @rn := @rn + 1 as rn, v1 from t0, (select @rn := 0) r order by v1";
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        assertContains(AstToSQLBuilder.toSQL(stmt), "@rn := (@rn) + 1", "@rn := 0");
    }
}
