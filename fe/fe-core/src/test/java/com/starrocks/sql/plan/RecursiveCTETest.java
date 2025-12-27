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
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
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
    public void testNestedRecursiveCTE() throws Exception {
        String sql = "with recursive cte1 as " +
                "(select v1 from t0 union all select v1 + 1 from cte1 where v1 < 10) " +
                "select * from (select * from cte1 where v1 > 5) t";
        String plan = explainRecursiveCte(sql);
        assertContains(plan, ".*, 0 AS `_cte_level`");
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
    public void testRecursiveCTEInNormalProcess() {
        String sql = "with recursive cte as " +
                "(select v1 from t0 union select v1 + 1 from cte where v1 < 10) " +
                "select * from cte";
        Assertions.assertThrows(SemanticException.class, () -> getFragmentPlan(sql), "Recursive CTE is not supported.");
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
}
