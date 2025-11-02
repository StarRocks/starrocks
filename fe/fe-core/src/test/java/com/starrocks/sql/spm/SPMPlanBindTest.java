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

package com.starrocks.sql.spm;

import com.google.common.base.Preconditions;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.analyzer.ShowStmtAnalyzer;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.spm.CreateBaselinePlanStmt;
import com.starrocks.sql.ast.spm.ShowBaselinePlanStmt;
import com.starrocks.sql.common.UnsupportedException;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

public class SPMPlanBindTest extends PlanTestBase {
    @BeforeAll
    public static void beforeAll() throws Exception {
        PlanTestBase.beforeAll();
        connectContext.getSessionVariable().setEnableSPMRewrite(true);
    }

    @BeforeEach
    public void before() {
        SPMFunctions.enableSPMParamsPrint = true;
    }

    public CreateBaselinePlanStmt createBaselinePlanStmt(String sql) {
        String createSql = "create baseline using " + sql;
        List<StatementBase> statements = SqlParser.parse(createSql, connectContext.getSessionVariable());
        Preconditions.checkState(statements.size() == 1);
        Preconditions.checkState(statements.get(0) instanceof CreateBaselinePlanStmt);
        return (CreateBaselinePlanStmt) statements.get(0);
    }

    public CreateBaselinePlanStmt createBaselinePlanStmt(String bind, String sql) {
        String createSql = "create baseline on " + bind + " using " + sql;
        List<StatementBase> statements = SqlParser.parse(createSql, connectContext.getSessionVariable());
        Preconditions.checkState(statements.size() == 1);
        Preconditions.checkState(statements.get(0) instanceof CreateBaselinePlanStmt);
        return (CreateBaselinePlanStmt) statements.get(0);
    }

    @Test
    public void testBindScan() {
        SPMFunctions.enableSPMParamsPrint = false;
        CreateBaselinePlanStmt stmt = createBaselinePlanStmt("select * from t0 where v2 = 1");
        SPMPlanBuilder generator = new SPMPlanBuilder(connectContext, stmt);

        generator.execute();
        assertContains(generator.getBindSqlDigest(), "SELECT * FROM `test`.`t0` WHERE `test`.`t0`.`v2` = ?");
        assertContains(generator.getPlanStmtSQL(), "SELECT v1, v2, v3 FROM t0 WHERE v2 = _spm_const_var(1)");
    }

    @Test
    public void testBindJoin() {
        SPMFunctions.enableSPMParamsPrint = false;
        CreateBaselinePlanStmt stmt = createBaselinePlanStmt(
                "select * from t0 join t1 on t0.v3 = t1.v6 where t0.v2 = 1");
        SPMPlanBuilder generator = new SPMPlanBuilder(connectContext, stmt);

        generator.execute();
        assertContains(generator.getBindSqlDigest(), "SELECT * "
                + "FROM `test`.`t0` INNER JOIN `test`.`t1` ON `test`.`t0`.`v3` = `test`.`t1`.`v6` "
                + "WHERE `test`.`t0`.`v2` = ?");

        assertContains(generator.getPlanStmtSQL(), "SELECT v1, v2, v3, v4, v5, v6 FROM "
                + "(SELECT * FROM t0 WHERE v2 = _spm_const_var(1)) t_0 INNER JOIN[BROADCAST] t1 ON v3 = v6");
    }

    @Test
    public void testBindJoin2() {
        SPMFunctions.enableSPMParamsPrint = false;
        CreateBaselinePlanStmt stmt = createBaselinePlanStmt(
                "select t1.v4, t0.v2 from t0 join t1 on t0.v3 = t1.v6 where t0.v2 = 1");
        SPMPlanBuilder generator = new SPMPlanBuilder(connectContext, stmt);

        generator.execute();
        assertContains(generator.getBindSqlDigest(), "SELECT `test`.`t1`.`v4`, `test`.`t0`.`v2` " +
                "FROM `test`.`t0` INNER JOIN `test`.`t1` ON `test`.`t0`.`v3` = `test`.`t1`.`v6` " +
                "WHERE `test`.`t0`.`v2` = ?");

        assertContains(generator.getPlanStmtSQL(), "SELECT v4, v2 FROM (SELECT v2, v4 FROM"
                + " (SELECT * FROM t0 WHERE v2 = _spm_const_var(1)) t_0 INNER JOIN[BROADCAST] t1 ON v3 = v6) t2");
    }

    @Test
    public void testBindJoin3() {
        SPMFunctions.enableSPMParamsPrint = false;
        CreateBaselinePlanStmt stmt = createBaselinePlanStmt(
                "select t1.v4, t0.v2 from t0 join[SHUFFLE] t1 on t0.v3 = t1.v6 where t0.v2 = 1");
        SPMPlanBuilder generator = new SPMPlanBuilder(connectContext, stmt);

        generator.execute();
        assertContains(generator.getBindSqlDigest(), "SELECT `test`.`t1`.`v4`, `test`.`t0`.`v2` " +
                "FROM `test`.`t0` INNER JOIN `test`.`t1` ON `test`.`t0`.`v3` = `test`.`t1`.`v6` " +
                "WHERE `test`.`t0`.`v2` = ?");

        assertContains(generator.getBindSql(), "SELECT `test`.`t1`.`v4`, `test`.`t0`.`v2` "
                + "FROM `test`.`t0` INNER JOIN `test`.`t1` ON `test`.`t0`.`v3` = `test`.`t1`.`v6` "
                + "WHERE `test`.`t0`.`v2` = _spm_const_var(1)");

        assertContains(generator.getPlanStmtSQL(), "SELECT v4, v2 FROM (SELECT v2, v4 FROM "
                + "(SELECT * FROM t0 WHERE v2 = _spm_const_var(1)) t_0 INNER JOIN[SHUFFLE] t1 ON v3 = v6) t2");
    }

    @Test
    public void testBindInPredicate() {
        CreateBaselinePlanStmt stmt = createBaselinePlanStmt(
                "select t1.v4, t0.v2 from t0 join[SHUFFLE] t1 on t0.v3 = t1.v6 where t0.v2 in (1,2,3,4)");
        SPMPlanBuilder generator = new SPMPlanBuilder(connectContext, stmt);

        generator.execute();
        assertContains(generator.getBindSqlDigest(), "SELECT `test`.`t1`.`v4`, `test`.`t0`.`v2` " +
                "FROM `test`.`t0` INNER JOIN `test`.`t1` ON `test`.`t0`.`v3` = `test`.`t1`.`v6` " +
                "WHERE `test`.`t0`.`v2` IN (?)");

        assertContains(generator.getBindSql(), "SELECT `test`.`t1`.`v4`, `test`.`t0`.`v2` "
                + "FROM `test`.`t0` INNER JOIN `test`.`t1` ON `test`.`t0`.`v3` = `test`.`t1`.`v6` "
                + "WHERE `test`.`t0`.`v2` IN (_spm_const_list(1, 1, 2, 3, 4))");

        assertContains(generator.getPlanStmtSQL(), "SELECT v4, v2 FROM (SELECT v2, v4 FROM "
                + "(SELECT * FROM t0 WHERE v2 IN (_spm_const_list(1, 1, 2, 3, 4))) t_0 "
                + "INNER JOIN[SHUFFLE] t1 ON v3 = v6) t2");
    }

    @Test
    public void testPlanHints() {
        CreateBaselinePlanStmt stmt = createBaselinePlanStmt(
                "select /*+SET_VAR(cbo_cte_reuse=false,cbo_push_down_aggregate=false)*/ * from t0");
        SPMPlanBuilder generator = new SPMPlanBuilder(connectContext, stmt);
        generator.execute();
        assertContains(generator.getBindSqlDigest(), "SELECT * "
                + "FROM `test`.`t0`");

        assertContains(generator.getBindSql(), "SELECT * "
                + "FROM `test`.`t0`");

        assertContains(generator.getPlanStmtSQL(),
                "SELECT /*+SET_VAR(cbo_cte_reuse=false,cbo_push_down_aggregate=false)*/v1, v2, v3 FROM t0");
    }

    @Test
    public void testBindPlan() {
        CreateBaselinePlanStmt stmt = createBaselinePlanStmt(
                "select t1.v4, t0.v2 from t0, t1 where t0.v3 = t1.v6 and t0.v2 in (1,2,3,4)",
                "SELECT v2, v4 FROM " +
                        "(SELECT * FROM t0 WHERE v2 IN (1, 2, 3, 4)) t_0 " +
                        " INNER JOIN[SHUFFLE] " +
                        " t1 ON v3 = v6");
        SPMPlanBuilder generator = new SPMPlanBuilder(connectContext, stmt);
        generator.execute();
        assertContains(generator.getBindSqlDigest(), "SELECT `test`.`t1`.`v4`, `test`.`t0`.`v2` FROM `test`.`t0` , `test`.`t1`  "
                + "WHERE (`test`.`t0`.`v3` = `test`.`t1`.`v6`) AND (`test`.`t0`.`v2` IN (?))");

        assertContains(generator.getBindSql(), "SELECT `test`.`t1`.`v4`, `test`.`t0`.`v2` "
                + "FROM `test`.`t0` , `test`.`t1`  WHERE (`test`.`t0`.`v3` = `test`.`t1`.`v6`) "
                + "AND (`test`.`t0`.`v2` IN (_spm_const_list(1, 1, 2, 3, 4)))");

        assertContains(generator.getPlanStmtSQL(),
                "SELECT v2, v4 FROM (SELECT * FROM t0 WHERE v2 IN (_spm_const_list(1, 1, 2, 3, 4))) t_0 "
                        + "INNER JOIN[SHUFFLE] t1 ON v3 = v6");
    }

    @Test
    public void testLargeInPreicate() {
        connectContext.getSessionVariable().setLargeInPredicateThreshold(2);
        CreateBaselinePlanStmt stmt = createBaselinePlanStmt(
                "select t1.v4, t0.v2 from t0, t1 where t0.v3 = t1.v6 and " +
                        "t0.v2 in (_spm_const_list(1, 10, 11)) and t1.v5 = _spm_const_var(2, 5)",
                "SELECT v2, v4 FROM " +
                        "(SELECT * FROM t0 WHERE t0.v2 in (_spm_const_list(1, 10, 11)) ) t_0 " +
                        " INNER JOIN[SHUFFLE] " +
                        " (SELECT * FROM t1 WHERE v5 = _spm_const_var(2, 5)) t_1 ON v3 = v6");
        SPMPlanBuilder generator = new SPMPlanBuilder(connectContext, stmt);
        generator.execute();
        assertContains(generator.getBindSqlDigest(), "SELECT `test`.`t1`.`v4`, `test`.`t0`.`v2` "
                + "FROM `test`.`t0` , `test`.`t1`  "
                + "WHERE ((`test`.`t0`.`v3` = `test`.`t1`.`v6`) "
                + "AND (`test`.`t0`.`v2` IN (?))) AND (`test`.`t1`.`v5` = ?)");

        assertContains(generator.getBindSql(), "SELECT `test`.`t1`.`v4`, `test`.`t0`.`v2` "
                + "FROM `test`.`t0` , `test`.`t1`  "
                + "WHERE ((`test`.`t0`.`v3` = `test`.`t1`.`v6`) "
                + "AND (`test`.`t0`.`v2` IN (_spm_const_list(1, 10, 11)))) "
                + "AND (`test`.`t1`.`v5` = _spm_const_var(2, 5))");

        assertContains(generator.getPlanStmtSQL(),
                "SELECT v2, v4 FROM (SELECT * FROM t0 WHERE v2 IN (_spm_const_list(1, 10, 11))) t_0 "
                        + "INNER JOIN[SHUFFLE] "
                        + "(SELECT v4, v6 FROM t1 WHERE v5 = _spm_const_var(2, 5)) t_1 ON v3 = v6");
        connectContext.getSessionVariable().setLargeInPredicateThreshold(100000);
    }

    @Test
    public void testUserBindPlan() {
        CreateBaselinePlanStmt stmt = createBaselinePlanStmt(
                "select t1.v4, t0.v2 from t0, t1 where t0.v3 = t1.v6 and " +
                        "t0.v2 in (_spm_const_list(1, 10, 11)) and t1.v5 = _spm_const_var(2, 5)",
                "SELECT v2, v4 FROM " +
                        "(SELECT * FROM t0 WHERE t0.v2 in (_spm_const_list(1, 10, 11)) ) t_0 " +
                        " INNER JOIN[SHUFFLE] " +
                        " (SELECT * FROM t1 WHERE v5 = _spm_const_var(2, 5)) t_1 ON v3 = v6");
        SPMPlanBuilder generator = new SPMPlanBuilder(connectContext, stmt);
        generator.execute();
        assertContains(generator.getBindSqlDigest(), "SELECT `test`.`t1`.`v4`, `test`.`t0`.`v2` "
                + "FROM `test`.`t0` , `test`.`t1`  "
                + "WHERE ((`test`.`t0`.`v3` = `test`.`t1`.`v6`) "
                + "AND (`test`.`t0`.`v2` IN (?))) AND (`test`.`t1`.`v5` = ?)");

        assertContains(generator.getBindSql(), "SELECT `test`.`t1`.`v4`, `test`.`t0`.`v2` "
                + "FROM `test`.`t0` , `test`.`t1`  "
                + "WHERE ((`test`.`t0`.`v3` = `test`.`t1`.`v6`) "
                + "AND (`test`.`t0`.`v2` IN (_spm_const_list(1, 10, 11)))) "
                + "AND (`test`.`t1`.`v5` = _spm_const_var(2, 5))");

        assertContains(generator.getPlanStmtSQL(),
                "SELECT v2, v4 FROM (SELECT * FROM t0 WHERE v2 IN (_spm_const_list(1, 10, 11))) t_0 "
                        + "INNER JOIN[SHUFFLE] "
                        + "(SELECT v4, v6 FROM t1 WHERE v5 = _spm_const_var(2, 5)) t_1 ON v3 = v6");
    }

    @Test
    public void testUserBindPlan2() {
        CreateBaselinePlanStmt stmt = createBaselinePlanStmt(
                "select t1.v4, t0.v2 from t0, t1 where t0.v3 = t1.v6 and " +
                        "t0.v2 in (_spm_const_list(1, 10, 11)) and t1.v5 = 1",
                "SELECT v2, v4 FROM " +
                        "(SELECT * FROM t0 WHERE t0.v2 in (_spm_const_list(1, 10, 11)) ) t_0 " +
                        " INNER JOIN[SHUFFLE] " +
                        " (SELECT * FROM t1 WHERE v5 = 1) t_1 ON v3 = v6");
        SPMPlanBuilder generator = new SPMPlanBuilder(connectContext, stmt);
        generator.execute();
        assertContains(generator.getBindSqlDigest(), "SELECT `test`.`t1`.`v4`, `test`.`t0`.`v2` "
                + "FROM `test`.`t0` , `test`.`t1`  "
                + "WHERE ((`test`.`t0`.`v3` = `test`.`t1`.`v6`) "
                + "AND (`test`.`t0`.`v2` IN (?))) AND (`test`.`t1`.`v5` = ?)");

        assertContains(generator.getBindSql(), "SELECT `test`.`t1`.`v4`, `test`.`t0`.`v2` "
                + "FROM `test`.`t0` , `test`.`t1`  "
                + "WHERE ((`test`.`t0`.`v3` = `test`.`t1`.`v6`) "
                + "AND (`test`.`t0`.`v2` IN (_spm_const_list(1, 10, 11)))) "
                + "AND (`test`.`t1`.`v5` = _spm_const_var(2, 1))");

        assertContains(generator.getPlanStmtSQL(), "SELECT v2, v4 FROM "
                + "(SELECT * FROM t0 WHERE v2 IN (_spm_const_list(1, 10, 11))) t_0 "
                + "INNER JOIN[SHUFFLE] (SELECT v4, v6 FROM t1 WHERE v5 = _spm_const_var(2, 1)) t_1 ON v3 = v6");
    }

    @Test
    public void testUserBindPlanInvalid1() {
        CreateBaselinePlanStmt stmt = createBaselinePlanStmt(
                "select t1.v4, t0.v2 from t0, t1 where t0.v3 = t1.v6 and " +
                        "t0.v2 in (_spm_const_list(1, 10, 11)) and t1.v5 = 1",
                "SELECT v2, v4 FROM " +
                        "(SELECT * FROM t0 WHERE t0.v2 in (_spm_const_list(1, 10, 11)) ) t_0 " +
                        " INNER JOIN[SHUFFLE] " +
                        " (SELECT * FROM t1 WHERE v5 = 234) t_1 ON v3 = v6");
        SPMPlanBuilder generator = new SPMPlanBuilder(connectContext, stmt);
        generator.analyze();
        Assertions.assertThrows(SemanticException.class, generator::parameterizedStmt);
    }

    @Test
    public void testUserBindPlanInvalid2() {
        CreateBaselinePlanStmt stmt = createBaselinePlanStmt(
                "select t1.v4, t0.v2 from t0, t1 where t0.v3 = t1.v6 and " +
                        "t0.v2 in (_spm_const_list(1, 10, 11)) and t1.v5 = 1",
                "SELECT v2, v4 FROM " +
                        "(SELECT * FROM t0 WHERE t0.v2 in (1,2,3) ) t_0 " +
                        " INNER JOIN[SHUFFLE] " +
                        " (SELECT * FROM t1 WHERE v5 = 234) t_1 ON v3 = v6");
        SPMPlanBuilder generator = new SPMPlanBuilder(connectContext, stmt);
        generator.analyze();
        Assertions.assertThrows(SemanticException.class, generator::parameterizedStmt);
    }

    @Test
    public void testBindAggregate() {
        CreateBaselinePlanStmt stmt = createBaselinePlanStmt("SELECT v2, sum(v3) FROM t0 WHERE v1 = 1 GROUP BY v2");
        SPMPlanBuilder generator = new SPMPlanBuilder(connectContext, stmt);
        generator.execute();

        assertContains(generator.getBindSqlDigest(), "SELECT `test`.`t0`.`v2`, sum(`test`.`t0`.`v3`) AS `sum(v3)` "
                + "FROM `test`.`t0` WHERE `test`.`t0`.`v1` = ? GROUP BY `test`.`t0`.`v2`");

        assertContains(generator.getBindSql(), "SELECT `test`.`t0`.`v2`, sum(`test`.`t0`.`v3`) AS `sum(v3)` "
                + "FROM `test`.`t0` WHERE `test`.`t0`.`v1` = _spm_const_var(1, 1) GROUP BY `test`.`t0`.`v2`");

        assertContains(generator.getPlanStmtSQL(), "SELECT v2, sum(v3) AS c_4 "
                + "FROM (SELECT v2, v3 FROM t0 WHERE v1 = _spm_const_var(1, 1)) t_0 "
                + "GROUP BY v2");
    }

    @Test
    public void testBindAggregateHaving() {
        CreateBaselinePlanStmt stmt =
                createBaselinePlanStmt("SELECT v2, sum(v3) FROM t0 WHERE v1 = 1 GROUP BY v2 HAVING sum(v3) > 100");
        SPMPlanBuilder generator = new SPMPlanBuilder(connectContext, stmt);
        generator.execute();

        assertContains(generator.getBindSqlDigest(), "SELECT `test`.`t0`.`v2`, sum(`test`.`t0`.`v3`) AS `sum(v3)` "
                + "FROM `test`.`t0` "
                + "WHERE `test`.`t0`.`v1` = ? "
                + "GROUP BY `test`.`t0`.`v2` "
                + "HAVING (sum(`test`.`t0`.`v3`)) > ?");

        assertContains(generator.getBindSql(), "SELECT `test`.`t0`.`v2`, sum(`test`.`t0`.`v3`) AS `sum(v3)` "
                + "FROM `test`.`t0` "
                + "WHERE `test`.`t0`.`v1` = _spm_const_var(1, 1) "
                + "GROUP BY `test`.`t0`.`v2` "
                + "HAVING (sum(`test`.`t0`.`v3`)) > _spm_const_var(2, 100)");

        assertContains(generator.getPlanStmtSQL(), "SELECT v2, sum(v3) AS c_4 "
                + "FROM (SELECT v2, v3 FROM t0 WHERE v1 = _spm_const_var(1, 1)) t_0 "
                + "GROUP BY v2 HAVING sum(v3) > _spm_const_var(2, 100)");
    }

    @Test
    public void testBindAggregateHavingProjection() {
        CreateBaselinePlanStmt stmt =
                createBaselinePlanStmt("SELECT v2 + 2, sum(v3) FROM t0 WHERE v1 = 1 GROUP BY v2 HAVING sum(v3) > 100");
        SPMPlanBuilder generator = new SPMPlanBuilder(connectContext, stmt);
        generator.execute();
        assertContains(generator.getPlanStmtSQL(), "SELECT sum(v3) AS c_4, v2 + _spm_const_var(1, 2) AS c_5 "
                + "FROM (SELECT v2, v3 FROM t0 WHERE v1 = _spm_const_var(2, 1)) t_0 "
                + "GROUP BY v2 HAVING sum(v3) > _spm_const_var(3, 100)");
    }

    @Test
    public void testValuesSQL() {
        CreateBaselinePlanStmt stmt = createBaselinePlanStmt("SELECT * from (values(1,2,3), (4,5,6))");
        SPMPlanBuilder generator = new SPMPlanBuilder(connectContext, stmt);
        generator.execute();

        assertContains(generator.getBindSqlDigest(), "SELECT * "
                + "FROM (VALUES(?, ?, ?), (?, ?, ?))");

        assertContains(generator.getBindSql(), "SELECT * "
                + "FROM (VALUES(_spm_const_var(1, 1), _spm_const_var(2, 2), _spm_const_var(3, 3)), "
                + "(_spm_const_var(4, 4), _spm_const_var(5, 5), _spm_const_var(6, 6)))");

        assertContains(generator.getPlanStmtSQL(),
                "SELECT c_1, c_2, c_3 FROM (VALUES (_spm_const_var(1, 1), _spm_const_var(2, 2), _spm_const_var(3, 3)), "
                        + "(_spm_const_var(4, 4), _spm_const_var(5, 5), _spm_const_var(6, 6))) AS t(c_1, c_2, c_3)");
    }

    @Test
    public void testJoinValuesSQL() {
        CreateBaselinePlanStmt stmt = createBaselinePlanStmt("SELECT * from (values(1,2,3), (4,5,6)) x(v1, v2, v3) "
                + "join (values(9,7,8), (11,22,33)) y(v4, v5, v6) on x.v1 = y.v4");
        SPMPlanBuilder generator = new SPMPlanBuilder(connectContext, stmt);
        generator.execute();

        assertContains(generator.getBindSqlDigest(), "SELECT * "
                + "FROM (VALUES(?, ?, ?), (?, ?, ?)) x(v1,v2,v3) INNER JOIN"
                + " (VALUES(?, ?, ?), (?, ?, ?)) y(v4,v5,v6) ON `x`.`v1` = `y`.`v4`");

        assertContains(generator.getBindSql(), "SELECT * "
                + "FROM (VALUES(_spm_const_var(1, 1), _spm_const_var(2, 2), _spm_const_var(3, 3)), "
                + "(_spm_const_var(4, 4), _spm_const_var(5, 5), _spm_const_var(6, 6))) "
                + "x(v1,v2,v3) "
                + "INNER JOIN "
                + "(VALUES(_spm_const_var(7, 9), _spm_const_var(8, 7), _spm_const_var(9, 8)), "
                + "(_spm_const_var(10, 11), _spm_const_var(11, 22), _spm_const_var(12, 33))) "
                + "y(v4,v5,v6) ON `x`.`v1` = `y`.`v4`");

        assertContains(generator.getPlanStmtSQL(), "SELECT c_1, c_2, c_3, c_4, c_5, c_6 FROM "
                + "(SELECT * FROM (VALUES (_spm_const_var(1, 1), _spm_const_var(2, 2), _spm_const_var(3, 3)), "
                + "(_spm_const_var(4, 4), _spm_const_var(5, 5), _spm_const_var(6, 6))) AS t(c_1, c_2, c_3)) t_0"
                + " INNER JOIN[BROADCAST] "
                + "(SELECT * FROM (VALUES (_spm_const_var(7, 9), _spm_const_var(8, 7), _spm_const_var(9, 8)), "
                + "(_spm_const_var(10, 11), _spm_const_var(11, 22), _spm_const_var(12, 33))) AS t(c_4, c_5, c_6)) t_1"
                + " ON c_1 = c_4");
    }

    @Test
    public void testUnion() {
        CreateBaselinePlanStmt stmt =
                createBaselinePlanStmt("SELECT * from t0 where v1 = 1 union all select * from t0 where v1 = 2");
        SPMPlanBuilder generator = new SPMPlanBuilder(connectContext, stmt);
        generator.execute();

        assertContains(generator.getBindSqlDigest(), "SELECT * FROM `test`.`t0` WHERE `test`.`t0`.`v1` = ? "
                + "UNION ALL "
                + "SELECT * FROM `test`.`t0` WHERE `test`.`t0`.`v1` = ?");

        assertContains(generator.getBindSql(),
                "SELECT * FROM `test`.`t0` WHERE `test`.`t0`.`v1` = _spm_const_var(1, 1) "
                        + "UNION ALL "
                        + "SELECT * FROM `test`.`t0` WHERE `test`.`t0`.`v1` = _spm_const_var(2, 2)");

        assertContains(generator.getPlanStmtSQL(), "SELECT c_7, c_8, c_9 FROM ("
                + "SELECT v1 AS c_7, v2 AS c_8, v3 AS c_9 FROM t0 WHERE v1 = _spm_const_var(1, 1) "
                + "UNION ALL "
                + "SELECT v1 AS c_7, v2 AS c_8, v3 AS c_9 FROM t0 WHERE v1 = _spm_const_var(2, 2)"
                + ") t_2");
    }

    @Test
    public void testOrderBy() {
        CreateBaselinePlanStmt stmt = createBaselinePlanStmt("SELECT * from t0 where v1 = 1 order by v2 limit 10, 20");
        SPMPlanBuilder generator = new SPMPlanBuilder(connectContext, stmt);
        generator.execute();

        assertContains(generator.getBindSqlDigest(),
                "SELECT * FROM `test`.`t0` WHERE `test`.`t0`.`v1` = ? ORDER BY `test`.`t0`.`v2` ASC  LIMIT 10, 20");

        assertContains(generator.getBindSql(), "SELECT * FROM `test`.`t0` "
                + "WHERE `test`.`t0`.`v1` = _spm_const_var(1, 1) ORDER BY `test`.`t0`.`v2` ASC  LIMIT 10, 20");

        assertContains(generator.getPlanStmtSQL(),
                "SELECT v1, v2, v3 FROM t0 WHERE v1 = _spm_const_var(1, 1) ORDER BY v2 ASC LIMIT 10, 20");
    }

    @Test
    public void testCte() {
        connectContext.getSessionVariable().setCboCTERuseRatio(0);
        CreateBaselinePlanStmt stmt = createBaselinePlanStmt(
                "with x as (select * from t0), y as (select * from x) SELECT x.* from x, y where x.v1 = 1");
        SPMPlanBuilder generator = new SPMPlanBuilder(connectContext, stmt);
        generator.execute();

        assertContains(generator.getBindSqlDigest(),
                "WITH `x` (`v1`, `v2`, `v3`) AS (SELECT * FROM `test`.`t0`) , "
                        + "`y` (`v1`, `v2`, `v3`) AS (SELECT * FROM `x`) "
                        + "SELECT x.* FROM `x` , `y`  WHERE `x`.`v1` = ?");

        assertContains(generator.getBindSql(), "WITH `x` (`v1`, `v2`, `v3`) AS (SELECT * FROM `test`.`t0`) , "
                + "`y` (`v1`, `v2`, `v3`) AS (SELECT * FROM `x`) "
                + "SELECT x.* FROM `x` , `y`  WHERE `x`.`v1` = _spm_const_var(1, 1)");

        assertContains(generator.getPlanStmtSQL(), "WITH t_0 AS (SELECT * FROM t0) "
                + "SELECT v1, v2, v3 FROM "
                + "(SELECT * FROM t_0 WHERE v1 = _spm_const_var(1, 1)) t_1 "
                + "CROSS JOIN[BROADCAST] (SELECT 1 AS c_12 FROM t_0) t_2");
    }

    @Test
    public void testWindow() {
        connectContext.getSessionVariable().setCboCTERuseRatio(0);
        CreateBaselinePlanStmt stmt = createBaselinePlanStmt(
                "select v3, avg(v2) over (partition by v2, v3 order by v2 desc), "
                        + "sum(v2) over (partition by v2, v3 order by v2 desc) as sum1 from t0");
        SPMPlanBuilder generator = new SPMPlanBuilder(connectContext, stmt);
        generator.execute();

        assertContains(generator.getBindSqlDigest(),
                "SELECT `test`.`t0`.`v3`, avg(`test`.`t0`.`v2`) OVER (PARTITION BY `test`.`t0`.`v2`, `test`.`t0`.`v3`"
                        + " ORDER BY `test`.`t0`.`v2` DESC ) AS `avg(v2) OVER (PARTITION BY v2, v3 ORDER BY v2 DESC )"
                        + "`, sum(`test`.`t0`.`v2`) OVER (PARTITION BY `test`.`t0`.`v2`, `test`.`t0`.`v3` ORDER BY "
                        + "`test`.`t0`.`v2` DESC ) AS `sum1` "
                        + "FROM `test`.`t0`");

        assertContains(generator.getBindSql(),
                "SELECT `test`.`t0`.`v3`, avg(`test`.`t0`.`v2`) OVER (PARTITION BY `test`.`t0`.`v2`, `test`.`t0`.`v3`"
                        + " ORDER BY `test`.`t0`.`v2` DESC ) AS `avg(v2) OVER (PARTITION BY v2, v3 ORDER BY v2 DESC )"
                        + "`, sum(`test`.`t0`.`v2`) OVER (PARTITION BY `test`.`t0`.`v2`, `test`.`t0`.`v3` ORDER BY "
                        + "`test`.`t0`.`v2` DESC ) AS `sum1` "
                        + "FROM `test`.`t0`");

        assertContains(generator.getPlanStmtSQL(),
                "SELECT v3, avg(v2) OVER (PARTITION BY v2, v3 ORDER BY v2 DESC ) AS c_4, "
                        + "sum(v2) OVER (PARTITION BY v2, v3 ORDER BY v2 DESC ) AS c_5 FROM t0");
    }

    @Test
    public void testWindow2() {
        connectContext.getSessionVariable().setCboCTERuseRatio(0);
        CreateBaselinePlanStmt stmt = createBaselinePlanStmt(
                "select v3, sum(v2), avg(v2) over (partition by v2, v3 order by v2 desc), "
                        + "sum(v2) over (partition by v2, v3 order by v2 desc) as sum1 from t0 "
                        + "group by v2,v3 having sum(v2) > 100");
        SPMPlanBuilder generator = new SPMPlanBuilder(connectContext, stmt);
        generator.execute();

        assertContains(generator.getBindSqlDigest(),
                "SELECT `test`.`t0`.`v3`, sum(`test`.`t0`.`v2`) AS `sum(v2)`, avg(`test`.`t0`.`v2`) OVER (PARTITION "
                        + "BY `test`.`t0`.`v2`, `test`.`t0`.`v3` ORDER BY `test`.`t0`.`v2` DESC ) AS `avg(v2) OVER "
                        + "(PARTITION BY v2, v3 ORDER BY v2 DESC )`, sum(`test`.`t0`.`v2`) OVER (PARTITION BY `test`"
                        + ".`t0`.`v2`, `test`.`t0`.`v3` ORDER BY `test`.`t0`.`v2` DESC ) AS `sum1` "
                        + "FROM `test`.`t0` "
                        + "GROUP BY `test`.`t0`.`v2`, `test`.`t0`.`v3` "
                        + "HAVING (sum(`test`.`t0`.`v2`)) > ?");

        assertContains(generator.getBindSql(),
                "SELECT `test`.`t0`.`v3`, sum(`test`.`t0`.`v2`) AS `sum(v2)`, avg(`test`.`t0`.`v2`) OVER (PARTITION "
                        + "BY `test`.`t0`.`v2`, `test`.`t0`.`v3` ORDER BY `test`.`t0`.`v2` DESC ) AS `avg(v2) OVER "
                        + "(PARTITION BY v2, v3 ORDER BY v2 DESC )`, sum(`test`.`t0`.`v2`) OVER (PARTITION BY `test`"
                        + ".`t0`.`v2`, `test`.`t0`.`v3` ORDER BY `test`.`t0`.`v2` DESC ) AS `sum1` "
                        + "FROM `test`.`t0` "
                        + "GROUP BY `test`.`t0`.`v2`, `test`.`t0`.`v3` "
                        + "HAVING (sum(`test`.`t0`.`v2`)) > _spm_const_var(1, 100)");

        assertContains(generator.getPlanStmtSQL(),
                "SELECT v3, c_4, avg(v2) OVER (PARTITION BY v2, v3 ORDER BY v2 DESC ) AS c_5, "
                        + "sum(v2) OVER (PARTITION BY v2, v3 ORDER BY v2 DESC ) AS c_6 "
                        + "FROM (SELECT v2, v3, sum(v2) AS c_4 FROM t0 "
                        + "GROUP BY v2, v3 HAVING sum(v2) > _spm_const_var(1, 100)) t_0");
    }

    @Test
    public void testGroupingSets() {
        connectContext.getSessionVariable().setCboCTERuseRatio(0);
        CreateBaselinePlanStmt stmt = createBaselinePlanStmt(
                "select v1, v2, grouping_id(v1, v2) as b, sum(v3) from t0 group by grouping sets((), (v1, v2))");
        SPMPlanBuilder generator = new SPMPlanBuilder(connectContext, stmt);
        generator.execute();

        assertContains(generator.getBindSqlDigest(), "SELECT `test`.`t0`.`v1`, `test`.`t0`.`v2`, "
                + "grouping(`test`.`t0`.`v1`, `test`.`t0`.`v2`) AS `b`, sum(`test`.`t0`.`v3`) AS `sum(v3)` "
                + "FROM `test`.`t0` GROUP BY GROUPING SETS ((), (`test`.`t0`.`v1`, `test`.`t0`.`v2`))");

        assertContains(generator.getBindSql(),
                "SELECT `test`.`t0`.`v1`, `test`.`t0`.`v2`, grouping(`test`.`t0`.`v1`, `test`.`t0`.`v2`) AS `b`, sum"
                        + "(`test`.`t0`.`v3`) AS `sum(v3)` "
                        + "FROM `test`.`t0` "
                        + "GROUP BY GROUPING SETS ((), (`test`.`t0`.`v1`, `test`.`t0`.`v2`))");

        assertContains(generator.getPlanStmtSQL(), "SELECT c_1, c_2, c_6, c_4 FROM "
                + "(SELECT v1 AS c_1, v2 AS c_2, sum(v3) AS c_4, GROUPING(v1, v2) AS c_6 "
                + "FROM t0 GROUP BY GROUPING SETS((), (v1, v2))) t1");
    }

    public List<BaselinePlan> executeShowBaselinePlan(String sql) {
        List<StatementBase> statements = SqlParser.parse(sql, connectContext.getSessionVariable());
        Preconditions.checkState(statements.size() == 1);
        Preconditions.checkState(statements.get(0) instanceof ShowBaselinePlanStmt);
        ShowBaselinePlanStmt s = (ShowBaselinePlanStmt) statements.get(0);
        ShowStmtAnalyzer.analyze(s, connectContext);
        return connectContext.getSqlPlanStorage().getBaselines(s.getWhere());
    }

    @Test
    public void testShowBaselineStmt() {
        BaselinePlan p1 = SPMStmtExecutor.execute(connectContext,
                createBaselinePlanStmt("select t1.v4, t0.v2 from t0 join t1 on t0.v3 = t1.v6 where t0.v2 = 1"));
        BaselinePlan p2 = SPMStmtExecutor.execute(connectContext,
                createBaselinePlanStmt("select t1.v5, t0.v1 from t0 join t1 on t0.v3 = t1.v6 where t0.v2 = 1"));
        BaselinePlan p3 = SPMStmtExecutor.execute(connectContext,
                createBaselinePlanStmt("select * from t0 join t1 on t0.v3 = t1.v6 where t0.v2 = 1"));
        p3.setEnable(false);

        {
            List<BaselinePlan> k = executeShowBaselinePlan("show baseline");
            Assertions.assertEquals(3, k.size());
        }
        {
            List<BaselinePlan> k = executeShowBaselinePlan("show baseline where enable = true");
            Assertions.assertEquals(2, k.size());
        }
        {
            List<BaselinePlan> k =
                    executeShowBaselinePlan("show baseline where bindSQLDigest like '" + p2.getBindSqlDigest() + "'");
            Assertions.assertEquals(1, k.size());
            Assertions.assertEquals(p2.getId(), k.get(0).getId());
        }
        {
            List<BaselinePlan> k = executeShowBaselinePlan(
                    "show baseline where plansql = '" + p1.getPlanSql() + "' and global = false");
            Assertions.assertEquals(1, k.size());
            Assertions.assertEquals(p1.getId(), k.get(0).getId());
        }
        {
            List<BaselinePlan> k =
                    executeShowBaselinePlan("show baseline where date(updatetime) = date(now())");
            Assertions.assertEquals(3, k.size());
        }
        {
            Assertions.assertThrows(UnsupportedException.class,
                    () -> executeShowBaselinePlan("show baseline where bindSQLDigest = ucase('%t1.v5, t0.v1%')"));
        }
    }

    public ShowBaselinePlanStmt createShowBaselinePlan(String sql) {
        List<StatementBase> statements = SqlParser.parse(sql, connectContext.getSessionVariable());
        Preconditions.checkState(statements.size() == 1);
        Preconditions.checkState(statements.get(0) instanceof ShowBaselinePlanStmt);
        ShowBaselinePlanStmt s = (ShowBaselinePlanStmt) statements.get(0);
        ShowStmtAnalyzer.analyze(s, connectContext);
        return s;
    }

    @Test
    public void testGlobalShowBaselineStmt() {
        SQLPlanGlobalStorage storage = new SQLPlanGlobalStorage();
        {
            ShowBaselinePlanStmt s = createShowBaselinePlan("show baseline");
            String result = storage.generateQuerySql(s.getWhere()).orElse("asdf");
            assertContains(result, "SELECT * FROM spm_baselines");
        }
        {
            ShowBaselinePlanStmt s = createShowBaselinePlan("show baseline where enable = true");
            String result = storage.generateQuerySql(s.getWhere()).orElse("asdf");
            assertContains(result, "WHERE is_enable");
        }
        {
            ShowBaselinePlanStmt s = createShowBaselinePlan("show baseline where bindSQLDigest like '%asdf%'");
            String result = storage.generateQuerySql(s.getWhere()).orElse("asdf");
            assertContains(result, "WHERE bind_sql_digest LIKE '%asdf%'");

        }
        {
            ShowBaselinePlanStmt s = createShowBaselinePlan("show baseline where global = false");
            String result = storage.generateQuerySql(s.getWhere()).orElse("bad");
            assertContains(result, "bad");

        }
        {
            ShowBaselinePlanStmt s = createShowBaselinePlan("show baseline where plansql = 'asdf' and global = false");
            String result = storage.generateQuerySql(s.getWhere()).orElse("bad");
            assertContains(result, "bad");
        }
        {
            ShowBaselinePlanStmt s = createShowBaselinePlan("show baseline where global = true and plansql = 'asdf'");
            String result = storage.generateQuerySql(s.getWhere()).orElse("bad");
            assertContains(result, "WHERE plan_sql = 'asdf'");
        }
    }
}