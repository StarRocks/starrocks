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

import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.spm.CreateBaselinePlanStmt;
import com.starrocks.sql.parser.SqlParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

public class SPMPlanTest extends SPMTestBase {

    @BeforeAll
    public static void beforeAll() throws Exception {
        SPMTestBase.beforeAll();
        connectContext.getSessionVariable().setEnableSPMRewrite(true);
    }

    @Test
    public void testBindScan() {
        CreateBaselinePlanStmt stmt = createBaselinePlanStmt("select * from t0 where v2 = 1");
        SPMPlanBuilder generator = new SPMPlanBuilder(connectContext, stmt);

        generator.analyze();
        generator.parameterizedStmt();
        assertContains(generator.getBindSqlDigest(), "SELECT *\n" +
                "FROM `test`.`t0`\n" +
                "WHERE `test`.`t0`.`v2` = ?");

        generator.generatePlan();
        assertContains(generator.getPlanStmtSQL(), "SELECT * FROM t0 WHERE v2 = _spm_const_var(0)");
    }

    @Test
    public void testSPMReplaceScanPlan() {
        CreateBaselinePlanStmt stmt = createBaselinePlanStmt("select * from t0 where v2 = 1");
        SPMStmtExecutor.execute(connectContext, stmt);
        SPMPlanner planner = new SPMPlanner(connectContext);

        List<StatementBase> statements =
                SqlParser.parse("select * from t0 where v2 = 20", connectContext.getSessionVariable());

        StatementBase query = planner.plan(statements.get(0));
        Assertions.assertTrue(planner.getBaseline().getId() > 1);
        Assertions.assertNotEquals(query, statements.get(0));
    }

    @Test
    public void testBindJoin() {
        CreateBaselinePlanStmt stmt = createBaselinePlanStmt(
                "select * from t0 join t1 on t0.v3 = t1.v6 where t0.v2 = 1");
        SPMPlanBuilder generator = new SPMPlanBuilder(connectContext, stmt);

        generator.analyze();
        generator.parameterizedStmt();
        assertContains(generator.getBindSqlDigest(), "SELECT *\n" +
                "FROM `test`.`t0` INNER JOIN `test`.`t1` ON `test`.`t0`.`v3` = `test`.`t1`.`v6`\n" +
                "WHERE `test`.`t0`.`v2` = ?");

        generator.generatePlan();
        assertContains(generator.getPlanStmtSQL(),
                "SELECT * FROM " +
                        "(SELECT * FROM t0 WHERE v2 = _spm_const_var(0)) t_0 INNER JOIN[BROADCAST] " +
                        "(SELECT * FROM t1 WHERE v6 IS NOT NULL) t_1 ON v3 = v6");
    }

    @Test
    public void testBindJoin2() {
        CreateBaselinePlanStmt stmt = createBaselinePlanStmt(
                "select t1.v4, t0.v2 from t0 join t1 on t0.v3 = t1.v6 where t0.v2 = 1");
        SPMPlanBuilder generator = new SPMPlanBuilder(connectContext, stmt);

        generator.analyze();
        generator.parameterizedStmt();
        assertContains(generator.getBindSqlDigest(), "SELECT `test`.`t1`.`v4`, `test`.`t0`.`v2`\n" +
                "FROM `test`.`t0` INNER JOIN `test`.`t1` ON `test`.`t0`.`v3` = `test`.`t1`.`v6`\n" +
                "WHERE `test`.`t0`.`v2` = ?");

        generator.generatePlan();
        assertContains(generator.getPlanStmtSQL(),
                "SELECT v2, v4 FROM " +
                        "(SELECT * FROM t0 WHERE v2 = _spm_const_var(0)) t_0 INNER JOIN[BROADCAST] " +
                        "(SELECT * FROM t1 WHERE v6 IS NOT NULL) t_1 ON v3 = v6");
    }

    @Test
    public void testBindJoin3() {
        CreateBaselinePlanStmt stmt = createBaselinePlanStmt(
                "select t1.v4, t0.v2 from t0 join[SHUFFLE] t1 on t0.v3 = t1.v6 where t0.v2 = 1");
        SPMPlanBuilder generator = new SPMPlanBuilder(connectContext, stmt);

        generator.analyze();
        generator.parameterizedStmt();
        assertContains(generator.getBindSqlDigest(), "SELECT `test`.`t1`.`v4`, `test`.`t0`.`v2`\n" +
                "FROM `test`.`t0` INNER JOIN `test`.`t1` ON `test`.`t0`.`v3` = `test`.`t1`.`v6`\n" +
                "WHERE `test`.`t0`.`v2` = ?");

        assertContains(generator.getBindSql(), "SELECT `test`.`t1`.`v4`, `test`.`t0`.`v2`\n" +
                "FROM `test`.`t0` INNER JOIN `test`.`t1` ON `test`.`t0`.`v3` = `test`.`t1`.`v6`\n" +
                "WHERE `test`.`t0`.`v2` = _spm_const_var(0)");

        generator.generatePlan();
        assertContains(generator.getPlanStmtSQL(),
                "SELECT v2, v4 FROM " +
                        "(SELECT * FROM t0 WHERE v2 = _spm_const_var(0)) t_0 INNER JOIN[SHUFFLE] " +
                        "(SELECT * FROM t1 WHERE v6 IS NOT NULL) t_1 ON v3 = v6");
    }

    @Test
    public void testSPMReplaceJoinPlan() {
        CreateBaselinePlanStmt stmt = createBaselinePlanStmt(
                "select t1.v4, t0.v2 from t0 join[SHUFFLE] t1 on t0.v3 = t1.v6 where t0.v2 = 1000");
        SPMStmtExecutor.execute(connectContext, stmt);
        SPMPlanner planner = new SPMPlanner(connectContext);

        List<StatementBase> statements =
                SqlParser.parse("select t1.v4, t0.v2 from t0 join t1 on t0.v3 = t1.v6 where t0.v2 = 2",
                        connectContext.getSessionVariable());

        StatementBase query = planner.plan(statements.get(0));
        Assertions.assertTrue(planner.getBaseline().getId() > 1);
        Assertions.assertNotEquals(query, statements.get(0));
        assertContains(planner.getBaseline().planSql, "SELECT v2, v4 FROM " +
                "(SELECT * FROM t0 WHERE v2 = _spm_const_var(0)) t_0 INNER JOIN[SHUFFLE] " +
                "(SELECT * FROM t1 WHERE v6 IS NOT NULL) t_1 ON v3 = v6");
        assertContains(AstToSQLBuilder.toSQL(query), "SELECT `v2`, `v4`\n" +
                "FROM (SELECT *\n" +
                "FROM `t0`\n" +
                "WHERE `v2` = 2) `t_0` INNER JOIN [SHUFFLE] (SELECT *\n" +
                "FROM `t1`\n" +
                "WHERE `v6` IS NOT NULL) `t_1` ON `v3` = `v6`");
    }

    @Test
    public void testBindInPredicate() {
        SPMFunctions.enableSPMParamsPrint = true;
        CreateBaselinePlanStmt stmt = createBaselinePlanStmt(
                "select t1.v4, t0.v2 from t0 join[SHUFFLE] t1 on t0.v3 = t1.v6 where t0.v2 in (1,2,3,4)");
        SPMPlanBuilder generator = new SPMPlanBuilder(connectContext, stmt);

        generator.analyze();
        generator.parameterizedStmt();
        generator.generatePlan();
        SPMFunctions.enableSPMParamsPrint = false;

        assertContains(generator.getBindSqlDigest(), "SELECT `test`.`t1`.`v4`, `test`.`t0`.`v2`\n" +
                "FROM `test`.`t0` INNER JOIN `test`.`t1` ON `test`.`t0`.`v3` = `test`.`t1`.`v6`\n" +
                "WHERE `test`.`t0`.`v2` IN (?)");

        assertContains(generator.getBindSql(), "SELECT `test`.`t1`.`v4`, `test`.`t0`.`v2`\n" +
                "FROM `test`.`t0` INNER JOIN `test`.`t1` ON `test`.`t0`.`v3` = `test`.`t1`.`v6`\n" +
                "WHERE `test`.`t0`.`v2` IN (_spm_const_list(0, 1, 2, 3, 4))");

        assertContains(generator.getPlanStmtSQL(),
                "SELECT v2, v4 FROM " +
                        "(SELECT * FROM t0 WHERE v2 IN (_spm_const_list(0, 1, 2, 3, 4))) t_0 " +
                        "INNER JOIN[SHUFFLE] " +
                        "(SELECT * FROM t1 WHERE v6 IS NOT NULL) t_1 ON v3 = v6");
    }

    @Test
    public void testPlanHints() {
        SPMFunctions.enableSPMParamsPrint = true;
        CreateBaselinePlanStmt stmt = createBaselinePlanStmt(
                "select /*+SET_VAR(cbo_cte_reuse=false,cbo_push_down_aggregate=false)*/ * from t0");
        SPMPlanBuilder generator = new SPMPlanBuilder(connectContext, stmt);
        generator.analyze();
        generator.parameterizedStmt();
        generator.generatePlan();
        SPMFunctions.enableSPMParamsPrint = false;

        assertContains(generator.getBindSqlDigest(), "SELECT *\n" +
                "FROM `test`.`t0`");

        assertContains(generator.getBindSql(), "SELECT *\n" +
                "FROM `test`.`t0`");

        assertContains(generator.getPlanStmtSQL(),
                "SELECT /*+SET_VAR(cbo_cte_reuse=false,cbo_push_down_aggregate=false)*/* FROM t0");
    }

    @Test
    public void testBindPlan() {
        SPMFunctions.enableSPMParamsPrint = true;
        CreateBaselinePlanStmt stmt = createBaselinePlanStmt(
                "select t1.v4, t0.v2 from t0, t1 where t0.v3 = t1.v6 and t0.v2 in (1,2,3,4)",
                "SELECT v2, v4 FROM " +
                        "(SELECT * FROM t0 WHERE v2 IN (1, 2, 3, 4)) t_0 " +
                        " INNER JOIN[SHUFFLE] " +
                        " t1 ON v3 = v6");
        SPMPlanBuilder generator = new SPMPlanBuilder(connectContext, stmt);
        generator.analyze();
        generator.parameterizedStmt();
        generator.generatePlan();
        SPMFunctions.enableSPMParamsPrint = false;

        assertContains(generator.getBindSqlDigest(), "SELECT `test`.`t1`.`v4`, `test`.`t0`.`v2`\n" +
                "FROM `test`.`t0` , `test`.`t1` \n" +
                "WHERE (`test`.`t0`.`v3` = `test`.`t1`.`v6`) AND (`test`.`t0`.`v2` IN (?))");

        assertContains(generator.getBindSql(), "SELECT `test`.`t1`.`v4`, `test`.`t0`.`v2`\n" +
                "FROM `test`.`t0` , `test`.`t1` \n" +
                "WHERE (`test`.`t0`.`v3` = `test`.`t1`.`v6`) AND (`test`.`t0`.`v2` IN (_spm_const_list(1, 1, 2, 3, 4)))");

        assertContains(generator.getPlanStmtSQL(),
                "SELECT v2, v4 " +
                        "FROM (SELECT * FROM t0 WHERE v3 IS NOT NULL AND v2 IN (_spm_const_list(1, 1, 2, 3, 4))) t_0 " +
                        "INNER JOIN[SHUFFLE] (SELECT * FROM t1 WHERE v6 IS NOT NULL) t_1 ON v3 = v6");
    }

    @Test
    public void testUserBindPlan() {
        SPMFunctions.enableSPMParamsPrint = true;
        CreateBaselinePlanStmt stmt = createBaselinePlanStmt(
                "select t1.v4, t0.v2 from t0, t1 where t0.v3 = t1.v6 and " +
                        "t0.v2 in (_spm_const_list(1, 10, 11)) and t1.v5 = _spm_const_var(2, 5)",
                "SELECT v2, v4 FROM " +
                        "(SELECT * FROM t0 WHERE t0.v2 in (_spm_const_list(1, 10, 11)) ) t_0 " +
                        " INNER JOIN[SHUFFLE] " +
                        " (SELECT * FROM t1 WHERE v5 = _spm_const_var(2, 5)) t_1 ON v3 = v6");
        SPMPlanBuilder generator = new SPMPlanBuilder(connectContext, stmt);
        generator.analyze();
        generator.parameterizedStmt();
        generator.generatePlan();
        SPMFunctions.enableSPMParamsPrint = false;

        assertContains(generator.getBindSqlDigest(), "SELECT `test`.`t1`.`v4`, `test`.`t0`.`v2`\n" +
                "FROM `test`.`t0` , `test`.`t1` \n" +
                "WHERE (`test`.`t0`.`v3` = `test`.`t1`.`v6`) AND (`test`.`t0`.`v2` IN (?)) AND (`test`.`t1`.`v5` = ?)");

        assertContains(generator.getBindSql(), "SELECT `test`.`t1`.`v4`, `test`.`t0`.`v2`\n" +
                "FROM `test`.`t0` , `test`.`t1` \n" +
                "WHERE (`test`.`t0`.`v3` = `test`.`t1`.`v6`) " +
                "AND (`test`.`t0`.`v2` IN (_spm_const_list(1, 10, 11))) " +
                "AND (`test`.`t1`.`v5` = _spm_const_var(2, 5))");

        assertContains(generator.getPlanStmtSQL(), "SELECT v2, v4 FROM " +
                "(SELECT * FROM t0 WHERE v3 IS NOT NULL AND v2 IN (_spm_const_list(1, 10, 11))) t_0 " +
                "INNER JOIN[SHUFFLE] " +
                "(SELECT * FROM t1 WHERE v6 IS NOT NULL AND v5 = _spm_const_var(2, 5)) t_1 ON v3 = v6");
    }

    @Test
    public void testUserBindPlan2() {
        SPMFunctions.enableSPMParamsPrint = true;
        CreateBaselinePlanStmt stmt = createBaselinePlanStmt(
                "select t1.v4, t0.v2 from t0, t1 where t0.v3 = t1.v6 and " +
                        "t0.v2 in (_spm_const_list(1, 10, 11)) and t1.v5 = 1",
                "SELECT v2, v4 FROM " +
                        "(SELECT * FROM t0 WHERE t0.v2 in (_spm_const_list(1, 10, 11)) ) t_0 " +
                        " INNER JOIN[SHUFFLE] " +
                        " (SELECT * FROM t1 WHERE v5 = 1) t_1 ON v3 = v6");
        SPMPlanBuilder generator = new SPMPlanBuilder(connectContext, stmt);
        generator.analyze();
        generator.parameterizedStmt();
        generator.generatePlan();
        SPMFunctions.enableSPMParamsPrint = false;

        assertContains(generator.getBindSqlDigest(), "SELECT `test`.`t1`.`v4`, `test`.`t0`.`v2`\n" +
                "FROM `test`.`t0` , `test`.`t1` \n" +
                "WHERE (`test`.`t0`.`v3` = `test`.`t1`.`v6`) AND (`test`.`t0`.`v2` IN (?)) AND (`test`.`t1`.`v5` = ?)");

        assertContains(generator.getBindSql(), "SELECT `test`.`t1`.`v4`, `test`.`t0`.`v2`\n" +
                "FROM `test`.`t0` , `test`.`t1` \n" +
                "WHERE (`test`.`t0`.`v3` = `test`.`t1`.`v6`) " +
                "AND (`test`.`t0`.`v2` IN (_spm_const_list(1, 10, 11))) " +
                "AND (`test`.`t1`.`v5` = _spm_const_var(2, 5))");

        assertContains(generator.getPlanStmtSQL(), "SELECT v2, v4 FROM " +
                "(SELECT * FROM t0 WHERE v3 IS NOT NULL AND v2 IN (_spm_const_list(1, 10, 11))) t_0 " +
                "INNER JOIN[SHUFFLE] " +
                "(SELECT * FROM t1 WHERE v6 IS NOT NULL AND v5 = _spm_const_var(2, 5)) t_1 ON v3 = v6");
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
}
