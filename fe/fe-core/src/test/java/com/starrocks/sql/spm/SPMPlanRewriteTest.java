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
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.spm.CreateBaselinePlanStmt;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

public class SPMPlanRewriteTest extends PlanTestBase {
    @BeforeAll
    public static void beforeAll() throws Exception {
        PlanTestBase.beforeAll();
        connectContext.getSessionVariable().setEnableSPMRewrite(true);
        connectContext.getSessionVariable().setSpmRewriteTimeoutMs(-1);
    }

    @BeforeEach
    public void before() {
        SPMFunctions.enableSPMParamsPrint = true;
        connectContext.getSqlPlanStorage().dropAllBaselinePlans();
    }

    public CreateBaselinePlanStmt createBaselinePlanStmt(String sql) {
        String createSql = "create baseline using " + sql;
        List<StatementBase> statements = SqlParser.parse(createSql, connectContext.getSessionVariable());
        Preconditions.checkState(statements.size() == 1);
        Preconditions.checkState(statements.get(0) instanceof CreateBaselinePlanStmt);
        return (CreateBaselinePlanStmt) statements.get(0);
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
    public void testSPMReplaceJoinPlan() {
        SPMFunctions.enableSPMParamsPrint = false;
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
        assertContains(planner.getBaseline().getPlanSql(),
                "SELECT v4, v2 FROM (SELECT v2, v4 FROM "
                        + "(SELECT * FROM t0 WHERE v2 = _spm_const_var(1)) t_0 INNER JOIN[SHUFFLE] t1 ON v3 = v6) t2");
        assertContains(AstToSQLBuilder.toSQL(query), "SELECT `v4`, `v2`\n"
                + "FROM (SELECT `v2`, `v4`\n"
                + "FROM (SELECT *\n"
                + "FROM `t0`\n"
                + "WHERE `v2` = 2) `t_0` INNER JOIN [SHUFFLE] `t1` ON `v3` = `v6`) `t2`");
    }

    @Test
    public void testSPMFunctionRangePlan() throws Exception {
        SPMFunctions.enableSPMParamsPrint = false;
        CreateBaselinePlanStmt stmt = createBaselinePlanStmt(
                "select t1.v4, t0.v2 from t0 join[SHUFFLE] t1 on t0.v3 = t1.v6 "
                        + "where t0.v2 = _spm_const_range(1, 1, 10000)");
        BaselinePlan base1 = SPMStmtExecutor.execute(connectContext, stmt);

        {
            String plan = getFragmentPlan("select t1.v4, t0.v2 from t0 join[SHUFFLE] t1 on t0.v3 = t1.v6 "
                    + "where t0.v2 = 123");
            assertContains(plan, "Using baseline plan[" + base1.getId() + "]");
        }
        {
            String plan = getFragmentPlan("select t1.v4, t0.v2 from t0 join[SHUFFLE] t1 on t0.v3 = t1.v6 "
                    + "where t0.v2 = -12313");
            assertNotContains(plan, "Using baseline");
        }
        {
            String plan = getFragmentPlan("select t1.v4, t0.v2 from t0 join[SHUFFLE] t1 on t0.v3 = t1.v6 "
                    + "where t0.v2 = 10001");
            assertNotContains(plan, "Using baseline");
        }
        stmt = createBaselinePlanStmt(
                "select t1.v4, t0.v2 from t0 join[SHUFFLE] t1 on t0.v3 = t1.v6 "
                        + "where t0.v2 = _spm_const_range(1, 10000, 20000)");
        BaselinePlan b2 = SPMStmtExecutor.execute(connectContext, stmt);
        stmt = createBaselinePlanStmt(
                "select t1.v4, t0.v2 from t0 join[SHUFFLE] t1 on t0.v3 = t1.v6 "
                        + "where t0.v2 = _spm_const_range(1, 30000, 40000)");
        BaselinePlan b3 = SPMStmtExecutor.execute(connectContext, stmt);
        stmt = createBaselinePlanStmt(
                "select t1.v4, t0.v2 from t0 join[SHUFFLE] t1 on t0.v3 = t1.v6 "
                        + "where t0.v2 = _spm_const_var(1)");
        BaselinePlan b4 = SPMStmtExecutor.execute(connectContext, stmt);
        {
            String plan = getFragmentPlan("select t1.v4, t0.v2 from t0 join[SHUFFLE] t1 on t0.v3 = t1.v6 "
                    + "where t0.v2 = 15000");
            assertContains(plan, "Using baseline plan[" + b2.getId() + "]");
        }
        {
            String plan = getFragmentPlan("select t1.v4, t0.v2 from t0 join[SHUFFLE] t1 on t0.v3 = t1.v6 "
                    + "where t0.v2 = 35000");
            assertContains(plan, "Using baseline plan[" + b3.getId() + "]");
        }
        {
            String plan = getFragmentPlan("select t1.v4, t0.v2 from t0 join[SHUFFLE] t1 on t0.v3 = t1.v6 "
                    + "where t0.v2 = 46001");
            assertContains(plan, "Using baseline plan[" + b4.getId() + "]");
        }
    }

    @Test
    public void testSPMFunctionEnumPlan() throws Exception {
        SPMFunctions.enableSPMParamsPrint = false;
        CreateBaselinePlanStmt stmt = createBaselinePlanStmt(
                "select t1.v4, t0.v2 from t0 join[SHUFFLE] t1 on t0.v3 = t1.v6 "
                        + "where t0.v2 = _spm_const_enum(1, 1, 2, 3, 4)");
        BaselinePlan base1 = SPMStmtExecutor.execute(connectContext, stmt);

        {
            String plan = getFragmentPlan("select t1.v4, t0.v2 from t0 join[SHUFFLE] t1 on t0.v3 = t1.v6 "
                    + "where t0.v2 = 2");
            assertContains(plan, "Using baseline plan[" + base1.getId() + "]");
        }
        {
            String plan = getFragmentPlan("select t1.v4, t0.v2 from t0 join[SHUFFLE] t1 on t0.v3 = t1.v6 "
                    + "where t0.v2 = 5");
            assertNotContains(plan, "Using baseline");
        }
        {
            String plan = getFragmentPlan("select t1.v4, t0.v2 from t0 join[SHUFFLE] t1 on t0.v3 = t1.v6 "
                    + "where t0.v2 = 10");
            assertNotContains(plan, "Using baseline");
        }
        stmt = createBaselinePlanStmt(
                "select t1.v4, t0.v2 from t0 join[SHUFFLE] t1 on t0.v3 = t1.v6 "
                        + "where t0.v2 = _spm_const_range(1, 5, 10)");
        BaselinePlan b2 = SPMStmtExecutor.execute(connectContext, stmt);
        stmt = createBaselinePlanStmt(
                "select t1.v4, t0.v2 from t0 join[SHUFFLE] t1 on t0.v3 = t1.v6 "
                        + "where t0.v2 = _spm_const_var(1)");
        BaselinePlan b4 = SPMStmtExecutor.execute(connectContext, stmt);
        {
            String plan = getFragmentPlan("select t1.v4, t0.v2 from t0 join[SHUFFLE] t1 on t0.v3 = t1.v6 "
                    + "where t0.v2 = 6");
            assertContains(plan, "Using baseline plan[" + b2.getId() + "]");
        }
        {
            String plan = getFragmentPlan("select t1.v4, t0.v2 from t0 join[SHUFFLE] t1 on t0.v3 = t1.v6 "
                    + "where t0.v2 = 46001");
            assertContains(plan, "Using baseline plan[" + b4.getId() + "]");
        }
    }
}