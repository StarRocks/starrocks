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

import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TExplainLevel;
import org.junit.jupiter.api.Test;

import java.util.List;

public class SPMPlan2SQLTest extends PlanTestBase {
    @Test
    public void testPrintQuantifiedSubquery() throws Exception {
        String sql = "select * from t0 " +
                "join t1 on " +
                "t0.v1 in (select v7 from t2 where t2.v9 = t0.v3) " +
                "and t0.v2 not in (select v8 from t2 where t2.v9 = t0.v2)";
        ExecPlan plan = getExecPlan(sql);
        SPMPlan2SQLBuilder builder = new SPMPlan2SQLBuilder();

        String newSql = builder.toSQL(List.of(), plan.getPhysicalPlan(), plan.getOutputColumns());
        assertContains(newSql, "SELECT v1, v2, v3, v4, v5, v6 "
                + "FROM ("
                + "SELECT v1, v2, v3 "
                + "FROM ("
                + "SELECT v1, v2, v3 "
                + "FROM t0 LEFT SEMI JOIN[BROADCAST] "
                + "(SELECT * FROM t2 WHERE v7 IS NOT NULL AND v9 IS NOT NULL) t_0 ON v1 = v7 AND v3 = v9) t_1 "
                + "NULL AWARE LEFT ANTI JOIN[BROADCAST] t2 ON v2 = v8 AND v9 = v2) t_2 CROSS JOIN[BROADCAST] t1");

        String newPlan = getFragmentPlan(newSql);
        assertContains(newPlan, "LEFT SEMI JOIN");
        assertContains(newPlan, "NULL AWARE LEFT ANTI JOIN");
        assertContains(newPlan, "CROSS JOIN");
    }

    @Test
    public void testScalarSubquery() throws Exception {
        String sql = "select * from t0 where 2 = (select v4 from t1)";
        ExecPlan plan = getExecPlan(sql);
        SPMPlan2SQLBuilder builder = new SPMPlan2SQLBuilder();

        String newSql = builder.toSQL(List.of(), plan.getPhysicalPlan(), plan.getOutputColumns());
        assertContains(newSql, "SELECT v1, v2, v3 "
                + "FROM t0 CROSS JOIN[BROADCAST] (SELECT * FROM ASSERT_ROWS (SELECT v4 FROM t1) t_0 WHERE v4 = 2) t_1");

        String newPlan = getFragmentPlan(newSql);
        assertContains(newPlan, "CROSS JOIN");
        assertContains(newPlan, "  4:SELECT\n"
                + "  |  predicates: 4: v4 = 2\n"
                + "  |  \n"
                + "  3:ASSERT NUMBER OF ROWS\n"
                + "  |  assert number of rows: LE 1");
    }

    @Test
    public void testScalarSubquery2() throws Exception {
        String sql = "select * from t0 where v3 = (select v4 from t1)";
        ExecPlan execPlan = getExecPlan(sql);
        String plan = execPlan.getExplainString(TExplainLevel.NORMAL);

        SPMPlan2SQLBuilder builder = new SPMPlan2SQLBuilder();

        String newSql = builder.toSQL(List.of(), execPlan.getPhysicalPlan(), execPlan.getOutputColumns());
        assertContains(newSql, "SELECT v1, v2, v3 "
                + "FROM (SELECT * FROM t0 WHERE v3 IS NOT NULL) t_0 INNER JOIN[BROADCAST] "
                + "(SELECT * FROM ASSERT_ROWS (SELECT v4 FROM t1) t_1 WHERE v4 IS NOT NULL) t_2 ON v3 = v4");

        String newPlan = getFragmentPlan(newSql);
        assertContains(plan, newPlan);
    }

    @Test
    public void testScalarSubquery3() throws Exception {
        String sql = "select *, (select v4 from t1) as x from t0 ";
        ExecPlan execPlan = getExecPlan(sql);

        SPMPlan2SQLBuilder builder = new SPMPlan2SQLBuilder();
        String newSql = builder.toSQL(List.of(), execPlan.getPhysicalPlan(), execPlan.getOutputColumns());
        assertContains(newSql, "SELECT v1, v2, v3, v4 AS c_7 "
                + "FROM t0 CROSS JOIN[BROADCAST] ASSERT_ROWS (SELECT v4 FROM t1) t_0");

        String newPlan = getFragmentPlan(newSql);
        assertContains(newPlan, "  3:ASSERT NUMBER OF ROWS\n"
                + "  |  assert number of rows: LE 1\n"
                + "  |  \n"
                + "  2:EXCHANGE");
        assertContains(newPlan, "CROSS JOIN");
    }

    @Test
    public void testTableFunction() throws Exception {
        String sql = "select t1a, unnest from test_all_type t, unnest(split(t1a, ','))";

        ExecPlan execPlan = getExecPlan(sql);
        SPMPlan2SQLBuilder builder = new SPMPlan2SQLBuilder();
        String newSql = builder.toSQL(List.of(), execPlan.getPhysicalPlan(), execPlan.getOutputColumns());

        assertContains(newSql, "SELECT t1a, unnest "
                + "FROM (SELECT t1a, split(t1a, ',') AS c_12 FROM test_all_type) t_0, unnest(c_12)");

        String newPlan = getFragmentPlan(newSql);
        assertContains(newPlan, "  2:TableValueFunction\n"
                + "  |  tableFunctionName: unnest");
    }

    @Test
    public void testTableFunctionScan() throws Exception {
        String sql = "SELECT * FROM TABLE(unnest(ARRAY<INT>[1])) t0(x)";

        ExecPlan execPlan = getExecPlan(sql);
        SPMPlan2SQLBuilder builder = new SPMPlan2SQLBuilder();
        String newSql = builder.toSQL(List.of(), execPlan.getPhysicalPlan(), execPlan.getOutputColumns());

        assertContains(newSql, "SELECT x "
                + "FROM (SELECT array<int>[1] AS c_3 FROM (VALUES (null)) AS t(c_1)) t_0, unnest(c_3) AS t_1(x)");

        String newPlan = getFragmentPlan(newSql);
        assertContains(newPlan, "  2:TableValueFunction\n"
                + "  |  tableFunctionName: unnest");
    }
}
