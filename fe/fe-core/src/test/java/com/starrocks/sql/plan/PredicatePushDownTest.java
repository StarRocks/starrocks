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

import org.junit.Test;

public class PredicatePushDownTest extends PlanTestBase {

    @Test
    public void testCoalescePushDown() throws Exception {
        {
            String sql = "select * from t0 where coalesce(v1 < 2)";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "PREDICATES: 1: v1 < 2");
        }
        {
            String sql = "select * from t0 where coalesce(v1 < 2, null)";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "PREDICATES: 1: v1 < 2");
        }
        {
            String sql = "select * from t0 where coalesce(v1 < 2, null, null)";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "PREDICATES: 1: v1 < 2");
        }
        {
            String sql = "select * from t0 where coalesce(v1 < 2, null, false)";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "PREDICATES: 1: v1 < 2");
        }
        {
            String sql = "select * from t0 where coalesce(v1 < 2, null, false) and v3 > 2";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "PREDICATES: 1: v1 < 2, 3: v3 > 2");
        }
        {
            String sql = "select * from t0 where coalesce(v1 < 2, null, false) or v3 > 2";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "(coalesce(1: v1 < 2, NULL, FALSE)) OR (3: v3 > 2)");
        }
        {
            String sql = "select * from t0 where coalesce(v1 < 2, null, false) is null";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "PREDICATES: coalesce(1: v1 < 2, NULL, FALSE) IS NULL");
        }
        {
            String sql = "select coalesce(v1 < 2, null, false) from t0";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "coalesce(1: v1 < 2, NULL, FALSE)");
        }
        {
            String sql = "select * from t0 where coalesce(v1 < 2, null, true)";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "PREDICATES: coalesce(1: v1 < 2, NULL, TRUE)");
        }
        {
            String sql = "select * from t0 where coalesce(v1 < 2, null, v2 > 1)";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "predicates: coalesce(1: v1 < 2, NULL, 2: v2 > 1)");
        }
        {
            String sql = "WITH cte0 AS (\n" +
                    "    SELECT v1, v2, (\n" +
                    "        v3 BETWEEN 1 AND 3\n" +
                    "    ) AS v3\n" +
                    "    FROM t0\n" +
                    "),\n" +
                    "cte1 AS (\n" +
                    "    SELECT cte0.v1 AS v1, cte0.v2 AS v2, cte0.v3 AS v3\n" +
                    "    FROM cte0\n" +
                    "),\n" +
                    "cte2 AS (\n" +
                    "    SELECT cte1.v1 AS v1, cte1.v2 AS v2, COALESCE(cte1.v3, false) AS v3\n" +
                    "    FROM cte1\n" +
                    "),\n" +
                    "cte3 AS (\n" +
                    "    SELECT * FROM cte2\n" +
                    "    WHERE cte2.v3\n" +
                    ")\n" +
                    "SELECT * FROM cte3;";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "PREDICATES: 3: v3 >= 1, 3: v3 <= 3");
        }
        {
            String sql = "WITH cte0 AS (\n" +
                    "    SELECT v1, v2, (\n" +
                    "        v3 BETWEEN 1 AND 3\n" +
                    "    ) AS v3\n" +
                    "    FROM t0\n" +
                    "),\n" +
                    "cte1 AS (\n" +
                    "    SELECT cte0.v1 AS v1, cte0.v2 AS v2, cte0.v3 AS v3\n" +
                    "    FROM cte0\n" +
                    "),\n" +
                    "cte2 AS (\n" +
                    "    SELECT cte1.v1 AS v1, cte1.v2 AS v2, COALESCE(cte1.v3, false) AS v3\n" +
                    "    FROM cte1\n" +
                    ")\n" +
                    "SELECT * FROM cte2;";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "<slot 5> : coalesce((3: v3 >= 1) AND (3: v3 <= 3), FALSE)");
        }
    }

    @Test
    public void testNonDeterministicFunctionInCTE() throws Exception {
        // Test that predicates with non-deterministic functions are not pushed down through Project
        // This prevents rand() from being calculated twice in CTE scenarios
        String sql = "WITH input AS (SELECT v1, rand() AS rn FROM t0) " +
                "SELECT v1, rn, rn < 0.5 FROM input WHERE rn < 0.5";
        String plan = getFragmentPlan(sql);

        // The plan should not push down the predicate through the project that contains rand()
        // Instead, the filter should remain above the project
        // The filter should not be pushed down to the scan level
        assertContains(plan, "3:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 4> : 4: rand\n" +
                "  |  <slot 5> : 4: rand < 0.5\n" +
                "  |  \n" +
                "  2:SELECT\n" +
                "  |  predicates: 4: rand < 0.5\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 4> : rand()\n" +
                "  |  \n" +
                "  0:OlapScanNode");
    }
<<<<<<< HEAD
=======

    @Test
    public void testDisablePredicatePushdown() throws Exception {
        connectContext.getSessionVariable().setCboDisabledRules("GP_PUSH_DOWN_PREDICATE");
        String sql = "select * from t0 where coalesce(v1 < 2)";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "1:SELECT\n" +
                "  |  predicates: 1: v1 < 2\n" +
                "  |  \n" +
                "  0:OlapScanNode");
        connectContext.getSessionVariable().setCboDisabledRules("");
    }

    @Test
    public void testNonDeterministicFunctionPushDown1() throws Exception {
        String sql = "WITH input AS (select t0.v1, t1.v5 from t0 join t1 on t0.v1=t1.v4) " +
                "SELECT v1, v5  from input WHERE rand() < 0.5";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  3:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 4: v4 = 1: v1\n" +
                "  |  other join predicates: rand() < 0.5");
    }

    @Test
    public void testNonDeterministicFunctionPushDown2() throws Exception {
        String sql = "WITH input AS (select t0.v1, t1.v5 from t0 left join t1 on t0.v1=t1.v4) " +
                "SELECT v1, v5  from input WHERE rand() < 0.5";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  4:HASH JOIN\n" +
                "  |  join op: RIGHT OUTER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 4: v4 = 1: v1\n" +
                "  |  other predicates: rand() < 0.5");
    }

    @Test
    public void testNonDeterministicFunctionPushDown3() throws Exception {
        String sql = "WITH input AS (select t0.v1, t1.v5 from t0 full join t1 on t0.v1=t1.v4) " +
                "SELECT v1, v5  from input WHERE rand() < 0.5";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  4:HASH JOIN\n" +
                "  |  join op: FULL OUTER JOIN (PARTITIONED)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 4: v4 = 1: v1\n" +
                "  |  other predicates: rand() < 0.5");
    }

    @Test
    public void testNonDeterministicFunctionPushDown4() throws Exception {
        String sql = "WITH input AS (select t0.v1, t1.v5 from t0 cross join t1 on t0.v1>t1.v4) " +
                "SELECT v1, v5  from input WHERE rand() < 0.5";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  3:NESTLOOP JOIN\n" +
                "  |  join op: INNER JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  other join predicates: 1: v1 > 4: v4, rand() < 0.5");
    }

    @Test
    public void testNonDeterministicFunctionPushDown5() throws Exception {
        String sql = "WITH input AS (select * from t0 where t0.v1 in (select max(v4) from t1)) " +
                "SELECT * from input WHERE rand() < 0.5";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  4:HASH JOIN\n" +
                "  |  join op: LEFT SEMI JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 7: max\n" +
                "  |  other predicates: rand() < 0.5");
    }

    @Test
    public void testNonDeterministicFunctionPushDown6() throws Exception {
        String sql = "WITH input AS (select * from t0 where t0.v1 not in (select max(v4) from t1)) " +
                "SELECT * from input WHERE rand() < 0.5";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  6:HASH JOIN\n" +
                "  |  join op: NULL AWARE LEFT ANTI JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 7: max\n" +
                "  |  other predicates: rand() < 0.5");
    }

    @Test
    public void testNonDeterministicFunctionPushDown7() throws Exception {
        starRocksAssert.withView("create view test_view1 as select * from t0 where t0.v1 " +
                "not in (select max(v4) from t1);");
        String sql = "SELECT * from test_view1 WHERE rand() < 0.5";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  6:HASH JOIN\n" +
                "  |  join op: NULL AWARE LEFT ANTI JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 7: max\n" +
                "  |  other predicates: rand() < 0.5");
    }

    @Test
    public void testNonDeterministicFunctionPushDown8() throws Exception {
        String sql = "WITH input AS (select v1, count(v2), sum(v3) from t0 group by t0.v1) " +
                "SELECT * from input WHERE v1 > 1 and rand() < 0.5";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  1:AGGREGATE (update finalize)\n" +
                "  |  output: count(2: v2), sum(3: v3)\n" +
                "  |  group by: 1: v1\n" +
                "  |  having: rand() < 0.5");
    }

    @Test
    public void testNonDeterministicFunctionPushDown9() throws Exception {
        String sql = "select * from (select v1, v2, v3, grouping_id(v1, v3), grouping(v2) " +
                "from t0 group by rollup(v1, v2, v3)) x where coalesce(v1, v2, v3) = 1 and rand() < 0.5;";
        String planFragment = getFragmentPlan(sql);
        assertContains(planFragment, "  4:AGGREGATE (merge finalize)\n" +
                "  |  group by: 1: v1, 2: v2, 3: v3, 4: GROUPING_ID, 5: GROUPING, 6: GROUPING\n" +
                "  |  having: rand() < 0.5");
    }

    @Test
    public void testNonDeterministicFunctionPushDown10() throws Exception {
        String sql = "with input as (select * from test_all_type, unnest(split(t1a, ',')) as unnest_tbl(a)) " +
                "select * from input where a > 1 and rand() < 0.5";
        String planFragment = getFragmentPlan(sql);
        assertContains(planFragment, "  3:SELECT\n" +
                "  |  predicates: CAST(11: a AS DOUBLE) > 1.0, rand() < 0.5\n" +
                "  |  \n" +
                "  2:TableValueFunction\n" +
                "  |  tableFunctionName: unnest\n" +
                "  |  columns: [unnest]\n" +
                "  |  returnTypes: [VARCHAR]");
    }

    @Test
    public void testNonDeterministicFunctionPushDown11() throws Exception {
        String sql = "WITH input AS (select v1, sum(v2) over (partition by v1) from t0) " +
                "SELECT * from input WHERE v1 > 1 and rand() < 0.5";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  3:SELECT\n" +
                "  |  predicates: rand() < 0.5\n" +
                "  |  \n" +
                "  2:ANALYTIC\n" +
                "  |  functions: [, sum(2: v2), ]\n" +
                "  |  partition by: 1: v1");
    }
>>>>>>> a9bb16d4da ([BugFix] Disable push down non-determined functions down operators (#66323))
}
