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
}
