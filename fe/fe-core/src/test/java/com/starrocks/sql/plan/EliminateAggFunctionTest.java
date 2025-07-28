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

import com.starrocks.common.FeConstants;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class EliminateAggFunctionTest extends PlanTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
    }

    @Test
    public void testEliminateFunction() throws Exception {
        String sql;
        String plan;
        {
            sql = "select max(v1) from t0 group by v1";
            plan = getFragmentPlan(sql);
            assertContains(plan, "  1:AGGREGATE (update finalize)\n" +
                    "  |  group by: 1: v1");
        }
        {
            // multi case
            sql = "select max(v1), sum(v1) from t0 group by v1";
            plan = getFragmentPlan(sql);
            assertContains(plan, "  1:AGGREGATE (update finalize)\n" +
                    "  |  output: sum(1: v1)\n" +
                    "  |  group by: 1: v1");
        }
        {
            // basic prune case
            sql = "select max(v1) from t0 group by v1, v2";
            plan = getFragmentPlan(sql);
            assertContains(plan, "  1:AGGREGATE (update finalize)\n" +
                    "  |  group by: 1: v1, 2: v2");
        }
        {
            // multi-columns
            sql = "select max(v1), v2 from t0 group by v1, v2";
            plan = getFragmentPlan(sql);
            assertContains(plan, "  2:Project\n" +
                    "  |  <slot 2> : 2: v2\n" +
                    "  |  <slot 4> : 1: v1\n" +
                    "  |  \n" +
                    "  1:AGGREGATE (update finalize)\n" +
                    "  |  group by: 1: v1, 2: v2");
        }
        {
            // having predicate
            sql = "select max(v1), v2 from t0 group by v1, v2 having max(v1) < 10;";
            plan = getFragmentPlan(sql);
            assertContains(plan, "  1:AGGREGATE (update finalize)\n" +
                    "  |  group by: 1: v1, 2: v2\n" +
                    "  |  having: 1: v1 < 10");
        }
        {
            // multi used predicate
            sql = "select max(v1), v2 from t0 group by v1, v2 having max(v1) + v2 < 10;";
            plan = getFragmentPlan(sql);
            assertContains(plan, "  1:AGGREGATE (update finalize)\n" +
                    "  |  group by: 1: v1, 2: v2\n" +
                    "  |  having: 1: v1 + 2: v2 < 10");
        }
        {
            // multi function
            sql = "select max(v1) + 1, max(v1),v2 from t0 group by v1, v2 having max(v1) + v2 < 10;";
            plan = getFragmentPlan(sql);
            assertContains(plan, "  2:Project\n" +
                    "  |  <slot 2> : 2: v2\n" +
                    "  |  <slot 4> : 1: v1\n" +
                    "  |  <slot 5> : 1: v1 + 1");
        }
        {
            // group by with order by
            sql = "select max(v1), min(v1) from t0 group by v1, v2 order by 1,2";
            plan = getFragmentPlan(sql);
            assertContains(plan, "  3:SORT\n" +
                    "  |  order by: <slot 4> 4: max ASC, <slot 5> 5: min ASC\n" +
                    "  |  offset: 0");
        }
        {
            // partial support
            sql = "select max(v1), min(upper(v1)) from t0 group by v1, v2 order by 1,2";
            plan = getFragmentPlan(sql);
            assertContains(plan, "  3:Project\n" +
                    "  |  <slot 5> : 1: v1\n" +
                    "  |  <slot 6> : 6: min\n" +
                    "  |  \n" +
                    "  2:AGGREGATE (update finalize)\n" +
                    "  |  output: min(4: upper)\n" +
                    "  |  group by: 1: v1, 2: v2");
        }
        {
            // 2-stage aggregate
            sql = "select max(v3), max(v3) + 1, min(v3)  from t0 group by v3 having min(v3) + max(v3) < 10 order by 1";
            plan = getFragmentPlan(sql);
            assertContains(plan, "  4:Project\n" +
                    "  |  <slot 4> : clone(3: v3)\n" +
                    "  |  <slot 5> : 3: v3\n" +
                    "  |  <slot 6> : 3: v3 + 1\n" +
                    "  |  \n" +
                    "  3:AGGREGATE (merge finalize)\n" +
                    "  |  group by: 3: v3\n" +
                    "  |  having: 3: v3 + 3: v3 < 10");
        }

    }

}
