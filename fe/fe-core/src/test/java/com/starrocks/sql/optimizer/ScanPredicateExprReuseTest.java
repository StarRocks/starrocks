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

package com.starrocks.sql.optimizer;

import com.starrocks.sql.plan.PlanTestBase;
import org.junit.Test;

public class ScanPredicateExprReuseTest extends PlanTestBase {
    @Test
    public void test() throws Exception {
        // 1. all predicates can be pushed down to scan node, no need to reuse common expressions
        {
            String sql = "select * from t0 where v1 = 10 and v2 = 5";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  0:OlapScanNode\n" +
                    "     TABLE: t0\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     PREDICATES: 1: v1 = 10, 2: v2 = 5");
        }
        {
            String sql = "select * from t0 where v1 = 10 or v2 = 5";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  0:OlapScanNode\n" +
                    "     TABLE: t0\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     PREDICATES: (1: v1 = 10) OR (2: v2 = 5)");
        }
        {
            String sql = "select * from t0 where v1 in (10, 20) and v2 = 5";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  0:OlapScanNode\n" +
                    "     TABLE: t0\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     PREDICATES: 1: v1 IN (10, 20), 2: v2 = 5");
        }
        {
            String sql = "select * from t0 where bit_shift_left(v1,1) = 10 and v2 = 5";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  0:OlapScanNode\n" +
                    "     TABLE: t0\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     PREDICATES: 1: v1 BITSHIFTLEFT 1 = 10, 2: v2 = 5");
        }
        {
            String sql = "select * from t0 where v1 > v2";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "PREDICATES: 1: v1 > 2: v2");
        }
        {
            String sql = "select * from t0 where v1 > v2 or v1 = 10";
            String plan = getFragmentPlan(sql);
            assertContains(plan, " PREDICATES: (1: v1 > 2: v2) OR (1: v1 = 10)");
        }
        // 2. all predicates can't be pushed down
        {
            String sql = "select * from tarray where all_match(x->x>10, v3)";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  1:SELECT\n" +
                    "  |  predicates: all_match(array_map(<slot 4> -> <slot 4> > 10, 3: v3))");
        }
        // 3. some predicates can be pushed down
        {
            String sql = "select * from t0 where v1 + v2 > 10 and v1 + v2 + v3 > 20 and v1 = 5";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  1:SELECT\n" +
                    "  |  predicates: 4: add > 10, 4: add + 3: v3 > 20\n" +
                    "  |    common sub expr:\n" +
                    "  |    <slot 4> : 1: v1 + 2: v2\n" +
                    "  |  \n" +
                    "  0:OlapScanNode\n" +
                    "     TABLE: t0\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     PREDICATES: 1: v1 = 5");
        }
        {
            String sql = "select * from tarray where v1 = 10 and array_max(array_map(x->x + v2, v3)) > 1";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  1:SELECT\n" +
                    "  |  predicates: array_max(array_map(<slot 4> -> <slot 4> + 2: v2, 3: v3)) > 1\n" +
                    "  |  \n" +
                    "  0:OlapScanNode\n" +
                    "     TABLE: tarray\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     PREDICATES: 1: v1 = 10");
        }
        {
            String sql = "with input as (\n" +
                    "    select array_min(array_map(x->x+v1, v3)) as x from tarray\n" +
                    "),\n" +
                    "input2 as (\n" +
                    "    select x + 1 as a, x + 2 as b, x + 3 as c from input\n" +
                    ")\n" +
                    "select * from input2 where a + b + c < 10;";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "  2:SELECT\n" +
                    "  |  predicates: 6: expr + 7: expr + 8: expr < 10\n" +
                    "  |  \n" +
                    "  1:Project\n" +
                    "  |  <slot 6> : 10: array_min + 1\n" +
                    "  |  <slot 7> : 10: array_min + 2\n" +
                    "  |  <slot 8> : 10: array_min + 3\n" +
                    "  |  common expressions:\n" +
                    "  |  <slot 9> : array_map(<slot 4> -> <slot 4> + 1: v1, 3: v3)\n" +
                    "  |  <slot 10> : array_min(9: array_map)");
        }
    }
}
