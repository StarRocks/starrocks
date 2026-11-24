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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PivotTest extends PlanTestBase {

    @Test
    public void testSubqueryPivot() throws Exception {
        String sql = "select * from (select t1b, t1c, t1d, t1g, t1a from test_all_type_not_null) t"
                + " pivot (sum(t1g) for t1a in ('a', 'b'))";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  2:AGGREGATE (update finalize)\n"
                + "  |  output: sum(11: case), sum(12: case)\n"
                + "  |  group by: 2: t1b, 3: t1c, 4: t1d");
    }

    @Test
    public void testSubqueryInMeasurePlannedOnce() throws Exception {
        // RB-1380: every pivot value gets agg(CASE WHEN ... THEN <measure argument> END). A subquery in the argument
        // must be planned once, not once per pivot value.
        String sql = "select * from (select v1, v2, v3 from t0) t pivot (" +
                "sum((v2 not in (select v4 from t1)) = (v3 not in (select v5 from t1))) " +
                "for v1 in (1, 2, 3, 4))";
        String plan = getFragmentPlan(sql);
        Assertions.assertEquals(2, plan.split("TABLE: t1", -1).length - 1, plan);
        Assertions.assertEquals(1, plan.split("TABLE: t0", -1).length - 1, plan);
        assertContains(plan, "output: sum(");
    }
}
