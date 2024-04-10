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

package com.starrocks.sql.optimizer.rewrite;

import com.starrocks.sql.plan.PlanTestBase;
import org.junit.BeforeClass;
import org.junit.Test;

public class JoinPredicatePushdownTest extends PlanTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
    }

    @Test
    public void testJoinPushdownCTE() throws Exception {
        // enable cte reuse to trigger calling PUSH_DOWN_PREDICATE rule set twice
        connectContext.getSessionVariable().setCboCteReuse(true);
        String query = "with xxx1 as (\n" +
                "with x as (select * from t1 join t2 where t1.v4 = t2.v7 )\n" +
                "select x1.v6, x2.v7 \n" +
                "from (select * from x where x.v5 = 1 ) x1 left outer join" +
                " (select * from x where x.v8 = 2) x2 on x1.v4 = x2.v7)\n" +
                "select * from xxx1 where xxx1.v6 = 2\n" +
                "union \n" +
                "select * from xxx1 where xxx1.v7 = 3";
        String plan = getFragmentPlan(query);
        // must has left outer join
        PlanTestBase.assertContains(plan, "13:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 7: v4 = 16: v7\n" +
                "  |  other predicates: (16: v7 = 3) OR (9: v6 = 2)");
    }

    @Test
    public void testMultiLeftOuterJoin2() throws Exception {
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(3000000);
        connectContext.getSessionVariable().disableJoinReorder();
        String query = "select x.v1 v11, x.v2 v21, x.v3 v31, sub2.v1 v12, sub2.v2 v22 from test.t0 x inner join" +
                " (select v1, v2, v3, v4, v5, v8 " +
                "from test.t0 left outer join (select * from test.t1 " +
                "inner join test.t2 on v5 = v7) sub on v1 = v4) sub2 on x.v1 = sub2.v1";
        String plan = getFragmentPlan(query);
    }

    @Test
    public void testMultiLeftOuterJoin() throws Exception {
        String query = "select v1, v2, v5, v8 " +
                "from test.t0 left outer join test.t1 on v1 = v4 " +
                "left outer join test.t2 on v5 = v7 where v9 = 10";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "3:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 4: v4");
        PlanTestBase.assertContains(plan, "8:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 5: v5 = 7: v7");

        String query2 = "select v1, v2, v5, v8 " +
                "from test.t0 left outer join test.t1 on v1 = v4 " +
                "left outer join test.t2 on v5 = v7 where v9 = 10 and v3 = 1";
        String plan2 = getFragmentPlan(query2);
        PlanTestBase.assertContains(plan2, "9:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 5: v5 = 7: v7");
        PlanTestBase.assertContains(plan2, "4:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 4: v4");
        PlanTestBase.assertNotContains(plan2, "LEFT OUTER JOIN");
    }

    @Test
    public void testMultiRightOuterJoin() throws Exception {
        String query = "select v1, v2, v5, v8 " +
                "from test.t0 right outer join test.t1 on v1 = v4 " +
                "right outer join test.t2 on v2 = v7 where v3 = 10";
        String plan = getFragmentPlan(query);
        PlanTestBase.assertContains(plan, "4:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 4: v4");
        PlanTestBase.assertContains(plan, "8:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: v2 = 7: v7");

        String query2 = "select v1, v2, v5, v8 " +
                "from test.t0 right outer join test.t1 on v1 = v4 " +
                "right outer join test.t2 on v2 = v7 where v5 = 10";
        String plan2 = getFragmentPlan(query2);
        PlanTestBase.assertContains(plan2, "7:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: v2 = 7: v7");
        PlanTestBase.assertContains(plan2, "3:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 4: v4");
        PlanTestBase.assertNotContains(plan2, "LEFT OUTER JOIN");
    }
}
