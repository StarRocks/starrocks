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

import com.google.common.collect.Lists;
import com.starrocks.common.FeConstants;
import org.junit.Test;
import org.junit.jupiter.api.BeforeAll;

import java.util.List;

public class OuterJoinReorderTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(30000);
        FeConstants.runningUnitTest = true;
    }

    @Test
    public void joinAssocRuleSqls() throws Exception {
        List<String> sqlList = Lists.newArrayList();
        List<String> planList = Lists.newArrayList();
        sqlList.add("select t1.* from t0 join t1 on v1 > v4 left join t2 on v1 < v7 ");
        planList.add("8:Project\n" +
                "  |  <slot 4> : 4: v4\n" +
                "  |  <slot 5> : 5: v5\n" +
                "  |  <slot 6> : 6: v6\n" +
                "  |  \n" +
                "  7:NESTLOOP JOIN\n" +
                "  |  join op: INNER JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  other join predicates: 1: v1 > 4: v4");
        sqlList.add("select * from t0 join t1 join t2 on v1 = v4 + v7");
        planList.add("7:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 10: add = 1: v1");
        sqlList.add("select * from t0 left join (select v4 from t1 union select v7 from t2) t1 on v2 > v4 " +
                "left semi join t2 on v1 = v7");
        planList.add("3:HASH JOIN\n" +
                "  |  join op: LEFT SEMI JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 11: v7");
        sqlList.add("select subq_0.v1 as c0 from (select 88 as v1 from t0 as ref_0) as subq_0 inner join t1 as ref_1 " +
                "on (subq_0.v1 = ref_1.v4) inner join t2 as ref_2 on (subq_0.v1 = ref_2.v7) inner join t3 as ref_3 " +
                "on (ref_2.v8 = ref_3.v10) inner join t4 as ref_4 on (ref_2.v9 = ref_4.v14) " +
                "where (space(cast(ref_4.v15 as INT)) <= ref_4.v14) or ( ref_1.v5 = tan(cast(ref_1.v6 as DOUBLE))) " +
                "limit 111;");
        planList.add("7:NESTLOOP JOIN\n" +
                "  |  join op: INNER JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  other join predicates: (CAST(space(CAST(16: v15 AS INT)) AS DOUBLE) <= CAST(15: v14 AS DOUBLE)) " +
                "OR (CAST(6: v5 AS DOUBLE) = tan(CAST(7: v6 AS DOUBLE)))");

        sqlList.add("select tmp.a, t0.v3 from t0 left join (select * from test_all_type, unnest(split(t1a, ',')) " +
                "as unnest_tbl(a)) tmp on t0.v1 = tmp.a join t1 on t0.v2 = t1.v4");
        planList.add("10:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 19: cast = 14: a\n" +
                "  |  \n" +
                "  |----9:EXCHANGE\n" +
                "  |    \n" +
                "  5:Project\n" +
                "  |  <slot 3> : 3: v3\n" +
                "  |  <slot 19> : 19: cast\n" +
                "  |  \n" +
                "  4:HASH JOIN");

        sqlList.add("select tmp.a, t0.v3 from t0 left join (select * from tarray, unnest(v3) as unnest_tbl(a)) " +
                "tmp on t0.v1 = tmp.a join t1 on t0.v2 = t1.v4");
        planList.add("8:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 7: a\n" +
                "  |  \n" +
                "  |----7:EXCHANGE\n" +
                "  |    \n" +
                "  4:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 3> : 3: v3\n" +
                "  |  \n" +
                "  3:HASH JOIN");
        sqlList.add("select t0.* from t0 left join t1 on t0.v1 = t1.v4 join t2 on t0.v1 = t2.v7 " +
                "where t0.v2 in (select max(v10) from t3) and t1.v5 is null;");
        planList.add(" 12:HASH JOIN\n" +
                "  |  join op: LEFT SEMI JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: v2 = 13: max\n" +
                "  |  \n" +
                "  |----11:EXCHANGE\n" +
                "  |    \n" +
                "  8:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 2> : 2: v2\n" +
                "  |  <slot 3> : 3: v3\n" +
                "  |  \n" +
                "  7:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 4: v4\n" +
                "  |  other predicates: 5: v5 IS NULL");
        sqlList.add("select t0.*, t1.v5 from t0 join t1 on t0.v1 = t1.v4 left join t2 on t1.v5 = t2.v7 " +
                "where t2.v8 <=> t0.v2 and t0.v3 in (select max(v10) from t3) and t1.v5 is null");
        planList.add("13:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 2> : 2: v2\n" +
                "  |  <slot 3> : 3: v3\n" +
                "  |  <slot 5> : 5: v5\n" +
                "  |  \n" +
                "  12:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 5: v5 = 7: v7\n" +
                "  |  other predicates: 8: v8 <=> 2: v2\n" +
                "  |  \n" +
                "  |----11:EXCHANGE");
        sqlList.add("select * from (select t0.*, concat(abs(abs(v7)), ifnull(v8, 1), null) from colocate_t0 t0 left join" +
                " t2 on v2 = v7) t left join colocate_t1 on v1 = v4");
        planList.add("4:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 2> : 2: v2\n" +
                "  |  <slot 3> : 3: v3\n" +
                "  |  <slot 7> : concat(CAST(abs(abs(4: v7)) AS VARCHAR), CAST(ifnull(5: v8, 1) AS VARCHAR), NULL)\n" +
                "  |  \n" +
                "  3:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: v2 = 4: v7");
        for (int i = 0; i < sqlList.size(); i++) {
            String sql = sqlList.get(i);
            String expectedPlan = planList.get(i);
            String plan = getFragmentPlan(sql);
            assertContains(plan, expectedPlan);
        }
    }
}
