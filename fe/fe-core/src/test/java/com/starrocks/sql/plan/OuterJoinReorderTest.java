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
import com.starrocks.common.Pair;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

public class OuterJoinReorderTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(30000);
        FeConstants.runningUnitTest = true;
    }


    @ParameterizedTest(name = "sql_{index}: {0}.")
    @MethodSource("joinAssocRuleSqls")
    void joinAssociativityRuleSql(Pair<String, String> pair) throws Exception {
        String plan = getFragmentPlan(pair.first);
        assertContains(plan, pair.second);
    }


    public static Stream<Arguments> joinAssocRuleSqls() {
        List<String> sqlList = Lists.newArrayList();
        List<String> planList = Lists.newArrayList();
        sqlList.add("select t1.* from t0 join t1 on v1 > v4 left join t2 on v1 < v7 ");
        planList.add("5:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  \n" +
                "  4:NESTLOOP JOIN\n" +
                "  |  join op: LEFT OUTER JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  other join predicates: 1: v1 < 7: v7");
        sqlList.add("select * from t0 join t1 join t2 on v1 = v4 + v7");
        planList.add("7:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 12: add = 1: v1");
        sqlList.add("select * from t0 left join (select v4 from t1 union select v7 from t2) t1 on v2 > v4 " +
                "left semi join t2 on v1 = v7");
        planList.add("3:HASH JOIN\n" +
                "  |  join op: LEFT SEMI JOIN (BUCKET_SHUFFLE)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 11: v7");
        sqlList.add("select subq_0.v1 as c0 from (select 88 as v1 from t0 as ref_0) as subq_0 inner join t1 as ref_1 " +
                "on (subq_0.v1 = ref_1.v4) inner join t2 as ref_2 on (subq_0.v1 = ref_2.v7) inner join t3 as ref_3 " +
                "on (ref_2.v8 = ref_3.v10) inner join t4 as ref_4 on (ref_2.v9 = ref_4.v14) " +
                "where (space(cast(ref_4.v15 as INT)) <= ref_4.v14) or ( ref_1.v5 = tan(cast(ref_1.v6 as DOUBLE))) " +
                "limit 111;");
        planList.add("16:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 10: v9 = 15: v14\n" +
                "  |  other join predicates: (CAST(space(CAST(16: v15 AS INT)) AS DOUBLE) <= CAST(15: v14 AS DOUBLE)) " +
                "OR (CAST(6: v5 AS DOUBLE) = tan(CAST(7: v6 AS DOUBLE)))");
        List<Pair<String, String>> zips = zipSqlAndPlan(sqlList, planList);
        return zips.stream().map(e -> Arguments.of(e));
    }
}
