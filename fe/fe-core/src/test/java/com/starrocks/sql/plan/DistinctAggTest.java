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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

public class DistinctAggTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
    }

    @ParameterizedTest(name = "sql_{index}: {0}.")
    @MethodSource("sqlWithDistinctLimit")
    public void testSqlWithDistinctLimit(String sql, String expectedPlan) throws Exception {
        String plan = getFragmentPlan(sql);
        assertContains(plan, expectedPlan);
    }

    @Test
    public void testDistinctConstants() throws Exception {
        String sql = "select count(distinct 1, 2, 3, 4), sum(distinct 1), avg(distinct 1), " +
                "group_concat(distinct 1, 2 order by 1), array_agg(distinct 1 order by 1) from t0 group by v2;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "4:AGGREGATE (update finalize)\n" +
                "  |  output: count(if(1 IS NULL, NULL, if(2 IS NULL, NULL, if(3 IS NULL, NULL, 4)))), sum(1), avg(1), " +
                "group_concat('1', '2', ','), array_agg(1)");
        sql = "select count(distinct 1, 2, 3, 4) from t0 group by v2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "4:AGGREGATE (update finalize)\n" +
                "  |  output: count(if(1 IS NULL, NULL, if(2 IS NULL, NULL, if(3 IS NULL, NULL, 4))))");

        sql = "select count(distinct v3, 1) from t0 group by v2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "4:AGGREGATE (update finalize)\n" +
                "  |  output: count(if(3: v3 IS NULL, NULL, 1))");
    }

    private static Stream<Arguments> sqlWithDistinctLimit() {
        List<Arguments> argumentsList = Lists.newArrayList();
        argumentsList.add(Arguments.of("select count(distinct v1, v2) from (select * from t0 limit 2) t",
                "4:AGGREGATE (merge finalize)\n" +
                        "  |  output: count(4: count)\n" +
                        "  |  group by:"));
        argumentsList.add(Arguments.of("select group_concat(distinct v1, v2) from (select * from t0 limit 2) t",
                "4:AGGREGATE (merge finalize)\n" +
                        "  |  output: group_concat(4: group_concat, ',')\n" +
                        "  |  group by: "));
        argumentsList.add(Arguments.of("select count(distinct v1, v2) from (select * from t0 limit 2) t group by 1 + 1",
                "5:AGGREGATE (merge finalize)\n" +
                        "  |  output: count(5: count)\n" +
                        "  |  group by: 4: expr"));
        argumentsList.add(Arguments.of("select group_concat(distinct v1, v2) from (select * from t0 limit 2) t group by v3",
                "3:AGGREGATE (update finalize)\n" +
                        "  |  output: group_concat(CAST(1: v1 AS VARCHAR), CAST(2: v2 AS VARCHAR), ',')\n" +
                        "  |  group by: 3: v3"));

        argumentsList.add(Arguments.of("select count(distinct v1, v2), sum(v1) + max(v2) " +
                        "from (select * from t0 limit 2) t",
                "4:AGGREGATE (merge finalize)\n" +
                        "  |  output: count(4: count), sum(5: sum), max(6: max)\n" +
                        "  |  group by: "));
        argumentsList.add(Arguments.of("select group_concat(distinct v1, v2), sum(v2) + max(v1)" +
                        " from (select * from t0 limit 2) t",
                "4:AGGREGATE (merge finalize)\n" +
                        "  |  output: group_concat(4: group_concat, ','), sum(5: sum), max(6: max)\n" +
                        "  |  group by: "));
        argumentsList.add(Arguments.of("select count(distinct v1, v2), sum(v1) - min(v2) " +
                        "from (select * from t0 limit 2) t group by 1 + 1",
                "4:AGGREGATE (update finalize)\n" +
                        "  |  output: count(if(1: v1 IS NULL, NULL, 2: v2)), sum(6: sum), min(7: min)\n" +
                        "  |  group by: 4: expr"));
        argumentsList.add(Arguments.of("select group_concat(distinct v1, v2), sum(v2) - min(v1) " +
                        "from (select * from t0 limit 2) t group by v3",
                "3:AGGREGATE (update finalize)\n" +
                        "  |  output: group_concat(CAST(1: v1 AS VARCHAR), CAST(2: v2 AS VARCHAR), ',')"));



        argumentsList.add(Arguments.of("select array_agg(distinct v1 order by 1, v3), sum(v2) from t0 " +
                        "group by rollup(v3, abs(v1 + v2))",
                "6:AGGREGATE (update finalize)\n" +
                        "  |  output: array_agg(1: v1, 1: v1, 5: expr), sum(7: sum)\n" +
                        "  |  group by: 3: v3, 4: abs, 8: GROUPING_ID"));

        argumentsList.add(Arguments.of("select array_agg_distinct(v1 order by 1, v3), sum(v2) from t0 " +
                        "group by rollup(v3, abs(v1 + v2))",
                "6:AGGREGATE (update finalize)\n" +
                        "  |  output: array_agg(1: v1, 1: v1, 5: expr), sum(7: sum)\n" +
                        "  |  group by: 3: v3, 4: abs, 8: GROUPING_ID"));

        argumentsList.add(Arguments.of("select /*+set_var(new_planner_agg_stage = 2) */" +
                        " array_agg(distinct v1 order by 1, v3), sum(v2) from t0 " +
                        "group by rollup(v3, abs(v1 + v2))",
                "6:AGGREGATE (update finalize)\n" +
                        "  |  output: array_agg(1: v1, 1: v1, 5: expr), sum(7: sum)\n" +
                        "  |  group by: 3: v3, 4: abs, 8: GROUPING_ID"));

        argumentsList.add(Arguments.of("select /*+set_var(new_planner_agg_stage = 2) */" +
                        " array_agg_distinct(v1 order by 1, v3), sum(v2) from t0 " +
                        "group by rollup(v3, abs(v1 + v2))",
                "6:AGGREGATE (update finalize)\n" +
                        "  |  output: array_agg(1: v1, 1: v1, 5: expr), sum(7: sum)\n" +
                        "  |  group by: 3: v3, 4: abs, 8: GROUPING_ID"));

        argumentsList.add(Arguments.of("select group_concat(distinct v1 order by 1, v3), sum(v2) from t0 " +
                        "group by rollup(v3, abs(v1 + v2))",
                "6:AGGREGATE (update finalize)\n" +
                        "  |  output: group_concat(CAST(1: v1 AS VARCHAR), ',', 1: v1, 5: expr), sum(7: sum)\n" +
                        "  |  group by: 3: v3, 4: abs, 8: GROUPING_ID"));

        argumentsList.add(Arguments.of("select /*+set_var(new_planner_agg_stage = 4) */" +
                        "if(length(group_concat(distinct v1, v2 order by 2, v3)) > 10, " +
                        "max(v1), min(v1)), sum(v2) from t0 group by rollup(v3, abs(v1 + v2))",
                "8:AGGREGATE (merge finalize)\n" +
                        "  |  output: group_concat(6: group_concat, ','), max(7: max), min(8: min), sum(9: sum)\n" +
                        "  |  group by: 3: v3, 4: abs, 10: GROUPING_ID"));

        argumentsList.add(Arguments.of("select group_concat(distinct v2), array_agg(distinct v2), " +
                        "count(distinct v2), sum(v3 + v1) from t0 group by rollup(v3, v1);",
                "6:AGGREGATE (update finalize)\n" +
                        "  |  output: group_concat(CAST(2: v2 AS VARCHAR), ','), array_agg(2: v2), count(2: v2), sum(8: sum)"));
        argumentsList.add(Arguments.of("select group_concat(distinct 1), array_agg(distinct 2), sum(v3) from t0 " +
                        "group by v2, v3",
                "3:AGGREGATE (merge finalize)\n" +
                        "  |  output: group_concat(4: group_concat, '1', ','), array_agg_distinct(5: array_agg), sum(6: sum)"));

        return argumentsList.stream();
    }
}
