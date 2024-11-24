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

package com.starrocks.planner;

import com.google.common.collect.Lists;
import com.starrocks.common.FeConstants;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class OnPredicateMoveAroundRuleTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
    }

    @ParameterizedTest(name = "sql_{index}: {0}.")
    @MethodSource("complexPredicateCases")
    void testComplexPredicateCases(String sql, String expect) throws Exception {
        String plan = getFragmentPlan(sql);
        assertContains(plan, expect);
    }

    @ParameterizedTest(name = "sql_{index}: {0}.")
    @MethodSource("complexJoinCases")
    void testComplexJoinCases(String sql, int expect) throws Exception {
        String plan = getFragmentPlan(sql);
        int numOfPredicate = (int) Arrays.stream(plan.split("\n"))
                .filter(ln -> Pattern.compile("PREDICATES").matcher(ln).find()).count();
        Assert.assertEquals(plan, expect, numOfPredicate);
    }

    @ParameterizedTest(name = "sql_{index}: {0}.")
    @MethodSource("redundantPredicateCases")
    void testRedundantPredicate(String sql, String expect) throws Exception {
        String plan = getFragmentPlan(sql);
        assertContains(plan, expect);
    }

    private static Stream<Arguments> complexPredicateCases() {
        List<Arguments> argumentsList = Lists.newArrayList();

        // scene 1
        String templateSql = "select * from \n" +
                "(select * from test_all_type where t1a in ('abc', '中文') and t1b = 1 and t1c = 1 \n" +
                "and t1d = 20 and t1f = 1.1 and id_datetime between '2021-01-01' and '2021-02-01' and id_decimal = 1.2) t1\n" +
                " %s join test_all_type_not_null t2\n" +
                "on t1.t1a > t2.t1a and t1.t1b = t2.t1b and t1.t1c > t2.t1c and t1.id_datetime  < t2.id_datetime;";
        String expectPredicate = "12: t1b = 1, 11: t1a <= '中文', 13: t1c <= 1, 18: id_datetime >= '2021-01-01 00:00:00'";
        argumentsList.add(Arguments.of(String.format(templateSql, "inner"), expectPredicate));
        argumentsList.add(Arguments.of(String.format(templateSql, "left semi"), expectPredicate));
        argumentsList.add(Arguments.of(String.format(templateSql, "right semi"), expectPredicate));


        expectPredicate = "11: t1a <= '中文', 12: t1b = 1, 13: t1c <= 1, 18: id_datetime >= '2021-01-01 00:00:00'";
        argumentsList.add(Arguments.of(String.format(templateSql, "left"), expectPredicate));

        // scene 2
        templateSql = "select * from \n" +
                "(select * from test_all_type where (t1d in (1, 2, 3) and id_date = '2021-01-01') " +
                "or (id_date >'2021-04-01')) t1 %s join test_all_type_not_null t2\n" +
                "on t1.t1d > t2.t1d and t1.id_date = t2.id_date;";
        expectPredicate = "(19: id_date = '2021-01-01') OR (19: id_date > '2021-04-01'), 19: id_date >= '2021-01-01'";
        argumentsList.add(Arguments.of(String.format(templateSql, "inner"), expectPredicate));
        argumentsList.add(Arguments.of(String.format(templateSql, "left"), expectPredicate));

        // scene 3
        templateSql = "select * from \n" +
                "(select * from test_all_type where abs(t1b + t1d) > 20 and t1a in ('abc', '中文')) t1\n" +
                "%s join test_all_type_not_null t2\n" +
                "on abs(t1.t1b + t1.t1d) = t2.t1b;";
        expectPredicate = "12: t1b > 20, CAST(12: t1b AS LARGEINT) IS NOT NULL";
        argumentsList.add(Arguments.of(String.format(templateSql, "inner"), expectPredicate));
        expectPredicate = "PREDICATES: 12: t1b > 20";
        argumentsList.add(Arguments.of(String.format(templateSql, "left"), expectPredicate));

        // scene 4
        templateSql = "select * from test_all_type_not_null t2 %s join " +
                "(select * from test_all_type where t1a in ('abc', '中文') and t1b = 1 and t1c = 1 \n" +
                "and t1d = 20 and t1f = 1.1 and id_datetime between '2021-01-01' and '2021-02-01' and id_decimal = 1.2) t1\n" +
                "on t1.t1a > t2.t1a and t1.t1b = t2.t1b and t1.t1c > t2.t1c and t1.id_datetime  < t2.id_datetime;";
        expectPredicate = "2: t1b = 1, 1: t1a <= '中文', 3: t1c <= 1, 8: id_datetime >= '2021-01-01 00:00:00'";
        argumentsList.add(Arguments.of(String.format(templateSql, "inner"), expectPredicate));

        expectPredicate = "1: t1a <= '中文', 2: t1b = 1, 3: t1c <= 1, 8: id_datetime >= '2021-01-01 00:00:00'";
        argumentsList.add(Arguments.of(String.format(templateSql, "right"), expectPredicate));

        // scene 5
        templateSql = "select * from \n" +
                "(select max(t1d) t1d, t1a from test_all_type group by t1a " +
                "having max(t1d) > 10 and t1a in ('abc', '中文')) t1\n" +
                "%s join test_all_type_not_null t2\n" +
                "on t1.t1d = t2.t1d and t1.t1a = t2.t1a;";
        expectPredicate = "15: t1d > 10, 12: t1a IN ('abc', '中文')";
        argumentsList.add(Arguments.of(String.format(templateSql, "inner"), expectPredicate));
        argumentsList.add(Arguments.of(String.format(templateSql, "left"), expectPredicate));

        // scene 6
        templateSql = "select * from t0 %s join t1 on v1 = v4 where all_match( x-> x > 1, [v1]) or v1 < 0;";
        expectPredicate = "(all_match(array_map(<slot 7> -> <slot 7> > 1, [4: v4]))) OR (4: v4 < 0)";
        argumentsList.add(Arguments.of(String.format(templateSql, "inner"), expectPredicate));
        argumentsList.add(Arguments.of(String.format(templateSql, "left"), expectPredicate));

        // scene 7
        templateSql = "select * from t0 join (select * from t1 where v4 = 1) t1 " +
                "on t0.v1 = t1.v4 where all_match( x-> x > 1, [v1]) or v1 < 0;";
        expectPredicate = "PREDICATES: 1: v1 = 1, (all_match(array_map(<slot 7> -> <slot 7> > 1, [1: v1]))) OR (1: v1 < 0)";
        argumentsList.add(Arguments.of(String.format(templateSql, "inner"), expectPredicate));
        argumentsList.add(Arguments.of(String.format(templateSql, "left"), expectPredicate));

        return argumentsList.stream();
    }

    private static Stream<Arguments> complexJoinCases() {
        List<Arguments> argumentsList = Lists.newArrayList();

        // scene 1
        String templateSql = "select * from (select * from t0 where v1 < 10) t0 " +
                "%s join t1 on v1 = v4 %s join (select * from t2 where v7 > 1) t2\n" +
                "on v4 = v7;";
        argumentsList.add(Arguments.of(String.format(templateSql, "inner", "inner"), 3));
        argumentsList.add(Arguments.of(String.format(templateSql, "left", "inner"), 3));
        argumentsList.add(Arguments.of(String.format(templateSql, "inner", "left"), 3));
        argumentsList.add(Arguments.of(String.format(templateSql, "left", "left"), 3));

        // scene 2
        templateSql = "select * from (select * from t0 where v1 < 10) t0 " +
                "%s join t1 on v1 = v4 join t2 %s join (select * from t3 where v10 > 1) t3\n" +
                "on v7 = v10 and v10 = v4;";
        argumentsList.add(Arguments.of(String.format(templateSql, "inner", "inner"), 4));
        argumentsList.add(Arguments.of(String.format(templateSql, "left", "left"), 3));
        argumentsList.add(Arguments.of(String.format(templateSql, "right", "right"), 4));
        argumentsList.add(Arguments.of(String.format(templateSql, "inner", "left"), 3));
        argumentsList.add(Arguments.of(String.format(templateSql, "left", "inner"), 4));
        argumentsList.add(Arguments.of(String.format(templateSql, "left", "right"), 4));
        argumentsList.add(Arguments.of(String.format(templateSql, "right", "right"), 4));

        // scene 3
        templateSql = "select * from (select * from t0 where v1 < 10) t0 " +
                "%s join t1 on v1 = v4 + abs(1) join t2 %s join (select * from t3 where v10 > 1) t3\n" +
                "on v7 > v10 and v10 > v4;";
        argumentsList.add(Arguments.of(String.format(templateSql, "inner", "inner"), 4));
        argumentsList.add(Arguments.of(String.format(templateSql, "left", "left"), 3));
        argumentsList.add(Arguments.of(String.format(templateSql, "right", "right"), 3));
        argumentsList.add(Arguments.of(String.format(templateSql, "inner", "left"), 3));
        argumentsList.add(Arguments.of(String.format(templateSql, "left", "inner"), 4));
        argumentsList.add(Arguments.of(String.format(templateSql, "left", "right"), 4));
        argumentsList.add(Arguments.of(String.format(templateSql, "right", "right"), 3));

        return argumentsList.stream();
    }

    private static Stream<Arguments> redundantPredicateCases() {
        List<Arguments> argumentsList = Lists.newArrayList();

        // scene 1
        String templateSql = "select * from \n" +
                "(select * from t0 where v1 = 1) t0\n" +
                "%s join\n" +
                "(select * from t1 where v4 = 1) t1\n" +
                "on t0.v1 = t1.v4;";
        String expectPredicate = "PREDICATES: 1: v1 IS NOT NULL, 1: v1 = 1";
        argumentsList.add(Arguments.of(String.format(templateSql, "inner"), expectPredicate));

        expectPredicate = "PREDICATES: 1: v1 = 1";
        argumentsList.add(Arguments.of(String.format(templateSql, "left"), expectPredicate));

        // scene 2
        templateSql = "select * from \n" +
                "(select * from t0 where v1 = 1) t0\n" +
                "%s join\n" +
                "(select * from t1 where v4 = 1) t1\n" +
                "on t0.v1 > t1.v4;";
        argumentsList.add(Arguments.of(String.format(templateSql, "inner"), expectPredicate));
        argumentsList.add(Arguments.of(String.format(templateSql, "left"), expectPredicate));

        // scene 3
        templateSql = "select * from \n" +
                "(select * from t0 where v1 > 4 and v2 = 3) t0\n" +
                "%s join\n" +
                "(select * from t1 where v4 = 1) t1\n" +
                "on t0.v1 > t1.v4;";
        expectPredicate = "PREDICATES: 1: v1 > 4, 2: v2 = 3";
        argumentsList.add(Arguments.of(String.format(templateSql, "inner"), expectPredicate));
        argumentsList.add(Arguments.of(String.format(templateSql, "left"), expectPredicate));

        // scene 4
        templateSql = "select * from \n" +
                "(select * from t0 where v1 > 1 and v1 < 4) t0\n" +
                "%s join\n" +
                "(select * from t1 where v4 = 2) t1\n" +
                "on t0.v1 > t1.v4;";
        expectPredicate = "PREDICATES: 1: v1 >= 2, 1: v1 < 4";
        argumentsList.add(Arguments.of(String.format(templateSql, "inner"), expectPredicate));
        argumentsList.add(Arguments.of(String.format(templateSql, "right"), expectPredicate));

        return argumentsList.stream();
    }

}
