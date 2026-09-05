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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

class PushDownPredicateJoinTypeTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
    }


    @ParameterizedTest
    @MethodSource("innerJoinStream")
    void testInnerJoin(String sql) throws Exception {
        String plan = getFragmentPlan(sql);
        assertContains(plan, "INNER JOIN");
        assertNotContains(plan, "CROSS JOIN");
    }

    @ParameterizedTest
    @MethodSource("crossJoinStream")
    void testCrossJoin(String sql) throws Exception {
        String plan = getFragmentPlan(sql);
        assertNotContains(plan, "INNER JOIN");
        assertContains(plan, "CROSS JOIN");
    }

    @ParameterizedTest
    @MethodSource("semiJoinStream")
    void testSemiJoin(String sql) throws Exception {
        String plan = getFragmentPlan(sql);
        assertContains(plan, "SEMI JOIN");
    }

    @ParameterizedTest
    @MethodSource("antiJoinStream")
    void testAntiJoin(String sql) throws Exception {
        String plan = getFragmentPlan(sql);
        assertContains(plan, "ANTI JOIN");
    }

    @ParameterizedTest
    @MethodSource("outerJoinStream")
    void testOuterJoin(String sql) throws Exception {
        String plan = getFragmentPlan(sql);
        assertContains(plan, "OUTER JOIN");
    }

    @ParameterizedTest
    @MethodSource("asofInnerJoinStream")
    void testAsofInnerJoin(String sql) throws Exception {
        String plan = getFragmentPlan(sql);
        assertContains(plan, "ASOF INNER JOIN");
    }

    @ParameterizedTest
    @MethodSource("asofLeftJoinStream")
    void testAsofLeftJoin(String sql) throws Exception {
        String plan = getFragmentPlan(sql);
        assertContains(plan, "ASOF LEFT OUTER JOIN");
    }

    private static Stream<Arguments> innerJoinStream() {
        List<String> sqls = Lists.newArrayList();
        sqls.add("select * from t0, t1 where v1 = v4 and v1 = 1 and v5 = 2");
        sqls.add("select * from t0, t1 where v1 > 4 or v5 < 2");
        sqls.add("select * from t0, t1 where v1 + v4 = v2 + v5");
        sqls.add("select * from t0, t1 where v1 < abs(v4) and v2 > v3");
        sqls.add("select * from t0, t1, t2 where v1 = v4 and v2 = v7");
        sqls.add("select * from t0 join t1 on v1 = 4 or v5 < 1");
        sqls.add("select * from t0 join t1 join t2 on v1 = v4 and v2 = v7");
        return sqls.stream().map(e -> Arguments.of(e));
    }

    private static Stream<Arguments> asofInnerJoinStream() {
        List<String> sqls = Lists.newArrayList();
        sqls.add("select * from t0 asof join t1 on t0.v1 = t1.v4 and v1 = 1 and v5 = 2 and t0.v2 <= t1.v5");
        sqls.add("select * from t0 asof inner join t1 on t0.v1 = t1.v4 and t0.v2 >= t1.v5 where v1 > 4 or v5 < 2");
        sqls.add("select * from t0 asof join t1 on t0.v1 = t1.v4 and t0.v2 < t1.v5 where t0.v1 > 1");
        sqls.add("select * from t0 asof join t1 on t0.v1 = t1.v4 and t0.v2 > t1.v5 and t1.v6 = 2");
        sqls.add("select * from t0 asof join t1 on t0.v1 = t1.v4 and t0.v2 <= t1.v5 join t2 on t1.v4 = t2.v7");
        return sqls.stream().map(e -> Arguments.of(e));
    }

    private static Stream<Arguments> crossJoinStream() {
        List<String> sqls = Lists.newArrayList();
        sqls.add("select * from t0, t1 where v1 = 1 and v4 = 4");
        sqls.add("select * from t0, t1, t2 where v1 < 1 and v4 != 4 and v7 > 1");
        sqls.add("select * from t0 join t1 on v1 = 1 and v4 = 4");
        sqls.add("select * from t0 join t1 on v1 + 1 = v2 + 1 and abs(v4) = v5");
        sqls.add("select * from t0 join t1 join t2 on v1 = 1 and v7 = 1");
        return sqls.stream().map(e -> Arguments.of(e));
    }

    private static Stream<Arguments> semiJoinStream() {
        List<String> sqls = Lists.newArrayList();
        sqls.add("select * from t0 where v1 in (select v4 from t1 where v1 = 1)");
        sqls.add("select * from t0 where exists (select v4 from t1 where v1 = 1)");
        return sqls.stream().map(e -> Arguments.of(e));
    }

    private static Stream<Arguments> antiJoinStream() {
        List<String> sqls = Lists.newArrayList();
        sqls.add("select * from t0 where v1 not in (select v4 from t1 where v1 = 1)");
        sqls.add("select * from t0 where '1' not in (select v4 from t1)");
        sqls.add("select * from t0 where 1 not in (select v4 from t1)");
        sqls.add("select * from t0 where null not in (select v4 from t1)");
        sqls.add("select * from t0 where not exists (select v4 from t1 where v1 = 1)");
        return sqls.stream().map(e -> Arguments.of(e));
    }

    private static Stream<Arguments> outerJoinStream() {
        List<String> sqls = Lists.newArrayList();
        sqls.add("select * from t0 left join t1 on v1 = 1");
        sqls.add("select * from t0 left join t1 on v1 < 1 and v4 > 1");
        sqls.add("select * from t0 left join t1 on v1 < 1 and v4 > 1 and v2 > v5");
        sqls.add("select * from t0 left join t1 on v4 = 4");
        sqls.add("select * from t0 full outer join t1 on v1 = 1 and v4 = 4");
        sqls.add("select * from t0 full outer join t1 on v1 < 1 and v4 > 4 and v2 > v5");
        sqls.add("select * from t0 left join t1 on t0.v1 = t1.v4 and t0.v1 > 1");
        return sqls.stream().map(e -> Arguments.of(e));
    }

    private static Stream<Arguments> asofLeftJoinStream() {
        List<String> sqls = Lists.newArrayList();
        sqls.add("select * from t0 asof left join t1 on t0.v1 = t1.v4 and t0.v2 <= t1.v5");
        sqls.add("select * from t0 asof left outer join t1 on t0.v1 = t1.v4 and t0.v2 >= t1.v5");
        sqls.add("select * from t0 asof left join t1 on t0.v1 = t1.v4 and t0.v2 < t1.v5 where t0.v1 > 1");
        sqls.add("select * from t0 asof left join t1 on t0.v1 = t1.v4 and t0.v2 > t1.v5 and t1.v6 = 2");
        sqls.add("select * from t0 asof left join t1 on t0.v1 = t1.v4 and t0.v2 <= t1.v5 left join t2 on t1.v4 = t2.v7");
        return sqls.stream().map(e -> Arguments.of(e));
    }


}
