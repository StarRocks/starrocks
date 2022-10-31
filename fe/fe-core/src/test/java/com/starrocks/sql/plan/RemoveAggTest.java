// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.plan;

import com.google.common.collect.Lists;
import com.starrocks.common.FeConstants;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

public class RemoveAggTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
    }

    @ParameterizedTest(name = "sql_{index}: {0}.")
    @MethodSource("removeAggSqlCases")
    void removeAggTest(String sql) throws Exception {
        String plan = getFragmentPlan(sql);
        assertNotContains(plan, "AGGREGATE");
        assertContains(plan, "PREDICATES");
        assertContains(plan, "PREAGGREGATION: OFF");
    }


    @ParameterizedTest(name = "sql_{index}: {0}.")
    @MethodSource("remainAggSqlCases")
    void remainAggTest(String sql) throws Exception {
        String plan = getFragmentPlan(sql);
        assertContains(plan, "AGGREGATE");
    }


    private static Stream<Arguments> removeAggSqlCases() {
        List<String> sqlList = Lists.newArrayList();
        sqlList.add("select max(v1) from test_agg group by k1, k2, k3 having (max(v1) = 1)");
        sqlList.add("select max(v1), min(v2) from test_agg group by k1, k2, k3 " +
                "having (max(v1) > min(v2) or abs(max(v1)) is null)");
        sqlList.add("select max(v1), min(v2), abs(123) from test_agg group by k1, k2, k3" +
                " having (max(v1) + min(v2) > sum(v3))");
        sqlList.add("select max(v1) + min(v2) from test_agg group by k1, k2, k3 having (max(v1) + min(v2)) is not null");
        sqlList.add("select max(v1) + min(v2) from test_agg group by k1, k2, k3 " +
                "having ((max(v1) + min(v2)) is not null or k1 > 4)");
        sqlList.add("select k1, k2 from test_agg group by k1, k2, k3 having k1 > 1");
        sqlList.add("select k1, k2 from test_agg group by k1, k2, k3 having k1 > 1 and k2 < 1 " +
                "and k1 + k2 >3 and max(v1) = 1");
        return sqlList.stream().map(e -> Arguments.of(e));
    }

    private static Stream<Arguments> remainAggSqlCases() {
        List<String> sqlList = Lists.newArrayList();
        sqlList.add("select max(v1) from t0 group by v2");
        sqlList.add("select min(v1) from test_agg group by k1, k2, k3");
        sqlList.add("select max(v1) from test_agg group by k1");
        sqlList.add("select max(v1) from test_agg group by abs(k1), k1, k2, k3");
        sqlList.add("select hll_union_agg(h1) from test_agg group by k1, k2, k3");
        sqlList.add("select max(v1) from t0 group by v2 having max(v1) > 2");
        sqlList.add("select max(v1) from test_agg group by k1, k2, k3 having (min(v1) = 1)");
        sqlList.add("select k1, k2 from test_agg group by k1, k2, k3 having max(v1) > 1 or min(v1) < 1");
        return sqlList.stream().map(e -> Arguments.of(e));
    }

    @AfterAll
    public static void afterClass() {
        FeConstants.runningUnitTest = false;
    }
}
