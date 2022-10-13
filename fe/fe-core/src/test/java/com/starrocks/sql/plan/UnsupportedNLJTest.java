// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.plan;

import com.google.common.collect.Lists;
import com.starrocks.common.FeConstants;
import com.starrocks.sql.analyzer.SemanticException;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

/**
 * As to 2022/10, nestloop join doesn't support semi/anti join.
 * Hence, there isn't an executable plan when some subquery transformed to semi/anti join
 * without qualified equal join predicate.
 * These test cases should be modified synchronously when nestloop join support semi/anti join.
 */

class UnsupportedNLJTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;

    }

    @ParameterizedTest(name = "sql_{index}: {0}.")
    @MethodSource("inSubquerySqlList")
    void testConstantInSubquery(String sql) {
        Assert.assertThrows(SemanticException.class, () -> getFragmentPlan(sql));
    }

    private static Stream<Arguments> inSubquerySqlList() {
        List<String> sqlList = Lists.newArrayList();
        sqlList.add("select * from t0,t1 where 1 in (select 2 from t2,t3 where t0.v1 = 1 and t1.v4 = 2)");
        sqlList.add("select * from t0,t1 where 1 in (select v7 from t2,t3 where t0.v1 = 1 and t1.v4 = 2)");
        sqlList.add("select * from t0,t1 where v1 in (select 1+2+3 from t2,t3 where t0.v1 = 1 and t1.v4 = 2)");
        sqlList.add("select * from t0,t1 where abs(1) - 1 in (select 'abc' from t2,t3 where t0.v1 = 1 and t1.v4 = 2)");
        sqlList.add("select * from t0,t1 where 1 not in (select v7 from t2,t3 where t0.v1 = 1 and t1.v4 = 2)");
        sqlList.add("select * from t0,t1 where 1 not in (select v7 from t2,t3 where t0.v1 = 1 and t1.v4 = 2)");
        sqlList.add("select * from t0,t1 where v1 not in (select 1+2+3 from t2,t3 where t0.v1 = 1 and t1.v4 = 2)");
        sqlList.add("select * from t0,t1 where abs(1) - 1 not in (select v7 + 1 from t2,t3 where t0.v1 = 1 and t1.v4 = 2)");
        return sqlList.stream().map(e -> Arguments.of(e));
    }

}
