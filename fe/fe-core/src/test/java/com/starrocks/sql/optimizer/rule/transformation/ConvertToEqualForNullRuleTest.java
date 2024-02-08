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

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.common.FeConstants;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

class ConvertToEqualForNullRuleTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
    }

    @ParameterizedTest(name = "sql_{index}: {0}.")
    @MethodSource("getSqlList")
    void testToEqualForNull(String sql, String expectedPlan) throws Exception {
        String plan = getFragmentPlan(sql);
        assertContains(plan, expectedPlan);
    }


    private static Stream<Arguments> getSqlList() {
        List<Arguments> sqlList = Lists.newArrayList();
        sqlList.add(Arguments.of("select * from t0 join t1 on v1 = v4 or v1 is null and v4 is null",
                "equal join conjunct: 1: v1 <=> 4: v4"));
        sqlList.add(Arguments.of("select * from t0 join t1 on v1 = v4 or v1 is not null and v4 is null",
                "other join predicates: (1: v1 = 4: v4) OR ((1: v1 IS NOT NULL) AND (4: v4 IS NULL))"));
        sqlList.add(Arguments.of("select * from t0 join t1 on v2 = v4 or v1 is not null and v4 is null",
                "other join predicates: (2: v2 = 4: v4) OR ((1: v1 IS NOT NULL) AND (4: v4 IS NULL))"));
        sqlList.add(Arguments.of("select * from t0 join t1 on v4 = v1 or v1 is null and v4 is null",
                "equal join conjunct: 1: v1 <=> 4: v4"));
        sqlList.add(Arguments.of("select * from t0 join t1 on v4 = v1 or v4 is null and v1 is null",
                "equal join conjunct: 1: v1 <=> 4: v4"));
        sqlList.add(Arguments.of("select * from t0 join t1 on v4 is null and v1 is null or v1 = v4 ",
                "equal join conjunct: 1: v1 <=> 4: v4"));
        sqlList.add(Arguments.of("select * from t0 join t1 on (v4 is null and v1 is null or v1 = v4) and v1 > v5 ",
                "equal join conjunct: 1: v1 <=> 4: v4"));
        sqlList.add(Arguments.of("select * from t0 join t1 on (abs(v4) is null and abs(v1) is null or abs(v4) = abs(v1))" +
                        " and v1 > v5 ", "equal join conjunct: 8: abs <=> 7: abs"));
        sqlList.add(Arguments.of("select * from t0 join t1 on (abs(v4) is null and abs(v1) is null or abs(v4) = abs(v1)) " +
                        "and v1 > v5 ", "equal join conjunct: 8: abs <=> 7: abs"));
        sqlList.add(Arguments.of("select * from t0 join t1 on (abs(v4) is null and abs(v1) is null or abs(v4) = abs(v1)) " +
                "and v1 > v5 join t2 on abs(v4) = v7 or v7 is null and abs(v4) is null",
                "equal join conjunct: 10: abs <=> 11: abs"));
        return sqlList.stream();
    }


}