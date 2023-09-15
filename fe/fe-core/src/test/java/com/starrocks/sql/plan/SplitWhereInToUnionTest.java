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

import com.google.api.client.util.Lists;
import com.google.common.collect.ImmutableList;
import com.starrocks.common.FeConstants;
import com.starrocks.statistic.MockTPCHHistogramStatisticStorage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Stream;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SplitWhereInToUnionTest extends DistributedEnvPlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        DistributedEnvPlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
        FeConstants.USE_MOCK_DICT_MANAGER = true;
        connectContext.getSessionVariable().setWhereInToUnionThreshold(3);
        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(true);

        int scale = 100;
        connectContext.getGlobalStateMgr().setStatisticStorage(new MockTPCHHistogramStatisticStorage(scale));
    }

    @AfterAll
    public static void afterClass() {
        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(false);
        connectContext.getSessionVariable().setWhereInToUnionThreshold(3);
    }

    private static String generateMultipleValues(int num) {
        StringJoiner joiner = new StringJoiner(", ", "(", ")");
        for (int i = 0; i < num; i++) {
            joiner.add(String.valueOf(i));
        }
        return joiner.toString();
    }

    private static Stream<Arguments> testSplitWhereInSqls() {
        List<Arguments> list = Lists.newArrayList();

        String sql = "select * from test.jdbc_test where a in (1, 2, 3, 4)";
        Arguments arguments = Arguments.of(sql, ImmutableList.of("UNION",
                "QUERY: SELECT a, b, c FROM `test_table` WHERE (a IN (1, 2, 3))",
                "QUERY: SELECT a, b, c FROM `test_table` WHERE (a IN (4))"));
        list.add(arguments);

        sql = "select * from test.jdbc_test where a in (1, 2, 2, 3, 3, 4)";
        arguments = Arguments.of(sql, ImmutableList.of("UNION",
            "QUERY: SELECT a, b, c FROM `test_table` WHERE (a IN (1, 2, 3))",
            "QUERY: SELECT a, b, c FROM `test_table` WHERE (a IN (4))"));
        list.add(arguments);

        sql = "select * from test.jdbc_test where a in (4, 2, 2, 3, 3, 1)";
        arguments = Arguments.of(sql, ImmutableList.of("UNION",
            "QUERY: SELECT a, b, c FROM `test_table` WHERE (a IN (4, 2, 3))",
            "QUERY: SELECT a, b, c FROM `test_table` WHERE (a IN (1))"));
        list.add(arguments);

        sql = "select * from jdbc_test where (a in (1, 2, 3, 4) and b <=> NULL and c > 1.0) or (b = 'hello' and c < 2.0)";
        arguments = Arguments.of(sql, ImmutableList.of("UNION",
                "SELECT a, b, c FROM `test_table` WHERE (b = 'hello') AND (c < 2.0)",
                "SELECT a, b, c FROM `test_table` WHERE (b <=> NULL) AND (c > 1.0) " +
                        "AND ((NOT ((b = 'hello') AND (c < 2.0))) OR ((b = 'hello') AND (c < 2.0) IS NULL)) " +
                        "AND (a IN (1, 2, 3))",
                "SELECT a, b, c FROM `test_table` WHERE (b <=> NULL) AND (c > 1.0) " +
                        "AND ((NOT ((b = 'hello') AND (c < 2.0))) OR ((b = 'hello') AND (c < 2.0) IS NULL)) " +
                        "AND (a IN (4))"));
        list.add(arguments);

        sql = "select * from jdbc_test where (a in (1, 2, 3, 4)) or (b = 'hello' and c < 2.0) or (b <=> NULL and c > 1.0)";
        arguments = Arguments.of(sql, ImmutableList.of("UNION",
                "SELECT a, b, c FROM `test_table` WHERE (((b = 'hello') AND (c < 2.0)) OR ((b <=> NULL) AND (c > 1.0)))",
                "SELECT a, b, c FROM `test_table` WHERE ((NOT (((b = 'hello') AND (c < 2.0)) OR ((b <=> NULL) AND (c > 1.0)))) " +
                        "OR (((b = 'hello') AND (c < 2.0)) OR ((b <=> NULL) AND (c > 1.0)) IS NULL)) " +
                        "AND (a IN (1, 2, 3))",
                "SELECT a, b, c FROM `test_table` WHERE ((NOT (((b = 'hello') AND (c < 2.0)) OR ((b <=> NULL) AND (c > 1.0)))) " +
                        "OR (((b = 'hello') AND (c < 2.0)) OR ((b <=> NULL) AND (c > 1.0)) IS NULL)) " +
                        "AND (a IN (4))"));
        list.add(arguments);


        sql = "select * from jdbc_test where a in " + generateMultipleValues(11);
        arguments = Arguments.of(sql, ImmutableList.of("UNION",
                        "SELECT a, b, c FROM `test_table` WHERE (a IN (0, 1, 2))"),
                "SELECT a, b, c FROM `test_table` WHERE (a IN (3, 4, 5))",
                "SELECT a, b, c FROM `test_table` WHERE (a IN (6, 7, 8))",
                "SELECT a, b, c FROM `test_table` WHERE (a IN (9, 10))");
        list.add(arguments);

        sql = "select * from jdbc_test where a in " + generateMultipleValues(11) + " and b != 'a'";
        arguments = Arguments.of(sql, ImmutableList.of("UNION",
                "SELECT a, b, c FROM `test_table` WHERE (b != 'a') AND (a IN (0, 1, 2))",
                "SELECT a, b, c FROM `test_table` WHERE (b != 'a') AND (a IN (3, 4, 5))",
                "SELECT a, b, c FROM `test_table` WHERE (b != 'a') AND (a IN (6, 7, 8))",
                "SELECT a, b, c FROM `test_table` WHERE (b != 'a') AND (a IN (9, 10))"));
        list.add(arguments);

        sql = "select * from jdbc_test where b != 'a' and a in " + generateMultipleValues(11);
        arguments = Arguments.of(sql, ImmutableList.of("UNION",
                "SELECT a, b, c FROM `test_table` WHERE (b != 'a') AND (a IN (0, 1, 2))",
                "SELECT a, b, c FROM `test_table` WHERE (b != 'a') AND (a IN (3, 4, 5))",
                "SELECT a, b, c FROM `test_table` WHERE (b != 'a') AND (a IN (6, 7, 8))",
                "SELECT a, b, c FROM `test_table` WHERE (b != 'a') AND (a IN (9, 10))"));
        list.add(arguments);

        sql = "select * from jdbc_test where a in " + generateMultipleValues(4) + " and b in ('a', 'a', 'b', 'c', 'd', 'd')";
        arguments = Arguments.of(sql, ImmutableList.of("UNION",
                "SELECT a, b, c FROM `test_table` WHERE (a IN (0, 1, 2)) AND (b IN ('a', 'b', 'c'))",
                "SELECT a, b, c FROM `test_table` WHERE (a IN (0, 1, 2)) AND (b IN ('d'))",
                "SELECT a, b, c FROM `test_table` WHERE (a IN (3)) AND (b IN ('a', 'b', 'c'))",
                "SELECT a, b, c FROM `test_table` WHERE (a IN (3)) AND (b IN ('d'))"));
        list.add(arguments);

        sql = "select * from jdbc_test where a in " + generateMultipleValues(4) + " and b in ('a', 'b', 'c', 'd') or c > 1.0";
        arguments = Arguments.of(sql, ImmutableList.of("UNION",
                "SELECT a, b, c FROM `test_table` WHERE (c > 1.0)",
                "SELECT a, b, c FROM `test_table` " +
                    "WHERE ((c <= 1.0) OR (c IS NULL)) AND (a IN (0, 1, 2)) AND (b IN ('a', 'b', 'c'))",
                "SELECT a, b, c FROM `test_table` WHERE ((c <= 1.0) OR (c IS NULL)) AND (a IN (0, 1, 2)) AND (b IN ('d'))",
                "SELECT a, b, c FROM `test_table` WHERE ((c <= 1.0) OR (c IS NULL)) AND (a IN (3)) AND (b IN ('a', 'b', 'c'))",
                "SELECT a, b, c FROM `test_table` WHERE ((c <= 1.0) OR (c IS NULL)) AND (a IN (3)) AND (b IN ('d'))"));
        list.add(arguments);

        return list.stream();
    }

    private static Stream<Arguments> testNotSplitWhereInSqls() {
        List<Arguments> list = Lists.newArrayList();

        String sql = "select * from jdbc_test where a not in " + generateMultipleValues(11);
        Arguments arguments = Arguments.of(sql);
        list.add(arguments);

        sql = "select * from jdbc_test where a in (1, 2, 3)";
        arguments = Arguments.of(sql);
        list.add(arguments);

        sql = "select * from jdbc_test where (a in (1, 2, 3, 4) or b <=> NULL or c > 1.0) and (b = 'hello' or c < 2.0)";
        arguments = Arguments.of(sql);
        list.add(arguments);

        sql = "select * from jdbc_test where a in " + generateMultipleValues(4) + " or b in ('a', 'b', 'c', 'd') or c > 1.0";
        arguments = Arguments.of(sql);
        list.add(arguments);

        return list.stream();
    }

    @ParameterizedTest
    @MethodSource("testSplitWhereInSqls")
    @Order(1)
    void testSplitWhereIn(String sql, List<String> patterns) throws Exception {
        String plan = getFragmentPlan(sql);
        assertContains(plan, patterns);
    }

    @ParameterizedTest
    @MethodSource("testNotSplitWhereInSqls")
    @Order(2)
    void testNotSplitWhereIn(String sql) throws Exception {
        String plan = getFragmentPlan(sql);
        assertNotContains(plan, "UNION");
    }

}
