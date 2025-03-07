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
import com.starrocks.sql.optimizer.rewrite.scalar.FilterSelectivityEvaluator;
import com.starrocks.statistic.MockHistogramStatisticStorage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Stream;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SplitScanToUnionTest extends DistributedEnvPlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        DistributedEnvPlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
        FeConstants.USE_MOCK_DICT_MANAGER = true;
        connectContext.getSessionVariable().setScanOrToUnionThreshold(100000);
        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(true);

        int scale = 100;
        connectContext.getGlobalStateMgr().setStatisticStorage(new MockHistogramStatisticStorage(scale));
    }

    @ParameterizedTest
    @MethodSource("testSplitUnionSqls")
    @Order(1)
    void testSplitUnion(String sql, List<String> patterns) throws Exception {
        String plan = getFragmentPlan(sql);
        assertContains(plan, patterns);
    }

    @ParameterizedTest
    @MethodSource("testNotSplitUnionSqls")
    @Order(2)
    void testNotSplitUnion(String sql) throws Exception {
        String plan = getFragmentPlan(sql);
        assertNotContains(plan, "UNION");
    }

    @Test
    @Order(3)
    void testForceSplit() throws Exception {
        String sql = "select * from t0 where v1 = 1 or v2 = 2";
        connectContext.getSessionVariable().setSelectRatioThreshold(-1);
        assertContains(getFragmentPlan(sql), "UNION");

        sql = "select * from t0 where v1 > 1 and v1 < 3 or v1 >100000 and v1 < 100010 or v1 >200000 and v1 < 200010 " +
                "or v1 > 400000 and v1 < 500010";
        assertContains(getFragmentPlan(sql), "UNION");

        sql = "select * from test_all_type where date_trunc('year', id_date) > '2021-01-01' " +
                "and date_trunc('year', id_date) < '2022-01-01' or date_trunc('month', id_date) > '2020-06-01' and " +
                "date_trunc('month', id_date) < '2021-01-01'";
        assertContains(getFragmentPlan(sql), "UNION");

        sql = "select * from t0 where v1 > v2 or v1 = 1";
        assertContains(getFragmentPlan(sql), "UNION");
    }

    @Test
    void testForceSplitWithPartition() throws Exception {
        connectContext.getSessionVariable().setSelectRatioThreshold(-1);
        connectContext.getSessionVariable().setEnableSyncMaterializedViewRewrite(false);
        String sql = "select * from pushdown_test where k1 >= 0 and (k3 > k4 or k3 = 1)";
        assertContains(getFragmentPlan(sql), "UNION");
    }

    @Test
    void testForceUnion() throws Exception {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10; i++) {
            sb.append(i).append(", ");
        }
        String sql = "select * from t0 where v1 in (" + sb + "1);";
        connectContext.getSessionVariable().setSelectRatioThreshold(-1);
        FilterSelectivityEvaluator.IN_CHILDREN_THRESHOLD = 5;
        String plan = getVerboseExplain(sql);
        FilterSelectivityEvaluator.IN_CHILDREN_THRESHOLD = 1024;
        assertContains(plan, "  0:UNION\n" +
                "  |  output exprs:\n" +
                "  |      [1, BIGINT, true] | [2, BIGINT, true] | [3, BIGINT, true]\n" +
                "  |  child exprs:\n" +
                "  |      [4: v1, BIGINT, true] | [5: v2, BIGINT, true] | [6: v3, BIGINT, true]\n" +
                "  |      [7: v1, BIGINT, true] | [8: v2, BIGINT, true] | [9: v3, BIGINT, true]");
        assertContains(plan, "Predicates: 4: v1 IN (0, 1, 2, 3, 4)");
        assertContains(plan, "Predicates: 7: v1 IN (5, 6, 7, 8, 9)");
    }

    private static Stream<Arguments> testSplitUnionSqls() {
        List<Arguments> list = Lists.newArrayList();
        String sql = "select * from orders where (O_TOTALPRICE != 1 or O_ORDERPRIORITY > 'a') " +
                "and (O_CUSTKEY = 1 or O_ORDERKEY = 2)";
        Arguments arguments = Arguments.of(sql, ImmutableList.of("UNION",
                "PREDICATES: 11: O_ORDERKEY = 2, (14: O_TOTALPRICE != 1.0) OR (16: O_ORDERPRIORITY > 'a')",
                "PREDICATES: 22: O_CUSTKEY = 1, 21: O_ORDERKEY != 2, (24: O_TOTALPRICE != 1.0) OR (26: O_ORDERPRIORITY > 'a')"));
        list.add(arguments);

        sql = "select * from orders where (O_CUSTKEY = abs(1) or O_ORDERKEY <=> null or " +
                "O_ORDERDATE = str_to_date('2014-12-21', '%Y-%m')) and O_CLERK > O_ORDERPRIORITY";
        arguments = Arguments.of(sql, ImmutableList.of("UNION",
                "PREDICATES: 11: O_ORDERKEY <=> NULL, 17: O_CLERK > 16: O_ORDERPRIORITY",
                "PREDICATES: 22: O_CUSTKEY = CAST(abs(1) AS INT), NOT (21: O_ORDERKEY <=> NULL), " +
                        "27: O_CLERK > 26: O_ORDERPRIORITY",
                "PREDICATES: 35: O_ORDERDATE = str_to_date('2014-12-21', '%Y-%m'), NOT (31: O_ORDERKEY <=> NULL), " +
                        "(32: O_CUSTKEY != CAST(abs(1) AS INT)) OR (32: O_CUSTKEY = CAST(abs(1) AS INT) IS NULL), " +
                        "37: O_CLERK > 36: O_ORDERPRIORITY"));
        list.add(arguments);

        sql = "select * from orders where O_CUSTKEY in (1, 100, 1000, 2000) or O_COMMENT in ('a', 'b')";
        arguments = Arguments.of(sql, ImmutableList.of("UNION",
                "PREDICATES: 12: O_CUSTKEY IN (1, 100, 1000, 2000)",
                "PREDICATES: 29: O_COMMENT IN ('a', 'b'), 22: O_CUSTKEY NOT IN (1, 100, 1000, 2000)"));
        list.add(arguments);

        sql = "select * from orders where O_CUSTKEY in " + generateMultipleValues(2048);
        arguments = Arguments.of(sql, ImmutableList.of("UNION",
                "PREDICATES: 22: O_CUSTKEY IN", "PREDICATES: 12: O_CUSTKEY IN"));
        list.add(arguments);

        sql = "select * from orders where O_CUSTKEY in " + generateMultipleValues(2048) + " and O_COMMENT != 'a'";
        arguments = Arguments.of(sql, ImmutableList.of("UNION",
                "PREDICATES: 22: O_CUSTKEY IN (", "PREDICATES: 12: O_CUSTKEY IN"));
        list.add(arguments);

        sql =
                "select * from orders where (O_ORDERKEY < 1 and O_CLERK = 'a') or (O_COMMENT = 'c' and O_CUSTKEY <=> null)";
        arguments = Arguments.of(sql, ImmutableList.of("UNION",
                "PREDICATES: 19: O_COMMENT = 'c', 12: O_CUSTKEY <=> NULL",
                "PREDICATES: 21: O_ORDERKEY < 1, 27: O_CLERK = 'a', NOT ((29: O_COMMENT = 'c') AND (22: O_CUSTKEY <=> NULL))"));
        list.add(arguments);

        sql = "select * from orders where ((O_COMMENT = 'c' and O_CUSTKEY in (1, 100, 1000)) " +
                "or (O_CLERK = 'a')) or (O_ORDERKEY in (200, 300))";
        arguments = Arguments.of(sql, ImmutableList.of("UNION",
                "PREDICATES: 19: O_COMMENT = 'c', 12: O_CUSTKEY IN (1, 100, 1000)",
                "PREDICATES: 21: O_ORDERKEY IN (200, 300), NOT ((29: O_COMMENT = 'c') AND (22: O_CUSTKEY IN (1, 100, 1000)))",
                "PREDICATES: 37: O_CLERK = 'a', NOT ((39: O_COMMENT = 'c') AND (32: O_CUSTKEY IN (1, 100, 1000))), " +
                        "31: O_ORDERKEY NOT IN (200, 300)"));
        list.add(arguments);

        sql = "select max(p_type) from part where p_name = 'a' or p_size = 1 group by p_name";
        arguments = Arguments.of(sql, ImmutableList.of("UNION", "7:Decode", "3:Decode"));
        list.add(arguments);

        sql = "select max(p_type) from part left semi join (" +
                "select * from orders where O_COMMENT != 'c' and (O_CUSTKEY in (1, 100, 1000) or O_CLERK = 'a' )" +
                " " +
                ") t on p_size = O_ORDERKEY where p_type > 'a' and (p_name = 'a' or p_size = 1)";
        arguments = Arguments.of(sql, ImmutableList.of("UNION", "3:Decode", "7:Decode"));
        list.add(arguments);

        return list.stream();
    }

    @AfterAll
    public static void afterClass() {
        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(false);
        connectContext.getSessionVariable().setScanOrToUnionThreshold(50000000);
        connectContext.getSessionVariable().setSelectRatioThreshold(0.15);
    }

    private static Stream<Arguments> testNotSplitUnionSqls() {
        List<String> list = Lists.newArrayList();

        String sql = "select * from orders where O_COMMENT = 'c' and (O_CUSTKEY = 1 or O_ORDERKEY = 2)";
        list.add(sql);

        sql = "select * from orders where O_COMMENT != 'c' and O_COMMENT > 'a' and O_COMMENT < 'd'";
        list.add(sql);

        sql = "select * from orders where O_COMMENT != 'c' or O_ORDERKEY = 1";
        list.add(sql);

        sql = "select * from orders where O_CUSTKEY = 1 or O_ORDERKEY = 2 or O_COMMENT = 'c' " +
                "or O_CUSTKEY = abs(1) or O_ORDERDATE = '2021-01-01'";
        list.add(sql);

        sql = "select * from orders where O_COMMENT = O_CLERK and O_ORDERKEY + O_ORDERKEY = 1";
        list.add(sql);

        sql = "select * from orders where O_CUSTKEY in " + generateMultipleValues(3000) +
                " or O_SHIPPRIORITY in " + generateMultipleValues(3000) + " or O_COMMENT != 'a'";
        list.add(sql);

        sql = "select * from orders where O_CUSTKEY in " + generateMultipleValues(3000) +
                " or o_comment in('a', 'b')";
        list.add(sql);

        sql = "select * from orders where O_CUSTKEY in " + generateMultipleValues(5000);
        list.add(sql);

        // cannot transform to union because the column statistic is unknown
        sql = "select * from test_all_type where t1a = 'a' or t1b = 1";
        list.add(sql);

        sql = "select * from test_all_type where t1b in (1, 2, 3) or t1a in ('a', 'b', 'c')";
        list.add(sql);

        sql = "select max(p_type) from part where p_name = 'a' or p_name > p_size";
        list.add(sql);

        return list.stream().map(e -> Arguments.of(e));
    }

    private static String generateMultipleValues(int num) {
        StringJoiner joiner = new StringJoiner(", ", "(", ")");
        for (int i = 0; i < num; i++) {
            joiner.add(i + "");
        }
        return joiner.toString();
    }
}
