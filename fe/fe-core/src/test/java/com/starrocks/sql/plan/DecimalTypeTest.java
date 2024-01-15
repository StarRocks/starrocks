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

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class DecimalTypeTest extends PlanTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        starRocksAssert.withTable("CREATE TABLE tab0 (" +
                "c_0_0 DECIMAL(26, 2) NOT NULL ," +
                "c_0_1 DECIMAL128(19, 3) NOT NULL ," +
                "c_0_2 DECIMAL128(4, 3) NULL ," +
                "c_0_3 BOOLEAN NOT NULL ," +
                "c_0_4 DECIMAL128(25, 19) NOT NULL ," +
                "c_0_5 BOOLEAN REPLACE NOT NULL ," +
                "c_0_6 DECIMAL32(8, 5) MIN NULL ," +
                "c_0_7 BOOLEAN REPLACE NULL ," +
                "c_0_8 PERCENTILE PERCENTILE_UNION NULL ," +
                "c_0_9 LARGEINT SUM NULL ," +
                "c_0_10 PERCENTILE PERCENTILE_UNION NOT NULL ," +
                "c_0_11 BITMAP BITMAP_UNION NULL ," +
                "c_0_12 HLL HLL_UNION NOT NULL ," +
                "c_0_13 DECIMAL(16, 3) MIN NULL ," +
                "c_0_14 DECIMAL128(18, 6) MAX NOT NULL " +
                ") AGGREGATE KEY (c_0_0,c_0_1,c_0_2,c_0_3,c_0_4) " +
                "DISTRIBUTED BY HASH (c_0_3,c_0_0,c_0_2) " +
                "properties(\"replication_num\"=\"1\") ;");

        starRocksAssert.withTable("CREATE TABLE IF NOT EXISTS tab1 (" +
                "c_1_0 DECIMAL64(9, 9) NULL ," +
                "c_1_1 CHAR(1) NOT NULL ," +
                "c_1_2 DECIMAL32(5, 4) NOT NULL ," +
                "c_1_3 DECIMAL32(4, 0) NOT NULL ," +
                "c_1_4 CHAR(11) NOT NULL ," +
                "c_1_5 DATE NOT NULL ," +
                "c_1_6 DECIMAL128(20, 5) NULL ) " +
                "UNIQUE KEY (c_1_0,c_1_1) " +
                "DISTRIBUTED BY HASH (c_1_0) " +
                "properties(\"replication_num\"=\"1\") ;");

        starRocksAssert.withTable("CREATE TABLE tab2 (" +
                "c_2_0 BOOLEAN NULL ) " +
                "AGGREGATE KEY (c_2_0) " +
                "DISTRIBUTED BY HASH (c_2_0) " +
                "properties(\"replication_num\"=\"1\") ;");
    }

    @Test
    public void testCastDecimal() throws Exception {
        String sql = "SELECT DISTINCT subt2.c_2_0 FROM tab0, " +
                "(SELECT tab2.c_2_0 FROM tab2 " +
                "WHERE ( ( tab2.c_2_0 ) = ( true ) ) < " +
                "( ((tab2.c_2_0) IN (false) ) BETWEEN (tab2.c_2_0) AND (tab2.c_2_0) ) ) subt2" +
                " FULL OUTER JOIN " +
                "(SELECT tab1.c_1_0, tab1.c_1_1, tab1.c_1_2, tab1.c_1_3, tab1.c_1_4, tab1.c_1_5, tab1.c_1_6 FROM tab1 " +
                " ORDER BY tab1.c_1_4, tab1.c_1_2) subt1 " +
                "ON subt2.c_2_0 = subt1.c_1_2 AND (6453) IN (4861, 4302) < subt1.c_1_2 " +
                " AND subt2.c_2_0 != subt1.c_1_1 AND subt2.c_2_0 <= subt1.c_1_1 " +
                "AND subt2.c_2_0 > subt1.c_1_0 AND subt2.c_2_0 = subt1.c_1_0 " +
                " WHERE (((0.00) BETWEEN (CASE WHEN (subt1.c_1_5) BETWEEN (subt1.c_1_5) AND (subt1.c_1_5) " +
                "THEN CAST(151971657 AS DECIMAL32 ) " +
                " WHEN false THEN CASE WHEN NULL THEN 0.03 ELSE 0.02 END ELSE 0.04 END) AND (0.04) ) IS NULL)";
        String explain = getFragmentPlan(sql);
        String snippet = "5:OlapScanNode\n" +
                "     TABLE: tab1\n" +
                "     PREAGGREGATION: OFF. Reason: Has can not pre-aggregation Join\n" +
                "     partitions=0/1\n" +
                "     rollup: tab1\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=1\n" +
                "     avgRowSize=3.0\n" +
                "     numNodes=0";
        assertContains(explain, snippet);
    }

    @Test
    public void testDecimalCast() throws Exception {
        String sql = "select * from baseall where cast(k5 as decimal32(4,3)) = 1.234";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("PREDICATES: CAST(5: k5 AS DECIMAL32(4,3)) = 1.234"));

        sql = "SELECT k5 FROM baseall WHERE (CAST(k5 AS DECIMAL32 ) ) IN (0.006) " +
                "GROUP BY k5 HAVING (k5) IN (0.005, 0.006)";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan,
                plan.contains("PREDICATES: 5: k5 IN (0.005, 0.006), CAST(5: k5 AS DECIMAL32(9,9)) = 0.006"));
    }

    @Test
    public void testDecimalConstRewrite() throws Exception {
        String sql = "select * from t0 WHERE CAST( - 8 AS DECIMAL ) * + 52 + 87 < - 86";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1");
    }

    @Test
    public void testCountDecimalV3Literal() throws Exception {
        String sql = "select count( - - cast(89 AS DECIMAL )) from t0";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("output: count(89)"));

        sql = "select max( - - cast(89 AS DECIMAL )) from t0";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("output: max(89)"));

        sql = "select min( - - cast(89 AS DECIMAL )) from t0";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("output: min(89)"));

        sql = "select sum( - - cast(89 AS DECIMAL )) from t0";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("output: sum(89)"));

        sql = "select avg( - - cast(89 AS DECIMAL )) from t0";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("output: avg(89)"));
    }

    @Test
    public void testDecimalV3Distinct() throws Exception {
        String sql = "select avg(t1c), count(distinct id_decimal) from test_all_type;";
        String plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains("multi_distinct_count[([10: id_decimal, DECIMAL64(10,2), true]); " +
                "args: DECIMAL64; result: BIGINT; args nullable: true; result nullable: false]"));
    }

    @Test
    public void testArithmeticDecimalReuse() throws Exception {
        String sql = "select t1a, sum(id_decimal * t1f), sum(id_decimal * t1f)" +
                "from test_all_type group by t1a";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("OUTPUT EXPRS:1: t1a | 12: sum | 12: sum"));
    }

    @Test
    public void testDecimalV3LiteralCast() throws Exception {
        String sql = "select id_datetime " +
                "from test_all_type WHERE CAST(IF(true, 0.38542880072101215, '-Inf')  AS BOOLEAN )";
        String thrift = getThriftPlan(sql);
        Assert.assertTrue(thrift.contains("string_literal:TStringLiteral(value:0.38542880072101215)"));
    }

    @Test
    public void testJoinDecimalAndBool() throws Exception {
        String sql =
                "select t3.v10 from t3 inner join test_all_type on t3.v11 = test_all_type.id_decimal and t3.v11 > true";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  0:OlapScanNode\n"
                + "     TABLE: t3\n"
                + "     PREAGGREGATION: ON\n"
                + "     PREDICATES: 2: v11 > 1"));

        assertContains(plan, "  2:OlapScanNode\n" +
                "     TABLE: test_all_type\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: CAST(13: id_decimal AS DECIMAL128(21,2)) IS NOT NULL\n" +
                "     partitions=0/1\n" +
                "     rollup: test_all_type");
    }

    @Test
    public void testExpressionRangeCheck() throws Exception {
        String sql = "select * from tab1 where c_1_3 between c_1_3 and 1000";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "PREDICATES: 4: c_1_3 <= 1000");
    }


    @Test
    public void testArrayAggDecimal() throws Exception {
        int stage = connectContext.getSessionVariable().getNewPlannerAggStage();
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        try {
            String sql = "select array_agg(c_0_0) from tab0";
            String plan = getVerboseExplain(sql);
<<<<<<< HEAD
            assertContains(plan, "array_agg[([16: array_agg, STRUCT<ARRAY<decimal128(26, 2)>>, true]); " +
                    "args: DECIMAL128; result: ARRAY<DECIMAL128(26,2)>;");
            assertContains(plan, "array_agg[([1: c_0_0, DECIMAL128(26,2), false]); " +
                    "args: DECIMAL128; result: STRUCT<ARRAY<decimal128(26, 2)>>;");
=======
            assertContains(plan, "array_agg[([16: array_agg, struct<col1 array<DECIMAL128(26,2)>>, true]); " +
                    "args: DECIMAL128; result: ARRAY<DECIMAL128(26,2)>;");
            assertContains(plan, "array_agg[([1: c_0_0, DECIMAL128(26,2), false]); " +
                    "args: DECIMAL128; result: struct<col1 array<DECIMAL128(26,2)>>;");
>>>>>>> 9cfa912818 ([Enhancement] The decimal creation type is consistent with the display type (#38639))
        } finally {
            connectContext.getSessionVariable().setNewPlanerAggStage(stage);
        }

        connectContext.getSessionVariable().setNewPlanerAggStage(0);
        try {
            String sql = "select array_agg(c_0_0) from tab0";
            String plan = getVerboseExplain(sql);
            assertContains(plan, "array_agg[([1: c_0_0, DECIMAL128(26,2), false]); " +
                    "args: DECIMAL128; result: ARRAY<DECIMAL128(26,2)>;");
        } finally {
            connectContext.getSessionVariable().setNewPlanerAggStage(stage);
        }
        try {
            String sql = "select array_agg(distinct c_2_0) from tab2";
            String plan = getVerboseExplain(sql);
            assertContains(plan, "array_agg_distinct");
        } finally {
            connectContext.getSessionVariable().setNewPlanerAggStage(stage);
        }
    }

    @Test
    public void testDecimalV2Cast() throws Exception {
        String sql = "select cast('12.367' as decimalv2(9,0));";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "12.367");

        sql = "select cast('12.367' as decimalv2(9,1));";
        plan = getFragmentPlan(sql);
        assertContains(plan, "12.367");

        sql = "select cast('12.367' as decimalv2(9,2));";
        plan = getFragmentPlan(sql);
        assertContains(plan, "12.367");

        sql = "select cast(cast('12.56' as decimalv2(9,1)) as varchar);";
        plan = getFragmentPlan(sql);
        assertContains(plan, "'12.56'");
    }
}
