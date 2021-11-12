// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.plan;

import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.utframe.StarRocksAssert;
import mockit.Expectations;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;

public class PlanFragmentTest extends PlanTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withTable("create table test_array(c0 INT, c1 array<varchar(65533)>)" +
                " duplicate key(c0) distributed by hash(c0) buckets 1 " +
                "properties('replication_num'='1');");
        starRocksAssert.withTable("create table test.colocate1\n" +
                "(k1 int, k2 int, k3 int) distributed by hash(k1, k2) buckets 1\n" +
                "properties(\"replication_num\" = \"1\"," +
                "\"colocate_with\" = \"group1\");")
                .withTable("create table test.colocate2\n" +
                        "(k1 int, k2 int, k3 int) distributed by hash(k1, k2) buckets 1\n" +
                        "properties(\"replication_num\" = \"1\"," +
                        "\"colocate_with\" = \"group1\");")
                .withTable("create table test.nocolocate3\n" +
                        "(k1 int, k2 int, k3 int) distributed by hash(k1, k2) buckets 10\n" +
                        "properties(\"replication_num\" = \"1\");");
        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(true);
    }

    @Test
    public void testScan() throws Exception {
        String sql = "select * from t0";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains(" OUTPUT EXPRS:1: v1 | 2: v2 | 3: v3\n"
                + "  PARTITION: RANDOM"));
    }

    @Test
    public void testProject() throws Exception {
        String sql = "select v1 from t0";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PLAN FRAGMENT 0\n"
                + " OUTPUT EXPRS:1: v1\n"
                + "  PARTITION: RANDOM\n"
                + "\n"
                + "  RESULT SINK\n"
                + "\n"
                + "  0:OlapScanNode\n"
                + "     TABLE: t0\n"
                + "     PREAGGREGATION: ON\n"
                + "     partitions=0/1"));
    }

    @Test
    public void testLimit() throws Exception {
        String sql = "select v1 from t0 limit 1";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PLAN FRAGMENT 0\n"
                + " OUTPUT EXPRS:1: v1\n"
                + "  PARTITION: RANDOM\n"
                + "\n"
                + "  RESULT SINK\n"
                + "\n"
                + "  0:OlapScanNode\n"
                + "     TABLE: t0\n"
                + "     PREAGGREGATION: ON\n"
                + "     partitions=0/1\n"
                + "     rollup: t0\n"
                + "     tabletRatio=0/0\n"
                + "     tabletList=\n"
                + "     cardinality=1\n"
                + "     avgRowSize=1.0\n"
                + "     numNodes=0\n"
                + "     limit: 1"));
    }

    @Test
    public void testFilter() throws Exception {
        String sql = "select v1 from t0 where v2 > 1";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PREDICATES: 2: v2 > 1"));
    }

    @Test
    public void testHaving() throws Exception {
        String sql = "select v2 from t0 group by v2 having v2 > 0";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PREDICATES: 2: v2 > 0"));

        sql = "select sum(v1) from t0 group by v2 having v2 > 0";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PREDICATES: 2: v2 > 0"));

        sql = "select sum(v1) from t0 group by v2 having sum(v1) > 0";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("having: 4: sum(1: v1) > 0"));
    }

    @Test
    public void testCountDistinctBitmapHll() throws Exception {
        String sql = "select count(distinct v1), count(distinct v2), count(distinct v3), count(distinct v4), " +
                "count(distinct b1), count(distinct b2), count(distinct b3), count(distinct b4) from test_object;";
        getFragmentPlan(sql);

        sql = "select count(distinct v1), count(distinct v2), " +
                "count(distinct h1), count(distinct h2) from test_object";
        getFragmentPlan(sql);
    }

    @Test
    public void testExpression() throws Exception {
        String sql = "select v1 + v2, v1 + v2 + v3, v1 + v2 + v3 + 1 from t0";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  |  common expressions:\n"
                + "  |  <slot 7> : 1: v1 + 2: v2\n"
                + "  |  <slot 8> : 7: add + 3: v3\n"));
    }

    @Test
    public void testReduceCast() throws Exception {
        String sql = "select t1a, t1b from test_all_type where t1c > 2000 + 1";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  0:OlapScanNode\n" +
                "     TABLE: test_all_type\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 3: t1c > 2001"));
    }

    @Test
    public void testExpression1() throws Exception {
        String sql = "select sum(v1 + v2) from t0";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n"
                + "  |  <slot 4> : 1: v1 + 2: v2"));
    }

    @Test
    public void testExpression2() throws Exception {
        String sql = "select sin(v1) + cos(v2) as a from t0";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("sin(CAST(1: v1 AS DOUBLE)) + cos(CAST(2: v2 AS DOUBLE))"));

        sql = "select * from test_all_type where id_date = 20200202";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PREDICATES: 9: id_date = '2020-02-02'"));
    }

    @Test
    public void testExpression3() throws Exception {
        String sql =
                "select cast (v1 as boolean), cast (v1 as tinyint), cast (v1 as smallint), cast (v1 as int), cast (v1"
                        + " as bigint), cast (v1 as largeint), cast (v1 as float), cast (v1 as double), cast(v1 as "
                        + "date), cast(v1 as datetime), cast(v1 as decimalv2), cast(v1 as varchar) from t0";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n"
                + "  |  <slot 1> : 1: v1\n"
                + "  |  <slot 4> : CAST(1: v1 AS BOOLEAN)\n"
                + "  |  <slot 5> : CAST(1: v1 AS TINYINT)\n"
                + "  |  <slot 6> : CAST(1: v1 AS SMALLINT)\n"
                + "  |  <slot 7> : CAST(1: v1 AS INT)\n"
                + "  |  <slot 8> : CAST(1: v1 AS LARGEINT)\n"
                + "  |  <slot 9> : CAST(1: v1 AS FLOAT)\n"
                + "  |  <slot 10> : CAST(1: v1 AS DOUBLE)\n"
                + "  |  <slot 11> : CAST(1: v1 AS DATE)\n"
                + "  |  <slot 12> : CAST(1: v1 AS DATETIME)\n"
                + "  |  <slot 13> : CAST(1: v1 AS DECIMAL(9,0))\n"
                + "  |  <slot 14> : CAST(1: v1 AS VARCHAR)\n"));
    }

    @Test
    public void testExpression4() throws Exception {
        String sql =
                "select v1 * v1 / v1 % v1 + v1 - v1 DIV v1, v2&~v1|v3^1 from t0 where v1 >= 1 and v1 <=10 and v2 > 1 "
                        + "and v2 < 10 and v3 != 10 and v3 <=> 10 and !(v1 = 1 and v2 = 2 or v3 =3) and v1 between 1 "
                        + "and 2";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n"
                + "  |  <slot 4> : CAST(1: v1 * 1: v1 AS DOUBLE) / 6: cast % 6: cast + 6: cast - CAST(1: v1 DIV 1: v1"
                + " AS DOUBLE)\n"
                + "  |  <slot 5> : 2: v2 & ~ 1: v1 | 3: v3 ^ 1\n"
                + "  |  common expressions:\n"
                + "  |  <slot 6> : CAST(1: v1 AS DOUBLE)\n"));
        Assert.assertTrue(planFragment.contains("PREDICATES: 1: v1 >= 1, 1: v1 <= 10, 2: v2 > 1, 2: v2 < 10, 3: "
                + "v3 != 10, 3: v3 <=> 10, (1: v1 != 1) OR (2: v2 != 2), 3: v3 != 3, 1: v1 <= 2\n"));
    }

    @Test
    public void testExpression5() throws Exception {
        String sql = "select v1+20, case v2 when v3 then 1 else 0 end from t0 where v1 is null";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n"
                + "  |  <slot 4> : 1: v1 + 20\n"
                + "  |  <slot 5> : if(2: v2 = 3: v3, 1, 0)"));

        sql = "select v1+20, case when true then v1 else v2 end from t0 where v1 is null";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 4> : 1: v1 + 20"));

        sql = "select v1+20, ifnull(null, v2) from t0 where v1 is null";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n" +
                "  |  <slot 2> : 2: v2\n" +
                "  |  <slot 4> : 1: v1 + 20\n" +
                "  |  use vectorized: true"));

        sql = "select v1+20, if(true, v1, v2) from t0 where v1 is null";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 4> : 1: v1 + 20"));

        sql = "select v1+20, if(false, v1, NULL) from t0 where v1 is null";
        planFragment = getFragmentPlan(sql);
        System.out.println(planFragment);
        Assert.assertTrue(planFragment.contains("  1:Project\n" +
                "  |  <slot 4> : 1: v1 + 20\n" +
                "  |  <slot 5> : NULL"));
    }

    @Test
    public void testExpression6() throws Exception {
        String sql = "select cast(v1 as decimal64(7,2)) + cast(v2 as decimal64(9,3)) from t0";
        Config.enable_decimal_v3 = true;
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n" +
                "  |  <slot 4> : CAST(CAST(1: v1 AS DECIMAL64(7,2)) AS DECIMAL64(18,2)) + CAST(CAST(2: v2 AS DECIMAL64(9,3)) AS DECIMAL64(18,3))\n" +
                "  |  use vectorized: true\n"));
        Config.enable_decimal_v3 = false;
    }

    @Test
    public void testExpression7() throws Exception {
        String sql = "select cast(v1 as decimal128(27,2)) - cast(v2 as decimal64(10,3)) from t0";
        Config.enable_decimal_v3 = true;
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n" +
                "  |  <slot 4> : CAST(CAST(1: v1 AS DECIMAL128(27,2)) AS DECIMAL128(38,2)) - CAST(CAST(2: v2 AS DECIMAL64(10,3)) AS DECIMAL128(38,3))\n" +
                "  |  use vectorized: true"));
        Config.enable_decimal_v3 = false;
    }

    @Test
    public void testExpression8() throws Exception {
        String sql = "select cast(v1 as decimal128(10,5)) * cast(v2 as decimal64(9,7)) from t0";
        Config.enable_decimal_v3 = true;
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n" +
                "  |  <slot 4> : CAST(CAST(1: v1 AS DECIMAL128(10,5)) AS DECIMAL128(38,5)) * CAST(CAST(2: v2 AS DECIMAL64(9,7)) AS DECIMAL128(38,7))\n" +
                "  |  use vectorized: true"));
        Config.enable_decimal_v3 = false;
    }

    @Test
    public void testExpression9() throws Exception {
        String sql = "select cast(v1 as decimal128(18,5)) / cast(v2 as decimal32(9,7)) from t0";
        Config.enable_decimal_v3 = true;
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n" +
                "  |  <slot 4> : CAST(CAST(1: v1 AS DECIMAL128(18,5)) AS DECIMAL128(38,5)) / CAST(CAST(2: v2 AS DECIMAL32(9,7)) AS DECIMAL128(38,7))\n" +
                "  |  use vectorized: true"));
        Config.enable_decimal_v3 = false;
    }

    @Test
    public void testExpression10() throws Exception {
        String sql = "select cast(v1 as decimal64(18,5)) % cast(v2 as decimal32(9,7)) from t0";
        Config.enable_decimal_v3 = true;
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n" +
                "  |  <slot 4> : CAST(1: v1 AS DECIMAL64(18,5)) % CAST(CAST(2: v2 AS DECIMAL32(9,7)) AS DECIMAL64(18,7))\n" +
                "  |  use vectorized: true"));
        Config.enable_decimal_v3 = false;
    }

    @Test
    public void testMergeTwoFilters() throws Exception {
        String sql = "select v1 from t0 where v2 < null group by v1 HAVING NULL IS NULL;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:AGGREGATE (update finalize)\n"
                + "  |  group by: 1: v1\n"
                + "  |  having: TRUE\n"
                + "  |  use vectorized: true"));

        Assert.assertTrue(planFragment.contains("  0:EMPTYSET\n"
                + "     use vectorized: true"));
    }

    @Test
    public void testInColumnPredicate() throws Exception {
        String sql = "select v1 from t0 where v1 in (v1 + v2, sin(v2))";
        String thriftPlan = getThriftPlan(sql);
        Assert.assertTrue(thriftPlan.contains("FILTER_NEW_IN"));
        Assert.assertFalse(thriftPlan.contains("FILTER_IN"));
    }

    @Test
    public void testCountConstantWithSubquery() throws Exception {
        String sql = "SELECT 1 FROM (SELECT COUNT(1) FROM t0 WHERE false) t;";
        String thriftPlan = getThriftPlan(sql);
        Assert.assertTrue(thriftPlan.contains("function_name:count"));
    }

    @Test
    public void testOlapScanSelectedIndex() throws Exception {
        String sql = "select v1 from t0";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("rollup: t0"));
    }

    @Test
    public void testHaving2() throws Exception {
        String sql = "SELECT 8 from t0 group by v1 having avg(v2) < 63;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("having: 4: avg(2: v2) < 63.0"));
    }

    @Test
    public void testLimitWithHaving() throws Exception {
        String sql = "SELECT v1, sum(v3) as v from t0 where v2 = 0 group by v1 having sum(v3) > 0 limit 10";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("having: 4: sum(3: v3) > 0"));
        Assert.assertTrue(planFragment.contains("limit: 10"));
    }

    @Test
    public void testInnerJoinWithPredicate() throws Exception {
        String sql = "SELECT * from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 = 1;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PREDICATES: 1: v1 = 1"));
    }

    @Test
    public void testInnerJoinWithConstPredicate() throws Exception {
        String sql = "SELECT * from t0 join test_all_type on NOT NULL >= NULL";

        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  0:EMPTYSET\n"
                + "     use vectorized: true\n"));
    }

    @Test
    public void testInnerJoinWithCastPredicate() throws Exception {
        String sql = "SELECT t0.v1 from t0 join test_all_type on t0.v1 = test_all_type.t1c";
        getFragmentPlan(sql);
    }

    @Test
    public void testCorssJoinWithPredicate() throws Exception {
        String sql = "SELECT * from t0 join test_all_type where t0.v1 = 2;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PREDICATES: 1: v1 = 2"));
    }

    @Test
    public void testLeftOuterJoinWithPredicate() throws Exception {
        String sql = "SELECT * from t0 left join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 > 1;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PREDICATES: 1: v1 > 1"));
    }

    @Test
    public void testCrossJoinToInnerJoin() throws Exception {
        String sql = "SELECT t0.v1 from t0, test_all_type where t0.v1 = test_all_type.t1d";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("join op: INNER JOIN"));
        Assert.assertTrue(planFragment.contains("equal join conjunct: 1: v1 = 7: t1d"));
    }

    @Test
    public void testWherePredicatesToOnPredicate() throws Exception {
        String sql =
                "SELECT t0.v1 from t0 join test_all_type on t0.v2 = test_all_type.t1d where t0.v1 = test_all_type.t1d";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("join op: INNER JOIN"));
        Assert.assertTrue(planFragment.contains("  |  equal join conjunct: 2: v2 = 7: t1d\n"
                + "  |  equal join conjunct: 1: v1 = 7: t1d"));
    }

    @Test
    public void testJoinColumnsPrune() throws Exception {
        String sql = " select count(a.v3) from t0 a join t0 b on a.v3 = b.v3;";
        getFragmentPlan(sql);

        sql = " select a.v2 from t0 a join t0 b on a.v3 = b.v3;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("4:Project\n"
                + "  |  <slot 2> : 2: v2"));
    }

    @Test
    public void testCrossJoin() throws Exception {
        String sql = "SELECT * from t0 join test_all_type;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  3:CROSS JOIN\n" +
                "  |  cross join:\n" +
                "  |  predicates is NULL.\n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  |----2:EXCHANGE\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  0:OlapScanNode"));
    }

    @Test
    public void testCaseWhenOperatorReuse() throws Exception {
        String sql =
                "select max(case when SUBSTR(DATE_FORMAT('2020-09-02 23:59:59', '%Y-%m'), 6) > 0 then v1 else v2 end),"
                        +
                        "min(case when SUBSTR(DATE_FORMAT('2020-09-02 23:59:59', '%Y-%m'), 6) > 0 then v2 else v1 end),"
                        +
                        "count(case when SUBSTR(DATE_FORMAT('2020-09-02 23:59:59', '%Y-%m'), 6) > 0 then v3 else v2 "
                        + "end) from t0";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("<slot 10> : substr('2020-09', 6)"));
        Assert.assertTrue(planFragment.contains("  |  <slot 4> : if(12: expr, 1: v1, 2: v2)"));
    }

    @Test
    public void testTimestampArithmeticExpr() throws Exception {
        String sql = "select id_date + interval '3' month," +
                "id_date + interval '1' day," +
                "id_date + interval '2' year," +
                "id_date - interval '3' day from test_all_type";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  |  <slot 11> : months_add(15: cast, 3)\n"
                + "  |  <slot 12> : days_add(15: cast, 1)\n"
                + "  |  <slot 13> : years_add(15: cast, 2)\n"
                + "  |  <slot 14> : days_sub(15: cast, 3)\n"));
    }

    @Test
    public void testGroupByNull() throws Exception {
        String sql = "select count(*) from test_all_type group by null";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("<slot 11> : NULL"));
    }

    @Test
    public void testFullOuterJoin() throws Exception {
        String sql = "select * from t0 full outer join t1 on t0.v1 = t1.v4 where abs(1) > 2;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("     TABLE: t1\n"
                + "     PREAGGREGATION: ON\n"
                + "     PREDICATES: abs(1) > 2"));
        Assert.assertTrue(planFragment.contains("     TABLE: t0\n"
                + "     PREAGGREGATION: ON\n"
                + "     PREDICATES: abs(1) > 2"));
    }

    @Test
    public void testSumDistinctConst() throws Exception {
        String sql = "select sum(2), sum(distinct 2) from test_all_type";
        String thriftPlan = getThriftPlan(sql);
        Assert.assertTrue(thriftPlan.contains("function_name:multi_distinct_sum"));
    }

    @Test
    public void testGroupByAsAnalyze() throws Exception {
        String sql = "select BITOR(825279661, 1960775729) as a from test_all_type group by a";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("group by: 11: bitor"));
    }

    @Test
    public void testHavingAsAnalyze() throws Exception {
        String sql = "select count(*) as count1 from test_all_type having count1 > 1";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("having: 11: count() > 1"));
    }

    @Test
    public void testGroupByAsAnalyze2() throws Exception {
        String sql = "select v1 as v2 from t0 group by v1, v2;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("group by: 1: v1, 2: v2"));
    }

    @Test
    public void testWindowLimitPushdown() throws Exception {
        String sql = "select lag(v1, 1,1) OVER () from t0 limit 1";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  |  window: ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING\n" +
                "  |  limit: 1"));
    }

    @Test
    public void testDistinctRedundant() throws Exception {
        String sql = "SELECT DISTINCT + + v1, v1 AS col2 FROM t0;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  |  group by: 1: v1\n"
                + "  |  use vectorized: true"));
    }

    @Test
    public void testSelectStarWhereSubQueryLimit1() throws Exception {
        String sql = "SELECT * FROM t0 where v1 = (select v1 from t0 limit 1);";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("ASSERT NUMBER OF ROWS"));
    }

    @Test
    public void testCrossJoinWithLimit() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "select * from t0 join t1 on t0.v2 = t1.v4 limit 2";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("3:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 4: v4 = 2: v2\n" +
                "  |  limit: 2"));
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testExistOrderBy() throws Exception {
        String sql = "SELECT * \n" +
                "FROM   emp \n" +
                "WHERE  EXISTS (SELECT dept.dept_id \n" +
                "               FROM   dept \n" +
                "               WHERE  emp.dept_id = dept.dept_id \n" +
                "               ORDER  BY state) \n" +
                "ORDER  BY hiredate";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("LEFT SEMI JOIN"));
    }

    @Test
    public void testFullOuterJoinPredicatePushDown() throws Exception {
        String sql = "select * from t0 full outer join t1 on t0.v1 = t1.v4 " +
                " where (NOT (t0.v2 IS NOT NULL))";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("other predicates: 2: v2 IS NULL"));
    }

    @Test
    public void testRightSemiJoinWithFilter() throws Exception {
        String sql = "select t1.v4 from t0 right semi join t1 on t0.v1 = t1.v4 and t0.v1 > 1 ";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PREDICATES: 1: v1 > 1"));
    }

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void testGroupByCube() throws Exception {
        String sql = "select grouping_id(v1, v3), grouping(v2) from t0 group by cube(v1, v2, v3);";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("REPEAT_NODE"));
    }

    @Test
    public void testCountDistinctArray() throws Exception {
        String sql = "select count(*), count(c1), count(distinct c1) from test_array";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("AGGREGATE (merge serialize)"));
    }

    @Test
    public void testProjectUsingConstantArgs() throws Exception {
        String sql = "select months_diff(\"2074-03-04T17:43:24\", \"2074-03-04T17:43:24\") from test_all_type";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("1:Project\n"
                + "  |  <slot 11> : months_diff(12: cast, 12: cast)"));
    }

    @Test
    public void testSumDistinctSmallInt() throws Exception {
        String sql = " select sum(distinct t1b) from test_all_type;";
        String thriftPlan = getThriftPlan(sql);
        Assert.assertTrue(thriftPlan.contains("arg_types:[TTypeDesc(types:" +
                "[TTypeNode(type:SCALAR, scalar_type:TScalarType(type:SMALLINT))])]"));
    }

    @Test
    public void testScalarReuseIsNull() throws Exception {
        String sql =
                getFragmentPlan("SELECT (abs(1) IS NULL) = true AND ((abs(1) IS NULL) IS NOT NULL) as count FROM t1;");
        Assert.assertTrue(sql.contains("1:Project\n"
                + "  |  <slot 4> : (6: expr = TRUE) AND (6: expr IS NOT NULL)\n"
                + "  |  common expressions:\n"
                + "  |  <slot 5> : abs(1)\n"
                + "  |  <slot 6> : 5: abs IS NULL"));
    }

    @Test
    public void testColocateJoin() throws Exception {
        String queryStr = "select * from test.colocate1 t1, test.colocate2 t2 " +
                "where t1.k1 = t2.k1 and t1.k2 = t2.k2 and t1.k3 = t2.k3";
        String explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: true"));

        // t1.k1 = t2.k2 not same order with distribute column
        queryStr = "select * from test.colocate1 t1, test.colocate2 t2 " +
                "where t1.k1 = t2.k2 and t1.k2 = t2.k1 and t1.k3 = t2.k3";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: false"));

        queryStr = "select * from test.colocate1 t1, test.colocate2 t2 where t1.k1 = t2.k1";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: false"));

        queryStr = "select * from test.colocate1 t1, test.colocate2 t2 where t1.k2 = t2.k2";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: false"));

        queryStr = "select * from test.colocate1 t1, test.colocate2 t2 " +
                "where t1.k1 = t2.k1 and t1.k2 = t2.k2 + 1";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: false"));
    }

    @Test
    public void testColocateJoinWithOneAggChild() throws Exception {
        String queryStr =
                "select * from test.colocate1 t1 left join (select k1, k2, count(k3) from test.colocate2 group by k1,"
                        + " k2) t2 on  "
                        +
                        "t1.k1 = t2.k1 and t1.k2 = t2.k2";
        String explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: true"));

        queryStr =
                "select * from test.colocate1 t1 left join (select k1, k2, k3, count(k3) from test.colocate2 group by"
                        + " k1, k2, k3) t2 on  "
                        +
                        "t1.k1 = t2.k1 and t1.k2 = t2.k2 and t1.k3 = t2.k3";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: true"));

        queryStr =
                "select * from (select k1, k2, count(k3) from test.colocate2 group by k1, k2) t2 left join test"
                        + ".colocate1 t1 on  "
                        +
                        "t2.k1 = t1.k1 and t2.k2 = t1.k2";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: true"));

        queryStr =
                "select * from test.colocate1 t1 left join (select k1, k2, k3, count(k3) from test.colocate2 group by"
                        + " k1, k2, k3) t2 on  "
                        +
                        "t1.k1 = t2.k1 and t1.k2 = t2.k2";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: true"));

        queryStr =
                "select * from test.colocate1 t1 left join (select k1, k2, count(k3) from test.colocate2 group by k2,"
                        + " k1) t2 on  "
                        +
                        "t1.k1 = t2.k1 and t1.k2 = t2.k2";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: true"));

        queryStr =
                "select * from test.colocate1 t1 left join (select k1, k2, count(k3) from test.colocate2 group by k2,"
                        + " k1) t2 on  "
                        +
                        "t1.k1 = t2.k1";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: false"));

        queryStr =
                "select * from test.colocate1 t1 left join (select k1, k2, count(k3) from test.colocate2 group by k2,"
                        + " k1) t2 on  "
                        +
                        "t1.k2 = t2.k2";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: false"));

        queryStr =
                "select * from test.colocate1 t1 left join (select k1, k2, count(k3) from test.colocate2 group by k2,"
                        + " k1) t2 on  "
                        +
                        "t1.k1 = t2.k2 and t1.k2 = t2.k1";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: false"));
    }

    @Test
    public void testColocateJoinWithTwoAggChild() throws Exception {
        String queryStr =
                "select * from (select k1, k2, count(k3) from test.colocate1 group by k1, k2) t1 left join (select "
                        + "k1, k2, count(k3) from test.colocate2 group by k1, k2) t2 on  "
                        +
                        "t1.k1 = t2.k1 and t1.k2 = t2.k2";
        String explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: true"));

        queryStr =
                "select * from (select k1, k2, k3, count(k3) from test.colocate1 group by k1, k2, k3) t1 left join "
                        + "(select k1, k2, k3, count(k3) from test.colocate2 group by k1, k2, k3) t2 on  "
                        +
                        "t1.k1 = t2.k1 and t1.k2 = t2.k2 ";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: true"));

        queryStr =
                "select * from (select k1, k2, k3, count(k3) from test.colocate1 group by k1, k2, k3) t1 left join "
                        + "(select k1, k2, count(k3) from test.colocate2 group by k1, k2) t2 on  "
                        +
                        "t1.k1 = t2.k1 and t1.k2 = t2.k2 ";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: true"));

        queryStr =
                "select * from (select k1, k2, k3, count(k3) from test.colocate1 group by k1, k2, k3) t1 left join "
                        + "(select k1, k2, k3, count(k3) from test.colocate2 group by k1, k2, k3) t2 on  "
                        +
                        "t1.k1 = t2.k1 and t1.k2 = t2.k2 and t1.k3 = t2.k3";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: true"));

        queryStr =
                "select * from (select k1, k2, count(k3) from test.colocate1 group by k2, k1) t1 left join (select "
                        + "k1, k2, count(k3) from test.colocate2 group by k1, k2) t2 on  "
                        +
                        "t1.k1 = t2.k1 and t1.k2 = t2.k2";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: true"));

        queryStr =
                "select * from (select k1, k2, count(k3) from test.colocate1 group by k1, k2) t1 left join (select "
                        + "k1, k2, count(k3) from test.colocate2 group by k1, k2) t2 on  "
                        +
                        "t1.k2 = t2.k1 and t1.k1 = t2.k2";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: false"));

        queryStr =
                "select * from (select k1, k2, count(k3) from test.colocate1 group by k1, k2) t1 left join (select "
                        + "k1, k2, count(k3) from test.colocate2 group by k1, k2) t2 on  "
                        +
                        "t1.k1 = t2.k1";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: false"));
    }

    @Test
    public void testColocateJoinWithTwoAggChild2() throws Exception {
        String queryStr =
                "select * from (select k2, count(k3) from test.colocate1 group by k2) t1 left join (select "
                        + "k1, k2, count(k3) from test.colocate2 group by k1, k2) t2 on  "
                        + "t1.k2 = t2.k2";
        String explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: false"));
    }

    @Test
    public void testColocateAgg() throws Exception {
        FeConstants.runningUnitTest = true;
        String queryStr;
        String explainString;
        queryStr = "select k2, count(k3) from nocolocate3 group by k2";
        explainString = getFragmentPlan(queryStr);
        System.out.println(explainString);
        Assert.assertTrue(explainString.contains("  3:AGGREGATE (merge finalize)\n"
                + "  |  output: count(4: count(3: k3))\n"
                + "  |  group by: 2: k2\n"
                + "  |  use vectorized: true\n"
                + "  |  \n"
                + "  2:EXCHANGE\n"
                + "     use vectorized: true\n"
                + "\n"
                + "PLAN FRAGMENT 2\n"
                + " OUTPUT EXPRS:"));
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testEmptySet() throws Exception {
        String queryStr = "select * from test.colocate1 t1, test.colocate2 t2 " +
                "where NOT NULL IS NULL";
        String explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("  0:EMPTYSET\n"
                + "     use vectorized: true"));

        queryStr = "select * from test.colocate1 t1, test.colocate2 t2 where FALSE";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("  0:EMPTYSET\n"
                + "     use vectorized: true"));
    }

    @Test
    public void testConnectionId() throws Exception {
        String queryStr = "select connection_id()";
        String explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("0:UNION"));
        Assert.assertTrue(explainString.contains("use vectorized: true"));

        queryStr = "select database();";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("0:UNION"));
        Assert.assertTrue(explainString.contains("use vectorized: true"));
    }

    @Test
    public void testLimit0WithAgg() throws Exception {
        String queryStr = "select count(*) from t0 limit 0";
        String explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("OUTPUT EXPRS:4: count()"));
        Assert.assertTrue(explainString.contains("0:EMPTYSET"));
    }

    @Test
    public void testSubQueryWithLimit0() throws Exception {
        String queryStr = "select v1 from (select * from t0 limit 0) t";
        String explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("0:EMPTYSET"));
    }

    @Test
    public void testAggSubQueryWithLimit0() throws Exception {
        String queryStr = "select sum(a) from (select v1 as a from t0 limit 0) t";
        String explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("0:EMPTYSET"));
    }

    @Test
    public void testDistinctWithGroupBy1() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(3);
        String queryStr = "select avg(v1), count(distinct v1) from t0 group by v1";
        String explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("  3:AGGREGATE (update finalize)\n"
                + "  |  output: avg(4: avg(1: v1)), count(1: v1)\n"
                + "  |  group by: 1: v1\n"
                + "  |  use vectorized: true\n"
                + "  |  \n"
                + "  2:AGGREGATE (merge serialize)\n"
                + "  |  output: avg(4: avg(1: v1))\n"
                + "  |  group by: 1: v1"));
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    @Test
    public void testGroupBy2() throws Exception {
        String queryStr = "select avg(v2) from t0 group by v2";
        String explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("  2:Project\n"
                + "  |  <slot 4> : 4: avg(2: v2)\n"
                + "  |  use vectorized: true\n"
                + "  |  \n"
                + "  1:AGGREGATE (update finalize)\n"
                + "  |  output: avg(2: v2)\n"
                + "  |  group by: 2: v2\n"
                + "  |  use vectorized: true\n"
                + "  |  \n"
                + "  0:OlapScanNode"));
    }

    @Test
    public void testValuesNodePredicate() throws Exception {
        String queryStr = "SELECT 1 AS z, MIN(a.x) FROM (select 1 as x) a WHERE abs(1) = 2";
        String explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("  2:AGGREGATE (update finalize)\n"
                + "  |  output: min(1: expr)\n"
                + "  |  group by: \n"
                + "  |  use vectorized: true\n"
                + "  |  \n"
                + "  1:SELECT\n"
                + "  |  predicates: abs(1) = 2\n"
                + "  |  use vectorized: true"));
    }

    @Test
    public void testAggConstPredicate() throws Exception {
        String queryStr = "select MIN(v1) from t0 having abs(1) = 2";
        String explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("  1:AGGREGATE (update finalize)\n"
                + "  |  output: min(1: v1)\n"
                + "  |  group by: \n"
                + "  |  having: abs(1) = 2\n"
                + "  |  use vectorized: true"));
    }

    @Test
    public void testProjectFilterRewrite() throws Exception {
        String queryStr = "select 1 as b, MIN(v1) from t0 having (b + 1) != b;";
        String explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("  1:AGGREGATE (update finalize)\n"
                + "  |  output: min(1: v1)\n"
                + "  |  group by: \n"
                + "  |  having: TRUE\n"
                + "  |  use vectorized: true"));
    }

    @Test
    public void testUnionLimit() throws Exception {
        String queryStr = "select 1 from (select 4, 3 from t0 union all select 2, 3 ) as a limit 3";
        String explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("  2:Project\n"
                + "  |  <slot 4> : 4\n"
                + "  |  limit: 3\n"
                + "  |  use vectorized: true\n"
                + "  |  \n"
                + "  1:OlapScanNode"));
    }

    @Test
    public void testExceptLimit() throws Exception {
        String queryStr = "select 1 from (select 1, 3 from t0 except select 2, 3 ) as a limit 3";
        String explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("  6:Project\n"
                + "  |  <slot 10> : 1\n"
                + "  |  limit: 3\n"
                + "  |  use vectorized: true\n"
                + "  |  \n"
                + "  0:EXCEPT\n"
                + "  |  limit: 3\n"
                + "  |  use vectorized: true"));

        Assert.assertTrue(explainString.contains("  2:Project\n"
                + "  |  <slot 4> : 1\n"
                + "  |  <slot 5> : 3\n"
                + "  |  use vectorized: true\n"
                + "  |  \n"
                + "  1:OlapScanNode"));
    }

    @Test
    public void testIntersectLimit() throws Exception {
        String queryStr = "select 1 from (select 1, 3 from t0 intersect select 2, 3 ) as a limit 3";
        String explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("  6:Project\n"
                + "  |  <slot 10> : 1\n"
                + "  |  limit: 3\n"
                + "  |  use vectorized: true\n"
                + "  |  \n"
                + "  0:INTERSECT\n"
                + "  |  limit: 3\n"
                + "  |  use vectorized: true"));

        Assert.assertTrue(explainString.contains("  2:Project\n"
                + "  |  <slot 4> : 1\n"
                + "  |  <slot 5> : 3\n"
                + "  |  use vectorized: true\n"
                + "  |  \n"
                + "  1:OlapScanNode"));
    }

    @Test
    public void testUnionSameValues() throws Exception {
        String query = "SELECT 76072, COUNT(DISTINCT b3) * 10, '', '', now() FROM test_object" +
                " UNION ALL" +
                " SELECT 76072, COUNT(DISTINCT b4) *10, '', '', now() FROM test.test_object";
        getFragmentPlan(query);
    }

    @Test
    public void testCrossJoinEliminate() throws Exception {
        String query = "select t1.* from t0, t2, t3, t1 where t1.v4 = t2.v7 " +
                "and t1.v4 = t3.v1 and t3.v1 = t0.v1";
        String explainString = getFragmentPlan(query);
        Assert.assertFalse(explainString.contains("CROSS JOIN"));
    }

    @Test
    public void testScalarOperatorToExpr() {
        ColumnRefOperator columnRefOperator = new ColumnRefOperator(2, Type.INT, "e", true);
        ScalarOperator cast = new CastOperator(Type.DOUBLE, columnRefOperator);
        ColumnRefOperator castColumnRef = new ColumnRefOperator(1, Type.INT, "cast", true);

        HashMap<ColumnRefOperator, ScalarOperator> projectMap = new HashMap<>();
        projectMap.put(castColumnRef, cast);
        projectMap.put(columnRefOperator, ConstantOperator.createInt(1));

        HashMap<ColumnRefOperator, Expr> variableToSlotRef = new HashMap<>();
        variableToSlotRef.put(columnRefOperator, new IntLiteral(1));

        ScalarOperatorToExpr.FormatterContext context =
                new ScalarOperatorToExpr.FormatterContext(variableToSlotRef, projectMap);

        Expr castExpression = ScalarOperatorToExpr.buildExecExpression(castColumnRef, context);

        Assert.assertTrue(castExpression instanceof CastExpr);
    }

    @Test
    public void testSort() throws Exception {
        String sql = "select count(*) from (select L_QUANTITY, L_PARTKEY, L_ORDERKEY from lineitem " +
                "order by L_QUANTITY, L_PARTKEY, L_ORDERKEY limit 5000, 10000) as a;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("2:MERGING-EXCHANGE"));
    }

    @Test
    public void testSemiJoinPushDown() throws Exception {
        String sql = "SELECT *\n"
                + "FROM (\n"
                + "    SELECT t0.v1, t0.v2, t0.v3\n"
                + "    FROM t0\n"
                + ") subt0\n"
                + "    LEFT SEMI JOIN (\n"
                + "        SELECT t1.v4, t1.v5, t1.v6\n"
                + "        FROM t1\n"
                + "    ) subt1\n"
                + "    ON subt0.v1 = subt1.v4\n"
                + "        AND subt0.v2 != subt0.v2\n"
                + "        AND subt0.v2 = subt1.v5\n"
                + "        AND (subt0.v3 <= subt0.v3 < subt1.v6) = (subt1.v5)\n";

        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  |  colocate: false, reason: \n"
                + "  |  equal join conjunct: 4: v4 = 1: v1\n"
                + "  |  equal join conjunct: 5: v5 = 2: v2\n"
                + "  |  other join predicates: CAST(CAST(3: v3 <= 3: v3 AS BIGINT) < 6: v6 AS BIGINT) = 5: v5\n"
                + "  |  use vectorized: true"));
    }

    @Test
    public void testInnerJoinPushDown() throws Exception {
        String sql = "SELECT *\n"
                + "FROM (\n"
                + "    SELECT t0.v1, t0.v2, t0.v3\n"
                + "    FROM t0\n"
                + ") subt0\n"
                + "    INNER JOIN (\n"
                + "        SELECT t1.v4, t1.v5, t1.v6\n"
                + "        FROM t1\n"
                + "    ) subt1\n"
                + "    ON subt0.v1 = subt1.v4\n"
                + "        AND subt0.v2 != subt0.v2\n"
                + "        AND subt0.v2 = subt1.v5\n"
                + "        AND (subt0.v3 <= subt0.v3 < subt1.v6) = (subt1.v5)\n";

        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  |  colocate: false, reason: \n"
                + "  |  equal join conjunct: 4: v4 = 1: v1\n"
                + "  |  equal join conjunct: 5: v5 = 2: v2\n"
                + "  |  other join predicates: CAST(CAST(3: v3 <= 3: v3 AS BIGINT) < 6: v6 AS BIGINT) = 5: v5\n"
                + "  |  use vectorized: true\n"));
    }

    @Test
    public void testCastFloat() throws Exception {
        String sql = "SELECT SUM(count) FROM (" +
                "SELECT CAST((CAST( ( CAST(CAST(t1.v4 AS BOOLEAN )  AS FLOAT )  ) >= ( t1.v5 )  AS BOOLEAN) = true)\n" +
                "AND (CAST( ( CAST(CAST(t1.v4 AS BOOLEAN )  AS FLOAT )  ) >= ( t1.v5 )  AS BOOLEAN) IS NOT NULL) AS "
                + "INT) "
                +
                "as count FROM t1) t;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("CAST(1: v4 AS BOOLEAN)"));
    }

    @Test
    public void testJoinCastFloat() throws Exception {
        String sql = "select * from t1, t3 right semi join test_all_type as a on t3.v1 = a.t1a and 1 > 2;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("equal join conjunct: 18: cast = 17: cast"));
    }

    @Test
    public void testCastUnCompatibleType1() throws Exception {
        String sql = "select CAST(CAST(CAST(t1e AS DATE) AS BOOLEAN) AS BOOLEAN) from test_all_type;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("CAST(CAST(5: t1e AS DATE) AS BOOLEAN)"));
    }

    @Test
    public void testCastUnCompatibleType2() throws Exception {
        String sql = "SELECT COUNT(*) FROM test_all_type WHERE CAST(CAST(t1e AS DATE) AS BOOLEAN);";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("CAST(CAST(5: t1e AS DATE) AS BOOLEAN)"));
    }

    @Test
    public void testLagWindowFunction() throws Exception {
        String sql = "select lag(id_datetime, 1, '2020-01-01') over(partition by t1c) from test_all_type;";
        String plan = getThriftPlan(sql);
        Assert.assertTrue(plan.contains("signature:lag(DATETIME, BIGINT, DATETIME)"));

        sql = "select lag(id_decimal, 1, 10000) over(partition by t1c) from test_all_type;";
        plan = getThriftPlan(sql);
        String expectSlice = "fn:TFunction(name:TFunctionName(function_name:lag), binary_type:BUILTIN," +
                " arg_types:[TTypeDesc(types:[TTypeNode(type:SCALAR, scalar_type:TScalarType(type:DECIMAL64," +
                " precision:10, scale:2))])], ret_type:TTypeDesc(types:[TTypeNode(type:SCALAR, " +
                "scalar_type:TScalarType(type:DECIMAL64, precision:10, scale:2))]), has_var_args:false, " +
                "signature:lag(DECIMAL64(10,2))";
        Assert.assertTrue(plan.contains(expectSlice));

        sql = "select lag(null, 1,1) OVER () from t0";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("functions: [, lag(NULL, 1, 1), ]"));

        sql = "select lag(id_datetime, 1, '2020-01-01xxx') over(partition by t1c) from test_all_type;";
        expectedEx.expect(SemanticException.class);
        expectedEx.expectMessage("The third parameter of `lag` can't not convert to DATETIME");
        getThriftPlan(sql);
    }

    @Test
    public void testPruneWindowColumn() throws Exception {
        String sql = "select sum(t1c) from (select t1c, lag(id_datetime, 1, '2020-01-01') over( partition by t1c)" +
                "from test_all_type) a ;";
        String plan = getFragmentPlan(sql);
        Assert.assertFalse(plan.contains("ANALYTIC"));
    }

    @Test
    public void testUnionAllConst() throws Exception {
        String sql = "select b from (select t1a as a, t1b as b, t1c as c, t1d as d from test_all_type " +
                "union all select 1 as a, 2 as b, 3 as c, 4 as d) t1;";
        String plan = getThriftPlan(sql);
        Assert.assertTrue(plan.contains(
                "const_expr_lists:[[TExpr(nodes:[TExprNode(node_type:INT_LITERAL, type:TTypeDesc(types:[TTypeNode"
                        + "(type:SCALAR, scalar_type:TScalarType(type:SMALLINT))]), num_children:0, "
                        + "int_literal:TIntLiteral"
                        + "(value:2), "
                        + "output_scale:-1, use_vectorized:true, has_nullable_child:false, is_nullable:false, "
                        + "is_monotonic:true)])]]"));
    }

    @Test
    public void testUnionEmpty() throws Exception {
        String sql =
                "SELECT DISTINCT RPAD('kZcD', 1300605171, '') FROM t0 WHERE false UNION ALL SELECT DISTINCT RPAD"
                        + "('kZcD', 1300605171, '') FROM t0 WHERE false IS NULL;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("0:UNION"));
    }

    @Test
    public void testWindowFunctionTest() throws Exception {
        String sql = "select sum(id_decimal - ifnull(id_decimal, 0)) over (partition by t1c) from test_all_type";
        String plan = getThriftPlan(sql);
        Assert.assertTrue(plan.contains("decimal_literal:TDecimalLiteral(value:0)"));
    }

    @Test
    public void testEquivalenceTest() throws Exception {
        String sql = "select * from t0 as x1 join t0 as x2 on x1.v2 = x2.v2 where x2.v2 = 'zxcv';";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  0:OlapScanNode\n"
                + "     TABLE: t0\n"
                + "     PREAGGREGATION: ON\n"
                + "     PREDICATES: CAST(2: v2 AS DOUBLE) = CAST('zxcv' AS DOUBLE)"));

        Assert.assertTrue(plan.contains("  1:OlapScanNode\n"
                + "     TABLE: t0\n"
                + "     PREAGGREGATION: ON\n"
                + "     PREDICATES: CAST(5: v2 AS DOUBLE) = CAST('zxcv' AS DOUBLE)\n"));
    }

    @Test
    public void testOuterJoinToInnerWithCast() throws Exception {
        String sql = "select * from test_all_type a left join test_all_type b on a.t1c = b.t1c " +
                "where b.id_date = '2021-05-19'";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("join op: INNER JOIN"));
    }

    @Test
    public void testCountStarWithLimitForOneAggStage() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        String sql = "select count(*) from (select v1 from t0 order by v2 limit 10,20) t;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("3:AGGREGATE (update finalize)"));
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    @Test
    public void testSingleTabletOutput() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        FeConstants.runningUnitTest = true;
        String sql = "select S_COMMENT from supplier;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains(" OUTPUT EXPRS:7: S_COMMENT\n"
                + "  PARTITION: RANDOM\n"
                + "\n"
                + "  RESULT SINK\n"
                + "\n"
                + "  0:OlapScanNode\n"
                + "     TABLE: supplier"));
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testSingleTabletOutput2() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        FeConstants.runningUnitTest = true;
        String sql = "select SUM(S_NATIONKEY) from supplier;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains(" OUTPUT EXPRS:9: sum(4: S_NATIONKEY)\n"
                + "  PARTITION: UNPARTITIONED\n"
                + "\n"
                + "  RESULT SINK\n"
                + "\n"
                + "  3:AGGREGATE (merge finalize)\n"
                + "  |  output: sum(9: sum(4: S_NATIONKEY))\n"
                + "  |  group by: \n"
                + "  |  use vectorized: true\n"));
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testJoinDecimalAndBool() throws Exception {
        String sql =
                "select t3.v1 from t3 inner join test_all_type on t3.v2 = test_all_type.id_decimal and t3.v2 > true";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  0:OlapScanNode\n"
                + "     TABLE: t3\n"
                + "     PREAGGREGATION: ON\n"
                + "     PREDICATES: 2: v2 > 1"));

        Assert.assertTrue(plan.contains("  2:OlapScanNode\n"
                + "     TABLE: test_all_type\n"
                + "     PREAGGREGATION: ON\n"
                + "     partitions=0/1\n"
                + "     rollup: test_all_type\n"));
    }

    @Test
    public void testCastExprAnalyze() throws Exception {
        String sql = "select AVG(DATEDIFF(curdate(),DATE_ADD(curdate(),interval -day(curdate())+1 day))) as a FROM t0";
        String plan = getFragmentPlan(sql);
        Assert.assertFalse(plan.contains("cast(curdate() as datetime)"));
    }

    @Test
    public void testPruneSortColumns() throws Exception {
        String sql = "select count(v1) from (select v1 from t0 order by v2 limit 10) t";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  3:Project\n" +
                "  |  <slot 1> : 1: v1"));
    }

    @Test
    public void testEquivalenceLoopDependency() throws Exception {
        String sql = "select * from t0 join t1 on t0.v1 = t1.v4 and cast(t0.v1 as STRING) = t0.v1";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("|  equal join conjunct: 1: v1 = 4: v4"));
        Assert.assertTrue(plan.contains("     TABLE: t0\n"
                + "     PREAGGREGATION: ON\n"
                + "     PREDICATES: CAST(CAST(1: v1 AS VARCHAR(65533)) AS DOUBLE) = CAST(1: v1 AS DOUBLE)\n"
                + "     partitions=0/1"));
    }

    @Test
    public void testSortWithLimitSubQuery() throws Exception {
        String sql = "select * from (select v1, v2 from t0 limit 10) a order by a.v1";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:EXCHANGE\n" +
                "     limit: 10"));

        sql = "select * from (select v1, v2 from t0 limit 10) a order by a.v1 limit 1000";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:EXCHANGE\n" +
                "     limit: 10"));

        sql = "select * from (select v1, v2 from t0 limit 10) a order by a.v1 limit 1";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:EXCHANGE\n" +
                "     limit: 10"));

        sql = "select * from (select v1, v2 from t0 limit 1) a order by a.v1 limit 10,1";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:EXCHANGE\n" +
                "     limit: 1"));
    }

    @Test
    public void testAggWithLimitSubQuery() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "select a.v1 from (select v1, v2 from t0 limit 10) a group by a.v1";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:EXCHANGE\n" +
                "     limit: 10"));

        sql = "select a.v2 from (select v1, v2 from t0 limit 10) a group by a.v2";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:EXCHANGE\n" +
                "     limit: 10"));

        sql = "select count(a.v2) from (select v1, v2 from t0 limit 10) a";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:EXCHANGE\n" +
                "     limit: 10"));

        sql = "select count(a.v2) from (select v1, v2 from t0 limit 10) a group by a.v2";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:EXCHANGE\n" +
                "     limit: 10"));
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testWindowWithLimitSubQuery() throws Exception {
        String sql = "select sum(a.v1) over(partition by a.v2) from (select v1, v2 from t0 limit 10) a";
        String plan = getFragmentPlan(sql);

        Assert.assertTrue(plan.contains("  1:EXCHANGE\n" +
                "     limit: 10"));

        sql = "select sum(a.v1) over(partition by a.v2 order by a.v1) from (select v1, v2 from t0 limit 10) a";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:EXCHANGE\n" +
                "     limit: 10"));

        sql = "select sum(a.v1) over() from (select v1, v2 from t0 limit 10) a";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:EXCHANGE\n"
                + "     limit: 10\n"));
    }

    @Test
    public void testJoinWithLimitSubQuery() throws Exception {
        String sql = "select * from (select v1, v2 from t0 limit 10) a join " +
                "(select v1, v2 from t0 limit 1) b";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("    EXCHANGE ID: 03\n" +
                "    UNPARTITIONED"));
        Assert.assertTrue(plan.contains("    EXCHANGE ID: 01\n"
                + "    UNPARTITIONED\n"));
    }

    @Test
    public void testJoinWithLimitSubQuery1() throws Exception {
        String sql = "select * from (select v1, v2 from t0 limit 10) a join [broadcast] " +
                "(select v1, v2 from t0 limit 1) b on a.v1 = b.v1";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("    EXCHANGE ID: 03\n" +
                "    UNPARTITIONED"));

    }

    @Test
    public void testJoinWithLimitSubQuery2() throws Exception {
        String sql = "select * from (select v1, v2 from t0) a join [broadcast] " +
                "(select v1, v2 from t0 limit 1) b on a.v1 = b.v1";
        String plan = getFragmentPlan(sql);

        Assert.assertTrue(plan.contains("    EXCHANGE ID: 02\n" +
                "    UNPARTITIONED"));
    }

    @Test
    public void testJoinWithLimitSubQuery3() throws Exception {
        String sql = "select * from (select v1, v2 from t0 limit 10) a join [shuffle] " +
                "(select v1, v2 from t0 limit 1) b on a.v1 = b.v1";
        String plan = getFragmentPlan(sql);
        System.out.println(plan);
        Assert.assertTrue(plan.contains("join op: INNER JOIN (PARTITIONED)"));
        Assert.assertTrue(plan.contains("  |----5:EXCHANGE\n" +
                "  |       limit: 1"));
        Assert.assertTrue(plan.contains("  2:EXCHANGE\n" +
                "     limit: 10"));
    }

    @Test
    public void testJoinWithLimitSubQuery4() throws Exception {
        String sql = "select * from (select v1, v2 from t0) a join [shuffle] " +
                "(select v4 from t1 limit 1) b on a.v1 = b.v4";
        String plan = getFragmentPlan(sql);
        System.out.println(plan);
        Assert.assertTrue(plan.contains("join op: INNER JOIN (PARTITIONED)"));
    }

    @Test
    public void testJoinWithLimitSubQuery5() throws Exception {
        String sql = "select * from (select v1, v2 from t0 limit 10) a join [shuffle] " +
                "(select v4 from t1 ) b on a.v1 = b.v4";
        String plan = getFragmentPlan(sql);
        System.out.println(plan);
        Assert.assertTrue(plan.contains("join op: INNER JOIN (PARTITIONED)"));
    }

    @Test
    public void testUnionWithLimitSubQuery() throws Exception {
        String sql = "select v1, v2 from t0 limit 10 union all " +
                "select v1, v2 from t0 limit 1 ";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("    EXCHANGE ID: 02\n" +
                "    UNPARTITIONED"));
        Assert.assertTrue(plan.contains("    EXCHANGE ID: 05\n" +
                "    UNPARTITIONED"));

        sql = "select v1, v2 from t0 union all " +
                "select a.v1, a.v2 from (select v1, v2 from t0 limit 1) a ";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("    EXCHANGE ID: 04\n" +
                "    UNPARTITIONED"));

        sql = "select v1, v2 from t0 limit 10 union all " +
                "select v1, v2 from t0";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("    EXCHANGE ID: 02\n" +
                "    UNPARTITIONED"));
    }

    @Test
    public void testSubqueryGatherJoin() throws Exception {
        String sql = "select t1.v5 from (select * from t0 limit 1) as x inner join t1 on x.v1 = t1.v4";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains(" OUTPUT EXPRS:\n"
                + "  PARTITION: RANDOM\n"
                + "\n"
                + "  STREAM DATA SINK\n"
                + "    EXCHANGE ID: 02\n"
                + "    UNPARTITIONED\n"
                + "\n"
                + "  1:OlapScanNode\n"
                + "     TABLE: t0"));
    }

    @Test
    public void testSubqueryBroadJoin() throws Exception {
        String sql = "select t1.v5 from t0 inner join[broadcast] t1 on cast(t0.v1 as int) = cast(t1.v4 as int)";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  |  equal join conjunct: 7: cast = 8: cast\n"
                + "  |  use vectorized: true\n"));
        Assert.assertTrue(plan.contains("<slot 7> : CAST(1: v1 AS INT)"));
        Assert.assertTrue(plan.contains("<slot 8> : CAST(4: v4 AS INT)"));
    }

    @Test
    public void testMergeLimitForFilterNode() throws Exception {
        String sql =
                "SELECT CAST(nullif(subq_0.c1, subq_0.c1) AS INTEGER) AS c0, subq_0.c0 AS c1, 42 AS c2, subq_0.c0 AS "
                        + "c3, subq_0.c1 AS c4\n"
                        +
                        "\t, subq_0.c0 AS c5, subq_0.c0 AS c6\n" +
                        "FROM (\n" +
                        "\tSELECT ref_2.v8 AS c0, ref_2.v8 AS c1\n" +
                        "\tFROM t2 ref_0\n" +
                        "\t\tRIGHT JOIN t1 ref_1 ON ref_0.v7 = ref_1.v4\n" +
                        "\t\tRIGHT JOIN t2 ref_2 ON ref_1.v4 = ref_2.v7\n" +
                        "\tWHERE ref_1.v4 IS NOT NULL\n" +
                        "\tLIMIT 110\n" +
                        ") subq_0\n" +
                        "WHERE CAST(coalesce(true, true) AS BOOLEAN) < true\n" +
                        "LIMIT 157";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("10:SELECT\n" +
                "  |  predicates: coalesce(TRUE, TRUE) < TRUE\n" +
                "  |  limit: 157"));
    }

    @Test
    public void testSortProject() throws Exception {
        String sql = "select avg(null) over (order by ref_0.v1) as c2 "
                + "from t0 as ref_0 left join t1 as ref_1 on (ref_0.v1 = ref_1.v4 );";
        String plan = getThriftPlan(sql);
        Assert.assertTrue(plan.contains(
                "sort_tuple_slot_exprs:[TExpr(nodes:[TExprNode(node_type:SLOT_REF, type:TTypeDesc(types:[TTypeNode"
                        + "(type:SCALAR, scalar_type:TScalarType(type:BIGINT))]), num_children:0, slot_ref:TSlotRef"
                        + "(slot_id:1, tuple_id:2), output_scale:-1, output_column:-1, use_vectorized:true, "
                        + "has_nullable_child:false, is_nullable:true, is_monotonic:true)])]"));
    }

    @Test
    public void testScalarRewrite() throws Exception {
        String sql = "select t0.v1, case when true then t0.v1 else t0.v1 end from t0;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains(" OUTPUT EXPRS:1: v1 | 1: v1\n"));
    }

    @Test
    public void testScalarRewrite2() throws Exception {
        String sql = "select j.x1, j.x2 from "
                + "(select t0.v1 as x1, case when true then t0.v1 else t0.v1 end as x2, t0.v3 as x3 from t0 limit 10)"
                + " as j "
                + "where j.x3 > 1;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  2:Project\n"
                + "  |  <slot 1> : 1: v1\n"
                + "  |  use vectorized: true\n"
                + "  |  \n"
                + "  1:SELECT\n"
                + "  |  predicates: 3: v3 > 1\n"
                + "  |  use vectorized: true\n"
                + "  |  \n"
                + "  0:OlapScanNode\n"
                + "     TABLE: t0\n"));
    }

    @Test
    public void testJoinLimit() throws Exception {
        String sql;
        String plan;
        sql = "select * from t0 inner join t1 on t0.v1 = t1.v4 limit 10";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  |  join op: INNER JOIN (BROADCAST)\n"
                + "  |  hash predicates:\n"
                + "  |  colocate: false, reason: \n"
                + "  |  equal join conjunct: 1: v1 = 4: v4\n"
                + "  |  limit: 10\n"
                + "  |  use vectorized: true\n"
                + "  |  \n"
                + "  |----2:EXCHANGE\n"
                + "  |       use vectorized: true\n"
                + "  |    \n"
                + "  0:OlapScanNode\n"
                + "     TABLE: t0"));

        sql = "select * from t0 left anti join t1 on t0.v1 = t1.v4 limit 10";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  |  join op: LEFT ANTI JOIN (BROADCAST)\n"
                + "  |  hash predicates:\n"
                + "  |  colocate: false, reason: \n"
                + "  |  equal join conjunct: 1: v1 = 4: v4\n"
                + "  |  limit: 10\n"
                + "  |  use vectorized: true\n"
                + "  |  \n"
                + "  |----2:EXCHANGE\n"
                + "  |       use vectorized: true\n"
                + "  |    \n"
                + "  0:OlapScanNode\n"
                + "     TABLE: t0\n"));

        sql = "select * from t0 right semi join t1 on t0.v1 = t1.v4 limit 10";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  |  join op: LEFT SEMI JOIN (BROADCAST)\n"
                + "  |  hash predicates:\n"
                + "  |  colocate: false, reason: \n"
                + "  |  equal join conjunct: 4: v4 = 1: v1\n"
                + "  |  limit: 10\n"
                + "  |  use vectorized: true\n"
                + "  |  \n"
                + "  |----2:EXCHANGE\n"
                + "  |       use vectorized: true\n"
                + "  |    \n"
                + "  0:OlapScanNode\n"
                + "     TABLE: t1\n"));
    }

    @Test
    public void testJoinLimitLeft() throws Exception {
        String sql = "select * from t0 left outer join t1 on t0.v1 = t1.v4 limit 10";
        String plan = getFragmentPlan(sql);
        System.out.println(plan);
        Assert.assertTrue(plan.contains("  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 4: v4\n" +
                "  |  limit: 10\n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  |----2:EXCHANGE\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  0:OlapScanNode"));
        Assert.assertTrue(plan.contains("     TABLE: t0\n"
                + "     PREAGGREGATION: ON\n"
                + "     partitions=0/1\n"
                + "     rollup: t0\n"
                + "     tabletRatio=0/0\n"
                + "     tabletList=\n"
                + "     cardinality=1\n"
                + "     avgRowSize=3.0\n"
                + "     numNodes=0\n"
                + "     limit: 10"));
    }

    @Test
    public void testJoinLimitFull() throws Exception {
        String sql;
        String plan;
        sql = "select * from t0 full outer join t1 on t0.v1 = t1.v4 limit 10";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  4:HASH JOIN\n"
                + "  |  join op: FULL OUTER JOIN (PARTITIONED)\n"
                + "  |  hash predicates:\n"
                + "  |  colocate: false, reason: \n"
                + "  |  equal join conjunct: 1: v1 = 4: v4\n"
                + "  |  limit: 10\n"
                + "  |  use vectorized: true\n"
                + "  |  \n"
                + "  |----3:EXCHANGE\n"
                + "  |       limit: 10\n"
                + "  |       use vectorized: true\n"
                + "  |    \n"
                + "  1:EXCHANGE\n"
                + "     limit: 10\n"
                + "     use vectorized: true"));

        sql = "select * from t0, t1 limit 10";
        plan = getFragmentPlan(sql);
        System.out.println(plan);
        Assert.assertTrue(plan.contains("3:CROSS JOIN\n" +
                "  |  cross join:\n" +
                "  |  predicates is NULL.\n" +
                "  |  limit: 10\n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  |----2:EXCHANGE\n" +
                "  |       limit: 10\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0"));
    }

    @Test
    public void testAggregateConst() throws Exception {
        String sql = "select 'a', v2, sum(v1) from t0 group by 'a', v2; ";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  2:Project\n"
                + "  |  <slot 2> : 2: v2\n"
                + "  |  <slot 5> : 5: sum(1: v1)\n"
                + "  |  <slot 6> : 'a'\n"
                + "  |  use vectorized: true\n"
                + "  |  \n"
                + "  1:AGGREGATE (update finalize)\n"
                + "  |  output: sum(1: v1)\n"
                + "  |  group by: 2: v2\n"
                + "  |  use vectorized: true"));
    }

    @Test
    public void testAggregateAllConst() throws Exception {
        String sql = "select 'a', 'b' from t0 group by 'a', 'b'; ";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  3:Project\n"
                + "  |  <slot 4> : 4: expr\n"
                + "  |  <slot 6> : 'b'\n"
                + "  |  use vectorized: true\n"
                + "  |  \n"
                + "  2:AGGREGATE (update finalize)\n"
                + "  |  group by: 4: expr\n"
                + "  |  use vectorized: true\n"
                + "  |  \n"
                + "  1:Project\n"
                + "  |  <slot 4> : 'a'\n"
                + "  |  use vectorized: true\n"
                + "  |  \n"
                + "  0:OlapScanNode\n"
                + "     TABLE: t0"));
    }

    @Test
    public void testSupersetEnforce() throws Exception {
        String sql = "select * from (select v3, rank() over (partition by v1 order by v2) as j1 from t0) as x0 "
                + "join t1 on x0.v3 = t1.v4 order by x0.v3, t1.v4 limit 100;";
        getFragmentPlan(sql);
    }

    @Test
    public void testMergeProject() throws Exception {
        String sql = "select case when v1 then 2 else 2 end from (select v1, case when true then v1 else v1 end as c2"
                + " from t0 limit 1) as x where c2 > 2 limit 2;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  2:Project\n"
                + "  |  <slot 4> : 2\n"
                + "  |  limit: 2\n"
                + "  |  use vectorized: true\n"
                + "  |  \n"
                + "  1:SELECT\n"
                + "  |  predicates: 1: v1 > 2\n"
                + "  |  limit: 2\n"
                + "  |  use vectorized: true\n"
                + "  |  \n"
                + "  0:OlapScanNode\n"
                + "     TABLE: t0"));
    }

    @Test
    public void testUsingJoin() throws Exception {
        String sql = "select * from t0 as x0 join t0 as x1 using(v1);";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  2:HASH JOIN\n"
                + "  |  join op: INNER JOIN (COLOCATE)\n"
                + "  |  hash predicates:\n"
                + "  |  colocate: true\n"
                + "  |  equal join conjunct: 1: v1 = 4: v1"));
    }

    @Test(expected = SemanticException.class)
    public void testArithCastCheck() throws Exception {
        String sql = "select v1 + h1 from test_object;";
        getFragmentPlan(sql);
    }

    @Test
    public void testNullSafeEqualJoin() throws Exception {
        String sql = "select * from t0 join t1 on t0.v3 <=> t1.v4";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("equal join conjunct: 3: v3 <=> 4: v4"));

        sql = "select * from t0 left join t1 on t0.v3 <=> t1.v4";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("equal join conjunct: 3: v3 <=> 4: v4"));
    }

    @Test
    public void testColocateHint() throws Exception {
        String sql = "select * from t0 as x0 inner join t0 as x1 on x0.v1 = x1.v1;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  |  join op: INNER JOIN (COLOCATE)\n"
                + "  |  hash predicates:\n"
                + "  |  colocate: true"));

        sql = "select * from t0 as x0 inner join[shuffle] t0 as x1 on x0.v1 = x1.v1;";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  |  join op: INNER JOIN (PARTITIONED)\n"
                + "  |  hash predicates:\n"
                + "  |  colocate: false, reason: "));

        sql = "select * from t0 as x0 inner join[colocate] t0 as x1 on x0.v1 = x1.v1;";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  |  join op: INNER JOIN (COLOCATE)\n"
                + "  |  hash predicates:\n"
                + "  |  colocate: true"));
    }

    @Test
    public void testBucketHint() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "select * from t0 as x0 inner join t1 as x1 on x0.v1 = x1.v4;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n"
                + "  |  hash predicates:\n"
                + "  |  colocate: false, reason: "));

        sql = "select * from t0 as x0 inner join[shuffle] t1 as x1 on x0.v1 = x1.v4;";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  |  join op: INNER JOIN (PARTITIONED)\n"
                + "  |  hash predicates:\n"
                + "  |  colocate: false, reason: "));

        sql = "select * from t0 as x0 inner join[bucket] t1 as x1 on x0.v1 = x1.v4;";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n"
                + "  |  hash predicates:\n"
                + "  |  colocate: false, reason: "));
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testInformationSchema() throws Exception {
        String sql = "select column_name from information_schema.columns limit 1;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  RESULT SINK\n" +
                "\n" +
                "  0:SCAN SCHEMA\n" +
                "     limit: 1\n" +
                "     use vectorized: true"));
    }

    @Test
    public void testInformationSchema1() throws Exception {
        String sql = "select column_name, UPPER(DATA_TYPE) from information_schema.columns;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:Project\n"
                + "  |  <slot 4> : 4: COLUMN_NAME\n"
                + "  |  <slot 25> : upper(8: DATA_TYPE)\n"
                + "  |  use vectorized: true\n"
                + "  |  \n"
                + "  0:SCAN SCHEMA\n"
                + "     use vectorized: true"));
    }

    @Test
    public void testJoinOnInDatePredicate() throws Exception {
        String sql =
                "select a.id_datetime from test_all_type as a join test_all_type as b where a.id_date in (b.id_date)";
        String plan = getFragmentPlan(sql);
        Assert.assertFalse(plan.contains("CAST(9: id_date AS DATETIME)"));
        Assert.assertTrue(plan.contains("equal join conjunct: 9: id_date = 19: id_date"));
    }

    @Test
    public void testDecimalV3LiteralCast() throws Exception {
        String sql =
                "select id_datetime from test_all_type WHERE CAST(IF(true, 0.38542880072101215, '-Inf')  AS BOOLEAN )";
        String thrift = getThriftPlan(sql);
        Assert.assertTrue(thrift.contains("string_literal:TStringLiteral(value:0.38542880072101215)"));
    }

    @Test
    public void testMysqlTableFilter() throws Exception {
        String sql = "select * from ods_order where order_dt = '2025-08-07' and order_no = 'p' limit 10;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("0:SCAN MYSQL\n" +
                "     TABLE: `ods_order`\n" +
                "     Query: SELECT `order_dt`, `order_no`, `org_order_no`, `bank_transaction_id`, `up_trade_no`, `mchnt_no`, `pay_st` FROM `ods_order` WHERE (order_dt = '2025-08-07') AND (order_no = 'p')\n" +
                "     limit: 10"));
    }

    @Test
    public void testMysqlTableAggregateSort() throws Exception {
        String sql = "select order_dt,order_no,sum(pay_st) from ods_order where order_dt = '2025-08-07' group by " +
                "order_dt,order_no order by order_no limit 10;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("2:TOP-N\n" +
                "  |  order by: <slot 2> 2: order_no ASC\n" +
                "  |  offset: 0\n" +
                "  |  limit: 10\n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(pay_st)\n" +
                "  |  group by: order_dt, order_no\n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  0:SCAN MYSQL\n" +
                "     TABLE: `ods_order`\n" +
                "     Query: SELECT `order_dt`, `order_no`, `pay_st` FROM `ods_order` WHERE (order_dt = '2025-08-07')"));
    }

    @Test
    public void testMysqlTableJoin() throws Exception {
        String sql = "select order_dt,order_no,sum(pay_st) from ods_order join test_all_type on order_no = t1a where " +
                "order_dt = '2025-08-07' group by order_dt,order_no order by order_no limit 10;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("3:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: order_no = 8: t1a\n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  |----2:EXCHANGE\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  0:SCAN MYSQL\n" +
                "     TABLE: `ods_order`\n" +
                "     Query: SELECT `order_dt`, `order_no`, `pay_st` FROM `ods_order` WHERE (order_dt = '2025-08-07')"));
    }

    @Test
    public void testMysqlPredicateWithoutCast() throws Exception {
        String sql = "select * from ods_order where pay_st = 214748364;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains(
                "Query: SELECT `order_dt`, `order_no`, `org_order_no`, `bank_transaction_id`, `up_trade_no`, `mchnt_no`, `pay_st` FROM `ods_order` WHERE (pay_st = 214748364)"));
    }

    @Test
    public void testSqlSelectLimitSession() throws Exception {
        connectContext.getSessionVariable().setSqlSelectLimit(10);
        String sql = "select * from test_all_type";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("limit: 10"));

        connectContext.getSessionVariable().setSqlSelectLimit(10);
        sql = "select * from test_all_type limit 20000";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("limit: 20000"));

        connectContext.getSessionVariable().setSqlSelectLimit(SessionVariable.DEFAULT_SELECT_LIMIT);
        sql = "select * from test_all_type";
        plan = getFragmentPlan(sql);
        Assert.assertFalse(plan.contains("limit: 10"));

        connectContext.getSessionVariable().setSqlSelectLimit(8888);
        sql = "select * from (select * from test_all_type limit 10) as a join " +
                "(select * from test_all_type limit 100) as b";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("limit: 8888"));
        connectContext.getSessionVariable().setSqlSelectLimit(SessionVariable.DEFAULT_SELECT_LIMIT);

        connectContext.getSessionVariable().setSqlSelectLimit(-100);
        sql = "select * from test_all_type";
        plan = getFragmentPlan(sql);
        Assert.assertFalse(plan.contains("limit"));

        connectContext.getSessionVariable().setSqlSelectLimit(0);
        sql = "select * from test_all_type";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("EMPTYSET"));
        connectContext.getSessionVariable().setSqlSelectLimit(SessionVariable.DEFAULT_SELECT_LIMIT);
    }

    @Test
    public void testBetweenDate() throws Exception {
        String sql = "select * from test_all_type where id_date between '2020-12-12' and '2021-12-12'";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("PREDICATES: 9: id_date >= '2020-12-12', 9: id_date <= '2021-12-12'"));
    }

    @Test
    public void testOrderBySameColumnDiffOrder() throws Exception {
        String sql = "select v1 from t0 order by v1 desc, v1 asc";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("1:SORT\n" +
                "  |  order by: <slot 1> 1: v1 DESC"));
    }

    @Test
    public void testMysqlTableWithPredicate() throws Exception {
        String sql = "select max(order_dt) over (partition by order_no) from ods_order where order_no > 1";
        String plan = getThriftPlan(sql);
        Assert.assertFalse(plan.contains("use_vectorized:false"));
    }

    @Test
    public void testMysqlJoinSelf() throws Exception {
        String sql = "SELECT ref_0.order_dt AS c0\n" +
                "  FROM ods_order ref_0\n" +
                "    LEFT JOIN ods_order ref_1 ON ref_0.order_dt = ref_1.order_dt\n" +
                "  WHERE ref_1.order_no IS NOT NULL;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("4:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)"));
    }

    @Test
    public void testCastType() throws Exception {
        String sql = "select * from test_all_type where t1a = 123 AND t1b = 999999999 AND t1d = 999999999 "
                + "AND id_datetime = '2020-12-20 20:20:20' AND id_date = '2020-12-11' AND id_datetime = 'asdlfkja';";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("1: t1a = '123', CAST(2: t1b AS INT) = 999999999, 4: t1d = 999999999, "
                + "8: id_datetime = '2020-12-20 20:20:20', 9: id_date = '2020-12-11', "
                + "8: id_datetime = CAST('asdlfkja' AS DATETIME)"));
    }

    @Test
    public void testCountDistinctWithMultiColumns() throws Exception {
        String sql = "select count(distinct t1b,t1c) from test_all_type";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("6:AGGREGATE (merge finalize)"));
        Assert.assertTrue(plan.contains("4:AGGREGATE (update serialize)\n" +
                "  |  output: count(if(2: t1b IS NULL, NULL, 3: t1c))"));
    }

    @Test
    public void testCountDistinctWithIfNested() throws Exception {
        String sql = "select count(distinct t1b,t1c,t1d) from test_all_type";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("output: count(if(2: t1b IS NULL, NULL, if(3: t1c IS NULL, NULL, 4: t1d)))"));

        sql = "select count(distinct t1b,t1c,t1d,t1e) from test_all_type group by t1f";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains(
                "output: count(if(2: t1b IS NULL, NULL, if(3: t1c IS NULL, NULL, if(4: t1d IS NULL, NULL, 5: t1e))))"));
    }

    @Test
    public void testCountDistinctGroupByWithMultiColumns() throws Exception {
        String sql = "select count(distinct t1b,t1c) from test_all_type group by t1d";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("4:AGGREGATE (update finalize)\n" +
                "  |  output: count(if(2: t1b IS NULL, NULL, 3: t1c))"));
    }

    @Test
    public void testCountDistinctWithDiffMultiColumns() throws Exception {
        String sql = "select count(distinct t1b,t1c), count(distinct t1b,t1d) from test_all_type";
        try {
            getFragmentPlan(sql);
        } catch (StarRocksPlannerException e) {
            Assert.assertEquals(
                    "The query contains multi count distinct or sum distinct, each can't have multi columns.",
                    e.getMessage());
        }
    }

    @Test
    public void testCountDistinctWithSameMultiColumns() throws Exception {
        String sql = "select count(distinct t1b,t1c), count(distinct t1b,t1c) from test_all_type";
        String plan = getFragmentPlan(sql);
        System.out.println(plan);
        Assert.assertTrue(plan.contains("6:AGGREGATE (merge finalize)"));

        sql = "select count(distinct t1b,t1c), count(distinct t1b,t1c) from test_all_type group by t1d";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("4:AGGREGATE (update finalize)"));
    }

    @Test
    public void testMultiScalarSubquery() throws Exception {
        String sql = "SELECT CASE \n"
                + "    WHEN (SELECT count(*) FROM t1 WHERE v4 BETWEEN 1 AND 20) > 74219\n"
                + "    THEN ( \n"
                + "        SELECT avg(v7) FROM t2 WHERE v7 BETWEEN 1 AND 20\n"
                + "        )\n"
                + "    ELSE (\n"
                + "        SELECT avg(v8) FROM t2 WHERE v8 BETWEEN 1 AND 20\n"
                + "        ) END AS bucket1\n"
                + "FROM t0\n"
                + "WHERE v1 = 1;";
        String plan = getFragmentPlan(sql);
        Assert.assertNotNull(plan);
    }

    @Test
    public void testWindowDuplicatedColumnInPartitionExprAndOrderByExpr() throws Exception {
        String sql = "select v1, sum(v2) over (partition by v1, v2 order by v2 desc) as sum1 from t0";
        String plan = getFragmentPlan(sql);
        Assert.assertNotNull(plan);
    }

    @Test
    public void testSelectDistinctWithOrderBy() throws Exception {
        String sql = "select distinct v1 from tarray order by v1+1";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("2:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 4> : 1: v1 + 1"));
    }

    @Test
    public void testSelectDistinctWithOrderBy2() throws Exception {
        String sql = "select distinct v1+1 as v from tarray order by v+1";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("3:Project\n" +
                "  |  <slot 4> : 4: expr\n" +
                "  |  <slot 5> : 4: expr + 1\n" +
                "  |  use vectorized: true"));
        Assert.assertTrue(plan.contains("1:Project\n" +
                "  |  <slot 4> : 1: v1 + 1"));
    }

    @Test
    public void testSelectArrayElement() throws Exception {
        String sql = "select [1,2][1]";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("ARRAY<tinyint(4)>[1,2][1]"));

        sql = "select [][1]";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("ARRAY<unknown type: NULL_TYPE>[][1]"));
    }

    @Test
    public void testSelectMultidimensionalArray() throws Exception {
        String sql = "select [[1,2],[3,4]][1][2]";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("ARRAY<ARRAY<tinyint(4)>>[[1,2],[3,4]][1][2]"));
    }

    @Test
    public void testSelectArrayElementFromArrayColumn() throws Exception {
        String sql = "select v3[1] from tarray";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("1:Project\n" +
                "  |  <slot 4> : 3: v3[1]"));
    }

    @Test
    public void testArrayElementWithFunction() throws Exception {
        String sql = "select v1, sum(v3[1]) from tarray group by v1";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("1:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 4> : 3: v3[1]"));
    }

    @Test
    public void testArrayCountDistinctWithOrderBy() throws Exception {
        String sql = "select distinct v3 from tarray order by v3[1];";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("2:Project\n" +
                "  |  <slot 3> : 3: v3\n" +
                "  |  <slot 4> : 3: v3[1]"));
    }

    @Test
    public void testArrayElementExpr() throws Exception {
        String sql = "select [][1] + 1, [1,2,3][1] + [[1,2,3],[1,1,1]][2][2]";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains(
                "NULL | CAST(ARRAY<tinyint(4)>[1,2,3][1] AS BIGINT) + CAST(ARRAY<ARRAY<tinyint(4)>>[[1,2,3],[1,1,1]][2][2] AS BIGINT)"));

        sql = "select v1, v3[1] + [1,2,3][1] as v, sum(v3[1]) from tarray group by v1, v order by v";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("2:AGGREGATE (update finalize)\n" +
                "  |  output: sum(5: expr)\n" +
                "  |  group by: 1: v1, 4: expr\n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 4> : 3: v3[1] + CAST(ARRAY<tinyint(4)>[1,2,3][1] AS BIGINT)\n" +
                "  |  <slot 5> : 3: v3[1]\n" +
                "  |  use vectorized: true"));
    }

    @Test
    public void testSetVar() throws Exception {
        String sql = "select * from db1.tbl3 as t1 JOIN db1.tbl4 as t2 ON t1.c2 = t2.c2";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("join op: INNER JOIN (BROADCAST)"));

        sql = "select /*+ SET_VAR(broadcast_row_limit=0) */ * from db1.tbl3 as t1 JOIN db1.tbl4 as t2 ON t1.c2 = t2.c2";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("join op: INNER JOIN (PARTITIONED)"));
    }

    @Test
    public void testAggregateTwoLevelToOneLevelOptimization() throws Exception {
        String sql = "SELECT c2, count(*) FROM db1.tbl3 WHERE c1<10 GROUP BY c2;";
        String plan = getFragmentPlan(sql);
        Assert.assertEquals(1, StringUtils.countMatches(plan, "AGGREGATE (update finalize)"));

        sql = " SELECT c2, count(*) FROM (SELECT t1.c2 as c2 FROM db1.tbl3 as t1 INNER JOIN [shuffle] db1.tbl4 " +
                "as t2 ON t1.c2=t2.c2 WHERE t1.c1<10) as t3 GROUP BY c2;";
        plan = getFragmentPlan(sql);

        Assert.assertEquals(1, StringUtils.countMatches(plan, "AGGREGATE (merge finalize)"));

        sql = "SELECT c2, count(*) FROM db1.tbl5 GROUP BY c2;";
        plan = getFragmentPlan(sql);
        Assert.assertEquals(1, StringUtils.countMatches(plan, "AGGREGATE (update finalize)"));

        sql = "SELECT c3, count(*) FROM db1.tbl4 GROUP BY c3;";
        plan = getFragmentPlan(sql);
        Assert.assertEquals(1, StringUtils.countMatches(plan, "AGGREGATE (update finalize)"));
    }

    @Test
    public void testExplicitlyBroadcastJoin() throws Exception {
        String sql = "select * from db1.tbl1 join [BROADCAST] db1.tbl2 on tbl1.k1 = tbl2.k3";
        String plan = getFragmentPlan(sql);
        Assert.assertEquals(1, StringUtils.countMatches(plan, "INNER JOIN (BROADCAST)"));

        sql = "select * from db1.tbl1 join [SHUFFLE] db1.tbl2 on tbl1.k1 = tbl2.k3";
        plan = getFragmentPlan(sql);
        Assert.assertEquals(1, StringUtils.countMatches(plan, "INNER JOIN (PARTITIONED)"));
    }

    @Test
    public void testWindowDuplicatePartition() throws Exception {
        String sql = "select max(v3) over (partition by v2,v2,v2 order by v2,v2) from t0;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  2:SORT\n"
                + "  |  order by: <slot 2> 2: v2 ASC\n"
                + "  |  offset: 0"));

    }

    @Test
    public void testBitmapQuery() throws Exception {
        starRocksAssert.query(
                "select * from test.bitmap_table;").explainContains(
                "OUTPUT EXPRS:1: id | 2: id2"
        );

        starRocksAssert.query("select count(id2) from test.bitmap_table;")
                .explainContains("OUTPUT EXPRS:3: count(2: id2)",
                        "1:AGGREGATE (update finalize)", "output: count(2: id2)", "group by:", "0:OlapScanNode",
                        "PREAGGREGATION: OFF. Reason: Aggregate Operator not match: COUNT <--> BITMAP_UNION");

        starRocksAssert.query("select group_concat(id2) from test.bitmap_table;")
                .analysisError(
                        "group_concat requires first parameter to be of getType() STRING: group_concat(`default_cluster:test`.`bitmap_table`.`id2`)");

        starRocksAssert.query("select sum(id2) from test.bitmap_table;").analysisError(
                "sum requires a numeric parameter: sum(`default_cluster:test`.`bitmap_table`.`id2`)");

        starRocksAssert.query("select avg(id2) from test.bitmap_table;")
                .analysisError("avg requires a numeric parameter: avg(`default_cluster:test`.`bitmap_table`.`id2`)");

        starRocksAssert.query("select max(id2) from test.bitmap_table;").analysisError(Type.OnlyMetricTypeErrorMsg);

        starRocksAssert.query("select min(id2) from test.bitmap_table;").analysisError(Type.OnlyMetricTypeErrorMsg);

        starRocksAssert.query("select count(*) from test.bitmap_table group by id2;")
                .analysisError(Type.OnlyMetricTypeErrorMsg);

        starRocksAssert.query("select count(*) from test.bitmap_table where id2 = 1;").analysisError(
                "binary type bitmap with type double is invalid.");
    }

    @Test
    public void testHLLTypeQuery() throws Exception {
        starRocksAssert.query("select * from test.hll_table;").explainContains(
                "OUTPUT EXPRS:1: id | 2: id2");

        starRocksAssert.query("select count(id2) from test.hll_table;").explainContains("OUTPUT EXPRS:3: count(2: id2)",
                "1:AGGREGATE (update finalize)", "output: count(2: id2)", "group by:", "0:OlapScanNode",
                "PREAGGREGATION: OFF. Reason: Aggregate Operator not match: COUNT <--> HLL_UNION");

        starRocksAssert.query("select group_concat(id2) from test.hll_table;")
                .analysisError("No matching function with signature: group_concat(hll).");

        starRocksAssert.query("select sum(id2) from test.hll_table;")
                .analysisError("No matching function with signature: sum(hll).");

        starRocksAssert.query("select avg(id2) from test.hll_table;")
                .analysisError("No matching function with signature: avg(hll).");

        starRocksAssert.query("select max(id2) from test.hll_table;").analysisError(Type.OnlyMetricTypeErrorMsg);

        starRocksAssert.query("select min(id2) from test.hll_table;").analysisError(Type.OnlyMetricTypeErrorMsg);

        starRocksAssert.query("select min(id2) from test.hll_table;").analysisError(Type.OnlyMetricTypeErrorMsg);

        starRocksAssert.query("select count(*) from test.hll_table group by id2;")
                .analysisError(Type.OnlyMetricTypeErrorMsg);

        starRocksAssert.query("select count(*) from test.hll_table where id2 = 1").analysisError(
                "binary type hll with type double is invalid.");
    }

    @Test
    public void testCountDistinctRewrite() throws Exception {
        String sql = "select count(distinct id) from test.bitmap_table";
        starRocksAssert.query(sql).explainContains("count(distinct 1: id)", "multi_distinct_count(1: id)");

        sql = "select count(distinct id2) from test.bitmap_table";
        starRocksAssert.query(sql).explainContains("count(distinct 2: id2)", "bitmap_union_count(2: id2)");

        sql = "select sum(id) / count(distinct id2) from test.bitmap_table";
        starRocksAssert.query(sql).explainContains("output: sum(1: id), bitmap_union_count(2: id2)");

        sql = "select count(distinct id2) from test.hll_table";
        starRocksAssert.query(sql).explainContains("hll_union_agg(2: id2)", "count(distinct 2: id2)");

        sql = "select sum(id) / count(distinct id2) from test.hll_table";
        starRocksAssert.query(sql).explainContains("sum(1: id), hll_union_agg(2: id2)");

        sql = "select count(distinct id2) from test.bitmap_table group by id order by count(distinct id2)";
        starRocksAssert.query(sql).explainContains();

        sql = "select count(distinct id2) from test.bitmap_table having count(distinct id2) > 0";
        starRocksAssert.query(sql)
                .explainContains("bitmap_union_count(2: id2)", "having: 3: count(distinct 2: id2) > 0");

        sql = "select count(distinct id2) from test.bitmap_table order by count(distinct id2)";
        starRocksAssert.query(sql).explainContains("3: count(distinct 2: id2)", "3:MERGING-EXCHANGE",
                "order by: <slot 3> 3: count(distinct 2: id2) ASC",
                "output: bitmap_union_count(2: id2)");
    }

    @Test
    public void testDateTypeCastSyntax() throws Exception {
        String castSql = "select * from test.baseall where k11 < cast('2020-03-26' as date)";
        starRocksAssert.query(castSql).explainContains("8: k11 < '2020-03-26 00:00:00'");

        String castSql2 = "select str_to_date('11/09/2011', '%m/%d/%Y');";
        starRocksAssert.query(castSql2).explainContains("constant exprs:", "'2011-11-09 00:00:00'");
    }

    @Test
    public void testSetOperation() throws Exception {
        // union
        String sql1 = "select * from\n"
                + "  (select k1, k2 from db1.tbl6\n"
                + "   union all\n"
                + "   select k1, k2 from db1.tbl6) a\n"
                + "  inner join\n"
                + "  db1.tbl6 b\n"
                + "  on (a.k1 = b.k1)\n"
                + "where b.k1 = 'a'";
        starRocksAssert.query(sql1).explainContains("UNION", 1);

        String sql2 = "select * from db1.tbl6 where k1='a' and k4=1\n"
                + "union distinct\n"
                + "  (select * from db1.tbl6 where k1='b' and k4=2\n"
                + "   union all\n"
                + "   select * from db1.tbl6 where k1='b' and k4=2)\n"
                + "union distinct\n"
                + "  (select * from db1.tbl6 where k1='b' and k4=2\n"
                + "   union all\n"
                + "   (select * from db1.tbl6 where k1='b' and k4=3)\n"
                + "   order by 3 limit 3)\n"
                + "union all\n"
                + "  (select * from db1.tbl6 where k1='b' and k4=3\n"
                + "   union all\n"
                + "   select * from db1.tbl6 where k1='b' and k4=4)\n"
                + "union all\n"
                + "  (select * from db1.tbl6 where k1='b' and k4=3\n"
                + "   union all\n"
                + "   (select * from db1.tbl6 where k1='b' and k4=5)\n"
                + "   order by 3 limit 3)";
        starRocksAssert.query(sql2).explainContains("UNION", 5);

        // intersect
        String sql3 = "select * from\n"
                + "  (select k1, k2 from db1.tbl6\n"
                + "   intersect\n"
                + "   select k1, k2 from db1.tbl6) a\n"
                + "  inner join\n"
                + "  db1.tbl6 b\n"
                + "  on (a.k1 = b.k1)\n"
                + "where b.k1 = 'a'";
        starRocksAssert.query(sql3).explainContains("INTERSECT", 1);

        String sql4 = "select * from db1.tbl6 where k1='a' and k4=1\n"
                + "intersect distinct\n"
                + "  (select * from db1.tbl6 where k1='b' and k4=2\n"
                + "   intersect\n"
                + "   select * from db1.tbl6 where k1='b' and k4=2)\n"
                + "intersect distinct\n"
                + "  (select * from db1.tbl6 where k1='b' and k4=2\n"
                + "   intersect\n"
                + "   (select * from db1.tbl6 where k1='b' and k4=3)\n"
                + "   order by 3 limit 3)\n"
                + "intersect\n"
                + "  (select * from db1.tbl6 where k1='b' and k4=3\n"
                + "   intersect\n"
                + "   select * from db1.tbl6 where k1='b' and k4=4)\n"
                + "intersect\n"
                + "  (select * from db1.tbl6 where k1='b' and k4=3\n"
                + "   intersect\n"
                + "   (select * from db1.tbl6 where k1='b' and k4=5)\n"
                + "   order by 3 limit 3)";
        starRocksAssert.query(sql4).explainContains("INTERSECT", 5);

        // except
        String sql5 = "select * from\n"
                + "  (select k1, k2 from db1.tbl6\n"
                + "   except\n"
                + "   select k1, k2 from db1.tbl6) a\n"
                + "  inner join\n"
                + "  db1.tbl6 b\n"
                + "  on (a.k1 = b.k1)\n"
                + "where b.k1 = 'a'";
        starRocksAssert.query(sql5).explainContains("EXCEPT", 1);

        String sql6 = "select * from db1.tbl6 where k1='a' and k4=1\n"
                + "except\n"
                + "select * from db1.tbl6 where k1='a' and k4=1\n"
                + "except\n"
                + "select * from db1.tbl6 where k1='a' and k4=2\n"
                + "except distinct\n"
                + "(select * from db1.tbl6 where k1='a' and k4=2)\n"
                + "order by 3 limit 3";
        starRocksAssert.query(sql6).explainContains("EXCEPT", 1);

        String sql7 = "select * from db1.tbl6 where k1='a' and k4=1\n"
                + "except distinct\n"
                + "select * from db1.tbl6 where k1='a' and k4=1\n"
                + "except\n"
                + "select * from db1.tbl6 where k1='a' and k4=2\n"
                + "except\n"
                + "(select * from db1.tbl6 where k1='a' and k4=2)\n"
                + "order by 3 limit 3";
        starRocksAssert.query(sql7).explainContains("EXCEPT", 1);

        // mixed
        String sql8 = "select * from db1.tbl6 where k1='a' and k4=1\n"
                + "union\n"
                + "select * from db1.tbl6 where k1='a' and k4=1\n"
                + "except\n"
                + "select * from db1.tbl6 where k1='a' and k4=2\n"
                + "intersect\n"
                + "(select * from db1.tbl6 where k1='a' and k4=2)\n"
                + "order by 3 limit 3";
        starRocksAssert.query(sql8).explainContains("UNION", "INTERSECT", "EXCEPT");

        String sql9 = "select * from db1.tbl6 where k1='a' and k4=1\n"
                + "intersect distinct\n"
                + "  (select * from db1.tbl6 where k1='b' and k4=2\n"
                + "   union all\n"
                + "   select * from db1.tbl6 where k1='b' and k4=2)\n"
                + "intersect distinct\n"
                + "  (select * from db1.tbl6 where k1='b' and k4=2\n"
                + "   except\n"
                + "   (select * from db1.tbl6 where k1='b' and k4=3)\n"
                + "   order by 3 limit 3)\n"
                + "union all\n"
                + "  (select * from db1.tbl6 where k1='b' and k4=3\n"
                + "   intersect\n"
                + "   select * from db1.tbl6 where k1='b' and k4=4)\n"
                + "except\n"
                + "  (select * from db1.tbl6 where k1='b' and k4=3\n"
                + "   intersect\n"
                + "   (select * from db1.tbl6 where k1='b' and k4=5)\n"
                + "   order by 3 limit 3)";
        starRocksAssert.query(sql9).explainContains("UNION", 2);
        starRocksAssert.query(sql9).explainContains("INTERSECT", 3);
        starRocksAssert.query(sql9).explainContains("EXCEPT", 2);

        String sql10 = "select 499 union select 670 except select 499";
        String plan = getFragmentPlan(sql10);
        Assert.assertTrue(plan.contains("  10:UNION\n" +
                "     constant exprs: \n" +
                "         499"));
        Assert.assertTrue(plan.contains("1:UNION"));
        Assert.assertTrue(plan.contains("  4:UNION\n" +
                "     constant exprs: \n" +
                "         670"));
        Assert.assertTrue(plan.contains("  2:UNION\n" +
                "     constant exprs: \n" +
                "         499"));
        Assert.assertTrue(plan.contains("0:EXCEPT"));
    }

    @Test
    public void testPushDown() throws Exception {
        String sql1 = "SELECT\n" +
                "    IF(k2 IS NULL, 'ALL', k2) AS k2,\n" +
                "    IF(k3 IS NULL, 'ALL', k3) AS k3,\n" +
                "    k4\n" +
                "FROM\n" +
                "(\n" +
                "    SELECT\n" +
                "        k1,\n" +
                "        k2,\n" +
                "        k3,\n" +
                "        SUM(k4) AS k4\n" +
                "    FROM  db1.tbl6\n" +
                "    WHERE k1 = 0\n" +
                "        AND k4 = 1\n" +
                "        AND k3 = 'foo'\n" +
                "    GROUP BY \n" +
                "    GROUPING SETS (\n" +
                "        (k1),\n" +
                "        (k1, k2),\n" +
                "        (k1, k3),\n" +
                "        (k1, k2, k3)\n" +
                "    )\n" +
                ") t\n" +
                "WHERE IF(k2 IS NULL, 'ALL', k2) = 'ALL'";
        String plan = getFragmentPlan(sql1);
        Assert.assertTrue(plan.contains("  5:Project\n" +
                "  |  <slot 5> : 5: sum(4: k4)\n" +
                "  |  <slot 7> : if(2: k2 IS NULL, 'ALL', 2: k2)\n" +
                "  |  <slot 8> : if(3: k3 IS NULL, 'ALL', 3: k3)"));
        Assert.assertTrue(plan.contains("2:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: sum(4: k4)\n" +
                "  |  group by: 1: k1, 3: k3, 2: k2, 6: GROUPING_ID"));
        Assert.assertTrue(plan.contains("1:REPEAT_NODE\n" +
                "  |  repeat: repeat 3 lines [[1], [1, 2], [1, 3], [1, 2, 3]]\n" +
                "  |  PREDICATES: if(2: k2 IS NULL, 'ALL', 2: k2) = 'ALL'"));

        String sql2 =
                "SELECT\n" +
                        "    IF(k2 IS NULL, 'ALL', k2) AS k2,\n" +
                        "    IF(k3 IS NULL, 'ALL', k3) AS k3,\n" +
                        "    k4\n" +
                        "FROM\n" +
                        "(\n" +
                        "    SELECT\n" +
                        "        k1,\n" +
                        "        k2,\n" +
                        "        k3,\n" +
                        "        SUM(k4) AS k4\n" +
                        "    FROM  db1.tbl6\n" +
                        "    WHERE k1 = 0\n" +
                        "        AND k4 = 1\n" +
                        "        AND k3 = 'foo'\n" +
                        "    GROUP BY k1, k2, k3\n" +
                        ") t\n" +
                        "WHERE IF(k2 IS NULL, 'ALL', k2) = 'ALL'";
        plan = getFragmentPlan(sql2);
        Assert.assertTrue(plan.contains("  2:Project\n" +
                "  |  <slot 5> : 5: sum(4: k4)\n" +
                "  |  <slot 6> : if(2: k2 IS NULL, 'ALL', 2: k2)\n" +
                "  |  <slot 7> : if(3: k3 IS NULL, 'ALL', 3: k3)"));
        Assert.assertTrue(plan.contains("  0:OlapScanNode\n" +
                "     TABLE: tbl6\n" +
                "     PREAGGREGATION: OFF. Reason: The key column don't support aggregate function: SUM\n" +
                "     PREDICATES: if(2: k2 IS NULL, 'ALL', 2: k2) = 'ALL', 1: k1 = '0', 4: k4 = 1, 3: k3 = 'foo'"));
    }

    @Test
    public void testJoinPredicateTransitivity() throws Exception {
        // test left join : left table where binary predicate
        String sql = "select join1.id\n" +
                "from join1\n" +
                "left join join2 on join1.id = join2.id\n" +
                "where join1.id > 1;";
        String explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("PREDICATES: 2: id > 1"));
        Assert.assertTrue(explainString.contains("PREDICATES: 5: id > 1"));

        // test left join: left table where in predicate
        sql = "select join1.id\n" +
                "from join1\n" +
                "left join join2 on join1.id = join2.id\n" +
                "where join1.id in (2);";
        explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("PREDICATES: 2: id = 2"));
        Assert.assertTrue(explainString.contains("PREDICATES: 5: id = 2"));

        // test left join: left table where between predicate
        sql = "select join1.id\n" +
                "from join1\n" +
                "left join join2 on join1.id = join2.id\n" +
                "where join1.id BETWEEN 1 AND 2;";
        explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("PREDICATES: 2: id >= 1, 2: id <= 2"));
        Assert.assertTrue(explainString.contains("PREDICATES: 5: id >= 1, 5: id <= 2"));

        // test left join: left table join predicate, left table couldn't push down
        sql = "select *\n from join1\n" +
                "left join join2 on join1.id = join2.id\n" +
                "and join1.id > 1;";
        explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("  3:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: id = 5: id\n" +
                "  |  other join predicates: 2: id > 1"));
        Assert.assertTrue(explainString.contains("  0:OlapScanNode\n" +
                "     TABLE: join1\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1"));
        Assert.assertTrue(explainString.contains("  1:OlapScanNode\n" +
                "     TABLE: join2\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 5: id > 1"));

        // test left join: right table where predicate.
        // If we eliminate outer join, we could push predicate down to join1 and join2.
        // Currently, we push predicate to join1 and keep join predicate for join2
        sql = "select *\n from join1\n" +
                "left join join2 on join1.id = join2.id\n" +
                "where join2.id > 1;";
        explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("  1:OlapScanNode\n" +
                "     TABLE: join2\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 5: id > 1"));

        // test left join: right table join predicate, only push down right table
        sql = "select *\n from join1\n" +
                "left join join2 on join1.id = join2.id\n" +
                "and join2.id > 1;";
        explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("  1:OlapScanNode\n" +
                "     TABLE: join2\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 5: id > 1"));
        Assert.assertTrue(explainString.contains("0:OlapScanNode\n" +
                "     TABLE: join1\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1\n" +
                "     rollup: join1"));

        // test inner join: left table where predicate, both push down left table and right table
        sql = "select *\n from join1\n" +
                "join join2 on join1.id = join2.id\n" +
                "where join1.id > 1;";
        explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("  0:OlapScanNode\n" +
                "     TABLE: join1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 2: id > 1"));
        Assert.assertTrue(explainString.contains("  1:OlapScanNode\n" +
                "     TABLE: join2\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 5: id > 1"));

        // test inner join: left table join predicate, both push down left table and right table
        sql = "select *\n from join1\n" +
                "join join2 on join1.id = join2.id\n" +
                "and join1.id > 1;";
        explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("  0:OlapScanNode\n" +
                "     TABLE: join1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 2: id > 1"));
        Assert.assertTrue(explainString.contains("  1:OlapScanNode\n" +
                "     TABLE: join2\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 5: id > 1"));

        // test inner join: right table where predicate, both push down left table and right table
        sql = "select *\n from join1\n" +
                "join join2 on join1.id = join2.id\n" +
                "where join2.id > 1;";
        explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("  0:OlapScanNode\n" +
                "     TABLE: join1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 2: id > 1"));
        Assert.assertTrue(explainString.contains("  1:OlapScanNode\n" +
                "     TABLE: join2\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 5: id > 1"));

        // test inner join: right table join predicate, both push down left table and right table
        sql = "select *\n from join1\n" +
                "join join2 on join1.id = join2.id\n" +
                "and 1 < join2.id;";
        explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("  0:OlapScanNode\n" +
                "     TABLE: join1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 2: id > 1"));
        Assert.assertTrue(explainString.contains("  1:OlapScanNode\n" +
                "     TABLE: join2\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 5: id > 1"));

        sql = "select *\n from join1\n" +
                "join join2 on join1.id = join2.value\n" +
                "and join2.value in ('abc');";
        explainString = getFragmentPlan(sql);
        Assert.assertTrue(
                explainString.contains("equal join conjunct: 7: cast = 8: cast"));
        Assert.assertTrue(explainString.contains("<slot 7> : CAST(2: id AS DOUBLE)"));
        Assert.assertTrue(explainString.contains("<slot 8> : CAST(6: value AS DOUBLE)"));
        Assert.assertTrue(explainString.contains("  2:OlapScanNode\n" +
                "     TABLE: join2\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 6: value = 'abc'"));

        // test anti join, right table join predicate, only push to right table
        sql = "select *\n from join1\n" +
                "left anti join join2 on join1.id = join2.id\n" +
                "and join2.id > 1;";
        explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("  1:OlapScanNode\n" +
                "     TABLE: join2\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 5: id > 1"));
        Assert.assertTrue(explainString.contains("  3:HASH JOIN\n" +
                "  |  join op: LEFT ANTI JOIN (BROADCAST)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: id = 5: id"));

        // test semi join, right table join predicate, only push to right table
        sql = "select *\n from join1\n" +
                "left semi join join2 on join1.id = join2.id\n" +
                "and join2.id > 1;";
        explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("  3:HASH JOIN\n" +
                "  |  join op: LEFT SEMI JOIN (BROADCAST)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: id = 5: id"));
        Assert.assertTrue(explainString.contains("  1:OlapScanNode\n" +
                "     TABLE: join2\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 5: id > 1"));

        // test anti join, left table join predicate, left table couldn't push down
        sql = "select *\n from join1\n" +
                "left anti join join2 on join1.id = join2.id\n" +
                "and join1.id > 1;";
        explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("  3:HASH JOIN\n" +
                "  |  join op: LEFT ANTI JOIN (BROADCAST)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: id = 5: id\n" +
                "  |  other join predicates: 2: id > 1"));

        // test semi join, left table join predicate, only push to left table
        sql = "select *\n from join1\n" +
                "left semi join join2 on join1.id = join2.id\n" +
                "and join1.id > 1;";
        explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("  3:HASH JOIN\n" +
                "  |  join op: LEFT SEMI JOIN (BROADCAST)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: id = 5: id"));
        Assert.assertTrue(explainString.contains("  0:OlapScanNode\n" +
                "     TABLE: join1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 2: id > 1"));

        // test anti join, left table where predicate, only push to left table
        sql = "select join1.id\n" +
                "from join1\n" +
                "left anti join join2 on join1.id = join2.id\n" +
                "where join1.id > 1;";
        explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("  4:HASH JOIN\n" +
                "  |  join op: RIGHT ANTI JOIN (PARTITIONED)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 5: id = 2: id"));
        Assert.assertTrue(explainString.contains("  2:OlapScanNode\n" +
                "     TABLE: join1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 2: id > 1"));

        // test semi join, left table where predicate, only push to left table
        sql = "select join1.id\n" +
                "from join1\n" +
                "left semi join join2 on join1.id = join2.id\n" +
                "where join1.id > 1;";
        explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("  3:HASH JOIN\n" +
                "  |  join op: LEFT SEMI JOIN (BROADCAST)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: id = 5: id"));
        Assert.assertTrue(explainString.contains("  0:OlapScanNode\n" +
                "     TABLE: join1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 2: id > 1"));
    }

    @Test
    public void testNullAddNull() throws Exception {
        String sql = "select null+null as c3 from test.join2;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains(" OUTPUT EXPRS:4: expr"));
        Assert.assertTrue(plan.contains("  1:Project\n" +
                "  |  <slot 4> : NULL"));
    }

    @Test
    public void testJoinConst() throws Exception {
        String sql =
                "with user_info as (select 2 as user_id, 'mike' as user_name), address as (select 1 as user_id, 'newzland' as address_name) \n" +
                        "select * from address a right join user_info b on b.user_id=a.user_id;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  2:HASH JOIN\n" +
                "  |  join op: RIGHT OUTER JOIN (COLOCATE)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: true\n" +
                "  |  equal join conjunct: 1: expr = 3: expr"));
        Assert.assertTrue(plan.contains("  |----1:UNION\n" +
                "  |       constant exprs: \n" +
                "  |           2 | 'mike'"));
        Assert.assertTrue(plan.contains("  0:UNION\n" +
                "     constant exprs: \n" +
                "         1 | 'newzland'\n" +
                "     use vectorized: true"));
    }

    @Test
    public void testVarianceStddevAnalyze() throws Exception {
        String sql = "select stddev_pop(1222) from (select 1) t;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:AGGREGATE (update finalize)\n" +
                "  |  output: stddev_pop(1222)\n" +
                "  |  group by: "));
        Assert.assertTrue(plan.contains("  0:UNION\n" +
                "     constant exprs: \n" +
                "         1"));
    }

    @Test
    public void testDateDateTimeFunctionMatch() throws Exception {
        String sql = "select if(3, date('2021-01-12'), STR_TO_DATE('2020-11-02', '%Y-%m-%d %H:%i:%s'));";
        starRocksAssert.query(sql).explainContains("  0:UNION\n" +
                "     constant exprs: \n" +
                "         if(CAST(3 AS BOOLEAN), '2021-01-12 00:00:00', str_to_date('2020-11-02', '%Y-%m-%d %H:%i:%s'))");

        sql = "select nullif(date('2021-01-12'), date('2021-01-11'));";
        starRocksAssert.query(sql).explainContains("  0:UNION\n" +
                "     constant exprs: \n" +
                "         nullif('2021-01-12', '2021-01-11')");

        sql = "select nullif(date('2021-01-12'), STR_TO_DATE('2020-11-02', '%Y-%m-%d %H:%i:%s'));";
        starRocksAssert.query(sql).explainContains("  0:UNION\n" +
                "     constant exprs: \n" +
                "         nullif('2021-01-12 00:00:00', str_to_date('2020-11-02', '%Y-%m-%d %H:%i:%s'))");

        sql = "select if(3, 4, 5);";
        starRocksAssert.query(sql).explainContains("  0:UNION\n" +
                "     constant exprs: \n" +
                "         if(CAST(3 AS BOOLEAN), 4, 5)");

        sql = "select ifnull(date('2021-01-12'), 123);";
        starRocksAssert.query(sql).explainContains("  0:UNION\n" +
                "     constant exprs: \n" +
                "         ifnull(CAST('2021-01-12' AS INT), 123)");

        sql = "select ifnull(date('2021-01-12'), 'kks');";
        starRocksAssert.query(sql).explainContains("  0:UNION\n" +
                "     constant exprs: \n" +
                "         '2021-01-12'");

        sql = "select ifnull(1234, 'kks');";
        starRocksAssert.query(sql).explainContains("  0:UNION\n" +
                "     constant exprs: \n" +
                "         '1234'");
    }

    @Test
    public void testLeadAndLagFunction() {
        String sql = "select LAG(k7, 3, 3) OVER () from baseall";
        starRocksAssert.query(sql).analysisError("The third parameter of `lag` can't not convert");

        sql = "select lead(k7, 3, 3) OVER () from baseall";
        starRocksAssert.query(sql).analysisError("The third parameter of `lead` can't not convert");

        sql = "select lead(k3, 3, 'kks') OVER () from baseall";
        starRocksAssert.query(sql)
                .analysisError("Convert type error in offset fn(default value); old_type=VARCHAR new_type=INT");

        sql = "select lead(id2, 1, 1) OVER () from bitmap_table";
        starRocksAssert.query(sql).analysisError("No matching function with signature: lead(bitmap,");

        sql = "select lag(id2, 1, 1) OVER () from hll_table";
        starRocksAssert.query(sql).analysisError("No matching function with signature: lag(hll,");
    }

    @Test
    public void testAntiJoinOnFalseConstantPredicate() throws Exception {
        String sql = "select join2.id from join1 RIGHT ANTI JOIN join2 on join1.id = join2.id" +
                " and 1 > 2 group by join2.id" +
                " union select join2.id from join1 RIGHT ANTI JOIN join2 on join1.id = join2.id " +
                " and 1 > 2 WHERE (NOT (true)) group by join2.id ";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  3:HASH JOIN\n" +
                "  |  join op: LEFT ANTI JOIN (COLOCATE)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: true\n" +
                "  |  equal join conjunct: 5: id = 2: id"));
        Assert.assertTrue(plan.contains("  |----2:EMPTYSET\n" +
                "  |       use vectorized: true"));
        Assert.assertTrue(plan.contains("  7:EMPTYSET\n" +
                "     use vectorized: true"));
    }

    @Test
    public void testFullOuterJoin2() throws Exception {
        String sql =
                "SELECT 1 FROM join1 RIGHT ANTI JOIN join2 on join1.id = join2.id and join2.dt = 1 FULL OUTER JOIN "
                        + "pushdown_test on join2.dt = pushdown_test.k3 WHERE join2.value != join2.value";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  9:HASH JOIN\n" +
                "  |  join op: RIGHT OUTER JOIN (PARTITIONED)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 9: k3 = 4: dt"));
        Assert.assertTrue(plan.contains("  6:HASH JOIN\n" +
                "  |  join op: LEFT ANTI JOIN (BROADCAST)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 5: id = 2: id\n" +
                "  |  other join predicates: 4: dt = 1"));
    }

    @Test
    public void testFullOuterJoin3() throws Exception {
        String sql =
                "SELECT 1 FROM join1 RIGHT ANTI JOIN join2 on join1.id = join2.id FULL OUTER JOIN "
                        + "pushdown_test on join2.dt = pushdown_test.k3 WHERE join2.value != join2.value";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  9:HASH JOIN\n" +
                "  |  join op: RIGHT OUTER JOIN (PARTITIONED)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 9: k3 = 4: dt"));
        Assert.assertTrue(plan.contains("  6:HASH JOIN\n" +
                "  |  join op: LEFT ANTI JOIN (BROADCAST)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 5: id = 2: id"));
    }

    @Test
    public void testLargeIntLiteralCompare() throws Exception {
        String sql =
                "select k2 from baseall group by ((10800861)/(((NULL)%(((-1114980787)+(-1182952114)))))), ((10800861)*(-9223372036854775808)), k2";
        starRocksAssert.query(sql).explainContains("group by: 2: k2");
    }

    @Test
    public void testGroupingFunctions() throws Exception {
        String sql = "select GROUPING(k10) from baseall;";
        starRocksAssert.query(sql).analysisError("cannot use GROUPING functions without");

        sql = "select k10 from baseall group by k10, GROUPING(1193275260000);";
        starRocksAssert.query(sql).analysisError("grouping functions only support column");

        sql = "select k10 from baseall group by k10 having GROUPING(1193275260000) > 2;";
        starRocksAssert.query(sql).analysisError("HAVING clause cannot contain grouping");

        sql = "select k10, GROUPING(k10) from baseall group by GROUPING SETS (  (k10), ( ) );";
        starRocksAssert.query(sql).explainContains("group by: 7: k10, 12: GROUPING_ID, 13: GROUPING");
    }

    @Test
    public void testWindowPartitionAndSortSameColumn() throws Exception {
        String sql = "SELECT k3, avg(k3) OVER (partition by k3 order by k3) AS sum FROM baseall;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  3:ANALYTIC\n" +
                "  |  functions: [, avg(3: k3), ]\n" +
                "  |  partition by: 3: k3\n" +
                "  |  order by: 3: k3 ASC"));
        Assert.assertTrue(plan.contains("  2:SORT\n" +
                "  |  order by: <slot 3> 3: k3 ASC"));
    }

    @Test
    public void testJoinReorderWithReanalyze() throws Exception {
        Catalog catalog = connectContext.getCatalog();
        Table table = catalog.getDb("default_cluster:test").getTable("join2");
        OlapTable olapTable1 = (OlapTable) table;
        new Expectations(olapTable1) {
            {
                olapTable1.getRowCount();
                result = 2L;
                minTimes = 0;
            }
        };
        String sql = "select * from join1 join join2 on join1.id = join2.id and 1 < join1.id ";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  0:OlapScanNode\n" +
                "     TABLE: join1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 2: id > 1"));
        Assert.assertTrue(plan.contains("  1:OlapScanNode\n" +
                "     TABLE: join2\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 5: id > 1"));
    }

    @Test
    public void testPreAggregateForCrossJoin() throws Exception {
        String sql = "select join1.id from join1, join2 group by join1.id";
        String plan = getFragmentPlan(sql);

        Assert.assertTrue(plan.contains("  0:OlapScanNode\n" +
                "     TABLE: join1\n" +
                "     PREAGGREGATION: ON"));
        Assert.assertTrue(plan.contains("  1:OlapScanNode\n" +
                "     TABLE: join2\n" +
                "     PREAGGREGATION: ON"));

        // AGGREGATE KEY table PREAGGREGATION should be off
        sql = "select join2.id from baseall, join2 group by join2.id";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  0:OlapScanNode\n" +
                "     TABLE: join2\n" +
                "     PREAGGREGATION: ON"));
        Assert.assertTrue(plan.contains("  1:OlapScanNode\n" +
                "     TABLE: baseall\n" +
                "     PREAGGREGATION: OFF. Reason: Has can not pre-aggregation Join"));
    }

    @Test
    public void testPreAggregationWithJoin() throws Exception {
        // check left agg table with pre-aggregation
        String sql = "select k2, sum(k9) from baseall join join2 on k1 = id group by k2";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("0:OlapScanNode\n" +
                "     TABLE: baseall\n" +
                "     PREAGGREGATION: ON"));

        // check right agg table with pre-agg
        sql = "select k2, sum(k9) from join2 join [broadcast] baseall on k1 = id group by k2";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("1:OlapScanNode\n" +
                "     TABLE: baseall\n" +
                "     PREAGGREGATION: ON"));

        // check two agg tables only one agg table can pre-aggregation
        sql = "select t1.k2, sum(t1.k9) from baseall t1 join baseall t2 on t1.k1 = t2.k1 group by t1.k2";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("0:OlapScanNode\n" +
                "     TABLE: baseall\n" +
                "     PREAGGREGATION: ON"));
        Assert.assertTrue(plan.contains("1:OlapScanNode\n" +
                "  |       TABLE: baseall\n" +
                "  |       PREAGGREGATION: OFF. Reason: Has can not pre-aggregation Join"));

        sql = "select t2.k2, sum(t2.k9) from baseall t1 join [broadcast] baseall t2 on t1.k1 = t2.k1 group by t2.k2";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("0:OlapScanNode\n" +
                "     TABLE: baseall\n" +
                "     PREAGGREGATION: OFF. Reason: Has can not pre-aggregation Join"));
        Assert.assertTrue(plan.contains("1:OlapScanNode\n" +
                "     TABLE: baseall\n" +
                "     PREAGGREGATION: ON"));

        // check multi tables only one agg table can pre-aggregation
        sql =
                "select t1.k2, sum(t1.k9) from baseall t1 join join2 t2 on t1.k1 = t2.id join baseall t3 on t1.k1 = t3.k1 group by t1.k2";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("6:OlapScanNode\n" +
                "  |       TABLE: baseall\n" +
                "  |       PREAGGREGATION: OFF. Reason: Has can not pre-aggregation Join"));
        Assert.assertTrue(plan.contains("0:OlapScanNode\n" +
                "     TABLE: baseall\n" +
                "     PREAGGREGATION: ON"));

        sql =
                "select t3.k2, sum(t3.k9) from baseall t1 join [broadcast] join2 t2 on t1.k1 = t2.id join [broadcast] baseall t3 on t1.k1 = t3.k1 group by t3.k2";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("6:OlapScanNode\n" +
                "     TABLE: baseall\n" +
                "     PREAGGREGATION: ON"));
        Assert.assertTrue(plan.contains("0:OlapScanNode\n" +
                "     TABLE: baseall\n" +
                "     PREAGGREGATION: OFF. Reason: Has can not pre-aggregation Join"));

        // check join predicate with non key columns
        sql = "select t1.k2, sum(t1.k9) from baseall t1 join baseall t2 on t1.k9 = t2.k9 group by t1.k2";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("0:OlapScanNode\n" +
                "     TABLE: baseall\n" +
                "     PREAGGREGATION: OFF. Reason: Predicates include the value column"));

        sql =
                "select t1.k2, sum(t1.k9) from baseall t1 join baseall t2 on t1.k1 = t2.k1 where t1.k9 + t2.k9 = 1 group by t1.k2";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("0:OlapScanNode\n" +
                "     TABLE: baseall\n" +
                "     PREAGGREGATION: OFF. Reason: Predicates include the value column"));

        // check group by two tables columns
        sql = "select t1.k2, t2.k2, sum(t1.k9) from baseall t1 join baseall t2 on t1.k1 = t2.k1 group by t1.k2, t2.k2";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("0:OlapScanNode\n" +
                "     TABLE: baseall\n" +
                "     PREAGGREGATION: ON"));

        // check aggregate two table columns
        sql =
                "select t1.k2, t2.k2, sum(t1.k9), sum(t2.k9) from baseall t1 join baseall t2 on t1.k1 = t2.k1 group by t1.k2, t2.k2";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("0:OlapScanNode\n" +
                "     TABLE: baseall\n" +
                "     PREAGGREGATION: OFF. Reason: Has can not pre-aggregation Join"));
    }

    @Test
    public void testOuterJoinEliminate() throws Exception {
        // test left join eliminate
        String sql = "select * from join1 left join join2 on join1.id = join2.id\n" +
                "where join2.id > 1;";
        starRocksAssert.query(sql).explainContains("  3:HASH JOIN\n" +
                        "  |  join op: INNER JOIN (BROADCAST)\n" +
                        "  |  hash predicates:\n" +
                        "  |  colocate: false, reason: \n" +
                        "  |  equal join conjunct: 2: id = 5: id",
                "  0:OlapScanNode\n" +
                        "     TABLE: join1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 2: id > 1",
                "  1:OlapScanNode\n" +
                        "     TABLE: join2\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 5: id > 1");

        // test left join eliminate with compound predicate
        sql = "select * from join1 left join join2 on join1.id = join2.id\n" +
                "where join2.id > 1 or join2.id < 10 ;";
        starRocksAssert.query(sql).explainContains("  3:HASH JOIN\n" +
                        "  |  join op: INNER JOIN (BROADCAST)\n" +
                        "  |  hash predicates:\n" +
                        "  |  colocate: false, reason: \n" +
                        "  |  equal join conjunct: 2: id = 5: id",
                "  1:OlapScanNode\n" +
                        "     TABLE: join2\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: (5: id > 1) OR (5: id < 10)");

        // test left join eliminate with compound predicate
        sql = "select * from join1 left join join2 on join1.id = join2.id\n" +
                "where join2.id > 1 or join2.id is null;";
        //        getFragmentPlan(sql);
        starRocksAssert.query(sql).explainContains("  3:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: id = 5: id\n" +
                "  |  other predicates: (5: id > 1) OR (5: id IS NULL)");

        // test left join eliminate with inline view
        sql = "select * from join1 left join (select * from join2) b on join1.id = b.id\n" +
                "where b.id > 1;";
        starRocksAssert.query(sql).explainContains("  3:HASH JOIN\n" +
                        "  |  join op: INNER JOIN (BROADCAST)\n" +
                        "  |  hash predicates:\n" +
                        "  |  colocate: false, reason: \n" +
                        "  |  equal join conjunct: 2: id = 5: id",
                "  0:OlapScanNode\n" +
                        "     TABLE: join1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 2: id > 1",
                "  1:OlapScanNode\n" +
                        "     TABLE: join2\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 5: id > 1");

        // test left join eliminate with inline view
        sql = "select * from (select * from join1) a left join (select * from join2) b on a.id = b.id\n" +
                "where b.id > 1;";
        starRocksAssert.query(sql).explainContains("  3:HASH JOIN\n" +
                        "  |  join op: INNER JOIN (BROADCAST)\n" +
                        "  |  hash predicates:\n" +
                        "  |  colocate: false, reason: \n" +
                        "  |  equal join conjunct: 2: id = 5: id",
                "  0:OlapScanNode\n" +
                        "     TABLE: join1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 2: id > 1",
                "  1:OlapScanNode\n" +
                        "     TABLE: join2\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 5: id > 1");

        // test not left join eliminate
        sql = "select * from join1 left join join2 on join1.id = join2.id\n" +
                "where join2.id is null;";
        starRocksAssert.query(sql).explainContains("  3:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: id = 5: id\n" +
                "  |  other predicates: 5: id IS NULL");

        // test having group column
        sql = "select count(*) from join1 left join join2 on join1.id = join2.id\n" +
                "group by join2.id having join2.id > 1;";
        starRocksAssert.query(sql).explainContains("  3:HASH JOIN\n" +
                        "  |  join op: INNER JOIN (BROADCAST)\n" +
                        "  |  hash predicates:\n" +
                        "  |  colocate: false, reason: \n" +
                        "  |  equal join conjunct: 5: id = 2: id",
                "  0:OlapScanNode\n" +
                        "     TABLE: join2\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 5: id > 1",
                "  1:OlapScanNode\n" +
                        "     TABLE: join1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 2: id > 1");

        // test having aggregate column
        sql = "select count(*) as count from join1 left join join2 on join1.id = join2.id\n" +
                "having count > 1;";
        starRocksAssert.query(sql).explainContains("7:AGGREGATE (merge finalize)\n" +
                        "  |  output: count(7: count())\n" +
                        "  |  group by: \n" +
                        "  |  having: 7: count() > 1",
                "  3:HASH JOIN\n" +
                        "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                        "  |  hash predicates:\n" +
                        "  |  colocate: false, reason: \n" +
                        "  |  equal join conjunct: 2: id = 5: id");

        // test right join eliminate
        sql = "select * from join1 right join join2 on join1.id = join2.id\n" +
                "where join1.id > 1;";
        starRocksAssert.query(sql).explainContains("  3:HASH JOIN\n" +
                        "  |  join op: INNER JOIN (BROADCAST)\n" +
                        "  |  hash predicates:\n" +
                        "  |  colocate: false, reason: \n" +
                        "  |  equal join conjunct: 2: id = 5: id",
                "  0:OlapScanNode\n" +
                        "     TABLE: join1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 2: id > 1",
                "  1:OlapScanNode\n" +
                        "     TABLE: join2\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 5: id > 1");

        // test full outer join convert to left join
        sql = "select * from join1 full outer join join2 on join1.id = join2.id\n" +
                "where join1.id > 1;";
        starRocksAssert.query(sql).explainContains("  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                        "  |  hash predicates:\n" +
                        "  |  colocate: false, reason: \n" +
                        "  |  equal join conjunct: 2: id = 5: id",
                "  0:OlapScanNode\n" +
                        "     TABLE: join1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 2: id > 1",
                "  1:OlapScanNode\n" +
                        "     TABLE: join2\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 5: id > 1");

        sql = "select * from join1 full outer join join2 on join1.id = join2.id and join1.dt != 2\n" +
                "where join1.id > 1;";
        starRocksAssert.query(sql).explainContains("  3:HASH JOIN\n" +
                        "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                        "  |  hash predicates:\n" +
                        "  |  colocate: false, reason: \n" +
                        "  |  equal join conjunct: 2: id = 5: id\n" +
                        "  |  other join predicates: 1: dt != 2",
                "  0:OlapScanNode\n" +
                        "     TABLE: join1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 2: id > 1",
                "  1:OlapScanNode\n" +
                        "     TABLE: join2\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 5: id > 1");

        // test full outer join convert to right join
        sql = "select * from join1 full outer join join2 on join1.id = join2.id\n" +
                "where join2.id > 1;";
        starRocksAssert.query(sql).explainContains("  4:HASH JOIN\n" +
                        "  |  join op: RIGHT OUTER JOIN (PARTITIONED)\n" +
                        "  |  hash predicates:\n" +
                        "  |  colocate: false, reason: \n" +
                        "  |  equal join conjunct: 2: id = 5: id",
                "  2:OlapScanNode\n" +
                        "     TABLE: join2\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 5: id > 1",
                "  0:OlapScanNode\n" +
                        "     TABLE: join1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 2: id > 1");

        // test full outer join convert to inner join
        sql = "select * from join1 full outer join join2 on join1.id = join2.id\n" +
                "where join2.id > 1 and join1.id > 10;";
        starRocksAssert.query(sql).explainContains("  3:HASH JOIN\n" +
                        "  |  join op: INNER JOIN (BROADCAST)\n" +
                        "  |  hash predicates:\n" +
                        "  |  colocate: false, reason: \n" +
                        "  |  equal join conjunct: 2: id = 5: id",
                "  0:OlapScanNode\n" +
                        "     TABLE: join1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 2: id > 10",
                "  1:OlapScanNode\n" +
                        "     TABLE: join2\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 5: id > 1");

        // test multi left join eliminate
        sql = "select * from join1 left join join2 as b on join1.id = b.id\n" +
                "left join join2 as c on join1.id = c.id \n" +
                "where b.id > 1;";

        starRocksAssert.query(sql).explainContains("7:HASH JOIN\n" +
                        "  |  join op: LEFT OUTER JOIN (BUCKET_SHUFFLE(S))\n" +
                        "  |  hash predicates:\n" +
                        "  |  colocate: false, reason: \n" +
                        "  |  equal join conjunct: 2: id = 8: id",
                "4:HASH JOIN\n" +
                        "  |  join op: INNER JOIN (PARTITIONED)\n" +
                        "  |  hash predicates:\n" +
                        "  |  colocate: false, reason: \n" +
                        "  |  equal join conjunct: 5: id = 2: id",
                "0:OlapScanNode\n" +
                        "     TABLE: join2\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 5: id > 1",
                "2:OlapScanNode\n" +
                        "     TABLE: join1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 2: id > 1");

        sql = "select * from join1 left join join2 as b on join1.id = b.id\n" +
                "left join join2 as c on join1.id = c.id \n" +
                "where b.dt > 1 and c.dt > 1;";
        starRocksAssert.query(sql).explainContains("6:HASH JOIN\n" +
                        "  |  join op: INNER JOIN (BROADCAST)\n" +
                        "  |  hash predicates:\n" +
                        "  |  colocate: false, reason: \n" +
                        "  |  equal join conjunct: 2: id = 8: id",
                "  3:HASH JOIN\n" +
                        "  |  join op: INNER JOIN (BROADCAST)\n" +
                        "  |  hash predicates:\n" +
                        "  |  colocate: false, reason: \n" +
                        "  |  equal join conjunct: 2: id = 5: id",
                "  4:OlapScanNode\n" +
                        "     TABLE: join2\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 7: dt > 1",
                "  1:OlapScanNode\n" +
                        "     TABLE: join2\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 4: dt > 1");
    }

    @Test
    public void testConvertCaseWhenToConstant() throws Exception {
        // basic test
        String caseWhenSql = "select "
                + "case when date_format(now(),'%H%i')  < 123 then 1 else 0 end as col "
                + "from test.baseall "
                +
                "where k11 = case when date_format(now(),'%H%i')  < 123 then date_format(date_sub(now(),2),'%Y%m%d') else date_format(date_sub(now(),1),'%Y%m%d') end";
        Assert.assertFalse(StringUtils.containsIgnoreCase(getFragmentPlan(caseWhenSql), "CASE WHEN"));

        // test 1: case when then
        // 1.1 multi when in on `case when` and can be converted to constants
        String sql11 = "select case when false then 2 when true then 3 else 0 end as col11;";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql11), "constant exprs: \n         3"));

        // 1.2 multi `when expr` in on `case when` ,`when expr` can not be converted to constants
        String sql121 =
                "select case when false then 2 when substr(k7,2,1) then 3 else 0 end as col121 from test.baseall";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql121),
                "CASE WHEN FALSE THEN 2 WHEN CAST(substr(9: k7, 2, 1) AS BOOLEAN) THEN 3 ELSE 0 END"));

        // 1.2.2 when expr which can not be converted to constants in the first
        String sql122 =
                "select case when substr(k7,2,1) then 2 when false then 3 else 0 end as col122 from test.baseall";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql122),
                "CASE WHEN CAST(substr(9: k7, 2, 1) AS BOOLEAN) THEN 2 WHEN FALSE THEN 3 ELSE 0 END"));

        // 1.2.3 test return `then expr` in the middle
        String sql124 = "select case when false then 1 when true then 2 when false then 3 else 'other' end as col124";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql124), "constant exprs: \n         '2'"));

        // 1.3 test return null
        String sql3 = "select case when false then 2 end as col3";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql3), "constant exprs: \n         NULL"));

        // 1.3.1 test return else expr
        String sql131 = "select case when false then 2 when false then 3 else 4 end as col131";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql131), "constant exprs: \n         4"));

        // 1.4 nest `case when` and can be converted to constants
        String sql14 =
                "select case when (case when true then true else false end) then 2 when false then 3 else 0 end as col";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql14), "constant exprs: \n         2"));

        // 1.5 nest `case when` and can not be converted to constants
        String sql15 =
                "select case when case when substr(k7,2,1) then true else false end then 2 when false then 3 else 0 end as col from test.baseall";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql15),
                "CASE WHEN if(CAST(substr(9: k7, 2, 1) AS BOOLEAN), TRUE, FALSE) THEN 2 WHEN FALSE THEN 3 ELSE 0 END"));

        // 1.6 test when expr is null
        String sql16 = "select case when null then 1 else 2 end as col16;";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql16), "constant exprs: \n         2"));

        // test 2: case xxx when then
        // 2.1 test equal
        String sql2 = "select case 1 when 1 then 'a' when 2 then 'b' else 'other' end as col2;";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql2), "constant exprs: \n         'a'"));

        // FIXME(yan): following cases are correct, we have to fix for them.
        // 2.1.2 test not equal
        String sql212 = "select case 'a' when 1 then 'a' when 'a' then 'b' else 'other' end as col212;";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql212), "constant exprs: \n         'b'"));

        // 2.2 test return null
        String sql22 = "select case 'a' when 1 then 'a' when 'b' then 'b' end as col22;";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql22), "constant exprs: \n         NULL"));

        // 2.2.2 test return else
        String sql222 = "select case 1 when 2 then 'a' when 3 then 'b' else 'other' end as col222;";
        Assert.assertTrue(
                StringUtils.containsIgnoreCase(getFragmentPlan(sql222), "constant exprs: \n         'other'"));

        // 2.3 test can not convert to constant,middle when expr is not constant
        String sql23 =
                "select case 'a' when 'b' then 'a' when substr(k7,2,1) then 2 when false then 3 else 0 end as col23 from test.baseall";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql23),
                "CASE'a' WHEN 'b' THEN 'a' WHEN substr(9: k7, 2, 1) THEN '2' WHEN '0' THEN '3' ELSE '0' END"));

        // 2.3.1  first when expr is not constant
        String sql231 =
                "select case 'a' when substr(k7,2,1) then 2 when 1 then 'a' when false then 3 else 0 end as col231 from test.baseall";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql231),
                "CASE'a' WHEN substr(9: k7, 2, 1) THEN '2' WHEN '1' THEN 'a' WHEN '0' THEN '3' ELSE '0' END"));

        // 2.3.2 case expr is not constant
        String sql232 =
                "select case k1 when substr(k7,2,1) then 2 when 1 then 'a' when false then 3 else 0 end as col232 from test.baseall";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql232),
                "CASECAST(1: k1 AS VARCHAR) WHEN substr(9: k7, 2, 1) THEN '2' WHEN '1' THEN 'a' WHEN '0' THEN '3' ELSE '0' END"));

        // 3.1 test float,float in case expr
        String sql31 = "select case cast(100 as float) when 1 then 'a' when 2 then 'b' else 'other' end as col31;";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql31), "constant exprs: \n         'other'"));

        // 4.1 test null in case expr return else
        String sql41 = "select case null when 1 then 'a' when 2 then 'b' else 'other' end as col41";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql41), "constant exprs: \n         'other'"));

        // 4.1.2 test null in case expr return null
        String sql412 = "select case null when 1 then 'a' when 2 then 'b' end as col41";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql412), "constant exprs: \n         NULL"));

        // 4.2.1 test null in when expr
        String sql421 = "select case 'a' when null then 'a' else 'other' end as col421";
        Assert.assertTrue(
                StringUtils.containsIgnoreCase(getFragmentPlan(sql421), "constant exprs: \n         'other'"));
    }

    @Test
    public void testJoinPredicateTransitivityWithSubqueryInWhereClause() throws Exception {
        String sql = "SELECT *\n" +
                "FROM test.pushdown_test\n" +
                "WHERE 0 < (\n" +
                "    SELECT MAX(k9)\n" +
                "    FROM test.pushdown_test);";
        starRocksAssert.query(sql).explainContains("  4:CROSS JOIN\n" +
                        "  |  cross join:\n" +
                        "  |  predicates is NULL",
                "  2:AGGREGATE (update finalize)\n" +
                        "  |  output: max(22: k9)\n" +
                        "  |  group by: \n" +
                        "  |  having: CAST(23: max(22: k9) AS DOUBLE) > 0.0");
    }

    @Test
    public void testDistinctPushDown() throws Exception {
        String sql = "select distinct k1 from (select distinct k1 from test.pushdown_test) t where k1 > 1";
        starRocksAssert.query(sql).explainContains("  RESULT SINK\n" +
                "\n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  group by: 1: k1\n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  0:OlapScanNode");
    }

    @Test
    public void testOrCompoundPredicateFold() throws Exception {
        String queryStr = "select * from baseall where (k1 > 1) or (k1 > 1 and k2 < 1)";
        String explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("PREDICATES: 1: k1 > 1"));

        queryStr = "select * from  baseall where (k1 > 1 and k2 < 1) or  (k1 > 1)";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("PREDICATES: 1: k1 > 1\n"));

        queryStr = "select * from  baseall where (k1 > 1) or (k1 > 1)";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("PREDICATES: 1: k1 > 1\n"));
    }

    @Test
    public void testColocateJoin2() throws Exception {
        String queryStr =
                "select * from test.colocate1 t1, test.colocate2 t2 where t1.k1 = t2.k1 and t1.k2 = t2.k2 and t1.k3 = t2.k3";
        String explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: true"));

        queryStr =
                "select * from test.colocate1 t1 join (select k1, k2 from test.colocate2 group by k1, k2) t2 on t1.k1 = t2.k1 and t1.k2 = t2.k2";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: true"));

        queryStr =
                "select * from test.colocate1 t1 join (select k1, k2 from test.colocate2 group by k1, k2, k3) t2 on t1.k1 = t2.k1 and t1.k2 = t2.k2";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: true"));

        queryStr =
                "select * from (select k1, k2 from test.colocate1 group by k1, k2) t1 join (select k1, k2 from test.colocate2 group by k1, k2) t2 on t1.k1 = t2.k1 and t1.k2 = t2.k2";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: true"));

        queryStr =
                "select * from test.colocate1 t1 join [shuffle] test.colocate2 t2 on t1.k1 = t2.k1 and t1.k2 = t2.k2";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: false"));

        // t1.k1 = t2.k2 not same order with distribute column
        queryStr =
                "select * from test.colocate1 t1, test.colocate2 t2 where t1.k1 = t2.k2 and t1.k2 = t2.k1 and t1.k3 = t2.k3";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: false"));

        queryStr = "select * from test.colocate1 t1, test.colocate2 t2 where t1.k2 = t2.k2";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: false"));

        queryStr = "select count(*) from test.colocate1 t1 group by t1.k1, t1.k2";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("1:AGGREGATE (update finalize)"));
        Assert.assertFalse(explainString.contains("3:AGGREGATE (merge finalize)"));

        queryStr = "select count(*) from test.colocate1 t1 group by t1.k1";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("1:AGGREGATE (update finalize)"));
    }

    @Test
    public void testSelfColocateJoin() throws Exception {
        // single partition
        String queryStr = "select * from test.jointest t1, test.jointest t2 where t1.k1 = t2.k1";
        String explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: true"));

        // multi partition
        queryStr = "select * from test.dynamic_partition t1, test.dynamic_partition t2 where t1.k1 = t2.k1";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: true"));
    }

    @Test
    public void testJoinWithMysqlTable() throws Exception {
        // set data size and row count for the olap table
        Database db = Catalog.getCurrentCatalog().getDb("default_cluster:test");
        OlapTable tbl = (OlapTable) db.getTable("jointest");
        for (Partition partition : tbl.getPartitions()) {
            partition.updateVisibleVersionAndVersionHash(2, 0);
            for (MaterializedIndex mIndex : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE)) {
                mIndex.setRowCount(10000);
                for (Tablet tablet : mIndex.getTablets()) {
                    for (Replica replica : tablet.getReplicas()) {
                        replica.updateVersionInfo(2, 0, 200000, 10000);
                    }
                }
            }
        }

        String queryStr = "select * from mysql_table t2, jointest t1 where t1.k1 = t2.k1";
        String explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("INNER JOIN (BUCKET_SHUFFLE)"));
        Assert.assertTrue(explainString.contains("1:SCAN MYSQL"));

        queryStr = "select * from jointest t1, mysql_table t2 where t1.k1 = t2.k1";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("INNER JOIN (BUCKET_SHUFFLE)"));
        Assert.assertTrue(explainString.contains("1:SCAN MYSQL"));

        queryStr = "select * from jointest t1, mysql_table t2, mysql_table t3 where t1.k1 = t3.k1";
        explainString = getFragmentPlan(queryStr);
        Assert.assertFalse(explainString.contains("INNER JOIN (BUCKET_SHUFFLE))"));
        Assert.assertTrue(explainString.contains("1:SCAN MYSQL"));
    }

    @Test
    public void testConstPredicateInRightJoin() throws Exception {
        String sql = "select * from test.join1 right join test.join2 on join1.id = join2.id where round(2.0, 0) > 3.0";
        String explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("  2:OlapScanNode\n" +
                "     TABLE: join2\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: round(2.0, 0) > 3.0"));

        sql = "select * from test.join1 right semi join test.join2 on join1.id = join2.id where round(2.0, 0) > 3.0";
        explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("  0:OlapScanNode\n" +
                "     TABLE: join2\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: round(2.0, 0) > 3.0"));

        sql = "select * from test.join1 right anti join test.join2 on join1.id = join2.id where round(2.0, 0) > 3.0";
        explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("  0:OlapScanNode\n" +
                "     TABLE: join2\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: round(2.0, 0) > 3.0"));

        sql = "select * from test.join1 left join test.join2 on join1.id = join2.id where round(2.0, 0) > 3.0";
        explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("  2:OlapScanNode\n" +
                "     TABLE: join1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: round(2.0, 0) > 3.0"));

        sql = "select * from test.join1 left semi join test.join2 on join1.id = join2.id where round(2.0, 0) > 3.0";
        explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("  0:OlapScanNode\n" +
                "     TABLE: join1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: round(2.0, 0) > 3.0"));

        sql = "select * from test.join1 left anti join test.join2 on join1.id = join2.id where round(2.0, 0) > 3.0";
        explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("  0:OlapScanNode\n" +
                "     TABLE: join1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: round(2.0, 0) > 3.0"));

        sql = "select * from test.join1 inner join test.join2 on join1.id = join2.id where round(2.0, 0) > 3.0";
        explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("  0:OlapScanNode\n" +
                "     TABLE: join1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: round(2.0, 0) > 3.0"));

        sql = "select * from test.join1 where round(2.0, 0) > 3.0";
        explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("  0:OlapScanNode\n" +
                "     TABLE: join1\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: round(2.0, 0) > 3.0"));
    }

    @Test
    public void testExistsRewrite() throws Exception {
        String sql =
                "select count(*) FROM  test.join1 WHERE  EXISTS (select max(id) from test.join2 where join2.id = join1.id)";
        String explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("LEFT SEMI JOIN"));
    }

    @Test
    public void TestSemiJoinNameResolve() {
        String sql = "select join1.dt from  test.join1 right semi join test.join2 on join1.id = join2.id";
        starRocksAssert.query(sql).analysisError("Column '`join1`.`dt`' cannot be resolved");

        sql = "select a.dt from test.join1 a left ANTI join test.join2 b on a.id = b.id, " +
                "test.join1 c right ANTI join test.join2 d on c.id = d.id";
        starRocksAssert.query(sql).analysisError("Column '`a`.`dt`' cannot be resolved");
    }

    @Test
    public void TestConstantConjunct() throws Exception {
        String sql =
                "select * from  test.join1 where ST_Contains(\"\", APPEND_TRAILING_CHAR_IF_ABSENT(-1338745708, \"RDBLIQK\") )";
        String explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString
                .contains("PREDICATES: st_contains('', append_trailing_char_if_absent('-1338745708', 'RDBLIQK'))"));
    }

    @Test
    public void TestJoinOnBitmapColumn() {
        String sql = "select * from test.bitmap_table a join test.bitmap_table b on a.id2 = b.id2";
        starRocksAssert.query(sql).analysisError("binary type bitmap with type varchar(-1) is invalid.");

        sql = "select * from test.bitmap_table a join test.bitmap_table b on a.id2 = b.id";
        starRocksAssert.query(sql).analysisError("binary type bitmap with type double is invalid.");

        sql = "select * from test.bitmap_table a join test.hll_table b on a.id2 = b.id2";
        starRocksAssert.query(sql).analysisError("binary type bitmap with type varchar(-1) is invalid.");

        sql = "select * from test.bitmap_table a join test.hll_table b where a.id2 in (1, 2, 3)";
        starRocksAssert.query(sql).analysisError("HLL, BITMAP and PERCENTILE type couldn't as Predicate");
    }

    @Test
    public void testLeftOuterJoinOnOrPredicate() throws Exception {
        connectContext.setDatabase("default_cluster:test");

        String sql = "select * from join1 left join join2 on join1.id = join2.id\n" +
                "and (join2.id > 1 or join2.id < 10);";
        String explainString = getFragmentPlan(sql);

        Assert.assertTrue(explainString.contains("join op: LEFT OUTER JOIN (BROADCAST)"));
        Assert.assertTrue(explainString.contains("PREDICATES: (5: id > 1) OR (5: id < 10)"));
        Assert.assertTrue(explainString.contains("equal join conjunct: 2: id = 5: id"));
    }

    @Test
    public void testMultiCountDistinct() throws Exception {
        String queryStr = "select count(distinct k1, k2) from baseall group by k3";
        String explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("group by: 1: k1, 2: k2, 3: k3"));

        queryStr = "select count(distinct k1) from baseall group by k3";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("12: count(distinct 1: k1)"));
        Assert.assertTrue(explainString.contains("multi_distinct_count(1: k1)"));
        Assert.assertTrue(explainString.contains("group by: 3: k3"));

        queryStr = "select count(distinct k1) from baseall";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("multi_distinct_count(1: k1)"));

        queryStr = "select count(distinct k1, k2),  count(distinct k4) from baseall group by k3";
        starRocksAssert.query(queryStr).analysisError(
                "The query contains multi count distinct or sum distinct, each can't have multi columns.");
    }

    @Test
    public void testMultiNotExistPredicatePushDown() throws Exception {
        connectContext.setDatabase("default_cluster:test");

        String sql =
                "select * from join1 where join1.dt > 1 and NOT EXISTS (select * from join1 as a where join1.dt = 1 and a.id = join1.id)" +
                        "and NOT EXISTS (select * from join1 as a where join1.dt = 2 and a.id = join1.id);";
        String explainString = getFragmentPlan(sql);

        Assert.assertTrue(explainString.contains("  5:HASH JOIN\n" +
                "  |  join op: RIGHT ANTI JOIN (COLOCATE)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: true\n" +
                "  |  equal join conjunct: 9: id = 2: id\n" +
                "  |  other join predicates: 1: dt = 2"));
        Assert.assertTrue(explainString.contains("  |    3:HASH JOIN\n" +
                "  |    |  join op: LEFT ANTI JOIN (COLOCATE)\n" +
                "  |    |  hash predicates:\n" +
                "  |    |  colocate: true\n" +
                "  |    |  equal join conjunct: 2: id = 5: id\n" +
                "  |    |  other join predicates: 1: dt = 1"));
        Assert.assertTrue(explainString.contains("  |    1:OlapScanNode\n" +
                "  |       TABLE: join1\n" +
                "  |       PREAGGREGATION: ON\n" +
                "  |       PREDICATES: 1: dt > 1"));
    }

    @Test
    public void testJoinReorderTakeEffect() throws Exception {
        Catalog catalog = connectContext.getCatalog();
        Database db = catalog.getDb("default_cluster:test");
        Table table = db.getTable("join2");
        OlapTable olapTable1 = (OlapTable) table;
        new Expectations(olapTable1) {
            {
                olapTable1.getRowCount();
                result = 2L;
                minTimes = 0;
            }
        };
        String sql = "select * from join1 join join2 on join1.id = join2.id;";
        String explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("  3:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: id = 5: id"));
        Assert.assertTrue(explainString.contains("  0:OlapScanNode\n" +
                "     TABLE: join1"));
        Assert.assertTrue(explainString.contains("  1:OlapScanNode\n" +
                "     TABLE: join2"));
    }

    @Test
    public void testJoinReorderWithWithClause() throws Exception {
        connectContext.setDatabase("default_cluster:test");
        Catalog catalog = connectContext.getCatalog();
        Table table = catalog.getDb("default_cluster:test").getTable("join2");
        OlapTable olapTable1 = (OlapTable) table;
        new Expectations(olapTable1) {
            {
                olapTable1.getRowCount();
                result = 2L;
                minTimes = 0;
            }
        };
        String sql =
                "WITH t_temp AS (select join1.id as id1,  join2.id as id2 from join1 join join2 on join1.id = join2.id) select * from t_temp";
        String explainString = getFragmentPlan(sql);
        Assert.assertTrue(explainString.contains("equal join conjunct: 2: id = 5: id"));
        Assert.assertTrue(explainString.contains("  |----2:EXCHANGE\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: join1"));
        Assert.assertTrue(explainString.contains("  1:OlapScanNode\n" +
                "     TABLE: join2\n" +
                "     PREAGGREGATION: ON"));
    }

    @Test
    public void testMultiCountDistinctType() throws Exception {
        String sql = "select count(distinct t1a,t1b) from test_all_type";
        String plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains("3:AGGREGATE (update serialize)\n" +
                "  |  aggregate: count[(if[(1: t1a IS NULL, NULL, [2: t1b, SMALLINT, true]); args: BOOLEAN,SMALLINT,SMALLINT; result: SMALLINT; args nullable: true; result nullable: true]); args: SMALLINT; result: BIGINT; args nullable: true; result nullable: false]"));
        Assert.assertTrue(plan.contains("5:AGGREGATE (merge finalize)\n" +
                "  |  aggregate: count[([11: count(distinct 1: t1a, 2: t1b), BIGINT, false]); args: SMALLINT; result: BIGINT; args nullable: true; result nullable: false]"));
    }

    @Test
    public void testMultiCountDistinctAggPhase() throws Exception {
        String sql = "select count(distinct t1a,t1b), avg(t1c) from test_all_type";
        String plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains("3:AGGREGATE (update serialize)\n" +
                "  |  aggregate: count[(if[(1: t1a IS NULL, NULL, [2: t1b, SMALLINT, true]); args: BOOLEAN,SMALLINT,SMALLINT; result: SMALLINT; args nullable: true; result nullable: true]); args: SMALLINT; result: BIGINT; args nullable: true; result nullable: false], avg[([12: avg(3: t1c), DOUBLE, true]); args: INT; result: VARCHAR; args nullable: true; result nullable: true]"));
        Assert.assertTrue(plan.contains("2:AGGREGATE (merge serialize)\n" +
                "  |  aggregate: avg[([12: avg(3: t1c), VARCHAR, true]); args: INT; result: DOUBLE; args nullable: true; result nullable: true]\n" +
                "  |  group by: [1: t1a, VARCHAR, true], [2: t1b, SMALLINT, true]"));
    }

    @Test
    public void testCaseWhenType2() throws Exception {
        String sql =
                "select case '10000' when 10000 THEN 'TEST1' WHEN NULL THEN 'TEST2' WHEN 40000 THEN 'TEST4' END FROM t1;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:Project\n"
                + "  |  <slot 4> : 'TEST1'"));
    }

    @Test
    public void testDecimalV3Distinct() throws Exception {
        String sql = "select avg(t1c), count(distinct id_decimal) from test_all_type;";
        String plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains(
                "multi_distinct_count[([10: id_decimal, DECIMAL64(10,2), true]); args: DECIMAL64; result: BIGINT; args nullable: true; result nullable: false]"));
    }

    @Test
    public void testAddProjectForJoinPrune() throws Exception {
        String sql = "select\n" +
                "    c_custkey,\n" +
                "    c_name,\n" +
                "    sum(l_extendedprice * (1 - l_discount)) as revenue,\n" +
                "    c_acctbal,\n" +
                "    n_name,\n" +
                "    c_address,\n" +
                "    c_phone,\n" +
                "    c_comment\n" +
                "from\n" +
                "    customer,\n" +
                "    orders,\n" +
                "    lineitem,\n" +
                "    nation\n" +
                "where\n" +
                "        c_custkey = o_custkey\n" +
                "  and l_orderkey = o_orderkey\n" +
                "  and o_orderdate >= date '1994-05-01'\n" +
                "  and o_orderdate < date '1994-08-01'\n" +
                "  and l_returnflag = 'R'\n" +
                "  and c_nationkey = n_nationkey\n" +
                "group by\n" +
                "    c_custkey,\n" +
                "    c_name,\n" +
                "    c_acctbal,\n" +
                "    c_phone,\n" +
                "    n_name,\n" +
                "    c_address,\n" +
                "    c_comment\n" +
                "order by\n" +
                "    revenue desc limit 20;";
        String plan = getCostExplain(sql);
        Assert.assertTrue(plan.contains("8:Project\n" +
                "  |  output columns:\n" +
                "  |  11 <-> [11: O_CUSTKEY, INT, false]\n" +
                "  |  25 <-> [25: L_EXTENDEDPRICE, DOUBLE, false]\n" +
                "  |  26 <-> [26: L_DISCOUNT, DOUBLE, false]\n" +
                "  |  cardinality: 0\n" +
                "  |  column statistics: \n" +
                "  |  * O_CUSTKEY-->[1.0, 149999.0, 0.0, 8.0, 99996.0]\n" +
                "  |  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0]\n" +
                "  |  * L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0]\n" +
                "  |  \n" +
                "  7:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  equal join conjunct: [20: L_ORDERKEY, INT, false] = [10: O_ORDERKEY, INT, false]\n" +
                "  |  build runtime filters:\n" +
                "  |  - filter_id = 0, build_expr = (10: O_ORDERKEY), remote = false\n" +
                "  |  cardinality: 0\n" +
                "  |  column statistics: \n" +
                "  |  * O_ORDERKEY-->[1.0, 6000000.0, 0.0, 8.0, 1500000.0]\n" +
                "  |  * O_CUSTKEY-->[1.0, 149999.0, 0.0, 8.0, 99996.0]\n" +
                "  |  * L_ORDERKEY-->[1.0, 6000000.0, 0.0, 8.0, 1500000.0]\n" +
                "  |  * L_EXTENDEDPRICE-->[901.0, 104949.5, 0.0, 8.0, 932377.0]\n" +
                "  |  * L_DISCOUNT-->[0.0, 0.1, 0.0, 8.0, 11.0]"));
    }

    @Test
    public void testUnionAll() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "select * from t1 union all select * from t2;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("    EXCHANGE ID: 04\n" +
                "    RANDOM\n" +
                "\n" +
                "  3:OlapScanNode\n" +
                "     TABLE: t2"));
        Assert.assertTrue(plan.contains("    EXCHANGE ID: 02\n" +
                "    RANDOM\n" +
                "\n" +
                "  1:OlapScanNode\n" +
                "     TABLE: t1"));
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testShuffleHashBucket() throws Exception {
        String sql = "SELECT COUNT(*)\n" +
                "FROM lineitem JOIN [shuffle] orders o1 ON l_orderkey = o1.o_orderkey\n" +
                "JOIN [shuffle] orders o2 ON l_orderkey = o2.o_orderkey";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("join op: INNER JOIN (BUCKET_SHUFFLE(S))"));
    }

    @Test
    public void testShuffleHashBucket2() throws Exception {
        String sql = "select count(1) from lineitem t1 join [shuffle] orders t2 on " +
                "t1.l_orderkey = t2.o_orderkey and t2.O_ORDERDATE = t1.L_SHIPDATE join [shuffle] orders t3 " +
                "on t1.l_orderkey = t3.o_orderkey and t3.O_ORDERDATE = t1.L_SHIPDATE join [shuffle] orders t4 on\n" +
                "t1.l_orderkey = t4.o_orderkey and t4.O_ORDERDATE = t1.L_SHIPDATE;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("12:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))"));
        Assert.assertTrue(plan.contains("8:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))"));
        Assert.assertTrue(plan.contains("4:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)"));
    }

    @Test
    public void testSemiJoinPredicateDerive() throws Exception {
        String sql = "select * from t0 left semi join t1 on v1 = v4 where v1 = 2";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 1: v1 = 2"));
    }

    @Test
    public void testMergeAggregateNormal() throws Exception {
        String sql;
        String plan;

        sql = "select distinct x1 from (select distinct v1 as x1 from t0) as q";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  RESULT SINK\n" +
                "\n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  group by: 1: v1\n" +
                "  |  use vectorized: true"));

        sql = "select sum(x1) from (select sum(v1) as x1 from t0) as q";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  RESULT SINK\n" +
                "\n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(1: v1)\n" +
                "  |  group by: \n" +
                "  |  use vectorized: true"));

        sql = "select SUM(x1) from (select v2, sum(v1) as x1 from t0 group by v2) as q";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  RESULT SINK\n" +
                "\n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(1: v1)\n" +
                "  |  group by: \n" +
                "  |  use vectorized: true"));

        sql = "select v2, SUM(x1) from (select v2, v3, sum(v1) as x1 from t0 group by v2, v3) as q group by v2";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(1: v1)\n" +
                "  |  group by: 2: v2\n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0"));

        sql = "select SUM(x1) from (select v2, sum(distinct v1), sum(v3) as x1 from t0 group by v2) as q";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(3: v3)\n" +
                "  |  group by: \n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0"));
    }

    @Test
    public void testMergeAggregateFailed() throws Exception {
        String sql;
        String plan;
        sql = "select avg(x1) from (select avg(v1) as x1 from t0) as q";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:AGGREGATE (update finalize)\n" +
                "  |  output: avg(1: v1)\n" +
                "  |  group by: \n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  0:OlapScanNode"));

        sql = "select SUM(v2) from (select v2, sum(v1) as x1 from t0 group by v2) as q";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  2:AGGREGATE (update finalize)\n" +
                "  |  output: sum(2: v2)\n" +
                "  |  group by: \n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  group by: 2: v2\n" +
                "  |  use vectorized: true\n"));
        sql = "select SUM(v2) from (select v2, sum(distinct v2) as x1 from t0 group by v2) as q";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  2:AGGREGATE (update finalize)\n" +
                "  |  output: sum(2: v2)\n" +
                "  |  group by: \n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  group by: 2: v2\n" +
                "  |  use vectorized: true\n"));
        sql = "select sum(distinct x1) from (select v2, sum(v2) as x1 from t0 group by v2) as q";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(2: v2)\n" +
                "  |  group by: 2: v2\n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  0:OlapScanNode\n"));
    }

    @Test
    public void testReplicatedJoin() throws Exception {
        connectContext.getSessionVariable().setEnableReplicationJoin(true);
        String sql = "select * from join1 join join2 on join1.id = join2.id;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("join op: INNER JOIN (REPLICATED)"));
        Assert.assertFalse(plan.contains("EXCHANGE"));

        sql = "select * from join2 right join join1 on join1.id = join2.id;";
        plan = getFragmentPlan(sql);
        Assert.assertFalse(plan.contains("join op: INNER JOIN (REPLICATED)"));

        sql = "select * from join1 as a join (select sum(id),id from join2 group by id) as b on a.id = b.id;";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("join op: INNER JOIN (REPLICATED)"));
        Assert.assertFalse(plan.contains("EXCHANGE"));

        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        sql = "select * from join1 as a join (select sum(id),dt from join2 group by dt) as b on a.id = b.dt;";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("join op: INNER JOIN (BROADCAST)"));
        Assert.assertTrue(plan.contains("EXCHANGE"));
        connectContext.getSessionVariable().setNewPlanerAggStage(0);

        sql = "select a.* from join1 as a join join1 as b ;";
        plan = getFragmentPlan(sql);
        Assert.assertFalse(plan.contains("EXCHANGE"));

        sql = "select a.* from join1 as a join (select sum(id) from join1 group by dt) as b ;";
        plan = getFragmentPlan(sql);
        Assert.assertFalse(plan.contains("EXCHANGE"));

        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        sql = "select a.* from join1 as a join (select sum(id) from join1 group by dt) as b ;";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("EXCHANGE"));
        connectContext.getSessionVariable().setNewPlanerAggStage(0);

        connectContext.getSessionVariable().setEnableReplicationJoin(false);
    }

    @Test
    public void testReplicationJoinWithPartitionTable() throws Exception {
        connectContext.getSessionVariable().setEnableReplicationJoin(true);
        boolean oldValue = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = true;
        String sql = "select * from join1 join pushdown_test on join1.id = pushdown_test.k1;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("INNER JOIN (BROADCAST)"));
        FeConstants.runningUnitTest = oldValue;
        connectContext.getSessionVariable().setEnableReplicationJoin(false);
    }

    @Test
    public void testOuterJoinBucketShuffle() throws Exception {
        String sql = "SELECT DISTINCT t0.v1 FROM t0 RIGHT JOIN[BUCKET] t1 ON t0.v1 = t1.v4";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  6:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  group by: 1: v1\n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  5:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  4:HASH JOIN\n" +
                "  |  join op: RIGHT OUTER JOIN (PARTITIONED)\n" +
                "  |  hash predicates:"));

        sql = "SELECT DISTINCT t0.v1 FROM t0 FULL JOIN[BUCKET] t1 ON t0.v1 = t1.v4";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  4:HASH JOIN\n" +
                "  |  join op: FULL OUTER JOIN (PARTITIONED)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 4: v4\n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  |----3:EXCHANGE\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  1:EXCHANGE\n" +
                "     use vectorized: true"));

        sql = "SELECT DISTINCT t1.v4 FROM t0 LEFT JOIN[BUCKET] t1 ON t0.v1 = t1.v4";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  7:AGGREGATE (merge finalize)\n" +
                "  |  group by: 4: v4\n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  6:EXCHANGE\n" +
                "     use vectorized: true\n" +
                "\n" +
                "PLAN FRAGMENT 1\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 06\n" +
                "    HASH_PARTITIONED: 4: v4\n" +
                "\n" +
                "  5:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  group by: 4: v4\n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  4:Project\n" +
                "  |  <slot 4> : 4: v4\n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  3:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (BROADCAST)"));
    }

    @Test
    public void testSchemaScan() throws Exception {
        String sql = "select * from information_schema.columns";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  0:SCAN SCHEMA\n" +
                "     use vectorized: true"));
    }

    @Test
    public void testDuplicateAggregateFn() throws Exception {
        String sql = "select bitmap_union_count(b1) from test_object having count(distinct b1) > 2;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains(" OUTPUT EXPRS:13: bitmap_union_count(5: b1)\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  output: bitmap_union_count(5: b1)\n" +
                "  |  group by: \n" +
                "  |  having: 13: bitmap_union_count(5: b1) > 2"));
    }

    @Test
    public void testDuplicateAggregateFn2() throws Exception {
        String sql = "select bitmap_union_count(b1), count(distinct b1) from test_object;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  2:Project\n" +
                "  |  <slot 13> : 13: bitmap_union_count(5: b1)\n" +
                "  |  <slot 14> : 13: bitmap_union_count(5: b1)\n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  output: bitmap_union_count(5: b1)"));
    }

    @Test
    public void testNotExpr() throws Exception {
        String sql = "select v1 from t0 where not (v1 in (1, 2))";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("     PREDICATES: 1: v1 NOT IN (1, 2)"));

        sql = "select v1 from t0 where not (v1 > 2)";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PREDICATES: 1: v1 <= 2"));

        sql = "select v1 from t0 where not (v1 > 2)";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PREDICATES: 1: v1 <= 2"));

        sql = "select v1 from t0 where not (v1 > 2 and v2 < 3)";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PREDICATES: (1: v1 <= 2) OR (2: v2 >= 3)"));

        sql = "select v1 from t0 where not (v1 > 2 and v2 is null)";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PREDICATES: (1: v1 <= 2) OR (2: v2 IS NOT NULL)"));

        sql = "select v1 from t0 where not (v1 > 2 and v2 is null and v3 < 6)";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PREDICATES: ((1: v1 <= 2) OR (2: v2 IS NOT NULL)) OR (3: v3 >= 6)"));

        sql = "select v1 from t0 where not (v1 > 2 and if(v2 > 2, FALSE, TRUE))";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PREDICATES: (1: v1 <= 2) OR (NOT if(2: v2 > 2, FALSE, TRUE))"));

        sql = "select v1 from t0 where not (v1 > 2 or v2 is null or if(v3 > 2, FALSE, TRUE))";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(
                planFragment.contains("PREDICATES: 1: v1 <= 2, 2: v2 IS NOT NULL, NOT if(3: v3 > 2, FALSE, TRUE)"));
    }

    @Test
    public void testArithmeticCommutative() throws Exception {
        String sql = "select v1 from t0 where v1 + 2 > 3";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PREDICATES: 1: v1 > 1"));

        sql = "select v1 from t0 where  v1 / 2 <=> 3";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PREDICATES: CAST(1: v1 AS DOUBLE) <=> 6.0"));

        sql = "select v1 from t0 where  v1 / -2 > 3";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PREDICATES: CAST(1: v1 AS DOUBLE) < -6.0"));

        sql = "select v1 from t0 where  v1 / abs(-2) > 3";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PREDICATES: CAST(1: v1 AS DOUBLE) / CAST(abs(-2) AS DOUBLE) > 3.0"));

        sql = "select v1 from t0 where  v1 / -2 != 3";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PREDICATES: CAST(1: v1 AS DOUBLE) != -6.0"));

        sql = "select v1 from t0 where  v1 / abs(-2) = 3";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PREDICATES: CAST(1: v1 AS DOUBLE) = 3.0 * CAST(abs(-2) AS DOUBLE)"));

        sql = "select v1 from t0 where 2 + v1 <= 3";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PREDICATES: 1: v1 <= 1"));

        sql = "select v1 from t0 where 2 - v1 <= 3";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PREDICATES: 1: v1 >= -1"));

        sql = "select k5 from bigtable where k5 * 2 <= 3";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PREDICATES: CAST(5: k5 AS DECIMAL64(18,3)) * 2 <= 3"));

        sql = "select k5 from bigtable where 2 / k5 <= 3";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PREDICATES: 2 / CAST(5: k5 AS DECIMAL128(38,3)) <= 3"));

        sql = "select t1a from test_all_type where date_add(id_datetime, 2) = '2020-12-21'";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PREDICATES: 8: id_datetime = '2020-12-19 00:00:00'"));

        sql = "select t1a from test_all_type where date_sub(id_datetime, 2) = '2020-12-21'";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PREDICATES: 8: id_datetime = '2020-12-23 00:00:00'"));

        sql = "select t1a from test_all_type where years_sub(id_datetime, 2) = '2020-12-21'";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PREDICATES: 8: id_datetime = '2022-12-21 00:00:00'"));

        sql = "select t1a from test_all_type where years_add(id_datetime, 2) = '2020-12-21'";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PREDICATES: 8: id_datetime = '2018-12-21 00:00:00'"));
    }

    @Test
    public void testMetaScan() throws Exception {
        String sql = "select max(v1), min(v1) from t0 [_META_]";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  0:MetaScan\n" +
                "     <id 6> : max_v1\n" +
                "     <id 7> : min_v1"));

        String thrift = getThriftPlan(sql);
        Assert.assertTrue(thrift.contains("id_to_names:{6=max_v1, 7=min_v1}"));
    }

    @Test
    public void testMetaScan2() throws Exception {
        String sql = "select max(t1c), min(t1d), dict_merge(t1a) from test_all_type [_META_]";
        String plan = getFragmentPlan(sql);

        Assert.assertTrue(plan.contains("  0:MetaScan\n" +
                "     <id 16> : dict_merge_t1a\n" +
                "     <id 14> : max_t1c\n" +
                "     <id 15> : min_t1d"));

        String thrift = getThriftPlan(sql);
        Assert.assertTrue(thrift.contains("TFunctionName(function_name:dict_merge), " +
                "binary_type:BUILTIN, arg_types:[TTypeDesc(types:[TTypeNode(type:ARRAY), " +
                "TTypeNode(type:SCALAR, scalar_type:TScalarType(type:VARCHAR, len:-1))])]"));
    }

    @Test
    public void testLikeFunctionIdThrift() throws Exception {
        String sql = "select S_ADDRESS from supplier where S_ADDRESS " +
                "like '%Customer%Complaints%' ";
        String thrift = getThriftPlan(sql);
        Assert.assertTrue(thrift.contains("fid:60010"));
    }

    @Test
    public void testLimitRightJoin() throws Exception {
        String sql = "select v1 from t0 right outer join t1 on t0.v1 = t1.v4 limit 100";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  |  join op: RIGHT OUTER JOIN (PARTITIONED)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 4: v4\n" +
                "  |  limit: 100"));
        Assert.assertTrue(plan.contains("  |----3:EXCHANGE\n" +
                "  |       limit: 100"));

        sql = "select v1 from t0 full outer join t1 on t0.v1 = t1.v4 limit 100";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("join op: FULL OUTER JOIN (PARTITIONED)"));
    }

    @Test
    public void testLimitLeftJoin() throws Exception {
        String sql = "select v1 from (select * from t0 limit 1) x0 left outer join t1 on x0.v1 = t1.v4";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  |  join op: RIGHT OUTER JOIN (PARTITIONED)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 4: v4 = 1: v1"));
        Assert.assertTrue(plan.contains("  |----4:EXCHANGE\n" +
                "  |       limit: 1"));

        sql = "select v1 from (select * from t0 limit 10) x0 left outer join t1 on x0.v1 = t1.v4 limit 1";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  3:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 4: v4\n" +
                "  |  limit: 1\n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  |----2:EXCHANGE\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  0:OlapScanNode"));
        Assert.assertTrue(plan.contains("  0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1\n" +
                "     rollup: t0\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=1\n" +
                "     avgRowSize=1.0\n" +
                "     numNodes=0\n" +
                "     limit: 1"));

        sql = "select v1 from (select * from t0 limit 10) x0 left outer join t1 on x0.v1 = t1.v4 limit 100";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  5:HASH JOIN\n" +
                "  |  join op: RIGHT OUTER JOIN (PARTITIONED)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 4: v4 = 1: v1\n" +
                "  |  limit: 100\n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  |----4:EXCHANGE\n" +
                "  |       limit: 10\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  1:EXCHANGE"));
        Assert.assertTrue(plan.contains("PLAN FRAGMENT 1\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 04\n" +
                "    HASH_PARTITIONED: 1: v1\n" +
                "\n" +
                "  3:EXCHANGE\n" +
                "     limit: 10\n" +
                "     use vectorized: true\n"));

        sql =
                "select v1 from (select * from t0 limit 10) x0 left outer join (select * from t1 limit 5) x1 on x0.v1 = x1.v4 limit 7";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("join op: LEFT OUTER JOIN (BROADCAST)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 4: v4\n" +
                "  |  limit: 7\n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  |----3:EXCHANGE\n" +
                "  |       limit: 5\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  0:OlapScanNode"));
        Assert.assertTrue(plan.contains("PLAN FRAGMENT 1\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 03\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  2:EXCHANGE\n" +
                "     limit: 5\n" +
                "     use vectorized: true\n" +
                "\n" +
                "PLAN FRAGMENT 2"));
    }

    @Test
    public void testColocateCoverReplicate() throws Exception {
        String sql = "select * from join1 join join1 as xx on join1.id = xx.id;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  |  join op: INNER JOIN (COLOCATE)\n"));
    }

    @Test
    public void testReplicatedAgg() throws Exception {
        connectContext.getSessionVariable().setEnableReplicationJoin(true);

        String sql = "select value, SUM(id) from join1 group by value";
        String plan = getFragmentPlan(sql);
        System.out.println(plan);
        Assert.assertTrue(plan.contains("  1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(2: id)\n" +
                "  |  group by: 3: value\n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  0:OlapScanNode"));

        connectContext.getSessionVariable().setEnableReplicationJoin(false);
    }
  
    @Test  
    public void testUnionEmptyNode() throws Exception {
        String sql;
        String plan;
        sql = "select * from (select * from t0 union all select * from t1 union all select * from t2) as xx";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  0:UNION\n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  |----4:EXCHANGE\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  |----6:EXCHANGE\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  2:EXCHANGE\n" +
                "     use vectorized: true"));

        sql = "select * from (select * from t0 limit 0 union all select * from t1 union all select * from t2) as xx";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  0:UNION\n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  |----4:EXCHANGE\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  2:EXCHANGE\n" +
                "     use vectorized: true"));

        sql = "select * from (select * from t0 limit 0 union all select * from t1 where false" +
                " union all select * from t2) as xx";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:4: v1 | 5: v2 | 6: v3\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  1:Project\n" +
                "  |  <slot 4> : 10: v7\n" +
                "  |  <slot 5> : 11: v8\n" +
                "  |  <slot 6> : 12: v9\n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  0:OlapScanNode"));
    }

    @Test
    public void testIntersectEmptyNode() throws Exception {
        String sql;
        String plan;
        sql = "select * from (select * from t0 intersect select * from t1 intersect select * from t2) as xx";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  0:INTERSECT\n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  |----4:EXCHANGE\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  |----6:EXCHANGE\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  2:EXCHANGE"));

        sql = "select * from (select * from t0 limit 0 intersect select * from t1 intersect select * from t2) as xx";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:4: v1 | 5: v2 | 6: v3\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  0:EMPTYSET\n" +
                "     use vectorized: true"));

        sql = "select * from (select * from t0 limit 0 intersect select * from t1 where false " +
                "intersect select * from t2) as xx";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:4: v1 | 5: v2 | 6: v3\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  0:EMPTYSET\n" +
                "     use vectorized: true"));
    }

    @Test
    public void testExceptEmptyNode() throws Exception {
        String sql;
        String plan;
        sql = "select * from (select * from t0 except select * from t1 except select * from t2) as xx";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  0:EXCEPT\n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  |----4:EXCHANGE\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  |----6:EXCHANGE\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  2:EXCHANGE\n" +
                "     use vectorized: true"));

        sql = "select * from (select * from t0 limit 0 except select * from t1 except select * from t2) as xx";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:4: v1 | 5: v2 | 6: v3\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  0:EMPTYSET\n" +
                "     use vectorized: true"));

        sql = "select * from ( select * from t2 except select * from t0 limit 0 except " +
                "select * from t1) as xx";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  0:EXCEPT\n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  |----4:EXCHANGE\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  2:EXCHANGE\n" +
                "     use vectorized: true\n"));
    }
  
    @Test
    public void testPredicateOnRepeatNode() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "select * from (select v1, v2, sum(v3) from t0 group by rollup(v1, v2)) as xx where v1 is null;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains(" 1:REPEAT_NODE\n" +
                "  |  repeat: repeat 2 lines [[], [1], [1, 2]]\n" +
                "  |  PREDICATES: 1: v1 IS NULL"));
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testCaseWhen() throws Exception {
        String sql = "SELECT v1 FROM t0 WHERE CASE WHEN (v1 IS NOT NULL) THEN NULL END";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("PREDICATES: if(1: v1 IS NOT NULL, NULL, NULL)"));
    }
}
