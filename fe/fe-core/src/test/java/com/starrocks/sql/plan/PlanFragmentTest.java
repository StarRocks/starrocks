// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.plan;

import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.LocalTablet;
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
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

public class PlanFragmentTest extends PlanTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withTable("create table test_array(c0 INT, c1 array<varchar(65533)>)" +
                " duplicate key(c0) distributed by hash(c0) buckets 1 " +
                "properties('replication_num'='1');");
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
    public void testMergeTwoFilters() throws Exception {
        String sql = "select v1 from t0 where v2 < null group by v1 HAVING NULL IS NULL;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:AGGREGATE (update finalize)\n"
                + "  |  group by: 1: v1\n"
                + "  |  having: TRUE\n"));

        Assert.assertTrue(planFragment.contains("  0:EMPTYSET\n"));
    }

    @Test
    public void testLimitWithHaving() throws Exception {
        String sql = "SELECT v1, sum(v3) as v from t0 where v2 = 0 group by v1 having sum(v3) > 0 limit 10";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("having: 4: sum > 0"));
        Assert.assertTrue(planFragment.contains("limit: 10"));
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
        Assert.assertTrue(planFragment.contains("  |  group by: 1: v1\n"));
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

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

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
    public void testEmptySet() throws Exception {
        String queryStr = "select * from test.colocate1 t1, test.colocate2 t2 " +
                "where NOT NULL IS NULL";
        String explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("  0:EMPTYSET\n"));

        queryStr = "select * from test.colocate1 t1, test.colocate2 t2 where FALSE";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("  0:EMPTYSET\n"));
    }

    @Test
    public void testLimit0WithAgg() throws Exception {
        String queryStr = "select count(*) from t0 limit 0";
        String explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("OUTPUT EXPRS:4: count"));
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
    public void testProjectFilterRewrite() throws Exception {
        String queryStr = "select 1 as b, MIN(v1) from t0 having (b + 1) != b;";
        String explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("  1:AGGREGATE (update finalize)\n"
                + "  |  output: min(1: v1)\n"
                + "  |  group by: \n"
                + "  |  having: TRUE\n"));
    }

    @Test
    public void testUnionLimit() throws Exception {
        String queryStr = "select 1 from (select 4, 3 from t0 union all select 2, 3 ) as a limit 3";
        String explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("  2:Project\n"
                + "  |  <slot 4> : 4\n"
                + "  |  limit: 3\n"
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
                + "  |  \n"
                + "  0:EXCEPT\n"
                + "  |  limit: 3\n"));

        Assert.assertTrue(explainString.contains("  2:Project\n"
                + "  |  <slot 4> : 1\n"
                + "  |  <slot 5> : 3\n"
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
                + "  |  \n"
                + "  0:INTERSECT\n"
                + "  |  limit: 3\n"));

        Assert.assertTrue(explainString.contains("  2:Project\n"
                + "  |  <slot 4> : 1\n"
                + "  |  <slot 5> : 3\n"
                + "  |  \n"
                + "  1:OlapScanNode"));
    }

    @Test
    public void testSort() throws Exception {
        String sql = "select count(*) from (select L_QUANTITY, L_PARTKEY, L_ORDERKEY from lineitem " +
                "order by L_QUANTITY, L_PARTKEY, L_ORDERKEY limit 5000, 10000) as a;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("2:MERGING-EXCHANGE"));
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
        Assert.assertTrue(plan.contains("equal join conjunct: 7: t1a = 17: cast"));
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
    public void testEquivalenceTest() throws Exception {
        String sql = "select * from t0 as x1 join t0 as x2 on x1.v2 = x2.v2 where x2.v2 = 'zxcv';";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: CAST(2: v2 AS VARCHAR(1048576)) = 'zxcv'"));
        Assert.assertTrue(plan.contains("  1:OlapScanNode\n"
                + "     TABLE: t0\n"
                + "     PREAGGREGATION: ON\n"
                + "     PREDICATES: CAST(5: v2 AS VARCHAR(1048576)) = 'zxcv'\n"));
    }
    //

    @Test
    public void testCountStarWithLimitForOneAggStage() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        String sql = "select count(*) from (select v1 from t0 order by v2 limit 10,20) t;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("3:AGGREGATE (update finalize)"));
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
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
        System.out.println(plan);
        Assert.assertTrue(plan.contains("|  equal join conjunct: 1: v1 = 4: v4"));
        Assert.assertTrue(plan.contains("     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: CAST(1: v1 AS VARCHAR(65533)) = CAST(1: v1 AS VARCHAR(1048576))\n" +
                "     partitions=0/1\n"));
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
        Assert.assertTrue(plan.contains("join op: INNER JOIN (PARTITIONED)"));
    }

    @Test
    public void testJoinWithLimitSubQuery5() throws Exception {
        String sql = "select * from (select v1, v2 from t0 limit 10) a join [shuffle] " +
                "(select v4 from t1 ) b on a.v1 = b.v4";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("join op: INNER JOIN (PARTITIONED)"));
    }

    @Test
    public void testUnionWithLimitSubQuery() throws Exception {
        String sql = "select v1, v2 from t0 union all " +
                "select v1, v2 from t0 limit 1 ";
        String plan = getFragmentPlan(sql);
        System.out.println(plan);
        Assert.assertTrue(plan.contains("    EXCHANGE ID: 02\n" +
                "    UNPARTITIONED"));
        Assert.assertTrue(plan.contains("    EXCHANGE ID: 05\n" +
                "    UNPARTITIONED"));

        sql = "select v1, v2 from t0 union all " +
                "select a.v1, a.v2 from (select v1, v2 from t0 limit 1) a ";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("    EXCHANGE ID: 04\n" +
                "    UNPARTITIONED"));
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
                        + "(slot_id:1, tuple_id:2), output_scale:-1, output_column:-1, "
                        + "has_nullable_child:false, is_nullable:true, is_monotonic:true)])]"));
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
                + "  |  \n"
                + "  |----2:EXCHANGE\n"
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
                + "  |  \n"
                + "  |----2:EXCHANGE\n"
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
                + "  |  \n"
                + "  |----2:EXCHANGE\n"
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
                "  |  \n" +
                "  |----2:EXCHANGE\n" +
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
                + "  |  \n"
                + "  |----3:EXCHANGE\n"
                + "  |       limit: 10\n"
                + "  |    \n"
                + "  1:EXCHANGE\n"
                + "     limit: 10\n"));

        sql = "select * from t0, t1 limit 10";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("3:CROSS JOIN\n" +
                "  |  cross join:\n" +
                "  |  predicates is NULL.\n" +
                "  |  limit: 10\n" +
                "  |  \n" +
                "  |----2:EXCHANGE\n" +
                "  |       limit: 10\n" +
                "  |    \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0"));
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
                + "  |  \n"
                + "  1:SELECT\n"
                + "  |  predicates: 1: v1 > 2\n"
                + "  |  limit: 2\n"
                + "  |  \n"
                + "  0:OlapScanNode\n"
                + "     TABLE: t0"));
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
    public void testOrderBySameColumnDiffOrder() throws Exception {
        String sql = "select v1 from t0 order by v1 desc, v1 asc";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("1:SORT\n" +
                "  |  order by: <slot 1> 1: v1 DESC"));
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
        Assert.assertTrue(plan.contains("6:AGGREGATE (merge finalize)"));

        sql = "select count(distinct t1b,t1c), count(distinct t1b,t1c) from test_all_type group by t1d";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("4:AGGREGATE (update finalize)"));
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
                "  |  <slot 5> : 4: expr + 1\n"));
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

        sql = "select [v1,v2] from t0";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("1:Project\n" +
                "  |  <slot 4> : ARRAY<bigint(20)>[1: v1,2: v2]"));

        sql = "select [v1 = 1, v2 = 2, true] from t0";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("<slot 4> : ARRAY<boolean>[1: v1 = 1,2: v2 = 2,TRUE]"));
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
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 4> : 3: v3[1] + CAST(ARRAY<tinyint(4)>[1,2,3][1] AS BIGINT)\n" +
                "  |  <slot 5> : 3: v3[1]\n"));
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
                .explainContains("OUTPUT EXPRS:3: count",
                        "1:AGGREGATE (update finalize)", "output: count(2: id2)", "group by:", "0:OlapScanNode",
                        "PREAGGREGATION: OFF. Reason: Aggregate Operator not match: COUNT <--> BITMAP_UNION");

        starRocksAssert.query("select group_concat(id2) from test.bitmap_table;")
                .analysisError("No matching function with signature: group_concat(bitmap).");

        starRocksAssert.query("select sum(id2) from test.bitmap_table;").analysisError(
                "No matching function with signature: sum(bitmap).");

        starRocksAssert.query("select avg(id2) from test.bitmap_table;")
                .analysisError("No matching function with signature: avg(bitmap).");

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

        starRocksAssert.query("select count(id2) from test.hll_table;").explainContains("OUTPUT EXPRS:3: count",
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
        starRocksAssert.query(sql).explainContains("count(1: id)", "multi_distinct_count(1: id)");

        sql = "select count(distinct id2) from test.bitmap_table";
        starRocksAssert.query(sql).explainContains("count(2: id2)", "bitmap_union_count(2: id2)");

        sql = "select sum(id) / count(distinct id2) from test.bitmap_table";
        starRocksAssert.query(sql).explainContains("output: sum(1: id), bitmap_union_count(2: id2)");

        sql = "select count(distinct id2) from test.hll_table";
        starRocksAssert.query(sql).explainContains("hll_union_agg(2: id2)", "3: count");

        sql = "select sum(id) / count(distinct id2) from test.hll_table";
        starRocksAssert.query(sql).explainContains("sum(1: id), hll_union_agg(2: id2)");

        sql = "select count(distinct id2) from test.bitmap_table group by id order by count(distinct id2)";
        starRocksAssert.query(sql).explainContains();

        sql = "select count(distinct id2) from test.bitmap_table having count(distinct id2) > 0";
        starRocksAssert.query(sql)
                .explainContains("bitmap_union_count(2: id2)", "having: 3: count > 0");

        sql = "select count(distinct id2) from test.bitmap_table order by count(distinct id2)";
        starRocksAssert.query(sql).explainContains("3: count", "3:MERGING-EXCHANGE",
                "order by: <slot 3> 3: count ASC",
                "output: bitmap_union_count(2: id2)");
    }

    @Test
    public void testDateTypeCastSyntax() throws Exception {
        String castSql = "select * from test.baseall where k11 < cast('2020-03-26' as date)";
        starRocksAssert.query(castSql).explainContains("8: k11 < '2020-03-26 00:00:00'");

        String castSql2 = "select str_to_date('11/09/2011', '%m/%d/%Y');";
        starRocksAssert.query(castSql2).explainContains("constant exprs:", "'2011-11-09'");

        String castSql3 = "select str_to_date('11/09/2011', k6) from test.baseall";
        starRocksAssert.query(castSql3).explainContains("  1:Project\n" +
                "  |  <slot 12> : str_to_date('11/09/2011', 6: k6)");
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
                "  |  <slot 5> : 5: sum\n" +
                "  |  <slot 7> : if(2: k2 IS NULL, 'ALL', 2: k2)\n" +
                "  |  <slot 8> : if(3: k3 IS NULL, 'ALL', 3: k3)"));
        Assert.assertTrue(plan.contains("2:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: sum(4: k4)\n" +
                "  |  group by: 1: k1, 2: k2, 3: k3, 6: GROUPING_ID"));
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
                "  |  <slot 5> : 5: sum\n" +
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
        Assert.assertTrue(explainString.contains("equal join conjunct: 7: cast = 6: value"));
        Assert.assertTrue(explainString.contains("<slot 7> : CAST(2: id AS VARCHAR(1048576))"));
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
    public void testJoinConst() throws Exception {
        String sql =
                "with user_info as (select 2 as user_id, 'mike' as user_name), address as (select 1 as user_id, 'newzland' as address_name) \n" +
                        "select * from address a right join user_info b on b.user_id=a.user_id;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("4:HASH JOIN\n" +
                "  |  join op: RIGHT OUTER JOIN (PARTITIONED)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 5: expr = 7: expr"));
        Assert.assertTrue(plan.contains("2:UNION\n" +
                "     constant exprs: \n" +
                "         2 | 'mike'"));
        Assert.assertTrue(plan.contains("0:UNION\n" +
                "     constant exprs: \n" +
                "         1 | 'newzland'"));
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
    public void testAntiJoinOnFalseConstantPredicate() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "select join2.id from join1 RIGHT ANTI JOIN join2 on join1.id = join2.id" +
                " and 1 > 2 group by join2.id" +
                " union select join2.id from join1 RIGHT ANTI JOIN join2 on join1.id = join2.id " +
                " and 1 > 2 WHERE (NOT (true)) group by join2.id ";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("4:HASH JOIN\n" +
                "  |  join op: LEFT ANTI JOIN (BROADCAST)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 5: id = 2: id"));
        Assert.assertTrue(plan.contains("  2:EMPTYSET\n"));
        Assert.assertTrue(plan.contains("  8:EMPTYSET\n"));
        FeConstants.runningUnitTest = false;
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
        FeConstants.runningUnitTest = true;
        String sql = "select join1.id from join1, join2 group by join1.id";
        String plan = getFragmentPlan(sql);
        System.out.println(plan);

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
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testPreAggregationWithJoin() throws Exception {
        FeConstants.runningUnitTest = true;
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
        System.out.println(plan);
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
        FeConstants.runningUnitTest = false;
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
                        "  |  equal join conjunct: 2: id = 5: id",
                "  0:OlapScanNode\n" +
                        "     TABLE: join1\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 2: id > 1",
                "  1:OlapScanNode\n" +
                        "     TABLE: join2\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 5: id > 1");

        // test having aggregate column
        sql = "select count(*) as count from join1 left join join2 on join1.id = join2.id\n" +
                "having count > 1;";
        starRocksAssert.query(sql).explainContains("7:AGGREGATE (merge finalize)\n" +
                        "  |  output: count(7: count)\n" +
                        "  |  group by: \n" +
                        "  |  having: 7: count > 1",
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
                "where k11 = case when date_format(now(),'%H%i') < 123 then date_format(date_sub(now(),2),'%Y%m%d') else date_format(date_sub(now(),1),'%Y%m%d') end";
        Assert.assertFalse(StringUtils.containsIgnoreCase(getFragmentPlan(caseWhenSql), "CASE WHEN"));

        // test 1: case when then
        // 1.1 multi when in on `case when` and can be converted to constants
        String sql11 = "select case when false then 2 when true then 3 else 0 end as col11;";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql11), "constant exprs: \n         3"));

        // 1.2 multi `when expr` in on `case when` ,`when expr` can not be converted to constants
        String sql121 =
                "select case when false then 2 when substr(k7,2,1) then 3 else 0 end as col121 from test.baseall";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql121),
                "if(CAST(substr(9: k7, 2, 1) AS BOOLEAN), 3, 0)"));

        // 1.2.2 when expr which can not be converted to constants in the first
        String sql122 =
                "select case when substr(k7,2,1) then 2 when false then 3 else 0 end as col122 from test.baseall";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql122),
                "if(CAST(substr(9: k7, 2, 1) AS BOOLEAN), 2, 0)"));

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
                "if(if(CAST(substr(9: k7, 2, 1) AS BOOLEAN), TRUE, FALSE), 2, 0)"));

        // 1.6 test when expr is null
        String sql16 = "select case when null then 1 else 2 end as col16;";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql16), "constant exprs: \n         2"));

        // 1.7 test when true in first return directly
        String sql17 = "select case when true then 1 when substr(k7,2,1) then 3 else 2 end as col16 from test.baseall;";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql17), "<slot 12> : 1"));

        // 1.8 test when true in the middle not return directly
        String sql18 = "select case when substr(k7,2,1) then 3 when true then 1 else 2 end as col16 from test.baseall;";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql18),
                "CASE WHEN CAST(substr(9: k7, 2, 1) AS BOOLEAN) THEN 3 WHEN TRUE THEN 1 ELSE 2 END"));

        // 1.9 test remove when clause when is false/null
        String sql19 =
                "select case when substr(k7,2,1) then 3 when false then 1 when null then 5 else 2 end as col16 from test.baseall;";
        Assert.assertTrue(StringUtils
                .containsIgnoreCase(getFragmentPlan(sql19), "if(CAST(substr(9: k7, 2, 1) AS BOOLEAN), 3, 2)"));

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
                "if(substr(9: k7, 2, 1) = 'a', '2', '0')"));

        // 2.3.1  first when expr is not constant
        String sql231 =
                "select case 'a' when substr(k7,2,1) then 2 when 1 then 'a' when false then 3 else 0 end as col231 from test.baseall";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql231),
                "if(substr(9: k7, 2, 1) = 'a', '2', '0')"));

        // 2.3.2 case expr is not constant
        String sql232 =
                "select case k1 when substr(k7,2,1) then 2 when 1 then 'a' when false then 3 else 0 end as col232 from test.baseall";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql232),
                "CASE CAST(1: k1 AS VARCHAR) WHEN substr(9: k7, 2, 1) THEN '2' WHEN '1' THEN 'a' WHEN '0' THEN '3' ELSE '0' END"));

        // 2.4 when expr has true but not equals case expr
        String sql24 = "select case 10 when true then 'a' when 2 then 'b' else 'other' end as col2;";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql24),
                "constant exprs: \n         'other'"));

        // 2.5 when expr has true but equals case expr
        String sql25 = "select case 1 when true then 'a' when 2 then 'b' else 'other' end as col2;";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql25),
                "constant exprs: \n         'a'"));

        // 2.6 when expr equals case expr in middle
        String sql26 =
                "select case 'a' when substr(k7,2,1) then 2 when 'a' then 'b' else 'other' end as col2 from test.baseall;";
        Assert.assertTrue(StringUtils.containsIgnoreCase(getFragmentPlan(sql26),
                "CASE 'a' WHEN substr(9: k7, 2, 1) THEN '2' WHEN 'a' THEN 'b' ELSE 'other' END"));

        // 2.7 test remove when clause not equals case expr
        String sql27 =
                "select case 'a' when substr(k7,2,1) then 3 when false then 1 when null then 5 else 2 end as col16 from test.baseall;";
        Assert.assertTrue(
                StringUtils.containsIgnoreCase(getFragmentPlan(sql27), "if(substr(9: k7, 2, 1) = 'a', 3, 2)"));

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

        // 4.2.2 test null/false in when expr
        String sql422 = "select case 'a' when null then 'a' when false then 'b' else 'other' end as col421";
        Assert.assertTrue(
                StringUtils.containsIgnoreCase(getFragmentPlan(sql422), "constant exprs: \n         'other'"));

        // 4.2.3 test null false in when expr return null
        String sql423 = "select case 'a' when null then 'a' when false then 'b' end as col421";
        Assert.assertTrue(
                StringUtils.containsIgnoreCase(getFragmentPlan(sql423), "constant exprs: \n         NULL"));
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
                        "  |  having: CAST(23: max AS DOUBLE) > 0.0");
    }

    @Test
    public void testDistinctPushDown() throws Exception {
        String sql = "select distinct k1 from (select distinct k1 from test.pushdown_test) t where k1 > 1";
        starRocksAssert.query(sql).explainContains("  RESULT SINK\n" +
                "\n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  group by: 1: k1\n" +
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
        FeConstants.runningUnitTest = true;
        String queryStr = "select * from test.jointest t1, test.jointest t2 where t1.k1 = t2.k1";
        String explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: true"));

        // multi partition
        queryStr = "select * from test.dynamic_partition t1, test.dynamic_partition t2 where t1.k1 = t2.k1";
        explainString = getFragmentPlan(queryStr);
        Assert.assertTrue(explainString.contains("colocate: false"));
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testJoinWithMysqlTable() throws Exception {
        // set data size and row count for the olap table
        Database db = Catalog.getCurrentCatalog().getDb("default_cluster:test");
        OlapTable tbl = (OlapTable) db.getTable("jointest");
        for (Partition partition : tbl.getPartitions()) {
            partition.updateVisibleVersion(2);
            for (MaterializedIndex mIndex : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE)) {
                mIndex.setRowCount(10000);
                for (Tablet tablet : mIndex.getTablets()) {
                    for (Replica replica : ((LocalTablet) tablet).getReplicas()) {
                        replica.updateRowCount(2, 200000, 10000);
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

        sql = "select a.dt from test.join1 a left ANTI join test.join2 b on a.id = b.id " +
                "right ANTI join test.join2 d on a.id = d.id";
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
        Assert.assertTrue(explainString.contains("12: count"));
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
        FeConstants.runningUnitTest = true;
        connectContext.setDatabase("default_cluster:test");

        String sql =
                "select * from join1 where join1.dt > 1 and NOT EXISTS (select * from join1 as a where join1.dt = 1 and a.id = join1.id)" +
                        "and NOT EXISTS (select * from join1 as a where join1.dt = 2 and a.id = join1.id);";
        String explainString = getFragmentPlan(sql);
        System.out.println(explainString);

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
        FeConstants.runningUnitTest = false;
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
        Assert.assertTrue(explainString.contains("equal join conjunct: 8: id = 11: id"));
        Assert.assertTrue(explainString.contains("  |----2:EXCHANGE\n" +
                "  |    \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: join1"));
        Assert.assertTrue(explainString.contains("  1:OlapScanNode\n" +
                "     TABLE: join2\n" +
                "     PREAGGREGATION: ON"));
    }

    @Test
    public void testMultiCountDistinctType() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "select count(distinct t1a,t1b) from test_all_type";
        String plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains("2:AGGREGATE (update serialize)\n" +
                "  |  aggregate: count[(if[(1: t1a IS NULL, NULL, [2: t1b, SMALLINT, true]); args: BOOLEAN,SMALLINT,SMALLINT; result: SMALLINT; args nullable: true; result nullable: true]); args: SMALLINT; result: BIGINT; args nullable: true; result nullable: false]"));
        Assert.assertTrue(plan.contains("4:AGGREGATE (merge finalize)\n" +
                "  |  aggregate: count[([11: count, BIGINT, false]); args: SMALLINT; result: BIGINT; args nullable: true; result nullable: false]"));
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testMultiCountDistinctAggPhase() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "select count(distinct t1a,t1b), avg(t1c) from test_all_type";
        String plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains(" 2:AGGREGATE (update serialize)\n" +
                "  |  aggregate: count[(if[(1: t1a IS NULL, NULL, [2: t1b, SMALLINT, true]); args: BOOLEAN,SMALLINT,SMALLINT; result: SMALLINT; args nullable: true; result nullable: true]); args: SMALLINT; result: BIGINT; args nullable: true; result nullable: false], avg[([12: avg, VARCHAR, true]); args: INT; result: VARCHAR; args nullable: true; result nullable: true]"));
        Assert.assertTrue(plan.contains(" 1:AGGREGATE (update serialize)\n" +
                "  |  aggregate: avg[([3: t1c, INT, true]); args: INT; result: VARCHAR; args nullable: true; result nullable: true]\n" +
                "  |  group by: [1: t1a, VARCHAR, true], [2: t1b, SMALLINT, true]"));
        FeConstants.runningUnitTest = false;
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
        System.out.println(plan);
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
                "  |  group by: 1: v1\n"));

        sql = "select sum(x1) from (select sum(v1) as x1 from t0) as q";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  RESULT SINK\n" +
                "\n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(1: v1)\n" +
                "  |  group by: \n"));

        sql = "select SUM(x1) from (select v2, sum(v1) as x1 from t0 group by v2) as q";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  RESULT SINK\n" +
                "\n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(1: v1)\n" +
                "  |  group by: \n"));

        sql = "select v2, SUM(x1) from (select v2, v3, sum(v1) as x1 from t0 group by v2, v3) as q group by v2";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(1: v1)\n" +
                "  |  group by: 2: v2\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0"));

        sql = "select SUM(x1) from (select v2, sum(distinct v1), sum(v3) as x1 from t0 group by v2) as q";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(3: v3)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0"));

        sql = "select MAX(x1) from (select v2 as x1 from t0 union select v3 from t0) as q";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  7:AGGREGATE (merge finalize)\n" +
                "  |  output: max(8: v2)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  6:EXCHANGE\n" +
                "\n" +
                "PLAN FRAGMENT 1\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 06\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  5:AGGREGATE (update serialize)\n" +
                "  |  output: max(7: v2)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  0:UNION"));

        sql = "select MIN(x1) from (select distinct v2 as x1 from t0) as q";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:AGGREGATE (update finalize)\n" +
                "  |  output: min(2: v2)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0"));

        sql = "select MIN(x1) from (select v2 as x1 from t0 group by v2) as q";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:AGGREGATE (update finalize)\n" +
                "  |  output: min(2: v2)\n" +
                "  |  group by: \n" +
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
                "  |  \n" +
                "  0:OlapScanNode"));

        sql = "select SUM(v2) from (select v2, sum(v1) as x1 from t0 group by v2) as q";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  2:AGGREGATE (update finalize)\n" +
                "  |  output: sum(2: v2)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  group by: 2: v2\n"));
        sql = "select SUM(v2) from (select v2, sum(distinct v2) as x1 from t0 group by v2) as q";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  2:AGGREGATE (update finalize)\n" +
                "  |  output: sum(2: v2)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  group by: 2: v2\n"));
        sql = "select sum(distinct x1) from (select v2, sum(v2) as x1 from t0 group by v2) as q";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(2: v2)\n" +
                "  |  group by: 2: v2\n" +
                "  |  \n" +
                "  0:OlapScanNode\n"));

        sql = "select SUM(x1) from (select v2 as x1 from t0 union select v3 from t0) as q";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  7:AGGREGATE (merge finalize)\n" +
                "  |  group by: 7: v2\n" +
                "  |  \n" +
                "  6:EXCHANGE\n" +
                "\n" +
                "PLAN FRAGMENT 2\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 06\n" +
                "    HASH_PARTITIONED: 7: v2\n" +
                "\n" +
                "  5:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  group by: 7: v2\n"));

        sql = "select SUM(x1) from (select v2 as x1 from t0 group by v2) as q";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  2:AGGREGATE (update finalize)\n" +
                "  |  output: sum(2: v2)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  group by: 2: v2\n" +
                "  |  \n" +
                "  0:OlapScanNode"));
    }

    // todo(ywb) disable replicate join temporarily
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
        FeConstants.runningUnitTest = true;
        String sql = "select * from join1 join pushdown_test on join1.id = pushdown_test.k1;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("INNER JOIN (BROADCAST)"));
        FeConstants.runningUnitTest = false;
        connectContext.getSessionVariable().setEnableReplicationJoin(false);
    }

    // todo(ywb) disable replicate join temporarily
    public void testReplicationJoinWithEmptyNode() throws Exception {
        // check replicate join without exception
        connectContext.getSessionVariable().setEnableReplicationJoin(true);
        FeConstants.runningUnitTest = true;
        String sql = "with cross_join as (\n" +
                "  select * from \n" +
                "  (SELECT \n" +
                "      t0.v1, \n" +
                "      t0.v2, \n" +
                "      t0.v3\n" +
                "    FROM \n" +
                "      t0 \n" +
                "    WHERE \n" +
                "      false)\n" +
                "  subt0 LEFT SEMI \n" +
                "  JOIN \n" +
                "    (SELECT \n" +
                "      t2.v7, \n" +
                "      t2.v8, \n" +
                "      t2.v9\n" +
                "    FROM \n" +
                "      t2 \n" +
                "    WHERE \n" +
                "      false)\n" +
                "  subt2 ON subt0.v3 = subt2.v8, \n" +
                "  t1 \n" +
                ")\n" +
                "SELECT \n" +
                "  DISTINCT cross_join.v1 \n" +
                "FROM \n" +
                "  t0 LEFT JOIN\n" +
                "  cross_join\n" +
                "  ON cross_join.v4 = t0.v2;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("9:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (REPLICATED)"));
        FeConstants.runningUnitTest = false;
        connectContext.getSessionVariable().setEnableReplicationJoin(false);
    }

    @Test
    public void testOuterJoinBucketShuffle() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "SELECT DISTINCT t0.v1 FROM t0 RIGHT JOIN[BUCKET] t1 ON t0.v1 = t1.v4";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains(" 5:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  group by: 1: v1\n" +
                "  |  \n" +
                "  4:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  \n" +
                "  3:HASH JOIN\n" +
                "  |  join op: RIGHT OUTER JOIN (BUCKET_SHUFFLE)"));

        sql = "SELECT DISTINCT t0.v1 FROM t0 FULL JOIN[BUCKET] t1 ON t0.v1 = t1.v4";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("3:HASH JOIN\n" +
                "  |  join op: FULL OUTER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 4: v4\n" +
                "  |  \n" +
                "  |----2:EXCHANGE\n" +
                "  |    \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0"));

        sql = "SELECT DISTINCT t1.v4 FROM t0 LEFT JOIN[BUCKET] t1 ON t0.v1 = t1.v4";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("7:AGGREGATE (merge finalize)\n" +
                "  |  group by: 4: v4\n" +
                "  |  \n" +
                "  6:EXCHANGE\n" +
                "\n" +
                "PLAN FRAGMENT 2\n" +
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
                "  |  \n" +
                "  4:Project\n" +
                "  |  <slot 4> : 4: v4\n" +
                "  |  \n" +
                "  3:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (BUCKET_SHUFFLE)"));
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testSchemaScan() throws Exception {
        String sql = "select * from information_schema.columns";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  0:SCAN SCHEMA\n"));
    }

    @Test
    public void testDuplicateAggregateFn() throws Exception {
        String sql = "select bitmap_union_count(b1) from test_object having count(distinct b1) > 2;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains(" OUTPUT EXPRS:13: bitmap_union_count\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  output: bitmap_union_count(5: b1)\n" +
                "  |  group by: \n" +
                "  |  having: 13: bitmap_union_count > 2"));
    }

    @Test
    public void testDuplicateAggregateFn2() throws Exception {
        String sql = "select bitmap_union_count(b1), count(distinct b1) from test_object;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  2:Project\n" +
                "  |  <slot 13> : 13: bitmap_union_count\n" +
                "  |  <slot 14> : 13: bitmap_union_count\n" +
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
        Assert.assertTrue(planFragment.contains("PREDICATES: (1: v1 <= 2) OR (NOT (if(2: v2 > 2, FALSE, TRUE)))"));

        sql = "select v1 from t0 where not (v1 > 2 or v2 is null or if(v3 > 2, FALSE, TRUE))";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(
                planFragment.contains("PREDICATES: 1: v1 <= 2, 2: v2 IS NOT NULL, NOT (if(3: v3 > 2, FALSE, TRUE))"));
    }

    @Test
    public void testArithmeticCommutative() throws Exception {
        String sql = "select v1 from t0 where 2 / v1  > 3";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PREDICATES: 2.0 / CAST(1: v1 AS DOUBLE) > 3.0"));

        sql = "select v1 from t0 where 2 * v1  > 3";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("PREDICATES: 2 * 1: v1 > 3"));

        sql = "select v1 from t0 where v1 + 2 > 3";
        planFragment = getFragmentPlan(sql);
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
        String sql = "select v1 from (select * from t0 limit 1) x0 left outer join[shuffle] t1 on x0.v1 = t1.v4";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains(" 5:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (PARTITIONED)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 4: v4\n" +
                "  |  \n" +
                "  |----4:EXCHANGE\n" +
                "  |    \n" +
                "  2:EXCHANGE\n" +
                "     limit: 1"));

        sql = "select v1 from (select * from t0 limit 10) x0 left outer join t1 on x0.v1 = t1.v4 limit 1";
        plan = getFragmentPlan(sql);
        System.out.println(plan);
        Assert.assertTrue(plan.contains("  3:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 4: v4\n" +
                "  |  limit: 1\n" +
                "  |  \n" +
                "  |----2:EXCHANGE\n" +
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

        sql = "select v1 from (select * from t0 limit 10) x0 left outer join[shuffle] t1 on x0.v1 = t1.v4 limit 100";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("5:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (PARTITIONED)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 4: v4\n" +
                "  |  limit: 100\n" +
                "  |  \n" +
                "  |----4:EXCHANGE\n" +
                "  |    \n" +
                "  2:EXCHANGE\n" +
                "     limit: 10"));
        Assert.assertTrue(plan.contains("PLAN FRAGMENT 2\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 02\n" +
                "    HASH_PARTITIONED: 1: v1\n" +
                "\n" +
                "  1:EXCHANGE\n" +
                "     limit: 10"));

        sql =
                "select v1 from (select * from t0 limit 10) x0 left outer join (select * from t1 limit 5) x1 on x0.v1 = x1.v4 limit 7";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("5:HASH JOIN\n" +
                "  |  join op: RIGHT OUTER JOIN (PARTITIONED)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 4: v4 = 1: v1\n" +
                "  |  limit: 7\n" +
                "  |  \n" +
                "  |----4:EXCHANGE\n" +
                "  |       limit: 7\n" +
                "  |    \n" +
                "  2:EXCHANGE\n" +
                "     limit: 5"));
        Assert.assertTrue(plan.contains("PLAN FRAGMENT 2\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 02\n" +
                "    HASH_PARTITIONED: 4: v4\n" +
                "\n" +
                "  1:EXCHANGE\n" +
                "     limit: 5"));
    }

    @Test
    public void testColocateCoverReplicate() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "select * from join1 join join1 as xx on join1.id = xx.id;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  |  join op: INNER JOIN (COLOCATE)\n"));
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testReplicatedAgg() throws Exception {
        connectContext.getSessionVariable().setEnableReplicationJoin(true);

        String sql = "select value, SUM(id) from join1 group by value";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(2: id)\n" +
                "  |  group by: 3: value\n" +
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
                "  |  \n" +
                "  |----4:EXCHANGE\n" +
                "  |    \n" +
                "  |----6:EXCHANGE\n" +
                "  |    \n" +
                "  2:EXCHANGE\n"));

        sql =
                "select * from (select * from (select * from t0 limit 0) t union all select * from t1 union all select * from t2) as xx";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  0:UNION\n" +
                "  |  \n" +
                "  |----4:EXCHANGE\n" +
                "  |    \n" +
                "  2:EXCHANGE\n"));

        sql = "select * from (select * from (select * from t0 limit 0) t union all select * from t1 where false" +
                " union all select * from t2) as xx";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:10: v1 | 11: v2 | 12: v3\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  1:Project\n" +
                "  |  <slot 10> : 7: v7\n" +
                "  |  <slot 11> : 8: v8\n" +
                "  |  <slot 12> : 9: v9\n" +
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
                "  |  \n" +
                "  |----4:EXCHANGE\n" +
                "  |    \n" +
                "  |----6:EXCHANGE\n" +
                "  |    \n" +
                "  2:EXCHANGE"));

        sql =
                "select * from (select * from (select * from t0 limit 0) t intersect select * from t1 intersect select * from t2) as xx";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:10: v1 | 11: v2 | 12: v3\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  0:EMPTYSET\n"));

        sql = "select * from (select * from (select * from t0 limit 0) t intersect select * from t1 where false " +
                "intersect select * from t2) as xx";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:10: v1 | 11: v2 | 12: v3\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  0:EMPTYSET\n"));
    }

    @Test
    public void testExceptEmptyNode() throws Exception {
        String sql;
        String plan;
        sql = "select * from (select * from t0 except select * from t1 except select * from t2) as xx";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  0:EXCEPT\n" +
                "  |  \n" +
                "  |----4:EXCHANGE\n" +
                "  |    \n" +
                "  |----6:EXCHANGE\n" +
                "  |    \n" +
                "  2:EXCHANGE\n"));

        sql =
                "select * from (select * from (select * from t0 limit 0) t except select * from t1 except select * from t2) as xx";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:10: v1 | 11: v2 | 12: v3\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  0:EMPTYSET\n"));

        sql = "select * from ( select * from t2 except (select * from t0 limit 0) except " +
                "select * from t1) as xx";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  0:EXCEPT\n" +
                "  |  \n" +
                "  |----4:EXCHANGE\n" +
                "  |    \n" +
                "  2:EXCHANGE\n"));
    }

    @Test
    public void testPredicateOnRepeatNode() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "select * from (select v1, v2, sum(v3) from t0 group by rollup(v1, v2)) as xx where v1 is null;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("1:REPEAT_NODE\n" +
                "  |  repeat: repeat 2 lines [[], [1], [1, 2]]\n" +
                "  |  PREDICATES: 1: v1 IS NULL"));
        Assert.assertFalse(plan.contains("0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 1: v1 IS NULL"));

        sql = "select * from (select v1, v2, sum(v3) from t0 group by rollup(v1, v2)) as xx where v1 is not null;";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("1:REPEAT_NODE\n" +
                "  |  repeat: repeat 2 lines [[], [1], [1, 2]]\n" +
                "  |  PREDICATES: 1: v1 IS NOT NULL"));
        Assert.assertTrue(plan.contains("0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 1: v1 IS NOT NULL"));

        sql = "select * from (select v1, v2, sum(v3) from t0 group by rollup(v1, v2)) as xx where v1 = 1;";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("1:REPEAT_NODE\n" +
                "  |  repeat: repeat 2 lines [[], [1], [1, 2]]\n" +
                "  |  PREDICATES: 1: v1 = 1"));
        Assert.assertTrue(plan.contains("0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 1: v1 = 1"));

        sql = "select * from (select v1, v2, sum(v3) from t0 group by rollup(v1, v2)) as xx where v1 = 1 + 2;";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains(" 1:REPEAT_NODE\n" +
                "  |  repeat: repeat 2 lines [[], [1], [1, 2]]\n" +
                "  |  PREDICATES: 1: v1 = 3"));
        Assert.assertTrue(plan.contains("0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 1: v1 = 3"));

        sql = "select * from (select v1, v2, sum(v3) from t0 group by rollup(v1, v2)) as xx where v1 = v2;";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("1:REPEAT_NODE\n" +
                "  |  repeat: repeat 2 lines [[], [1], [1, 2]]\n" +
                "  |  PREDICATES: 1: v1 = 2: v2"));
        Assert.assertTrue(plan.contains("0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 1: v1 = 2: v2"));

        sql = "select * from (select v1, v2, sum(v3) from t0 group by rollup(v1, v2)) as xx where v1 <=> v2;";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("1:REPEAT_NODE\n" +
                "  |  repeat: repeat 2 lines [[], [1], [1, 2]]\n" +
                "  |  PREDICATES: 1: v1 <=> 2: v2"));
        Assert.assertFalse(plan.contains("0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON" +
                "     PREDICATES: 1: v1 <=> 2: v2"));
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testCountDecimalV3Literal() throws Exception {
        Config.enable_decimal_v3 = true;
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
        Config.enable_decimal_v3 = false;
    }

    @Test
    public void testLimitPushDownJoin() throws Exception {
        String sql = "select * from t0 left join[shuffle] t1 on t0.v2 = t1.v5 where t1.v6 is null limit 2";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  |  join op: LEFT OUTER JOIN (PARTITIONED)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: v2 = 5: v5\n" +
                "  |  other predicates: 6: v6 IS NULL\n" +
                "  |  limit: 2"));
        Assert.assertTrue(plan.contains("     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1\n" +
                "     rollup: t0\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=1\n" +
                "     avgRowSize=3.0\n" +
                "     numNodes=0\n"));
    }

    @Test
    public void testEmptyProjectCountStar() throws Exception {
        String sql = "select count(*) from test_all_type a, test_all_type b where a.t1a is not null";
        String plan = getCostExplain(sql);
        Assert.assertTrue(plan.contains("  3:CROSS JOIN\n" +
                "  |  cross join:\n" +
                "  |  predicates is NULL.\n" +
                "  |  cardinality: 1\n" +
                "  |  column statistics: \n" +
                "  |  * t1a-->[-Infinity, Infinity, 0.0, 1.0, 1.0] UNKNOWN\n" +
                "  |  * t1b-->[-Infinity, Infinity, 0.0, 1.0, 1.0] UNKNOWN"));
    }

    @Test
    public void testCountDistinctMultiColumns() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(1);
        String sql = "select count(distinct L_SHIPMODE,L_ORDERKEY) from lineitem";
        String plan = getFragmentPlan(sql);
        // check use 4 stage agg plan
        Assert.assertTrue(plan.contains("6:AGGREGATE (merge finalize)\n" +
                "  |  output: count(18: count)"));
        Assert.assertTrue(plan.contains("4:AGGREGATE (update serialize)\n" +
                "  |  output: count(if(15: L_SHIPMODE IS NULL, NULL, 1: L_ORDERKEY))\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  3:AGGREGATE (merge serialize)\n" +
                "  |  group by: 1: L_ORDERKEY, 15: L_SHIPMODE"));
        System.out.println(" 1:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  group by: 1: L_ORDERKEY, 15: L_SHIPMODE");

        sql = "select count(distinct L_SHIPMODE,L_ORDERKEY) from lineitem group by L_PARTKEY";
        plan = getFragmentPlan(sql);
        // check use 3 stage agg plan
        Assert.assertTrue(plan.contains(" 4:AGGREGATE (update finalize)\n" +
                "  |  output: count(if(15: L_SHIPMODE IS NULL, NULL, 1: L_ORDERKEY))\n" +
                "  |  group by: 2: L_PARTKEY\n" +
                "  |  \n" +
                "  3:AGGREGATE (merge serialize)\n" +
                "  |  group by: 1: L_ORDERKEY, 2: L_PARTKEY, 15: L_SHIPMODE"));
        Assert.assertTrue(plan.contains("1:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  group by: 1: L_ORDERKEY, 2: L_PARTKEY, 15: L_SHIPMODE"));
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    @Test
    public void testCountDistinctBoolTwoPhase() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        String sql = "select count(distinct id_bool) from test_bool";
        String plan = getCostExplain(sql);
        Assert.assertTrue(plan.contains("aggregate: multi_distinct_count[([11: id_bool, BOOLEAN, true]); " +
                "args: BOOLEAN; result: VARCHAR;"));

        sql = "select sum(distinct id_bool) from test_bool";
        plan = getCostExplain(sql);
        Assert.assertTrue(plan.contains("aggregate: multi_distinct_sum[([11: id_bool, BOOLEAN, true]); " +
                "args: BOOLEAN; result: VARCHAR;"));
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    @Test
    public void testCountDistinctFloatTwoPhase() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        String sql = "select count(distinct t1e) from test_all_type";
        String plan = getCostExplain(sql);
        Assert.assertTrue(plan.contains("aggregate: multi_distinct_count[([5: t1e, FLOAT, true]); " +
                "args: FLOAT; result: VARCHAR; args nullable: true; result nullable: false"));
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    @Test
    public void testCastDecimalZero() throws Exception {
        Config.enable_decimal_v3 = true;
        String sql = "select (CASE WHEN CAST(t0.v1 AS BOOLEAN ) THEN 0.00 END) BETWEEN (0.07) AND (0.04) from t0;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  |  <slot 7> : CAST(6: if AS DECIMAL32(2,2))\n"));
        Config.enable_decimal_v3 = false;
    }

    @Test
    public void testCountDistinctMultiColumns2() throws Exception {
        connectContext.getSessionVariable().setNewPlanerAggStage(2);
        String sql = "select count(distinct L_SHIPMODE,L_ORDERKEY) from lineitem";
        String plan = getFragmentPlan(sql);
        // check use 4 stage agg plan
        Assert.assertTrue(plan.contains("6:AGGREGATE (merge finalize)\n" +
                "  |  output: count(18: count)"));
        Assert.assertTrue(plan.contains("4:AGGREGATE (update serialize)\n" +
                "  |  output: count(if(15: L_SHIPMODE IS NULL, NULL, 1: L_ORDERKEY))\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  3:AGGREGATE (merge serialize)\n" +
                "  |  group by: 1: L_ORDERKEY, 15: L_SHIPMODE"));
        Assert.assertTrue(plan.contains("1:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  group by: 1: L_ORDERKEY, 15: L_SHIPMODE"));

        sql = "select count(distinct L_SHIPMODE,L_ORDERKEY) from lineitem group by L_PARTKEY";
        plan = getFragmentPlan(sql);
        // check use 3 stage agg plan
        Assert.assertTrue(plan.contains(" 4:AGGREGATE (update finalize)\n" +
                "  |  output: count(if(15: L_SHIPMODE IS NULL, NULL, 1: L_ORDERKEY))\n" +
                "  |  group by: 2: L_PARTKEY\n" +
                "  |  \n" +
                "  3:AGGREGATE (merge serialize)\n" +
                "  |  group by: 1: L_ORDERKEY, 2: L_PARTKEY, 15: L_SHIPMODE"));
        Assert.assertTrue(plan.contains("1:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  group by: 1: L_ORDERKEY, 2: L_PARTKEY, 15: L_SHIPMODE"));
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
    }

    @Test
    public void testJoinWithLimit() throws Exception {
        String sql = "select t2.v8 from (select v1, v2, v1 as v3 from t0 where v2<> v3 limit 15) as a join t1 " +
                "on a.v3 = t1.v4 join t2 on v4 = v7 join t2 as b" +
                " on a.v1 = b.v7 where b.v8 > t1.v5 limit 10";
        String plan = getFragmentPlan(sql);
        System.out.println(plan);
        // check join on predicate which has expression with limit operator
        sql = "select t2.v8 from (select v1, v2, v1 as v3 from t0 where v2<> v3 limit 15) as a join t1 " +
                "on a.v3 + 1 = t1.v4 join t2 on v4 = v7 join t2 as b" +
                " on a.v3 + 2 = b.v7 where b.v8 > t1.v5 limit 10";
        plan = getFragmentPlan(sql);
        System.out.println(plan);
    }

    @Test
    public void testArrayFunctionFilter() throws Exception {
        String sql = "select * from test_array where array_length(c1) between 2 and 3;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("PREDICATES: array_length(2: c1) >= 2, array_length(2: c1) <= 3"));
    }

    @Test
    public void testPredicateOnThreeTables() throws Exception {
        String sql = "SELECT \n" +
                "  DISTINCT t1.v4 \n" +
                "FROM \n" +
                "  t1, \n" +
                "  (\n" +
                "    SELECT \n" +
                "      t3.v1, \n" +
                "      t3.v2, \n" +
                "      t3.v3\n" +
                "    FROM \n" +
                "      t3\n" +
                "  ) subt3 FULL \n" +
                "  JOIN t0 ON subt3.v3 != t0.v1 \n" +
                "  AND subt3.v3 = t0.v1 \n" +
                "WHERE \n" +
                "  (\n" +
                "    (t0.v2) BETWEEN (\n" +
                "      CAST(subt3.v2 AS STRING)\n" +
                "    ) \n" +
                "    AND (t0.v2)\n" +
                "  ) = (t1.v4);";
        String plan = getFragmentPlan(sql);
        // check no exception
        Assert.assertTrue(plan.contains("HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v4 = 10: cast"));
    }

    @Test
    public void testDecimalCast() throws Exception {
        Config.enable_decimal_v3 = true;
        String sql = "select * from baseall where cast(k5 as decimal32(4,3)) = 1.234";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("PREDICATES: CAST(5: k5 AS DECIMAL32(4,3)) = 1.234"));

        sql = "SELECT k5 FROM baseall WHERE (CAST(k5 AS DECIMAL32 ) ) IN (0.006) " +
                "GROUP BY k5 HAVING (k5) IN (0.005, 0.006)";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("PREDICATES: 5: k5 IN (0.005, 0.006), CAST(5: k5 AS DECIMAL32(9,9)) = 0.006"));
        Config.enable_decimal_v3 = false;
    }

    @Test
    public void testDeriveOutputColumns() throws Exception {
        String sql = "select \n" +
                "  rand() as c0, \n" +
                "  round(\n" +
                "    cast(\n" +
                "      rand() as DOUBLE\n" +
                "    )\n" +
                "  ) as c1 \n" +
                "from \n" +
                "  (\n" +
                "    select \n" +
                "      subq_0.v1 as c0 \n" +
                "    from \n" +
                "      (\n" +
                "        select \n" +
                "          v1,v2,v3\n" +
                "        from \n" +
                "          t0 as ref_0 \n" +
                "        where \n" +
                "          ref_0.v1 = ref_0.v2 \n" +
                "        limit \n" +
                "          72\n" +
                "      ) as subq_0 \n" +
                "      right join t1 as ref_1 on (subq_0.v3 = ref_1.v5) \n" +
                "    where \n" +
                "      subq_0.v2 <> subq_0.v3 \n" +
                "    limit \n" +
                "      126\n" +
                "  ) as subq_1 \n" +
                "where \n" +
                "  66 <= unix_timestamp() \n" +
                "limit \n" +
                "  155;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("6:Project\n" +
                "  |  <slot 2> : 2: v2"));
    }

    @Test
    public void testPushDownEquivalenceDerivePredicate() throws Exception {
        // check is null predicate on t1.v5 which equivalences derive from t1.v4 can not push down to scan node
        String sql = "SELECT \n" +
                "  subt0.v2, \n" +
                "  t1.v6\n" +
                "FROM \n" +
                "  (\n" +
                "    SELECT \n" +
                "      t0.v1, \n" +
                "      t0.v2, \n" +
                "      t0.v3\n" +
                "    FROM \n" +
                "      t0\n" +
                "  ) subt0 \n" +
                "  LEFT JOIN t1 ON subt0.v3 = t1.v4 \n" +
                "  AND subt0.v3 = t1.v4 \n" +
                "  AND subt0.v3 = t1.v5 \n" +
                "  AND subt0.v3 >= t1.v5 \n" +
                "WHERE \n" +
                "  (\n" +
                "    (\n" +
                "      (t1.v4) < (\n" +
                "        (\n" +
                "          (-650850438)-(\n" +
                "            (\n" +
                "              (2000266938)%(-1243652117)\n" +
                "            )\n" +
                "          )\n" +
                "        )\n" +
                "      )\n" +
                "    ) IS NULL\n" +
                "  ) \n" +
                "GROUP BY \n" +
                " subt0.v2, \n" +
                "  t1.v6;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains(" 0:OlapScanNode\n" +
                "     TABLE: t1\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1"));
    }

    @Test
    public void testJoinOnPredicateRewrite() throws Exception {
        String sql = "select * from t0 left outer join t1 on v1=v4 and cast(v2 as bigint) = v5 and false";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("equal join conjunct: 1: v1 = 4: v4"));
        Assert.assertTrue(plan.contains("1:EMPTYSET"));
    }

    @Test
    public void testSetOpCast() throws Exception {
        String sql = "select * from t0 union all (select * from t1 union all select k1,k7,k8 from  baseall)";
        String plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains(
                "  0:UNION\n" +
                        "  |  child exprs:\n" +
                        "  |      [1, BIGINT, true] | [4, VARCHAR(20), true] | [5, DOUBLE, true]\n" +
                        "  |      [23, BIGINT, true] | [24, VARCHAR(20), true] | [25, DOUBLE, true]"));
        Assert.assertTrue(plan.contains(
                "  |  19 <-> [19: k7, VARCHAR, true]\n" +
                        "  |  20 <-> [20: k8, DOUBLE, true]\n" +
                        "  |  22 <-> cast([11: k1, TINYINT, true] as BIGINT)"));

        sql =
                "select * from t0 union all (select cast(v4 as int), v5,v6 from t1 except select cast(v7 as int), v8, v9 from t2)";
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains("  0:UNION\n" +
                "  |  child exprs:\n" +
                "  |      [1, BIGINT, true] | [2, BIGINT, true] | [3, BIGINT, true]\n" +
                "  |      [15, BIGINT, true] | [13, BIGINT, true] | [14, BIGINT, true]\n" +
                "  |  pass-through-operands: all\n" +
                "  |  cardinality: 2\n" +
                "  |  \n" +
                "  |----11:EXCHANGE\n" +
                "  |       cardinality: 1\n" +
                "  |    \n" +
                "  2:EXCHANGE\n" +
                "     cardinality: 1\n" +
                "\n" +
                "PLAN FRAGMENT 1(F02)\n" +
                "\n" +
                "  Input Partition: RANDOM\n" +
                "  OutPut Partition: RANDOM\n" +
                "  OutPut Exchange Id: 11\n" +
                "\n" +
                "  10:Project\n" +
                "  |  output columns:\n" +
                "  |  13 <-> [13: v5, BIGINT, true]\n" +
                "  |  14 <-> [14: v6, BIGINT, true]\n" +
                "  |  15 <-> cast([12: cast, INT, true] as BIGINT)\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  3:EXCEPT\n" +
                "  |  child exprs:\n" +
                "  |      [7, INT, true] | [5, BIGINT, true] | [6, BIGINT, true]\n" +
                "  |      [11, INT, true] | [9, BIGINT, true] | [10, BIGINT, true]"));
    }

    @Test
    public void testSemiJoinFalsePredicate() throws Exception {
        String sql = "select * from t0 left semi join t3 on t0.v1 = t3.v1 " +
                "AND CASE WHEN NULL THEN t0.v1 ELSE '' END = CASE WHEN true THEN 'fGrak3iTt' WHEN false THEN t3.v1 ELSE 'asf' END";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  |  join op: RIGHT SEMI JOIN (PARTITIONED)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 4: v1 = 1: v1"));
    }

    @Test
    public void testTopNOffsetError() throws Exception {
        long limit = connectContext.getSessionVariable().getSqlSelectLimit();
        connectContext.getSessionVariable().setSqlSelectLimit(200);
        String sql = "select * from (select * from t0 order by v1 limit 5) as a left join t1 on a.v1 = t1.v4";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:TOP-N\n" +
                "  |  order by: <slot 1> 1: v1 ASC\n" +
                "  |  offset: 0\n" +
                "  |  limit: 5"));
        connectContext.getSessionVariable().setSqlSelectLimit(limit);
    }

    @Test
    public void testProjectReuse() throws Exception {
        String sql = "select nullif(v1, v1) + (0) as a , nullif(v1, v1) + (1 - 1) as b from t0;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("<slot 4> : nullif(1: v1, 1: v1) + 0"));
        Assert.assertTrue(plan.contains(" OUTPUT EXPRS:4: expr | 4: expr"));
    }

    @Test
    public void testFunctionNullable() throws Exception {
        String sql = "select UNIX_TIMESTAMP(\"2015-07-28 19:41:12\", \"22\");";
        String plan = getThriftPlan(sql);
        Assert.assertTrue(
                plan.contains("signature:unix_timestamp(VARCHAR, VARCHAR), scalar_fn:TScalarFunction(symbol:), " +
                        "id:0, fid:50303, could_apply_dict_optimize:false), has_nullable_child:false, is_nullable:true"));
    }

    @Test
    public void testOnlyCrossJoin() throws Exception {
        String sql = "select * from t0 as x0 join t1 as x1 on (1 = 2) is not null;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("3:CROSS JOIN\n" +
                "  |  cross join:\n" +
                "  |  predicates is NULL"));
    }

    @Test
    public void testFailedLeftJoin() {
        String sql = "select * from t0 as x0 left outer join t1 as x1 on (1 = 2) is not null";
        Assert.assertThrows("No equal on predicate in LEFT OUTER JOIN is not supported", SemanticException.class,
                () -> getFragmentPlan(sql));
    }

    @Test
    public void testDecimalConstRewrite() throws Exception {
        String sql = "select * from t0 WHERE CAST( - 8 AS DECIMAL ) * + 52 + 87 < - 86";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: TRUE"));
    }

    @Test
    public void testJoinReorderWithPredicate() throws Exception {
        connectContext.getSessionVariable().setMaxTransformReorderJoins(2);
        String sql = "select t0.v1 from t0, t1, t2, t3 where t0.v1 + t3.v1 = 2";
        String plan = getFragmentPlan(sql);
        connectContext.getSessionVariable().setMaxTransformReorderJoins(4);
        Assert.assertTrue(plan.contains("11:CROSS JOIN\n" +
                "  |  cross join:\n" +
                "  |  predicates: 1: v1 + 10: v1 = 2"));
    }

    @Test
    public void testDateTypeReduceCast() throws Exception {
        String sql =
                "select * from test_all_type_distributed_by_datetime where cast(cast(id_datetime as date) as datetime) >= '1970-01-01 12:00:00' " +
                        "and cast(cast(id_datetime as date) as datetime) <= '1970-01-01 18:00:00'";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains(
                "8: id_datetime >= '1970-01-02 00:00:00', 8: id_datetime < '1970-01-02 00:00:00'"));
    }

    @Test
    public void testCharCompareWithVarchar() throws Exception {
        String sql = "select t2.tb from tall t1 join tall t2 " +
                "on t1.tc = t2.tb and t2.tt = 123 and (t2.tt != 'ax') = t2.td;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("PREDICATES: 20: tt = '123', " +
                "CAST(20: tt != 'ax' AS BIGINT) = 14: td, 14: td = 1"));
    }

    @Test
    public void testSubqueryLimit() throws Exception {
        String sql = "select * from t0 where 2 = (select v4 from t1 limit 1);";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  4:ASSERT NUMBER OF ROWS\n" +
                "  |  assert number of rows: LE 1"));
    }

    @Test
    public void testEqStringCast() throws Exception {
        String sql = "select 'a' = v1 from t0";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("CAST(1: v1 AS VARCHAR(1048576)) = 'a'\n"));
    }

    @Test
    public void testUnionChildProjectHasNullable() throws Exception {
        String sql = "SELECT \n" +
                "  DISTINCT * \n" +
                "FROM \n" +
                "  (\n" +
                "    SELECT \n" +
                "      DISTINCT DAY(\"292269055-12-03 00:47:04\") \n" +
                "    FROM \n" +
                "      t1\n" +
                "    WHERE \n" +
                "      true \n" +
                "    UNION ALL \n" +
                "    SELECT \n" +
                "      DISTINCT DAY(\"292269055-12-03 00:47:04\") \n" +
                "    FROM \n" +
                "      t1\n" +
                "    WHERE \n" +
                "      (\n" +
                "        (true) IS NULL\n" +
                "      )\n" +
                "  ) t;";
        String plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains("8:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  group by: [9: day, TINYINT, true]\n" +
                "  |  cardinality: 0\n" +
                "  |  \n" +
                "  0:UNION\n" +
                "  |  child exprs:\n" +
                "  |      [4, TINYINT, true]\n" +
                "  |      [8, TINYINT, true]\n" +
                "  |  pass-through-operands: all"));
    }

    @Test
    public void testNullableSameWithChildrenFunctions() throws Exception {
        Config.enable_decimal_v3 = true;
        String sql = "select distinct day(id_datetime) from test_all_type_partition_by_datetime";
        String plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains(" 1:Project\n" +
                "  |  output columns:\n" +
                "  |  11 <-> day[([2: id_datetime, DATETIME, false]); args: DATETIME; result: TINYINT; args nullable: false; result nullable: false]"));

        sql = "select distinct 2 * v1 from t0_not_null";
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains("2:AGGREGATE (update finalize)\n" +
                "  |  group by: [4: expr, BIGINT, false]"));

        sql = "select distinct cast(2.0 as decimal) * v1 from t0_not_null";
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains("2:AGGREGATE (update finalize)\n" +
                "  |  group by: [4: expr, DECIMAL64(18,0), true]"));
        Config.enable_decimal_v3 = false;
    }

    @Test
    public void testEqDoubleCast() throws Exception {
        String sql = "select 'a' = t1e from test_all_type";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("CAST(5: t1e AS DOUBLE) = CAST('a' AS DOUBLE)\n"));
    }

    @Test
    public void testNotEqStringCast() throws Exception {
        String sql = "select 'a' != v1 from t0";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("CAST(1: v1 AS VARCHAR(1048576)) != 'a'\n"));
    }

    @Test
    public void testConstantNullable() throws Exception {
        String sql = "SELECT MICROSECONDS_SUB(\"1969-12-25\", NULL) FROM t1";
        ExecPlan plan = UtFrameUtils.getPlanAndFragment(connectContext, sql).second;
        List<ColumnRefOperator> outColumns = plan.getOutputColumns();

        Assert.assertEquals(1, outColumns.size());
        Assert.assertEquals(Type.DATETIME, outColumns.get(0).getType());
        Assert.assertTrue(outColumns.get(0).isNullable());
    }

    @Test
    public void testUnionNullConstant() throws Exception {
        Config.enable_decimal_v3 = true;
        String sql = "select count(*) from (select null as c1 union all select null as c1) t group by t.c1";
        String plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains("0:UNION\n" +
                "  |  child exprs:\n" +
                "  |      [1, NULL_TYPE, true]\n" +
                "  |      [2, NULL_TYPE, true]"));

        sql = "select count(*) from (select 1 as c1 union all select null as c1) t group by t.c1";
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains(" 0:UNION\n" +
                "  |  child exprs:\n" +
                "  |      [1, TINYINT, false]\n" +
                "  |      [2, TINYINT, true]"));

        sql =
                "select count(*) from (select cast('1.2' as decimal(10,2)) as c1 union all select cast('1.2' as decimal(10,0)) as c1) t group by t.c1";
        plan = getVerboseExplain(sql);
        System.out.println(plan);
        Assert.assertTrue(plan.contains("0:UNION\n" +
                "  |  child exprs:\n" +
                "  |      [1, DECIMAL64(12,2), true]\n" +
                "  |      [2, DECIMAL64(12,2), true]"));

        sql =
                "select count(*) from (select cast('1.2' as decimal(5,2)) as c1 union all select cast('1.2' as decimal(10,0)) as c1) t group by t.c1";
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains("0:UNION\n" +
                "  |  child exprs:\n" +
                "  |      [1, DECIMAL64(12,2), true]\n" +
                "  |      [2, DECIMAL64(12,2), true]"));
        Config.enable_decimal_v3 = false;
    }

    @Test
    public void testBinaryPredicateNullable() throws Exception {
        String sql = "select distinct L_ORDERKEY < L_PARTKEY from lineitem";
        String plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains(" 2:AGGREGATE (update finalize)\n" +
                "  |  group by: [18: expr, BOOLEAN, false]"));

        sql = "select distinct v1 <=> v2 from t0";
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains("2:AGGREGATE (update finalize)\n" +
                "  |  group by: [4: expr, BOOLEAN, false]"));
    }

    @Test
    public void testSemiJoinReorder() throws Exception {
        String sql = "SELECT \n" +
                "  v2 \n" +
                "FROM \n" +
                "  t0 \n" +
                "WHERE \n" +
                "  v1 IN (\n" +
                "    SELECT \n" +
                "      v2 \n" +
                "    FROM \n" +
                "      t0 \n" +
                "    WHERE \n" +
                "      (\n" +
                "        v2 IN (\n" +
                "          SELECT \n" +
                "            v1\n" +
                "          FROM \n" +
                "            t0\n" +
                "        ) \n" +
                "        OR (\n" +
                "          v2 IN (\n" +
                "            SELECT \n" +
                "              v1\n" +
                "            FROM \n" +
                "              t0\n" +
                "          )\n" +
                "        )\n" +
                "      ) \n" +
                "      AND (\n" +
                "        v3 IN (\n" +
                "          SELECT \n" +
                "            v1 \n" +
                "          FROM \n" +
                "            t0\n" +
                "        )\n" +
                "      )\n" +
                "  );";
        // check no exception
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains(" 10:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (BROADCAST)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 5: v2 = 11: v1\n" +
                "  |  other predicates: (10: expr) OR (11: v1 IS NOT NULL)"));
    }

    @Test
    public void testMd5sum() throws Exception {
        String sql = "select 1 from t0 left outer join t1 on t0.v1= t1.v4 where md5sum(t1.v4) = 'a'";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("join op: LEFT OUTER JOIN (BROADCAST)"));
        Assert.assertTrue(plan.contains("other predicates: md5sum(CAST(4: v4 AS VARCHAR)) = 'a'"));
    }

    @Test
    public void testJoinOutput() throws Exception {
        String sql = "select v1,v4 from t0, t1 where v2 = v5";
        String plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains("output columns: 1, 4"));

        sql = "select v1+1,v4 from t0, t1 where v2 = v5";
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains("output columns: 1, 4"));

        sql = "select v2+1,v4 from t0, t1 where v2 = v5";
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains("output columns: 2, 4"));

        sql = "select v1+1,v4 from t0, t1 where v2 = v5 and v3 > v6";
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains("output columns: 1, 4"));

        sql = "select (v2+v6 = 1 or v2+v6 = 5) from t0, t1 where v2 = v5 ";
        plan = getVerboseExplain(sql);
        Assert.assertTrue(plan.contains("  4:Project\n" +
                "  |  output columns:\n" +
                "  |  7 <-> (8: add = 1) OR (8: add = 5)\n" +
                "  |  common expressions:\n" +
                "  |  8 <-> [2: v2, BIGINT, true] + [6: v6, BIGINT, true]\n" +
                "  |  cardinality: 1"));
        Assert.assertTrue(plan.contains("output columns: 2, 6"));

        sql = "select * from t0,t1 where v1 = v4";
        plan = getVerboseExplain(sql);
        Assert.assertFalse(plan.contains("output columns"));
    }

    @Test
    public void testSingleNodeExecPlan() throws Exception {
        String sql = "select v1,v2,v3 from t0";
        connectContext.getSessionVariable().setSingleNodeExecPlan(true);
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("PLAN FRAGMENT 0\n" +
                " OUTPUT EXPRS:1: v1 | 2: v2 | 3: v3\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1\n" +
                "     rollup: t0\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=1\n" +
                "     avgRowSize=3.0\n" +
                "     numNodes=0"));
        connectContext.getSessionVariable().setSingleNodeExecPlan(false);
    }

    @Test
    public void testIsNullPredicateFunctionThrift() throws Exception {
        String sql = "select v1 from t0 where v1 is null";
        String thrift = getThriftPlan(sql);
        Assert.assertTrue(thrift.contains("fn:TFunction(name:TFunctionName(function_name:is_null_pred)"));
    }

    @Test
    public void testSemiJoinReorderWithProject() throws Exception {
        String sql = "select x1.s1 from " +
                "(select t0.v1 + 1 as s1, t0.v2 from t0 left join t1 on t0.v2 = t1.v4) as x1 " +
                "left semi join t2 on x1.v2 = t2.v7";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  4:Project\n" +
                "  |  <slot 2> : 2: v2\n" +
                "  |  <slot 7> : 1: v1 + 1\n" +
                "  |  \n" +
                "  3:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN"));
    }

    @Test
    public void testWindowWithAgg() throws Exception {
        String sql = "SELECT v1, sum(v2),  sum(v2) over (ORDER BY v1) AS `rank` FROM t0 group BY v1, v2";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("window: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"));

        sql =
                "SELECT v1, sum(v2),  sum(v2) over (ORDER BY CASE WHEN v1 THEN 1 END DESC) AS `rank`  FROM t0 group BY v1, v2";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"));
    }

    @Test
    public void testWindowWithChildProjectAgg() throws Exception {
        String sql = "SELECT v1, sum(v2) as x1, row_number() over (ORDER BY CASE WHEN v1 THEN 1 END DESC) AS `rank` " +
                "FROM t0 group BY v1";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  2:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 4> : 4: sum\n" +
                "  |  <slot 8> : if(CAST(1: v1 AS BOOLEAN), 1, NULL)"));
    }

    @Test
    public void testBitmapCount() throws Exception {
        String sql = "SELECT 1 FROM t0 LEFT OUTER JOIN t1 ON t0.v1=t1.v4 " +
                "WHERE NOT CAST(bitmap_count(CASE WHEN t1.v4 in (10000) THEN bitmap_hash('abc') END) AS BOOLEAN)";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("join op: LEFT OUTER JOIN (BROADCAST)"));
        Assert.assertTrue(plan.contains(
                "other predicates: NOT (CAST(bitmap_count(if(4: v4 = 10000, bitmap_hash('abc'), NULL)) AS BOOLEAN))"));
    }

    @Test
    public void testCrossJoinOnPredicate() throws Exception {
        String sql = "select * from t0 cross join t1 on t0.v1 != t1.v4 and t0.v2 != t1.v5";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  3:CROSS JOIN\n" +
                "  |  cross join:\n" +
                "  |  predicates: 1: v1 != 4: v4, 2: v2 != 5: v5"));
    }

    @Test
    public void testCrossJoinCastToInner() throws Exception {
        String sql = "select * from t0 cross join t1 on t0.v1 = t1.v4 and t0.v2 != t1.v5";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  3:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 4: v4\n" +
                "  |  other join predicates: 2: v2 != 5: v5"));
    }

    @Test
    public void testCrossJoinPushLimit() throws Exception {
        String sql = "select * from t0 cross join t1 on t0.v2 != t1.v5 limit 10";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  3:CROSS JOIN\n" +
                "  |  cross join:\n" +
                "  |  predicates: 2: v2 != 5: v5\n" +
                "  |  limit: 10\n" +
                "  |  \n" +
                "  |----2:EXCHANGE\n" +
                "  |    \n" +
                "  0:OlapScanNode"));

        sql = "select * from t0 inner join t1 on t0.v2 != t1.v5 limit 10";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("3:CROSS JOIN\n" +
                "  |  cross join:\n" +
                "  |  predicates: 2: v2 != 5: v5\n" +
                "  |  limit: 10\n" +
                "  |  \n" +
                "  |----2:EXCHANGE\n" +
                "  |    \n" +
                "  0:OlapScanNode"));
    }

    @Test
    public void testUnionDefaultLimit() throws Exception {
        connectContext.getSessionVariable().setSqlSelectLimit(2);
        String sql = "select * from t0 union all select * from t0;";
        String plan = getFragmentPlan(sql);
        connectContext.getSessionVariable().setSqlSelectLimit(SessionVariable.DEFAULT_SELECT_LIMIT);
        Assert.assertTrue(plan.contains("  0:UNION\n" +
                "  |  limit: 2\n" +
                "  |  \n" +
                "  |----6:EXCHANGE\n" +
                "  |       limit: 2\n" +
                "  |    \n" +
                "  3:EXCHANGE\n" +
                "     limit: 2"));
    }

    @Test
    public void testValuesDefaultLimit() throws Exception {
        connectContext.getSessionVariable().setSqlSelectLimit(1);
        String sql = "select * from (values (1,2,3), (4,5,6)) x";
        String plan = getFragmentPlan(sql);
        connectContext.getSessionVariable().setSqlSelectLimit(SessionVariable.DEFAULT_SELECT_LIMIT);
        Assert.assertTrue(plan.contains("  0:UNION\n" +
                "     constant exprs: \n" +
                "         1 | 2 | 3\n" +
                "         4 | 5 | 6\n" +
                "     limit: 1"));
    }

    @Test
    public void testUnionSubqueryDefaultLimit() throws Exception {
        connectContext.getSessionVariable().setSqlSelectLimit(2);
        String sql = "select * from (select * from t0 union all select * from t0) xx limit 10;";
        String plan = getFragmentPlan(sql);
        connectContext.getSessionVariable().setSqlSelectLimit(SessionVariable.DEFAULT_SELECT_LIMIT);
        Assert.assertTrue(plan.contains("  0:UNION\n" +
                "  |  limit: 10\n" +
                "  |  \n" +
                "  |----6:EXCHANGE\n" +
                "  |       limit: 10\n" +
                "  |    \n" +
                "  3:EXCHANGE\n" +
                "     limit: 10"));
    }

    @Test
    public void testArithmeticDecimalReuse() throws Exception {
        String sql = "select t1a, sum(id_decimal * t1f), sum(id_decimal * t1f)" +
                "from test_all_type group by t1a";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("OUTPUT EXPRS:1: t1a | 12: sum | 12: sum"));
    }

    @Test
    public void testBitmapHashRewrite() throws Exception {
        String sql = "select bitmap_hash(NULL)";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("bitmap_hash(NULL)"));
    }

    public void testPlanContains(String sql, String content) throws Exception {
        String plan = getFragmentPlan(sql);
        Assert.assertTrue("plan is " + plan, plan.contains(content));
    }

    @Test
    public void testInPredicateNormalize() throws Exception {
        starRocksAssert.withTable("create table test_in_pred_norm" +
                "(c0 INT, c1 INT, c2 INT, c3 INT, c4 DATE, c5 DATE) " +
                " duplicate key(c0) distributed by hash(c0) buckets 1 " +
                "properties('replication_num'='1');");

        testPlanContains("SELECT * FROM test_in_pred_norm WHERE c0 IN (0) ", "c0 = 0");
        testPlanContains("SELECT * FROM test_in_pred_norm WHERE c0 IN (0, 1) ", "c0 IN (0, 1)");
        testPlanContains("SELECT * FROM test_in_pred_norm WHERE c0 IN (0, 1, 2) ", "c0 IN (0, 1, 2)");

        testPlanContains("SELECT * FROM test_in_pred_norm WHERE c0 IN (c1) ", "1: c0 = 2: c1");
        testPlanContains("SELECT * FROM test_in_pred_norm WHERE c0 IN (c1, c2) ", "(1: c0 = 2: c1) OR (1: c0 = 3: c2)");
        testPlanContains("SELECT * FROM test_in_pred_norm WHERE c0 IN (c1, c2, c3) ",
                "((1: c0 = 2: c1) OR (1: c0 = 3: c2)) OR (1: c0 = 4: c3)");

        testPlanContains("SELECT * FROM test_in_pred_norm WHERE c0 NOT IN (c1) ", "1: c0 != 2: c1");
        testPlanContains("SELECT * FROM test_in_pred_norm WHERE c0 NOT IN (c1, c2) ", "1: c0 != 2: c1, 1: c0 != 3: c2");
        testPlanContains("SELECT * FROM test_in_pred_norm WHERE c0 NOT IN (c1, c2, c3) ",
                "1: c0 != 2: c1, 1: c0 != 3: c2, 1: c0 != 4: c3");

        testPlanContains("SELECT * FROM test_in_pred_norm WHERE c0 IN (0, c1) ", "(1: c0 = 0) OR (1: c0 = 2: c1)");
        testPlanContains("SELECT * FROM test_in_pred_norm WHERE c0 IN (0, c1, c2) ",
                "((1: c0 = 0) OR (1: c0 = 2: c1)) OR (1: c0 = 3: c2)");
        testPlanContains("SELECT * FROM test_in_pred_norm WHERE c0 NOT IN (0, c1) ", "1: c0 != 0, 1: c0 != 2: c1");
        testPlanContains("SELECT * FROM test_in_pred_norm WHERE c0 NOT IN (0, c1, c2) ",
                "1: c0 != 0, 1: c0 != 2: c1, 1: c0 != 3: c2");

        // contains cast expression
        testPlanContains("SELECT * FROM test_in_pred_norm WHERE c4 IN ('1970-01-01', '1970-01-01', '1970-02-01') ",
                "c4 IN ('1970-01-01', '1970-01-01', '1970-02-01')");
        testPlanContains("SELECT * FROM test_in_pred_norm WHERE c4 IN ('292278994-08-17', '1970-01-01', '1970-02-01') ",
                "c4 IN (CAST('292278994-08-17' AS DATE), '1970-01-01', '1970-02-01')");

        // common expression
        testPlanContains("SELECT " +
                        "c4 IN ('292278994-08-17', '1970-02-01') AND " +
                        "c5 IN ('292278994-08-17', '1970-02-01') AND " +
                        "c5 IN ('292278994-08-17', '1970-02-01')  " +
                        " FROM test_in_pred_norm",
                "<slot 7> : ((5: c4 = 8: cast) OR (5: c4 = '1970-02-01')) AND ((6: c5 = 8: cast) OR (6: c5 = '1970-02-01'))");

        String plan = getFragmentPlan("SELECT " +
                "c4 IN ('292278994-08-17', '1970-02-01') AND c4 IN ('292278994-08-18', '1970-02-01') AND " +
                "c5 IN ('292278994-08-17', '1970-02-01') AND c5 IN ('292278994-08-18', '1970-02-01') AND " +
                "c5 IN ('292278994-08-17', '1970-02-01') AND c5 IN ('292278994-08-17', '1970-02-01')  " +
                " FROM test_in_pred_norm");
        Assert.assertTrue("plan is " + plan, plan.contains("common expressions:"));
        Assert.assertTrue("plan is \n" + plan, plan.contains("<slot 8> "));
        Assert.assertTrue("plan is \n" + plan, plan.contains("<slot 9> "));
    }

    @Test
    public void testLargeIntMod() throws Exception {
        String sql = "select -123 % 100000000000000000000000000000000000";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("     constant exprs: \n" +
                "         -123 % 100000000000000000000000000000000000"));
    }

    @Test
    public void testShuffleColumnsAdjustOrders() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql = "select t0.v1, t1.v4, t2.v7 from t0 join[shuffle] t1 on t0.v1 = t1.v4 and t0.v2 = t1.v5 " +
                "join[shuffle] t2 on t0.v2 = t2.v8 and t0.v1 = t2.v7";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains(" 8:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: v2 = 8: v8\n" +
                "  |  equal join conjunct: 1: v1 = 7: v7"));
        Assert.assertTrue(plan.contains("STREAM DATA SINK\n" +
                "    EXCHANGE ID: 03\n" +
                "    HASH_PARTITIONED: 5: v5, 4: v4"));
        Assert.assertTrue(plan.contains("STREAM DATA SINK\n" +
                "    EXCHANGE ID: 01\n" +
                "    HASH_PARTITIONED: 2: v2, 1: v1"));

        sql = "select t0.v1, t1.v4, t2.v7 from t0 join[shuffle] t1 on t0.v1 = t1.v4 and t0.v2 = t1.v5 " +
                "join[shuffle] t2 on t0.v2 = t2.v8 and t0.v1 = t2.v7 join[shuffle] t3 on t2.v7 = t3.v1 and t2.v8 = t3.v2 ";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("12:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 7: v7 = 10: v1\n" +
                "  |  equal join conjunct: 8: v8 = 11: v2"));
        Assert.assertTrue(plan.contains("8:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: v2 = 8: v8\n" +
                "  |  equal join conjunct: 1: v1 = 7: v7"));
        Assert.assertTrue(plan.contains(" STREAM DATA SINK\n" +
                "    EXCHANGE ID: 07\n" +
                "    HASH_PARTITIONED: 7: v7, 8: v8"));
        Assert.assertTrue(plan.contains("STREAM DATA SINK\n" +
                "    EXCHANGE ID: 01\n" +
                "    HASH_PARTITIONED: 1: v1, 2: v2"));

        sql = "select t0.v1, t1.v4, t2.v7 from t0 join[shuffle] t1 on t0.v2 = t1.v5 and t0.v1 = t1.v4 " +
                "join[shuffle] t2 on t0.v2 = t2.v8 and t0.v1 = t2.v7 join[shuffle] t3 on t2.v7 = t3.v1 and t2.v8 = t3.v2 ";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains(" 8:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: v2 = 8: v8\n" +
                "  |  equal join conjunct: 1: v1 = 7: v7"));
        Assert.assertTrue(plan.contains("STREAM DATA SINK\n" +
                "    EXCHANGE ID: 01\n" +
                "    HASH_PARTITIONED: 1: v1, 2: v2"));
        Assert.assertTrue(plan.contains("STREAM DATA SINK\n" +
                "    EXCHANGE ID: 03\n" +
                "    HASH_PARTITIONED: 4: v4, 5: v5"));

        sql = "select t0.v1, t1.v4, t2.v7 from t0 join[shuffle] t1 on t0.v2 = t1.v5 and t0.v1 = t1.v4 " +
                "join[shuffle] t2 on t0.v2 = t2.v8 and t0.v1 = t2.v7 join[shuffle] t3 on t0.v1 = t3.v1 and t0.v2 = t3.v2 ";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("12:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 1: v1 = 10: v1\n" +
                "  |  equal join conjunct: 2: v2 = 11: v2"));
        Assert.assertTrue(plan.contains("STREAM DATA SINK\n" +
                "    EXCHANGE ID: 01\n" +
                "    HASH_PARTITIONED: 1: v1, 2: v2"));
        Assert.assertTrue(plan.contains(" STREAM DATA SINK\n" +
                "    EXCHANGE ID: 03\n" +
                "    HASH_PARTITIONED: 4: v4, 5: v5"));

        sql = "select * from t0 left join[shuffle] (\n" +
                "    select t1.* from t1 left join[shuffle] t2 \n" +
                "    on t1.v4 = t2.v7 \n" +
                "    and t1.v6 = t2.v9 \n" +
                "    and t1.v5 = t2.v8) as j2\n" +
                "on t0.v3 = j2.v6\n" +
                "  and t0.v1 = j2.v4\n" +
                "  and t0.v2 = j2.v5;";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("8:HASH JOIN\n" +
                "  |  join op: LEFT OUTER JOIN (BUCKET_SHUFFLE(S))\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 3: v3 = 6: v6\n" +
                "  |  equal join conjunct: 1: v1 = 4: v4\n" +
                "  |  equal join conjunct: 2: v2 = 5: v5"));
        Assert.assertTrue(plan.contains("STREAM DATA SINK\n" +
                "    EXCHANGE ID: 03\n" +
                "    HASH_PARTITIONED: 6: v6, 4: v4, 5: v5"));
        Assert.assertTrue(plan.contains("STREAM DATA SINK\n" +
                "    EXCHANGE ID: 05\n" +
                "    HASH_PARTITIONED: 9: v9, 7: v7, 8: v8"));

        sql =
                "select a.v1, a.v4, b.v7, b.v1 from (select v1, v2, v4 from t0 join[shuffle] t1 on t0.v1 = t1.v4 and t0.v2 = t1.v5) a join[shuffle] " +
                        "(select v7, v8, v1 from t2 join[shuffle] t3 on t2.v7 = t3.v1 and t2.v8 = t3.v2) b " +
                        "on a.v2 = b.v8 and a.v1 = b.v7";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("12:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: v2 = 8: v8\n" +
                "  |  equal join conjunct: 1: v1 = 7: v7"));
        Assert.assertTrue(plan.contains(" STREAM DATA SINK\n" +
                "    EXCHANGE ID: 09\n" +
                "    HASH_PARTITIONED: 11: v2, 10: v1"));
        Assert.assertTrue(plan.contains("STREAM DATA SINK\n" +
                "    EXCHANGE ID: 07\n" +
                "    HASH_PARTITIONED: 8: v8, 7: v7"));
        Assert.assertTrue(plan.contains("STREAM DATA SINK\n" +
                "    EXCHANGE ID: 03\n" +
                "    HASH_PARTITIONED: 5: v5, 4: v4"));
        Assert.assertTrue(plan.contains("STREAM DATA SINK\n" +
                "    EXCHANGE ID: 01\n" +
                "    HASH_PARTITIONED: 2: v2, 1: v1"));

        // check can not adjust column orders
        sql =
                "select a.v1, a.v4, b.v7, b.v1 from (select v1, v2, v4, v5 from t0 join[shuffle] t1 on t0.v1 = t1.v4 and t0.v2 = t1.v5) a join[shuffle] " +
                        "(select v7, v8, v1, v2 from t2 join[shuffle] t3 on t2.v7 = t3.v1 and t2.v8 = t3.v2) b " +
                        "on a.v2 = b.v8 and a.v4 = b.v8";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("14:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: v2 = 8: v8\n" +
                "  |  equal join conjunct: 4: v4 = 8: v8"));
        Assert.assertTrue(plan.contains(" 11:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 7: v7 = 10: v1\n" +
                "  |  equal join conjunct: 8: v8 = 11: v2"));
        Assert.assertTrue(plan.contains(" STREAM DATA SINK\n" +
                "    EXCHANGE ID: 08\n" +
                "    HASH_PARTITIONED: 7: v7, 8: v8"));
        Assert.assertTrue(plan.contains("STREAM DATA SINK\n" +
                "    EXCHANGE ID: 10\n" +
                "    HASH_PARTITIONED: 10: v1, 11: v2"));

        // check can not adjust column orders
        sql =
                "select a.v1, a.v4, b.v7, b.v1 from (select v1, v2, v4, v5 from t0 join[shuffle] t1 on t0.v1 = t1.v4 and t0.v2 = t1.v5) a join[shuffle] " +
                        "(select v7, v8, v1, v2 from t2 join[shuffle] t3 on t2.v7 = t3.v1 and t2.v8 = t3.v2) b " +
                        "on a.v2 = b.v8 and a.v4 = b.v1";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("14:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 2: v2 = 8: v8\n" +
                "  |  equal join conjunct: 4: v4 = 10: v1"));
        Assert.assertTrue(plan.contains("11:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 7: v7 = 10: v1\n" +
                "  |  equal join conjunct: 8: v8 = 11: v2"));
        FeConstants.runningUnitTest = false;
    }
}
