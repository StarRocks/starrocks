// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.starrocks.common.Config;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class MaterializedViewRewriteOptimizationTest extends PlanTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        Config.enable_experimental_mv = true;
        PlanTestBase.beforeClass();
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(3000000);

        /*
        starRocksAssert.withNewMaterializedView("CREATE MATERIALIZED VIEW lo_mv_1" +
                " distributed by hash(LO_ORDERKEY) " +
                " as " +
                " select LO_ORDERDATE, LO_ORDERKEY from lineorder_flat_for_mv" +
                " where lo_orderpriority='5-LOW';");

         */

        starRocksAssert.withNewMaterializedView("CREATE MATERIALIZED VIEW lo_mv_2" +
                " distributed by hash(LO_ORDERKEY) " +
                " as " +
                " select LO_ORDERDATE, LO_ORDERKEY, LO_REVENUE from lineorder_flat_for_mv" +
                " where LO_REVENUE < 100000;");

        /*
        starRocksAssert.withNewMaterializedView("CREATE MATERIALIZED VIEW lo_mv_3" +
                " distributed by hash(LO_ORDERKEY) " +
                " as " +
                " select LO_ORDERDATE, LO_ORDERKEY, LO_REVENUE from lineorder_flat_for_mv" +
                " where LO_REVENUE < 100000 and lo_orderpriority='5-LOW';");

         */


        /*
        starRocksAssert.withNewMaterializedView("CREATE MATERIALIZED VIEW lo_mv_4" +
                " distributed by hash(LO_ORDERKEY) " +
                " as " +
                " select LO_ORDERDATE, LO_ORDERKEY, LO_REVENUE, LO_SUPPLYCOST + 1 as add_one from lineorder_flat_for_mv" +
                " where LO_REVENUE < 100000 and lo_orderpriority='5-LOW';");

         */


        /*
        starRocksAssert.withNewMaterializedView("CREATE MATERIALIZED VIEW lo_mv_5" +
                " distributed by hash(LO_ORDERKEY) " +
                " as " +
                " select LO_ORDERDATE, LO_ORDERKEY, LO_REVENUE from lineorder_flat_for_mv" +
                " where LO_REVENUE < 50000 and lo_orderpriority='5-LOW';");

         */

        starRocksAssert.withNewMaterializedView("create materialized view join_mv_1" +
                " distributed by hash(v1)" +
                " as " +
                " SELECT t0.v1 as v1, test_all_type.t1d, test_all_type.t1c" +
                " from t0 join test_all_type" +
                " on t0.v1 = test_all_type.t1d" +
                " where t0.v1 < 100");


        starRocksAssert.withNewMaterializedView("create materialized view agg_mv_1" +
                " distributed by hash(LO_ORDERKEY)" +
                " as " +
                " SELECT LO_ORDERKEY, LO_ORDERDATE, sum(LO_REVENUE) as total_revenue, count(LO_REVENUE) as total_num" +
                " from lineorder_flat_for_mv" +
                " where LO_ORDERKEY < 100" +
                " group by LO_ORDERKEY, LO_ORDERDATE");

        /*
        starRocksAssert.withNewMaterializedView("create materialized view agg_join_mv_1" +
                " distributed by hash(v1)" +
                " as " +
                " SELECT t0.v1 as v1, test_all_type.t1d," +
                " sum(test_all_type.t1c) as total_sum, count(test_all_type.t1c) as total_num" +
                " from t0 join test_all_type" +
                " on t0.v1 = test_all_type.t1d" +
                " where t0.v1 < 100" +
                " group by v1, test_all_type.t1d");

         */
    }

    @Test
    public void testFilterScan() throws Exception {
        /*
        String query1 = "select LO_ORDERDATE, LO_ORDERKEY from lineorder_flat_for_mv where lo_orderpriority='5-LOW';";
        String plan1 = getFragmentPlan(query1);
        assertContains(plan1, "1:Project\n" +
                "  |  <slot 1> : 40: LO_ORDERDATE\n" +
                "  |  <slot 2> : 41: LO_ORDERKEY\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: lo_mv_1\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1\n" +
                "     rollup: lo_mv_1");



        String query2 = "select LO_ORDERDATE, LO_ORDERKEY from lineorder_flat_for_mv where LO_REVENUE < 100000 ;";
        String plan2 = getFragmentPlan(query2);
        assertContains(plan2, "1:Project\n" +
                "  |  <slot 1> : 42: LO_ORDERDATE\n" +
                "  |  <slot 2> : 43: LO_ORDERKEY\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: lo_mv_2\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1\n" +
                "     rollup: lo_mv_2");
        */

        String query8 = "select LO_ORDERDATE, LO_ORDERKEY from lineorder_flat_for_mv where LO_REVENUE < 150000 ;";
        String plan8 = getFragmentPlan(query8);
        assertContains(plan8, "1:Project\n" +
                "  |  <slot 1> : 42: LO_ORDERDATE\n" +
                "  |  <slot 2> : 43: LO_ORDERKEY\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: lo_mv_2\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1\n" +
                "     rollup: lo_mv_2");

        /*
        String query3 = "select LO_ORDERDATE, LO_ORDERKEY from lineorder_flat_for_mv where LO_REVENUE < 50000 ;";
        String plan3 = getFragmentPlan(query3);
        assertContains(plan3, "2:Project\n" +
                "  |  <slot 1> : 40: LO_ORDERDATE\n" +
                "  |  <slot 2> : 41: LO_ORDERKEY\n" +
                "  |  \n" +
                "  1:SELECT\n" +
                "  |  predicates: 42: LO_REVENUE <= 49999\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: lo_mv_2\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1\n" +
                "     rollup: lo_mv_2");

         */

        /*
        String query4 = "select LO_ORDERDATE, LO_ORDERKEY from lineorder_flat_for_mv" +
                " where LO_REVENUE < 50000 and lo_orderpriority='5-LOW';";
        String plan4 = getFragmentPlan(query4);
        assertContains(plan4, "2:Project\n" +
                "  |  <slot 1> : 43: LO_ORDERDATE\n" +
                "  |  <slot 2> : 44: LO_ORDERKEY\n" +
                "  |  \n" +
                "  1:SELECT\n" +
                "  |  predicates: 45: LO_REVENUE <= 49999\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: lo_mv_3\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1\n" +
                "     rollup: lo_mv_3");

         */

        /*
        String query5 = "select LO_ORDERDATE, LO_ORDERKEY from lineorder_flat_for_mv" +
                " where LO_REVENUE < 100000 and lo_orderpriority='5-LOW';";
        String plan5 = getFragmentPlan(query5);
        assertContains(plan5, "1:Project\n" +
                "  |  <slot 1> : 43: LO_ORDERDATE\n" +
                "  |  <slot 2> : 44: LO_ORDERKEY\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: lo_mv_3\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1\n" +
                "     rollup: lo_mv_3");

         */

        String query6 = "select LO_ORDERDATE, LO_ORDERKEY, (LO_SUPPLYCOST + 1) * 2 from lineorder_flat_for_mv" +
                " where LO_REVENUE < 50000 and lo_orderpriority='5-LOW';";
        String plan6 = getFragmentPlan(query6);
        assertContains(plan6, "2:Project\n" +
                "  |  <slot 1> : 41: LO_ORDERDATE\n" +
                "  |  <slot 2> : 42: LO_ORDERKEY\n" +
                "  |  <slot 40> : 44: add_one * 2\n" +
                "  |  \n" +
                "  1:SELECT\n" +
                "  |  predicates: 43: LO_REVENUE <= 49999\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: lo_mv_4\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1\n" +
                "     rollup: lo_mv_4");


        String query7 = "select LO_ORDERKEY, (LO_SUPPLYCOST + 1) * 2, LO_ORDERDATE from lineorder_flat_for_mv" +
                " where LO_REVENUE < 50000 and lo_orderpriority='5-LOW';";
        String plan7 = getFragmentPlan(query7);
        assertContains(plan7, "2:Project\n" +
                "  |  <slot 1> : 41: LO_ORDERDATE\n" +
                "  |  <slot 2> : 42: LO_ORDERKEY\n" +
                "  |  <slot 40> : 44: add_one * 2\n" +
                "  |  \n" +
                "  1:SELECT\n" +
                "  |  predicates: 43: LO_REVENUE <= 49999\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: lo_mv_4\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1\n" +
                "     rollup: lo_mv_4");
    }

    @Test
    public void testJoin() throws Exception {

        /*
        String query1 = "SELECT (test_all_type.t1d + 1) * 2, test_all_type.t1c" +
                " from t0 join test_all_type on t0.v1 = test_all_type.t1d where t0.v1 = 1";
        String plan1 = getFragmentPlan(query1);
        assertContains(plan1, "OUTPUT EXPRS:1: v1 | 7: t1d | 6: t1c\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  1:Project\n" +
                "  |  <slot 1> : 14: v1\n" +
                "  |  <slot 6> : 16: t1c\n" +
                "  |  <slot 7> : 14: v1\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: join_mv_1\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1\n" +
                "     rollup: join_mv_1");

         */



        String query2 = "SELECT t0.v1, test_all_type.t1d, test_all_type.t1c" +
                " from t0 join test_all_type on t0.v1 = test_all_type.t1d where test_all_type.t1d < 200";
        String plan2 = getFragmentPlan(query2);
        assertContains(plan2, "OUTPUT EXPRS:1: v1 | 7: t1d | 6: t1c\n" +
                "  PARTITION: RANDOM\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  1:Project\n" +
                "  |  <slot 1> : 14: v1\n" +
                "  |  <slot 6> : 16: t1c\n" +
                "  |  <slot 7> : 14: v1\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: join_mv_1\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=0/1\n" +
                "     rollup: join_mv_1");

    }

    @Test
    public void test() throws Exception {
        String query = "select t1a, sum(t1g) + 1 as v1, sum(t1g) + 1 as v2  from test_all_type_not_null group by t1a, t1b";
        String plan = getFragmentPlan(query);
        Assert.assertEquals("", plan);

        /*
        String query2 = "select t1a + 1, t1b, sum(t1g + 1) from test_all_type_not_null group by grouping sets((t1a), (t1a, t1b))";
        String plan2 = getFragmentPlan(query2);
        Assert.assertEquals("", plan2);

         */
    }

    @Test
    public void testSingleTableAgg() throws Exception {

        String query1 = "SELECT LO_ORDERKEY, sum(LO_REVENUE), count(LO_REVENUE)" +
                " from lineorder_flat_for_mv where LO_ORDERKEY < 200 group by LO_ORDERKEY";
        String plan1 = getFragmentPlan(query1);
        Assert.assertEquals("", plan1);


        /*
        String query2 = "SELECT LO_ORDERKEY, LO_ORDERDATE, sum(LO_REVENUE), count(LO_REVENUE)" +
                " from lineorder_flat_for_mv group by LO_ORDERKEY, LO_ORDERDATE";
        String plan2 = getFragmentPlan(query2);
        Assert.assertEquals("", plan2);

         */
    }

    @Test
    public void testJoinAgg() throws Exception {

        String query2 = "SELECT t0.v1 as v1, test_all_type.t1d," +
                " sum(test_all_type.t1c) as total_sum, count(test_all_type.t1c) as total_num" +
                " from t0 join test_all_type" +
                " on t0.v1 = test_all_type.t1d" +
                " where t0.v1 = 1" +
                " group by v1, test_all_type.t1d";
        String plan2 = getFragmentPlan(query2);
        Assert.assertEquals("", plan2);
    }
}
