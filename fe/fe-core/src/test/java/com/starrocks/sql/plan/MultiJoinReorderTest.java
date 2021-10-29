// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.plan;

import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.FeConstants;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class MultiJoinReorderTest extends PlanTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();

        Catalog catalog = connectContext.getCatalog();

        OlapTable t0 = (OlapTable) catalog.getDb("default_cluster:test").getTable("t0");
        setTableStatistics(t0, 1);

        OlapTable t1 = (OlapTable) catalog.getDb("default_cluster:test").getTable("t1");
        setTableStatistics(t1, 10);
        OlapTable t2 = (OlapTable) catalog.getDb("default_cluster:test").getTable("t2");
        setTableStatistics(t2, 100000);

        OlapTable t3 = (OlapTable) catalog.getDb("default_cluster:test").getTable("t3");
        setTableStatistics(t3, 1000000000);
        connectContext.getSessionVariable().setMaxTransformReorderJoins(2);
        FeConstants.runningUnitTest = true;
    }

    @Test
    public void testCrossJoinReorderGreedy() throws Exception {
        connectContext.getSessionVariable().disableDPJoinReorder();

        String sql = "select * from t1, t2, t3, t0;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("4:CROSS JOIN\n" +
                "  |  cross join:\n" +
                "  |  predicates is NULL.\n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  |----3:EXCHANGE"));
        Assert.assertTrue(planFragment.contains("9:CROSS JOIN"));
        Assert.assertTrue(planFragment.contains("|----8:EXCHANGE\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  6:CROSS JOIN"));
        Assert.assertTrue(planFragment.contains("|----5:EXCHANGE\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t3"));
    }

    @Test
    public void testCrossAndInnerJoinReorderGreedy() throws Exception {
        connectContext.getSessionVariable().disableDPJoinReorder();
        String sql = "select * from t1 join t3 on t1.v4 = t3.v1 join t0 join t2";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  3:OlapScanNode\n" +
                "     TABLE: t0"));
        Assert.assertTrue(planFragment.contains("  |----4:EXCHANGE\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  2:OlapScanNode\n" +
                "     TABLE: t1"));
        Assert.assertTrue(planFragment.contains("  |----6:EXCHANGE\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  1:OlapScanNode\n" +
                "     TABLE: t2"));
        Assert.assertTrue(planFragment.contains("  9:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 4: v1 = 1: v4\n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  |----8:EXCHANGE\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t3"));
    }

    @Test
    public void testInnerJoinReorderGreedy() throws Exception {
        connectContext.getSessionVariable().disableDPJoinReorder();
        String sql = "select * from t1 " +
                "join t3 on t1.v4 = t3.v1 " +
                "join t0 on t1.v4 = t0.v2 " +
                "join t2 on t1.v5 = t2.v8 ";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  3:OlapScanNode\n" +
                "     TABLE: t0"));
        Assert.assertTrue(planFragment.contains("  |----4:EXCHANGE\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  2:OlapScanNode\n" +
                "     TABLE: t1"));
        Assert.assertTrue(planFragment.contains("  |----6:EXCHANGE\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  1:OlapScanNode\n" +
                "     TABLE: t2"));
        Assert.assertTrue(planFragment.contains("  |----8:EXCHANGE\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t3"));
    }

    @Test
    public void testLeftJoinReorderGreedy() throws Exception {
        connectContext.getSessionVariable().disableDPJoinReorder();
        String sql = "select v6 from t1 " +
                "left join (select t1.v5 from t1 join t3 on t1.v4 = t3.v1 join t0 join t2) a " +
                "on t1.v6 = a.v5";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("16:HASH JOIN\n" +
                "  |  join op: RIGHT OUTER JOIN (PARTITIONED)"));

        Assert.assertTrue(planFragment.contains("  |----4:EXCHANGE\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  2:OlapScanNode\n" +
                "     TABLE: t1"));
        Assert.assertTrue(planFragment.contains("  |----7:EXCHANGE\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  1:OlapScanNode\n" +
                "     TABLE: t2"));
        Assert.assertTrue(planFragment.contains("  |----10:EXCHANGE\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t3"));
    }

    @Test
    public void testTwoJoinRootGreedy() throws Exception {
        connectContext.getSessionVariable().disableDPJoinReorder();
        String sql = "select * from (select t1.v5 from t1 join t3 on t3.v1 = t1.v4 join t0 join t2) b " +
                "left join (select t1.v5 from t1 join t3 on t3.v1 = t1.v4 join t0 join t2) a " +
                "on b.v5 = a.v5";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  25:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 16: v1 = 13: v4\n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  |----24:EXCHANGE\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  14:OlapScanNode\n" +
                "     TABLE: t3"));
        Assert.assertTrue(planFragment.contains("  11:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 4: v1 = 1: v4\n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  |----10:EXCHANGE\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t3"));
    }

    // Should produce three join tree and reorder three join tree.
    @Test
    public void testThreeJoinRootGreedy() throws Exception {
        connectContext.getSessionVariable().disableDPJoinReorder();
        connectContext.getSessionVariable().enableGreedyJoinReorder();
        String sql = "select * from (select count(t1.v5) as v55 from t1 join t3 on t3.v1 = t1.v4 join t0) b " +
                "inner join (select t1.v5 from t1 join t3 on t3.v1 = t1.v4 join t0 join t2) a " +
                "on b.v55 = a.v5";
        String planFragment = getFragmentPlan(sql);

        // Top join tree
        Assert.assertTrue(planFragment.contains("  21:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 14: v1 = 11: v4\n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  |----20:EXCHANGE\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t3\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=1/1\n" +
                "     rollup: t3\n" +
                "     tabletRatio=3/3\n" +
                "     tabletList=10033,10035,10037\n" +
                "     cardinality=1000000000\n" +
                "     avgRowSize=1.0\n" +
                "     numNodes=0\n" +
                "     use vectorized: true"));

        // Left sub join tree (b)
        Assert.assertTrue(planFragment.contains("  19:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 12: v5 = 10: count(2: v5)\n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  |----18:EXCHANGE\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  1:OlapScanNode\n" +
                "     TABLE: t1\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=1/1\n" +
                "     rollup: t1\n" +
                "     tabletRatio=3/3\n" +
                "     tabletList=10015,10017,10019\n" +
                "     cardinality=10\n" +
                "     avgRowSize=2.0\n" +
                "     numNodes=0\n" +
                "     use vectorized: true"));

        // Right sub join tree (a)
        Assert.assertTrue(planFragment.contains("  16:CROSS JOIN\n" +
                "  |  cross join:\n" +
                "  |  predicates is NULL.\n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  |----15:EXCHANGE\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  13:AGGREGATE (merge finalize)\n" +
                "  |  output: count(10: count(2: v5))\n" +
                "  |  group by: \n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  12:EXCHANGE\n" +
                "     use vectorized: true"));
    }

    @Test
    public void testTwoJoinRootGreedy2() throws Exception {
        connectContext.getSessionVariable().disableDPJoinReorder();
        connectContext.getSessionVariable().enableGreedyJoinReorder();
        String sql = "select * from t1 " +
                "join t3 on t1.v4 = t3.v1 " +
                "join t0 on t1.v4 = t0.v2 " +
                "join (select * from t1 join t3 on t1.v4 = t3.v1 join t0 on t1.v4 = t0.v2 join t2 on t1.v5 = t2.v8) as a  " +
                "on t1.v5 = a.v8 ";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  |  join op: INNER JOIN (PARTITIONED)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 10: v4 = 13: v1\n" +
                "  |  use vectorized: true"));

        Assert.assertTrue(planFragment.contains("  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 4: v1 = 1: v4\n" +
                "  |  use vectorized: true"));
    }

    @Test
    public void testCrossJoinReorderDP() throws Exception {
        connectContext.getSessionVariable().enableDPJoinReorder();
        String sql = "select * from t1, t2, t3, t0;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("4:CROSS JOIN\n" +
                "  |  cross join:\n" +
                "  |  predicates is NULL.\n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  |----3:EXCHANGE"));
        Assert.assertTrue(planFragment.contains("9:CROSS JOIN"));
        Assert.assertTrue(planFragment.contains("|----8:EXCHANGE\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  6:CROSS JOIN"));
        Assert.assertTrue(planFragment.contains("|----5:EXCHANGE\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t3"));
    }

    @Test
    public void testCrossAndInnerJoinReorderDP() throws Exception {
        connectContext.getSessionVariable().enableDPJoinReorder();
        connectContext.getSessionVariable().disableGreedyJoinReorder();
        String sql = "select * from t1 join t3 on t1.v4 = t3.v1 join t0 join t2";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  3:OlapScanNode\n" +
                "     TABLE: t0"));
        Assert.assertTrue(planFragment.contains("  |----4:EXCHANGE\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  2:OlapScanNode\n" +
                "     TABLE: t1"));
        Assert.assertTrue(planFragment.contains("  |----6:EXCHANGE\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  1:OlapScanNode\n" +
                "     TABLE: t2"));
        Assert.assertTrue(planFragment.contains("  9:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 4: v1 = 1: v4\n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  |----8:EXCHANGE\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t3"));
    }

    @Test
    public void testInnerJoinReorderDP() throws Exception {
        connectContext.getSessionVariable().enableDPJoinReorder();
        String sql = "select * from t1 " +
                "join t3 on t1.v4 = t3.v1 " +
                "join t0 on t1.v4 = t0.v2 " +
                "join t2 on t1.v5 = t2.v8 ";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  3:OlapScanNode\n" +
                "     TABLE: t0"));
        Assert.assertTrue(planFragment.contains("  |----4:EXCHANGE\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  2:OlapScanNode\n" +
                "     TABLE: t1"));
        Assert.assertTrue(planFragment.contains("  |----6:EXCHANGE\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  1:OlapScanNode\n" +
                "     TABLE: t2"));
        Assert.assertTrue(planFragment.contains("  |----8:EXCHANGE\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t3"));
    }

    @Test
    public void testLeftJoinReorderDP() throws Exception {
        connectContext.getSessionVariable().enableDPJoinReorder();
        String sql = "select v6 from t1 " +
                "left join (select t1.v5 from t1 join t3 on t1.v4 = t3.v1 join t0 join t2) a " +
                "on t1.v6 = a.v5";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  16:HASH JOIN\n" +
                "  |  join op: RIGHT OUTER JOIN (PARTITIONED)"));

        Assert.assertTrue(planFragment.contains("  |----4:EXCHANGE\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  2:OlapScanNode\n" +
                "     TABLE: t1"));
        Assert.assertTrue(planFragment.contains("  |----7:EXCHANGE\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  1:OlapScanNode\n" +
                "     TABLE: t2"));
        Assert.assertTrue(planFragment.contains("  |----10:EXCHANGE\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t3"));
    }

    @Test
    public void testTwoJoinRootDP() throws Exception {
        connectContext.getSessionVariable().enableDPJoinReorder();
        connectContext.getSessionVariable().disableGreedyJoinReorder();
        String sql = "select * from (select t1.v5 from t1 join t3 on t3.v1 = t1.v4 join t0 join t2) b " +
                "left join (select t1.v5 from t1 join t3 on t3.v1 = t1.v4 join t0 join t2) a " +
                "on b.v5 = a.v5";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  25:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 16: v1 = 13: v4\n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  |----24:EXCHANGE\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  14:OlapScanNode\n" +
                "     TABLE: t3"));
        Assert.assertTrue(planFragment.contains("  11:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 4: v1 = 1: v4\n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  |----10:EXCHANGE\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t3"));
    }

    // Should produce three join tree and reorder three join tree.
    @Test
    public void testThreeJoinRootDP() throws Exception {
        connectContext.getSessionVariable().enableDPJoinReorder();
        connectContext.getSessionVariable().disableGreedyJoinReorder();
        String sql = "select * from (select count(t1.v5) as v55 from t1 join t3 on t3.v1 = t1.v4 join t0) b " +
                "inner join (select t1.v5 from t1 join t3 on t3.v1 = t1.v4 join t0 join t2) a " +
                "on b.v55 = a.v5";
        String planFragment = getFragmentPlan(sql);

        // Top join tree
        Assert.assertTrue(planFragment.contains("  25:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 14: v1 = 11: v4\n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  |----24:EXCHANGE\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t3\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=1/1\n" +
                "     rollup: t3\n" +
                "     tabletRatio=3/3\n" +
                "     tabletList=10033,10035,10037\n" +
                "     cardinality=1000000000\n" +
                "     avgRowSize=1.0\n" +
                "     numNodes=0\n" +
                "     use vectorized: true"));

        // Left sub join tree (b)
        Assert.assertTrue(planFragment.contains("  23:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 10: count(2: v5) = 12: v5\n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  |----22:EXCHANGE\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  20:Project\n" +
                "  |  <slot 10> : 10: count(2: v5)\n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  19:CROSS JOIN\n" +
                "  |  cross join:\n" +
                "  |  predicates is NULL.\n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  |----18:EXCHANGE\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  1:OlapScanNode\n" +
                "     TABLE: t2\n" +
                "     PREAGGREGATION: ON\n" +
                "     partitions=1/1\n" +
                "     rollup: t2\n" +
                "     tabletRatio=3/3\n" +
                "     tabletList=10024,10026,10028\n" +
                "     cardinality=100000\n" +
                "     avgRowSize=1.0\n" +
                "     numNodes=0\n" +
                "     use vectorized: true"));

        // Right sub join tree (a)
        Assert.assertTrue(planFragment.contains("  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 18\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  17:Project\n" +
                "  |  <slot 10> : 10: count(2: v5)\n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  16:CROSS JOIN\n" +
                "  |  cross join:\n" +
                "  |  predicates is NULL.\n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  |----15:EXCHANGE\n" +
                "  |       use vectorized: true\n" +
                "  |    \n" +
                "  13:AGGREGATE (merge finalize)\n" +
                "  |  output: count(10: count(2: v5))\n" +
                "  |  group by: \n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  12:EXCHANGE\n" +
                "     use vectorized: true"));
    }

    @Test
    public void testTwoJoinRootDP2() throws Exception {
        connectContext.getSessionVariable().enableDPJoinReorder();
        connectContext.getSessionVariable().disableGreedyJoinReorder();
        String sql = "select * from t1 " +
                "join t3 on t1.v4 = t3.v1 " +
                "join t0 on t1.v4 = t0.v2 " +
                "join (select * from t1 join t3 on t1.v4 = t3.v1 join t0 on t1.v4 = t0.v2 join t2 on t1.v5 = t2.v8) as a  " +
                "on t1.v5 = a.v8 ";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 13: v1 = 10: v4\n" +
                "  |  use vectorized: true"));

        Assert.assertTrue(planFragment.contains("  |  join op: INNER JOIN (BUCKET_SHUFFLE)\n" +
                "  |  hash predicates:\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 4: v1 = 1: v4\n" +
                "  |  use vectorized: true"));
    }

    @Test
    public void testOutputConstant() throws Exception {
        String sql = "select v from (select v1, 2 as v, 3 from t0 inner join t1 on v2 = v4) t,t2;";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("8:Project\n" +
                "  |  <slot 7> : 7: expr\n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  7:CROSS JOIN\n" +
                "  |  cross join:\n" +
                "  |  predicates is NULL.\n" +
                "  |  use vectorized: true"));

        sql = "select * from (select v1, 2 as v, 3 from t0 inner join t1 on v2 = v4) t,t2;";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("5:Project\n" +
                "  |  <slot 1> : 1: v1\n" +
                "  |  <slot 7> : 2\n" +
                "  |  <slot 8> : 3\n" +
                "  |  use vectorized: true\n" +
                "  |  \n" +
                "  4:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BUCKET_SHUFFLE)"));
    }
}
