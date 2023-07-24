// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.plan;

<<<<<<< HEAD
import com.clearspring.analytics.util.Lists;
=======
>>>>>>> e1ee806fa8 ([BugFix] Revert invalid date partition prune (#27780))
import com.starrocks.common.FeConstants;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class PartitionPruneTest extends PlanTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
        starRocksAssert.withTable("CREATE TABLE `ptest` (\n"
                + "  `k1` int(11) NOT NULL COMMENT \"\",\n"
                + "  `d2` date NOT NULL COMMENT \"\",\n"
                + "  `v1` int(11) NULL COMMENT \"\",\n"
                + "  `v2` int(11) NULL COMMENT \"\",\n"
                + "  `v3` int(11) NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`, `d2`)\n"
                + "COMMENT \"OLAP\"\n"
                + "PARTITION BY RANGE(`d2`)\n"
                + "(PARTITION p202001 VALUES [('0000-01-01'), ('2020-01-01')),\n"
                + "PARTITION p202004 VALUES [('2020-01-01'), ('2020-04-01')),\n"
                + "PARTITION p202007 VALUES [('2020-04-01'), ('2020-07-01')),\n"
                + "PARTITION p202012 VALUES [('2020-07-01'), ('2020-12-01')))\n"
                + "DISTRIBUTED BY HASH(`k1`) BUCKETS 10\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"DEFAULT\"\n"
                + ");");
    }

    @Test
    public void testPredicatePrune1() throws Exception {
        String sql = getFragmentPlan("select * from ptest where d2 >= '2020-01-01';");
        assertTrue(sql.contains("     TABLE: ptest\n"
                + "     PREAGGREGATION: ON\n"
                + "     partitions=3/4\n"
                + "     rollup: ptest"));
    }

    @Test
    public void testPredicatePrune2() throws Exception {
        String sql = getFragmentPlan("select * from ptest where d2 > '2020-01-01';");
        assertTrue(sql.contains("TABLE: ptest\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 2: d2 > '2020-01-01'\n" +
                "     partitions=3/4\n" +
                "     rollup: ptest"));
    }

    @Test
    public void testPredicatePrune3() throws Exception {
        String sql = getFragmentPlan("select * from ptest where d2 >= '2020-01-01' and d2 <= '2020-07-01';");
        assertTrue(sql.contains("TABLE: ptest\n"
                + "     PREAGGREGATION: ON\n"
                + "     PREDICATES: 2: d2 <= '2020-07-01'\n"
                + "     partitions=3/4\n"
                + "     rollup: ptest"));
    }

    @Test
    public void testPredicatePrune4() throws Exception {
        String sql = getFragmentPlan("select * from ptest where d2 >= '2020-01-01' and d2 < '2020-07-01';");
        assertTrue(sql.contains("     TABLE: ptest\n"
                + "     PREAGGREGATION: ON\n"
                + "     partitions=2/4\n"
                + "     rollup: ptest"));
    }

    @Test
    public void testPredicatePrune5() throws Exception {
        String sql = getFragmentPlan("select * from ptest where d2 = '2020-08-01' and d2 < '2020-07-01';");
        assertTrue(sql.contains("  0:EMPTYSET\n"));
    }

    @Test
    public void testPredicatePrune6() throws Exception {
        String sql = getFragmentPlan("select * from ptest where d2 = '2020-08-01' and d2 = '2020-09-01';");
        assertTrue(sql.contains("  0:EMPTYSET\n"));
    }

    @Test
    public void testPredicateEqPrune() throws Exception {
        String sql = getFragmentPlan("select * from ptest where d2 = '2020-07-01'");
        assertTrue(sql.contains("  0:OlapScanNode\n" +
                "     TABLE: ptest\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 2: d2 = '2020-07-01'\n" +
                "     partitions=1/4\n" +
                "     rollup: ptest"));
    }

    @Test
<<<<<<< HEAD
    public void testInvalidDatePrune() throws Exception {
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(300000);
        List<String> sqls = Lists.newArrayList();

        String plan = "";
        sqls.add("select * from ptest where d2 in ('1998-01-32', 'abc', 'abc')");
        sqls.add("select * from ptest where d2 <= '1998-01-32'");
        for (String sql : sqls) {
            plan = getFragmentPlan(sql);
            assertContains(plan, "partitions=0/4");
        }

        sqls.clear();
        sqls.add("select * from ptest where d2 in ('abc')");
        sqls.add("select * from ptest where d2 in ('1998-01-32')");
        sqls.add("select * from ptest where d2 = '1998-01-32'");
        sqls.add("select * from ptest where d2 in ('1998-01-01', 'abc', '1998-13-01')");
        for (String sql : sqls) {
            plan = getFragmentPlan(sql);
            assertContains(plan, "partitions=1/4");
        }

        sqls.clear();
        sqls.add("select * from ptest where d2 in ('2020-06-01', 'abc', '1998-11-01')");
        sqls.add("select * from ptest where d2 in ('2020-06-01', 'abc', '1998-11-01', '2001-01-33')");
        for (String sql : sqls) {
            plan = getFragmentPlan(sql);
            assertContains(plan, "partitions=2/4");
        }

        sqls.clear();
        sqls.add("select * from ptest where d2 in ('1998-01-32', cast(cast('2021-01-12' as SIGNED) as DATE))");
        sqls.add("select * from ptest where d2 in ('1998-01-01', cast(cast('2021-01-12' as SIGNED) as DATE))");
        for (String sql : sqls) {
            plan = getFragmentPlan(sql);
            assertContains(plan, "partitions=4/4");
        }
=======
    public void testInClauseCombineOr_1() throws Exception {
        String plan = getFragmentPlan("select * from ptest where (d2 > '1000-01-01') or (d2 in (null, '2020-01-01'));");
        assertTrue(plan.contains("  0:OlapScanNode\n" +
                "     TABLE: ptest\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: (2: d2 > '1000-01-01') OR (2: d2 IN (NULL, '2020-01-01')), 2: d2 > '1000-01-01'\n" +
                "     partitions=4/4\n" +
                "     rollup: ptest"));
    }

    @Test
    public void testInClauseCombineOr_2() throws Exception {
        String plan = getFragmentPlan("select * from ptest where (d2 > '1000-01-01') or (d2 in (null, null));");
        assertTrue(plan.contains("  0:OlapScanNode\n" +
                "     TABLE: ptest\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: (2: d2 > '1000-01-01') OR (2: d2 IN (NULL, NULL)), 2: d2 > '1000-01-01'\n" +
                "     partitions=4/4\n" +
                "     rollup: ptest"));
>>>>>>> e1ee806fa8 ([BugFix] Revert invalid date partition prune (#27780))
    }

    @Test
    public void testRightCastDatePrune() throws Exception {
        String sql = "select * from ptest where d2 <= '2020-05-01T13:45:57'";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "partitions=3/4");
    }
}
