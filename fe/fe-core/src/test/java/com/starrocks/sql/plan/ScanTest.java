// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.plan;

import com.starrocks.common.FeConstants;
import org.junit.Assert;
import org.junit.Test;

public class ScanTest extends PlanTestBase {

    @Test
    public void testScan() throws Exception {
        String sql = "select * from t0";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains(" OUTPUT EXPRS:1: v1 | 2: v2 | 3: v3\n"
                + "  PARTITION: RANDOM"));
    }

    @Test
    public void testInColumnPredicate() throws Exception {
        String sql = "select v1 from t0 where v1 in (v1 + v2, sin(v2))";
        String thriftPlan = getThriftPlan(sql);
        Assert.assertFalse(thriftPlan.contains("FILTER_IN"));
    }

    @Test
    public void testOlapScanSelectedIndex() throws Exception {
        String sql = "select v1 from t0";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("rollup: t0"));
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
        Assert.assertTrue(plan.contains(" OUTPUT EXPRS:9: sum\n"
                + "  PARTITION: UNPARTITIONED\n"
                + "\n"
                + "  RESULT SINK\n"
                + "\n"
                + "  3:AGGREGATE (merge finalize)\n"
                + "  |  output: sum(9: sum)\n"
                + "  |  group by: \n"));
        connectContext.getSessionVariable().setNewPlanerAggStage(0);
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testPreAggregation() throws Exception {
        String sql = "select k1 from t0 inner join baseall on v1 = cast(k8 as int) group by k1";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("1:Project\n" +
                "  |  <slot 4> : 4: k1\n" +
                "  |  <slot 15> : CAST(CAST(13: k8 AS INT) AS BIGINT)\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     TABLE: baseall\n" +
                "     PREAGGREGATION: OFF. Reason: Predicates include the value column\n" +
                "     partitions=0/1"));

        sql = "select 0 from baseall inner join t0 on v1 = k1 group by (v2 + k2),k1";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("0:OlapScanNode\n" +
                "     TABLE: baseall\n" +
                "     PREAGGREGATION: OFF. Reason: Group columns isn't bound table baseall"));
    }

    @Test
    public void testInformationSchema() throws Exception {
        String sql = "select column_name from information_schema.columns limit 1;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  RESULT SINK\n" +
                "\n" +
                "  0:SCAN SCHEMA\n" +
                "     limit: 1\n"));
    }

    @Test
    public void testInformationSchema1() throws Exception {
        String sql = "select column_name, UPPER(DATA_TYPE) from information_schema.columns;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  1:Project\n"
                + "  |  <slot 4> : 4: COLUMN_NAME\n"
                + "  |  <slot 25> : upper(8: DATA_TYPE)\n"
                + "  |  \n"
                + "  0:SCAN SCHEMA\n"));
    }
}
