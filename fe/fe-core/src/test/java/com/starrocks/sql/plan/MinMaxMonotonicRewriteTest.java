package com.starrocks.sql.plan;

import org.junit.jupiter.api.Test;

public class MinMaxMonotonicRewriteTest extends PlanTestBase {

    @Test
    public void testRewriteMinToDateOnDateTime() throws Exception {
        String sql = "select min(to_date(id_datetime)) from test_all_type";
        String plan = getFragmentPlan(sql);
        // Aggregation should be on the inner DATETIME slot, not on to_date()
        assertContains(plan, "output: min(8: id_datetime)");
        // There should be a Project reapplying to_date() after aggregation
        assertContains(plan, ":Project\n", "to_date(");
    }

    @Test
    public void testRewriteMaxToDateOnDateTime() throws Exception {
        String sql = "select max(to_date(id_datetime)) from test_all_type";
        String plan = getFragmentPlan(sql);
        // Aggregation should be on the inner DATETIME slot, not on to_date()
        assertContains(plan, "output: max(8: id_datetime)");
        // There should be a Project reapplying to_date() after aggregation
        assertContains(plan, ":Project\n", "to_date(");
    }

    @Test
    public void testRewriteMinCastAsDateOnDateTime() throws Exception {
        String sql = "select min(cast(id_datetime as date)) from test_all_type";
        String plan = getFragmentPlan(sql);
        // Aggregation should be on the inner DATETIME slot
        assertContains(plan, "output: min(8: id_datetime)");
        // There should be a Project with a cast to DATE after aggregation
        assertContains(plan, ":Project\n", "CAST(8: id_datetime AS DATE)");
    }
}