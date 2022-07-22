// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.plan;

import org.junit.Test;

public class FunctionTest extends PlanTestBase {
    @Test
    public void testAssertTrue() throws Exception {
        {
            String sql = "select assert_true(null)";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "<slot 2> : assert_true(NULL)");
        }
        {
            String sql = "select assert_true(true)";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "<slot 2> : assert_true(TRUE)");
        }
        {
            String sql = "select assert_true(false)";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "<slot 2> : assert_true(FALSE)");
        }
    }
}
