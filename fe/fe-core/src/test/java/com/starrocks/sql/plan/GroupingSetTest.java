// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.plan;

import org.junit.Assert;
import org.junit.Test;

public class GroupingSetTest extends PlanTestBase {
    @Test
    public void testGroupByCube() throws Exception {
        String sql = "select grouping_id(v1, v3), grouping(v2) from t0 group by cube(v1, v2, v3);";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("REPEAT_NODE"));
    }


}
