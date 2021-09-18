// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.plan;

import org.junit.Assert;
import org.junit.Test;

public class SchemaScanTest extends PlanTestBase {

    @Test
    public void testSchemaScan() throws Exception {
        String sql = "select * from information_schema.columns";
        String planFragment = getFragmentPlan(sql);
        System.out.println(planFragment);
        Assert.assertTrue(planFragment.contains("PARTITION: RANDOM\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  0:SCAN SCHEMA\n" +
                "     use vectorized: true"));
    }
}
