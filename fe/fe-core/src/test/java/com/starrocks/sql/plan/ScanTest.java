// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.plan;

import org.junit.Test;

public class ScanTest extends PlanTestBase {
    @Test
    public void testPushDownExternalTableMissNot() throws Exception {
        String sql = "select * from ods_order where order_no not like \"%hehe%\"";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "predicates: NOT (order_no LIKE '%hehe%')");

        sql = "select * from ods_order where order_no in (1,2,3)";
        plan = getFragmentPlan(sql);
        assertContains(plan, "FROM `ods_order` WHERE (order_no IN ('1', '2', '3'))");

        sql = "select * from ods_order where order_no not in (1,2,3)";
        plan = getFragmentPlan(sql);
        assertContains(plan, "FROM `ods_order` WHERE (order_no NOT IN ('1', '2', '3'))");
    }
}
