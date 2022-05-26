// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.plan;

import org.junit.Test;

public class AggregateTest extends PlanTestBase {
    @Test
    public void testAggregateDuplicatedExprs() throws Exception {
        String plan = getFragmentPlan("SELECT " +
                "sum(arrays_overlap(v3, [1])) as q1, " +
                "sum(arrays_overlap(v3, [1])) as q2, " +
                "sum(arrays_overlap(v3, [1])) as q3 FROM tarray;");
        assertContains(plan, "  2:AGGREGATE (update finalize)\n" +
                "  |  output: sum(4: arrays_overlap)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 4> : arrays_overlap(3: v3, CAST(ARRAY<tinyint(4)>[1] AS ARRAY<BIGINT>))\n" +
                "  |  \n" +
                "  0:OlapScanNode");
    }
}
