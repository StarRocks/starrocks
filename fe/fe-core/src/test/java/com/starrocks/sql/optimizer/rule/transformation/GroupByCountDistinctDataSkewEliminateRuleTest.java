package com.starrocks.sql.optimizer.rule.transformation;

import com.starrocks.sql.plan.PlanTestBase;
import org.junit.Test;

public class GroupByCountDistinctDataSkewEliminateRuleTest extends PlanTestBase {
    @Test
    public void testOptimization() {
        runFileUnitTest("group_by_count_distinct_skew_plan/skew_Q1", true);
    }

    @Test
    public void testSkewHint() {
        //runFileUnitTest();
    }
}
