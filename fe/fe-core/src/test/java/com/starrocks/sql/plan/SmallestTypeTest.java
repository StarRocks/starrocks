// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.plan;

import org.junit.Assert;
import org.junit.Test;

public class SmallestTypeTest extends PlanTestBase {
    @Test
    public void testAdd() throws Exception {
        String sql = "select t1b + 1 from test_all_type";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n" +
                "  |  <slot 11> : CAST(2: t1b AS INT) + 1\n" +
                "  |  "));

        sql = "select t1c + 1 from test_all_type";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n" +
                "  |  <slot 11> : CAST(3: t1c AS BIGINT) + 1\n" +
                "  |  "));

        sql = "select t1d + 1 from test_all_type";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n" +
                "  |  <slot 11> : 4: t1d + 1\n" +
                "  |  "));
    }

    @Test
    public void testSubtract() throws Exception {
        String sql = "select t1b - 1 from test_all_type";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n" +
                "  |  <slot 11> : CAST(2: t1b AS INT) - 1\n" +
                "  |  "));

        sql = "select t1c - 1 from test_all_type";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n" +
                "  |  <slot 11> : CAST(3: t1c AS BIGINT) - 1\n" +
                "  |  "));

        sql = "select t1d - 1 from test_all_type";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n" +
                "  |  <slot 11> : 4: t1d - 1\n" +
                "  |  "));
    }

    @Test
    public void testMultiply() throws Exception {
        String sql = "select t1b * 1 from test_all_type";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n" +
                "  |  <slot 11> : CAST(2: t1b AS INT) * 1\n" +
                "  |  "));

        sql = "select t1c * 1 from test_all_type";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n" +
                "  |  <slot 11> : CAST(3: t1c AS BIGINT) * 1\n" +
                "  |  "));

        sql = "select t1d * 1 from test_all_type";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n" +
                "  |  <slot 11> : 4: t1d * 1\n" +
                "  |  "));
    }

    @Test
    public void testMod() throws Exception {
        String sql = "select t1b % 1 from test_all_type";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n" +
                "  |  <slot 11> : 2: t1b % 1\n" +
                "  |  "));

        sql = "select t1c % 1 from test_all_type";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n" +
                "  |  <slot 11> : 3: t1c % 1\n" +
                "  |  "));

        sql = "select t1d % 1 from test_all_type";
        planFragment = getFragmentPlan(sql);
        System.out.println(planFragment);
        Assert.assertTrue(planFragment.contains("  1:Project\n" +
                "  |  <slot 11> : 4: t1d % 1\n" +
                "  |  "));
    }

    @Test
    public void testDivide() throws Exception {
        String sql = "select t1b / 1 from test_all_type";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n" +
                "  |  <slot 11> : CAST(2: t1b AS DOUBLE) / 1.0\n" +
                "  |  "));

        sql = "select t1c / 1 from test_all_type";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n" +
                "  |  <slot 11> : CAST(3: t1c AS DOUBLE) / 1.0\n" +
                "  |  "));

        sql = "select t1d / 1 from test_all_type";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n" +
                "  |  <slot 11> : CAST(4: t1d AS DOUBLE) / 1.0\n" +
                "  |  "));
    }

    @Test
    public void testIntDivide() throws Exception {
        String sql = "select t1b DIV 1 from test_all_type";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n" +
                "  |  <slot 11> : 2: t1b DIV 1\n" +
                "  |  "));

        sql = "select t1c DIV 1 from test_all_type";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n" +
                "  |  <slot 11> : 3: t1c DIV 1\n" +
                "  |  "));

        sql = "select t1d DIV 1 from test_all_type";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n" +
                "  |  <slot 11> : 4: t1d DIV 1\n" +
                "  |  "));
    }

    @Test
    public void testBitwiseOperations() throws Exception {
        String sql = "select t1b & 1 from test_all_type";
        String planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n" +
                "  |  <slot 11> : 2: t1b & 1\n" +
                "  |  "));

        sql = "select t1b | 1 from test_all_type";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n" +
                "  |  <slot 11> : 2: t1b | 1\n" +
                "  |  "));

        sql = "select t1b ^ 1 from test_all_type";
        planFragment = getFragmentPlan(sql);
        Assert.assertTrue(planFragment.contains("  1:Project\n" +
                "  |  <slot 11> : 2: t1b ^ 1\n" +
                "  |  "));
    }
}
