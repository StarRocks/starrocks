// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.plan;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TPCDSCTETest extends TPCDSPlanTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        TPCDSPlanTestBase.beforeClass();
        connectContext.getSessionVariable().setCboCTERuseRatio(0);
    }

    @AfterClass
    public static void afterClass() {
        connectContext.getSessionVariable().setCboCTERuseRatio(1.5);
    }

    public void testCTE(String sql) throws Exception {
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("MultiCastDataSinks"));
    }

    public void testAllInlineCTE(String sql) throws Exception {
        String plan = getFragmentPlan(sql);
        Assert.assertFalse(plan.contains("MultiCastDataSinks"));
    }

    @Test
    public void testQ3() throws Exception {
        testCTE(Q3);
    }

    @Test
    public void testQ5() throws Exception {
        testCTE(Q5);
    }

    @Test
    public void testQ6() throws Exception {
        testAllInlineCTE(Q6);
    }

    @Test
    public void testQ10() throws Exception {
        testAllInlineCTE(Q10);
    }

    @Test
    public void testQ12() throws Exception {
        testCTE(Q12);
    }

    @Test
    public void testQ19() throws Exception {
        testAllInlineCTE(Q19);
    }

    @Test
    public void testQ20() throws Exception {
        testCTE(Q20);
    }

    @Test
    public void testQ22() throws Exception {
        testAllInlineCTE(Q22);
    }

    @Test
    public void testQ29() throws Exception {
        testAllInlineCTE(Q29);
    }

    @Test
    public void testQ30() throws Exception {
        testCTE(Q30);
    }

    @Test
    public void testQ37() throws Exception {
        testCTE(Q37);
    }

    @Test
    public void testQ38() throws Exception {
        testAllInlineCTE(Q38);
    }

    @Test
    public void testQ42() throws Exception {
        testCTE(Q42);
    }

    @Test
    public void testQ43() throws Exception {
        testCTE(Q43);
    }

    @Test
    public void testQ46() throws Exception {
        testAllInlineCTE(Q46);
    }

    @Test
    public void testQ50() throws Exception {
        testCTE(Q50);
    }

    @Test
    public void testQ51() throws Exception {
        testCTE(Q51);
    }

    @Test
    public void testQ68() throws Exception {
        testCTE(Q68);
    }

    @Test
    public void testQ69() throws Exception {
        testCTE(Q69);
    }

    @Test
    public void testQ70() throws Exception {
        testCTE(Q70);
    }

    @Test
    public void testQ75() throws Exception {
        testCTE(Q75);
    }

    @Test
    public void testQ76() throws Exception {
        testCTE(Q76);
    }

    @Test
    public void testQ78() throws Exception {
        testAllInlineCTE(Q78);
    }

    @Test
    public void testQ81() throws Exception {
        testAllInlineCTE(Q81);
    }

    @Test
    public void testQ83() throws Exception {
        testAllInlineCTE(Q83);
    }

    @Test
    public void testQ84() throws Exception {
        testCTE(Q84);
    }

    @Test
    public void testQ92() throws Exception {
        testCTE(Q92);
    }

    @Test
    public void testQ93() throws Exception {
        testCTE(Q93);
    }

    @Test
    public void testQ96() throws Exception {
        testAllInlineCTE(Q96);
    }

    @Test
    public void testQ98() throws Exception {
        testAllInlineCTE(Q98);
    }

    @Test
    public void testQ80_2() throws Exception {
        testAllInlineCTE(Q80_2);
    }

    @Test
    public void testQ95_2() throws Exception {
        testCTE(Q95_2);
    }
}
