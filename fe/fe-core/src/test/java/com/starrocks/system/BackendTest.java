// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.system;

import org.junit.Assert;
import org.junit.Test;


public class BackendTest {

    @Test
    public void testSetHeartbeatPort() {
        Backend be = new Backend();
        be.setHeartbeatPort(1000);
        Assert.assertTrue(be.getHeartbeatPort() == 1000);
    }

    @Test
    public void cpuCoreUpdate() {
        BackendCoreStat.setNumOfHardwareCoresOfBe(1, 8);
        Assert.assertEquals(8, BackendCoreStat.getAvgNumOfHardwareCoresOfBe());
        Assert.assertEquals(4, BackendCoreStat.getDefaultDOP());

        BackendCoreStat.setNumOfHardwareCoresOfBe(1, 16);
        Assert.assertEquals(16, BackendCoreStat.getAvgNumOfHardwareCoresOfBe());
        Assert.assertEquals(8, BackendCoreStat.getDefaultDOP());
    }

    @Test
    public void defaultSinkDopTest() {
        BackendCoreStat.setNumOfHardwareCoresOfBe(1, 8);
        Assert.assertEquals(8, BackendCoreStat.getAvgNumOfHardwareCoresOfBe());
        Assert.assertEquals(2, BackendCoreStat.getSinkDefaultDOP());

        BackendCoreStat.setNumOfHardwareCoresOfBe(1, 16);
        Assert.assertEquals(16, BackendCoreStat.getAvgNumOfHardwareCoresOfBe());
        Assert.assertEquals(5, BackendCoreStat.getSinkDefaultDOP());

        BackendCoreStat.setNumOfHardwareCoresOfBe(1, 24);
        Assert.assertEquals(24, BackendCoreStat.getAvgNumOfHardwareCoresOfBe());
        Assert.assertEquals(8, BackendCoreStat.getSinkDefaultDOP());

        BackendCoreStat.setNumOfHardwareCoresOfBe(1, 32);
        Assert.assertEquals(32, BackendCoreStat.getAvgNumOfHardwareCoresOfBe());
        Assert.assertEquals(8, BackendCoreStat.getSinkDefaultDOP());

        BackendCoreStat.setNumOfHardwareCoresOfBe(1, 48);
        Assert.assertEquals(48, BackendCoreStat.getAvgNumOfHardwareCoresOfBe());
        Assert.assertEquals(12, BackendCoreStat.getSinkDefaultDOP());

        BackendCoreStat.setNumOfHardwareCoresOfBe(1, 64);
        Assert.assertEquals(64, BackendCoreStat.getAvgNumOfHardwareCoresOfBe());
        Assert.assertEquals(16, BackendCoreStat.getSinkDefaultDOP());
    }
}
