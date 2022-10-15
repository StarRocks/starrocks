// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

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
}
