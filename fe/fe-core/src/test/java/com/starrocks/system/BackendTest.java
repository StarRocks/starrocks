// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


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
        Assert.assertEquals(8, BackendCoreStat.getDefaultDOP());

        BackendCoreStat.setNumOfHardwareCoresOfBe(1, 16);
        Assert.assertEquals(16, BackendCoreStat.getAvgNumOfHardwareCoresOfBe());
        Assert.assertEquals(16, BackendCoreStat.getDefaultDOP());
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
