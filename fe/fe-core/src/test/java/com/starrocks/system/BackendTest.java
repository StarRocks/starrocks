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

<<<<<<< HEAD

=======
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
package com.starrocks.system;

import org.junit.Assert;
import org.junit.Test;

<<<<<<< HEAD

=======
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
public class BackendTest {

    @Test
    public void testSetHeartbeatPort() {
        Backend be = new Backend();
        be.setHeartbeatPort(1000);
        Assert.assertTrue(be.getHeartbeatPort() == 1000);
    }

    @Test
    public void cpuCoreUpdate() {
<<<<<<< HEAD
        BackendCoreStat.setNumOfHardwareCoresOfBe(1, 8);
        Assert.assertEquals(8, BackendCoreStat.getAvgNumOfHardwareCoresOfBe());
        Assert.assertEquals(4, BackendCoreStat.getDefaultDOP());

        BackendCoreStat.setNumOfHardwareCoresOfBe(1, 16);
        Assert.assertEquals(16, BackendCoreStat.getAvgNumOfHardwareCoresOfBe());
        Assert.assertEquals(8, BackendCoreStat.getDefaultDOP());

        // add new backend 2
        BackendCoreStat.setNumOfHardwareCoresOfBe(2, 8);
        Assert.assertEquals(12, BackendCoreStat.getAvgNumOfHardwareCoresOfBe());
        Assert.assertEquals(6, BackendCoreStat.getDefaultDOP());

        // remove new backend 2
        BackendCoreStat.removeNumOfHardwareCoresOfBe(2);
        Assert.assertEquals(16, BackendCoreStat.getAvgNumOfHardwareCoresOfBe());
        Assert.assertEquals(8, BackendCoreStat.getDefaultDOP());
=======
        BackendResourceStat.getInstance().setNumHardwareCoresOfBe(1, 8);
        Assert.assertEquals(8, BackendResourceStat.getInstance().getAvgNumHardwareCoresOfBe());
        Assert.assertEquals(4, BackendResourceStat.getInstance().getDefaultDOP());

        BackendResourceStat.getInstance().setNumHardwareCoresOfBe(1, 16);
        Assert.assertEquals(16, BackendResourceStat.getInstance().getAvgNumHardwareCoresOfBe());
        Assert.assertEquals(8, BackendResourceStat.getInstance().getDefaultDOP());

        // add new backend 2
        BackendResourceStat.getInstance().setNumHardwareCoresOfBe(2, 8);
        Assert.assertEquals(12, BackendResourceStat.getInstance().getAvgNumHardwareCoresOfBe());
        Assert.assertEquals(6, BackendResourceStat.getInstance().getDefaultDOP());

        // remove new backend 2
        BackendResourceStat.getInstance().removeBe(2);
        Assert.assertEquals(16, BackendResourceStat.getInstance().getAvgNumHardwareCoresOfBe());
        Assert.assertEquals(8, BackendResourceStat.getInstance().getDefaultDOP());
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    @Test
    public void defaultSinkDopTest() {
<<<<<<< HEAD
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
=======
        BackendResourceStat.getInstance().setNumHardwareCoresOfBe(1, 8);
        Assert.assertEquals(8, BackendResourceStat.getInstance().getAvgNumHardwareCoresOfBe());
        Assert.assertEquals(2, BackendResourceStat.getInstance().getSinkDefaultDOP());

        BackendResourceStat.getInstance().setNumHardwareCoresOfBe(1, 16);
        Assert.assertEquals(16, BackendResourceStat.getInstance().getAvgNumHardwareCoresOfBe());
        Assert.assertEquals(5, BackendResourceStat.getInstance().getSinkDefaultDOP());

        BackendResourceStat.getInstance().setNumHardwareCoresOfBe(1, 24);
        Assert.assertEquals(24, BackendResourceStat.getInstance().getAvgNumHardwareCoresOfBe());
        Assert.assertEquals(8, BackendResourceStat.getInstance().getSinkDefaultDOP());

        BackendResourceStat.getInstance().setNumHardwareCoresOfBe(1, 32);
        Assert.assertEquals(32, BackendResourceStat.getInstance().getAvgNumHardwareCoresOfBe());
        Assert.assertEquals(8, BackendResourceStat.getInstance().getSinkDefaultDOP());

        BackendResourceStat.getInstance().setNumHardwareCoresOfBe(1, 48);
        Assert.assertEquals(48, BackendResourceStat.getInstance().getAvgNumHardwareCoresOfBe());
        Assert.assertEquals(12, BackendResourceStat.getInstance().getSinkDefaultDOP());

        BackendResourceStat.getInstance().setNumHardwareCoresOfBe(1, 64);
        Assert.assertEquals(64, BackendResourceStat.getInstance().getAvgNumHardwareCoresOfBe());
        Assert.assertEquals(16, BackendResourceStat.getInstance().getSinkDefaultDOP());
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }
}
