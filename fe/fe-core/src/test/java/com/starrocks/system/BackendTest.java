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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BackendTest {

    @Test
    public void testSetHeartbeatPort() {
        Backend be = new Backend();
        be.setHeartbeatPort(1000);
        Assertions.assertTrue(be.getHeartbeatPort() == 1000);
    }

    @Test
    public void cpuCoreUpdate() {
        BackendResourceStat.getInstance().setNumHardwareCoresOfBe(1, 8);
        Assertions.assertEquals(8, BackendResourceStat.getInstance().getAvgNumHardwareCoresOfBe());
        Assertions.assertEquals(4, BackendResourceStat.getInstance().getDefaultDOP());

        BackendResourceStat.getInstance().setNumHardwareCoresOfBe(1, 16);
        Assertions.assertEquals(16, BackendResourceStat.getInstance().getAvgNumHardwareCoresOfBe());
        Assertions.assertEquals(8, BackendResourceStat.getInstance().getDefaultDOP());

        // add new backend 2
        BackendResourceStat.getInstance().setNumHardwareCoresOfBe(2, 8);
        Assertions.assertEquals(12, BackendResourceStat.getInstance().getAvgNumHardwareCoresOfBe());
        Assertions.assertEquals(6, BackendResourceStat.getInstance().getDefaultDOP());

        // remove new backend 2
        BackendResourceStat.getInstance().removeBe(2);
        Assertions.assertEquals(16, BackendResourceStat.getInstance().getAvgNumHardwareCoresOfBe());
        Assertions.assertEquals(8, BackendResourceStat.getInstance().getDefaultDOP());
    }

    @Test
    public void defaultSinkDopTest() {
        BackendResourceStat.getInstance().setNumHardwareCoresOfBe(1, 8);
        Assertions.assertEquals(8, BackendResourceStat.getInstance().getAvgNumHardwareCoresOfBe());
        Assertions.assertEquals(2, BackendResourceStat.getInstance().getSinkDefaultDOP());

        BackendResourceStat.getInstance().setNumHardwareCoresOfBe(1, 16);
        Assertions.assertEquals(16, BackendResourceStat.getInstance().getAvgNumHardwareCoresOfBe());
        Assertions.assertEquals(5, BackendResourceStat.getInstance().getSinkDefaultDOP());

        BackendResourceStat.getInstance().setNumHardwareCoresOfBe(1, 24);
        Assertions.assertEquals(24, BackendResourceStat.getInstance().getAvgNumHardwareCoresOfBe());
        Assertions.assertEquals(8, BackendResourceStat.getInstance().getSinkDefaultDOP());

        BackendResourceStat.getInstance().setNumHardwareCoresOfBe(1, 32);
        Assertions.assertEquals(32, BackendResourceStat.getInstance().getAvgNumHardwareCoresOfBe());
        Assertions.assertEquals(8, BackendResourceStat.getInstance().getSinkDefaultDOP());

        BackendResourceStat.getInstance().setNumHardwareCoresOfBe(1, 48);
        Assertions.assertEquals(48, BackendResourceStat.getInstance().getAvgNumHardwareCoresOfBe());
        Assertions.assertEquals(12, BackendResourceStat.getInstance().getSinkDefaultDOP());

        BackendResourceStat.getInstance().setNumHardwareCoresOfBe(1, 64);
        Assertions.assertEquals(64, BackendResourceStat.getInstance().getAvgNumHardwareCoresOfBe());
        Assertions.assertEquals(16, BackendResourceStat.getInstance().getSinkDefaultDOP());
    }
}
