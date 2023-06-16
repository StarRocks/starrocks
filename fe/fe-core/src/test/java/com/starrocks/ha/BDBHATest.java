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


package com.starrocks.ha;

import com.starrocks.journal.bdbje.BDBEnvironment;
import com.starrocks.journal.bdbje.BDBJEJournal;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class BDBHATest {

    @BeforeClass
    public static void beforeClass() {
        UtFrameUtils.createMinStarRocksCluster(true);
        UtFrameUtils.PseudoImage.setUpImageVersion();
    }

    @Test
    public void testAddAndRemoveUnstableNode() {
        BDBJEJournal journal = (BDBJEJournal) GlobalStateMgr.getCurrentState().getJournal();
        BDBEnvironment environment = journal.getBdbEnvironment();

        BDBHA ha = (BDBHA) GlobalStateMgr.getCurrentState().getHaProtocol();
        ha.addUnstableNode("host1", 3);
        Assert.assertEquals(2,
                environment.getReplicatedEnvironment().getRepMutableConfig().getElectableGroupSizeOverride());

        ha.addUnstableNode("host2", 4);
        Assert.assertEquals(2,
                environment.getReplicatedEnvironment().getRepMutableConfig().getElectableGroupSizeOverride());

        ha.removeUnstableNode("host1", 4);
        Assert.assertEquals(3,
                environment.getReplicatedEnvironment().getRepMutableConfig().getElectableGroupSizeOverride());

        ha.removeUnstableNode("host2", 4);
        Assert.assertEquals(0,
                environment.getReplicatedEnvironment().getRepMutableConfig().getElectableGroupSizeOverride());
    }
}