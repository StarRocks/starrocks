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
import com.starrocks.persist.DropFrontendInfo;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.server.RunMode;
import com.starrocks.system.Frontend;
import com.starrocks.system.FrontendHbResponse;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class BDBHATest {

    @BeforeAll
    public static void beforeClass() {
        UtFrameUtils.createMinStarRocksCluster(true, RunMode.SHARED_NOTHING);
    }

    @Test
    public void testAddAndRemoveUnstableNode() {
        BDBJEJournal journal = (BDBJEJournal) GlobalStateMgr.getCurrentState().getJournal();
        BDBEnvironment environment = journal.getBdbEnvironment();
        NodeMgr nodeMgr = GlobalStateMgr.getCurrentState().getNodeMgr();

        BDBHA ha = (BDBHA) GlobalStateMgr.getCurrentState().getHaProtocol();
        int baselineFollowerCnt = nodeMgr.getFollowerCnt();
        Frontend frontend1 = new Frontend(nodeMgr.allocateNextFrontendId(),
                FrontendNodeType.FOLLOWER, "host1", "192.168.2.3", 9010);
        Frontend frontend2 = null;
        boolean addedFrontend1 = false;
        boolean addedFrontend2 = false;

        try {
            nodeMgr.replayAddFrontend(frontend1);
            addedFrontend1 = true;
            ha.addUnstableNode(frontend1.getNodeName(), nodeMgr.getFollowerCnt());
            Assertions.assertEquals(baselineFollowerCnt,
                    environment.getReplicatedEnvironment().getRepMutableConfig().getElectableGroupSizeOverride());

            frontend2 = new Frontend(nodeMgr.allocateNextFrontendId(),
                    FrontendNodeType.FOLLOWER, "host2", "192.168.2.4", 9010);
            nodeMgr.replayAddFrontend(frontend2);
            addedFrontend2 = true;
            ha.addUnstableNode(frontend2.getNodeName(), nodeMgr.getFollowerCnt());
            Assertions.assertEquals(baselineFollowerCnt,
                    environment.getReplicatedEnvironment().getRepMutableConfig().getElectableGroupSizeOverride());

            Assertions.assertFalse(frontend1.isAlive());
            frontend1.handleHbResponse(new FrontendHbResponse(frontend1.getNodeName(), 9030,
                    9020, 1000, System.currentTimeMillis(), System.currentTimeMillis(),
                    "v1", 0.5f, 1, null), false);
            Assertions.assertTrue(frontend1.isAlive());
            Assertions.assertEquals(baselineFollowerCnt + 1,
                    environment.getReplicatedEnvironment().getRepMutableConfig().getElectableGroupSizeOverride());

            Assertions.assertFalse(frontend2.isAlive());
            frontend2.handleHbResponse(new FrontendHbResponse(frontend2.getNodeName(), 9030,
                    9020, 1000, System.currentTimeMillis(), System.currentTimeMillis(),
                    "v1", 0.5f, 1, null), false);
            Assertions.assertTrue(frontend2.isAlive());
            Assertions.assertEquals(0,
                    environment.getReplicatedEnvironment().getRepMutableConfig().getElectableGroupSizeOverride());
        } finally {
            if (addedFrontend2) {
                nodeMgr.replayDropFrontend(new DropFrontendInfo(frontend2.getNodeName()));
            }
            if (addedFrontend1) {
                nodeMgr.replayDropFrontend(new DropFrontendInfo(frontend1.getNodeName()));
            }
        }
    }
}
