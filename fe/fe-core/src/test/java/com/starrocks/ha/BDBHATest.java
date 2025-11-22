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
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
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

        BDBHA ha = (BDBHA) GlobalStateMgr.getCurrentState().getHaProtocol();
        ha.addUnstableNode("host1", 3);
        Assertions.assertEquals(2,
                environment.getReplicatedEnvironment().getRepMutableConfig().getElectableGroupSizeOverride());

        ha.addUnstableNode("host2", 4);
        Assertions.assertEquals(2,
                environment.getReplicatedEnvironment().getRepMutableConfig().getElectableGroupSizeOverride());

        ha.removeUnstableNode("host1", 4);
        Assertions.assertEquals(3,
                environment.getReplicatedEnvironment().getRepMutableConfig().getElectableGroupSizeOverride());

        ha.removeUnstableNode("host2", 4);
        Assertions.assertEquals(0,
                environment.getReplicatedEnvironment().getRepMutableConfig().getElectableGroupSizeOverride());
    }

    @Test
    public void testAddAndDropFollower() throws Exception {
        BDBJEJournal journal = (BDBJEJournal) GlobalStateMgr.getCurrentState().getJournal();
        BDBEnvironment environment = journal.getBdbEnvironment();
        NodeMgr nodeMgr = GlobalStateMgr.getCurrentState().getNodeMgr();

        // add two followers
        nodeMgr.addFrontend(FrontendNodeType.FOLLOWER, "192.168.2.3", 9010);
        Assertions.assertEquals(1,
                environment.getReplicatedEnvironment().getRepMutableConfig().getElectableGroupSizeOverride());
        nodeMgr.addFrontend(FrontendNodeType.FOLLOWER, "192.168.2.4", 9010);
        Assertions.assertEquals(1,
                environment.getReplicatedEnvironment().getRepMutableConfig().getElectableGroupSizeOverride());

        Frontend frontend = nodeMgr.getFeByHost("192.168.2.4");
        FrontendHbResponse hb = new FrontendHbResponse(frontend.getNodeName(), frontend.getQueryPort(),
                frontend.getRpcPort(), 1000, System.currentTimeMillis(), System.currentTimeMillis(),
                "v1", 0.5f, 1, null);
        // one joined successfully
        frontend.handleHbResponse(hb, false);
        Assertions.assertEquals(2,
                environment.getReplicatedEnvironment().getRepMutableConfig().getElectableGroupSizeOverride());

        // the other one is dropped
        nodeMgr.dropFrontend(FrontendNodeType.FOLLOWER, "192.168.2.3", 9010);

        Assertions.assertEquals(0,
                environment.getReplicatedEnvironment().getRepMutableConfig().getElectableGroupSizeOverride());

        UtFrameUtils.PseudoImage image1 = new UtFrameUtils.PseudoImage();
        nodeMgr.save(image1.getImageWriter());
        SRMetaBlockReader reader = new SRMetaBlockReaderV2(image1.getJsonReader());
        NodeMgr nodeMgr2 = new NodeMgr();
        nodeMgr2.load(reader);
        reader.close();
        Assertions.assertEquals(1, nodeMgr2.getRemovedFrontendNames().size());
        Assertions.assertEquals(2, nodeMgr2.getHelperNodes().size());
    }
}
