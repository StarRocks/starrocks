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
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Frontend;
import com.starrocks.system.FrontendHbResponse;
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

    @Test
    public void testAddAndDropFollower() throws Exception {
        BDBJEJournal journal = (BDBJEJournal) GlobalStateMgr.getCurrentState().getJournal();
        BDBEnvironment environment = journal.getBdbEnvironment();

        // add two followers
        GlobalStateMgr.getCurrentState().addFrontend(FrontendNodeType.FOLLOWER, "192.168.2.3", 9010);
        Assert.assertEquals(1,
                environment.getReplicatedEnvironment().getRepMutableConfig().getElectableGroupSizeOverride());
        GlobalStateMgr.getCurrentState().addFrontend(FrontendNodeType.FOLLOWER, "192.168.2.4", 9010);
        Assert.assertEquals(1,
                environment.getReplicatedEnvironment().getRepMutableConfig().getElectableGroupSizeOverride());

        // one joined successfully
        new Frontend(FrontendNodeType.FOLLOWER, "node1", "192.168.2.4", 9010)
                .handleHbResponse(new FrontendHbResponse("n1", 8030, 9050,
                                1000, System.currentTimeMillis(), System.currentTimeMillis(), "v1"),
                        false);
        Assert.assertEquals(2,
                environment.getReplicatedEnvironment().getRepMutableConfig().getElectableGroupSizeOverride());

        // the other one is dropped
        GlobalStateMgr.getCurrentState().dropFrontend(FrontendNodeType.FOLLOWER, "192.168.2.3", 9010);

        Assert.assertEquals(0,
                environment.getReplicatedEnvironment().getRepMutableConfig().getElectableGroupSizeOverride());

        UtFrameUtils.PseudoImage image1 = new UtFrameUtils.PseudoImage();
        GlobalStateMgr.getCurrentState().getNodeMgr().save(image1.getDataOutputStream());
        SRMetaBlockReader reader = new SRMetaBlockReader(image1.getDataInputStream());
        GlobalStateMgr.getCurrentState().getNodeMgr().load(reader);
        reader.close();
        Assert.assertEquals(GlobalStateMgr.getCurrentState().getRemovedFrontendNames().size(), 1);
    }
}