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

package com.starrocks.qe;

import com.starrocks.common.DdlException;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.journal.bdbje.BDBJEJournal;
import com.starrocks.leader.CheckpointController;
import com.starrocks.persist.EditLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.system.Frontend;
import com.starrocks.utframe.MockJournal;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Test;

public class GlobalConnectionIdTest {
    @Test
    public void testGlobalConnectionId() throws Exception {
        new MockUp<NodeMgr>() {
            @Mock
            public Frontend getMySelf() {
                return new Frontend(0, FrontendNodeType.LEADER, "", "", 0);
            }
        };

        ConnectScheduler scheduler = new ConnectScheduler(10);
        int threshold = 1 << 24;
        for (int i = 1; i < threshold; i++) {
            scheduler.getNextConnectionId();
        }

        Assert.assertEquals(0, scheduler.getNextConnectionId());

        new MockUp<NodeMgr>() {
            @Mock
            public Frontend getMySelf() {
                return new Frontend(1, FrontendNodeType.LEADER, "", "", 0);
            }
        };
        Assert.assertEquals(threshold + 1, scheduler.getNextConnectionId());
    }

    @Test
    public void testFrontendId() throws DdlException {
        new MockUp<EditLog>() {
            @Mock
            public void logJsonObject(short op, Object obj) {
            }
        };

        GlobalStateMgr.getCurrentState().setEditLog(new EditLog(null));
        GlobalStateMgr.getCurrentState().setCheckpointController(new CheckpointController("fe", new BDBJEJournal(null, ""), ""));
        GlobalStateMgr.getCurrentState().setHaProtocol(new MockJournal.MockProtocol());

        NodeMgr nodeMgr = new NodeMgr();
        nodeMgr.addFrontend(FrontendNodeType.FOLLOWER, "192.168.0.1", 9010);
        nodeMgr.addFrontend(FrontendNodeType.FOLLOWER, "192.168.0.2", 9010);

        Assert.assertNotNull(nodeMgr.getFrontend(1));
        Assert.assertNotNull(nodeMgr.getFrontend(2));
        Assert.assertNull(nodeMgr.getFrontend(3));
        nodeMgr.setMySelf(nodeMgr.getFrontend(1));

        for (int i = 3; i < 256; i++) {
            nodeMgr.addFrontend(FrontendNodeType.FOLLOWER, "192.168.0." + i, 9010);
        }

        Frontend frontend = nodeMgr.checkFeExist("192.168.0.255", 9010);
        Assert.assertEquals(255, frontend.getFid());

        Assert.assertThrows(DdlException.class, () -> nodeMgr.addFrontend(FrontendNodeType.FOLLOWER, "192.168.1.20", 9010));

        nodeMgr.dropFrontend(FrontendNodeType.FOLLOWER, "192.168.0.20", 9010);
        nodeMgr.dropFrontend(FrontendNodeType.FOLLOWER, "192.168.0.50", 9010);
        nodeMgr.dropFrontend(FrontendNodeType.FOLLOWER, "192.168.0.70", 9010);

        nodeMgr.addFrontend(FrontendNodeType.FOLLOWER, "192.168.1.20", 9010);
        frontend = nodeMgr.checkFeExist("192.168.1.20", 9010);
        Assert.assertEquals(20, frontend.getFid());
    }
}
