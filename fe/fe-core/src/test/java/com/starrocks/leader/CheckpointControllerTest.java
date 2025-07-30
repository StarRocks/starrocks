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

package com.starrocks.leader;

import com.starrocks.common.Config;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.system.Frontend;
import com.starrocks.utframe.MockJournal;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CheckpointControllerTest {
    private MockedStatic<GlobalStateMgr> globalStateMgrStatic;
    private CheckpointController controller;
    private Frontend leader;
    private Frontend follower1;
    private Frontend follower2;

    @BeforeEach
    public void setUp() {
        // Mock GlobalStateMgr and NodeMgr
        globalStateMgrStatic = Mockito.mockStatic(GlobalStateMgr.class, Mockito.CALLS_REAL_METHODS);
        GlobalStateMgr globalStateMgr = Mockito.mock(GlobalStateMgr.class, Mockito.RETURNS_DEEP_STUBS);
        NodeMgr nodeMgr = new NodeMgr();
        globalStateMgrStatic.when(GlobalStateMgr::getServingState).thenReturn(globalStateMgr);
        Mockito.when(globalStateMgr.getNodeMgr()).thenReturn(nodeMgr);

        controller = new CheckpointController("test", new MockJournal(), "");
        // leader and followers
        leader = new Frontend(FrontendNodeType.LEADER, "leader", "127.0.0.1", 9010);
        leader.setAlive(true);
        leader.setFid(1);
        leader.setRpcPort(9020);
        leader.setHeapUsedPercent(10.0f);
        follower1 = new Frontend(FrontendNodeType.FOLLOWER, "follower1", "127.0.0.2", 9011);
        follower1.setAlive(true);
        follower1.setFid(2);
        follower1.setRpcPort(9021);
        follower1.setHeapUsedPercent(20.0f);
        follower2 = new Frontend(FrontendNodeType.FOLLOWER, "follower2", "127.0.0.3", 9012);
        follower2.setAlive(true);
        follower2.setFid(3);
        follower2.setRpcPort(9022);
        follower2.setHeapUsedPercent(30.0f);
        nodeMgr.setMySelf(leader);
        nodeMgr.replayAddFrontend(leader);
        nodeMgr.replayAddFrontend(follower1);
        nodeMgr.replayAddFrontend(follower2);
    }

    @AfterEach
    public void tearDown() {
        globalStateMgrStatic.close();
    }

    @Test
    public void testGetWorkers_checkpointOnlyOnLeader_true() {
        boolean oldValue = Config.checkpoint_only_on_leader;
        Config.checkpoint_only_on_leader = true;
        List<Frontend> workers = controller.getWorkers(false);
        assertEquals(1, workers.size());
        assertEquals("leader", workers.get(0).getNodeName());
        Config.checkpoint_only_on_leader = oldValue;
    }

    @Test
    public void testGetWorkers_needClusterSnapshotInfo_true() {
        boolean oldValue = Config.checkpoint_only_on_leader;
        Config.checkpoint_only_on_leader = false;
        List<Frontend> workers = controller.getWorkers(true);
        assertEquals(1, workers.size());
        assertEquals("leader", workers.get(0).getNodeName());
        Config.checkpoint_only_on_leader = oldValue;
    }

    @Test
    public void testGetWorkers_sortByLastFailedTime() {
        boolean oldValue = Config.checkpoint_only_on_leader;
        Config.checkpoint_only_on_leader = false;
        controller.setLastFailedTime("follower2", System.currentTimeMillis());
        controller.setLastFailedTime("follower1", System.currentTimeMillis() - 10000);
        List<Frontend> workers = controller.getWorkers(false);
        int idx1 = workers.indexOf(follower1);
        int idx2 = workers.indexOf(follower2);
        assertTrue(idx1 < idx2);
        Config.checkpoint_only_on_leader = oldValue;
    }

    @Test
    public void testGetWorkers_sortByHeapUsedPercent() {
        boolean oldValue = Config.checkpoint_only_on_leader;
        Config.checkpoint_only_on_leader = false;
        List<Frontend> workers = controller.getWorkers(false);
        // follower1 heapUsedPercent=20, follower2=30, leader=10(MAX)
        int idx1 = workers.indexOf(follower1);
        int idx2 = workers.indexOf(follower2);
        int idxLeader = workers.indexOf(leader);
        assertTrue(idx1 < idx2);
        assertEquals(2, idxLeader);
        Config.checkpoint_only_on_leader = oldValue;
    }
}
