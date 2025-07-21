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

package com.starrocks.server;

import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.system.Frontend;
import com.starrocks.system.FrontendHbResponse;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.UnknownHostException;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class NodeMgrTest {

    @BeforeAll
    public static void setUp() {
        UtFrameUtils.setUpForPersistTest();
    }

    @Test
    public void testCheckFeExistByIpOrFqdnException() {
        assertThrows(UnknownHostException.class, () -> {
            NodeMgr nodeMgr = new NodeMgr();
            nodeMgr.checkFeExistByIpOrFqdn("not-exist-host.com");
        });
    }

    @Test
    public void testCheckFeExistByIpOrFqdn() throws UnknownHostException {
        NodeMgr nodeMgr = new NodeMgr();
        nodeMgr.replayAddFrontend(new Frontend(FrontendNodeType.FOLLOWER, "node1", "localhost", 9010));
        Assertions.assertTrue(nodeMgr.checkFeExistByIpOrFqdn("localhost"));
        Assertions.assertTrue(nodeMgr.checkFeExistByIpOrFqdn("127.0.0.1"));
    }

    @Test
    public void testCheckFeExistByRpcPort() {
        NodeMgr nodeMgr = new NodeMgr();
        Frontend fe = new Frontend(FrontendNodeType.FOLLOWER, "node1", "10.0.0.3", 9010);
        fe.handleHbResponse(new FrontendHbResponse("node1", 9030, 9020, 1,
                System.currentTimeMillis(), System.currentTimeMillis(), "v1", 0.5f), true);
        nodeMgr.replayAddFrontend(fe);

        Assertions.assertTrue(nodeMgr.checkFeExistByRPCPort("10.0.0.3", 9020));
        Assertions.assertFalse(nodeMgr.checkFeExistByRPCPort("10.0.0.3", 9030));
        Assertions.assertFalse(nodeMgr.checkFeExistByRPCPort("10.0.0.2", 9020));
    }

    @Test
    public void testRemoveClusterIdAndRoleFile() throws Exception {
        NodeMgr nodeMgr = new NodeMgr();
        nodeMgr.initialize(null);
        File imageDir = new File("/tmp/starrocks_nodemgr_test_" + UUID.randomUUID());
        imageDir.deleteOnExit();

        if (!imageDir.exists() && !imageDir.mkdirs()) {
            return;
        }

        File metaDir = new File(imageDir, "image");
        if (!metaDir.mkdirs()) {
            return;
        }

        Config.meta_dir = imageDir.getAbsolutePath();
        Assertions.assertTrue(nodeMgr.isVersionAndRoleFilesNotExist());
        nodeMgr.getClusterIdAndRoleOnStartup();
        Assertions.assertFalse(nodeMgr.isVersionAndRoleFilesNotExist());
        nodeMgr.removeClusterIdAndRole();
        Assertions.assertTrue(nodeMgr.isVersionAndRoleFilesNotExist());
    }

    @Test
    public void testResetFrontends() throws Exception {
        FrontendNodeType role = FrontendNodeType.FOLLOWER;
        String nodeName = "node1";
        Pair<String, Integer> selfNode = Pair.create("192.168.3.5", 9010);
        NodeMgr leaderNodeMgr = new NodeMgr(role, nodeName, selfNode);
        leaderNodeMgr.resetFrontends();

        UtFrameUtils.PseudoJournalReplayer.replayJournalToEnd();

        List<Frontend> frontends = GlobalStateMgr.getCurrentState().getNodeMgr().getFrontends(FrontendNodeType.FOLLOWER);
        Assertions.assertEquals(1, frontends.size());
        Assertions.assertEquals(role, frontends.get(0).getRole());
        Assertions.assertEquals(nodeName, frontends.get(0).getNodeName());
        Assertions.assertEquals(selfNode.first, frontends.get(0).getHost());
        Assertions.assertEquals((int) selfNode.second, frontends.get(0).getEditLogPort());
    }
}
