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
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
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
                System.currentTimeMillis(), System.currentTimeMillis(), "v1", 0.5f, 1, null), true);
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

    @Test
    public void testGetAllNodeHosts() {
        NodeMgr nodeMgr = new NodeMgr();

        // add BE and CN hosts
        nodeMgr.getClusterInfo().addBackend(new com.starrocks.system.Backend(1L, "be-host", 9050));
        nodeMgr.getClusterInfo().addComputeNode(new com.starrocks.system.ComputeNode(2L, "cn-host", 9050));

        // add FE hosts
        Frontend fe1 = new Frontend(FrontendNodeType.FOLLOWER, "fe-node-1", "fe1-host", 9010);
        fe1.handleHbResponse(new FrontendHbResponse("fe-node-1", 9030, 9020, 1,
                System.currentTimeMillis(), System.currentTimeMillis(), "v1", 0.5f, 4, "aa:bb"), true);
        nodeMgr.replayAddFrontend(fe1);

        Frontend fe2 = new Frontend(FrontendNodeType.OBSERVER, "fe-node-2", "fe2-host", 9011);
        fe2.handleHbResponse(new FrontendHbResponse("fe-node-2", 9031, 9021, 2,
                System.currentTimeMillis(), System.currentTimeMillis(), "v1", 0.5f, 2, "cc:dd"), true);
        nodeMgr.replayAddFrontend(fe2);

        java.util.Set<String> hosts = nodeMgr.getAllNodeHosts();
        Assertions.assertTrue(hosts.contains("be-host"));
        Assertions.assertTrue(hosts.contains("cn-host"));
        Assertions.assertTrue(hosts.contains("fe1-host"));
        Assertions.assertTrue(hosts.contains("fe2-host"));
        Assertions.assertEquals(4, hosts.size());
    }

    @Test
    public void testGetTotalCpuCores() {
        NodeMgr nodeMgr = new NodeMgr();

        // BE with 8 cores
        Backend be = new Backend(10L, "host1", 9050);
        be.setCpuCores(8);
        nodeMgr.getClusterInfo().addBackend(be);

        // CN with 16 cores
        ComputeNode cn = new ComputeNode(11L, "host2", 9050);
        cn.setCpuCores(16);
        nodeMgr.getClusterInfo().addComputeNode(cn);

        // FE with 4 cores
        Frontend fe1 = new Frontend(FrontendNodeType.FOLLOWER, "fe-node-3", "host3", 9010);
        fe1.handleHbResponse(new FrontendHbResponse("fe-node-3", 9030, 9020, 1,
                System.currentTimeMillis(), System.currentTimeMillis(), "v1", 0.5f, 4, "aa:aa"), true);
        nodeMgr.replayAddFrontend(fe1);

        // FE with 2 cores
        Frontend fe2 = new Frontend(FrontendNodeType.OBSERVER, "fe-node-4", "host4", 9011);
        fe2.handleHbResponse(new FrontendHbResponse("fe-node-4", 9031, 9021, 2,
                System.currentTimeMillis(), System.currentTimeMillis(), "v1", 0.5f, 2, "bb:bb"), true);
        nodeMgr.replayAddFrontend(fe2);

        Assertions.assertEquals(30L, nodeMgr.getTotalCpuCores());
    }

    @Test
    public void testGetAllFENodesMacAddress() {
        NodeMgr nodeMgr = new NodeMgr();

        Frontend fe1 = new Frontend(FrontendNodeType.FOLLOWER, "fe-node-5", "host5", 9010);
        fe1.handleHbResponse(new FrontendHbResponse("fe-node-5", 9030, 9020, 1,
                System.currentTimeMillis(), System.currentTimeMillis(), "v1", 0.5f, 4, "11:22:33:44:55:66"), true);
        nodeMgr.replayAddFrontend(fe1);

        Frontend fe2 = new Frontend(FrontendNodeType.OBSERVER, "fe-node-6", "host6", 9011);
        fe2.handleHbResponse(new FrontendHbResponse("fe-node-6", 9031, 9021, 2,
                System.currentTimeMillis(), System.currentTimeMillis(), "v1", 0.5f, 2, "aa:bb:cc:dd:ee:ff"), true);
        nodeMgr.replayAddFrontend(fe2);

        Frontend fe3 = new Frontend(FrontendNodeType.LEADER, "fe-node-7", "host7", 9012);
        fe3.handleHbResponse(new FrontendHbResponse("fe-node-7", 9032, 9022, 3,
                System.currentTimeMillis(), System.currentTimeMillis(), "v1", 0.5f, 1, null), true);
        nodeMgr.replayAddFrontend(fe3);

        java.util.Set<String> macs = nodeMgr.getAllFENodesMacAddress();
        Assertions.assertTrue(macs.contains("11:22:33:44:55:66"));
        Assertions.assertTrue(macs.contains("aa:bb:cc:dd:ee:ff"));
        Assertions.assertEquals(2, macs.size());
    }
}
