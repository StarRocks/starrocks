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

package com.starrocks.persist.image;

import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.starrocks.catalog.BrokerMgr;
import com.starrocks.catalog.DiskInfo;
import com.starrocks.catalog.FsBroker;
import com.starrocks.common.Pair;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.persist.ModifyBrokerInfo;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.NodeMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.Frontend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * NodeMgr through the v3 image must load to the same state as through the v2 image. The initial
 * state is assembled the way a v2 image would deliver it (GSON deserialization), then written and
 * read back through both formats; the two loaded managers must be GSON-equivalent.
 */
public class NodeMgrImageV3Test {
    @TempDir
    Path tempDir;

    private static NodeMgr buildInitialNodeMgr() {
        Frontend leader = new Frontend(1, FrontendNodeType.LEADER, "fe1", "10.0.0.1", 9010);
        Frontend follower = new Frontend(FrontendNodeType.FOLLOWER, "fe2", "10.0.0.2", 9010); // fid 0, no mac
        Frontend observer = new Frontend(3, FrontendNodeType.OBSERVER, "fe3", "10.0.0.3", 9010);
        JsonObject leaderJson = GsonUtils.GSON.toJsonTree(leader).getAsJsonObject();
        leaderJson.addProperty("macAddress", "aa:bb:cc:dd:ee:ff");
        JsonObject frontends = new JsonObject();
        frontends.add("fe1", leaderJson);
        frontends.add("fe2", GsonUtils.GSON.toJsonTree(follower));
        frontends.add("fe3", GsonUtils.GSON.toJsonTree(observer));

        SystemInfoService systemInfo = new SystemInfoService();
        Backend backend1 = new Backend(10001, "10.0.0.11", 9050);
        backend1.setBePort(9060);
        backend1.setHttpPort(8040);
        backend1.setBrpcPort(8060);
        backend1.setAlive(true);
        backend1.setVersion("4.2.0-abc");
        backend1.setCpuCores(16);
        backend1.setMemLimitBytes(64L << 30);
        backend1.setLastUpdateMs(1_700_000_000_000L);
        backend1.setStarletPort(9070);
        DiskInfo disk1 = new DiskInfo("/data1");
        DiskInfo disk2 = new DiskInfo("/data2");
        disk2.setState(DiskInfo.DiskState.DISABLED);
        backend1.setDisks(ImmutableMap.of("/data1", disk1, "/data2", disk2));
        backend1.setLocation(ImmutableMap.of("rack", "r1", "zone", "z1"));
        Backend backend2 = new Backend(10002, "10.0.0.12", 9050);
        backend2.setDecommissioned(true);
        backend2.setWarehouseId(7);
        systemInfo.addBackend(backend1);
        systemInfo.addBackend(backend2);
        ComputeNode computeNode = new ComputeNode(20001, "10.0.0.21", 9050);
        computeNode.setAlive(true);
        computeNode.setWorkerGroupId(5);
        systemInfo.addComputeNode(computeNode);

        BrokerMgr brokerMgr = new BrokerMgr();
        FsBroker brokerA = new FsBroker("10.0.0.31", 8000);
        brokerA.isAlive = true;
        brokerA.lastStartTime = 1_700_000_000_000L;
        FsBroker brokerB = new FsBroker("10.0.0.32", 8000);
        brokerMgr.replayAddBrokers(new ModifyBrokerInfo("hdfs_broker", Arrays.asList(brokerA, brokerB)));
        brokerMgr.replayAddBrokers(new ModifyBrokerInfo("oss_broker", List.of(new FsBroker("10.0.0.33", 8001))));

        JsonObject root = new JsonObject();
        root.addProperty("r", 9020);
        root.addProperty("h", 8030);
        root.addProperty("ip", "10.0.0.1");
        root.add("f", frontends);
        JsonArray removed = new JsonArray();
        removed.add("fe_old_1");
        removed.add("fe_old_2");
        root.add("rf", removed);
        root.add("s", GsonUtils.GSON.toJsonTree(systemInfo));
        root.add("b", GsonUtils.GSON.toJsonTree(brokerMgr));
        return GsonUtils.GSON.fromJson(root, NodeMgr.class);
    }

    @Test
    public void testV3RoundTripMatchesV2() throws Exception {
        NodeMgr initial = buildInitialNodeMgr();

        // reference: the v2 image round trip
        UtFrameUtils.PseudoImage v2Image = new UtFrameUtils.PseudoImage();
        initial.save(v2Image.getImageWriter());
        NodeMgr v2Loaded = new NodeMgr();
        v2Loaded.load(v2Image.getMetaBlockReader());

        // the v3 image round trip
        Path file = tempDir.resolve("node_mgr.image");
        try (OutputStream os = Files.newOutputStream(file);
                ImageWriterV3 writer = new ImageWriterV3(os, 1L)) {
            initial.save(writer);
            writer.finish();
        }
        NodeMgr v3Loaded = new NodeMgr();
        try (ImageReaderV3 reader = new ImageReaderV3(file)) {
            Assertions.assertEquals(1, reader.sections().size());
            Assertions.assertEquals(SectionId.NODE_MGR.getValue(), reader.sections().get(0).getType());
            Assertions.assertEquals(0, reader.sections().get(0).getNumEntries());
            reader.load(List.of(Pair.create(SectionId.NODE_MGR, v3Loaded::load)));
        }

        // Both loads deliver the manager they were given. The load hands fid 0 the next free id, so that one field
        // is the only difference from the source; comparing the two loads to each other alone would not notice a
        // shared post-load step going wrong.
        Set<String> assignedOnLoad = Set.of("^\\$\\.f\\.fe2\\.fid$");
        GsonEquivalence.assertEquivalent(initial, v2Loaded, assignedOnLoad, Collections.emptySet());
        GsonEquivalence.assertEquivalent(initial, v3Loaded, assignedOnLoad, Collections.emptySet());
        GsonEquivalence.assertEquivalent(v2Loaded, v3Loaded);

        // the post-load steps ran the same way on both paths (leader ip/ports are covered by the GSON
        // comparison; their getters consult the HA protocol, which a unit test does not have)
        Assertions.assertEquals(Arrays.asList("fe_old_1", "fe_old_2"), v3Loaded.getRemovedFrontendNames());
        Assertions.assertEquals(v2Loaded.getHelperNodes(), v3Loaded.getHelperNodes());
        Assertions.assertEquals(1, v3Loaded.getHelperNodes().size(), "only the follower is a helper node");
        Assertions.assertEquals("fe2", v3Loaded.getFrontend(2).getNodeName(), "fid 0 gets the next free id on load");
        Assertions.assertEquals(v2Loaded.getFrontend(2).getFid(), v3Loaded.getFrontend(2).getFid());
        Assertions.assertEquals("aa:bb:cc:dd:ee:ff", v3Loaded.getFrontend(1).getMacAddress());
        Assertions.assertNull(v3Loaded.getFrontend(2).getMacAddress());

        Backend backend1 = v3Loaded.getClusterInfo().getBackend(10001);
        Assertions.assertTrue(backend1.isAlive());
        Assertions.assertEquals("4.2.0-abc", backend1.getVersion());
        Assertions.assertEquals(DiskInfo.DiskState.DISABLED, backend1.getDisks().get("/data2").getState());
        Assertions.assertEquals(ImmutableMap.of("rack", "r1", "zone", "z1"), backend1.getLocation());
        Assertions.assertTrue(v3Loaded.getClusterInfo().getBackend(10002).isDecommissioned());
        Assertions.assertEquals(5, v3Loaded.getClusterInfo().getComputeNode(20001).getWorkerGroupId());
        List<FsBroker> hdfsBrokers = v3Loaded.getBrokerMgr().getBrokerListMap().get("hdfs_broker");
        Assertions.assertEquals(2, hdfsBrokers.size());
        // the list order comes from a hash-keyed multimap, so look the broker up by address
        FsBroker aliveBroker = hdfsBrokers.stream().filter(b -> b.ip.equals("10.0.0.31")).findFirst().orElseThrow();
        Assertions.assertTrue(aliveBroker.isAlive);
        Assertions.assertEquals(1_700_000_000_000L, aliveBroker.lastStartTime);
        Assertions.assertEquals(1, v3Loaded.getBrokerMgr().getBrokerListMap().get("oss_broker").size());
    }

    @Test
    public void testEmptyNodeMgrRoundTrip() throws Exception {
        NodeMgr empty = new NodeMgr();
        Path file = tempDir.resolve("empty.image");
        try (OutputStream os = Files.newOutputStream(file);
                ImageWriterV3 writer = new ImageWriterV3(os, 1L)) {
            empty.save(writer);
            writer.finish();
        }
        NodeMgr loaded = new NodeMgr();
        try (ImageReaderV3 reader = new ImageReaderV3(file)) {
            reader.load(List.of(Pair.create(SectionId.NODE_MGR, loaded::load)));
        }
        GsonEquivalence.assertEquivalent(empty, loaded);
    }
}
