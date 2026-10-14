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


package com.starrocks.lake;

import com.baidu.jprotobuf.pbrpc.utils.TalkTimeoutController;
import com.google.common.collect.Lists;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.DnsCache;
import com.starrocks.proto.PublishVersionRequest;
import com.starrocks.proto.PublishVersionResponse;
import com.starrocks.proto.TxnInfoPB;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.rpc.LakeServiceWithMetrics;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.NodeSelector;
import com.starrocks.system.SystemInfoService;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

public class UtilsTest {

    @Mocked
    NodeMgr nodeMgr;

    @Test
    public void testChooseBackend() {

        new MockUp<GlobalStateMgr>() {
            @Mock
            public NodeMgr getNodeMgr() {
                return nodeMgr;
            }
        };

        new MockUp<NodeMgr>() {
            @Mock
            public SystemInfoService getClusterInfo() {
                SystemInfoService systemInfo = new SystemInfoService();
                return systemInfo;
            }
        };

        new MockUp<LakeTablet>() {
            @Mock
            public long getPrimaryComputeNodeId(long clusterId) throws StarRocksException {
                throw new StarRocksException("Failed to get primary backend");
            }
        };

        new MockUp<NodeSelector>() {
            @Mock
            public Long seqChooseBackendOrComputeId() throws StarRocksException {
                throw new StarRocksException("No backend or compute node alive.");
            }
        };
    }

    @Test
    public void testGetWarehouseIdByNodeId() {
        SystemInfoService systemInfo = new SystemInfoService();
        Backend b1 = new Backend(10001L, "192.168.0.1", 9050);
        b1.setBePort(9060);
        b1.setWarehouseId(10001L);
        Backend b2 = new Backend(10002L, "192.168.0.2", 9050);
        b2.setBePort(9060);
        b2.setWarehouseId(10002L);

        // add two backends to different warehouses
        systemInfo.addBackend(b1);
        systemInfo.addBackend(b2);

        // If the version of be is old, it may pass null.
        Assertions.assertEquals(WarehouseManager.DEFAULT_WAREHOUSE_ID,
                Utils.getWarehouseIdByNodeId(systemInfo, 0).orElse(WarehouseManager.DEFAULT_WAREHOUSE_ID).longValue());

        // pass a wrong tBackend
        Assertions.assertEquals(WarehouseManager.DEFAULT_WAREHOUSE_ID,
                Utils.getWarehouseIdByNodeId(systemInfo, 10003).orElse(WarehouseManager.DEFAULT_WAREHOUSE_ID).longValue());

        // pass a right tBackend
        Assertions.assertEquals(10001L, Utils.getWarehouseIdByNodeId(systemInfo, 10001).get().longValue());
        Assertions.assertEquals(10002L, Utils.getWarehouseIdByNodeId(systemInfo, 10002).get().longValue());
    }

    // Both halves of the publish version timeout have to follow the config: PublishVersionRequest.timeoutMs
    // is the deadline the compute node applies to the publish task, and the brpc once-talk timeout is how
    // long FE waits for the answer. Raising only the former would make FE give up while the node still
    // publishes, so the publish path is pinned on both.
    @Test
    public void testPublishVersionTimeoutFollowsConfig() throws Exception {
        ComputeNode node = new ComputeNode(1001L, "127.0.0.1", 9040);
        node.setBrpcPort(9050);
        // 3.5 groups tablets inside publishVersionBatch itself, so hand it a pre-filled node map and no tablets.
        Map<ComputeNode, List<Long>> nodeToTablets = new HashMap<>();
        nodeToTablets.put(node, Lists.newArrayList(101L));
        mockSingleNodePublish();

        List<PublishVersionRequest> sentRequests = new ArrayList<>();
        AtomicLong talkTimeoutAtSend = new AtomicLong();
        new MockUp<LakeServiceWithMetrics>() {
            @Mock
            public Future<PublishVersionResponse> publishVersion(PublishVersionRequest request) {
                sentRequests.add(request);
                talkTimeoutAtSend.set(TalkTimeoutController.getTalkTimeout());
                return CompletableFuture.completedFuture(new PublishVersionResponse());
            }
        };

        int savedTimeoutMs = Config.lake_publish_version_timeout_ms;
        Config.lake_publish_version_timeout_ms = 12345;
        try {
            Utils.publishVersionBatch(Lists.newArrayList(), Lists.newArrayList(new TxnInfoPB()), 1L, 2L,
                    null, nodeToTablets, WarehouseManager.DEFAULT_WAREHOUSE_ID, null);
        } finally {
            Config.lake_publish_version_timeout_ms = savedTimeoutMs;
        }

        Assertions.assertEquals(1, sentRequests.size());
        Assertions.assertEquals(12345L, (long) sentRequests.get(0).timeoutMs);
        Assertions.assertEquals(12345L, talkTimeoutAtSend.get());
    }

    private void mockSingleNodePublish() {
        new MockUp<DnsCache>() {
            @Mock
            public String tryLookup(String hostname) {
                return hostname;
            }
        };

        new MockUp<GlobalStateMgr>() {
            @Mock
            public WarehouseManager getWarehouseMgr() {
                return new WarehouseManager();
            }
        };

        new MockUp<WarehouseManager>() {
            @Mock
            public boolean warehouseExists(long warehouseId) {
                return true;
            }
        };

        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(String host, int port) {
                return new LakeServiceWithMetrics(null);
            }
        };
    }
}
