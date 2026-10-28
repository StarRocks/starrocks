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
import com.starrocks.alter.reshard.PublishTabletsInfo;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.Config;
import com.starrocks.common.NoAliveBackendException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.DnsCache;
import com.starrocks.proto.AggregatePublishVersionRequest;
import com.starrocks.proto.PublishVersionRequest;
import com.starrocks.proto.PublishVersionResponse;
import com.starrocks.proto.StatusPB;
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
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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

    // The aggregator turns every ComputeNodePB into a brpc stub via
    // LakeServiceBrpcStubCache::get_stub(), which has to resolve the host before it can look up its
    // (EndPoint-keyed) cache. Shipping a hostname there therefore costs one uncached getaddrinfo per
    // sub-request per publish on the CN. Pin that FE sends the resolved IP instead.
    @Test
    public void testAggregatePublishSubRequestCarriesResolvedIp() throws Exception {
        ComputeNode node = new ComputeNode(1001L, "cn-0.starrocks-cn-search.svc.cluster.local", 9040);
        node.setBrpcPort(9050);

        PublishTabletsInfo tabletsInfo = new PublishTabletsInfo();
        tabletsInfo.addTabletId(101L);

        new MockUp<DnsCache>() {
            @Mock
            public String tryLookup(String hostname) {
                return "cn-0.starrocks-cn-search.svc.cluster.local".equals(hostname) ? "10.0.0.7" : hostname;
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
            public boolean isResourceAvailable(ComputeResource computeResource) {
                return true;
            }
        };

        new MockUp<Utils>() {
            @Mock
            public Map<ComputeNode, PublishTabletsInfo> processTablets(List<Tablet> tablets,
                                                                      ComputeResource computeResource,
                                                                      WarehouseManager warehouseManager,
                                                                      List<Long> rebuildPindexTabletIds,
                                                                      long baseVersion, long newVersion)
                    throws NoAliveBackendException {
                return Collections.singletonMap(node, tabletsInfo);
            }
        };

        AggregatePublishVersionRequest request = new AggregatePublishVersionRequest();
        Utils.createSubRequestForAggregatePublish(Lists.newArrayList(), Lists.newArrayList(new TxnInfoPB()),
                1L, 2L, null, WarehouseManager.DEFAULT_RESOURCE, request);

        Assertions.assertEquals(1, request.getComputeNodes().size());
        Assertions.assertEquals("10.0.0.7", request.getComputeNodes().get(0).getHost());
        Assertions.assertEquals(9050, (int) request.getComputeNodes().get(0).getBrpcPort());
        // The node id must still be the real id: FE matches PBs back to ComputeNode objects by id
        // when choosing an aggregator.
        Assertions.assertEquals(1001L, (long) request.getComputeNodes().get(0).getId());
    }

    // Both halves of the publish version timeout have to follow the config: PublishVersionRequest.timeoutMs
    // is the deadline the compute node applies to the publish task, and the brpc once-talk timeout is how
    // long FE waits for the answer. Raising only the former would make FE give up while the CN still
    // publishes, so each path is pinned on both.
    @Test
    public void testPublishVersionTimeoutFollowsConfig() throws Exception {
        ComputeNode node = new ComputeNode(1001L, "127.0.0.1", 9040);
        node.setBrpcPort(9050);
        PublishTabletsInfo tabletsInfo = new PublishTabletsInfo();
        tabletsInfo.addTabletId(101L);
        mockSingleNodePublish(node, tabletsInfo);

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
                    null, null, WarehouseManager.DEFAULT_RESOURCE, null);
        } finally {
            Config.lake_publish_version_timeout_ms = savedTimeoutMs;
        }

        Assertions.assertEquals(1, sentRequests.size());
        Assertions.assertEquals(12345L, (long) sentRequests.get(0).getTimeoutMs());
        Assertions.assertEquals(12345L, talkTimeoutAtSend.get());
    }

    @Test
    public void testAggregatePublishVersionTimeoutFollowsConfig() throws Exception {
        ComputeNode node = new ComputeNode(1002L, "127.0.0.1", 9040);
        node.setBrpcPort(9050);
        PublishTabletsInfo tabletsInfo = new PublishTabletsInfo();
        tabletsInfo.addTabletId(202L);
        mockSingleNodePublish(node, tabletsInfo);

        new MockUp<LakeAggregator>() {
            @Mock
            public ComputeNode chooseAggregatorNode(ComputeResource computeResource,
                                                    Collection<ComputeNode> candidateNodes) {
                return node;
            }
        };

        List<AggregatePublishVersionRequest> sentRequests = new ArrayList<>();
        AtomicLong talkTimeoutAtSend = new AtomicLong();
        new MockUp<LakeServiceWithMetrics>() {
            @Mock
            public Future<PublishVersionResponse> aggregatePublishVersion(AggregatePublishVersionRequest request) {
                sentRequests.add(request);
                talkTimeoutAtSend.set(TalkTimeoutController.getTalkTimeout());
                PublishVersionResponse response = new PublishVersionResponse();
                response.status = new StatusPB();
                response.status.statusCode = 0;
                return CompletableFuture.completedFuture(response);
            }
        };

        int savedTimeoutMs = Config.lake_publish_version_timeout_ms;
        Config.lake_publish_version_timeout_ms = 23456;
        try {
            Utils.aggregatePublishVersion(Lists.newArrayList(), Lists.newArrayList(new TxnInfoPB()), 1L, 2L,
                    null, null, WarehouseManager.DEFAULT_RESOURCE, null);
        } finally {
            Config.lake_publish_version_timeout_ms = savedTimeoutMs;
        }

        Assertions.assertEquals(1, sentRequests.size());
        Assertions.assertEquals(1, sentRequests.get(0).getPublishReqs().size());
        Assertions.assertEquals(23456L, (long) sentRequests.get(0).getPublishReqs().get(0).getTimeoutMs());
        Assertions.assertEquals(23456L, talkTimeoutAtSend.get());
    }

    private void mockSingleNodePublish(ComputeNode node, PublishTabletsInfo tabletsInfo) {
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
            public boolean isResourceAvailable(ComputeResource computeResource) {
                return true;
            }
        };

        new MockUp<Utils>() {
            @Mock
            public Map<ComputeNode, PublishTabletsInfo> processTablets(List<Tablet> tablets,
                                                                      ComputeResource computeResource,
                                                                      WarehouseManager warehouseManager,
                                                                      List<Long> rebuildPindexTabletIds,
                                                                      long baseVersion, long newVersion)
                    throws NoAliveBackendException {
                return Collections.singletonMap(node, tabletsInfo);
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
