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
import com.starrocks.alter.reshard.SplittingTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
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
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.NodeSelector;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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

    private static TxnInfoPB txn(long id, boolean unshare) {
        TxnInfoPB info = new TxnInfoPB();
        info.txnId = id;
        info.unshareCompaction = unshare;
        return info;
    }

    private static PublishVersionRequest req(TxnInfoPB... txnInfos) {
        PublishVersionRequest request = new PublishVersionRequest();
        request.setTxnInfos(List.of(txnInfos));
        return request;
    }

    /**
     * The UNSHARE publish retires a split's parent view, so it must not be handed parent metadata to
     * build. aggregatePublishWithCarryForward fills one request from two batches that share a single
     * parentTabletPublishInfos list, and the carry-forward batch's synthetic TXN_EMPTY infos do not
     * repeat the marker -- so the answer has to come from the whole request, not one batch.
     */
    @Test
    public void testUnshareMarkerIsReadAcrossEveryBatchInTheRequest() {
        TxnInfoPB unshare = txn(1001L, true);
        TxnInfoPB ordinary = txn(1002L, false);
        // A carry-forward batch as PublishVersionDaemon builds it: no marker of its own.
        TxnInfoPB carryForward = txn(-1L, false);

        Assertions.assertFalse(Utils.publishesUnshareCompaction(List.of(ordinary), List.of(req(ordinary))),
                "an ordinary publish still gets its parent metadata");
        Assertions.assertTrue(Utils.publishesUnshareCompaction(List.of(unshare), List.of(req(unshare))),
                "the batch carrying the marker is an unshare publish");
        Assertions.assertTrue(
                Utils.publishesUnshareCompaction(List.of(carryForward), List.of(req(carryForward), req(unshare))),
                "the carry-forward batch must not re-attach the parent view the first batch withheld");

        Assertions.assertFalse(Utils.publishesUnshareCompaction(null, null),
                "an empty request publishes no unshare compaction");
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

    // ---- prefer_shared_initial_metadata predicate ----------------------------------------
    //
    // This predicate decides whether the BE may skip probing a tablet's own version-1 metadata key
    // and read the partition-shared object instead. A false positive is not merely a wasted request:
    // where a shared object exists but belongs to a DIFFERENT index, the read succeeds and returns
    // the wrong schema, so every clause below is a correctness guard.

    private static OlapTable lakeTable(boolean fileBundling) {
        new MockUp<LakeTable>() {
            @Mock
            public boolean isCloudNativeTableOrMaterializedView() {
                return true;
            }

            @Mock
            public Boolean isFileBundling() {
                return fileBundling;
            }
        };
        return new LakeTable();
    }

    private static PhysicalPartition singleIndexPartition() {
        return new PhysicalPartition(100L, 10L, new MaterializedIndex(1000L));
    }

    @Test
    public void testSharedInitialMetadataOnBundledSingleIndexPartition() {
        Assertions.assertTrue(Utils.preferSharedInitialMetadata(lakeTable(true), singleIndexPartition(),
                PhysicalPartition.PARTITION_INIT_VERSION));
    }

    @Test
    public void testSharedInitialMetadataRequiresFileBundling() {
        Assertions.assertFalse(Utils.preferSharedInitialMetadata(lakeTable(false), singleIndexPartition(),
                PhysicalPartition.PARTITION_INIT_VERSION),
                "only file_bundling makes DDL write the shared version-1 object");
    }

    @Test
    public void testSharedInitialMetadataOnlyAtVersionOne() {
        Assertions.assertFalse(Utils.preferSharedInitialMetadata(lakeTable(true), singleIndexPartition(), 2L),
                "only version 1 is ever shared; later versions are per-tablet or bundled");
    }

    @Test
    public void testSharedInitialMetadataNotBeforeMetadataSwitchVersion() {
        PhysicalPartition partition = singleIndexPartition();
        // The partition predates the switch to bundling, so its version 1 is per-tablet even though
        // the table is bundling now.
        partition.setMetadataSwitchVersion(5L);
        Assertions.assertFalse(Utils.preferSharedInitialMetadata(lakeTable(true), partition,
                PhysicalPartition.PARTITION_INIT_VERSION));
    }

    /**
     * The regression guard. A rollup / schema-change shadow index keeps its own per-tablet version-1
     * metadata, and both alter jobs publish those tablets with base_version hardcoded to 1 and
     * enable_aggregate_publish set. Counting over ALL rather than VISIBLE is what keeps them out: a
     * shadow index is invisible to VISIBLE exactly while its tablets are reading version 1, so a
     * VISIBLE-based implementation would pass every other case here and hand the shadow tablets the
     * base index's metadata.
     */
    @Test
    public void testSharedInitialMetadataExcludesPartitionWithShadowIndex() {
        PhysicalPartition partition = singleIndexPartition();
        partition.createRollupIndex(
                new MaterializedIndex(2000L, 2000L, MaterializedIndex.IndexState.SHADOW, 0L));

        Assertions.assertEquals(1, partition.getLatestMaterializedIndices(
                MaterializedIndex.IndexExtState.VISIBLE).size(), "the shadow index is invisible to VISIBLE");
        Assertions.assertEquals(2, partition.getLatestMaterializedIndices(
                MaterializedIndex.IndexExtState.ALL).size());
        Assertions.assertFalse(Utils.preferSharedInitialMetadata(lakeTable(true), partition,
                PhysicalPartition.PARTITION_INIT_VERSION),
                "a shadow index in the same storage path must disable the hint");
    }

    @Test
    public void testSharedInitialMetadataExcludesPartitionWithRollupIndex() {
        PhysicalPartition partition = singleIndexPartition();
        partition.createRollupIndex(new MaterializedIndex(3000L, 3000L, MaterializedIndex.IndexState.NORMAL, 0L));

        Assertions.assertFalse(Utils.preferSharedInitialMetadata(lakeTable(true), partition,
                PhysicalPartition.PARTITION_INIT_VERSION),
                "DDL never writes the shared object for a multi-index partition");
    }

    @Test
    public void testSharedInitialMetadataNullSafe() {
        Assertions.assertFalse(Utils.preferSharedInitialMetadata(null, singleIndexPartition(), 1L));
        Assertions.assertFalse(Utils.preferSharedInitialMetadata(lakeTable(true), null, 1L));
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
                    null, null, WarehouseManager.DEFAULT_RESOURCE, null, null);
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
                    null, null, null, WarehouseManager.DEFAULT_RESOURCE, null, null);
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

    // Two nodes: node1 owns tablets 1 and 2 and publishes them; node2 owns 3 and 4, publishes 4 and
    // turns 3 down with the given status (it is still applying an earlier request for it).
    private void mockTwoNodePublish(ComputeNode node1, ComputeNode node2, int failedStatusCode,
                                    boolean node2Resharding) {
        PublishTabletsInfo node1Tablets = new PublishTabletsInfo();
        node1Tablets.addTabletId(1L);
        node1Tablets.addTabletId(2L);
        PublishTabletsInfo node2Tablets = new PublishTabletsInfo();
        node2Tablets.addTabletId(3L);
        node2Tablets.addTabletId(4L);
        if (node2Resharding) {
            // A split op registered on node2 besides its plain tablets.
            SplittingTablet splitting = new SplittingTablet(3L, Lists.newArrayList(30L, 31L));
            node2Tablets.addReshardingTablet(splitting);
        }
        Map<ComputeNode, PublishTabletsInfo> assignment = new HashMap<>();
        assignment.put(node1, node1Tablets);
        assignment.put(node2, node2Tablets);

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
                                                                      long baseVersion, long newVersion) {
                return assignment;
            }
        };
        new MockUp<LakeServiceWithMetrics>() {
            @Mock
            public Future<PublishVersionResponse> publishVersion(PublishVersionRequest request) {
                PublishVersionResponse response = new PublishVersionResponse();
                response.status = new StatusPB();
                response.compactionScores = new HashMap<>();
                if (request.tabletIds.contains(3L)) {
                    response.failedTablets = Lists.newArrayList(3L);
                    response.status.statusCode = failedStatusCode;
                    response.status.errorMsgs = Lists.newArrayList(
                            "The previous publish version task for tablet 3 has not finished");
                    response.compactionScores.put(4L, 4.0);
                } else {
                    response.status.statusCode = 0;
                    for (Long tabletId : request.tabletIds) {
                        response.compactionScores.put(tabletId, (double) tabletId);
                    }
                }
                return CompletableFuture.completedFuture(response);
            }
        };
        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(String host, int port) {
                return new LakeServiceWithMetrics(null);
            }
        };
    }

    @Test
    public void testPublishVersionBatchKeepsWhatOtherNodesPublished() throws Exception {
        ComputeNode node1 = new ComputeNode(1001L, "127.0.0.1", 9040);
        node1.setBrpcPort(9050);
        ComputeNode node2 = new ComputeNode(1002L, "127.0.0.2", 9040);
        node2.setBrpcPort(9050);
        mockTwoNodePublish(node1, node2, TStatusCode.RESOURCE_BUSY.getValue(), false);

        List<Tablet> tablets = Lists.newArrayList(new LakeTablet(1L), new LakeTablet(2L), new LakeTablet(3L),
                new LakeTablet(4L));
        Map<Long, Double> compactionScores = new HashMap<>();
        Map<ComputeNode, List<Long>> nodeToTablets = new HashMap<>();
        PublishVersionPartialFailureException ex = Assertions.assertThrows(
                PublishVersionPartialFailureException.class,
                () -> Utils.publishVersionBatch(tablets, Lists.newArrayList(new TxnInfoPB()), 1L, 2L,
                        compactionScores, nodeToTablets, WarehouseManager.DEFAULT_RESOURCE, null, null));

        // Only the busy tablet is reported, and it is reported as "still in progress".
        Assertions.assertEquals(Lists.newArrayList(3L), Lists.newArrayList(ex.getFailedTabletIds()));
        Assertions.assertTrue(ex.isInProgress());
        Assertions.assertTrue(ex.getMessage().contains("tablets [3]"), ex.getMessage());
        Assertions.assertTrue(ex.getMessage().contains("127.0.0.2"), ex.getMessage());

        // What the nodes did publish is kept for the caller: scores of 1, 2 (node1) and 4 (node2) ...
        Assertions.assertEquals(3, compactionScores.size());
        Assertions.assertEquals(4.0, compactionScores.get(4L));
        Assertions.assertFalse(compactionScores.containsKey(3L));
        // ... and the routing map names only published tablets, so their txn logs can be deleted later.
        Assertions.assertEquals(Lists.newArrayList(1L, 2L), nodeToTablets.get(node1));
        Assertions.assertEquals(Lists.newArrayList(4L), nodeToTablets.get(node2));
    }

    @Test
    public void testPublishVersionBatchRealFailureIsNotInProgress() {
        ComputeNode node1 = new ComputeNode(1003L, "127.0.0.1", 9040);
        node1.setBrpcPort(9050);
        ComputeNode node2 = new ComputeNode(1004L, "127.0.0.2", 9040);
        node2.setBrpcPort(9050);
        mockTwoNodePublish(node1, node2, TStatusCode.INTERNAL_ERROR.getValue(), false);

        List<Tablet> tablets = Lists.newArrayList(new LakeTablet(1L), new LakeTablet(3L));
        PublishVersionPartialFailureException ex = Assertions.assertThrows(
                PublishVersionPartialFailureException.class,
                () -> Utils.publishVersionBatch(tablets, Lists.newArrayList(new TxnInfoPB()), 1L, 2L,
                        new HashMap<>(), new HashMap<>(), WarehouseManager.DEFAULT_RESOURCE, null, null));
        Assertions.assertEquals(Lists.newArrayList(3L), Lists.newArrayList(ex.getFailedTabletIds()));
        Assertions.assertFalse(ex.isInProgress());
    }

    @Test
    public void testPublishVersionBatchReshardFailureStaysAllOrNothing() {
        ComputeNode node1 = new ComputeNode(1005L, "127.0.0.1", 9040);
        node1.setBrpcPort(9050);
        ComputeNode node2 = new ComputeNode(1006L, "127.0.0.2", 9040);
        node2.setBrpcPort(9050);
        mockTwoNodePublish(node1, node2, TStatusCode.RESOURCE_BUSY.getValue(), true);

        List<Tablet> tablets = Lists.newArrayList(new LakeTablet(1L), new LakeTablet(3L));
        // A node carrying a resharding op cannot be retried per tablet: the plain RpcException keeps
        // the caller on the old whole-partition retry.
        RpcException ex = Assertions.assertThrows(RpcException.class,
                () -> Utils.publishVersionBatch(tablets, Lists.newArrayList(new TxnInfoPB()), 1L, 2L,
                        new HashMap<>(), new HashMap<>(), WarehouseManager.DEFAULT_RESOURCE, null, null));
        Assertions.assertFalse(ex instanceof PublishVersionPartialFailureException);
    }

    @Test
    public void testIsPublishInProgressStatus() {
        Assertions.assertFalse(Utils.isPublishInProgressStatus(null));
        Assertions.assertTrue(Utils.isPublishInProgressStatus(TStatusCode.RESOURCE_BUSY.getValue()));
        Assertions.assertTrue(Utils.isPublishInProgressStatus(TStatusCode.TIMEOUT.getValue()));
        Assertions.assertTrue(Utils.isPublishInProgressStatus(TStatusCode.PUBLISH_TIMEOUT.getValue()));
        Assertions.assertFalse(Utils.isPublishInProgressStatus(TStatusCode.INTERNAL_ERROR.getValue()));
        Assertions.assertFalse(Utils.isPublishInProgressStatus(TStatusCode.OK.getValue()));
    }
}
