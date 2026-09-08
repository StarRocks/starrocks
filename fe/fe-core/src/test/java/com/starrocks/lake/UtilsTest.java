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

import com.google.common.collect.Lists;
import com.starrocks.alter.reshard.PublishTabletsInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.NoAliveBackendException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.DnsCache;
import com.starrocks.proto.AggregatePublishVersionRequest;
import com.starrocks.proto.TxnInfoPB;
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

import java.util.Collections;
import java.util.List;
import java.util.Map;

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
}
