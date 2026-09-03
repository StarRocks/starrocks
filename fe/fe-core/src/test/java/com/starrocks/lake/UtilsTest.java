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

import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.common.StarRocksException;
import com.starrocks.epack.lake.StarOSAgentEpack;
import com.starrocks.proto.PublishVersionRequest;
import com.starrocks.proto.TxnInfoPB;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class UtilsTest {

    @Mocked
    GlobalStateMgr globalStateMgr;

    @Mocked
    StarOSAgentEpack starOSAgentEpack;

    NodeMgr nodeMgr;

    @Test
    public void testChooseBackend() {

        new MockUp<GlobalStateMgr>() {
            @Mock
            public StarOSAgent getStarOSAgent() {
                return starOSAgentEpack;
            }
        };

        new MockUp<StarOSAgentEpack>() {
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
