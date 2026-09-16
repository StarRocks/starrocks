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

package com.starrocks.service;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.thrift.TImmutablePartitionRequest;
import com.starrocks.thrift.TImmutablePartitionResult;
import com.starrocks.thrift.TNodeInfo;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TTabletLocation;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.TransactionState;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.CRAcquireContext;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Regression test for the shared-data CTAS/INSERT "Unknown node_id" failure.
 *
 * <p>{@code updateImmutablePartitionInternal} must build the tablet locations and the
 * {@code nodes_info} from the <em>same</em> compute resource. When a tablet location points to a
 * compute node in one worker group but {@code nodes_info} is built from a different worker group,
 * the BE cannot resolve the node id and throws {@code Unknown node_id}. This happens when a
 * warehouse has more than one CN group (worker group).
 */
public class FrontendServiceImplUpdateImmutablePartitionTest {

    // Compute node belonging to the worker group the load transaction is actually running on.
    private static final long LOAD_WORKER_GROUP_NODE_ID = 50001L;
    // Compute node belonging to the default worker group, i.e. a different CN group.
    private static final long DEFAULT_WORKER_GROUP_NODE_ID = 50002L;
    // Compute node belonging to the worker group a strategy-based acquire would hand out, i.e. yet
    // another CN group. Used to prove the handler must NOT reacquire its own resource.
    private static final long ACQUIRED_WORKER_GROUP_NODE_ID = 50003L;

    @Mocked
    private ExecuteEnv executeEnv;

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.enable_strict_storage_medium_check = false;
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        UtFrameUtils.addMockComputeNode((int) LOAD_WORKER_GROUP_NODE_ID);
        UtFrameUtils.addMockComputeNode((int) DEFAULT_WORKER_GROUP_NODE_ID);
        UtFrameUtils.addMockComputeNode((int) ACQUIRED_WORKER_GROUP_NODE_ID);
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);

        starRocksAssert.withDatabase("immutable_partition_test").useDatabase("immutable_partition_test")
                .withTable("CREATE TABLE site_access_random (\n" +
                        "    event_day DATETIME NOT NULL,\n" +
                        "    site_id INT DEFAULT '10',\n" +
                        "    pv BIGINT DEFAULT '0'\n" +
                        ")\n" +
                        "DUPLICATE KEY(event_day, site_id)\n" +
                        "DISTRIBUTED BY RANDOM\n" +
                        "PROPERTIES(\n" +
                        "    \"replication_num\" = \"1\"\n" +
                        ");");
    }

    @Test
    public void testTabletLocationNodesContainedInNodesInfo() throws Exception {
        Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("immutable_partition_test");
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(database.getFullName(), "site_access_random");
        Assertions.assertTrue(olapTable.isCloudNativeTable());

        // Grow the table to two physical sub-partitions using the default resource, so the measured
        // updateImmutablePartition call finds a mutable sub-partition to build tablets for and does
        // not have to create new shards itself.
        Partition partition = olapTable.getPartitions().iterator().next();
        GlobalStateMgr.getCurrentState().getLocalMetastore()
                .addSubPartitions(database, olapTable, partition, 1, WarehouseManager.DEFAULT_RESOURCE);

        // The compute resource the load transaction actually runs on. Its worker group differs from
        // the default worker group, mimicking a separate CN group.
        final ComputeResource loadComputeResource = workerGroupResource(1L);
        // A different resource that a strategy-based acquireComputeResource would hand out. Keeping
        // it distinct from the transaction resource means the test fails if the handler builds any
        // part of the response from a reacquired resource instead of the transaction's resource.
        final ComputeResource acquiredComputeResource = workerGroupResource(2L);

        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(long databaseId, long transactionId) {
                TransactionState transactionState = new TransactionState();
                transactionState.setComputeResource(loadComputeResource);
                return transactionState;
            }
        };

        new MockUp<WarehouseManager>() {
            @Mock
            public boolean isResourceAvailable(ComputeResource computeResource) {
                return true;
            }

            @Mock
            public ComputeResource acquireComputeResource(CRAcquireContext acquireContext) {
                return acquiredComputeResource;
            }

            // Tablet -> node assignment depends on which worker group asks for it.
            @Mock
            public Long getAliveComputeNodeId(ComputeResource computeResource, long tabletId) {
                return nodeIdOf(computeResource, loadComputeResource, acquiredComputeResource);
            }

            // nodes_info is built from the node ids of the given resource's worker group.
            @Mock
            public List<Long> getAllComputeNodeIds(ComputeResource computeResource) {
                return Lists.newArrayList(
                        nodeIdOf(computeResource, loadComputeResource, acquiredComputeResource));
            }
        };

        // Immute only the first physical partition; the second stays mutable and gets its tablets
        // (and thus locations) built into the response.
        long firstPhysicalPartitionId = olapTable.getPhysicalPartitions().iterator().next().getId();

        FrontendServiceImpl frontendService = new FrontendServiceImpl(executeEnv);
        TImmutablePartitionRequest request = new TImmutablePartitionRequest();
        request.setDb_id(database.getId());
        request.setTable_id(olapTable.getId());
        request.setTxn_id(1L);
        request.setPartition_ids(Lists.newArrayList(firstPhysicalPartitionId));

        TImmutablePartitionResult result = frontendService.updateImmutablePartition(request);

        Assertions.assertEquals(TStatusCode.OK, result.getStatus().getStatus_code(),
                () -> "status: " + result.getStatus());
        Assertions.assertFalse(result.getTablets().isEmpty(), "expected tablet locations in the response");

        Set<Long> nodeIdsInNodesInfo = result.getNodes().stream()
                .map(TNodeInfo::getId)
                .collect(Collectors.toSet());

        // nodes_info must be built from the transaction's compute resource, not the default resource
        // and not a freshly reacquired one. Asserting the exact node (rather than mere consistency)
        // also rejects a handler that reacquires a resource and uses it consistently for everything.
        Assertions.assertEquals(Set.of(LOAD_WORKER_GROUP_NODE_ID), nodeIdsInNodesInfo,
                "nodes_info must be built from the transaction's compute resource");

        for (TTabletLocation location : result.getTablets()) {
            for (long nodeId : location.getNode_ids()) {
                // Each tablet must be located on the transaction's compute resource ...
                Assertions.assertEquals(LOAD_WORKER_GROUP_NODE_ID, nodeId,
                        "tablet " + location.getTablet_id()
                                + " must be located on the transaction's compute resource");
                // ... and therefore appear in nodes_info, which is the invariant the BE enforces
                // (a missing node id makes the BE report "Unknown node_id").
                Assertions.assertTrue(nodeIdsInNodesInfo.contains(nodeId),
                        "tablet " + location.getTablet_id() + " location node " + nodeId
                                + " is not present in nodes_info " + nodeIdsInNodesInfo);
            }
        }
    }

    @Test
    public void testReturnsErrorWhenComputeResourceUnavailable() throws Exception {
        Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("immutable_partition_test");
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(database.getFullName(), "site_access_random");

        final ComputeResource loadComputeResource = workerGroupResource(1L);
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(long databaseId, long transactionId) {
                TransactionState transactionState = new TransactionState();
                transactionState.setComputeResource(loadComputeResource);
                return transactionState;
            }
        };
        new MockUp<WarehouseManager>() {
            @Mock
            public boolean isResourceAvailable(ComputeResource computeResource) {
                return false;
            }
        };

        FrontendServiceImpl frontendService = new FrontendServiceImpl(executeEnv);
        TImmutablePartitionRequest request = new TImmutablePartitionRequest();
        request.setDb_id(database.getId());
        request.setTable_id(olapTable.getId());
        request.setTxn_id(1L);
        request.setPartition_ids(Lists.newArrayList());

        TImmutablePartitionResult result = frontendService.updateImmutablePartition(request);

        // The handler must fail fast, before marking any partition immutable, when the transaction's
        // worker group has no available compute node.
        Assertions.assertEquals(TStatusCode.RUNTIME_ERROR, result.getStatus().getStatus_code());
        Assertions.assertTrue(result.getStatus().getError_msgs().get(0).contains("No available worker group"),
                () -> "unexpected error: " + result.getStatus().getError_msgs());
    }

    private static ComputeResource workerGroupResource(long workerGroupId) {
        return new ComputeResource() {
            @Override
            public long getWarehouseId() {
                return WarehouseManager.DEFAULT_WAREHOUSE_ID;
            }

            @Override
            public long getWorkerGroupId() {
                return workerGroupId;
            }
        };
    }

    // Map a compute resource to the single node id that lives in its worker group, so the test can
    // tell which resource the handler used to build each part of the response.
    private static long nodeIdOf(ComputeResource resource, ComputeResource loadComputeResource,
                                 ComputeResource acquiredComputeResource) {
        if (resource == loadComputeResource) {
            return LOAD_WORKER_GROUP_NODE_ID;
        }
        if (resource == acquiredComputeResource) {
            return ACQUIRED_WORKER_GROUP_NODE_ID;
        }
        return DEFAULT_WORKER_GROUP_NODE_ID;
    }
}
