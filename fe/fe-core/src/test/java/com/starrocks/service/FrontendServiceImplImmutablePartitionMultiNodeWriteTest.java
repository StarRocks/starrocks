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
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.lake.qe.scheduler.DefaultSharedDataWorkerProvider;
import com.starrocks.planner.OlapTableSink;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TImmutablePartitionRequest;
import com.starrocks.thrift.TImmutablePartitionResult;
import com.starrocks.thrift.TOlapTablePartition;
import com.starrocks.thrift.TOlapTablePartitionParam;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TTabletLocation;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.TransactionState;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * The immutable-partition RPC and multi-node write.
 *
 * <p>When automatic bucketing marks a sub-partition immutable during a load, FE adds a new one and
 * hands its tablets back over updateImmutablePartition. Like a partition created by automatic
 * partitioning, it is not in the plan, so its node lists come from here and must follow the width the
 * plan recorded. Every sink instance of the load asks for them separately, and key-hash routing needs
 * all of them to see the same node list for a tablet.
 */
public class FrontendServiceImplImmutablePartitionMultiNodeWriteTest {
    private static final String DB = "test_multi_node_immutable_partition";

    @Mocked
    ExecuteEnv exeEnv;

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    private static final AtomicLong TXN_ID = new AtomicLong(880001L);

    @BeforeAll
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.enable_strict_storage_medium_check = false;
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        // Every node id a test hands out must exist, or building nodes_info fails.
        for (int id = 50001; id <= 50005; id++) {
            UtFrameUtils.addMockComputeNode(id);
        }

        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);

        // RANDOM distribution is what gets automatic bucketing, and one bucket is the shape a
        // sub-partition added during the load has.
        starRocksAssert.withDatabase(DB).useDatabase(DB)
                .withTable("CREATE TABLE t_random (\n" +
                        "    event_day DATETIME NOT NULL,\n" +
                        "    site_id INT DEFAULT '10',\n" +
                        "    pv BIGINT DEFAULT '0'\n" +
                        ")\n" +
                        "DUPLICATE KEY(event_day, site_id)\n" +
                        "DISTRIBUTED BY RANDOM BUCKETS 1\n" +
                        "PROPERTIES (\"replication_num\" = \"1\");")
                .withTable("CREATE TABLE t_hash (\n" +
                        "    event_day DATETIME NOT NULL,\n" +
                        "    site_id INT DEFAULT '10',\n" +
                        "    pv BIGINT DEFAULT '0'\n" +
                        ")\n" +
                        "DUPLICATE KEY(event_day, site_id)\n" +
                        "DISTRIBUTED BY HASH(site_id) BUCKETS 1\n" +
                        "PROPERTIES (\"replication_num\" = \"1\");");
    }

    private static OlapTable table(String name) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB);
        return (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), name);
    }

    private static TransactionState newTxnState(long txnId) {
        return new TransactionState(1000L, Lists.newArrayList(100L),
                txnId, "label_" + txnId, null,
                TransactionState.LoadJobSourceType.BACKEND_STREAMING,
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, "127.0.0.1"),
                -1, 60000L);
    }

    // The nodes FE sees for this call: which one owns every tablet, and which are alive.
    private static void mockNodes(TransactionState txnState, long ownerNode, List<Long> aliveNodes) {
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(long dbId, long transactionId) {
                return txnState;
            }
        };
        new MockUp<WarehouseManager>() {
            @Mock
            public boolean isResourceAvailable(ComputeResource computeResource) {
                return true;
            }

            @Mock
            public List<ComputeNode> getAliveComputeNodes(ComputeResource computeResource) {
                return aliveNodes.stream().map(id -> {
                    ComputeNode node = new ComputeNode(id, "127.0.0.1", 9050);
                    node.setAlive(true);
                    return node;
                }).collect(Collectors.toList());
            }

            @Mock
            public List<Long> getAllComputeNodeIds(ComputeResource computeResource) {
                return aliveNodes;
            }

            @Mock
            public ComputeNode getComputeNodeAssignedToTablet(ComputeResource computeResource, long tabletId) {
                return new ComputeNode(ownerNode, "127.0.0.1", 9050);
            }
        };
    }

    // The one sub-partition still taking writes. Every test immutes it, so FE adds a new one and the
    // invariant holds for the next test.
    private static long onlyMutableSubPartition(OlapTable table) {
        Partition partition = table.getPartitions().iterator().next();
        List<PhysicalPartition> mutable = partition.getSubPartitions().stream()
                .filter(p -> !p.isImmutable()).collect(Collectors.toList());
        Assertions.assertEquals(1, mutable.size());
        return mutable.get(0).getId();
    }

    private TImmutablePartitionResult updateImmutablePartition(OlapTable table, long txnId, long physicalPartitionId)
            throws Exception {
        TImmutablePartitionRequest request = new TImmutablePartitionRequest();
        request.setDb_id(GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB).getId());
        request.setTable_id(table.getId());
        request.setTxn_id(txnId);
        request.setPartition_ids(Lists.newArrayList(physicalPartitionId));
        TImmutablePartitionResult result = new FrontendServiceImpl(exeEnv).updateImmutablePartition(request);
        Assertions.assertEquals(TStatusCode.OK, result.getStatus().getStatus_code(),
                () -> String.valueOf(result.getStatus().getError_msgs()));
        Assertions.assertFalse(result.getTablets().isEmpty());
        return result;
    }

    @Test
    public void testNewSubPartitionSpreadsWhenThePlanRecordedAWidth() throws Exception {
        OlapTable table = table("t_random");
        long txnId = TXN_ID.incrementAndGet();
        TransactionState txnState = newTxnState(txnId);
        txnState.setMultiNodeWriteWidth(table.getId(), 3);
        List<Long> alive = Lists.newArrayList(50001L, 50002L, 50003L);
        mockNodes(txnState, 50001L, alive);

        TImmutablePartitionResult result = updateImmutablePartition(table, txnId, onlyMutableSubPartition(table));
        for (TTabletLocation tablet : result.getTablets()) {
            Assertions.assertEquals(3, tablet.getNode_ids().size(),
                    "a one-tablet sub-partition added during the load must spread over the recorded width");
            // The owner comes first so the node that later publishes the tablet also holds part of it.
            Assertions.assertEquals(50001L, tablet.getNode_ids().get(0).longValue());
            Assertions.assertEquals(3, tablet.getNode_ids().stream().distinct().count());
            Assertions.assertTrue(alive.containsAll(tablet.getNode_ids()));
        }
    }

    @Test
    public void testNewSubPartitionKeepsOneNodeWithoutAWidth() throws Exception {
        // No width recorded means the sink went out without enable_multi_node_write, and BE would
        // read a multi-node list as a replica set -- every row written to every node in it.
        OlapTable table = table("t_random");
        long txnId = TXN_ID.incrementAndGet();
        TransactionState txnState = newTxnState(txnId);
        mockNodes(txnState, 50001L, Lists.newArrayList(50001L, 50002L, 50003L));

        TImmutablePartitionResult result = updateImmutablePartition(table, txnId, onlyMutableSubPartition(table));
        for (TTabletLocation tablet : result.getTablets()) {
            Assertions.assertEquals(Lists.newArrayList(50001L), tablet.getNode_ids());
        }
    }

    @Test
    public void testEverySinkInstanceGetsTheSameNodeList() throws Exception {
        // Two sink instances of one load each see the sub-partition fill up and each send the RPC.
        // The second arrives after the first created the replacement, and by then the owner and the
        // alive nodes have both changed. Answering from what FE sees at that moment would give the two
        // instances different node lists for the same tablet, and key-hash routing would send one
        // key's rows to two writers.
        OlapTable table = table("t_random");
        long txnId = TXN_ID.incrementAndGet();
        TransactionState txnState = newTxnState(txnId);
        txnState.setMultiNodeWriteWidth(table.getId(), 3);
        long filled = onlyMutableSubPartition(table);

        mockNodes(txnState, 50001L, Lists.newArrayList(50001L, 50002L, 50003L));
        TImmutablePartitionResult first = updateImmutablePartition(table, txnId, filled);

        mockNodes(txnState, 50004L, Lists.newArrayList(50002L, 50003L, 50004L, 50005L));
        TImmutablePartitionResult second = updateImmutablePartition(table, txnId, filled);

        Map<Long, List<Long>> firstNodes = first.getTablets().stream()
                .collect(Collectors.toMap(TTabletLocation::getTablet_id, TTabletLocation::getNode_ids));
        Map<Long, List<Long>> secondNodes = second.getTablets().stream()
                .collect(Collectors.toMap(TTabletLocation::getTablet_id, TTabletLocation::getNode_ids));
        Assertions.assertEquals(firstNodes, secondNodes);
        Assertions.assertTrue(firstNodes.values().stream().allMatch(nodes -> nodes.size() == 3), firstNodes::toString);
    }

    @Test
    public void testCreateLocationRecordsTheWidthForAnAutomaticBucketTable() throws Exception {
        // The immutable-partition path can only spread on a width the plan recorded, and the plan must
        // record one for a table whose load can reach that path even when it does not partition
        // automatically.
        // createLocation's closing liveness check does not know the mock nodes; it is not what this
        // test is about.
        new MockUp<DefaultSharedDataWorkerProvider>() {
            @Mock
            public boolean allowUsingBackupNode() {
                return false;
            }
        };
        OlapTable randomTable = table("t_random");
        Assertions.assertTrue(randomTable.getAutomaticBucketSize() > 0);
        TransactionState randomTxn = newTxnState(TXN_ID.incrementAndGet());
        mockNodes(randomTxn, 50001L, Lists.newArrayList(50001L, 50002L, 50003L));
        OlapTableSink.createLocation(randomTable, partitionParam(randomTable), false,
                WarehouseManager.DEFAULT_RESOURCE, randomTxn, 3);
        Assertions.assertEquals(3, randomTxn.getMultiNodeWriteWidth(randomTable.getId()));

        // Neither automatic partitioning nor automatic bucketing: no runtime path, so nothing to record,
        // and recording would turn on enable_multi_node_write for nothing.
        OlapTable hashTable = table("t_hash");
        Assertions.assertEquals(0L, hashTable.getAutomaticBucketSize().longValue());
        TransactionState hashTxn = newTxnState(TXN_ID.incrementAndGet());
        mockNodes(hashTxn, 50001L, Lists.newArrayList(50001L, 50002L, 50003L));
        OlapTableSink.createLocation(hashTable, partitionParam(hashTable), false,
                WarehouseManager.DEFAULT_RESOURCE, hashTxn, 3);
        Assertions.assertEquals(1, hashTxn.getMultiNodeWriteWidth(hashTable.getId()));
    }

    private static TOlapTablePartitionParam partitionParam(OlapTable table) {
        TOlapTablePartitionParam partitionParam = new TOlapTablePartitionParam();
        partitionParam.setEnable_automatic_partition(false);
        for (PhysicalPartition physicalPartition : table.getPhysicalPartitions()) {
            TOlapTablePartition tPartition = new TOlapTablePartition();
            tPartition.setId(physicalPartition.getId());
            partitionParam.addToPartitions(tPartition);
        }
        return partitionParam;
    }
}
