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
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TCreatePartitionRequest;
import com.starrocks.thrift.TCreatePartitionResult;
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
import org.apache.thrift.TException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * The create-partition RPC and multi-node write.
 *
 * <p>A partition that automatic partitioning creates DURING a load never reaches the planner, so its
 * tablets get their node lists from here instead of from OlapTableSink.createLocation. These check
 * that the two agree: a load that spreads gets a runtime-created partition spread too, and a load
 * that does not is still handed exactly one node per tablet -- which it must be, because without
 * TOlapTableSink.enable_multi_node_write BE reads a tablet's node list as a replica set.
 */
public class FrontendServiceImplMultiNodeWriteTest {
    private static final long OWNER_NODE = 50001L;
    private static final List<Long> ALIVE_NODES = Lists.newArrayList(50001L, 50002L, 50003L);

    @Mocked
    ExecuteEnv exeEnv;

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.enable_strict_storage_medium_check = false;
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        UtFrameUtils.addMockComputeNode(50001);

        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);

        // One bucket on purpose: the shape a brand-new partition has, and the shape multi-node write
        // exists for.
        starRocksAssert.withDatabase("test_multi_node_create_partition")
                .useDatabase("test_multi_node_create_partition")
                .withTable("CREATE TABLE t_auto (\n" +
                        "    event_day DATE,\n" +
                        "    site_id INT,\n" +
                        "    pv BIGINT\n" +
                        ")\n" +
                        "DUPLICATE KEY(event_day, site_id)\n" +
                        "PARTITION BY date_trunc('day', event_day)\n" +
                        "DISTRIBUTED BY HASH(event_day) BUCKETS 1\n" +
                        "PROPERTIES (\"replication_num\" = \"1\");");
    }

    private List<TTabletLocation> createPartition(int writerWidth, String day, long txnId) throws TException {
        final TransactionState txnState = new TransactionState(1000L, Lists.newArrayList(100L),
                txnId, "label_" + txnId, null,
                TransactionState.LoadJobSourceType.BACKEND_STREAMING,
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, "127.0.0.1"),
                -1, 60000L);

        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(long dbId, long transactionId) {
                return txnState;
            }
        };
        new MockUp<WarehouseManager>() {
            @Mock
            public Long getAliveComputeNodeId(ComputeResource computeResource, long tabletId) {
                return OWNER_NODE;
            }

            @Mock
            public List<ComputeNode> getAliveComputeNodes(ComputeResource computeResource) {
                return ALIVE_NODES.stream().map(id -> {
                    ComputeNode node = new ComputeNode(id, "127.0.0.1", 9050);
                    node.setAlive(true);
                    return node;
                }).collect(Collectors.toList());
            }
        };

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test_multi_node_create_partition");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "t_auto");
        // Keyed by table: the create-partition path must answer with the width THIS table's plan
        // recorded, never one another table in the same transaction happened to record.
        txnState.setMultiNodeWriteWidth(table.getId(), writerWidth);

        TCreatePartitionRequest request = new TCreatePartitionRequest();
        request.setDb_id(db.getId());
        request.setTable_id(table.getId());
        request.setTxn_id(txnId);
        List<List<String>> partitionValues = Lists.newArrayList();
        partitionValues.add(Lists.newArrayList(day));
        request.setPartition_values(partitionValues);

        TCreatePartitionResult result = new FrontendServiceImpl(exeEnv).createPartition(request);
        Assertions.assertEquals(TStatusCode.OK, result.getStatus().getStatus_code(),
                () -> String.valueOf(result.getStatus().getError_msgs()));
        Assertions.assertFalse(result.getTablets().isEmpty());
        return result.getTablets();
    }

    /** Record a width against a DIFFERENT table id than the one the request names. */
    private List<TTabletLocation> createPartitionWithForeignWidth(String day, long txnId) throws TException {
        final TransactionState txnState = new TransactionState(1000L, Lists.newArrayList(100L),
                txnId, "label_" + txnId, null,
                TransactionState.LoadJobSourceType.BACKEND_STREAMING,
                new TransactionState.TxnCoordinator(TransactionState.TxnSourceType.FE, "127.0.0.1"),
                -1, 60000L);

        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(long dbId, long transactionId) {
                return txnState;
            }
        };
        new MockUp<WarehouseManager>() {
            @Mock
            public Long getAliveComputeNodeId(ComputeResource computeResource, long tabletId) {
                return OWNER_NODE;
            }

            @Mock
            public List<ComputeNode> getAliveComputeNodes(ComputeResource computeResource) {
                return ALIVE_NODES.stream().map(id -> {
                    ComputeNode node = new ComputeNode(id, "127.0.0.1", 9050);
                    node.setAlive(true);
                    return node;
                }).collect(Collectors.toList());
            }
        };

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test_multi_node_create_partition");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "t_auto");
        // Some other eligible table in the same transaction resolved a width of 3.
        txnState.setMultiNodeWriteWidth(table.getId() + 1, 3);

        TCreatePartitionRequest request = new TCreatePartitionRequest();
        request.setDb_id(db.getId());
        request.setTable_id(table.getId());
        request.setTxn_id(txnId);
        List<List<String>> partitionValues = Lists.newArrayList();
        partitionValues.add(Lists.newArrayList(day));
        request.setPartition_values(partitionValues);

        TCreatePartitionResult result = new FrontendServiceImpl(exeEnv).createPartition(request);
        Assertions.assertEquals(TStatusCode.OK, result.getStatus().getStatus_code(),
                () -> String.valueOf(result.getStatus().getError_msgs()));
        Assertions.assertFalse(result.getTablets().isEmpty());
        return result.getTablets();
    }

    private static final AtomicLong TXN_ID = new AtomicLong(770001L);

    @Test
    public void testRuntimePartitionSpreadsWhenThePlanRecordedAWidth() throws TException {
        List<TTabletLocation> tablets = createPartition(3, "2026-04-01", TXN_ID.incrementAndGet());
        for (TTabletLocation tablet : tablets) {
            Assertions.assertEquals(3, tablet.getNode_ids().size(),
                    "a one-tablet runtime partition must spread over the width the plan resolved");
            // The owner comes first so the node that later publishes the tablet also holds part of it.
            Assertions.assertEquals(OWNER_NODE, tablet.getNode_ids().get(0).longValue());
            Assertions.assertEquals(3, tablet.getNode_ids().stream().distinct().count());
            Assertions.assertTrue(ALIVE_NODES.containsAll(tablet.getNode_ids()));
        }
    }

    @Test
    public void testAnotherTablesWidthIsNotSpent() throws TException {
        // A multi-table Broker Load plans one sink per table on a single txn id, and only some of
        // those tables may be eligible. Recording the width against table A must not make the
        // create-partition RPC for table B spread: B's sink went out with
        // enable_multi_node_write false, and BE reads a multi-node list without that flag as a
        // REPLICA set -- every row written to every node in it.
        List<TTabletLocation> tablets = createPartitionWithForeignWidth("2026-04-03", TXN_ID.incrementAndGet());
        for (TTabletLocation tablet : tablets) {
            Assertions.assertEquals(Lists.newArrayList(OWNER_NODE), tablet.getNode_ids());
        }
    }

    @Test
    public void testRuntimePartitionKeepsOneNodeWithoutAWidth() throws TException {
        // No width recorded means the sink went out without enable_multi_node_write. Handing back a
        // multi-node list here would make BE write every row to every node in it.
        List<TTabletLocation> tablets = createPartition(1, "2026-04-02", TXN_ID.incrementAndGet());
        for (TTabletLocation tablet : tablets) {
            Assertions.assertEquals(Lists.newArrayList(OWNER_NODE), tablet.getNode_ids());
        }
    }
}
