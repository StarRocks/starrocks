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

package com.starrocks.planner;

import com.google.common.collect.Maps;
import com.staros.client.StarClient;
import com.staros.client.StarClientException;
import com.staros.proto.ShardInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TOlapTablePartition;
import com.starrocks.thrift.TOlapTablePartitionParam;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ScanNodeComputeScanRangeTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void setUp() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster(true, RunMode.SHARED_DATA);
        // There are two available backends, (10001, 10002)
        UtFrameUtils.addMockBackend(10002);
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        Assertions.assertEquals(2L, systemInfoService.getAvailableBackends().size());

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        connectContext.setQueryId(UUIDUtil.genUUID());
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE test.t1(k1 int, k2 int, k3 int)" +
                        " distributed by hash(k1) buckets 10 properties('replication_num' = '1');")
                // 4 partitions x 3 buckets = 12 tablets, so a per-partition regression in the
                // scan-range heap-safety check is visible as a call count of 4 instead of 1.
                .withTable("CREATE TABLE test.t_multi_partition(k1 date, k2 int)" +
                        " partition by range(k1) (" +
                        "   partition p1 values less than ('2024-01-01')," +
                        "   partition p2 values less than ('2024-02-01')," +
                        "   partition p3 values less than ('2024-03-01')," +
                        "   partition p4 values less than ('2024-04-01'))" +
                        " distributed by hash(k1) buckets 3 properties('replication_num' = '1');");
    }

    @Test
    public void testOlapScanNodeRetrieveTabletLocationPerPhysicalPartition() {
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "t1");
        Assertions.assertNotNull(table);
        Assertions.assertInstanceOf(OlapTable.class, table);
        desc.setTable(table);
        OlapTable olapTable = (OlapTable) table;
        OlapScanNode scanNode =
                new OlapScanNode(new PlanNodeId(1), desc, "OlapScanNode", olapTable.getBaseIndexMetaId());
        Assertions.assertEquals(1L, olapTable.getAllPartitionIds().size());
        long partitionId = olapTable.getAllPartitionIds().get(0);
        Partition partition = olapTable.getPartition(partitionId);
        PhysicalPartition physicalPartition = partition.getDefaultPhysicalPartition();
        MaterializedIndex selectedIndex = physicalPartition.getLatestIndex(olapTable.getBaseIndexMetaId());
        AtomicInteger invokeCounter = new AtomicInteger(0);

        new MockUp<StarClient>() {
            @Mock
            public List<ShardInfo> getShardInfo(Invocation invocation, String serviceId, List<Long> shardIds,
                                                long workerGroupId) throws
                    StarClientException {
                invokeCounter.incrementAndGet();
                return invocation.proceed(serviceId, shardIds, workerGroupId);
            }
        };

        Assertions.assertDoesNotThrow(() -> scanNode.addScanRangeLocations(partition, physicalPartition, selectedIndex,
                selectedIndex.getTablets(), List.of(), -1));
        Assertions.assertEquals(1, invokeCounter.get());
    }

    /**
     * Records how many scan ranges the observed node had already built each time the heap-safety
     * check ran, for scan nodes over {@code tableName} only -- the MockUp intercepts every
     * OlapScanNode in the JVM, so scan nodes over other tables would otherwise contaminate the
     * capture, possibly off-thread.
     */
    private static List<Integer> captureHeapSafetyCheckCalls(String tableName) {
        List<Integer> scanRangesAtCheckTime = Collections.synchronizedList(new ArrayList<>());
        new MockUp<OlapScanNode>() {
            @Mock
            public void checkIfScanRangeNumSafe(Invocation invocation, long scanRangeSize) {
                OlapScanNode node = invocation.getInvokedInstance();
                if (tableName.equals(node.getOlapTable().getName())) {
                    scanRangesAtCheckTime.add(node.getScanRangeLocations(0).size());
                }
                invocation.proceed(scanRangeSize);
            }
        };
        return scanRangesAtCheckTime;
    }

    /**
     * The scan-range heap-safety check walks every selected partition and sub-partition, so it must
     * run once per scan node. Guarding it with a method-local flag inside addScanRangeLocations(),
     * which runs once per physical partition, made planning quadratic in the number of physical
     * partitions. See #64158. The guard has to be an instance field.
     */
    @Test
    public void testHeapSafetyCheckRunsOncePerScanNodeNotPerAddScanRangeCall() {
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "t1");
        Assertions.assertInstanceOf(OlapTable.class, table);
        desc.setTable(table);
        OlapTable olapTable = (OlapTable) table;
        OlapScanNode scanNode =
                new OlapScanNode(new PlanNodeId(1), desc, "OlapScanNode", olapTable.getBaseIndexMetaId());
        long partitionId = olapTable.getAllPartitionIds().get(0);
        Partition partition = olapTable.getPartition(partitionId);
        PhysicalPartition physicalPartition = partition.getDefaultPhysicalPartition();
        MaterializedIndex selectedIndex = physicalPartition.getLatestIndex(olapTable.getBaseIndexMetaId());

        List<Integer> checkCalls = captureHeapSafetyCheckCalls("t1");

        // Twice, because the defect was a method-local flag that re-armed itself on every
        // addScanRangeLocations() invocation. With an instance field, the second call is a no-op.
        Assertions.assertDoesNotThrow(() -> scanNode.addScanRangeLocations(partition, physicalPartition, selectedIndex,
                selectedIndex.getTablets(), List.of(), -1));
        Assertions.assertDoesNotThrow(() -> scanNode.addScanRangeLocations(partition, physicalPartition, selectedIndex,
                selectedIndex.getTablets(), List.of(), -1));
        // A method-local guard re-arms per call and yields [0, 10] here instead.
        Assertions.assertEquals(1, checkCalls.size(),
                "the heap-safety check must run once per scan node, not once per addScanRangeLocations() call");
        Assertions.assertEquals(0, checkCalls.get(0),
                "the check must fire before the first scan range is appended, while the heap is still intact");
        // Anchor: both calls really did build scan ranges (10 buckets each), so the single capture
        // cannot be an artifact of addScanRangeLocations() doing nothing.
        Assertions.assertEquals(20, scanNode.getScanRangeLocations(0).size());
    }

    /**
     * Same contract driven through the real CBO planner, where addScanRangeLocations() is called once
     * per physical partition: four partitions must still yield exactly one check.
     */
    @Test
    public void testHeapSafetyCheckRunsOncePerScanNodeInCboPath() throws Exception {
        List<Integer> checkCalls = captureHeapSafetyCheckCalls("t_multi_partition");

        Pair<String, ExecPlan> plan =
                UtFrameUtils.getPlanAndFragment(connectContext, "select * from test.t_multi_partition");

        // A method-local guard re-arms per physical partition and yields [0, 3, 6, 9] here instead.
        Assertions.assertEquals(1, checkCalls.size(),
                "the heap-safety check must run once per scan node, not once per physical partition");
        Assertions.assertEquals(0, checkCalls.get(0),
                "the check must fire before the first scan range is appended, while the heap is still intact");
        // Anchor: all 4 partitions x 3 buckets really were scanned. Without this, a future change that
        // left only one physical partition selected would let the test pass while losing its ability
        // to detect a per-partition guard.
        OlapScanNode scanNode = (OlapScanNode) plan.second.getScanNodes().get(0);
        Assertions.assertEquals(12, scanNode.getScanRangeLocations(0).size());
    }

    @Test
    public void testMetaScanNodeRetrieveTabletLocationPerPhysicalPartition() {
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "t1");
        Assertions.assertNotNull(table);
        Assertions.assertInstanceOf(OlapTable.class, table);
        desc.setTable(table);
        OlapTable olapTable = (OlapTable) table;
        MetaScanNode metaScanNode = new MetaScanNode(new PlanNodeId(1), desc, olapTable, Maps.newHashMap(), List.of(),
                null, olapTable.getBaseIndexMetaId(), null);
        Assertions.assertEquals(1L, olapTable.getAllPartitionIds().size());
        AtomicInteger invokeCounter = new AtomicInteger(0);

        new MockUp<StarClient>() {
            @Mock
            public List<ShardInfo> getShardInfo(Invocation invocation, String serviceId, List<Long> shardIds,
                                                long workerGroupId) throws
                    StarClientException {
                invokeCounter.incrementAndGet();
                return invocation.proceed(serviceId, shardIds, workerGroupId);
            }
        };

        ComputeResource computeResource =
                GlobalStateMgr.getCurrentState().getWarehouseMgr().getBackgroundComputeResource();
        Assertions.assertDoesNotThrow(() -> metaScanNode.computeRangeLocations(computeResource));
        Assertions.assertEquals(1, invokeCounter.get());
    }

    @Test
    public void testOlapTableSinkCreateLocationBatchesStarClientCalls() {
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "t1");
        Assertions.assertNotNull(table);
        Assertions.assertInstanceOf(OlapTable.class, table);
        OlapTable olapTable = (OlapTable) table;
        Assertions.assertEquals(1L, olapTable.getAllPartitionIds().size());
        long partitionId = olapTable.getAllPartitionIds().get(0);
        Partition partition = olapTable.getPartition(partitionId);
        PhysicalPartition physicalPartition = partition.getDefaultPhysicalPartition();

        TOlapTablePartitionParam partitionParam = new TOlapTablePartitionParam();
        TOlapTablePartition tPartition = new TOlapTablePartition();
        tPartition.setId(physicalPartition.getId());
        partitionParam.addToPartitions(tPartition);

        AtomicInteger invokeCounter = new AtomicInteger(0);
        new MockUp<StarClient>() {
            @Mock
            public List<ShardInfo> getShardInfo(Invocation invocation, String serviceId, List<Long> shardIds,
                                                long workerGroupId) throws StarClientException {
                invokeCounter.incrementAndGet();
                return invocation.proceed(serviceId, shardIds, workerGroupId);
            }
        };

        ComputeResource computeResource =
                GlobalStateMgr.getCurrentState().getWarehouseMgr().getBackgroundComputeResource();
        Assertions.assertDoesNotThrow(() ->
                OlapTableSink.createLocation(olapTable, partitionParam, false, computeResource, null));
        // 10 tablets share a single batched StarClient.getShardInfo call.
        Assertions.assertEquals(1, invokeCounter.get());
    }
}
