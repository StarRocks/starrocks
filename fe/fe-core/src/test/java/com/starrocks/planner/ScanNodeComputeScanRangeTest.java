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
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.analysis.TupleId;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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
                        " distributed by hash(k1) buckets 10 properties('replication_num' = '1');");
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
                new OlapScanNode(new PlanNodeId(1), desc, "OlapScanNode", olapTable.getBaseIndexId());
        Assertions.assertEquals(1L, olapTable.getAllPartitionIds().size());
        long partitionId = olapTable.getAllPartitionIds().get(0);
        Partition partition = olapTable.getPartition(partitionId);
        PhysicalPartition physicalPartition = partition.getDefaultPhysicalPartition();
        MaterializedIndex selectedIndex = physicalPartition.getIndex(olapTable.getBaseIndexId());
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
                selectedIndex.getTablets(), -1));
        Assertions.assertEquals(1, invokeCounter.get());
    }

    @Test
    public void testMetaScanNodeRetrieveTabletLocationPerPhysicalPartition() {
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "t1");
        Assertions.assertNotNull(table);
        Assertions.assertInstanceOf(OlapTable.class, table);
        desc.setTable(table);g
        OlapTable olapTable = (OlapTable) table;
        MetaScanNode metaScanNode = new MetaScanNode(new PlanNodeId(1), desc, olapTable, Maps.newHashMap(), List.of(),
                olapTable.getBaseIndexId(), null);
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
}
