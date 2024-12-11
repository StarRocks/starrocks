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

import com.staros.client.StarClientException;
import com.staros.proto.FilePathInfo;
import com.staros.proto.ShardInfo;
import com.staros.proto.StatusCode;
import com.starrocks.catalog.MaterializedIndex;
<<<<<<< HEAD
import com.starrocks.catalog.Partition;
=======
import com.starrocks.catalog.PhysicalPartition;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import com.starrocks.proto.DropTableRequest;
import com.starrocks.proto.DropTableResponse;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
<<<<<<< HEAD
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TNetworkAddress;
=======
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.warehouse.DefaultWarehouse;
import com.starrocks.warehouse.Warehouse;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.assertj.core.util.Lists;
import org.junit.Assert;
<<<<<<< HEAD
=======
import org.junit.Before;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import org.junit.Test;

import java.util.concurrent.CompletableFuture;

<<<<<<< HEAD

public class LakeTableCleanerTest {
    private final ShardInfo shardInfo;

    public LakeTableCleanerTest() {
        shardInfo = ShardInfo.newBuilder().setFilePath(FilePathInfo.newBuilder().setFullPath("oss://1/2")).build();
=======
public class LakeTableCleanerTest {
    private final ShardInfo shardInfo;

    @Mocked
    private StarOSAgent starOSAgent;

    @Mocked
    private WarehouseManager warehouseManager;

    public LakeTableCleanerTest() {
        shardInfo = ShardInfo.newBuilder().setFilePath(FilePathInfo.newBuilder().setFullPath("oss://1/2")).build();
        warehouseManager = new WarehouseManager();
        warehouseManager.initDefaultWarehouse();
    }

    @Before
    public void setup() {
        new MockUp<GlobalStateMgr>() {
            @Mock
            public StarOSAgent getStarOSAgent() {
                return starOSAgent;
            }
        };

        new MockUp<StarOSAgent>() {
            @Mock
            public ShardInfo getShardInfo(long shardId, long workerGroupId) throws StarClientException {
                return shardInfo;
            }
        };
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

    @Test
    public void test(@Mocked LakeTable table,
<<<<<<< HEAD
                     @Mocked Partition partition,
=======
                     @Mocked PhysicalPartition partition,
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                     @Mocked MaterializedIndex index,
                     @Mocked LakeTablet tablet,
                     @Mocked LakeService lakeService) throws StarClientException {
        LakeTableCleaner cleaner = new LakeTableCleaner(table);

        new MockUp<Utils>() {
            @Mock
<<<<<<< HEAD
            public ComputeNode chooseNode(LakeTablet tablet) {
=======
            public ComputeNode chooseNode(ShardInfo info) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                return new ComputeNode();
            }
        };

        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(TNetworkAddress address) {
                return lakeService;
            }
        };

<<<<<<< HEAD
        new Expectations() {
            {
                table.getAllPartitions();
=======
        new MockUp<WarehouseManager>() {
            @Mock
            public Warehouse getWarehouse(String warehouseName) {
                return new DefaultWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_ID, WarehouseManager.DEFAULT_WAREHOUSE_NAME);
            }

            @Mock
            public Warehouse getWarehouse(long warehouseId) {
                return new DefaultWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_ID, WarehouseManager.DEFAULT_WAREHOUSE_NAME);
            }
        };

        new Expectations() {
            {
                table.getAllPhysicalPartitions();
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                result = Lists.newArrayList(partition);
                minTimes = 1;
                maxTimes = 1;

<<<<<<< HEAD

=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL);
                result = Lists.newArrayList(index);
                minTimes = 1;
                maxTimes = 1;

                index.getTablets();
                result = Lists.newArrayList(tablet);
                minTimes = 1;
                maxTimes = 1;

<<<<<<< HEAD
                tablet.getShardInfo();
                result = shardInfo;
                minTimes = 1;
                maxTimes = 1;

=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                lakeService.dropTable((DropTableRequest) any);
                result = CompletableFuture.completedFuture(new DropTableResponse());
                minTimes = 1;
                maxTimes = 1;
            }
        };

        Assert.assertTrue(cleaner.cleanTable());
    }

    @Test
    public void testNoTablet(@Mocked LakeTable table,
<<<<<<< HEAD
                             @Mocked Partition partition,
=======
                             @Mocked PhysicalPartition partition,
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                             @Mocked MaterializedIndex index,
                             @Mocked LakeTablet tablet,
                             @Mocked LakeService lakeService) {
        LakeTableCleaner cleaner = new LakeTableCleaner(table);

        new Expectations() {
            {
<<<<<<< HEAD
                table.getAllPartitions();
=======
                table.getAllPhysicalPartitions();
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                result = Lists.newArrayList(partition);
                minTimes = 1;
                maxTimes = 1;

<<<<<<< HEAD

=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL);
                result = Lists.newArrayList(index);
                minTimes = 1;
                maxTimes = 1;

                index.getTablets();
                result = Lists.emptyList();
                minTimes = 1;
                maxTimes = 1;
            }
        };

        Assert.assertTrue(cleaner.cleanTable());
    }

    @Test
    public void testNoAliveNode(@Mocked LakeTable table,
<<<<<<< HEAD
                                @Mocked Partition partition,
=======
                                @Mocked PhysicalPartition partition,
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                                @Mocked MaterializedIndex index,
                                @Mocked LakeTablet tablet,
                                @Mocked LakeService lakeService) throws StarClientException {
        LakeTableCleaner cleaner = new LakeTableCleaner(table);

        new MockUp<Utils>() {
            @Mock
<<<<<<< HEAD
            public ComputeNode chooseNode(LakeTablet tablet) {
=======
            public ComputeNode chooseNode(ShardInfo info) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                return null;
            }
        };

<<<<<<< HEAD
        new Expectations() {
            {
                table.getAllPartitions();
=======
        new MockUp<WarehouseManager>() {
            @Mock
            public Warehouse getWarehouse(String warehouseName) {
                return new DefaultWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_ID, WarehouseManager.DEFAULT_WAREHOUSE_NAME);
            }

            @Mock
            public Warehouse getWarehouse(long warehouseId) {
                return new DefaultWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_ID, WarehouseManager.DEFAULT_WAREHOUSE_NAME);
            }
        };

        new Expectations() {
            {
                table.getAllPhysicalPartitions();
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                result = Lists.newArrayList(partition);
                minTimes = 1;
                maxTimes = 1;

<<<<<<< HEAD

=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL);
                result = Lists.newArrayList(index);
                minTimes = 1;
                maxTimes = 1;

                index.getTablets();
                result = Lists.newArrayList(tablet);
                minTimes = 1;
                maxTimes = 1;
<<<<<<< HEAD

                tablet.getShardInfo();
                result = shardInfo;
                minTimes = 1;
                maxTimes = 1;
=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            }
        };

        Assert.assertFalse(cleaner.cleanTable());
    }

    @Test
    public void testGetShardInfoFailed(@Mocked LakeTable table,
<<<<<<< HEAD
                                       @Mocked Partition partition,
=======
                                       @Mocked PhysicalPartition partition,
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                                       @Mocked MaterializedIndex index,
                                       @Mocked LakeTablet tablet,
                                       @Mocked LakeService lakeService) throws StarClientException {
        LakeTableCleaner cleaner = new LakeTableCleaner(table);

<<<<<<< HEAD
        new Expectations() {
            {
                table.getAllPartitions();
=======
        new MockUp<WarehouseManager>() {
            @Mock
            public Warehouse getWarehouse(String warehouseName) {
                return new DefaultWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_ID, WarehouseManager.DEFAULT_WAREHOUSE_NAME);
            }

            @Mock
            public Warehouse getWarehouse(long warehouseId) {
                return new DefaultWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_ID, WarehouseManager.DEFAULT_WAREHOUSE_NAME);
            }
        };

        new Expectations() {
            {
                table.getAllPhysicalPartitions();
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                result = Lists.newArrayList(partition);
                minTimes = 1;
                maxTimes = 1;

<<<<<<< HEAD

=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL);
                result = Lists.newArrayList(index);
                minTimes = 1;
                maxTimes = 1;

                index.getTablets();
                result = Lists.newArrayList(tablet);
                minTimes = 1;
                maxTimes = 1;
<<<<<<< HEAD

                tablet.getShardInfo();
                result = new StarClientException(StatusCode.IO, "injected error");
                minTimes = 1;
                maxTimes = 1;
=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            }
        };

        Assert.assertFalse(cleaner.cleanTable());
    }

    @Test
    public void testRPCFailed(@Mocked LakeTable table,
<<<<<<< HEAD
                              @Mocked Partition partition,
=======
                              @Mocked PhysicalPartition partition,
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                              @Mocked MaterializedIndex index,
                              @Mocked LakeTablet tablet,
                              @Mocked LakeService lakeService) throws StarClientException {
        LakeTableCleaner cleaner = new LakeTableCleaner(table);

        new MockUp<Utils>() {
            @Mock
<<<<<<< HEAD
            public ComputeNode chooseNode(LakeTablet tablet) {
=======
            public ComputeNode chooseNode(ShardInfo info) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                return new ComputeNode();
            }
        };

        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(TNetworkAddress address) {
                return lakeService;
            }
        };

<<<<<<< HEAD
        new Expectations() {
            {
                table.getAllPartitions();
=======
        new MockUp<WarehouseManager>() {
            @Mock
            public Warehouse getWarehouse(String warehouseName) {
                return new DefaultWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_ID, WarehouseManager.DEFAULT_WAREHOUSE_NAME);
            }

            @Mock
            public Warehouse getWarehouse(long warehouseId) {
                return new DefaultWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_ID, WarehouseManager.DEFAULT_WAREHOUSE_NAME);
            }
        };

        new Expectations() {
            {
                table.getAllPhysicalPartitions();
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                result = Lists.newArrayList(partition);
                minTimes = 1;
                maxTimes = 1;

<<<<<<< HEAD

=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL);
                result = Lists.newArrayList(index);
                minTimes = 1;
                maxTimes = 1;

                index.getTablets();
                result = Lists.newArrayList(tablet);
                minTimes = 1;
                maxTimes = 1;

<<<<<<< HEAD
                tablet.getShardInfo();
                result = shardInfo;
                minTimes = 1;
                maxTimes = 1;

=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                lakeService.dropTable((DropTableRequest) any);
                result = new RuntimeException("Injected RPC error");
                minTimes = 1;
                maxTimes = 1;
            }
        };

        Assert.assertFalse(cleaner.cleanTable());
    }

    @Test
    public void testShardNotFound(@Mocked LakeTable table,
<<<<<<< HEAD
                                  @Mocked Partition partition,
=======
                                  @Mocked PhysicalPartition partition,
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                                  @Mocked MaterializedIndex index,
                                  @Mocked LakeTablet tablet,
                                  @Mocked LakeService lakeService) throws StarClientException {
        LakeTableCleaner cleaner = new LakeTableCleaner(table);

<<<<<<< HEAD
        new Expectations() {
            {
                table.getAllPartitions();
=======
        new MockUp<WarehouseManager>() {
            @Mock
            public Warehouse getWarehouse(String warehouseName) {
                return new DefaultWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_ID, WarehouseManager.DEFAULT_WAREHOUSE_NAME);
            }

            @Mock
            public Warehouse getWarehouse(long warehouseId) {
                return new DefaultWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_ID, WarehouseManager.DEFAULT_WAREHOUSE_NAME);
            }
        };

        new Expectations() {
            {
                table.getAllPhysicalPartitions();
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                result = Lists.newArrayList(partition);
                minTimes = 1;
                maxTimes = 1;

<<<<<<< HEAD

=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL);
                result = Lists.newArrayList(index);
                minTimes = 1;
                maxTimes = 1;

                index.getTablets();
                result = Lists.newArrayList(tablet);
                minTimes = 1;
                maxTimes = 1;
<<<<<<< HEAD

                tablet.getShardInfo();
                result = new StarClientException(StatusCode.NOT_EXIST, "injected error");
                minTimes = 1;
                maxTimes = 1;
=======
            }
        };

        new MockUp<StarOSAgent>() {
            @Mock
            public ShardInfo getShardInfo(long shardId, long workerGroupId) throws StarClientException {
                throw new StarClientException(StatusCode.NOT_EXIST, "injected error");
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            }
        };

        Assert.assertTrue(cleaner.cleanTable());
    }
}
