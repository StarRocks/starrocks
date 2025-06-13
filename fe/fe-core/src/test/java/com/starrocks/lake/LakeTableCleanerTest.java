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
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.proto.DropTableRequest;
import com.starrocks.proto.DropTableResponse;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;

public class LakeTableCleanerTest {
    private final ShardInfo shardInfo;

    @Mocked
    private StarOSAgent starOSAgent;

    public LakeTableCleanerTest() {
        shardInfo = ShardInfo.newBuilder().setFilePath(FilePathInfo.newBuilder().setFullPath("oss://1/2")).build();
    }

    @Before
    public void setup() {
        UtFrameUtils.mockInitWarehouseEnv();

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
    }

    @Test
    public void test(@Mocked LakeTable table,
                     @Mocked PhysicalPartition partition,
                     @Mocked MaterializedIndex index,
                     @Mocked LakeTablet tablet,
                     @Mocked LakeService lakeService) throws StarClientException {
        LakeTableCleaner cleaner = new LakeTableCleaner(table);

        new MockUp<Utils>() {
            @Mock
            public ComputeNode chooseNode(ShardInfo info) {
                return new ComputeNode();
            }
        };

        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(TNetworkAddress address) {
                return lakeService;
            }
        };

        new Expectations() {
            {
                table.getAllPhysicalPartitions();
                result = Lists.newArrayList(partition);
                minTimes = 1;
                maxTimes = 1;

                partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL);
                result = Lists.newArrayList(index);
                minTimes = 1;
                maxTimes = 1;

                index.getTablets();
                result = Lists.newArrayList(tablet);
                minTimes = 1;
                maxTimes = 1;

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
                             @Mocked PhysicalPartition partition,
                             @Mocked MaterializedIndex index,
                             @Mocked LakeTablet tablet,
                             @Mocked LakeService lakeService) {
        LakeTableCleaner cleaner = new LakeTableCleaner(table);

        new Expectations() {
            {
                table.getAllPhysicalPartitions();
                result = Lists.newArrayList(partition);
                minTimes = 1;
                maxTimes = 1;

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
                                @Mocked PhysicalPartition partition,
                                @Mocked MaterializedIndex index,
                                @Mocked LakeTablet tablet,
                                @Mocked LakeService lakeService) throws StarClientException {
        LakeTableCleaner cleaner = new LakeTableCleaner(table);

        new MockUp<Utils>() {
            @Mock
            public ComputeNode chooseNode(ShardInfo info) {
                return null;
            }
        };

        new Expectations() {
            {
                table.getAllPhysicalPartitions();
                result = Lists.newArrayList(partition);
                minTimes = 1;
                maxTimes = 1;

                partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL);
                result = Lists.newArrayList(index);
                minTimes = 1;
                maxTimes = 1;

                index.getTablets();
                result = Lists.newArrayList(tablet);
                minTimes = 1;
                maxTimes = 1;
            }
        };

        Assert.assertFalse(cleaner.cleanTable());
    }

    @Test
    public void testGetShardInfoFailed(@Mocked LakeTable table,
                                       @Mocked PhysicalPartition partition,
                                       @Mocked MaterializedIndex index,
                                       @Mocked LakeTablet tablet,
                                       @Mocked LakeService lakeService) throws StarClientException {
        LakeTableCleaner cleaner = new LakeTableCleaner(table);

        new Expectations() {
            {
                table.getAllPhysicalPartitions();
                result = Lists.newArrayList(partition);
                minTimes = 1;
                maxTimes = 1;

                partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL);
                result = Lists.newArrayList(index);
                minTimes = 1;
                maxTimes = 1;

                index.getTablets();
                result = Lists.newArrayList(tablet);
                minTimes = 1;
                maxTimes = 1;
            }
        };

        Assert.assertFalse(cleaner.cleanTable());
    }

    @Test
    public void testRPCFailed(@Mocked LakeTable table,
                              @Mocked PhysicalPartition partition,
                              @Mocked MaterializedIndex index,
                              @Mocked LakeTablet tablet,
                              @Mocked LakeService lakeService) throws StarClientException {
        LakeTableCleaner cleaner = new LakeTableCleaner(table);

        new MockUp<Utils>() {
            @Mock
            public ComputeNode chooseNode(ShardInfo info) {
                return new ComputeNode();
            }
        };

        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(TNetworkAddress address) {
                return lakeService;
            }
        };

        new Expectations() {
            {
                table.getAllPhysicalPartitions();
                result = Lists.newArrayList(partition);
                minTimes = 1;
                maxTimes = 1;

                partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL);
                result = Lists.newArrayList(index);
                minTimes = 1;
                maxTimes = 1;

                index.getTablets();
                result = Lists.newArrayList(tablet);
                minTimes = 1;
                maxTimes = 1;

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
                                  @Mocked PhysicalPartition partition,
                                  @Mocked MaterializedIndex index,
                                  @Mocked LakeTablet tablet,
                                  @Mocked LakeService lakeService) throws StarClientException {
        LakeTableCleaner cleaner = new LakeTableCleaner(table);

        new Expectations() {
            {
                table.getAllPhysicalPartitions();
                result = Lists.newArrayList(partition);
                minTimes = 1;
                maxTimes = 1;

                partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL);
                result = Lists.newArrayList(index);
                minTimes = 1;
                maxTimes = 1;

                index.getTablets();
                result = Lists.newArrayList(tablet);
                minTimes = 1;
                maxTimes = 1;
            }
        };

        new MockUp<StarOSAgent>() {
            @Mock
            public ShardInfo getShardInfo(long shardId, long workerGroupId) throws StarClientException {
                throw new StarClientException(StatusCode.NOT_EXIST, "injected error");
            }
        };

        Assert.assertTrue(cleaner.cleanTable());
    }
}
