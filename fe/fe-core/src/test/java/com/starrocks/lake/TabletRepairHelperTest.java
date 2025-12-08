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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.lake.TabletRepairHelper.PhysicalPartitionInfo;
import com.starrocks.proto.GetTabletMetadatasRequest;
import com.starrocks.proto.GetTabletMetadatasResponse;
import com.starrocks.proto.StatusPB;
import com.starrocks.proto.TabletMetadataPB;
import com.starrocks.proto.TabletMetadatas;
import com.starrocks.rpc.LakeServiceWithMetrics;
import com.starrocks.rpc.RpcException;
import com.starrocks.system.ComputeNode;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class TabletRepairHelperTest {
    private final long physicalPartitionId = 100L;
    private final long tabletId1 = 10001L;
    private final long tabletId2 = 10002L;
    private final long maxVersion = 8L;
    private final long minVersion = 3L;

    private ComputeNode node;
    private Map<ComputeNode, Set<Long>> nodeToTablets;
    private PhysicalPartitionInfo info;

    @BeforeEach
    public void beforeEach() {
        nodeToTablets = Maps.newHashMap();
        node = new ComputeNode(1L, "127.0.0.1", 9050);
        node.setBrpcPort(8060);
        nodeToTablets.put(node, Sets.newHashSet(tabletId1, tabletId2));

        info = new PhysicalPartitionInfo(physicalPartitionId, Lists.newArrayList(tabletId1, tabletId2),
                Sets.newHashSet(tabletId1, tabletId2), nodeToTablets, maxVersion, minVersion);
    }

    @Test
    public void testGetTabletMetadatas() {
        new MockUp<LakeServiceWithMetrics>() {
            @Mock
            public Future<GetTabletMetadatasResponse> getTabletMetadatas(GetTabletMetadatasRequest request) {
                GetTabletMetadatasResponse response = new GetTabletMetadatasResponse();
                response.status = new StatusPB();
                response.status.statusCode = 0;

                // tablet1 with 2 versions metadata
                TabletMetadatas tm1 = new TabletMetadatas();
                tm1.tabletId = tabletId1;
                tm1.status = new StatusPB();
                tm1.status.statusCode = 0;
                tm1.versionMetadatas = Maps.newHashMap();

                TabletMetadataPB meta11 = new TabletMetadataPB();
                meta11.id = tabletId1;
                meta11.version = maxVersion;
                tm1.versionMetadatas.put(maxVersion, meta11);

                TabletMetadataPB meta12 = new TabletMetadataPB();
                meta12.id = tabletId1;
                meta12.version = minVersion;
                tm1.versionMetadatas.put(minVersion, meta12);

                // tablet2 metadata not found
                TabletMetadatas tm2 = new TabletMetadatas();
                tm2.tabletId = tabletId2;
                tm2.status = new StatusPB();
                tm2.status.statusCode = 31;
                tm2.versionMetadatas = Maps.newHashMap();

                response.tabletMetadatas = Lists.newArrayList(tm1, tm2);

                return CompletableFuture.completedFuture(response);
            }
        };

        Map<Long, Map<Long, TabletMetadataPB>> tabletVersionMetadatas = Deencapsulation.invoke(
                TabletRepairHelper.class, "getTabletMetadatas", info, maxVersion, minVersion);

        Assertions.assertEquals(1, tabletVersionMetadatas.size());
        Assertions.assertTrue(tabletVersionMetadatas.containsKey(tabletId1));
        Assertions.assertEquals(2, tabletVersionMetadatas.get(tabletId1).size());
        Assertions.assertEquals(maxVersion, tabletVersionMetadatas.get(tabletId1).get(maxVersion).version);
        Assertions.assertEquals(minVersion, tabletVersionMetadatas.get(tabletId1).get(minVersion).version);
        Assertions.assertFalse(tabletVersionMetadatas.containsKey(tabletId2));
    }

    @Test
    public void testGetTabletMetadatasRpcException() {
        new MockUp<LakeServiceWithMetrics>() {
            @Mock
            public Future<GetTabletMetadatasResponse> getTabletMetadatas(GetTabletMetadatasRequest request) throws RpcException {
                throw new RpcException("rpc exception");
            }
        };

        ExceptionChecker.expectThrowsWithMsg(RpcException.class, "rpc exception",
                () -> Deencapsulation.invoke(TabletRepairHelper.class, "getTabletMetadatas", info, maxVersion, minVersion));
    }

    @Test
    public void testGetTabletMetadatasRpcLevelStatusFail() {
        new MockUp<LakeServiceWithMetrics>() {
            @Mock
            public Future<GetTabletMetadatasResponse> getTabletMetadatas(GetTabletMetadatasRequest request) {
                GetTabletMetadatasResponse response = new GetTabletMetadatasResponse();
                response.status = new StatusPB();
                response.status.statusCode = 1;
                response.status.errorMsgs = Lists.newArrayList("missing tablet_ids");
                return CompletableFuture.completedFuture(response);
            }
        };

        ExceptionChecker.expectThrowsWithMsg(StarRocksException.class, "missing tablet_ids",
                () -> Deencapsulation.invoke(TabletRepairHelper.class, "getTabletMetadatas", info, maxVersion, minVersion));
    }

    @Test
    public void testGetTabletMetadatasPartialFail() {
        new MockUp<LakeServiceWithMetrics>() {
            @Mock
            public Future<GetTabletMetadatasResponse> getTabletMetadatas(GetTabletMetadatasRequest request) {
                GetTabletMetadatasResponse response = new GetTabletMetadatasResponse();
                response.status = new StatusPB();
                response.status.statusCode = 0;

                // tablet1 with 1 version metadata
                TabletMetadatas tm1 = new TabletMetadatas();
                tm1.tabletId = tabletId1;
                tm1.status = new StatusPB();
                tm1.status.statusCode = 0;
                tm1.versionMetadatas = Maps.newHashMap();

                TabletMetadataPB meta11 = new TabletMetadataPB();
                meta11.id = tabletId1;
                meta11.version = maxVersion;
                tm1.versionMetadatas.put(maxVersion, meta11);

                // tablet2 get metadata failed
                TabletMetadatas tm2 = new TabletMetadatas();
                tm2.tabletId = tabletId2;
                tm2.status = new StatusPB();
                tm2.status.statusCode = 1;
                tm2.status.errorMsgs = Lists.newArrayList("get tablet metadata failed");

                response.tabletMetadatas = Lists.newArrayList(tm1, tm2);

                return CompletableFuture.completedFuture(response);
            }
        };

        ExceptionChecker.expectThrowsWithMsg(StarRocksException.class, "get tablet metadata failed",
                () -> Deencapsulation.invoke(TabletRepairHelper.class, "getTabletMetadatas", info, maxVersion, minVersion));
    }
}
