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
import com.starrocks.proto.RepairTabletMetadataRequest;
import com.starrocks.proto.RepairTabletMetadataResponse;
import com.starrocks.proto.StatusPB;
import com.starrocks.proto.TabletMetadataPB;
import com.starrocks.proto.TabletMetadataRepairStatus;
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
    public void testGetTabletMetadatasResponseNull() {
        new MockUp<LakeServiceWithMetrics>() {
            @Mock
            public Future<GetTabletMetadatasResponse> getTabletMetadatas(GetTabletMetadatasRequest request) {
                return CompletableFuture.completedFuture(null);
            }
        };

        ExceptionChecker.expectThrowsWithMsg(StarRocksException.class, "response is null",
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

    private TabletMetadataPB createTabletMetadataPB(long tabletId, long version) {
        TabletMetadataPB metadata = new TabletMetadataPB();
        metadata.id = tabletId;
        metadata.version = version;
        return metadata;
    }

    @Test
    public void testFindValidTabletMetadata() {
        // case 1: enforceConsistentVersion = true with consistent metadata
        {
            Map<Long, Map<Long, TabletMetadataPB>> tabletVersionMetadatas = Maps.newHashMap();
            Map<Long, TabletMetadataPB> tablet1Versions = Maps.newHashMap();
            tablet1Versions.put(maxVersion - 1, createTabletMetadataPB(tabletId1, maxVersion - 1));
            tablet1Versions.put(minVersion, createTabletMetadataPB(tabletId1, minVersion));
            tabletVersionMetadatas.put(tabletId1, tablet1Versions);

            Map<Long, TabletMetadataPB> tablet2Versions = Maps.newHashMap();
            tablet2Versions.put(maxVersion - 1, createTabletMetadataPB(tabletId2, maxVersion - 1));
            tablet2Versions.put(minVersion, createTabletMetadataPB(tabletId2, minVersion));
            tabletVersionMetadatas.put(tabletId2, tablet2Versions);

            Map<Long, TabletMetadataPB> validMetadatas = Maps.newHashMap();
            Deencapsulation.invoke(TabletRepairHelper.class, "findValidTabletMetadata",
                    info, tabletVersionMetadatas, maxVersion, minVersion, true, validMetadatas);

            Assertions.assertEquals(2, validMetadatas.size());
            Assertions.assertTrue(validMetadatas.containsKey(tabletId1));
            Assertions.assertEquals(maxVersion - 1, validMetadatas.get(tabletId1).version);
            Assertions.assertTrue(validMetadatas.containsKey(tabletId2));
            Assertions.assertEquals(maxVersion - 1, validMetadatas.get(tabletId2).version);

            ExceptionChecker.expectThrowsNoException(
                    () -> Deencapsulation.invoke(TabletRepairHelper.class, "checkOrCreateEmptyTabletMetadata",
                            info, validMetadatas, true, false));
        }

        // case 2: enforceConsistentVersion = true without consistent metadata
        {
            Map<Long, Map<Long, TabletMetadataPB>> tabletVersionMetadatas = Maps.newHashMap();
            Map<Long, TabletMetadataPB> tablet1Versions = Maps.newHashMap();
            tablet1Versions.put(maxVersion, createTabletMetadataPB(tabletId1, maxVersion));
            tabletVersionMetadatas.put(tabletId1, tablet1Versions);

            Map<Long, TabletMetadataPB> tablet2Versions = Maps.newHashMap();
            tablet2Versions.put(maxVersion - 1, createTabletMetadataPB(tabletId2, maxVersion - 1));
            tabletVersionMetadatas.put(tabletId2, tablet2Versions);

            Map<Long, TabletMetadataPB> validMetadatas = Maps.newHashMap();
            Deencapsulation.invoke(TabletRepairHelper.class, "findValidTabletMetadata",
                    info, tabletVersionMetadatas, maxVersion, minVersion, true, validMetadatas);

            Assertions.assertTrue(validMetadatas.isEmpty());

            ExceptionChecker.expectThrowsWithMsg(StarRocksException.class,
                    "no consistent valid tablet metadata version was found",
                    () -> Deencapsulation.invoke(TabletRepairHelper.class, "checkOrCreateEmptyTabletMetadata",
                            info, validMetadatas, true, false));
        }

        // case 3: enforceConsistentVersion = false with all latest metadata
        {
            Map<Long, Map<Long, TabletMetadataPB>> tabletVersionMetadatas = Maps.newHashMap();
            Map<Long, TabletMetadataPB> tablet1Versions = Maps.newHashMap();
            tablet1Versions.put(maxVersion, createTabletMetadataPB(tabletId1, maxVersion));
            tablet1Versions.put(minVersion, createTabletMetadataPB(tabletId1, minVersion));
            tabletVersionMetadatas.put(tabletId1, tablet1Versions);

            Map<Long, TabletMetadataPB> tablet2Versions = Maps.newHashMap();
            tablet2Versions.put(maxVersion - 1, createTabletMetadataPB(tabletId2, maxVersion - 1));
            tabletVersionMetadatas.put(tabletId2, tablet2Versions);

            Map<Long, TabletMetadataPB> validMetadatas = Maps.newHashMap();
            Deencapsulation.invoke(TabletRepairHelper.class, "findValidTabletMetadata",
                    info, tabletVersionMetadatas, maxVersion, minVersion, false, validMetadatas);

            Assertions.assertEquals(2, validMetadatas.size());
            Assertions.assertTrue(validMetadatas.containsKey(tabletId1));
            Assertions.assertEquals(maxVersion, validMetadatas.get(tabletId1).version);
            Assertions.assertTrue(validMetadatas.containsKey(tabletId2));
            Assertions.assertEquals(maxVersion - 1, validMetadatas.get(tabletId2).version);

            ExceptionChecker.expectThrowsNoException(
                    () -> Deencapsulation.invoke(TabletRepairHelper.class, "checkOrCreateEmptyTabletMetadata",
                            info, validMetadatas, false, false));
        }

        // case 4: enforceConsistentVersion = false with some missing metadata
        {
            Map<Long, Map<Long, TabletMetadataPB>> tabletVersionMetadatas = Maps.newHashMap();
            Map<Long, TabletMetadataPB> tablet1Versions = Maps.newHashMap();
            tablet1Versions.put(maxVersion, createTabletMetadataPB(tabletId1, maxVersion));
            tabletVersionMetadatas.put(tabletId1, tablet1Versions);

            // tabletId2 has no metadata

            Map<Long, TabletMetadataPB> validMetadatas = Maps.newHashMap();
            Deencapsulation.invoke(TabletRepairHelper.class, "findValidTabletMetadata", info, tabletVersionMetadatas,
                    maxVersion, minVersion, false, validMetadatas);

            Assertions.assertEquals(1, validMetadatas.size());
            Assertions.assertTrue(validMetadatas.containsKey(tabletId1));
            Assertions.assertEquals(maxVersion, validMetadatas.get(tabletId1).version);
            Assertions.assertFalse(validMetadatas.containsKey(tabletId2));

            ExceptionChecker.expectThrowsWithMsg(StarRocksException.class,
                    "tablet 10002 has no valid tablet metadatas",
                    () -> Deencapsulation.invoke(TabletRepairHelper.class, "checkOrCreateEmptyTabletMetadata",
                            info, validMetadatas, false, false));

            // recover with empty tablet metadata
            ExceptionChecker.expectThrowsNoException(
                    () -> Deencapsulation.invoke(TabletRepairHelper.class, "checkOrCreateEmptyTabletMetadata",
                            info, validMetadatas, false, true));
            Assertions.assertTrue(validMetadatas.containsKey(tabletId2));
            Assertions.assertEquals(0L, validMetadatas.get(tabletId2).version);
        }

        // case 5: enforceConsistentVersion = false with pre-existing valid metadata
        {
            Map<Long, Map<Long, TabletMetadataPB>> tabletVersionMetadatas = Maps.newHashMap();
            Map<Long, TabletMetadataPB> tablet2Versions = Maps.newHashMap();
            tablet2Versions.put(minVersion, createTabletMetadataPB(tabletId2, minVersion));
            tablet2Versions.put(minVersion + 1, createTabletMetadataPB(tabletId2, minVersion + 1));
            tabletVersionMetadatas.put(tabletId2, tablet2Versions);

            Map<Long, TabletMetadataPB> validMetadatas = Maps.newHashMap();
            validMetadatas.put(tabletId1, createTabletMetadataPB(tabletId1, maxVersion)); // Pre-existing

            Deencapsulation.invoke(TabletRepairHelper.class, "findValidTabletMetadata", info, tabletVersionMetadatas,
                    maxVersion, minVersion, false, validMetadatas);

            Assertions.assertEquals(2, validMetadatas.size());
            Assertions.assertTrue(validMetadatas.containsKey(tabletId1));
            Assertions.assertEquals(maxVersion, validMetadatas.get(tabletId1).version); // Should remain as pre-existing
            Assertions.assertTrue(validMetadatas.containsKey(tabletId2));
            Assertions.assertEquals(minVersion + 1, validMetadatas.get(tabletId2).version);

            ExceptionChecker.expectThrowsNoException(
                    () -> Deencapsulation.invoke(TabletRepairHelper.class, "checkOrCreateEmptyTabletMetadata",
                            info, validMetadatas, false, false));
        }
    }

    @Test
    public void testRepairTabletMetadata() {
        Map<Long, TabletMetadataPB> validMetadatas = Maps.newHashMap();
        validMetadatas.put(tabletId1, createTabletMetadataPB(tabletId1, maxVersion));
        validMetadatas.put(tabletId2, createTabletMetadataPB(tabletId2, maxVersion));

        new MockUp<LakeServiceWithMetrics>() {
            @Mock
            public Future<RepairTabletMetadataResponse> repairTabletMetadata(RepairTabletMetadataRequest request) {
                Assertions.assertFalse(request.enableFileBundling);
                Assertions.assertEquals(2, request.tabletMetadatas.size());

                RepairTabletMetadataResponse response = new RepairTabletMetadataResponse();
                response.status = new StatusPB();
                response.status.statusCode = 0;
                response.tabletRepairStatuses = Lists.newArrayList();

                for (TabletMetadataPB metadata : request.tabletMetadatas) {
                    TabletMetadataRepairStatus status = new TabletMetadataRepairStatus();
                    status.tabletId = metadata.id;
                    status.status = new StatusPB();
                    status.status.statusCode = 0;
                    response.tabletRepairStatuses.add(status);
                }

                return CompletableFuture.completedFuture(response);
            }
        };

        Map<Long, String> tabletErrors = Deencapsulation.invoke(TabletRepairHelper.class, "repairTabletMetadata",
                info, validMetadatas, false);
        Assertions.assertTrue(tabletErrors.isEmpty());
    }

    @Test
    public void testRepairTabletMetadataWithFileBundling() {
        Map<Long, TabletMetadataPB> validMetadatas = Maps.newHashMap();
        validMetadatas.put(tabletId1, createTabletMetadataPB(tabletId1, maxVersion));
        validMetadatas.put(tabletId2, createTabletMetadataPB(tabletId2, maxVersion));

        new MockUp<LakeServiceWithMetrics>() {
            @Mock
            public Future<RepairTabletMetadataResponse> repairTabletMetadata(RepairTabletMetadataRequest request) {
                Assertions.assertTrue(request.enableFileBundling);
                Assertions.assertEquals(2, request.tabletMetadatas.size());

                RepairTabletMetadataResponse response = new RepairTabletMetadataResponse();
                response.status = new StatusPB();
                response.status.statusCode = 0;
                response.tabletRepairStatuses = Lists.newArrayList();

                for (TabletMetadataPB metadata : request.tabletMetadatas) {
                    TabletMetadataRepairStatus status = new TabletMetadataRepairStatus();
                    status.tabletId = metadata.id;
                    status.status = new StatusPB();
                    status.status.statusCode = 0;
                    response.tabletRepairStatuses.add(status);
                }

                return CompletableFuture.completedFuture(response);
            }
        };

        Map<Long, String> tabletErrors = Deencapsulation.invoke(TabletRepairHelper.class, "repairTabletMetadata",
                info, validMetadatas, true);
        Assertions.assertTrue(tabletErrors.isEmpty());
    }

    @Test
    public void testRepairTabletMetadataRpcException() {
        Map<Long, TabletMetadataPB> validMetadatas = Maps.newHashMap();
        validMetadatas.put(tabletId1, createTabletMetadataPB(tabletId1, maxVersion));
        validMetadatas.put(tabletId2, createTabletMetadataPB(tabletId2, maxVersion));

        new MockUp<LakeServiceWithMetrics>() {
            @Mock
            public Future<RepairTabletMetadataResponse> repairTabletMetadata(RepairTabletMetadataRequest request)
                    throws RpcException {
                throw new RpcException("rpc exception");
            }
        };

        ExceptionChecker.expectThrowsWithMsg(RpcException.class, "rpc exception",
                () -> Deencapsulation.invoke(TabletRepairHelper.class, "repairTabletMetadata",
                        info, validMetadatas, false));
    }

    @Test
    public void testRepairTabletMetadataResponseNull() {
        Map<Long, TabletMetadataPB> validMetadatas = Maps.newHashMap();
        validMetadatas.put(tabletId1, createTabletMetadataPB(tabletId1, maxVersion));
        validMetadatas.put(tabletId2, createTabletMetadataPB(tabletId2, maxVersion));

        new MockUp<LakeServiceWithMetrics>() {
            @Mock
            public Future<RepairTabletMetadataResponse> repairTabletMetadata(RepairTabletMetadataRequest request) {
                return CompletableFuture.completedFuture(null);
            }
        };

        ExceptionChecker.expectThrowsWithMsg(StarRocksException.class, "response is null",
                () -> Deencapsulation.invoke(TabletRepairHelper.class, "repairTabletMetadata",
                        info, validMetadatas, false));
    }

    @Test
    public void testRepairTabletMetadataRpcLevelStatusFail() {
        Map<Long, TabletMetadataPB> validMetadatas = Maps.newHashMap();
        validMetadatas.put(tabletId1, createTabletMetadataPB(tabletId1, maxVersion));
        validMetadatas.put(tabletId2, createTabletMetadataPB(tabletId2, maxVersion));

        new MockUp<LakeServiceWithMetrics>() {
            @Mock
            public Future<RepairTabletMetadataResponse> repairTabletMetadata(RepairTabletMetadataRequest request) {
                RepairTabletMetadataResponse response = new RepairTabletMetadataResponse();
                response.status = new StatusPB();
                response.status.statusCode = 1;
                response.status.errorMsgs = Lists.newArrayList("missing tablet_metadatas");
                return CompletableFuture.completedFuture(response);
            }
        };

        ExceptionChecker.expectThrowsWithMsg(StarRocksException.class, "missing tablet_metadatas",
                () -> Deencapsulation.invoke(TabletRepairHelper.class, "repairTabletMetadata",
                        info, validMetadatas, false));
    }

    @Test
    public void testRepairTabletMetadataPartialFailure() {
        Map<Long, TabletMetadataPB> validMetadatas = Maps.newHashMap();
        validMetadatas.put(tabletId1, createTabletMetadataPB(tabletId1, maxVersion));
        validMetadatas.put(tabletId2, createTabletMetadataPB(tabletId2, maxVersion));

        new MockUp<LakeServiceWithMetrics>() {
            @Mock
            public Future<RepairTabletMetadataResponse> repairTabletMetadata(RepairTabletMetadataRequest request) {
                RepairTabletMetadataResponse response = new RepairTabletMetadataResponse();
                response.status = new StatusPB();
                response.status.statusCode = 0;
                response.tabletRepairStatuses = Lists.newArrayList();

                for (TabletMetadataPB metadata : request.tabletMetadatas) {
                    TabletMetadataRepairStatus status = new TabletMetadataRepairStatus();
                    status.tabletId = metadata.id;
                    status.status = new StatusPB();
                    if (metadata.id == tabletId2) {
                        status.status.statusCode = 1;
                        status.status.errorMsgs = Lists.newArrayList("repair failed");
                    } else {
                        status.status.statusCode = 0;
                    }
                    response.tabletRepairStatuses.add(status);
                }

                return CompletableFuture.completedFuture(response);
            }
        };

        Map<Long, String> tabletErrors = Deencapsulation.invoke(TabletRepairHelper.class, "repairTabletMetadata",
                info, validMetadatas, false);

        Assertions.assertEquals(1, tabletErrors.size());
        Assertions.assertTrue(tabletErrors.containsKey(tabletId2));
        Assertions.assertEquals("repair failed", tabletErrors.get(tabletId2));
    }

    @Test
    public void testRepairPhysicalPartition() {
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

                TabletMetadataPB meta11 = createTabletMetadataPB(tabletId1, maxVersion);
                tm1.versionMetadatas.put(maxVersion, meta11);

                TabletMetadataPB meta12 = createTabletMetadataPB(tabletId1, minVersion);
                tm1.versionMetadatas.put(minVersion, meta12);

                // tablet2 with 2 versions metadata
                TabletMetadatas tm2 = new TabletMetadatas();
                tm2.tabletId = tabletId2;
                tm2.status = new StatusPB();
                tm2.status.statusCode = 0;
                tm2.versionMetadatas = Maps.newHashMap();

                TabletMetadataPB meta21 = createTabletMetadataPB(tabletId2, maxVersion - 1);
                tm2.versionMetadatas.put(maxVersion - 1, meta21);

                TabletMetadataPB meta22 = createTabletMetadataPB(tabletId2, minVersion);
                tm2.versionMetadatas.put(minVersion, meta22);

                response.tabletMetadatas = Lists.newArrayList(tm1, tm2);

                return CompletableFuture.completedFuture(response);
            }

            @Mock
            public Future<RepairTabletMetadataResponse> repairTabletMetadata(RepairTabletMetadataRequest request) {
                Assertions.assertFalse(request.enableFileBundling);
                Assertions.assertFalse(request.writeBundlingFile);
                Assertions.assertEquals(2, request.tabletMetadatas.size());
                Assertions.assertEquals(maxVersion, request.tabletMetadatas.get(0).version);
                Assertions.assertEquals(maxVersion, request.tabletMetadatas.get(1).version);

                RepairTabletMetadataResponse response = new RepairTabletMetadataResponse();
                response.status = new StatusPB();
                response.status.statusCode = 0;
                response.tabletRepairStatuses = Lists.newArrayList();

                for (TabletMetadataPB metadata : request.tabletMetadatas) {
                    TabletMetadataRepairStatus status = new TabletMetadataRepairStatus();
                    status.tabletId = metadata.id;
                    status.status = new StatusPB();
                    status.status.statusCode = 0;
                    response.tabletRepairStatuses.add(status);
                }

                return CompletableFuture.completedFuture(response);
            }
        };

        // consistent version
        Map<Long, String> tabletErrors =
                Deencapsulation.invoke(TabletRepairHelper.class, "repairPhysicalPartition", info, true, false, false);
        Assertions.assertTrue(tabletErrors.isEmpty());

        // inconsistent version
        tabletErrors = Deencapsulation.invoke(TabletRepairHelper.class, "repairPhysicalPartition", info, false, false, false);
        Assertions.assertTrue(tabletErrors.isEmpty());
    }
}
