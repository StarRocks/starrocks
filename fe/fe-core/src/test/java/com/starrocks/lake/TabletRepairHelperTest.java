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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.Config;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.lake.TabletRepairHelper.PhysicalPartitionInfo;
import com.starrocks.proto.GetTabletMetadatasRequest;
import com.starrocks.proto.GetTabletMetadatasResponse;
import com.starrocks.proto.PersistentIndexSstableMetaPB;
import com.starrocks.proto.RepairTabletMetadataRequest;
import com.starrocks.proto.RepairTabletMetadataResponse;
import com.starrocks.proto.StatusPB;
import com.starrocks.proto.TabletMetadataEntry;
import com.starrocks.proto.TabletMetadataPB;
import com.starrocks.proto.TabletMetadataRepairStatus;
import com.starrocks.proto.TabletResult;
import com.starrocks.rpc.LakeServiceWithMetrics;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.AdminRepairTableStmt;
import com.starrocks.sql.ast.LakeTabletStatus;
import com.starrocks.sql.ast.PartitionRef;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.type.VarcharType;
import com.starrocks.warehouse.cngroup.ComputeResource;
import com.starrocks.warehouse.cngroup.WarehouseComputeResource;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class TabletRepairHelperTest {
    private final long dbId = 1L;
    private final long tableId = 2L;
    private final long indexId = 3L;
    private final long physicalPartitionId1 = 4L;
    private final long tabletId11 = 11L;
    private final long tabletId12 = 12L;
    private final long physicalPartitionId2 = 5L;
    private final long tabletId21 = 21L;
    private final long tabletId22 = 22L;
    private final long maxVersion = 8L;
    private final long minVersion = 3L;

    private Database db;
    private OlapTable table;

    private ComputeNode node;
    private Map<ComputeNode, Set<Long>> nodeToTablets;
    private PhysicalPartitionInfo info;

    @BeforeEach
    public void beforeEach() {
        nodeToTablets = Maps.newHashMap();
        node = new ComputeNode(1L, "127.0.0.1", 9050);
        node.setBrpcPort(8060);
        nodeToTablets.put(node, Sets.newHashSet(tabletId11, tabletId12));

        info = new PhysicalPartitionInfo(physicalPartitionId1, Lists.newArrayList(tabletId11, tabletId12),
                Sets.newHashSet(tabletId11, tabletId12), nodeToTablets, maxVersion, minVersion);

        // create table
        List<Column> cols = Lists.newArrayList(new Column("province", VarcharType.VARCHAR));
        PartitionInfo listPartitionInfo = new ListPartitionInfo(PartitionType.LIST, cols);
        table = new OlapTable(tableId, "table", cols, null, listPartitionInfo, null);
        table.getIndexNameToMetaId().put("index", indexId);

        MaterializedIndex index1 = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);
        TabletMeta tabletMeta1 = new TabletMeta(dbId, tableId, physicalPartitionId1, indexId, TStorageMedium.HDD);
        LakeTablet tablet11 = new LakeTablet(tabletId11);
        tablet11.setMinVersion(minVersion);
        index1.addTablet(tablet11, tabletMeta1);
        LakeTablet tablet12 = new LakeTablet(tabletId12);
        tablet12.setMinVersion(minVersion);
        index1.addTablet(tablet12, tabletMeta1);

        Partition partition1 =
                new Partition(physicalPartitionId1, physicalPartitionId1, "p1", index1, new HashDistributionInfo(2, cols));
        partition1.getDefaultPhysicalPartition().setVisibleVersion(maxVersion, 0L);
        table.addPartition(partition1);

        MaterializedIndex index2 = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);
        TabletMeta tabletMeta2 = new TabletMeta(dbId, tableId, physicalPartitionId2, indexId, TStorageMedium.HDD);
        LakeTablet tablet21 = new LakeTablet(tabletId21);
        tablet21.setMinVersion(minVersion);
        index2.addTablet(tablet21, tabletMeta2);
        LakeTablet tablet22 = new LakeTablet(tabletId22);
        tablet22.setMinVersion(minVersion + 1);
        index2.addTablet(tablet22, tabletMeta2);

        Partition partition2 =
                new Partition(physicalPartitionId2, physicalPartitionId2, "p2", index2, new HashDistributionInfo(2, cols));
        partition2.getDefaultPhysicalPartition().setVisibleVersion(maxVersion, 0L);
        table.addPartition(partition2);

        db = new Database(dbId, "db");
        db.registerTableUnlocked(table);
    }

    @Test
    public void testGetPhysicalPartitionIds() {
        // case 1: test no partition specified
        {
            List<Long> ids = Deencapsulation.invoke(TabletRepairHelper.class, "getPhysicalPartitionIds",
                    db, table, Lists.newArrayList());
            Assertions.assertEquals(2, ids.size());
            Assertions.assertEquals("[4, 5]", ids.toString());
        }

        // case 2: test partition specified
        {
            List<Long> ids = Deencapsulation.invoke(TabletRepairHelper.class, "getPhysicalPartitionIds",
                    db, table, Lists.newArrayList("p1"));

            Assertions.assertEquals(1, ids.size());
            Assertions.assertEquals("[4]", ids.toString());
        }
    }

    @Test
    public void testGetPhysicalPartitionInfo() {
        // mock warehouse manager
        new MockUp<WarehouseManager>() {
            @Mock
            public ComputeNode getComputeNodeAssignedToTablet(ComputeResource computeResource, long tabletId) {
                return node;
            }
        };

        // case 1: test partition 1 with enforceConsistentVersion = true
        {
            PhysicalPartitionInfo info = Deencapsulation.invoke(TabletRepairHelper.class, "getPhysicalPartitionInfo",
                    db, table, physicalPartitionId1, true, WarehouseComputeResource.DEFAULT);

            Assertions.assertEquals(physicalPartitionId1, info.physicalPartitionId());
            Assertions.assertEquals(2, info.allTablets().size());
            Assertions.assertEquals("[11, 12]", info.allTablets().toString());
            Assertions.assertEquals(2, info.unverifiedTablets().size());
            Assertions.assertEquals("[11, 12]", info.unverifiedTablets().toString());
            Assertions.assertTrue(info.nodeToTablets().containsKey(node));
            Assertions.assertEquals(maxVersion, info.maxVersion());
            Assertions.assertEquals(minVersion, info.minVersion());
        }

        // case 2: test partition 1 with enforceConsistentVersion = false
        {
            PhysicalPartitionInfo info = Deencapsulation.invoke(TabletRepairHelper.class, "getPhysicalPartitionInfo",
                    db, table, physicalPartitionId1, false, WarehouseComputeResource.DEFAULT);

            Assertions.assertEquals(physicalPartitionId1, info.physicalPartitionId());
            Assertions.assertEquals(2, info.allTablets().size());
            Assertions.assertEquals("[11, 12]", info.allTablets().toString());
            Assertions.assertEquals(2, info.unverifiedTablets().size());
            Assertions.assertEquals("[11, 12]", info.unverifiedTablets().toString());
            Assertions.assertTrue(info.nodeToTablets().containsKey(node));
            Assertions.assertEquals(maxVersion, info.maxVersion());
            Assertions.assertEquals(minVersion, info.minVersion());
        }

        // case 3: test partition 2 with enforceConsistentVersion = true
        {
            PhysicalPartitionInfo info = Deencapsulation.invoke(TabletRepairHelper.class, "getPhysicalPartitionInfo",
                    db, table, physicalPartitionId2, true, WarehouseComputeResource.DEFAULT);

            Assertions.assertEquals(physicalPartitionId2, info.physicalPartitionId());
            Assertions.assertEquals(2, info.allTablets().size());
            Assertions.assertEquals("[21, 22]", info.allTablets().toString());
            Assertions.assertEquals(2, info.unverifiedTablets().size());
            Assertions.assertEquals("[21, 22]", info.unverifiedTablets().toString());
            Assertions.assertTrue(info.nodeToTablets().containsKey(node));
            Assertions.assertEquals(maxVersion, info.maxVersion());
            Assertions.assertEquals(minVersion + 1, info.minVersion());
        }

        // case 4: test partition 2 with enforceConsistentVersion = false
        {
            PhysicalPartitionInfo info = Deencapsulation.invoke(TabletRepairHelper.class, "getPhysicalPartitionInfo",
                    db, table, physicalPartitionId2, false, WarehouseComputeResource.DEFAULT);

            Assertions.assertEquals(physicalPartitionId2, info.physicalPartitionId());
            Assertions.assertEquals(2, info.allTablets().size());
            Assertions.assertEquals("[21, 22]", info.allTablets().toString());
            Assertions.assertEquals(2, info.unverifiedTablets().size());
            Assertions.assertEquals("[21, 22]", info.unverifiedTablets().toString());
            Assertions.assertTrue(info.nodeToTablets().containsKey(node));
            Assertions.assertEquals(maxVersion, info.maxVersion());
            Assertions.assertEquals(minVersion, info.minVersion());
        }
    }

    private TabletMetadataPB createTabletMetadataPB(long tabletId, long version, boolean hasSstableMeta) {
        TabletMetadataPB metadata = new TabletMetadataPB();
        metadata.id = tabletId;
        metadata.version = version;
        if (hasSstableMeta) {
            metadata.sstableMeta = new PersistentIndexSstableMetaPB();
        }
        return metadata;
    }

    private TabletMetadataPB createTabletMetadataPB(long tabletId, long version) {
        return createTabletMetadataPB(tabletId, version, false);
    }

    private TabletMetadataEntry createTabletMetadataEntry(long tabletId, long version, List<String> missingFiles,
                                                          boolean hasSstableMeta) {
        TabletMetadataEntry entry = new TabletMetadataEntry();
        entry.metadata = createTabletMetadataPB(tabletId, version, hasSstableMeta);
        entry.missingFiles = missingFiles;
        return entry;
    }

    private TabletMetadataEntry createTabletMetadataEntry(long tabletId, long version, List<String> missingFiles) {
        return createTabletMetadataEntry(tabletId, version, missingFiles, false);
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
                TabletResult tr1 = new TabletResult();
                tr1.tabletId = tabletId11;
                tr1.status = new StatusPB();
                tr1.status.statusCode = 0;
                tr1.metadataEntries = Lists.newArrayList();

                tr1.metadataEntries.add(createTabletMetadataEntry(tabletId11, maxVersion, null));
                tr1.metadataEntries.add(createTabletMetadataEntry(tabletId11, minVersion,
                        Lists.newArrayList("file1.sst", "file2.dat")));

                // tablet2 metadata not found
                TabletResult tr2 = new TabletResult();
                tr2.tabletId = tabletId12;
                tr2.status = new StatusPB();
                tr2.status.statusCode = 31;
                tr2.metadataEntries = Lists.newArrayList();

                response.tabletResults = Lists.newArrayList(tr1, tr2);

                // test printTabletMetadatas
                Deencapsulation.invoke(TabletRepairHelper.class, "printTabletMetadatas", response, 1L, 2L, maxVersion,
                        minVersion);

                return CompletableFuture.completedFuture(response);
            }
        };

        Map<Long, Map<Long, TabletMetadataEntry>> tabletToVersionMetadataEntry = Deencapsulation.invoke(
                TabletRepairHelper.class, "getTabletMetadatas", info, maxVersion, minVersion);

        Assertions.assertEquals(1, tabletToVersionMetadataEntry.size());
        Assertions.assertTrue(tabletToVersionMetadataEntry.containsKey(tabletId11));
        Assertions.assertEquals(2, tabletToVersionMetadataEntry.get(tabletId11).size());
        Assertions.assertEquals(maxVersion, tabletToVersionMetadataEntry.get(tabletId11).get(maxVersion).metadata.version);
        Assertions.assertEquals(minVersion, tabletToVersionMetadataEntry.get(tabletId11).get(minVersion).metadata.version);
        Assertions.assertFalse(tabletToVersionMetadataEntry.containsKey(tabletId12));

        // Assert missingFiles for tabletId11 with minVersion
        TabletMetadataEntry entryWithMissingFiles = tabletToVersionMetadataEntry.get(tabletId11).get(minVersion);
        List<String> missingFiles = entryWithMissingFiles.missingFiles;
        Assertions.assertNotNull(missingFiles);
        Assertions.assertEquals(2, missingFiles.size());
        Assertions.assertTrue(missingFiles.contains("file1.sst"));
        Assertions.assertTrue(missingFiles.contains("file2.dat"));
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
                TabletResult tr1 = new TabletResult();
                tr1.tabletId = tabletId11;
                tr1.status = new StatusPB();
                tr1.status.statusCode = 0;
                tr1.metadataEntries = Lists.newArrayList();

                tr1.metadataEntries.add(createTabletMetadataEntry(tabletId11, maxVersion, null));

                // tablet2 get metadata failed
                TabletResult tr2 = new TabletResult();
                tr2.tabletId = tabletId12;
                tr2.status = new StatusPB();
                tr2.status.statusCode = 1;
                tr2.status.errorMsgs = Lists.newArrayList("get tablet metadata failed");

                response.tabletResults = Lists.newArrayList(tr1, tr2);

                return CompletableFuture.completedFuture(response);
            }
        };

        ExceptionChecker.expectThrowsWithMsg(StarRocksException.class, "get tablet metadata failed",
                () -> Deencapsulation.invoke(TabletRepairHelper.class, "getTabletMetadatas", info, maxVersion, minVersion));
    }

    @Test
    public void testCheckTabletMetadataValid() {
        // case 1: no missing files
        {
            TabletMetadataEntry entry = createTabletMetadataEntry(tabletId11, maxVersion, null);
            boolean isValid = Deencapsulation.invoke(TabletRepairHelper.class, "checkTabletMetadataValid", entry);
            Assertions.assertTrue(isValid);

            TabletMetadataPB metadata = Deencapsulation.invoke(TabletRepairHelper.class, "getValidTabletMetadata", entry);
            Assertions.assertNotNull(metadata);
            Assertions.assertEquals(tabletId11, metadata.id);
            Assertions.assertEquals(maxVersion, metadata.version);
            Assertions.assertNull(metadata.sstableMeta);
        }

        // case 2: empty missing files list
        {
            TabletMetadataEntry entry = createTabletMetadataEntry(tabletId11, maxVersion, Lists.newArrayList());
            boolean isValid = Deencapsulation.invoke(TabletRepairHelper.class, "checkTabletMetadataValid", entry);
            Assertions.assertTrue(isValid);

            TabletMetadataPB metadata = Deencapsulation.invoke(TabletRepairHelper.class, "getValidTabletMetadata", entry);
            Assertions.assertNotNull(metadata);
            Assertions.assertEquals(tabletId11, metadata.id);
            Assertions.assertEquals(maxVersion, metadata.version);
            Assertions.assertNull(metadata.sstableMeta);
        }

        // case 3: only missing sst files, with sstableMeta initially present
        {
            TabletMetadataEntry entry = createTabletMetadataEntry(tabletId11, maxVersion,
                    Lists.newArrayList("file1.sst", "file2.sst"), true);
            boolean isValid = Deencapsulation.invoke(TabletRepairHelper.class, "checkTabletMetadataValid", entry);
            Assertions.assertTrue(isValid);

            TabletMetadataPB metadata = Deencapsulation.invoke(TabletRepairHelper.class, "getValidTabletMetadata", entry);
            Assertions.assertNotNull(metadata);
            Assertions.assertEquals(tabletId11, metadata.id);
            Assertions.assertEquals(-1 * maxVersion, metadata.version);
            Assertions.assertNull(metadata.sstableMeta); // sstableMeta should be cleared
        }

        // case 4: missing sst and dat files
        {
            TabletMetadataEntry entry = createTabletMetadataEntry(tabletId11, maxVersion,
                    Lists.newArrayList("file1.sst", "file2.dat"));
            boolean isValid = Deencapsulation.invoke(TabletRepairHelper.class, "checkTabletMetadataValid", entry);
            Assertions.assertFalse(isValid);

            ExceptionChecker.expectThrowsWithMsg(IllegalStateException.class, "should not reach here",
                    () -> Deencapsulation.invoke(TabletRepairHelper.class, "getValidTabletMetadata", entry));
        }

        // case 5: missing only non-sst files
        {
            TabletMetadataEntry entry = createTabletMetadataEntry(tabletId11, maxVersion,
                    Lists.newArrayList("file2.dat"));
            boolean isValid = Deencapsulation.invoke(TabletRepairHelper.class, "checkTabletMetadataValid", entry);
            Assertions.assertFalse(isValid);

            ExceptionChecker.expectThrowsWithMsg(IllegalStateException.class, "should not reach here",
                    () -> Deencapsulation.invoke(TabletRepairHelper.class, "getValidTabletMetadata", entry));
        }
    }

    @Test
    public void testFindValidTabletMetadata() {
        // case 1: enforceConsistentVersion = true with consistent metadata
        {
            Map<Long, Map<Long, TabletMetadataEntry>> tabletToVersionMetadataEntry = Maps.newHashMap();
            Map<Long, TabletMetadataEntry> tablet1Versions = Maps.newHashMap();
            tablet1Versions.put(maxVersion - 1, createTabletMetadataEntry(tabletId11, maxVersion - 1, null));
            tablet1Versions.put(minVersion, createTabletMetadataEntry(tabletId11, minVersion, null));
            tabletToVersionMetadataEntry.put(tabletId11, tablet1Versions);

            Map<Long, TabletMetadataEntry> tablet2Versions = Maps.newHashMap();
            tablet2Versions.put(maxVersion - 1, createTabletMetadataEntry(tabletId12, maxVersion - 1, null));
            tablet2Versions.put(minVersion, createTabletMetadataEntry(tabletId12, minVersion, null));
            tabletToVersionMetadataEntry.put(tabletId12, tablet2Versions);

            Map<Long, TabletMetadataPB> tabletToValidMetadata = Maps.newHashMap();
            Deencapsulation.invoke(TabletRepairHelper.class, "findValidTabletMetadata",
                    info, tabletToVersionMetadataEntry, maxVersion, minVersion, true, tabletToValidMetadata);

            Assertions.assertEquals(2, tabletToValidMetadata.size());
            Assertions.assertTrue(tabletToValidMetadata.containsKey(tabletId11));
            Assertions.assertEquals(maxVersion - 1, tabletToValidMetadata.get(tabletId11).version);
            Assertions.assertTrue(tabletToValidMetadata.containsKey(tabletId12));
            Assertions.assertEquals(maxVersion - 1, tabletToValidMetadata.get(tabletId12).version);

            ExceptionChecker.expectThrowsNoException(
                    () -> Deencapsulation.invoke(TabletRepairHelper.class, "checkOrCreateEmptyTabletMetadata",
                            info, tabletToValidMetadata, true, false));
        }

        // case 2: enforceConsistentVersion = true without consistent metadata
        {
            Map<Long, Map<Long, TabletMetadataEntry>> tabletToVersionMetadataEntry = Maps.newHashMap();
            Map<Long, TabletMetadataEntry> tablet1Versions = Maps.newHashMap();
            tablet1Versions.put(maxVersion, createTabletMetadataEntry(tabletId11, maxVersion, null));
            tabletToVersionMetadataEntry.put(tabletId11, tablet1Versions);

            Map<Long, TabletMetadataEntry> tablet2Versions = Maps.newHashMap();
            tablet2Versions.put(maxVersion - 1, createTabletMetadataEntry(tabletId12, maxVersion - 1, null));
            tabletToVersionMetadataEntry.put(tabletId12, tablet2Versions);

            Map<Long, TabletMetadataPB> tabletToValidMetadata = Maps.newHashMap();
            Deencapsulation.invoke(TabletRepairHelper.class, "findValidTabletMetadata",
                    info, tabletToVersionMetadataEntry, maxVersion, minVersion, true, tabletToValidMetadata);

            Assertions.assertTrue(tabletToValidMetadata.isEmpty());

            ExceptionChecker.expectThrowsWithMsg(StarRocksException.class,
                    "no consistent valid tablet metadata version was found",
                    () -> Deencapsulation.invoke(TabletRepairHelper.class, "checkOrCreateEmptyTabletMetadata",
                            info, tabletToValidMetadata, true, false));
        }

        // case 3: enforceConsistentVersion = false with all latest metadata
        {
            Map<Long, Map<Long, TabletMetadataEntry>> tabletToVersionMetadataEntry = Maps.newHashMap();
            Map<Long, TabletMetadataEntry> tablet1Versions = Maps.newHashMap();
            tablet1Versions.put(maxVersion, createTabletMetadataEntry(tabletId11, maxVersion, null));
            tablet1Versions.put(minVersion, createTabletMetadataEntry(tabletId11, minVersion, null));
            tabletToVersionMetadataEntry.put(tabletId11, tablet1Versions);

            Map<Long, TabletMetadataEntry> tablet2Versions = Maps.newHashMap();
            tablet2Versions.put(maxVersion - 1, createTabletMetadataEntry(tabletId12, maxVersion - 1, null));
            tabletToVersionMetadataEntry.put(tabletId12, tablet2Versions);

            Map<Long, TabletMetadataPB> tabletToValidMetadata = Maps.newHashMap();
            Deencapsulation.invoke(TabletRepairHelper.class, "findValidTabletMetadata",
                    info, tabletToVersionMetadataEntry, maxVersion, minVersion, false, tabletToValidMetadata);

            Assertions.assertEquals(2, tabletToValidMetadata.size());
            Assertions.assertTrue(tabletToValidMetadata.containsKey(tabletId11));
            Assertions.assertEquals(maxVersion, tabletToValidMetadata.get(tabletId11).version);
            Assertions.assertTrue(tabletToValidMetadata.containsKey(tabletId12));
            Assertions.assertEquals(maxVersion - 1, tabletToValidMetadata.get(tabletId12).version);

            ExceptionChecker.expectThrowsNoException(
                    () -> Deencapsulation.invoke(TabletRepairHelper.class, "checkOrCreateEmptyTabletMetadata",
                            info, tabletToValidMetadata, false, false));
        }

        // case 4: enforceConsistentVersion = false with some missing metadata
        {
            Map<Long, Map<Long, TabletMetadataEntry>> tabletToVersionMetadataEntry = Maps.newHashMap();
            Map<Long, TabletMetadataEntry> tablet1Versions = Maps.newHashMap();
            tablet1Versions.put(maxVersion, createTabletMetadataEntry(tabletId11, maxVersion, null));
            tabletToVersionMetadataEntry.put(tabletId11, tablet1Versions);

            // tabletId2 has no metadata

            Map<Long, TabletMetadataPB> tabletToValidMetadata = Maps.newHashMap();
            Deencapsulation.invoke(TabletRepairHelper.class, "findValidTabletMetadata", info, tabletToVersionMetadataEntry,
                    maxVersion, minVersion, false, tabletToValidMetadata);

            Assertions.assertEquals(1, tabletToValidMetadata.size());
            Assertions.assertTrue(tabletToValidMetadata.containsKey(tabletId11));
            Assertions.assertEquals(maxVersion, tabletToValidMetadata.get(tabletId11).version);
            Assertions.assertFalse(tabletToValidMetadata.containsKey(tabletId12));

            ExceptionChecker.expectThrowsWithMsg(StarRocksException.class,
                    "no tablet metadatas were found for tablets [12]",
                    () -> Deencapsulation.invoke(TabletRepairHelper.class, "checkOrCreateEmptyTabletMetadata",
                            info, tabletToValidMetadata, false, false));

            // recover with empty tablet metadata
            ExceptionChecker.expectThrowsNoException(
                    () -> Deencapsulation.invoke(TabletRepairHelper.class, "checkOrCreateEmptyTabletMetadata",
                            info, tabletToValidMetadata, false, true));
            Assertions.assertTrue(tabletToValidMetadata.containsKey(tabletId12));
            Assertions.assertEquals(0L, tabletToValidMetadata.get(tabletId12).version);
        }

        // case 5: enforceConsistentVersion = false with pre-existing valid metadata
        {
            Map<Long, Map<Long, TabletMetadataEntry>> tabletToVersionMetadataEntry = Maps.newHashMap();
            Map<Long, TabletMetadataEntry> tablet2Versions = Maps.newHashMap();
            tablet2Versions.put(minVersion, createTabletMetadataEntry(tabletId12, minVersion, null));
            tablet2Versions.put(minVersion + 1, createTabletMetadataEntry(tabletId12, minVersion + 1, null));
            tabletToVersionMetadataEntry.put(tabletId12, tablet2Versions);

            Map<Long, TabletMetadataPB> tabletToValidMetadata = Maps.newHashMap();
            tabletToValidMetadata.put(tabletId11, createTabletMetadataPB(tabletId11, maxVersion)); // Pre-existing

            Deencapsulation.invoke(TabletRepairHelper.class, "findValidTabletMetadata", info, tabletToVersionMetadataEntry,
                    maxVersion, minVersion, false, tabletToValidMetadata);

            Assertions.assertEquals(2, tabletToValidMetadata.size());
            Assertions.assertTrue(tabletToValidMetadata.containsKey(tabletId11));
            Assertions.assertEquals(maxVersion, tabletToValidMetadata.get(tabletId11).version); // Should remain as pre-existing
            Assertions.assertTrue(tabletToValidMetadata.containsKey(tabletId12));
            Assertions.assertEquals(minVersion + 1, tabletToValidMetadata.get(tabletId12).version);

            ExceptionChecker.expectThrowsNoException(
                    () -> Deencapsulation.invoke(TabletRepairHelper.class, "checkOrCreateEmptyTabletMetadata",
                            info, tabletToValidMetadata, false, false));
        }

        // case 6: enforceConsistentVersion = false with missingFiles (some valid, some invalid)
        {
            Map<Long, Map<Long, TabletMetadataEntry>> tabletToVersionMetadataEntry = Maps.newHashMap();
            // tablet1: 3 versions
            Map<Long, TabletMetadataEntry> tablet1Versions = Maps.newHashMap();
            // maxVersion: missing sst and dat files (invalid)
            tablet1Versions.put(maxVersion, createTabletMetadataEntry(tabletId11, maxVersion,
                    Lists.newArrayList("file1.sst", "file2.dat"), true));
            // maxVersion - 1: only missing sst files (valid)
            tablet1Versions.put(maxVersion - 1, createTabletMetadataEntry(tabletId11, maxVersion - 1,
                    Lists.newArrayList("file3.sst"), true));
            // minVersion: no missing files (valid)
            tablet1Versions.put(minVersion, createTabletMetadataEntry(tabletId11, minVersion, null));
            tabletToVersionMetadataEntry.put(tabletId11, tablet1Versions);

            // tablet2: 2 versions
            Map<Long, TabletMetadataEntry> tablet2Versions = Maps.newHashMap();
            // maxVersion: no missing files (valid)
            tablet2Versions.put(maxVersion, createTabletMetadataEntry(tabletId12, maxVersion, null, true));
            // minVersion: missing sst and dat files (invalid)
            tablet2Versions.put(minVersion, createTabletMetadataEntry(tabletId12, minVersion,
                    Lists.newArrayList("file4.sst", "file5.dat"), true));
            tabletToVersionMetadataEntry.put(tabletId12, tablet2Versions);

            Map<Long, TabletMetadataPB> tabletToValidMetadata = Maps.newHashMap();
            Deencapsulation.invoke(TabletRepairHelper.class, "findValidTabletMetadata", info, tabletToVersionMetadataEntry,
                    maxVersion, minVersion, false, tabletToValidMetadata);

            Assertions.assertEquals(2, tabletToValidMetadata.size());
            Assertions.assertTrue(tabletToValidMetadata.containsKey(tabletId11));
            // Should pick maxVersion - 1 because maxVersion has invalid missing files
            Assertions.assertEquals(-1 * (maxVersion - 1), tabletToValidMetadata.get(tabletId11).version);
            Assertions.assertNull(tabletToValidMetadata.get(tabletId11).sstableMeta); // should be cleared

            Assertions.assertTrue(tabletToValidMetadata.containsKey(tabletId12));
            // Should pick maxVersion because it has no missing files
            Assertions.assertEquals(maxVersion, tabletToValidMetadata.get(tabletId12).version);
            Assertions.assertNotNull(tabletToValidMetadata.get(tabletId12).sstableMeta); // initially true
        }
    }

    @Test
    public void testRepairTabletMetadata() {
        Map<Long, TabletMetadataPB> tabletToValidMetadata = Maps.newHashMap();
        tabletToValidMetadata.put(tabletId11, createTabletMetadataPB(tabletId11, maxVersion));
        tabletToValidMetadata.put(tabletId12, createTabletMetadataPB(tabletId12, maxVersion));

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
                info, tabletToValidMetadata, false);
        Assertions.assertTrue(tabletErrors.isEmpty());
    }

    @Test
    public void testRepairTabletMetadataWithFileBundling() {
        Map<Long, TabletMetadataPB> tabletToValidMetadata = Maps.newHashMap();
        tabletToValidMetadata.put(tabletId11, createTabletMetadataPB(tabletId11, maxVersion));
        tabletToValidMetadata.put(tabletId12, createTabletMetadataPB(tabletId12, maxVersion));

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
                info, tabletToValidMetadata, true);
        Assertions.assertTrue(tabletErrors.isEmpty());
    }

    @Test
    public void testRepairTabletMetadataRpcException() {
        Map<Long, TabletMetadataPB> tabletToValidMetadata = Maps.newHashMap();
        tabletToValidMetadata.put(tabletId11, createTabletMetadataPB(tabletId11, maxVersion));
        tabletToValidMetadata.put(tabletId12, createTabletMetadataPB(tabletId12, maxVersion));

        new MockUp<LakeServiceWithMetrics>() {
            @Mock
            public Future<RepairTabletMetadataResponse> repairTabletMetadata(RepairTabletMetadataRequest request)
                    throws RpcException {
                throw new RpcException("rpc exception");
            }
        };

        ExceptionChecker.expectThrowsWithMsg(RpcException.class, "rpc exception",
                () -> Deencapsulation.invoke(TabletRepairHelper.class, "repairTabletMetadata",
                        info, tabletToValidMetadata, false));
    }

    @Test
    public void testRepairTabletMetadataResponseNull() {
        Map<Long, TabletMetadataPB> tabletToValidMetadata = Maps.newHashMap();
        tabletToValidMetadata.put(tabletId11, createTabletMetadataPB(tabletId11, maxVersion));
        tabletToValidMetadata.put(tabletId12, createTabletMetadataPB(tabletId12, maxVersion));

        new MockUp<LakeServiceWithMetrics>() {
            @Mock
            public Future<RepairTabletMetadataResponse> repairTabletMetadata(RepairTabletMetadataRequest request) {
                return CompletableFuture.completedFuture(null);
            }
        };

        ExceptionChecker.expectThrowsWithMsg(StarRocksException.class, "response is null",
                () -> Deencapsulation.invoke(TabletRepairHelper.class, "repairTabletMetadata",
                        info, tabletToValidMetadata, false));
    }

    @Test
    public void testRepairTabletMetadataRpcLevelStatusFail() {
        Map<Long, TabletMetadataPB> tabletToValidMetadata = Maps.newHashMap();
        tabletToValidMetadata.put(tabletId11, createTabletMetadataPB(tabletId11, maxVersion));
        tabletToValidMetadata.put(tabletId12, createTabletMetadataPB(tabletId12, maxVersion));

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
                        info, tabletToValidMetadata, false));
    }

    @Test
    public void testRepairTabletMetadataPartialFailure() {
        Map<Long, TabletMetadataPB> tabletToValidMetadata = Maps.newHashMap();
        tabletToValidMetadata.put(tabletId11, createTabletMetadataPB(tabletId11, maxVersion));
        tabletToValidMetadata.put(tabletId12, createTabletMetadataPB(tabletId12, maxVersion));

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
                    if (metadata.id == tabletId12) {
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
                info, tabletToValidMetadata, false);

        Assertions.assertEquals(1, tabletErrors.size());
        Assertions.assertTrue(tabletErrors.containsKey(tabletId12));
        Assertions.assertEquals("repair failed", tabletErrors.get(tabletId12));
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
                TabletResult tr1 = new TabletResult();
                tr1.tabletId = tabletId11;
                tr1.status = new StatusPB();
                tr1.status.statusCode = 0;
                tr1.metadataEntries = Lists.newArrayList();

                tr1.metadataEntries.add(createTabletMetadataEntry(tabletId11, maxVersion, null));
                tr1.metadataEntries.add(createTabletMetadataEntry(tabletId11, minVersion, null));

                // tablet2 with 2 versions metadata
                TabletResult tr2 = new TabletResult();
                tr2.tabletId = tabletId12;
                tr2.status = new StatusPB();
                tr2.status.statusCode = 0;
                tr2.metadataEntries = Lists.newArrayList();

                tr2.metadataEntries.add(createTabletMetadataEntry(tabletId12, maxVersion - 1, null));
                tr2.metadataEntries.add(createTabletMetadataEntry(tabletId12, minVersion, null));

                response.tabletResults = Lists.newArrayList(tr1, tr2);

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

        // case 1: enforceConsistentVersion = true
        Map<Long, String> tabletErrors =
                Deencapsulation.invoke(TabletRepairHelper.class, "repairPhysicalPartition", info, true, false, false);
        Assertions.assertTrue(tabletErrors.isEmpty());

        // case 2: enforceConsistentVersion = false
        info = new PhysicalPartitionInfo(physicalPartitionId1, Lists.newArrayList(tabletId11, tabletId12),
                Sets.newHashSet(tabletId11, tabletId12), nodeToTablets, maxVersion, minVersion);
        tabletErrors = Deencapsulation.invoke(TabletRepairHelper.class, "repairPhysicalPartition", info, false, false, false);
        Assertions.assertTrue(tabletErrors.isEmpty());
    }

    @Test
    public void testRepair() {
        // mock warehouse manager
        new MockUp<WarehouseManager>() {
            @Mock
            public ComputeNode getComputeNodeAssignedToTablet(ComputeResource computeResource, long tabletId) {
                return node;
            }
        };

        new MockUp<LakeServiceWithMetrics>() {
            @Mock
            public Future<GetTabletMetadatasResponse> getTabletMetadatas(GetTabletMetadatasRequest request) {
                GetTabletMetadatasResponse response = new GetTabletMetadatasResponse();
                response.status = new StatusPB();
                response.status.statusCode = 0;

                // tablet1 with 2 versions metadata
                TabletResult tr1 = new TabletResult();
                tr1.tabletId = tabletId11;
                tr1.status = new StatusPB();
                tr1.status.statusCode = 0;
                tr1.metadataEntries = Lists.newArrayList();

                tr1.metadataEntries.add(createTabletMetadataEntry(tabletId11, maxVersion, null));
                tr1.metadataEntries.add(createTabletMetadataEntry(tabletId11, minVersion, null));

                // tablet2 with 2 versions metadata
                TabletResult tr2 = new TabletResult();
                tr2.tabletId = tabletId12;
                tr2.status = new StatusPB();
                tr2.status.statusCode = 0;
                tr2.metadataEntries = Lists.newArrayList();

                tr2.metadataEntries.add(createTabletMetadataEntry(tabletId12, maxVersion - 1, null));
                tr2.metadataEntries.add(createTabletMetadataEntry(tabletId12, minVersion, null));

                response.tabletResults = Lists.newArrayList(tr1, tr2);

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

        AdminRepairTableStmt stmt = new AdminRepairTableStmt(
                new TableRef(QualifiedName.of(Lists.newArrayList("db", "table")),
                        new PartitionRef(Lists.newArrayList("p1"), false, NodePosition.ZERO),
                        NodePosition.ZERO),
                Maps.newHashMap(),
                NodePosition.ZERO);

        // case 1: enforceConsistentVersion = true
        stmt.setEnforceConsistentVersion(true);
        ExceptionChecker.expectThrowsNoException(
                () -> Deencapsulation.invoke(TabletRepairHelper.class, "repair", stmt, db, table, Lists.newArrayList("p1"),
                        WarehouseComputeResource.DEFAULT));

        // case 2: enforceConsistentVersion = false
        stmt.setEnforceConsistentVersion(false);
        ExceptionChecker.expectThrowsNoException(
                () -> Deencapsulation.invoke(TabletRepairHelper.class, "repair", stmt, db, table, Lists.newArrayList("p1"),
                        WarehouseComputeResource.DEFAULT));
    }

    @Test
    public void testRepairFail() {
        // mock warehouse manager
        new MockUp<WarehouseManager>() {
            @Mock
            public ComputeNode getComputeNodeAssignedToTablet(ComputeResource computeResource, long tabletId) {
                return node;
            }
        };

        new MockUp<LakeServiceWithMetrics>() {
            @Mock
            public Future<GetTabletMetadatasResponse> getTabletMetadatas(GetTabletMetadatasRequest request) throws RpcException {
                throw new RpcException("rpc exception");
            }
        };

        AdminRepairTableStmt stmt = new AdminRepairTableStmt(
                new TableRef(QualifiedName.of(Lists.newArrayList("db", "table")),
                        new PartitionRef(Lists.newArrayList("p1"), false, NodePosition.ZERO),
                        NodePosition.ZERO),
                Maps.newHashMap(),
                NodePosition.ZERO);

        ExceptionChecker.expectThrowsWithMsg(StarRocksException.class,
                "Fail to repair tablet metadata for 1 partition, the first 1 partition: [{partition: 4, error: rpc exception}]",
                () -> Deencapsulation.invoke(TabletRepairHelper.class, "repair", stmt, db, table, Lists.newArrayList("p1"),
                        WarehouseComputeResource.DEFAULT));
    }

    @Test
    public void testDryRunRepairRecoverable() throws Exception {
        new MockUp<WarehouseManager>() {
            @Mock
            public ComputeNode getComputeNodeAssignedToTablet(ComputeResource computeResource, long tabletId) {
                return node;
            }
        };

        new MockUp<LakeServiceWithMetrics>() {
            @Mock
            public Future<GetTabletMetadatasResponse> getTabletMetadatas(GetTabletMetadatasRequest request) {
                GetTabletMetadatasResponse response = new GetTabletMetadatasResponse();
                response.status = new StatusPB();
                response.status.statusCode = 0;

                TabletResult tr1 = new TabletResult();
                tr1.tabletId = tabletId11;
                tr1.status = new StatusPB();
                tr1.status.statusCode = 0;
                tr1.metadataEntries = Lists.newArrayList();
                tr1.metadataEntries.add(createTabletMetadataEntry(tabletId11, maxVersion, null));
                tr1.metadataEntries.add(createTabletMetadataEntry(tabletId11, minVersion, null));

                TabletResult tr2 = new TabletResult();
                tr2.tabletId = tabletId12;
                tr2.status = new StatusPB();
                tr2.status.statusCode = 0;
                tr2.metadataEntries = Lists.newArrayList();
                tr2.metadataEntries.add(createTabletMetadataEntry(tabletId12, maxVersion - 1, null));
                tr2.metadataEntries.add(createTabletMetadataEntry(tabletId12, minVersion, null));

                response.tabletResults = Lists.newArrayList(tr1, tr2);

                return CompletableFuture.completedFuture(response);
            }
        };

        AdminRepairTableStmt stmt = new AdminRepairTableStmt(
                new TableRef(QualifiedName.of(Lists.newArrayList("db", "table")),
                        new PartitionRef(Lists.newArrayList("p1"), false, NodePosition.ZERO),
                        NodePosition.ZERO),
                Maps.newHashMap(),
                NodePosition.ZERO);

        stmt.setEnforceConsistentVersion(false);
        List<List<String>> result = TabletRepairHelper.dryRunRepair(stmt, db, table, Lists.newArrayList("p1"),
                WarehouseComputeResource.DEFAULT);

        Assertions.assertEquals(1, result.size());
        
        List<String> row = result.get(0);
        Assertions.assertEquals("4", row.get(0)); // PartitionId
        Assertions.assertEquals("8", row.get(1)); // VisibleVersion
        Assertions.assertEquals("RECOVERABLE", row.get(2)); // RepairStatus
        
        String tabletInfoJson = row.get(3);
        Assertions.assertTrue(tabletInfoJson.contains("\"tabletId\":11"));
        Assertions.assertTrue(tabletInfoJson.contains("\"recoverVersion\":8"));
        
        Assertions.assertTrue(tabletInfoJson.contains("\"tabletId\":12"));
        Assertions.assertTrue(tabletInfoJson.contains("\"recoverVersion\":7"));
        
        Assertions.assertEquals("", row.get(4)); // ErrorMsg
    }

    @Test
    public void testDryRunRepairUnrecoverable() throws Exception {
        new MockUp<WarehouseManager>() {
            @Mock
            public ComputeNode getComputeNodeAssignedToTablet(ComputeResource computeResource, long tabletId) {
                return node;
            }
        };

        new MockUp<LakeServiceWithMetrics>() {
            @Mock
            public Future<GetTabletMetadatasResponse> getTabletMetadatas(GetTabletMetadatasRequest request) {
                GetTabletMetadatasResponse response = new GetTabletMetadatasResponse();
                response.status = new StatusPB();
                response.status.statusCode = 0;

                // tablet1 with 1 version metadata
                TabletResult tr1 = new TabletResult();
                tr1.tabletId = tabletId11;
                tr1.status = new StatusPB();
                tr1.status.statusCode = 0;
                tr1.metadataEntries = Lists.newArrayList();
                tr1.metadataEntries.add(createTabletMetadataEntry(tabletId11, maxVersion, null));

                // tablet2 with no metadata (UNRECOVERABLE)
                TabletResult tr2 = new TabletResult();
                tr2.tabletId = tabletId12;
                tr2.status = new StatusPB();
                tr2.status.statusCode = 0;
                tr2.metadataEntries = Lists.newArrayList();

                response.tabletResults = Lists.newArrayList(tr1, tr2);

                return CompletableFuture.completedFuture(response);
            }
        };

        AdminRepairTableStmt stmt = new AdminRepairTableStmt(
                new TableRef(QualifiedName.of(Lists.newArrayList("db", "table")),
                        new PartitionRef(Lists.newArrayList("p1"), false, NodePosition.ZERO),
                        NodePosition.ZERO),
                Maps.newHashMap(),
                NodePosition.ZERO);

        stmt.setEnforceConsistentVersion(false);
        List<List<String>> result = TabletRepairHelper.dryRunRepair(stmt, db, table, Lists.newArrayList("p1"),
                WarehouseComputeResource.DEFAULT);

        Assertions.assertEquals(1, result.size());
        
        List<String> row = result.get(0);
        Assertions.assertEquals("4", row.get(0)); // PartitionId
        Assertions.assertEquals("8", row.get(1)); // VisibleVersion
        Assertions.assertEquals("UNRECOVERABLE", row.get(2)); // RepairStatus
        
        String tabletInfoJson = row.get(3);
        Assertions.assertEquals("[]", tabletInfoJson);

        Assertions.assertTrue(row.get(4).contains("no tablet metadatas were found for tablets"));  // ErrorMsg
    }

    @Test
    public void testDryRunRepairNormal() throws Exception {
        new MockUp<WarehouseManager>() {
            @Mock
            public ComputeNode getComputeNodeAssignedToTablet(ComputeResource computeResource, long tabletId) {
                return node;
            }
        };

        new MockUp<LakeServiceWithMetrics>() {
            @Mock
            public Future<GetTabletMetadatasResponse> getTabletMetadatas(GetTabletMetadatasRequest request) {
                GetTabletMetadatasResponse response = new GetTabletMetadatasResponse();
                response.status = new StatusPB();
                response.status.statusCode = 0;

                // tablet1 with valid metadata
                TabletResult tr1 = new TabletResult();
                tr1.tabletId = tabletId11;
                tr1.status = new StatusPB();
                tr1.status.statusCode = 0;
                tr1.metadataEntries = Lists.newArrayList();
                tr1.metadataEntries.add(createTabletMetadataEntry(tabletId11, maxVersion, null));

                // tablet2 with valid metadata
                TabletResult tr2 = new TabletResult();
                tr2.tabletId = tabletId12;
                tr2.status = new StatusPB();
                tr2.status.statusCode = 0;
                tr2.metadataEntries = Lists.newArrayList();
                tr2.metadataEntries.add(createTabletMetadataEntry(tabletId12, maxVersion, null));

                response.tabletResults = Lists.newArrayList(tr1, tr2);

                return CompletableFuture.completedFuture(response);
            }
        };

        AdminRepairTableStmt stmt = new AdminRepairTableStmt(
                new TableRef(QualifiedName.of(Lists.newArrayList("db", "table")),
                        new PartitionRef(Lists.newArrayList("p1"), false, NodePosition.ZERO),
                        NodePosition.ZERO),
                Maps.newHashMap(),
                NodePosition.ZERO);

        stmt.setEnforceConsistentVersion(false);
        List<List<String>> result = TabletRepairHelper.dryRunRepair(stmt, db, table, Lists.newArrayList("p1"),
                WarehouseComputeResource.DEFAULT);

        Assertions.assertEquals(1, result.size());
        
        List<String> row = result.get(0);
        Assertions.assertEquals("4", row.get(0)); // PartitionId
        Assertions.assertEquals("8", row.get(1)); // VisibleVersion
        Assertions.assertEquals("NORMAL", row.get(2)); // RepairStatus
        
        String tabletInfoJson = row.get(3);
        Assertions.assertEquals("[]", tabletInfoJson); // Should be empty for NORMAL
        
        Assertions.assertEquals("", row.get(4)); // ErrorMsg
    }

    @Test
    public void testDryRunRepairException() throws Exception {
        new MockUp<WarehouseManager>() {
            @Mock
            public ComputeNode getComputeNodeAssignedToTablet(ComputeResource computeResource, long tabletId) {
                return node;
            }
        };

        new MockUp<LakeServiceWithMetrics>() {
            @Mock
            public Future<GetTabletMetadatasResponse> getTabletMetadatas(GetTabletMetadatasRequest request) throws RpcException {
                throw new RpcException("rpc exception");
            }
        };

        AdminRepairTableStmt stmt = new AdminRepairTableStmt(
                new TableRef(QualifiedName.of(Lists.newArrayList("db", "table")),
                        new PartitionRef(Lists.newArrayList("p1"), false, NodePosition.ZERO),
                        NodePosition.ZERO),
                Maps.newHashMap(),
                NodePosition.ZERO);

        stmt.setEnforceConsistentVersion(false);
        List<List<String>> result = TabletRepairHelper.dryRunRepair(stmt, db, table, Lists.newArrayList("p1"),
                WarehouseComputeResource.DEFAULT);

        Assertions.assertEquals(1, result.size());

        List<String> row = result.get(0);
        Assertions.assertEquals("4", row.get(0)); // PartitionId
        Assertions.assertEquals("8", row.get(1)); // VisibleVersion
        Assertions.assertEquals("UNKNOWN", row.get(2)); // RepairStatus

        String tabletInfoJson = row.get(3);
        Assertions.assertEquals("[]", tabletInfoJson);

        Assertions.assertEquals("ERROR: rpc exception", row.get(4)); // ErrorMsg
    }
    @Test
    public void testGetTabletStatus() throws Exception {
        new MockUp<WarehouseManager>() {
            @Mock
            public ComputeNode getComputeNodeAssignedToTablet(ComputeResource computeResource, long tabletId) {
                return node;
            }
        };

        new MockUp<LakeServiceWithMetrics>() {
            @Mock
            public Future<GetTabletMetadatasResponse> getTabletMetadatas(GetTabletMetadatasRequest request) {
                GetTabletMetadatasResponse response = new GetTabletMetadatasResponse();
                response.status = new StatusPB();
                response.status.statusCode = 0;

                // tablet1 with valid metadata
                TabletResult tr1 = new TabletResult();
                tr1.tabletId = tabletId11;
                tr1.status = new StatusPB();
                tr1.status.statusCode = 0;
                tr1.metadataEntries = Lists.newArrayList();
                tr1.metadataEntries.add(createTabletMetadataEntry(tabletId11, maxVersion, null));

                // tablet2 with missing data files
                TabletResult tr2 = new TabletResult();
                tr2.tabletId = tabletId12;
                tr2.status = new StatusPB();
                tr2.status.statusCode = 0;
                tr2.metadataEntries = Lists.newArrayList();
                tr2.metadataEntries.add(createTabletMetadataEntry(
                        tabletId12, maxVersion, Lists.newArrayList("file1.dat", "file2.dat", "file3.dat")));

                response.tabletResults = Lists.newArrayList(tr1, tr2);

                return CompletableFuture.completedFuture(response);
            }
        };

        // case 1: no filter
        List<List<String>> result = TabletRepairHelper.getTabletStatus(
                db, table, Lists.newArrayList("p1"), null, null, 2, WarehouseComputeResource.DEFAULT);

        Assertions.assertEquals(2, result.size());

        List<String> row1 = result.get(0);
        Assertions.assertEquals("11", row1.get(0)); // tabletId
        Assertions.assertEquals("4", row1.get(1)); // partitionId
        Assertions.assertEquals("8", row1.get(2)); // version
        Assertions.assertEquals("NORMAL", row1.get(3)); // status
        Assertions.assertEquals("0", row1.get(4)); // missingFileCount
        Assertions.assertEquals("[]", row1.get(5)); // missingFiles

        List<String> row2 = result.get(1);
        Assertions.assertEquals("12", row2.get(0));
        Assertions.assertEquals("4", row2.get(1));
        Assertions.assertEquals("8", row2.get(2));
        Assertions.assertEquals("MISSING_DATA", row2.get(3));
        Assertions.assertEquals("3", row2.get(4));
        Assertions.assertEquals("[file1.dat, file2.dat, ... 1 more]", row2.get(5));

        // case 2: filter by NORMAL
        result = TabletRepairHelper.getTabletStatus(
                db, table, Lists.newArrayList("p1"), LakeTabletStatus.NORMAL, BinaryType.EQ, 5, WarehouseComputeResource.DEFAULT);
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("11", result.get(0).get(0));

        // case 3: filter by MISSING_DATA
        result = TabletRepairHelper.getTabletStatus(db, table, Lists.newArrayList("p1"), LakeTabletStatus.MISSING_DATA,
                BinaryType.EQ, 5, WarehouseComputeResource.DEFAULT);
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("12", result.get(0).get(0));
        Assertions.assertEquals("[file1.dat, file2.dat, file3.dat]", result.get(0).get(5)); // check limit=5 works
        
        // case 4: filter != NORMAL
        result = TabletRepairHelper.getTabletStatus(
                db, table, Lists.newArrayList("p1"), LakeTabletStatus.NORMAL, BinaryType.NE, 5, WarehouseComputeResource.DEFAULT);
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("12", result.get(0).get(0));
    }

    @Test
    public void testGetValidTabletMetadatasBatchSizeGrowth() {
        // Test that batch version num starts at 5 and doubles each iteration.
        // Use a wide version range: maxVersion=100, minVersion=1 (100 versions total).
        // Only return valid metadata for tablet11 at version 1 (the very last batch),
        // so the loop iterates many times and we can observe batch size growth.
        long wideMaxVersion = 100L;
        long wideMinVersion = 1L;

        PhysicalPartitionInfo wideInfo = new PhysicalPartitionInfo(physicalPartitionId1,
                Lists.newArrayList(tabletId11),
                Sets.newHashSet(tabletId11), nodeToTablets, wideMaxVersion, wideMinVersion);

        // Record the (maxVersion, minVersion) of each RPC call to verify batch growth
        List<long[]> rpcBatches = new ArrayList<>();

        new MockUp<LakeServiceWithMetrics>() {
            @Mock
            public Future<GetTabletMetadatasResponse> getTabletMetadatas(GetTabletMetadatasRequest request) {
                rpcBatches.add(new long[] {request.maxVersion, request.minVersion});

                GetTabletMetadatasResponse response = new GetTabletMetadatasResponse();
                response.status = new StatusPB();
                response.status.statusCode = 0;

                TabletResult tr1 = new TabletResult();
                tr1.tabletId = tabletId11;
                tr1.status = new StatusPB();
                tr1.status.statusCode = 0;
                tr1.metadataEntries = Lists.newArrayList();

                // Only return valid metadata when the batch includes version 1
                if (request.minVersion <= 1) {
                    tr1.metadataEntries.add(createTabletMetadataEntry(tabletId11, 1L, null));
                }

                response.tabletResults = Lists.newArrayList(tr1);
                return CompletableFuture.completedFuture(response);
            }
        };

        long savedMaxBatch = Config.lake_repair_metadata_fetch_max_version_batch_size;
        try {
            Config.lake_repair_metadata_fetch_max_version_batch_size = 160L;

            Map<Long, TabletMetadataPB> result = Deencapsulation.invoke(TabletRepairHelper.class,
                    "getValidTabletMetadatas", wideInfo, false);

            Assertions.assertNotNull(result);
            Assertions.assertTrue(result.containsKey(tabletId11));
            Assertions.assertEquals(1L, result.get(tabletId11).version);

            // Verify batch sizes: should be 5, 10, 20, 40, 80, ...
            // Batch 1: [100, 96], size=5
            // Batch 2: [95, 86],  size=10
            // Batch 3: [85, 66],  size=20
            // Batch 4: [65, 26],  size=40
            // Batch 5: [25, 1],   size=25 (clamped by minVersion, but versionBatchSize=80)
            Assertions.assertTrue(rpcBatches.size() >= 2, "Should have multiple batches");

            // First batch should have size 5
            long firstBatchSize = rpcBatches.get(0)[0] - rpcBatches.get(0)[1] + 1;
            Assertions.assertEquals(5, firstBatchSize, "First batch should have size 5");

            // Second batch should have size 10 (doubled)
            long secondBatchSize = rpcBatches.get(1)[0] - rpcBatches.get(1)[1] + 1;
            Assertions.assertEquals(10, secondBatchSize, "Second batch should have size 10");

            // Third batch should have size 20
            if (rpcBatches.size() > 2) {
                long thirdBatchSize = rpcBatches.get(2)[0] - rpcBatches.get(2)[1] + 1;
                Assertions.assertEquals(20, thirdBatchSize, "Third batch should have size 20");
            }

            // Fourth batch should have size 40
            if (rpcBatches.size() > 3) {
                long fourthBatchSize = rpcBatches.get(3)[0] - rpcBatches.get(3)[1] + 1;
                Assertions.assertEquals(40, fourthBatchSize, "Fourth batch should have size 40");
            }
        } finally {
            Config.lake_repair_metadata_fetch_max_version_batch_size = savedMaxBatch;
        }
    }

    @Test
    public void testGetValidTabletMetadatasBatchSizeCappedByConfig() {
        // Test that batch size is capped by lake_repair_metadata_fetch_max_version_batch_size.
        // Set max to 10, so batch sizes should be: 5, 10, 10, 10, ...
        long wideMaxVersion = 50L;
        long wideMinVersion = 1L;

        PhysicalPartitionInfo wideInfo = new PhysicalPartitionInfo(physicalPartitionId1,
                Lists.newArrayList(tabletId11),
                Sets.newHashSet(tabletId11), nodeToTablets, wideMaxVersion, wideMinVersion);

        List<long[]> rpcBatches = new ArrayList<>();

        new MockUp<LakeServiceWithMetrics>() {
            @Mock
            public Future<GetTabletMetadatasResponse> getTabletMetadatas(GetTabletMetadatasRequest request) {
                rpcBatches.add(new long[] {request.maxVersion, request.minVersion});

                GetTabletMetadatasResponse response = new GetTabletMetadatasResponse();
                response.status = new StatusPB();
                response.status.statusCode = 0;

                TabletResult tr1 = new TabletResult();
                tr1.tabletId = tabletId11;
                tr1.status = new StatusPB();
                tr1.status.statusCode = 0;
                tr1.metadataEntries = Lists.newArrayList();

                if (request.minVersion <= 1) {
                    tr1.metadataEntries.add(createTabletMetadataEntry(tabletId11, 1L, null));
                }

                response.tabletResults = Lists.newArrayList(tr1);
                return CompletableFuture.completedFuture(response);
            }
        };

        long savedMaxBatch = Config.lake_repair_metadata_fetch_max_version_batch_size;
        try {
            Config.lake_repair_metadata_fetch_max_version_batch_size = 10L;

            Map<Long, TabletMetadataPB> result = Deencapsulation.invoke(TabletRepairHelper.class,
                    "getValidTabletMetadatas", wideInfo, false);

            Assertions.assertNotNull(result);
            Assertions.assertTrue(result.containsKey(tabletId11));

            // First batch: size 5
            long firstBatchSize = rpcBatches.get(0)[0] - rpcBatches.get(0)[1] + 1;
            Assertions.assertEquals(5, firstBatchSize, "First batch should have size 5");

            // Second batch: size 10 (doubled from 5, capped at max 10)
            long secondBatchSize = rpcBatches.get(1)[0] - rpcBatches.get(1)[1] + 1;
            Assertions.assertEquals(10, secondBatchSize, "Second batch should have size 10");

            // Third batch: size 10 (stays at max 10)
            if (rpcBatches.size() > 2) {
                long thirdBatchSize = rpcBatches.get(2)[0] - rpcBatches.get(2)[1] + 1;
                Assertions.assertEquals(10, thirdBatchSize, "Third batch should stay at max size 10");
            }

            // All subsequent batches should also be 10
            for (int i = 3; i < rpcBatches.size() - 1; i++) {
                long batchSize = rpcBatches.get(i)[0] - rpcBatches.get(i)[1] + 1;
                Assertions.assertEquals(10, batchSize, "Batch " + i + " should be capped at max size 10");
            }
        } finally {
            Config.lake_repair_metadata_fetch_max_version_batch_size = savedMaxBatch;
        }
    }

    @Test
    public void testGetValidTabletMetadatasEarlyExitWithGrowingBatch() {
        // Test that when valid metadata is found early, the loop exits without growing batch further.
        long wideMaxVersion = 50L;
        long wideMinVersion = 1L;

        PhysicalPartitionInfo wideInfo = new PhysicalPartitionInfo(physicalPartitionId1,
                Lists.newArrayList(tabletId11),
                Sets.newHashSet(tabletId11), nodeToTablets, wideMaxVersion, wideMinVersion);

        List<long[]> rpcBatches = new ArrayList<>();

        new MockUp<LakeServiceWithMetrics>() {
            @Mock
            public Future<GetTabletMetadatasResponse> getTabletMetadatas(GetTabletMetadatasRequest request) {
                rpcBatches.add(new long[] {request.maxVersion, request.minVersion});

                GetTabletMetadatasResponse response = new GetTabletMetadatasResponse();
                response.status = new StatusPB();
                response.status.statusCode = 0;

                TabletResult tr1 = new TabletResult();
                tr1.tabletId = tabletId11;
                tr1.status = new StatusPB();
                tr1.status.statusCode = 0;
                tr1.metadataEntries = Lists.newArrayList();

                // Return valid metadata in the first batch
                tr1.metadataEntries.add(createTabletMetadataEntry(tabletId11, request.maxVersion, null));

                response.tabletResults = Lists.newArrayList(tr1);
                return CompletableFuture.completedFuture(response);
            }
        };

        Map<Long, TabletMetadataPB> result = Deencapsulation.invoke(TabletRepairHelper.class,
                "getValidTabletMetadatas", wideInfo, false);

        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.containsKey(tabletId11));
        // Should exit after just 1 batch since valid metadata was found for all tablets
        Assertions.assertEquals(1, rpcBatches.size(), "Should exit after first batch when all tablets found");
    }

}
