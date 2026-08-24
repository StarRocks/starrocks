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

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.alter.reshard.TabletReshardJobMgr;
import com.starrocks.alter.reshard.TabletReshardUtils;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.Utils;
import com.starrocks.proto.TabletStatRequest;
import com.starrocks.proto.TabletStatResponse;
import com.starrocks.proto.TabletStatResponse.TabletStat;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.AggregateType;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.thrift.TTabletStat;
import com.starrocks.thrift.TTabletStatResult;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TabletStatMgrTest {
    private static final long DB_ID = 1;
    private static final long TABLE_ID = 2;
    private static final long PARTITION_ID = 3;
    private static final long INDEX_ID = 4;
    private static final long PH_PARTITION_ID = 5;

    // A JMockit @Mock for a static method must itself be static, so it cannot read a captured local or
    // an instance field of the test; what the compute-node stub answers, and how often it was asked,
    // lives here instead. Reset at the start of every scan.
    private static final AtomicInteger STUBBED_NODE_COUNT = new AtomicInteger();
    // The bound the scan handed to the drain on the last runScan.
    private static int capturedBound;

    private static final AtomicInteger NODE_COUNT_RESOLUTIONS = new AtomicInteger();

    // The scan fixture is the only thing here that registers a database in the singleton metastore, so
    // its id must be one no built-in owns: ids below GlobalStateMgr.NEXT_ID_INIT_VALUE (10000) are
    // reserved, and LocalMetastore's constructor seeds idToDb with information_schema at
    // SystemId.INFORMATION_SCHEMA_DB_ID (1) and sys at SystemId.SYS_DB_ID (100). Registering over either
    // would evict the built-in from idToDb for the rest of the JVM.
    private static final long SCAN_DB_ID = 10001L;
    private static final String SCAN_DB_NAME = "tablet_stat_scan_test";

    private Database registeredDb;

    @BeforeEach
    public void before() {
        UtFrameUtils.mockInitWarehouseEnv();
    }

    @AfterEach
    public void after() {
        if (registeredDb == null) {
            return;
        }
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        metastore.getIdToDb().remove(registeredDb.getId());
        metastore.getFullNameToDb().remove(registeredDb.getFullName());
        registeredDb = null;
    }

    @Test
    public void testUpdateLocalTabletStat(@Mocked GlobalStateMgr globalStateMgr, @Mocked Utils utils,
                                          @Mocked SystemInfoService systemInfoService) {
        long tablet2Id = 11L;
        long backendId = 20L;
        TabletInvertedIndex invertedIndex = new TabletInvertedIndex();

        // Columns
        List<Column> columns = new ArrayList<Column>();
        Column k1 = new Column("k1", IntegerType.INT, true, null, "", "");
        columns.add(k1);
        columns.add(new Column("k2", IntegerType.BIGINT, true, null, "", ""));
        columns.add(new Column("v", IntegerType.BIGINT, false, AggregateType.SUM, "0", ""));

        // Tablet2 is LocalTablet
        TabletMeta tabletMeta2 = new TabletMeta(DB_ID, TABLE_ID, PARTITION_ID, INDEX_ID, TStorageMedium.HDD);
        invertedIndex.addTablet(tablet2Id, tabletMeta2);
        Replica replica = new Replica(tablet2Id + 1, backendId, 0, Replica.ReplicaState.NORMAL);
        invertedIndex.addReplica(tablet2Id, replica);

        // Partition info and distribution info
        DistributionInfo distributionInfo = new HashDistributionInfo(10, Lists.newArrayList(k1));
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(PARTITION_ID, new DataProperty(TStorageMedium.HDD));
        partitionInfo.setReplicationNum(PARTITION_ID, (short) 3);

        // Table
        MaterializedIndex index = new MaterializedIndex(INDEX_ID, MaterializedIndex.IndexState.NORMAL);
        Partition partition = new Partition(PARTITION_ID, PH_PARTITION_ID, "p1", index, distributionInfo);
        OlapTable table = new OlapTable(TABLE_ID, "t1", columns, KeysType.AGG_KEYS, partitionInfo, distributionInfo);
        Deencapsulation.setField(table, "baseIndexMetaId", INDEX_ID);
        table.addPartition(partition);
        table.setIndexMeta(INDEX_ID, "t1", columns, 0, 0, (short) 3, TStorageType.COLUMN, KeysType.AGG_KEYS);

        // Db
        Database db = new Database();
        db.registerTableUnlocked(table);

        TTabletStatResult result = new TTabletStatResult();
        Map<Long, TTabletStat> tabletsStats = Maps.newHashMap();
        result.setTablets_stats(tabletsStats);
        TTabletStat tablet2Stat = new TTabletStat(tablet2Id);
        tablet2Stat.setData_size(200L);
        tablet2Stat.setRow_num(201L);
        tabletsStats.put(tablet2Id, tablet2Stat);

        new Expectations() {{
                GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
                result = invertedIndex;
            }};

        // Check
        TabletStatMgr tabletStatMgr = new TabletStatMgr();
        Deencapsulation.invoke(tabletStatMgr, "updateLocalTabletStat", backendId, result);

        Assertions.assertEquals(200L, replica.getDataSize());
        Assertions.assertEquals(201L, replica.getRowCount());
    }

    private LakeTable createLakeTableForTest() {
        long tablet1Id = 10L;
        long tablet2Id = 11L;
        long tablet3Id = 12L;

        // Schema
        List<Column> columns = Lists.newArrayList();
        Column k1 = new Column("k1", IntegerType.INT, true, null, "", "");
        columns.add(k1);
        columns.add(new Column("k2", IntegerType.BIGINT, true, null, "", ""));
        columns.add(new Column("v", IntegerType.BIGINT, false, AggregateType.SUM, "0", ""));

        long visibleVersionTime = System.currentTimeMillis();

        // Tablet
        LakeTablet tablet1 = new LakeTablet(tablet1Id);
        LakeTablet tablet2 = new LakeTablet(tablet2Id);
        LakeTablet tablet3 = new LakeTablet(tablet3Id);
        tablet1.setDataSizeUpdateTime(0);
        tablet2.setDataSizeUpdateTime(0);
        tablet3.setDataSizeUpdateTime(visibleVersionTime);

        // Index
        MaterializedIndex index = new MaterializedIndex(INDEX_ID, MaterializedIndex.IndexState.NORMAL);
        TabletMeta tabletMeta = new TabletMeta(DB_ID, TABLE_ID, PARTITION_ID, INDEX_ID, TStorageMedium.HDD, true);
        index.addTablet(tablet1, tabletMeta);
        index.addTablet(tablet2, tabletMeta);

        // Partition
        DistributionInfo distributionInfo = new HashDistributionInfo(10, Lists.newArrayList(k1));
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setReplicationNum(PARTITION_ID, (short) 3);
        Partition partition = new Partition(PARTITION_ID, PH_PARTITION_ID, "p1", index, distributionInfo);
        partition.getDefaultPhysicalPartition().setVisibleVersion(2L, visibleVersionTime);

        // Lake table
        LakeTable table = new LakeTable(TABLE_ID, "t1", columns, KeysType.AGG_KEYS, partitionInfo, distributionInfo);
        Deencapsulation.setField(table, "baseIndexMetaId", INDEX_ID);
        table.addPartition(partition);
        table.setIndexMeta(INDEX_ID, "t1", columns, 0, 0, (short) 3, TStorageType.COLUMN, KeysType.AGG_KEYS);

        return table;
    }

    @Test
    public void testUpdateLakeTabletStat(@Mocked SystemInfoService systemInfoService,
                                         @Mocked LakeService lakeService) {

        LakeTable table = createLakeTableForTest();

        long tablet1Id =
                table.getPartition(PARTITION_ID).getDefaultPhysicalPartition().getLatestBaseIndex().getTablets().get(0).getId();
        long tablet2Id =
                table.getPartition(PARTITION_ID).getDefaultPhysicalPartition().getLatestBaseIndex().getTablets().get(1).getId();

        // db
        Database db = new Database(DB_ID, "db");
        db.registerTableUnlocked(table);

        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(TNetworkAddress addr) {
                return lakeService;
            }

            @Mock
            public LakeService getLakeService(String host, int port) {
                return lakeService;
            }
        };
        new MockUp<Utils>() {
            @Mock
            public Long chooseNodeId(LakeTablet tablet) {
                return 1000L;
            }

            @Mock
            public ComputeNode chooseNode(LakeTablet tablet) {
                return new ComputeNode();
            }
        };

        long tablet1NumRows = 20L;
        long tablet2NumRows = 21L;
        long tablet1DataSize = 30L;
        long tablet2DataSize = 31L;

        new Expectations() {
            {
                lakeService.getTabletStats((TabletStatRequest) any);
                minTimes = 1;
                maxTimes = 1;
                result = new Delegate() {
                    Future<TabletStatResponse> getTabletStats(TabletStatRequest request) {
                        Assertions.assertEquals(LakeService.TIMEOUT_GET_TABLET_STATS, (long) request.timeoutMs);
                        Assertions.assertEquals(2, request.tabletInfos.size());
                        Assertions.assertEquals(tablet1Id, (long) request.tabletInfos.get(0).tabletId);
                        Assertions.assertEquals(tablet2Id, (long) request.tabletInfos.get(1).tabletId);

                        return new Future<TabletStatResponse>() {
                            @Override
                            public boolean cancel(boolean mayInterruptIfRunning) {
                                return false;
                            }

                            @Override
                            public boolean isCancelled() {
                                return false;
                            }

                            @Override
                            public boolean isDone() {
                                return true;
                            }

                            @Override
                            public TabletStatResponse get() {
                                List<TabletStat> stats = Lists.newArrayList();
                                TabletStat stat1 = new TabletStat();
                                stat1.tabletId = tablet1Id;
                                stat1.numRows = tablet1NumRows;
                                stat1.dataSize = tablet1DataSize;
                                stats.add(stat1);
                                TabletStat stat2 = new TabletStat();
                                stat2.tabletId = tablet2Id;
                                stat2.numRows = tablet2NumRows;
                                stat2.dataSize = tablet2DataSize;
                                stats.add(stat2);

                                TabletStatResponse response = new TabletStatResponse();
                                response.tabletStats = stats;
                                return response;
                            }

                            @Override
                            public TabletStatResponse get(long timeout, @NotNull TimeUnit unit) {
                                return null;
                            }
                        };
                    }
                };
            }
        };

        long t1 = System.currentTimeMillis();
        TabletStatMgr tabletStatMgr = new TabletStatMgr();
        Deencapsulation.invoke(tabletStatMgr, "updateLakeTableTabletStat", db, table);
        long t2 = System.currentTimeMillis();

        LakeTablet tablet1 = (LakeTablet) table.getPartition(PARTITION_ID).getDefaultPhysicalPartition()
                .getLatestBaseIndex().getTablets().get(0);
        LakeTablet tablet2 = (LakeTablet) table.getPartition(PARTITION_ID).getDefaultPhysicalPartition()
                .getLatestBaseIndex().getTablets().get(1);

        Assertions.assertEquals(tablet1.getRowCount(-1), tablet1NumRows);
        Assertions.assertEquals(tablet1.getDataSize(true), tablet1DataSize);
        Assertions.assertEquals(tablet2.getRowCount(-1), tablet2NumRows);
        Assertions.assertEquals(tablet2.getDataSize(true), tablet2DataSize);
        Assertions.assertTrue(tablet1.getDataSizeUpdateTime() >= t1 && tablet1.getDataSizeUpdateTime() <= t2);
        Assertions.assertTrue(tablet2.getDataSizeUpdateTime() >= t1 && tablet2.getDataSizeUpdateTime() <= t2);
    }

    @Test
    public void testUpdateLakeTabletStat2(@Mocked SystemInfoService systemInfoService,
                                          @Mocked LakeService lakeService) {
        LakeTable table = createLakeTableForTest();


        // db
        Database db = new Database(DB_ID, "db");
        db.registerTableUnlocked(table);

        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(TNetworkAddress addr) {
                throw new RuntimeException("injected exception");
            }

            @Mock
            public LakeService getLakeService(String host, int port) {
                throw new RuntimeException("injected exception");
            }
        };
        new MockUp<Utils>() {
            @Mock
            public Long chooseNodeId(LakeTablet tablet) {
                return 1000L;
            }

            @Mock
            public ComputeNode chooseNode(LakeTablet tablet) {
                return new ComputeNode();
            }
        };

        TabletStatMgr tabletStatMgr = new TabletStatMgr();
        Deencapsulation.invoke(tabletStatMgr, "updateLakeTableTabletStat", db, table);

        LakeTablet tablet1 = (LakeTablet) table.getPartition(PARTITION_ID).getDefaultPhysicalPartition()
                .getLatestBaseIndex().getTablets().get(0);
        LakeTablet tablet2 = (LakeTablet) table.getPartition(PARTITION_ID).getDefaultPhysicalPartition()
                .getLatestBaseIndex().getTablets().get(1);

        Assertions.assertEquals(0, tablet1.getRowCount(-1));
        Assertions.assertEquals(0, tablet1.getDataSize(true));
        Assertions.assertEquals(0, tablet2.getRowCount(-1));
        Assertions.assertEquals(0, tablet2.getDataSize(true));
        Assertions.assertEquals(0L, tablet1.getDataSizeUpdateTime());
        Assertions.assertEquals(0L, tablet2.getDataSizeUpdateTime());
    }

    @Test
    public void testUpdateLakeTabletStat3(@Mocked SystemInfoService systemInfoService,
                                          @Mocked LakeService lakeService) {
        LakeTable table = createLakeTableForTest();


        // db
        Database db = new Database(DB_ID, "db");
        db.registerTableUnlocked(table);

        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(TNetworkAddress addr) {
                return lakeService;
            }

            @Mock
            public LakeService getLakeService(String host, int port) {
                return lakeService;
            }
        };
        new MockUp<Utils>() {
            @Mock
            public Long chooseNodeId(LakeTablet tablet) {
                return 1000L;
            }

            @Mock
            public ComputeNode chooseNode(LakeTablet tablet) {
                return new ComputeNode();
            }
        };


        new Expectations() {
            {
                lakeService.getTabletStats((TabletStatRequest) any);
                minTimes = 1;
                maxTimes = 1;
                result = new Delegate() {
                    Future<TabletStatResponse> getTabletStats(TabletStatRequest request) {
                        return new Future<TabletStatResponse>() {
                            @Override
                            public boolean cancel(boolean mayInterruptIfRunning) {
                                return false;
                            }

                            @Override
                            public boolean isCancelled() {
                                return false;
                            }

                            @Override
                            public boolean isDone() {
                                return true;
                            }

                            @Override
                            public TabletStatResponse get() throws ExecutionException {
                                throw new ExecutionException(new RuntimeException("injected"));
                            }

                            @Override
                            public TabletStatResponse get(long timeout, @NotNull TimeUnit unit) {
                                return null;
                            }
                        };
                    }
                };
            }
        };

        TabletStatMgr tabletStatMgr = new TabletStatMgr();
        Deencapsulation.invoke(tabletStatMgr, "updateLakeTableTabletStat", db, table);

        LakeTablet tablet1 = (LakeTablet) table.getPartition(PARTITION_ID).getDefaultPhysicalPartition()
                .getLatestBaseIndex().getTablets().get(0);
        LakeTablet tablet2 = (LakeTablet) table.getPartition(PARTITION_ID).getDefaultPhysicalPartition()
                .getLatestBaseIndex().getTablets().get(1);

        Assertions.assertEquals(0, tablet1.getRowCount(-1));
        Assertions.assertEquals(0, tablet1.getDataSize(true));
        Assertions.assertEquals(0, tablet2.getRowCount(-1));
        Assertions.assertEquals(0, tablet2.getDataSize(true));
        Assertions.assertEquals(0L, tablet1.getDataSizeUpdateTime());
        Assertions.assertEquals(0L, tablet2.getDataSizeUpdateTime());
    }

    @Test
    public void testNoAliveNode(@Mocked SystemInfoService systemInfoService, @Mocked LakeService lakeService) {
        LakeTable table = createLakeTableForTest();

        // db
        Database db = new Database(DB_ID, "db");
        db.registerTableUnlocked(table);

        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(TNetworkAddress addr) {
                return lakeService;
            }

            @Mock
            public LakeService getLakeService(String host, int port) {
                return lakeService;
            }
        };
        new MockUp<Utils>() {
            @Mock
            public Long chooseNodeId(LakeTablet tablet) {
                return 1000L;
            }

            @Mock
            public ComputeNode chooseNode(LakeTablet tablet) {
                return null;
            }
        };

        TabletStatMgr tabletStatMgr = new TabletStatMgr();
        assertDoesNotThrow(() -> {
            Deencapsulation.invoke(tabletStatMgr, "updateLakeTableTabletStat", db, table);
        });
    }


    @Test
    public void testExceptionAliveNode(@Mocked WarehouseManager warehouseManager, @Mocked LakeService lakeService,
                                       @Mocked GlobalStateMgr globalStateMgr) {
        LakeTable table = createLakeTableForTest();

        // db
        Database db = new Database(DB_ID, "db");
        db.registerTableUnlocked(table);

        new MockUp<GlobalStateMgr>() {
            @Mock
            public GlobalStateMgr getCurrentState() {
                return globalStateMgr;
            }
        };

        new MockUp<WarehouseManager>() {
            @Mock
            public WarehouseManager getWarehouseMgr() {
                return warehouseManager;
            }
        };

        new MockUp<Utils>() {
            @Mock
            public Long chooseNodeId(LakeTablet tablet) {
                return 1000L;
            }

            @Mock
            public ComputeNode chooseNode(LakeTablet tablet) {
                return null;
            }
        };

        new Expectations() {
            {
                warehouseManager.getComputeNodeAssignedToTablet((ComputeResource) any, anyLong);
                result = new Delegate() {
                    ComputeNode getComputeNodeAssignedToTablet(ComputeResource computeResource, long tabletId) {
                        throw ErrorReportException.report(ErrorCode.ERR_NO_NODES_IN_WAREHOUSE, tabletId);
                    }
                };
            }
        };

        new Expectations() {
            {
                lakeService.getTabletStats((TabletStatRequest) any);
                times = 0;
            }
        };

        TabletStatMgr tabletStatMgr = new TabletStatMgr();
        Deencapsulation.invoke(tabletStatMgr, "updateLakeTableTabletStat", db, table);

    }

    @Test
    public void testNullAliveNode(@Mocked WarehouseManager warehouseManager, @Mocked LakeService lakeService,
                                  @Mocked GlobalStateMgr globalStateMgr) {
        LakeTable table = createLakeTableForTest();

        // db
        Database db = new Database(DB_ID, "db");
        db.registerTableUnlocked(table);

        new MockUp<GlobalStateMgr>() {
            @Mock
            public GlobalStateMgr getCurrentState() {
                return globalStateMgr;
            }
        };

        new MockUp<WarehouseManager>() {
            @Mock
            public WarehouseManager getWarehouseMgr() {
                return warehouseManager;
            }
        };


        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(TNetworkAddress addr) {
                return lakeService;
            }

            @Mock
            public LakeService getLakeService(String host, int port) {
                return lakeService;
            }
        };
        new MockUp<Utils>() {
            @Mock
            public Long chooseNodeId(LakeTablet tablet) {
                return 1000L;
            }

            @Mock
            public ComputeNode chooseNode(LakeTablet tablet) {
                return null;
            }
        };

        new Expectations() {
            {
                warehouseManager.getComputeNodeAssignedToTablet((ComputeResource) any, anyLong);
                result = new Delegate() {
                    ComputeNode getComputeNodeAssignedToTablet(ComputeResource computeResource, long tabletId) {
                        if (tabletId == 10L) {
                            return null;
                        }
                        return new ComputeNode(1000L, "127.0.0.1", 9030);
                    }
                };
            }
        };

        new Expectations() {
            {
                lakeService.getTabletStats((TabletStatRequest) any);
                times = 1;
            }
        };

        TabletStatMgr tabletStatMgr = new TabletStatMgr();
        Deencapsulation.invoke(tabletStatMgr, "updateLakeTableTabletStat", db, table);

    }

    /**
     * Registers a range-distribution LakeTable whose base index holds exactly these tablets, all with a
     * fresh size, so one scan cycle sees a single index of the requested shape.
     */
    private void registerRangeDistributionTable(long... tabletSizes) {
        LakeTable table = createLakeTableForTest();
        // Reshard, and with it the early-split signal, only looks at range-distribution tables.
        table.setDefaultDistributionInfo(new RangeDistributionInfo());

        MaterializedIndex index =
                table.getPartition(PARTITION_ID).getDefaultPhysicalPartition().getLatestBaseIndex();
        index.clearTabletsForRestore();
        TabletMeta tabletMeta =
                new TabletMeta(SCAN_DB_ID, TABLE_ID, PARTITION_ID, INDEX_ID, TStorageMedium.HDD, true);
        long tabletId = 100L;
        for (long tabletSize : tabletSizes) {
            LakeTablet tablet = new LakeTablet(tabletId++);
            tablet.setDataSize(tabletSize);
            // Fresh by construction, so the merge-freshness walk of the same scan is well defined.
            tablet.setDataSizeUpdateTime(Long.MAX_VALUE);
            index.addTablet(tablet, tabletMeta, false);
        }

        registeredDb = new Database(SCAN_DB_ID, SCAN_DB_NAME);
        registeredDb.registerTableUnlocked(table);
        GlobalStateMgr.getCurrentState().getLocalMetastore().unprotectCreateDb(registeredDb);
    }

    /**
     * Runs one tablet-stat cycle over such a table as the given FE role and returns the early signal it
     * emitted, or -1 when no candidate was emitted at all. NODE_COUNT_RESOLUTIONS then holds how many
     * times the scan resolved a compute-node count; 0 means the eligibility gate short-circuited before
     * the probe.
     */
    private long runScan(boolean leader, int stubbedNodeCount, long... tabletSizes) {
        long[] captured = {-1L};
        capturedBound = -1;
        STUBBED_NODE_COUNT.set(stubbedNodeCount);
        NODE_COUNT_RESOLUTIONS.set(0);

        new MockUp<GlobalStateMgr>() {
            @Mock
            public boolean isLeader() {
                return leader;
            }
        };
        new MockUp<TabletReshardUtils>() {
            @Mock
            public static int safeComputeNodeCountForTable(long tableId) {
                // Answer the stub only on the FIRST call. A second resolution would mean the merge floor
                // and the adaptive bound came from different samples, which a warehouse resize could
                // make inconsistent, so make that show up as a wrong answer rather than a silent pass.
                return NODE_COUNT_RESOLUTIONS.incrementAndGet() == 1 ? STUBBED_NODE_COUNT.get() : 1;
            }
        };
        new MockUp<TabletReshardJobMgr>() {
            @Mock
            public void addReshardCandidate(long dbId, long tableId, long maxTabletSize,
                    long minAdjacentTabletPairSize, long maxAdaptiveSplitTabletSize, int adaptiveBound) {
                captured[0] = maxAdaptiveSplitTabletSize;
                capturedBound = adaptiveBound;
            }
        };

        int savedMaxSplitCount = Config.tablet_reshard_max_split_count;
        // Pinned above every node count used below so the bound is governed by the node count;
        // a change to this default must not be able to flatten these cases into each other.
        Config.tablet_reshard_max_split_count = 1024;
        try {
            registerRangeDistributionTable(tabletSizes);
            new TabletStatMgr().runAfterCatalogReady();
        } finally {
            Config.tablet_reshard_max_split_count = savedMaxSplitCount;
        }
        return captured[0];
    }

    @Test
    public void theScanHandsTheDrainTheBoundItResolved() {
        // The drain folds this into its suppression fingerprint and the planner spends it as headroom.
        // Resolving it again down there would re-probe StarMgr for a number the scan is already
        // holding -- and would keep paying for it on every scan of an index that stays suppressed.
        runScan(true, 4, 8L << 30);
        assertEquals(4, capturedBound, "the scan must hand over the bound it resolved");
    }

    @Test
    public void aFollowerResolvesNoNodeCount() {
        // TabletStatMgr runs on every FE, but reshard is leader-only. Without this, every follower
        // would resolve a compute-node count per eligible table on every scan, and the probe behind
        // that resolution reaches StarMgr.
        runScan(false, 4, 8L << 30);
        assertEquals(0, NODE_COUNT_RESOLUTIONS.get(), "a follower must not resolve a compute-node count");
    }

    @Test
    public void aLeaderResolvesTheNodeCountOnce() {
        // The companion to the above: the same fixture on a leader must reach the probe exactly once,
        // so the follower case above is demonstrably about the leader flag and not about the fixture
        // failing to reach the code at all.
        runScan(true, 4, 8L << 30);
        assertEquals(1, NODE_COUNT_RESOLUTIONS.get(),
                "a leader must resolve the compute-node count exactly once per eligible table");
    }

    @Test
    public void anIndexAtItsBoundEmitsNoEarlySignalHoweverLargeItsTabletsAre() {
        // The same four nodes and the same 12 GiB tablet as the case below, but five tablets against a
        // bound of four. The planner spends that bound as headroom and has none left here, so a signal
        // would only buy a walk of every partition under the table read lock for a plan that must come
        // out empty -- on every scan, for as long as the index keeps this shape. At its bound an index
        // is auto-merge's business, not the split rule's.
        assertEquals(0, runScan(true, 4, 6L << 30, 12L << 30, 4L << 30, 1L << 30, 1L << 30));
    }

    @Test
    public void emitsTheEarlySignalOnlyForUnderProvisionedIndexes() {
        // 4 nodes -> bound 4. 22 GiB over that bound wants 5.5 GiB tablets, so only a tablet worth two
        // of them splits: 12 GiB does, 6 and 4 do not. The largest is neither the first nor the last the
        // scan walks, so a fold that keeps the wrong one cannot land on the right answer by accident.
        assertEquals(12L << 30, runScan(true, 4, 6L << 30, 12L << 30, 4L << 30));
        assertEquals(1, NODE_COUNT_RESOLUTIONS.get(),
                "the merge floor and the early ceiling must derive from ONE probed sample per table");
    }

    @Test
    public void emitsNoEarlySignalWhenTheIndexIsAtTheCeiling() {
        // 2 compute nodes -> early ceiling 2, and the index already holds 2 tablets.
        assertEquals(0L, runScan(true, 2, 8L << 30, 8L << 30));
    }

    @Test
    public void emitsNoEarlySignalForASingleNodeWarehouse() {
        // 1 compute node -> early ceiling min(1, floor 2) = 1, so even a one-tablet index is already at
        // it. This is the one live-warehouse tablet count where the ceiling and the merge floor differ,
        // so it is what keeps the ceiling from being replaced by the floor already in scope.
        assertEquals(0L, runScan(true, 1, 8L << 30));
    }

    @Test
    public void emitsNoEarlySignalWhenTheNodeCountIsUnavailable() {
        assertEquals(0L, runScan(true, 0, 8L << 30),
                "an unresolved node count keeps the merge floor at 0 and emits no early signal");
    }
}
