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
import com.starrocks.common.Range;
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
import com.starrocks.sql.common.MetaUtils;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    // The merge signal the scan handed to the drain on the last runScan.
    private static long capturedMinAdjacentPairSize;

    private static final AtomicInteger NODE_COUNT_RESOLUTIONS = new AtomicInteger();

    // The split cap every runScan pins, named so a case can derive the parallelism floor the scan
    // actually applied instead of restating the number.
    private static final int PINNED_MAX_SPLIT_COUNT = 1024;

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
        return runScan(leader, stubbedNodeCount, () -> registerRangeDistributionTable(tabletSizes));
    }

    /**
     * As above, over a table the caller registers itself -- the range-colocate cases below need a
     * fixture the size-only builder cannot describe.
     */
    private long runScan(boolean leader, int stubbedNodeCount, Runnable registerFixture) {
        long[] captured = {-1L};
        capturedBound = -1;
        capturedMinAdjacentPairSize = -1L;
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
                capturedMinAdjacentPairSize = minAdjacentTabletPairSize;
            }
        };

        int savedMaxSplitCount = Config.tablet_reshard_max_split_count;
        // Pinned above every node count used below so the bound is governed by the node count;
        // a change to this default must not be able to flatten these cases into each other.
        Config.tablet_reshard_max_split_count = PINNED_MAX_SPLIT_COUNT;
        try {
            registerFixture.run();
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

    private static final long ROLLUP_INDEX_ID = 6L;
    private static final long COLOCATE_GROUP_ID = 7001L;
    private static final int COLOCATE_COLUMN_COUNT = 1;
    private static final long OBSERVER_TABLE_ID = 8L;
    private static final long ORPHANED_INDEX_ID = 9L;
    // Never passed to setIndexMeta, so getIndexMetaByMetaId cannot resolve it.
    private static final long ORPHANED_INDEX_META_ID = 10L;
    private static final long OBSERVER_ROWS_PER_TABLET = 100L;

    /**
     * What the mocked colocate index says about the fixture table. Three states, not two, because the
     * scan distinguishes three: not range-colocate at all (ordinary boundary-blind signal), range-colocate
     * and classifiable, and range-colocate but not classifiable right now -- the last withholds the merge
     * signal instead of falling back to the first.
     */
    private enum ColocateState {
        NOT_COLOCATE,
        STABLE,
        UNSTABLE
    }

    private static final Column COLOCATE_COLUMN = new Column("k1", IntegerType.INT, true, null, "", "");

    // Sort key of the second visible index: a strict prefix of the base index's (k1, k2), which is what
    // a rollup or an MV can carry. Only k1 is a key column, so the resolved sort key is (k1).
    private static final List<Column> ROLLUP_SCHEMA = List.of(COLOCATE_COLUMN,
            new Column("v", IntegerType.BIGINT, false, AggregateType.SUM, "0", ""));

    /**
     * The colocate topology every range-colocate case below tiles:
     * R0 = (-inf, 200), R1 = [200, 300), R2 = [300, +inf). The only within-range pair is carved out of
     * R1, the MIDDLE range, so a classification that collapsed onto the first or the last colocate
     * range cannot land on the right answer by accident.
     */
    private static final List<ColocateRange> COLOCATE_RANGES = List.of(
            new ColocateRange(Range.lt(colocatePrefix(200)), 9001L),
            new ColocateRange(Range.gelt(colocatePrefix(200), colocatePrefix(300)), 9002L),
            new ColocateRange(Range.ge(colocatePrefix(300)), 9003L));

    private static Tuple colocatePrefix(int k1) {
        return new Tuple(List.of(Variant.of(IntegerType.INT, String.valueOf(k1))));
    }

    /**
     * The tablet ranges of one index, tiling {@link #COLOCATE_RANGES} against that index's OWN sort key:
     * one tablet per colocate range, plus -- when {@code carveMiddleRange} -- a second tablet inside R1,
     * the topology's only pair a merge may legally act on.
     */
    private static List<Range<Tuple>> tileColocateRanges(List<Column> sortKeyColumns, boolean carveMiddleRange) {
        List<Range<Tuple>> expanded = ColocateRangeUtils.expandColocateRanges(
                COLOCATE_RANGES, sortKeyColumns, COLOCATE_COLUMN_COUNT);
        if (!carveMiddleRange) {
            return expanded;
        }
        // 250 lies inside R1 and is not a registered colocate boundary, so both halves stay in R1.
        Tuple carvePoint = ColocateRangeUtils.expandToFullSortKey(
                Range.ge(colocatePrefix(250)), sortKeyColumns, COLOCATE_COLUMN_COUNT).getLowerBound();
        return List.of(expanded.get(0),
                Range.gelt(expanded.get(1).getLowerBound(), carvePoint),
                Range.gelt(carvePoint, expanded.get(1).getUpperBound()),
                expanded.get(2));
    }

    /**
     * Registers a range-distribution LakeTable whose visible indexes tile the colocate topology, and
     * makes the colocate index answer for it according to {@code colocateState}. Returns the physical
     * partition, so a caller can reach either index to post-process it (see the classification-failure
     * cases) or to read its row counts back after the scan.
     *
     * <p>Three sizes give one tablet per ColocateRange, so every adjacent pair crosses a boundary; four
     * carve R1 in two and so add the topology's only within-range pair. A non-null {@code rollupSizes}
     * adds a second VISIBLE index, tiled the same way against its own shorter sort key.
     */
    private PhysicalPartition registerColocateTable(ColocateState colocateState, long[] baseSizes,
                                                    long[] rollupSizes) {
        LakeTable table = createLakeTableForTest();
        table.setDefaultDistributionInfo(new RangeDistributionInfo());
        PhysicalPartition physicalPartition = table.getPartition(PARTITION_ID).getDefaultPhysicalPartition();

        MaterializedIndex baseIndex = physicalPartition.getLatestBaseIndex();
        baseIndex.clearTabletsForRestore();
        addTiledTablets(table, baseIndex, 100L, baseSizes);

        if (rollupSizes != null) {
            table.setIndexMeta(ROLLUP_INDEX_ID, "r1", ROLLUP_SCHEMA, 0, 0, (short) 1,
                    TStorageType.COLUMN, KeysType.AGG_KEYS);
            physicalPartition.createRollupIndex(
                    new MaterializedIndex(ROLLUP_INDEX_ID, MaterializedIndex.IndexState.NORMAL));
            addTiledTablets(table, physicalPartition.getLatestIndex(ROLLUP_INDEX_ID), 200L, rollupSizes);
        }

        mockColocateTableIndex(colocateState);

        registeredDb = new Database(SCAN_DB_ID, SCAN_DB_NAME);
        registeredDb.registerTableUnlocked(table);
        GlobalStateMgr.getCurrentState().getLocalMetastore().unprotectCreateDb(registeredDb);
        return physicalPartition;
    }

    /**
     * The lower bound of a malformed or racing TabletRange: fewer values than the colocate prefix, which
     * is what trips {@code extractColocatePrefix}'s precondition inside {@code Classifier.indexOf}.
     */
    private static TabletRange rangeTooShortForColocatePrefix() {
        return new TabletRange(Range.ge(new Tuple(List.of())));
    }

    /**
     * Adds a second table to the same database and returns its base index, so a case can check that the
     * row-count pass still reached a table walked AFTER the one whose classification blew up.
     *
     * <p>Hash-distributed on purpose, which makes it reshard-INELIGIBLE: an eligible table would emit a
     * reshard candidate of its own and overwrite the very signal under assertion.
     */
    private MaterializedIndex registerObserverTable() {
        LakeTable table = createLakeTableForTest();
        Deencapsulation.setField(table, "id", OBSERVER_TABLE_ID);
        Deencapsulation.setField(table, "name", "observer");
        MaterializedIndex index =
                table.getPartition(PARTITION_ID).getDefaultPhysicalPartition().getLatestBaseIndex();
        for (Tablet tablet : index.getTablets()) {
            ((LakeTablet) tablet).setRowCount(OBSERVER_ROWS_PER_TABLET);
        }
        registeredDb.registerTableUnlocked(table);
        return index;
    }

    private static void addTiledTablets(OlapTable table, MaterializedIndex index, long firstTabletId,
                                        long[] tabletSizes) {
        addTiledTablets(MetaUtils.getRangeDistributionColumns(table, index.getMetaId()),
                index, firstTabletId, tabletSizes);
    }

    /**
     * As above, against an explicitly supplied sort key -- for an index whose own sort key cannot be
     * resolved at all, which is the point of the fixture that uses it.
     */
    private static void addTiledTablets(List<Column> sortKeyColumns, MaterializedIndex index,
                                        long firstTabletId, long[] tabletSizes) {
        List<Range<Tuple>> ranges = tileColocateRanges(sortKeyColumns, tabletSizes.length == 4);
        TabletMeta tabletMeta =
                new TabletMeta(SCAN_DB_ID, TABLE_ID, PARTITION_ID, index.getId(), TStorageMedium.HDD, true);
        for (int i = 0; i < tabletSizes.length; i++) {
            LakeTablet tablet = new LakeTablet(firstTabletId + i);
            tablet.setRange(new TabletRange(ranges.get(i)));
            tablet.setDataSize(tabletSizes[i]);
            // A row count as well, so a case can tell "the row-count pass ran" from its zero default.
            tablet.setRowCount(tabletSizes[i]);
            // Fresh by construction, so the merge-freshness walk of the same scan is well defined.
            tablet.setDataSizeUpdateTime(Long.MAX_VALUE);
            index.addTablet(tablet, tabletMeta, false);
        }
    }

    /**
     * Adds a second VISIBLE index whose metaId was never registered with setIndexMeta -- the shape a
     * stale id from a dropped rollup leaves behind. {@code MetaUtils.getRangeDistributionColumns} throws
     * on it, so this index's Classifier cannot be CONSTRUCTED. That is a different thing from an index
     * that simply never needed one, and the scan has to tell them apart.
     */
    private static MaterializedIndex addIndexWithUnresolvableMeta(OlapTable table,
                                                                  PhysicalPartition physicalPartition,
                                                                  long[] tabletSizes) {
        MaterializedIndex index = new MaterializedIndex(ORPHANED_INDEX_ID, ORPHANED_INDEX_META_ID,
                MaterializedIndex.IndexState.NORMAL, PhysicalPartition.INVALID_SHARD_GROUP_ID);
        physicalPartition.createRollupIndex(index);
        // Tiled against the BASE sort key: this index's own is unresolvable by construction, and the
        // ranges are immaterial anyway since classification never gets as far as reading them.
        addTiledTablets(MetaUtils.getRangeDistributionColumns(table, INDEX_ID), index, 300L, tabletSizes);
        return index;
    }

    private static void mockColocateTableIndex(ColocateState colocateState) {
        ColocateTableIndex.GroupId groupId = new ColocateTableIndex.GroupId(SCAN_DB_ID, COLOCATE_GROUP_ID);
        ColocateGroupSchema groupSchema = new ColocateGroupSchema(groupId, List.of(COLOCATE_COLUMN), 0,
                (short) 1, DistributionInfo.DistributionInfoType.RANGE);
        new MockUp<ColocateTableIndex>() {
            @Mock
            public ColocateTableIndex.GroupId getRangeColocateGroupId(long tableId) {
                // UNSTABLE still hands back a real GroupId: the group exists and the ranges are readable,
                // it is only the topology that is mid-flight. A mock that answered null here would test
                // the not-colocate path instead of the one it means to.
                return colocateState == ColocateState.NOT_COLOCATE ? null : groupId;
            }

            @Mock
            public List<ColocateRange> getColocateRanges(long colocateGroupId) {
                return COLOCATE_RANGES;
            }

            @Mock
            public ColocateGroupSchema getGroupSchema(ColocateTableIndex.GroupId id) {
                return groupSchema;
            }

            @Mock
            public boolean isAnyGroupWithSameColocateGroupIdUnstable(long colocateGroupId) {
                return colocateState == ColocateState.UNSTABLE;
            }
        };
    }

    @Test
    public void testMergeSignalSkipsCrossColocateBoundaryPair() {
        // 2 compute nodes -> parallelism floor 2, and every index below holds more than two tablets, so
        // eligibleForMerge is true: what these assertions read is the colocate classification and not
        // the parallelism floor swallowing the signal.
        runScan(true, 2, () -> registerColocateTable(ColocateState.STABLE, new long[] {1L, 2L, 4L}, null));
        assertEquals(Long.MAX_VALUE, capturedMinAdjacentPairSize,
                "one tablet per ColocateRange: every adjacent pair crosses a boundary, so no merge signal");

        // The same table with R1 carved in two. 2 + 4 is neither the smallest pair the walk sees
        // (1 + 2 crosses into R1) nor the last one (4 + 8 crosses out of it), so a boundary-blind walk
        // cannot produce it.
        runScan(true, 2, () -> registerColocateTable(ColocateState.STABLE, new long[] {1L, 2L, 4L, 8L}, null));
        assertEquals(6L, capturedMinAdjacentPairSize,
                "the pair inside R1 is the one actionable pair and must be the signal");
    }

    @Test
    public void testMergeSignalUsesPerIndexSortKey() {
        // The base index holds one tablet per range and so offers no pair; the rollup carries an
        // actionable within-R1 pair with a crossing pair on either side of it. The rollup's sort key is
        // one column shorter, so its tablet bounds are narrower than the BASE index's expansion of the
        // same ColocateRange: classifying it against the base sort key rejects the R1 lower half as
        // uncontained and loses the pair.
        runScan(true, 2, () -> registerColocateTable(ColocateState.STABLE,
                new long[] {100L, 200L, 400L}, new long[] {1L, 2L, 4L, 8L}));
        assertEquals(6L, capturedMinAdjacentPairSize,
                "the rollup's within-range pair must be classified against the rollup's own sort key");
    }

    @Test
    public void testMergeSignalUnchangedForNonColocateTable() {
        // The fixture of the second case above, minus the colocate group. Nothing separates the ranges
        // any more, so the smallest adjacent pair wins -- the pre-colocate behavior, and the proof that
        // the two cases above are about the classification rather than about the fixture.
        runScan(true, 2, () -> registerColocateTable(ColocateState.NOT_COLOCATE, new long[] {1L, 2L, 4L, 8L}, null));
        assertEquals(3L, capturedMinAdjacentPairSize,
                "a non-colocate table must still signal its smallest adjacent pair");
    }

    @Test
    public void testMergeSignalWithheldWhileTheColocateGroupIsUnstable() {
        // The group exists and its ranges read fine; only the topology is mid-flight. Signalling here
        // would be worse than silence: MergeTabletJobFactory refuses to plan while any peer is unstable,
        // so the candidate would be rebuilt and rejected on every scan for as long as alignment takes.
        runScan(true, 2, () -> registerColocateTable(ColocateState.UNSTABLE, new long[] {1L, 2L, 4L, 8L}, null));
        assertEquals(Long.MAX_VALUE, capturedMinAdjacentPairSize,
                "an unstable colocate group must withhold the merge signal, not fall back to blind pairing");

        // Not vacuous: the SAME tablets with the group stable do produce the R1 pair. Note what this
        // pins down -- an implementation that treated "unstable" as "not colocate" would answer 3 above
        // (the boundary-crossing 1 + 2), not Long.MAX_VALUE and not the 6 below.
        runScan(true, 2, () -> registerColocateTable(ColocateState.STABLE, new long[] {1L, 2L, 4L, 8L}, null));
        assertEquals(6L, capturedMinAdjacentPairSize,
                "the same fixture with a stable group must signal its within-range pair");
    }

    @Test
    public void testMergeSignalWithheldWhenTabletClassificationThrows() {
        MaterializedIndex[] indexes = new MaterializedIndex[2];
        runScan(true, 2, () -> {
            indexes[0] = registerColocateTable(ColocateState.STABLE, new long[] {1L, 2L, 4L, 8L}, null)
                    .getLatestBaseIndex();
            // Deliberately the LAST tablet: the walk has already banked the good R1 pair by then, so what
            // this pins is that the banked signal is DISCARDED, not merely that the walk stopped early.
            indexes[0].getTablets().get(3).setRange(rangeTooShortForColocatePrefix());
            indexes[1] = registerObserverTable();
        });

        assertEquals(Long.MAX_VALUE, capturedMinAdjacentPairSize,
                "an index whose classification blew up must contribute no merge signal at all");
        // The point of catching inside the tablet walk: the scan's per-table loop has no catch of its
        // own, so an escaping exception would cost this table AND every table after it its row counts.
        assertEquals(15L, indexes[0].getRowCount(),
                "the failing index must still get its row counts (1 + 2 + 4 + 8)");
        assertEquals(OBSERVER_TABLE_ID, registeredDb.getTables().get(1).getId(),
                "harness precondition: the observer must be walked AFTER the failing table");
        assertEquals(2 * OBSERVER_ROWS_PER_TABLET, indexes[1].getRowCount(),
                "a table walked after the failing one must still get its row counts");
    }

    @Test
    public void testMergeSignalIsTheMinimumAcrossIndexes() {
        // Two visible indexes with distinct adjacent-pair minima, 30 and 3. Run both arrangements: an
        // implementation that OVERWRITES the table minimum per index instead of folding it with Math.min
        // answers whichever index it happens to walk last, so it cannot survive both orderings whatever
        // that order is. Non-colocate on purpose -- this is about the fold, not the classification.
        runScan(true, 2, () -> registerColocateTable(ColocateState.NOT_COLOCATE,
                new long[] {10L, 20L, 100L}, new long[] {1L, 2L, 100L}));
        assertEquals(3L, capturedMinAdjacentPairSize,
                "the table signal is the smallest adjacent pair over ALL indexes (small pair in the rollup)");

        runScan(true, 2, () -> registerColocateTable(ColocateState.NOT_COLOCATE,
                new long[] {1L, 2L, 100L}, new long[] {10L, 20L, 100L}));
        assertEquals(3L, capturedMinAdjacentPairSize,
                "same, with the small pair in the base index instead");
    }

    @Test
    public void testOneUnclassifiableIndexWithholdsTheWholeTableSignal() {
        // Two range-colocate indexes: the base classifies cleanly and earns a legal within-R1 pair of
        // 60, the rollup blows up on one tablet. Both scans below share the fixture; only the second
        // corrupts a tablet.
        runScan(true, 2, () -> registerColocateTable(ColocateState.STABLE,
                new long[] {10L, 20L, 40L, 80L}, new long[] {1L, 2L, 4L, 8L}));
        assertEquals(6L, capturedMinAdjacentPairSize,
                "sanity: intact, both indexes classify and the table signal is min(60, 6)");

        runScan(true, 2, () -> registerColocateTable(ColocateState.STABLE,
                        new long[] {10L, 20L, 40L, 80L}, new long[] {1L, 2L, 4L, 8L})
                .getLatestIndex(ROLLUP_INDEX_ID).getTablets().get(3)
                .setRange(rangeTooShortForColocatePrefix()));
        // The base index's 60 is a perfectly good signal and it must STILL be withheld: the factory
        // walks every visible index, would hit the same bad tablet in the rollup and reject the whole
        // job, so emitting the base index's pair only rebuilds a doomed candidate on every scan.
        assertEquals(Long.MAX_VALUE, capturedMinAdjacentPairSize,
                "one unclassifiable index must withhold the whole table's merge signal, not just its own");
    }

    @Test
    public void testAnIndexBelowTheFloorDoesNotWithholdASiblingsSignal() {
        // 3 compute nodes -> parallelism floor 3. The base index holds exactly 3 tablets, so it is NOT
        // eligible for merge; the rollup holds 4 and is. An index below the floor is skipped on purpose
        // and therefore never gets a classifier -- but "was not classified" is not "could not be
        // classified", and reading it as the latter suppresses the rollup's perfectly legal pair.
        PhysicalPartition[] partition = new PhysicalPartition[1];
        runScan(true, 3, () -> partition[0] = registerColocateTable(ColocateState.STABLE,
                new long[] {10L, 20L, 40L}, new long[] {1L, 2L, 4L, 8L}));

        // Assert the eligibility split rather than trusting it: derived from the same floor function and
        // the same pinned split cap the scan used, so this cannot quietly stop being the case it claims.
        int parallelismFloor = TabletReshardUtils.parallelismFloor(3, PINNED_MAX_SPLIT_COUNT);
        assertTrue(partition[0].getLatestBaseIndex().getTablets().size() <= parallelismFloor,
                "precondition: the base index must sit AT or below the parallelism floor");
        assertTrue(partition[0].getLatestIndex(ROLLUP_INDEX_ID).getTablets().size() > parallelismFloor,
                "precondition: the rollup must sit above the parallelism floor");

        assertEquals(6L, capturedMinAdjacentPairSize,
                "a floor-suppressed index must not withhold the signal an eligible sibling earned");
    }

    @Test
    public void testAnIneligibleIndexWithAnUnbuildableClassifierWithholdsTheWholeSignal() {
        // 3 compute nodes -> parallelism floor 3. The base index is eligible (4 tablets) and earns a
        // legal within-R1 pair of 6. The second index is AT the floor and so contributes no pair of its
        // own -- but its classifier cannot be BUILT, and MergeTabletJobFactory builds one for every
        // visible index before it consults the merge budget. It would hit the same failure and reject
        // the whole job, so the base index's good pair has to be withheld rather than emitted.
        PhysicalPartition[] partition = new PhysicalPartition[1];
        MaterializedIndex[] orphaned = new MaterializedIndex[1];
        runScan(true, 3, () -> {
            partition[0] = registerColocateTable(ColocateState.STABLE, new long[] {1L, 2L, 4L, 8L}, null);
            orphaned[0] = addIndexWithUnresolvableMeta((OlapTable) registeredDb.getTable(TABLE_ID),
                    partition[0], new long[] {10L, 20L, 40L});
        });

        // Preconditions, asserted rather than assumed: the eligibility split is real, and the second
        // index really is one whose sort key cannot be resolved -- otherwise this would quietly become
        // the mixed-floor case above, which expects the opposite answer.
        int parallelismFloor = TabletReshardUtils.parallelismFloor(3, PINNED_MAX_SPLIT_COUNT);
        assertTrue(orphaned[0].getTablets().size() <= parallelismFloor,
                "precondition: the unbuildable index must sit AT or below the parallelism floor");
        assertTrue(partition[0].getLatestBaseIndex().getTablets().size() > parallelismFloor,
                "precondition: the base index must sit above the parallelism floor");
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> MetaUtils.getRangeDistributionColumns(
                        (OlapTable) registeredDb.getTable(TABLE_ID), ORPHANED_INDEX_META_ID),
                "precondition: the second index's sort key must genuinely be unresolvable");

        assertEquals(Long.MAX_VALUE, capturedMinAdjacentPairSize,
                "an index whose classifier cannot be built withholds the table signal even when it is "
                        + "below the merge floor");
    }
}
