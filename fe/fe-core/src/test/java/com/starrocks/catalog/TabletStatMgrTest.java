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
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.thrift.TTabletStat;
import com.starrocks.thrift.TTabletStatResult;
import com.starrocks.thrift.TTabletType;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class TabletStatMgrTest {
    private static final long DB_ID = 1;
    private static final long TABLE_ID = 2;
    private static final long PARTITION_ID = 3;
    private static final long INDEX_ID = 4;

    @Test
    public void testUpdateLocalTabletStat(@Mocked GlobalStateMgr globalStateMgr, @Mocked Utils utils,
                                          @Mocked SystemInfoService systemInfoService) {
        long tablet2Id = 11L;
        long backendId = 20L;
        TabletInvertedIndex invertedIndex = new TabletInvertedIndex();

        // Columns
        List<Column> columns = new ArrayList<Column>();
        Column k1 = new Column("k1", Type.INT, true, null, "", "");
        columns.add(k1);
        columns.add(new Column("k2", Type.BIGINT, true, null, "", ""));
        columns.add(new Column("v", Type.BIGINT, false, AggregateType.SUM, "0", ""));

        // Tablet2 is LocalTablet
        TabletMeta tabletMeta2 = new TabletMeta(DB_ID, TABLE_ID, PARTITION_ID, INDEX_ID, 0, TStorageMedium.HDD);
        invertedIndex.addTablet(tablet2Id, tabletMeta2);
        Replica replica = new Replica(tablet2Id + 1, backendId, 0, Replica.ReplicaState.NORMAL);
        invertedIndex.addReplica(tablet2Id, replica);

        // Partition info and distribution info
        DistributionInfo distributionInfo = new HashDistributionInfo(10, Lists.newArrayList(k1));
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(PARTITION_ID, new DataProperty(TStorageMedium.HDD));
        partitionInfo.setIsInMemory(PARTITION_ID, false);
        partitionInfo.setTabletType(PARTITION_ID, TTabletType.TABLET_TYPE_DISK);
        partitionInfo.setReplicationNum(PARTITION_ID, (short) 3);

        // Table
        MaterializedIndex index = new MaterializedIndex(INDEX_ID, MaterializedIndex.IndexState.NORMAL);
        Partition partition = new Partition(PARTITION_ID, "p1", index, distributionInfo);
        OlapTable table = new OlapTable(TABLE_ID, "t1", columns, KeysType.AGG_KEYS, partitionInfo, distributionInfo);
        Deencapsulation.setField(table, "baseIndexId", INDEX_ID);
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

        Assert.assertEquals(200L, replica.getDataSize());
        Assert.assertEquals(201L, replica.getRowCount());
    }

    private LakeTable createLakeTableForTest() {
        long tablet1Id = 10L;
        long tablet2Id = 11L;
        long tablet3Id = 12L;


        // Schema
        List<Column> columns = Lists.newArrayList();
        Column k1 = new Column("k1", Type.INT, true, null, "", "");
        columns.add(k1);
        columns.add(new Column("k2", Type.BIGINT, true, null, "", ""));
        columns.add(new Column("v", Type.BIGINT, false, AggregateType.SUM, "0", ""));

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
        TabletMeta tabletMeta = new TabletMeta(DB_ID,     TABLE_ID, PARTITION_ID, INDEX_ID, 0, TStorageMedium.HDD, true);
        index.addTablet(tablet1, tabletMeta);
        index.addTablet(tablet2, tabletMeta);

        // Partition
        DistributionInfo distributionInfo = new HashDistributionInfo(10, Lists.newArrayList(k1));
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setReplicationNum(PARTITION_ID, (short) 3);
        Partition partition = new Partition(PARTITION_ID, "p1", index, distributionInfo);
        partition.setVisibleVersion(2L, visibleVersionTime);

        // Lake table
        LakeTable table = new LakeTable(TABLE_ID, "t1", columns, KeysType.AGG_KEYS, partitionInfo, distributionInfo);
        Deencapsulation.setField(table, "baseIndexId", INDEX_ID);
        table.addPartition(partition);
        table.setIndexMeta(INDEX_ID, "t1", columns, 0, 0, (short) 3, TStorageType.COLUMN, KeysType.AGG_KEYS);

        return table;
    }

    @Test
    public void testUpdateLakeTabletStat(@Mocked SystemInfoService systemInfoService,
                                         @Mocked LakeService lakeService) {
        LakeTable table = createLakeTableForTest();

        long tablet1Id = table.getPartition(PARTITION_ID).getBaseIndex().getTablets().get(0).getId();
        long tablet2Id = table.getPartition(PARTITION_ID).getBaseIndex().getTablets().get(1).getId();

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
            public Long chooseBackend(LakeTablet tablet) {
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
                        Assert.assertEquals(LakeService.TIMEOUT_GET_TABLET_STATS, (long) request.timeoutMs);
                        Assert.assertEquals(2, request.tabletInfos.size());
                        Assert.assertEquals(tablet1Id, (long) request.tabletInfos.get(0).tabletId);
                        Assert.assertEquals(tablet2Id, (long) request.tabletInfos.get(1).tabletId);

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

        LakeTablet tablet1 = (LakeTablet) table.getPartition(PARTITION_ID).getBaseIndex().getTablets().get(0);
        LakeTablet tablet2 = (LakeTablet) table.getPartition(PARTITION_ID).getBaseIndex().getTablets().get(1);

        Assert.assertEquals(tablet1.getRowCount(-1), tablet1NumRows);
        Assert.assertEquals(tablet1.getDataSize(true), tablet1DataSize);
        Assert.assertEquals(tablet2.getRowCount(-1), tablet2NumRows);
        Assert.assertEquals(tablet2.getDataSize(true), tablet2DataSize);
        Assert.assertTrue(tablet1.getDataSizeUpdateTime() >= t1 && tablet1.getDataSizeUpdateTime() <= t2);
        Assert.assertTrue(tablet2.getDataSizeUpdateTime() >= t1 && tablet2.getDataSizeUpdateTime() <= t2);
    }

    @Test
    public void testUpdateLakeTabletStat2(@Mocked SystemInfoService systemInfoService,
                                         @Mocked LakeService lakeService) {
        LakeTable table = createLakeTableForTest();

        long tablet1Id = table.getPartition(PARTITION_ID).getBaseIndex().getTablets().get(0).getId();
        long tablet2Id = table.getPartition(PARTITION_ID).getBaseIndex().getTablets().get(1).getId();

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
            public Long chooseBackend(LakeTablet tablet) {
                return 1000L;
            }
            @Mock
            public ComputeNode chooseNode(LakeTablet tablet) {
                return new ComputeNode();
            }
        };

        TabletStatMgr tabletStatMgr = new TabletStatMgr();
        Deencapsulation.invoke(tabletStatMgr, "updateLakeTableTabletStat", db, table);

        LakeTablet tablet1 = (LakeTablet) table.getPartition(PARTITION_ID).getBaseIndex().getTablets().get(0);
        LakeTablet tablet2 = (LakeTablet) table.getPartition(PARTITION_ID).getBaseIndex().getTablets().get(1);

        Assert.assertEquals(0, tablet1.getRowCount(-1));
        Assert.assertEquals(0, tablet1.getDataSize(true));
        Assert.assertEquals(0, tablet2.getRowCount(-1));
        Assert.assertEquals(0, tablet2.getDataSize(true));
        Assert.assertEquals(0L, tablet1.getDataSizeUpdateTime());
        Assert.assertEquals(0L, tablet2.getDataSizeUpdateTime());
    }

    @Test
    public void testUpdateLakeTabletStat3(@Mocked SystemInfoService systemInfoService,
                                         @Mocked LakeService lakeService) {
        LakeTable table = createLakeTableForTest();

        long tablet1Id = table.getPartition(PARTITION_ID).getBaseIndex().getTablets().get(0).getId();
        long tablet2Id = table.getPartition(PARTITION_ID).getBaseIndex().getTablets().get(1).getId();

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
            public Long chooseBackend(LakeTablet tablet) {
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

        LakeTablet tablet1 = (LakeTablet) table.getPartition(PARTITION_ID).getBaseIndex().getTablets().get(0);
        LakeTablet tablet2 = (LakeTablet) table.getPartition(PARTITION_ID).getBaseIndex().getTablets().get(1);

        Assert.assertEquals(0, tablet1.getRowCount(-1));
        Assert.assertEquals(0, tablet1.getDataSize(true));
        Assert.assertEquals(0, tablet2.getRowCount(-1));
        Assert.assertEquals(0, tablet2.getDataSize(true));
        Assert.assertEquals(0L, tablet1.getDataSizeUpdateTime());
        Assert.assertEquals(0L, tablet2.getDataSizeUpdateTime());
    }
}