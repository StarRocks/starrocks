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
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.thrift.TTabletStat;
import com.starrocks.thrift.TTabletStatResult;
import com.starrocks.thrift.TTabletType;
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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class TabletStatMgrTest {
    @Test
    public void testUpdateLocalTabletStat(@Mocked GlobalStateMgr globalStateMgr, @Mocked Utils utils,
                                          @Mocked SystemInfoService systemInfoService) {
        long dbId = 1L;
        long tableId = 2L;
        long partitionId = 3L;
        long indexId = 4L;
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
        TabletMeta tabletMeta2 = new TabletMeta(dbId, tableId, partitionId, indexId, 0, TStorageMedium.HDD);
        invertedIndex.addTablet(tablet2Id, tabletMeta2);
        Replica replica = new Replica(tablet2Id + 1, backendId, 0, Replica.ReplicaState.NORMAL);
        invertedIndex.addReplica(tablet2Id, replica);

        // Partition info and distribution info
        DistributionInfo distributionInfo = new HashDistributionInfo(10, Lists.newArrayList(k1));
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(partitionId, new DataProperty(TStorageMedium.HDD));
        partitionInfo.setIsInMemory(partitionId, false);
        partitionInfo.setTabletType(partitionId, TTabletType.TABLET_TYPE_DISK);
        partitionInfo.setReplicationNum(partitionId, (short) 3);

        // Table
        MaterializedIndex index = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);
        Partition partition = new Partition(partitionId, "p1", index, distributionInfo);
        OlapTable table = new OlapTable(tableId, "t1", columns, KeysType.AGG_KEYS, partitionInfo, distributionInfo);
        Deencapsulation.setField(table, "baseIndexId", indexId);
        table.addPartition(partition);
        table.setIndexMeta(indexId, "t1", columns, 0, 0, (short) 3, TStorageType.COLUMN, KeysType.AGG_KEYS);

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
                GlobalStateMgr.getCurrentInvertedIndex();
                result = invertedIndex;
            }};

        // Check
        TabletStatMgr tabletStatMgr = new TabletStatMgr();
        Deencapsulation.invoke(tabletStatMgr, "updateLocalTabletStat", backendId, result);

        Assert.assertEquals(200L, replica.getDataSize());
        Assert.assertEquals(201L, replica.getRowCount());
    }

    @Test
    public void testUpdateLakeTabletStat(@Mocked SystemInfoService systemInfoService,
                                         @Mocked LakeService lakeService) {
        long dbId = 1L;
        long tableId = 2L;
        long partitionId = 3L;
        long indexId = 4L;
        long tablet1Id = 10L;
        long tablet2Id = 11L;
        long tablet1NumRows = 20L;
        long tablet2NumRows = 21L;
        long tablet1DataSize = 30L;
        long tablet2DataSize = 31L;

        // Schema
        List<Column> columns = Lists.newArrayList();
        Column k1 = new Column("k1", Type.INT, true, null, "", "");
        columns.add(k1);
        columns.add(new Column("k2", Type.BIGINT, true, null, "", ""));
        columns.add(new Column("v", Type.BIGINT, false, AggregateType.SUM, "0", ""));

        // Tablet
        Tablet tablet1 = new LakeTablet(tablet1Id);
        Tablet tablet2 = new LakeTablet(tablet2Id);

        // Index
        MaterializedIndex index = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);
        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, 0, TStorageMedium.HDD, true);
        index.addTablet(tablet1, tabletMeta);
        index.addTablet(tablet2, tabletMeta);

        // Partition
        DistributionInfo distributionInfo = new HashDistributionInfo(10, Lists.newArrayList(k1));
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setReplicationNum(partitionId, (short) 3);
        Partition partition = new Partition(partitionId, "p1", index, distributionInfo);
        partition.setVisibleVersion(2L, System.currentTimeMillis());

        // Lake table
        LakeTable table = new LakeTable(tableId, "t1", columns, KeysType.AGG_KEYS, partitionInfo, distributionInfo);
        Deencapsulation.setField(table, "baseIndexId", indexId);
        table.addPartition(partition);
        table.setIndexMeta(indexId, "t1", columns, 0, 0, (short) 3, TStorageType.COLUMN, KeysType.AGG_KEYS);

        // db
        Database db = new Database(dbId, "db");
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
        };
        new Expectations() {
            {
                systemInfoService.getBackendOrComputeNode(anyLong);
                result = new Backend(1000L, "", 123);

                lakeService.getTabletStats((TabletStatRequest) any);
                minTimes = 1;
                maxTimes = 1;
                result = new Future<TabletStatResponse>() {
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

        TabletStatMgr tabletStatMgr = new TabletStatMgr();
        Deencapsulation.invoke(tabletStatMgr, "updateLakeTableTabletStat", db, table);

        Assert.assertEquals(tablet1.getRowCount(-1), tablet1NumRows);
        Assert.assertEquals(tablet1.getDataSize(true), tablet1DataSize);
        Assert.assertEquals(tablet2.getRowCount(-1), tablet2NumRows);
        Assert.assertEquals(tablet2.getDataSize(true), tablet2DataSize);
        Map<Long, Long> partitionToUpdatedVersion =
                Deencapsulation.getField(tabletStatMgr, "partitionToUpdatedVersion");
        Assert.assertEquals(2L, (long) partitionToUpdatedVersion.get(partitionId));
    }
}