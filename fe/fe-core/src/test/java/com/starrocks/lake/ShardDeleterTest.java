// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.lake;

import com.google.common.collect.Lists;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.Type;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.proto.DeleteTabletRequest;
import com.starrocks.proto.DeleteTabletResponse;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ShardDeleterTest {

    private ShardDeleter shardDeleter = new ShardDeleter();

    @Mocked
    private GlobalStateMgr globalStateMgr;

    @Mocked
    private SystemInfoService systemInfoService;

    @Mocked
    private StarOSAgent starOSAgent;

    @Mocked
    private LakeService lakeService;

    @Mocked
    private Backend be;

    private Set<Long> shardIds = new HashSet<>();

    @Before
    public void setUp() throws Exception {
        long dbId = 1L;
        long tableId = 2L;
        long partitionId = 3L;
        long indexId = 4L;
        long tablet1Id = 10L;
        long tablet2Id = 11L;

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
        db.createTable(table);

        shardIds.add(1001L);
        shardIds.add(1002L);

        be = new Backend(100, "127.0.0.1", 8090);
        new MockUp<GlobalStateMgr>() {
            @Mock
            public SystemInfoService getCurrentSystemInfo() {
                return systemInfoService;
            }

            @Mock
            public StarOSAgent getStarOSAgent() {
                return starOSAgent;
            }

            @Mock
            public List<Long> getDbIdsIncludeRecycleBin() {
                return Stream.of(dbId).collect(Collectors.toList());
            }
            @Mock
            public Database getDbIncludeRecycleBin(long dbId) {
                return db;
            }
            @Mock
            public Table getTablesIncludeRecycleBin(Database db) {
                return table;
            }
            @Mock
            public Partition getAllPartitionsIncludeRecycleBin(OlapTable tbl) {
                return partition;
            }
        };

        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(TNetworkAddress address) {
                return lakeService;
            }
            @Mock
            public LakeService getLakeService(String host, int port) {
                return lakeService;
            }
        };

        new Expectations() {
            {
                starOSAgent.getPrimaryBackendIdByShard(anyLong);
                minTimes = 0;
                result = 1;

                systemInfoService.getBackend(1);
                minTimes = 0;
                result = be;
            }
        };

    }

    @Test
    public void testNormal() throws Exception {

        DeleteTabletResponse response = new DeleteTabletResponse();
        response.failedTablets = new ArrayList<>();

        Set<Long> allShardGroupId = Stream.of(1L, 2L, 3L, 4L, 5L).collect(Collectors.toSet());

        new Expectations() {
            {
                lakeService.deleteTablet((DeleteTabletRequest) any);
                minTimes = 1;
                result = CompletableFuture.completedFuture(response);

                starOSAgent.deleteShards(shardIds);
                minTimes = 1;
                result = null;

                starOSAgent.deleteShardGroup((List<Long>) any);
                minTimes = 1;
                result = null;

                starOSAgent.listShardGroup();
                minTimes = 1;
                result = allShardGroupId;
            }

        };

        shardDeleter.runAfterCatalogReady();
        // Assert.assertEquals(Deencapsulation.getField(shardDeleter, "shardIds"), new HashSet<>());
    }
}
