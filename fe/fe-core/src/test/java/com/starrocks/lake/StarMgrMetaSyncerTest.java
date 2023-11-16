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
import com.staros.client.StarClientException;
import com.staros.proto.ShardGroupInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StarMgrMetaSyncerTest {

    private StarMgrMetaSyncer starMgrMetaSyncer = new StarMgrMetaSyncer();

    @Mocked
    private GlobalStateMgr globalStateMgr;

    @Mocked
    private SystemInfoService systemInfoService;

    @Mocked
    private StarOSAgent starOSAgent;

    @Before
    public void setUp() throws Exception {
        long dbId = 1L;
        long tableId = 2L;
        long partitionId = 3L;
        long shardGroupId = 12L;

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
                return new Database(dbId, "test");
            }
            @Mock
            public List<Table> getTablesIncludeRecycleBin(Database db) {
                List<Column> baseSchema = new ArrayList<>();
                KeysType keysType = KeysType.AGG_KEYS;
                PartitionInfo partitionInfo = new PartitionInfo(PartitionType.RANGE);
                DistributionInfo defaultDistributionInfo = new HashDistributionInfo();
                Table table = new LakeTable(tableId, "lake_table", baseSchema, keysType, partitionInfo, defaultDistributionInfo);
                List<Table> tableList = new ArrayList<>();
                tableList.add(table);
                return tableList;
            }

            @Mock
            public Collection<Partition> getAllPartitionsIncludeRecycleBin(OlapTable tbl) {
                MaterializedIndex baseIndex = new MaterializedIndex();
                DistributionInfo distributionInfo = new HashDistributionInfo();
                return Lists.newArrayList(new Partition(partitionId, "p1", baseIndex, distributionInfo, shardGroupId));
            }

            @Mock
            public Database getDb(String dbName) {
                return new Database(dbId, dbName);
            }
        };

        new Expectations() {
            {
                starOSAgent.getPrimaryComputeNodeIdByShard(anyLong);
                minTimes = 0;
                result = 1;

                systemInfoService.getBackend(1);
                minTimes = 0;
                result = new Backend(10001, "host1", 1001);
            }
        };

    }

    @Test
    public void testNormal() throws Exception {
        Config.shard_group_clean_threshold_sec = 0;
        List<Long> allShardGroupId = Stream.of(1L, 2L, 3L, 4L, 12L).collect(Collectors.toList());
        // build shardGroupInfos

        List<ShardGroupInfo> shardGroupInfos = new ArrayList<>();
        for (long groupId : allShardGroupId) {
            ShardGroupInfo info = ShardGroupInfo.newBuilder()
                            .setGroupId(groupId)
                            .putProperties("createTime", String.valueOf(System.currentTimeMillis()))
                            .build();
            shardGroupInfos.add(info);
        }

        new MockUp<StarOSAgent>() {
            @Mock
            public void deleteShardGroup(List<Long> groupIds) throws
                    StarClientException {
                allShardGroupId.removeAll(groupIds);
                for (long groupId : groupIds) {
                    shardGroupInfos.removeIf(item -> item.getGroupId() == groupId);
                }
            }
            @Mock
            public List<ShardGroupInfo> listShardGroup() {
                return shardGroupInfos;
            }
        };

        starMgrMetaSyncer.runAfterCatalogReady();
        Assert.assertEquals(1, starOSAgent.listShardGroup().size());
    }

    @Test
    public void testDeleteUnusedWorker() throws Exception {
        new MockUp<SystemInfoService>() {
            @Mock
            public List<Backend> getBackends() {
                List<Backend> backends = new ArrayList<>();
                Backend be1 = new Backend(10001, "host1", 1001);
                be1.setStarletPort(888);
                backends.add(be1);
                Backend be2 = new Backend(10002, "host2", 1002);
                backends.add(be2);
                return backends;
            }
            @Mock
            public List<ComputeNode> getComputeNodes() {
                List<ComputeNode> computeNodes = new ArrayList<>();
                ComputeNode cn1 = new ComputeNode(10003, "host3", 1003);
                cn1.setStarletPort(999);
                computeNodes.add(cn1);
                ComputeNode cn2 = new ComputeNode(10004, "host4", 1004);
                computeNodes.add(cn2);
                return computeNodes;
            }
        };
        new MockUp<StarOSAgent>() {
            @Mock
            public List<String> listDefaultWorkerGroupIpPort() {
                List<String> addresses = new ArrayList<>();
                addresses.add("host0:777");
                addresses.add("host1:888");
                addresses.add("host3:999");
                addresses.add("host5:1000");
                return addresses;
            }
        };

        Assert.assertEquals(2, starMgrMetaSyncer.deleteUnusedWorker());
    }

    @Test
    public void testSyncTabletMetaDbNotExist() throws Exception {
        new MockUp<GlobalStateMgr>() {
            @Mock
            public Database getDb(String dbName) {
                return null;
            }
        };

        Exception exception = Assertions.assertThrows(DdlException.class, () -> {
            starMgrMetaSyncer.syncTableMeta("db", "table", true);
        });
    }

    @Test
    public void testSyncTabletMetaTableNotExist() throws Exception {
        new MockUp<GlobalStateMgr>() {
            @Mock
            public Database getDb(String dbName) {
                return new Database(100, dbName);
            }
        };

        new MockUp<Database>() {
            @Mock
            public Table getTable(String tableName) {
                return null;
            }
        };

        Exception exception = Assertions.assertThrows(DdlException.class, () -> {
            starMgrMetaSyncer.syncTableMeta("db", "table", true);
        });
    }

    @Test
    public void testSyncTabletMeta() throws Exception {
        List<Long> shards = new ArrayList<>();
        shards.add(111L);
        shards.add(222L);
        shards.add(333L);

        new MockUp<GlobalStateMgr>() {
            @Mock
            public Database getDb(String dbName) {
                return new Database(100, dbName);
            }
        };

        new MockUp<Database>() {
            @Mock
            public Table getTable(String tableName) {
                List<Column> baseSchema = new ArrayList<>();
                KeysType keysType = KeysType.AGG_KEYS;
                PartitionInfo partitionInfo = new PartitionInfo(PartitionType.RANGE);
                DistributionInfo defaultDistributionInfo = new HashDistributionInfo();
                Table table = new LakeTable(1000, tableName, baseSchema, keysType, partitionInfo, defaultDistributionInfo);
                return table;
            }
        };

        new MockUp<OlapTable>() {
            @Mock
            public Collection<Partition> getAllPartitions() {
                List<Partition> partitions = new ArrayList<>();
                DistributionInfo defaultDistributionInfo = new HashDistributionInfo();
                MaterializedIndex baseIndex = new MaterializedIndex();
                partitions.add(new Partition(2000, "aaa", baseIndex, defaultDistributionInfo));
                return partitions;
            }
        };

        new MockUp<MaterializedIndex>() {
            @Mock
            public List<Tablet> getTablets() {
                List<Tablet> tablets = new ArrayList<>();
                tablets.add(new LakeTablet(111));
                tablets.add(new LakeTablet(222));
                tablets.add(new LakeTablet(333));
                return tablets;
            }
        };

        new MockUp<Partition>() {
            @Mock
            public long getShardGroupId() {
                return 444;
            }
        };

        new MockUp<StarOSAgent>() {
            @Mock
            public List<Long> listShard(long groupId) {
                return shards;
            }

            @Mock
            public void deleteShards(List<Long> shardIds) {
                shards.removeAll(shardIds);
            }
        };

        starMgrMetaSyncer.syncTableMeta("db", "table", true);
        Assert.assertEquals(0, shards.size());
    }
}
