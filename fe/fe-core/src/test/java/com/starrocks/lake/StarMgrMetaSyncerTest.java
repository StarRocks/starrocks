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
import com.staros.proto.FilePathInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;
import com.staros.proto.S3FileStoreInfo;
import com.staros.proto.ShardGroupInfo;
import com.staros.proto.ShardInfo;
import com.staros.proto.StatusCode;
import com.starrocks.alter.AlterJobV2;
import com.starrocks.alter.MaterializedViewHandler;
import com.starrocks.alter.SchemaChangeHandler;
import com.starrocks.alter.SchemaChangeJobV2;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.ColocateTableIndex.GroupId;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.concurrent.lock.LockManager;
import com.starrocks.lake.snapshot.ClusterSnapshotJob;
import com.starrocks.lake.snapshot.ClusterSnapshotJob.ClusterSnapshotJobState;
import com.starrocks.lake.snapshot.ClusterSnapshotMgr;
import com.starrocks.lake.snapshot.ClusterSnapshotUtils;
import com.starrocks.metric.MetricRepo;
import com.starrocks.persist.EditLog;
import com.starrocks.proto.DeleteTabletRequest;
import com.starrocks.proto.DeleteTabletResponse;
import com.starrocks.proto.StatusPB;
import com.starrocks.pseudocluster.PseudoBackend;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.NodeMgr;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.transaction.GtidGenerator;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.ComputeResource;
import com.starrocks.warehouse.cngroup.WarehouseComputeResourceProvider;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StarMgrMetaSyncerTest {
    private static final Logger LOG = LogManager.getLogger(StarMgrMetaSyncerTest.class);

    @Mocked
    private GlobalStateMgr globalStateMgr;

    @Mocked
    private NodeMgr nodeMgr;

    @Mocked
    private SystemInfoService systemInfoService;

    @Mocked
    private StarOSAgent starOSAgent;

    @Mocked
    private ColocateTableIndex colocateTableIndex;

    @Mocked
    private LocalMetastore localMetastore;

    @Mocked
    private EditLog editLog;

    private ClusterSnapshotMgr clusterSnapshotMgr = new ClusterSnapshotMgr();

    private StarMgrMetaSyncer starMgrMetaSyncer;

    long shardGroupId = 12L;

    long tableId = 15L;

    private AtomicLong nextId = new AtomicLong(0);

    private long originalCleanConfigValue;

    @BeforeEach
    public void setUp() throws Exception {
        originalCleanConfigValue = Config.shard_group_clean_threshold_sec;
        long dbId = 1L;
        long tableId = 2L;
        long partitionId = 3L;
        long physicalPartitionId = 4L;
        long indexId = 5L;


        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;
            }
        };

        new Expectations(globalStateMgr) {
            {
                globalStateMgr.getNodeMgr();
                minTimes = 0;
                result = nodeMgr;

                globalStateMgr.getLocalMetastore();
                minTimes = 0;
                result = localMetastore;

                globalStateMgr.getStarOSAgent();
                minTimes = 0;
                result = starOSAgent;

                globalStateMgr.getLockManager();
                minTimes = 0;
                result = new LockManager();

                globalStateMgr.getGtidGenerator();
                minTimes = 0;
                result = new GtidGenerator();

                globalStateMgr.getClusterSnapshotMgr();
                minTimes = 0;
                result = clusterSnapshotMgr;
            }
        };

        new Expectations() {
            {
                starOSAgent.getPrimaryComputeNodeIdByShard(anyLong, anyLong);
                minTimes = 0;
                result = 1;

                systemInfoService.getBackend(1);
                minTimes = 0;
                result = new Backend(10001, "host1", 1001);
            }
        };

        new Expectations(nodeMgr) {
            {
                nodeMgr.getClusterInfo();
                minTimes = 0;
                result = systemInfoService;
            }
        };

        new MockUp<LocalMetastore>() {
            @Mock
            public Database getDb(String dbName) {
                return new Database(dbId, dbName);
            }

            @Mock
             public List<Long> getDbIdsIncludeRecycleBin() {
                return Stream.of(dbId).collect(Collectors.toList());
            }

            @Mock
            public ColocateTableIndex getColocateTableIndex() {
                return colocateTableIndex;
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
                baseIndex.setShardGroupId(shardGroupId);
                DistributionInfo distributionInfo = new HashDistributionInfo();
                return Lists.newArrayList(new Partition(partitionId, physicalPartitionId,
                        "p1", baseIndex, distributionInfo));
            }
        };

        UtFrameUtils.mockInitWarehouseEnv();

        // skip all the initialization in MetricRepo
        MetricRepo.hasInit = true;
        starMgrMetaSyncer = new StarMgrMetaSyncer();
    }

    @AfterEach
    public void tearDown() {
        Config.shard_group_clean_threshold_sec = originalCleanConfigValue;
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
                    .putLabels("tableId", String.valueOf(6L))
                    .putLabels("dbId", String.valueOf(66L))
                    .putLabels("partitionId", String.valueOf(666L))
                    .putLabels("indexId", String.valueOf(6666L))
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

            @Mock
            public StarOSAgent.ListShardGroupResult listShardGroup(long startGroupId) {
                return new StarOSAgent.ListShardGroupResult(shardGroupInfos, 0L);
            }
        };

        starMgrMetaSyncer.runAfterCatalogReady();
        Assertions.assertEquals(1, starOSAgent.listShardGroup().size());
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
            public List<String> listWorkerGroupIpPort(long workerGroupId) {
                List<String> addresses = new ArrayList<>();
                addresses.add("host0:777");
                addresses.add("host1:888");
                addresses.add("host3:999");
                addresses.add("host5:1000");
                return addresses;
            }
        };

        Assertions.assertEquals(2, starMgrMetaSyncer.deleteUnusedWorker());
    }

    @Test
    public void testSyncTableMetaDbNotExist() throws Exception {
        new MockUp<GlobalStateMgr>() {
            @Mock
            public Database getDb(String dbName) {
                return null;
            }

            @Mock
            public Database getDb(long dbId) {
                return null;
            }

            @Mock
            public List<Long> getDbIds() {
                return Lists.newArrayList(1000L);
            }
        };

        Exception exception =
                Assertions.assertThrows(DdlException.class, () -> starMgrMetaSyncer.syncTableMeta("db", "table", true));
        starMgrMetaSyncer.syncTableMetaAndColocationInfo();
    }

    @Test
    public void testSyncTableMetaTableNotExist() throws Exception {
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

        Exception exception =
                Assertions.assertThrows(DdlException.class, () -> starMgrMetaSyncer.syncTableMeta("db", "table", true));
    }

    @Test
    @Disabled
    public void testSyncTableMeta() throws Exception {
        long dbId = 100;
        long tableId = 1000;
        List<Long> shards = new ArrayList<>();

        new MockUp<GlobalStateMgr>() {
            @Mock
            public Database getDb(String dbName) {
                return new Database(dbId, dbName);
            }

            @Mock
            public Database getDb(long id) {
                return new Database(id, "aaa");
            }

            @Mock
            public List<Long> getDbIds() {
                return Lists.newArrayList(dbId);
            }
        };

        List<Column> baseSchema = new ArrayList<>();
        KeysType keysType = KeysType.AGG_KEYS;
        PartitionInfo partitionInfo = new PartitionInfo(PartitionType.RANGE);
        DistributionInfo defaultDistributionInfo = new HashDistributionInfo();
        Table table = new LakeTable(tableId, "bbb", baseSchema, keysType, partitionInfo, defaultDistributionInfo);

        new MockUp<Database>() {
            @Mock
            public Table getTable(String tableName) {
                return table;
            }

            @Mock
            public Table getTable(long tableId) {
                return table;
            }

            @Mock
            public List<Table> getTables() {
                return Lists.newArrayList(table);
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

        new MockUp<PhysicalPartition>() {
            @Mock
            public long getShardGroupId() {
                return 444;
            }
        };

        new MockUp<StarOSAgent>() {
            @Mock
            public List<Long> listShard(long groupId) throws DdlException {
                return shards;
            }

            @Mock
            public void deleteShards(Set<Long> shardIds) throws DdlException {
                shards.removeAll(shardIds);
            }
        };

        new MockUp<ColocateTableIndex>() {
            @Mock
            public boolean isLakeColocateTable(long tableId) {
                return true;
            }

            @Mock
            public void updateLakeTableColocationInfo(OlapTable olapTable, boolean isJoin,
                                                      GroupId expectGroupId) throws DdlException {
                return;
            }
        };

        new MockUp<SystemInfoService>() {
            @Mock
            public ComputeNode getBackendOrComputeNode(long nodeId) {
                return null;
            }
        };

        shards.clear();
        shards.add(111L);
        shards.add(222L);
        shards.add(333L);
        starMgrMetaSyncer.syncTableMeta("db", "table", true);
        Assertions.assertEquals(3, shards.size());

        shards.clear();
        shards.add(111L);
        shards.add(222L);
        shards.add(333L);
        shards.add(444L);
        starMgrMetaSyncer.syncTableMetaAndColocationInfo();
        Assertions.assertEquals(3, shards.size());
        Assertions.assertEquals((long) shards.get(0), 111L);
        Assertions.assertEquals((long) shards.get(1), 222L);
        Assertions.assertEquals((long) shards.get(2), 333L);
    }

    @Test
    public void testDeleteTabletsIgnoreInvalidArgumentError() {
        Config.shard_group_clean_threshold_sec = 0;
        long groupIdToClear = shardGroupId + 1;
        List<Long> allShardGroupId = Lists.newArrayList(groupIdToClear);
        // build shardGroupInfos
        List<Long> allShardIds = Stream.of(1000L, 1001L, 1002L, 1003L).collect(Collectors.toList());
        int numOfShards = allShardIds.size();
        List<ShardGroupInfo> shardGroupInfos = new ArrayList<>();
        for (long groupId : allShardGroupId) {
            ShardGroupInfo info = ShardGroupInfo.newBuilder()
                    .setGroupId(groupId)
                    .putLabels("tableId", String.valueOf(6L))
                    .putLabels("dbId", String.valueOf(66L))
                    .putLabels("partitionId", String.valueOf(666L))
                    .putLabels("indexId", String.valueOf(6666L))
                    .putProperties("createTime", String.valueOf(System.currentTimeMillis() - 86400 * 1000))
                    .addAllShardIds(allShardIds)
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
            public StarOSAgent.ListShardGroupResult listShardGroup(long startGroupId) {
                return new StarOSAgent.ListShardGroupResult(shardGroupInfos, 0L);
            }

            @Mock
            public List<Long> listShard(long groupId) throws DdlException {
                if (groupId == groupIdToClear) {
                    return allShardIds;
                } else {
                    return Lists.newArrayList();
                }
            }

            @Mock
            public void deleteShards(Set<Long> shardIds) throws DdlException {
                allShardIds.removeAll(shardIds);
            }
        };

        new MockUp<WarehouseComputeResourceProvider>() {
            @Mock
            public boolean isResourceAvailable(ComputeResource computeResource) {
                return true;
            }
        };

        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(String host, int port) throws RpcException {
                return new PseudoBackend.PseudoLakeService();
            }
        };

        new MockUp<PseudoBackend.PseudoLakeService>() {
            @Mock
            Future<DeleteTabletResponse> deleteTablet(DeleteTabletRequest request) {
                DeleteTabletResponse resp = new DeleteTabletResponse();
                resp.status = new StatusPB();
                resp.status.statusCode = TStatusCode.INTERNAL_ERROR.getValue();
                resp.failedTablets = new ArrayList<>(request.tabletIds);
                return CompletableFuture.completedFuture(resp);
            }
        };
        Deencapsulation.invoke(starMgrMetaSyncer, "deleteUnusedShardAndShardGroup");
        // No shards deleted
        Assertions.assertEquals(numOfShards, allShardIds.size());

        new MockUp<PseudoBackend.PseudoLakeService>() {
            @Mock
            Future<DeleteTabletResponse> deleteTablet(DeleteTabletRequest request) {
                DeleteTabletResponse resp = new DeleteTabletResponse();
                resp.status = new StatusPB();
                resp.status.statusCode = TStatusCode.INVALID_ARGUMENT.getValue();
                resp.failedTablets = new ArrayList<>(request.tabletIds);
                return CompletableFuture.completedFuture(resp);
            }
        };
        Deencapsulation.invoke(starMgrMetaSyncer, "deleteUnusedShardAndShardGroup");
        // can delete the shards, because the error is INVALID_ARGUMENT
        Assertions.assertEquals(0, allShardIds.size());
    }

    @Test
    public void testDeleteTabletWithExceptionNoCleanUp(@Mocked BrpcProxy brpcProxy)
            throws ExecutionException, InterruptedException {
        Config.shard_group_clean_threshold_sec = 0;
        long groupIdToClear = shardGroupId + 1;
        List<Long> allShardGroupId = Lists.newArrayList(groupIdToClear);
        // build shardGroupInfos
        List<Long> allShardIds = Stream.of(1000L, 1001L, 1002L, 1003L).collect(Collectors.toList());
        int numOfShards = allShardIds.size();
        List<ShardGroupInfo> shardGroupInfos = new ArrayList<>();
        for (long groupId : allShardGroupId) {
            ShardGroupInfo info = ShardGroupInfo.newBuilder()
                    .setGroupId(groupId)
                    .putLabels("tableId", String.valueOf(6L))
                    .putLabels("dbId", String.valueOf(66L))
                    .putLabels("partitionId", String.valueOf(666L))
                    .putLabels("indexId", String.valueOf(6666L))
                    .putProperties("createTime", String.valueOf(System.currentTimeMillis() - 86400 * 1000))
                    .addAllShardIds(allShardIds)
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
            public StarOSAgent.ListShardGroupResult listShardGroup(long startGroupId) {
                return new StarOSAgent.ListShardGroupResult(shardGroupInfos, 0L);
            }

            @Mock
            public List<Long> listShard(long groupId) throws DdlException {
                if (groupId == groupIdToClear) {
                    return allShardIds;
                } else {
                    return Lists.newArrayList();
                }
            }

            @Mock
            public void deleteShards(Set<Long> shardIds) throws DdlException {
                allShardIds.removeAll(shardIds);
            }
        };

        new MockUp<WarehouseComputeResourceProvider>() {
            @Mock
            public boolean isResourceAvailable(ComputeResource computeResource) {
                return true;
            }
        };

        PseudoBackend.PseudoLakeService lakeService = new PseudoBackend.PseudoLakeService();
        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(String host, int port) throws RpcException {
                return lakeService;
            }
        };
        new Expectations(lakeService) {
            {
                lakeService.deleteTablet((DeleteTabletRequest) any);
                result = new RpcException("127.0.0.1", "mocked rpc exception");
            }
        };

        Deencapsulation.invoke(starMgrMetaSyncer, "deleteUnusedShardAndShardGroup");
        // No shards deleted
        Assertions.assertEquals(numOfShards, allShardIds.size());


        Future<DeleteTabletResponse> future = new CompletableFuture<>();
        new Expectations(lakeService) {
            {
                lakeService.deleteTablet((DeleteTabletRequest) any);
                result = future;
            }
        };
        new Expectations(future) {
            {
                future.get();
                result = new InterruptedException();
                result = new Exception();
            }
        };
        Deencapsulation.invoke(starMgrMetaSyncer, "deleteUnusedShardAndShardGroup");
        // No shards deleted
        Assertions.assertEquals(numOfShards, allShardIds.size());
    }

    @Test
    public void testDeleteShardAndShardGroupWithForceAndGetShardException() {
        boolean oldValue = Config.meta_sync_force_delete_shard_meta;
        Config.meta_sync_force_delete_shard_meta = true;
        Config.shard_group_clean_threshold_sec = 0;
        long groupIdToClear = shardGroupId + 1;
        // build shardGroupInfos
        List<Long> allShardIds = Stream.of(1000L, 1001L, 1002L, 1003L).collect(Collectors.toList());
        List<ShardGroupInfo> shardGroupInfos = new ArrayList<>();
        ShardGroupInfo info = ShardGroupInfo.newBuilder()
                .setGroupId(groupIdToClear)
                .putLabels("tableId", String.valueOf(6L))
                .putLabels("dbId", String.valueOf(66L))
                .putLabels("partitionId", String.valueOf(666L))
                .putLabels("indexId", String.valueOf(6666L))
                .putProperties("createTime", String.valueOf(System.currentTimeMillis() - 86400 * 1000))
                .addAllShardIds(allShardIds)
                .build();
        shardGroupInfos.add(info);

        // case get shard info failed, should also delete shards
        {
            new MockUp<StarOSAgent>() {
                @Mock
                public StarOSAgent.ListShardGroupResult listShardGroup(long startGroupId) {
                    return new StarOSAgent.ListShardGroupResult(shardGroupInfos, 0L);
                }

                @Mock
                public List<Long> listShard(long groupId) {
                    return allShardIds;
                }

                @Mock
                public void deleteShards(Set<Long> shardIds) {
                    allShardIds.removeAll(shardIds);
                }

                @Mock
                public ShardInfo getShardInfo(long shardId, long workerGroupId) throws StarClientException {
                    throw new StarClientException(StatusCode.INTERNAL, "failed to get shard info");
                }
            };

            Deencapsulation.invoke(starMgrMetaSyncer, "deleteUnusedShardAndShardGroup");
            Assertions.assertEquals(0, allShardIds.size());
        }
        Config.meta_sync_force_delete_shard_meta = oldValue;
    }

    @Test
    public void testDeleteShardAndShardGroupWithForceAndDeleteException() {
        boolean oldValue = Config.meta_sync_force_delete_shard_meta;
        Config.meta_sync_force_delete_shard_meta = true;
        Config.shard_group_clean_threshold_sec = 0;
        long groupIdToClear = shardGroupId + 1;
        // build shardGroupInfos
        List<Long> allShardIds = Stream.of(1000L, 1001L, 1002L, 1003L).collect(Collectors.toList());
        List<ShardGroupInfo> shardGroupInfos = new ArrayList<>();
        ShardGroupInfo info = ShardGroupInfo.newBuilder()
                .setGroupId(groupIdToClear)
                .putLabels("tableId", String.valueOf(6L))
                .putLabels("dbId", String.valueOf(66L))
                .putLabels("partitionId", String.valueOf(666L))
                .putLabels("indexId", String.valueOf(6666L))
                .putProperties("createTime", String.valueOf(System.currentTimeMillis() - 86400 * 1000))
                .addAllShardIds(allShardIds)
                .build();
        shardGroupInfos.add(info);
        // case: delete shards failed, should also delete shards
        {
            new MockUp<StarOSAgent>() {
                @Mock
                public StarOSAgent.ListShardGroupResult listShardGroup(long startGroupId) {
                    return new StarOSAgent.ListShardGroupResult(shardGroupInfos, 0L);
                }

                @Mock
                public List<Long> listShard(long groupId) {
                    return allShardIds;
                }

                @Mock
                public void deleteShards(Set<Long> shardIds) throws DdlException {
                    throw new DdlException("delete shard failed..");
                }

                @Mock
                public ShardInfo getShardInfo(long shardId, long workerGroupId) throws StarClientException {
                    ShardInfo.Builder builder = ShardInfo.newBuilder();
                    builder.setShardId(1000L);

                    FilePathInfo.Builder filePathBuilder = FilePathInfo.newBuilder();
                    FileStoreInfo.Builder fsBuilder = filePathBuilder.getFsInfoBuilder();

                    S3FileStoreInfo.Builder s3FsBuilder = fsBuilder.getS3FsInfoBuilder();
                    s3FsBuilder.setBucket("test-bucket");
                    s3FsBuilder.setRegion("test-region");

                    fsBuilder.setFsType(FileStoreType.S3);
                    fsBuilder.setFsKey("test-bucket");
                    fsBuilder.setS3FsInfo(s3FsBuilder.build());
                    FileStoreInfo fsInfo = fsBuilder.build();

                    filePathBuilder.setFsInfo(fsInfo);
                    filePathBuilder.setFullPath("s3://test-bucket/1/");

                    builder.setFilePath(filePathBuilder);
                    return builder.build();
                }
            };

            Deencapsulation.invoke(starMgrMetaSyncer, "deleteUnusedShardAndShardGroup");
            Assertions.assertEquals(4, allShardIds.size());
        }
        Config.meta_sync_force_delete_shard_meta = oldValue;
    }

    @Test
    public void testDeleteShardAndShardGroupWithForce() {
        boolean oldValue = Config.meta_sync_force_delete_shard_meta;
        Config.meta_sync_force_delete_shard_meta = true;
        Config.shard_group_clean_threshold_sec = 0;
        long groupIdToClear = shardGroupId + 1;
        // build shardGroupInfos
        List<Long> allShardIds = Stream.of(1000L, 1001L, 1002L, 1003L).collect(Collectors.toList());
        List<ShardGroupInfo> shardGroupInfos = new ArrayList<>();
        ShardGroupInfo info = ShardGroupInfo.newBuilder()
                .setGroupId(groupIdToClear)
                .putLabels("tableId", String.valueOf(6L))
                .putLabels("dbId", String.valueOf(66L))
                .putLabels("partitionId", String.valueOf(666L))
                .putLabels("indexId", String.valueOf(6666L))
                .putProperties("createTime", String.valueOf(System.currentTimeMillis() - 86400 * 1000))
                .addAllShardIds(allShardIds)
                .build();
        shardGroupInfos.add(info);
        // iterate 1: delete all shards
        new MockUp<StarOSAgent>() {
            @Mock
            public StarOSAgent.ListShardGroupResult listShardGroup(long startGroupId) {
                return new StarOSAgent.ListShardGroupResult(shardGroupInfos, 0L);
            }

            @Mock
            public List<Long> listShard(long groupId) {
                return allShardIds;
            }

            @Mock
            public void deleteShards(Set<Long> shardIds) {
                allShardIds.removeAll(shardIds);
            }

            @Mock
            public ShardInfo getShardInfo(long shardId, long workerGroupId) throws StarClientException {
                ShardInfo.Builder builder = ShardInfo.newBuilder();
                builder.setShardId(1000L);

                FilePathInfo.Builder filePathBuilder = FilePathInfo.newBuilder();
                FileStoreInfo.Builder fsBuilder = filePathBuilder.getFsInfoBuilder();

                S3FileStoreInfo.Builder s3FsBuilder = fsBuilder.getS3FsInfoBuilder();
                s3FsBuilder.setBucket("test-bucket");
                s3FsBuilder.setRegion("test-region");

                fsBuilder.setFsType(FileStoreType.S3);
                fsBuilder.setFsKey("test-bucket");
                fsBuilder.setS3FsInfo(s3FsBuilder.build());
                FileStoreInfo fsInfo = fsBuilder.build();

                filePathBuilder.setFsInfo(fsInfo);
                filePathBuilder.setFullPath("s3://test-bucket/1/");

                builder.setFilePath(filePathBuilder);
                return builder.build();
            }
        };

        Deencapsulation.invoke(starMgrMetaSyncer, "deleteUnusedShardAndShardGroup");
        Assertions.assertEquals(0, allShardIds.size());


        // iterate 2: delete empty group (shards has been deleted by iterate 1)
        new MockUp<StarOSAgent>() {
            @Mock
            public void deleteShardGroup(List<Long> groupIds) throws StarClientException {
                for (long groupId : groupIds) {
                    shardGroupInfos.removeIf(item -> item.getGroupId() == groupId);
                }
            }
        };
        Deencapsulation.invoke(starMgrMetaSyncer, "deleteUnusedShardAndShardGroup");
        Assertions.assertEquals(0, shardGroupInfos.size());

        Config.meta_sync_force_delete_shard_meta = oldValue;
    }

    @Test
    public void testDeleteShardAndShardGroupWithoutForce() {
        boolean oldValue = Config.meta_sync_force_delete_shard_meta;
        Config.meta_sync_force_delete_shard_meta = false;
        Config.shard_group_clean_threshold_sec = 0;
        long groupIdToClear = shardGroupId + 1;
        // build shardGroupInfos
        List<Long> allShardIds = Stream.of(1000L, 1001L, 1002L, 1003L).collect(Collectors.toList());
        int numOfShards = allShardIds.size();
        List<ShardGroupInfo> shardGroupInfos = new ArrayList<>();
        ShardGroupInfo info = ShardGroupInfo.newBuilder()
                .setGroupId(groupIdToClear)
                .putLabels("tableId", String.valueOf(6L))
                .putLabels("dbId", String.valueOf(66L))
                .putLabels("partitionId", String.valueOf(666L))
                .putLabels("indexId", String.valueOf(6666L))
                .putProperties("createTime", String.valueOf(System.currentTimeMillis() - 86400 * 1000))
                .addAllShardIds(allShardIds)
                .build();
        shardGroupInfos.add(info);

        new MockUp<StarOSAgent>() {
            @Mock
            public void deleteShardGroup(List<Long> groupIds) {
                for (long groupId : groupIds) {
                    shardGroupInfos.removeIf(item -> item.getGroupId() == groupId);
                }
            }

            @Mock
            public StarOSAgent.ListShardGroupResult listShardGroup(long startGroupId) {
                return new StarOSAgent.ListShardGroupResult(shardGroupInfos, 0L);
            }

            @Mock
            public List<Long> listShard(long groupId) {
                if (groupId == groupIdToClear) {
                    return allShardIds;
                } else {
                    return Lists.newArrayList();
                }
            }

            @Mock
            public void deleteShards(Set<Long> shardIds) {
                allShardIds.removeAll(shardIds);
            }
        };

        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(String host, int port) throws RpcException {
                return new PseudoBackend.PseudoLakeService();
            }
        };

        new MockUp<PseudoBackend.PseudoLakeService>() {
            @Mock
            Future<DeleteTabletResponse> deleteTablet(DeleteTabletRequest request) {
                DeleteTabletResponse resp = new DeleteTabletResponse();
                resp.status = new StatusPB();
                resp.status.statusCode = TStatusCode.INTERNAL_ERROR.getValue();
                resp.failedTablets = new ArrayList<>(request.tabletIds);
                return CompletableFuture.completedFuture(resp);
            }
        };
        Deencapsulation.invoke(starMgrMetaSyncer, "deleteUnusedShardAndShardGroup");
        // No shards deleted
        Assertions.assertEquals(numOfShards, allShardIds.size());

        Config.meta_sync_force_delete_shard_meta = oldValue;
    }

    @Test
    public void testSyncTableMetaInternal() throws Exception {
        long dbId = 100;
        long tableId = 1000;
        List<Long> shards = new ArrayList<>();
        Database db = new Database(dbId, "db");

        new MockUp<GlobalStateMgr>() {
            @Mock
            public Database getDb(String dbName) {
                return db;
            }

            @Mock
            public Database getDb(long id) {
                return db;
            }

            @Mock
            public List<Long> getDbIds() {
                return Lists.newArrayList(dbId);
            }
        };

        List<Column> baseSchema = new ArrayList<>();
        KeysType keysType = KeysType.AGG_KEYS;
        PartitionInfo partitionInfo = new PartitionInfo(PartitionType.RANGE);
        DistributionInfo defaultDistributionInfo = new HashDistributionInfo();
        Table table = new LakeTable(tableId, "bbb", baseSchema, keysType, partitionInfo, defaultDistributionInfo);

        new MockUp<Database>() {
            @Mock
            public Table getTable(String tableName) {
                return table;
            }

            @Mock
            public Table getTable(long tableId) {
                return table;
            }

            @Mock
            public List<Table> getTables() {
                return Lists.newArrayList(table);
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

        new MockUp<PhysicalPartition>() {
            @Mock
            public long getShardGroupId() {
                return 444;
            }
        };

        new MockUp<StarOSAgent>() {
            @Mock
            public List<Long> listShard(long groupId) throws DdlException {
                return shards;
            }

            @Mock
            public void deleteShards(Set<Long> shardIds) throws DdlException {
                shards.removeAll(shardIds);
            }
        };

        new MockUp<ColocateTableIndex>() {
            @Mock
            public boolean isLakeColocateTable(long tableId) {
                return true;
            }

            @Mock
            public void updateLakeTableColocationInfo(OlapTable olapTable, boolean isJoin,
                                                      GroupId expectGroupId) throws DdlException {
                return;
            }
        };

        new MockUp<SystemInfoService>() {
            @Mock
            public ComputeNode getBackendOrComputeNode(long nodeId) {
                return null;
            }
        };

        shards.clear();
        shards.add(111L);
        shards.add(222L);
        shards.add(333L);
        shards.add(555L);
        Assertions.assertTrue(starMgrMetaSyncer.syncTableMetaInternal(db, (OlapTable) table, true));
        Assertions.assertEquals(3, shards.size());

        // test aggregator
        new MockUp<LakeAggregator>() {
            @Mock
            public static ComputeNode chooseAggregatorNode(ComputeResource computeResource) {
                return null;
            }
        };
        shards.clear();
        shards.add(111L);
        shards.add(222L);
        shards.add(333L);
        shards.add(555L);
        OlapTable olapTable = (OlapTable) table;
        olapTable.setFileBundling(true);
        Assertions.assertTrue(starMgrMetaSyncer.syncTableMetaInternal(db, olapTable, true));
        Assertions.assertEquals(3, shards.size());

        shards.clear();
        shards.add(111L);
        shards.add(222L);
        shards.add(333L);
        shards.add(555L);
        new MockUp<ClusterSnapshotMgr>() {
            @Mock
            public boolean isMaterializedIndexInClusterSnapshotInfo(
                    long dbId, long tableId, long partId, long physicalPartId, long indexId) {
                return tableId == 4245L;
            }

            @Mock
            public boolean isShardGroupIdInClusterSnapshotInfo(
                    long dbId, long tableId, long partId, long physicalPartId, long shardGroupId) {
                return tableId == 5424L;
            }
        };
        long oldTblId = table.getId();
        table.setId(4245L);
        Assertions.assertTrue(!starMgrMetaSyncer.syncTableMetaInternal(db, (OlapTable) table, true));
        Assertions.assertEquals(4, shards.size());

        table.setId(5424L);
        Assertions.assertTrue(!starMgrMetaSyncer.syncTableMetaInternal(db, (OlapTable) table, true));
        Assertions.assertEquals(4, shards.size());

        table.setId(oldTblId);
    }

    @Test
    public void testSyncerRejectByClusterSnapshot() {
        final ClusterSnapshotMgr localClusterSnapshotMgr = new ClusterSnapshotMgr();
        final StarMgrMetaSyncer syncer = new StarMgrMetaSyncer();

        new MockUp<GlobalStateMgr>() {
            @Mock
            public ClusterSnapshotMgr getClusterSnapshotMgr() {
                return localClusterSnapshotMgr;
            }
        };

        MaterializedViewHandler rollupHandler = new MaterializedViewHandler();
        SchemaChangeHandler schemaChangeHandler = new SchemaChangeHandler();

        new MockUp<GlobalStateMgr>() {
            @Mock
            public EditLog getEditLog() {
                return editLog;
            }

            @Mock
            public SchemaChangeHandler getSchemaChangeHandler() {
                return schemaChangeHandler;
            }

            @Mock
            public MaterializedViewHandler getRollupHandler() {
                return rollupHandler;
            }
        };

        AlterJobV2 alterjob = new SchemaChangeJobV2(1, 2, 100L, "table3", 100000);
        alterjob.setJobState(AlterJobV2.JobState.FINISHED);
        alterjob.setFinishedTimeMs(1000);
        schemaChangeHandler.addAlterJobV2(alterjob);

        ShardGroupInfo info1 = ShardGroupInfo.newBuilder()
                .setGroupId(99L)
                .putLabels("tableId", String.valueOf(6L))
                .putLabels("dbId", String.valueOf(66L))
                .putLabels("partitionId", String.valueOf(666L))
                .putLabels("indexId", String.valueOf(6666L))
                .putProperties("createTime", String.valueOf(System.currentTimeMillis()))
                .build();
        ShardGroupInfo info2 = ShardGroupInfo.newBuilder()
                .setGroupId(100L)
                .putLabels("tableId", String.valueOf(6L))
                .putLabels("dbId", String.valueOf(66L))
                .putLabels("partitionId", String.valueOf(666L))
                .putLabels("indexId", String.valueOf(6666L))
                .putProperties("createTime", String.valueOf(System.currentTimeMillis()))
                .build();
        List<ShardGroupInfo> shardGroupsInfo = new ArrayList();
        shardGroupsInfo.add(info1);
        shardGroupsInfo.add(info2);
        
        new MockUp<LocalMetastore>() {
            @Mock
            public List<Table> getTablesIncludeRecycleBin(Database db) {
                List<Column> baseSchema = new ArrayList<>();
                KeysType keysType = KeysType.AGG_KEYS;
                PartitionInfo partitionInfo = new PartitionInfo(PartitionType.RANGE);
                DistributionInfo defaultDistributionInfo = new HashDistributionInfo();
                Table table = new LakeTable(100, "lake_table", baseSchema, keysType, partitionInfo, defaultDistributionInfo);
                List<Table> tableList = new ArrayList<>();
                tableList.add(table);
                return tableList;
            }
        };
    
        new MockUp<StarOSAgent>() {
            @Mock
            public StarOSAgent.ListShardGroupResult listShardGroup(long startGroupId) {
                return new StarOSAgent.ListShardGroupResult(shardGroupsInfo, 0L);
            }
        };

        new MockUp<ClusterSnapshotMgr>() {
            @Mock
            public boolean isAutomatedSnapshotOn() {
                return true;
            }
        };

        new MockUp<ClusterSnapshotUtils>() {
            @Mock
            public static void clearClusterSnapshotFromRemote(ClusterSnapshotJob job) {
                return;
            }
        };

        new MockUp<GlobalStateMgr>() {
            @Mock
            public long getNextId() {
                long id = nextId.incrementAndGet();
                return id;
            }
        };

        long oldConfig = Config.shard_group_clean_threshold_sec;
        Config.shard_group_clean_threshold_sec = 0;
        syncer.runAfterCatalogReady();
        ClusterSnapshotJob j3 = localClusterSnapshotMgr.createAutomatedSnapshotJob();
        j3.setState(ClusterSnapshotJobState.FINISHED);
        syncer.runAfterCatalogReady();
        ClusterSnapshotJob j4 = localClusterSnapshotMgr.createAutomatedSnapshotJob();
        j4.setState(ClusterSnapshotJobState.FINISHED);
        syncer.runAfterCatalogReady();
        Config.shard_group_clean_threshold_sec = oldConfig;
    }

    @Test
    public void testStarMgrMetaSyncPerformance() {
        // Before the fix: scale: 500k, time cost: 67s
        // After the fix: scale: 1m, time cost: 0.3s
        int baseSize = 1000_000; // 10^6
        String strNow = String.valueOf(System.currentTimeMillis());
        String str4HrsAgo = String.valueOf(System.currentTimeMillis() - 4 * 3600 * 1000L);
        String str2HrsAgo = String.valueOf(System.currentTimeMillis() - 2 * 3600 * 1000L);

        HashSet<Long> groupIdsInFE = new HashSet<>(baseSize + 100);
        for (long i = 1; i <= baseSize; ++i) {
            groupIdsInFE.add(i);
        }

        long offset = 1000;
        // 1. add a few numbers in groupIdsInFE but not in shardGroupInfos list.
        for (long i = baseSize + offset; i < baseSize + offset + 10; ++i) {
            groupIdsInFE.add(i);
        }

        List<ShardGroupInfo> shardGroupInfos = new ArrayList<>(baseSize + 100);
        for (long i = 1; i <= baseSize; ++i) {
            shardGroupInfos.add(
                    ShardGroupInfo.newBuilder().setGroupId(i)
                    .putLabels("tableId", String.valueOf(6L))
                    .putLabels("dbId", String.valueOf(66L))
                    .putLabels("partitionId", String.valueOf(666L))
                    .putLabels("indexId", String.valueOf(6666L))
                    .putProperties("createTime", strNow).build());
        }

        // 2. add a few numbers in shardGroupInfos but not in groupIdsInFE abd the creation timestamp is NOT expired.
        List<Long> shardGroupSet1 = new ArrayList<>();
        offset += 2000;
        for (long i = baseSize + offset; i < baseSize + offset + 10; ++i) {
            shardGroupSet1.add(i);
            shardGroupInfos.add(
                    ShardGroupInfo.newBuilder().setGroupId(i)
                    .putLabels("tableId", String.valueOf(6L))
                    .putLabels("dbId", String.valueOf(66L))
                    .putLabels("partitionId", String.valueOf(666L))
                    .putLabels("indexId", String.valueOf(6666L))
                    .putProperties("createTime", str4HrsAgo).build());
        }

        // 3. add a few numbers in shardGroupInfos but not in groupIdsInFE and the creation timestamp is expired.
        List<Long> shardGroupSet2 = new ArrayList<>();
        offset += 2000;
        for (long i = baseSize + offset; i < baseSize + offset + 10; ++i) {
            shardGroupSet2.add(i);
            shardGroupInfos.add(
                    ShardGroupInfo.newBuilder().setGroupId(i)
                    .putLabels("tableId", String.valueOf(6L))
                    .putLabels("dbId", String.valueOf(66L))
                    .putLabels("partitionId", String.valueOf(666L))
                    .putLabels("indexId", String.valueOf(6666L))
                    .putProperties("createTime", str2HrsAgo).build());
        }

        List<Long> cleanedGroupIds = new ArrayList<>();

        new MockUp<StarOSAgent>() {
            @Mock
            public StarOSAgent.ListShardGroupResult listShardGroup(long startGroupId) {
                return new StarOSAgent.ListShardGroupResult(shardGroupInfos, 0L);
            }
        };

        new MockUp<StarMgrMetaSyncer>() {
            @Mock
            public int deleteUnusedWorker() {
                // DO NOTHING
                return 0;
            }

            @Mock
            public void syncTableMetaAndColocationInfo() {
                // DO NOTHING
            }

            @Mock
            public Set<Long> getAllPartitionShardGroupId() {
                return groupIdsInFE;
            }

            @Mock
            public boolean cleanOneGroup(ComputeResource computeResource, long groupId, StarOSAgent starOSAgent) {
                cleanedGroupIds.add(groupId);
                return false;
            }
        };

        long oldConfValue = Config.shard_group_clean_threshold_sec;
        {
            Config.shard_group_clean_threshold_sec = 3 * 3600; // 3 hours
            cleanedGroupIds.clear();
            // shardGroupSet1 will be expired
            long begin = System.currentTimeMillis();
            starMgrMetaSyncer.runAfterCatalogReady();
            long elapse = System.currentTimeMillis() - begin;
            LOG.warn("The check takes {}ms", elapse);
            Assertions.assertTrue(elapse < 5000, String.format("The check takes %dms.", elapse));
            Assertions.assertFalse(cleanedGroupIds.isEmpty());
            Assertions.assertEquals(new HashSet<>(shardGroupSet1), new HashSet<>(cleanedGroupIds));
        }
        {
            Config.shard_group_clean_threshold_sec = 3600; // 1 hour
            cleanedGroupIds.clear();
            // shardGroupSet1 and shardGroupSet2 will be expired
            long begin = System.currentTimeMillis();
            starMgrMetaSyncer.runAfterCatalogReady();
            long elapse = System.currentTimeMillis() - begin;
            LOG.warn("The check takes {}ms", elapse);
            Assertions.assertTrue(elapse < 5000, String.format("The check takes %dms.", elapse));
            Assertions.assertFalse(cleanedGroupIds.isEmpty());
            HashSet<Long> expectedSet = new HashSet<>(shardGroupSet1);
            expectedSet.addAll(shardGroupSet2);
            Assertions.assertEquals(expectedSet, new HashSet<>(cleanedGroupIds));
        }
        Config.shard_group_clean_threshold_sec = oldConfValue;
    }

    @Test
    public void testShardGroupWontBeCleanDueToDelay() {
        // Scenario:
        //  * 3 shardGroups created 2 seconds ago
        //  * shard_group_clean_threshold_sec = 4 seconds
        //  * getAllPartitionShardGroupId() call takes 3 seconds
        // Don't expect the shardGroups to be expired in this round check.
        String str2secsAgo = String.valueOf(System.currentTimeMillis() - 2 * 1000L);

        int size = 3;
        HashSet<Long> groupIds = new HashSet<>();
        List<ShardGroupInfo> shardGroupInfos = new ArrayList<>(size);
        for (long i = 1; i <= size; ++i) {
            groupIds.add(i);
            shardGroupInfos.add(
                    ShardGroupInfo.newBuilder().setGroupId(i)
                    .putLabels("tableId", String.valueOf(6L))
                    .putLabels("dbId", String.valueOf(66L))
                    .putLabels("partitionId", String.valueOf(666L))
                    .putLabels("indexId", String.valueOf(6666L))
                    .putProperties("createTime", str2secsAgo).build());
        }

        AtomicLong delayMs = new AtomicLong();
        delayMs.set(3 * 1000L);
        List<Long> cleanedGroupIds = new ArrayList<>();

        new MockUp<StarOSAgent>() {
            @Mock
            public StarOSAgent.ListShardGroupResult listShardGroup(long startGroupId) {
                return new StarOSAgent.ListShardGroupResult(shardGroupInfos, 0L);
            }
        };

        new MockUp<StarMgrMetaSyncer>() {
            @Mock
            public int deleteUnusedWorker() {
                // DO NOTHING
                return 0;
            }

            @Mock
            public void syncTableMetaAndColocationInfo() {
                // DO NOTHING
            }

            @Mock
            public Set<Long> getAllPartitionShardGroupId() {
                try {
                    Thread.sleep(delayMs.get());
                } catch (InterruptedException e) {
                    // DO NOTHING
                }
                return new HashSet<>();
            }

            @Mock
            public boolean cleanOneGroup(ComputeResource computeResource, long groupId, StarOSAgent starOSAgent) {
                cleanedGroupIds.add(groupId);
                return false;
            }
        };

        long oldConfValue = Config.shard_group_clean_threshold_sec;
        {
            Config.shard_group_clean_threshold_sec = 4; // 4 seconds
            cleanedGroupIds.clear();
            long begin = System.currentTimeMillis();
            starMgrMetaSyncer.runAfterCatalogReady();
            long elapse = System.currentTimeMillis() - begin;
            Assertions.assertTrue(elapse >= delayMs.get());
            // Nothing cleaned
            Assertions.assertTrue(cleanedGroupIds.isEmpty());
        }
        { // check again. should be expired in this round
            delayMs.set(1);
            Config.shard_group_clean_threshold_sec = 4; // 4 seconds
            cleanedGroupIds.clear();
            long begin = System.currentTimeMillis();
            starMgrMetaSyncer.runAfterCatalogReady();
            long elapse = System.currentTimeMillis() - begin;
            Assertions.assertTrue(elapse >= delayMs.get());
            // All cleaned
            Assertions.assertEquals(new HashSet<>(cleanedGroupIds), groupIds);
        }
        Config.shard_group_clean_threshold_sec = oldConfValue;
    }

    @Test
    public void testPaginationShardGroupListAndCleanUp() {
        long now = System.currentTimeMillis();
        String createTimeExpired = String.valueOf(now - (Config.shard_group_clean_threshold_sec + 3600) * 1000L);
        String createTimeNotExpired = String.valueOf(now);

        // generating [30k, 50k] items
        int size = 20_000 + ThreadLocalRandom.current().nextInt(10_000, 30_000);
        HashSet<Long> groupIds = new HashSet<>();
        HashSet<Long> expiredGroupIds = new HashSet<>();
        HashSet<Long> preservedGroupIds = new HashSet<>();
        TreeMap<Long, ShardGroupInfo> sortedShardGroups = new TreeMap<>();
        long groupId = 0;

        HashSet<Long> expectedCleanedGroupIds = new HashSet<>();
        for (long i = 1; i <= size; ++i) {
            // Generate non-sequential groupIds
            groupId += ThreadLocalRandom.current().nextInt(1, 1000);
            groupIds.add(groupId);

            // 50% chance to be expired
            boolean isExpired = ThreadLocalRandom.current().nextBoolean();
            String createTime = isExpired ? createTimeExpired : createTimeNotExpired;
            if (isExpired) {
                expiredGroupIds.add(groupId);
            }

            // 1/3 chance to be preserved
            boolean isPreserved = (ThreadLocalRandom.current().nextInt() % 3 == 0);
            if (isPreserved) {
                preservedGroupIds.add(groupId);
            }

            if (isExpired && !isPreserved) {
                expectedCleanedGroupIds.add(groupId);
            }
            ShardGroupInfo newItem = ShardGroupInfo.newBuilder().setGroupId(groupId)
                    .putLabels("tableId", String.valueOf(6L))
                    .putLabels("dbId", String.valueOf(66L))
                    .putLabels("partitionId", String.valueOf(666L))
                    .putLabels("indexId", String.valueOf(6666L))
                    .putProperties("createTime", createTime).build();
            sortedShardGroups.put(newItem.getGroupId(), newItem);
        }

        Set<Long> cleanedGroupIds = new HashSet<>();
        AtomicLong groupCounter = new AtomicLong(0);

        new MockUp<StarOSAgent>() {
            int limit = 2;

            @Mock
            public StarOSAgent.ListShardGroupResult listShardGroup(long startGroupId) {
                List<ShardGroupInfo> infos = new ArrayList<>();
                SortedMap<Long, ShardGroupInfo> truncatedSortedShardGroups = sortedShardGroups.tailMap(startGroupId);
                long nextGroupId = 0;
                long lastGroupId = 0;
                for (ShardGroupInfo info : truncatedSortedShardGroups.values()) {
                    if (infos.size() >= limit) {
                        nextGroupId = lastGroupId + 1;
                        break;
                    }
                    lastGroupId = info.getGroupId();
                    infos.add(info);
                }
                limit *= 2;
                if (limit > 1000) {
                    limit = 1000;
                }
                groupCounter.addAndGet(infos.size());
                return new StarOSAgent.ListShardGroupResult(infos, nextGroupId);
            }
        };

        new MockUp<StarMgrMetaSyncer>() {
            @Mock
            public int deleteUnusedWorker() {
                // DO NOTHING
                return 0;
            }

            @Mock
            public void syncTableMetaAndColocationInfo() {
                // DO NOTHING
            }

            @Mock
            public Set<Long> getAllPartitionShardGroupId() {
                return preservedGroupIds;
            }

            @Mock
            public boolean cleanOneGroup(ComputeResource computeResource, long groupId, StarOSAgent starOSAgent) {
                cleanedGroupIds.add(groupId);
                return false;
            }
        };

        // Not all the expired groups should be cleaned, some of them should be in the preservedGroupIds.
        Assertions.assertNotEquals(expiredGroupIds, expectedCleanedGroupIds);

        cleanedGroupIds.clear();
        Assertions.assertEquals(0L, groupCounter.get());
        starMgrMetaSyncer.runAfterCatalogReady();
        // all groups should be counted
        Assertions.assertEquals(groupIds.size(), groupCounter.get());
        // Assertions.assertEquals(expectedCleanedGroupIds.size(), cleanedGroupIds.size());
        // only groups in expectedCleanedGroupIds should be cleaned
        Assertions.assertEquals(expectedCleanedGroupIds, cleanedGroupIds);
    }

    @Test
    public void testDeleteUnusedShardAndShardGroupWithIndexSnapshotInfo() {
        boolean oldValue = Config.meta_sync_force_delete_shard_meta;
        Config.meta_sync_force_delete_shard_meta = false;
        Config.shard_group_clean_threshold_sec = 0;
        long groupIdToClear = shardGroupId + 1;
        // build shardGroupInfos
        List<Long> allShardIds = Stream.of(1000L, 1001L, 1002L, 1003L).collect(Collectors.toList());
        int numOfShards = allShardIds.size();
        List<ShardGroupInfo> shardGroupInfos = new ArrayList<>();
        ShardGroupInfo info = ShardGroupInfo.newBuilder()
                .setGroupId(groupIdToClear)
                .putLabels("tableId", String.valueOf(6L))
                .putLabels("dbId", String.valueOf(66L))
                .putLabels("partitionId", String.valueOf(666L))
                .putLabels("indexId", String.valueOf(6666L))
                .putProperties("createTime", String.valueOf(System.currentTimeMillis() - 86400 * 1000))
                .addAllShardIds(allShardIds)
                .build();
        shardGroupInfos.add(info);

        new MockUp<StarOSAgent>() {
            @Mock
            public void deleteShardGroup(List<Long> groupIds) {
                for (long groupId : groupIds) {
                    shardGroupInfos.removeIf(item -> item.getGroupId() == groupId);
                }
            }

            @Mock
            public StarOSAgent.ListShardGroupResult listShardGroup(long startGroupId) {
                return new StarOSAgent.ListShardGroupResult(shardGroupInfos, 0L);
            }

            @Mock
            public List<Long> listShard(long groupId) {
                if (groupId == groupIdToClear) {
                    return allShardIds;
                } else {
                    return Lists.newArrayList();
                }
            }

            @Mock
            public void deleteShards(Set<Long> shardIds) {
                allShardIds.removeAll(shardIds);
            }
        };

        new Expectations(clusterSnapshotMgr) {
            {
                clusterSnapshotMgr.isMaterializedIndexInClusterSnapshotInfo(anyLong, anyLong, anyLong, anyLong);
                minTimes = 1;
                result = true;
            }
        };

        Deencapsulation.invoke(starMgrMetaSyncer, "deleteUnusedShardAndShardGroup");

        Config.meta_sync_force_delete_shard_meta = oldValue;
    }

    @Test
    public void testDeleteUnusedShardAndShardGroupWithShardGroupIdSnapshotInfo() {
        boolean oldValue = Config.meta_sync_force_delete_shard_meta;
        Config.meta_sync_force_delete_shard_meta = false;
        Config.shard_group_clean_threshold_sec = 0;
        long groupIdToClear = shardGroupId + 1;
        // build shardGroupInfos
        List<Long> allShardIds = Stream.of(1000L, 1001L, 1002L, 1003L).collect(Collectors.toList());
        int numOfShards = allShardIds.size();
        List<ShardGroupInfo> shardGroupInfos = new ArrayList<>();
        ShardGroupInfo info = ShardGroupInfo.newBuilder()
                .setGroupId(groupIdToClear)
                .putLabels("tableId", String.valueOf(6L))
                .putLabels("dbId", String.valueOf(66L))
                .putLabels("partitionId", String.valueOf(666L))
                .putLabels("indexId", String.valueOf(6666L))
                .putProperties("createTime", String.valueOf(System.currentTimeMillis() - 86400 * 1000))
                .addAllShardIds(allShardIds)
                .build();
        shardGroupInfos.add(info);

        new MockUp<StarOSAgent>() {
            @Mock
            public void deleteShardGroup(List<Long> groupIds) {
                for (long groupId : groupIds) {
                    shardGroupInfos.removeIf(item -> item.getGroupId() == groupId);
                }
            }

            @Mock
            public StarOSAgent.ListShardGroupResult listShardGroup(long startGroupId) {
                return new StarOSAgent.ListShardGroupResult(shardGroupInfos, 0L);
            }

            @Mock
            public List<Long> listShard(long groupId) {
                if (groupId == groupIdToClear) {
                    return allShardIds;
                } else {
                    return Lists.newArrayList();
                }
            }

            @Mock
            public void deleteShards(Set<Long> shardIds) {
                allShardIds.removeAll(shardIds);
            }
        };

        new Expectations(clusterSnapshotMgr) {
            {
                clusterSnapshotMgr.isShardGroupIdInClusterSnapshotInfo(anyLong, anyLong, anyLong, anyLong);
                minTimes = 1;
                result = true;
            }
        };

        Deencapsulation.invoke(starMgrMetaSyncer, "deleteUnusedShardAndShardGroup");

        Config.meta_sync_force_delete_shard_meta = oldValue;
    }
}
