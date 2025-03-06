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
import com.starrocks.catalog.KeysType;
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
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.transaction.GtidGenerator;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StarMgrMetaSyncerTest {

    private StarMgrMetaSyncer starMgrMetaSyncer = new StarMgrMetaSyncer();

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

    long shardGroupId = 12L;

    long tableId = 15L;

    private AtomicLong nextId = new AtomicLong(0);

    @Before
    public void setUp() throws Exception {
        long dbId = 1L;
        long tableId = 2L;
        long partitionId = 3L;
        long physicalPartitionId = 4L;


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
                    .putLabels("tableId", String.valueOf(tableId))
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
            public List<String> listWorkerGroupIpPort(long workerGroupId) {
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

        Exception exception = Assertions.assertThrows(DdlException.class, () -> {
            starMgrMetaSyncer.syncTableMeta("db", "table", true);
        });
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

        Exception exception = Assertions.assertThrows(DdlException.class, () -> {
            starMgrMetaSyncer.syncTableMeta("db", "table", true);
        });
    }

    @Test
    @Ignore
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
        Assert.assertEquals(3, shards.size());

        shards.clear();
        shards.add(111L);
        shards.add(222L);
        shards.add(333L);
        shards.add(444L);
        starMgrMetaSyncer.syncTableMetaAndColocationInfo();
        Assert.assertEquals(3, shards.size());
        Assert.assertEquals((long) shards.get(0), 111L);
        Assert.assertEquals((long) shards.get(1), 222L);
        Assert.assertEquals((long) shards.get(2), 333L);
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
                    .putLabels("tableId", String.valueOf(tableId))
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
            public List<ShardGroupInfo> listShardGroup() {
                return shardGroupInfos;
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
        Assert.assertEquals(numOfShards, allShardIds.size());

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
        Assert.assertEquals(0, allShardIds.size());
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
                .putLabels("tableId", String.valueOf(tableId))
                .putProperties("createTime", String.valueOf(System.currentTimeMillis() - 86400 * 1000))
                .addAllShardIds(allShardIds)
                .build();
        shardGroupInfos.add(info);

        // case get shard info failed, should also delete shards
        {
            new MockUp<StarOSAgent>() {
                @Mock
                public List<ShardGroupInfo> listShardGroup() {
                    return shardGroupInfos;
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
            Assert.assertEquals(0, allShardIds.size());
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
                .putLabels("tableId", String.valueOf(tableId))
                .putProperties("createTime", String.valueOf(System.currentTimeMillis() - 86400 * 1000))
                .addAllShardIds(allShardIds)
                .build();
        shardGroupInfos.add(info);
        // case: delete shards failed, should also delete shards
        {
            new MockUp<StarOSAgent>() {
                @Mock
                public List<ShardGroupInfo> listShardGroup() {
                    return shardGroupInfos;
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
            Assert.assertEquals(4, allShardIds.size());
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
                .putLabels("tableId", String.valueOf(tableId))
                .putProperties("createTime", String.valueOf(System.currentTimeMillis() - 86400 * 1000))
                .addAllShardIds(allShardIds)
                .build();
        shardGroupInfos.add(info);
        // iterate 1: delete all shards
        new MockUp<StarOSAgent>() {
            @Mock
            public List<ShardGroupInfo> listShardGroup() {
                return shardGroupInfos;
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
        Assert.assertEquals(0, allShardIds.size());


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
        Assert.assertEquals(0, shardGroupInfos.size());

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
                .putLabels("tableId", String.valueOf(tableId))
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
            public List<ShardGroupInfo> listShardGroup() {
                return shardGroupInfos;
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
        Assert.assertEquals(numOfShards, allShardIds.size());

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
        starMgrMetaSyncer.syncTableMetaInternal(db, (OlapTable) table, true);
        Assert.assertEquals(3, shards.size());
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
                .putLabels("tableId", String.valueOf(100L))
                .putProperties("createTime", String.valueOf(System.currentTimeMillis()))
                .build();
        ShardGroupInfo info2 = ShardGroupInfo.newBuilder()
                .setGroupId(100L)
                .putLabels("tableId", String.valueOf(111L))
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
            public List<ShardGroupInfo> listShardGroup() {
                return shardGroupsInfo;
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
            public static void clearAutomatedSnapshotFromRemote(String snapshotName) {
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
}