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


package com.staros.journal;

import com.google.common.collect.ImmutableMap;
import com.staros.common.TestHelper;
import com.staros.common.TestUtils;
import com.staros.credential.AwsCredential;
import com.staros.credential.AwsDefaultCredential;
import com.staros.filestore.S3FileStore;
import com.staros.manager.StarManager;
import com.staros.proto.AwsCredentialInfo;
import com.staros.proto.AwsDefaultCredentialInfo;
import com.staros.proto.CreateMetaGroupInfo;
import com.staros.proto.CreateShardInfo;
import com.staros.proto.CreateShardJournalInfo;
import com.staros.proto.DeleteMetaGroupInfo;
import com.staros.proto.DeleteShardGroupInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;
import com.staros.proto.JoinMetaGroupInfo;
import com.staros.proto.LeaderInfo;
import com.staros.proto.MetaGroupInfo;
import com.staros.proto.MetaGroupJournalInfo;
import com.staros.proto.PlacementPolicy;
import com.staros.proto.ReplicationType;
import com.staros.proto.S3FileStoreInfo;
import com.staros.proto.ShardInfo;
import com.staros.proto.UpdateMetaGroupInfo;
import com.staros.proto.UpdateWorkerGroupInfo;
import com.staros.proto.WarmupLevel;
import com.staros.proto.WorkerGroupSpec;
import com.staros.proto.WorkerInfo;
import com.staros.service.Service;
import com.staros.service.ServiceTemplate;
import com.staros.shard.MetaGroup;
import com.staros.shard.Shard;
import com.staros.shard.ShardGroup;
import com.staros.shard.ShardManager;
import com.staros.shard.ShardManagerTest;
import com.staros.shard.ShardTest;
import com.staros.starlet.StarletAgentFactory;
import com.staros.util.Config;
import com.staros.util.Constant;
import com.staros.worker.Worker;
import com.staros.worker.WorkerGroup;
import com.staros.worker.WorkerManager;
import com.staros.worker.WorkerTest;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JournalReplayerTest {
    private String serviceId;
    private StarManager starManager;
    private JournalReplayer journalReplayer;

    @BeforeClass
    public static void prepareBeforeClass() {
        StarletAgentFactory.AGENT_TYPE = StarletAgentFactory.AgentType.MOCK_STARLET_AGENT;
    }

    @Before
    public void prepare() {
        Config.S3_BUCKET = "test-bucket";
        starManager = new StarManager();
        starManager.becomeLeader();
        String serviceTemplateName = "JournalReplayerTest";
        String serviceName = "JournalReplayerTest-1";
        starManager.registerService(serviceTemplateName, null);
        serviceId = starManager.bootstrapService(serviceTemplateName, serviceName);
        journalReplayer = new JournalReplayer(starManager);
    }

    @After
    public void clean() {
        starManager.becomeFollower();
        Config.S3_BUCKET = "";
    }

    @Test
    public void testReplayShardOperation() {
        long groupId = Constant.DEFAULT_ID;
        long shardId1 = 100;
        long shardId2 = 101;

        // test replay create shard
        Shard shard1 = ShardTest.getTestShard(serviceId, groupId, shardId1);
        Shard shard2 = ShardTest.getTestShard(serviceId, groupId, shardId2);
        List<Shard> shards = new ArrayList<>();
        shards.add(shard1);
        shards.add(shard2);
        CreateShardJournalInfo.Builder builder = CreateShardJournalInfo.newBuilder();
        for (Shard shard : shards) {
            builder.addShardInfos(shard.toProtobuf());
        }
        Journal j = null;
        try {
            j = StarMgrJournal.logCreateShard(serviceId, builder.build());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        journalReplayer.replay(j);
        Assert.assertEquals(shards.size(), starManager.getShardManager(serviceId).getShardCount());

        // test replay update shard
        List<Long> shardIds = shards.stream().map(Shard::getShardId).collect(Collectors.toList());
        List<ShardInfo> shardInfos1 = starManager.getShardManager(serviceId).getShardInfo(shardIds);
        long workerId = 123654;
        Assert.assertTrue(shard1.addReplica(workerId));
        Assert.assertTrue(shard2.addReplica(workerId));
        try {
            j = StarMgrJournal.logUpdateShard(serviceId, shards);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        journalReplayer.replay(j);
        List<ShardInfo> shardInfos2 = starManager.getShardManager(serviceId).getShardInfo(shardIds);
        for (int i = 0; i < shards.size(); ++i) {
            Assert.assertEquals(shardInfos1.get(i).getReplicaInfoList().size() + 1,
                    shardInfos2.get(i).getReplicaInfoList().size());
        }

        // test replay delete shard
        try {
            j = StarMgrJournal.logDeleteShard(serviceId, shardIds);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        journalReplayer.replay(j);
        Assert.assertEquals(0, starManager.getShardManager(serviceId).getShardCount());

        journalReplayer.replay(j);
        Assert.assertEquals(0, starManager.getShardManager(serviceId).getShardCount());
    }

    @Test
    public void testReplayShardGroupOperation() {
        long groupId1 = 6789;
        long groupId2 = 9876;

        // test replay create shard group
        ShardGroup shardGroup1 = new ShardGroup(serviceId, groupId1);
        ShardGroup shardGroup2 = new ShardGroup(serviceId, groupId2);
        List<ShardGroup> shardGroups = new ArrayList<>();
        shardGroups.add(shardGroup1);
        shardGroups.add(shardGroup2);
        Journal j = null;
        try {
            j = StarMgrJournal.logCreateShardGroup(serviceId, shardGroups);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        journalReplayer.replay(j);
        Assert.assertEquals(shardGroups.size() + 1 /* plus default group */,
                starManager.getShardManager(serviceId).getShardGroupCount());

        // add shards to group
        long shardId1 = 1111;
        long shardId2 = 2222;
        Shard shard1 = ShardTest.getTestShard(serviceId, groupId1, shardId1);
        Shard shard2 = ShardTest.getTestShard(serviceId, groupId2, shardId2);
        List<Shard> shards = new ArrayList<>();
        shards.add(shard1);
        shards.add(shard2);
        CreateShardJournalInfo.Builder builder = CreateShardJournalInfo.newBuilder();
        for (Shard shard : shards) {
            builder.addShardInfos(shard.toProtobuf());
        }
        try {
            j = StarMgrJournal.logCreateShard(serviceId, builder.build());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertEquals(0, starManager.getShardManager(serviceId).getShardCount());
        journalReplayer.replay(j);
        Assert.assertEquals(shards.size(), starManager.getShardManager(serviceId).getShardCount());

        // test replay update shard group
        List<ShardGroup> shardGroups1 = new ArrayList<>();
        shardGroups1.add(starManager.getShardManager(serviceId).getShardGroup(groupId1));
        shardGroups1.add(starManager.getShardManager(serviceId).getShardGroup(groupId2));
        try {
            j = StarMgrJournal.logUpdateShardGroup(serviceId, shardGroups1);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        journalReplayer.replay(j);

        // test replay delete shard group
        List<Long> groupIds = shardGroups.stream().map(ShardGroup::getGroupId).collect(Collectors.toList());
        DeleteShardGroupInfo deleteInfo = DeleteShardGroupInfo.newBuilder()
                .addAllGroupIds(groupIds)
                .setCascadeDeleteShard(true)
                .build();
        try {
            j = StarMgrJournal.logDeleteShardGroup(serviceId, deleteInfo);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        journalReplayer.replay(j);
        Assert.assertEquals(1 /* plus default group */, starManager.getShardManager(serviceId).getShardGroupCount());
        // verify shards also deleted
        Assert.assertEquals(0, starManager.getShardManager(serviceId).getShardCount());

        // test replay empty shard group
        DeleteShardGroupInfo deleteInfo2 = DeleteShardGroupInfo.newBuilder()
                .addAllGroupIds(new ArrayList<>())
                .build();
        try {
            j = StarMgrJournal.logDeleteShardGroup(serviceId, deleteInfo2);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        journalReplayer.replay(j);

        // test replay delete non-exist shard group
        journalReplayer.replay(j);
        Assert.assertEquals(1 /* plus default group */, starManager.getShardManager(serviceId).getShardGroupCount());
    }

    @Test
    public void testReplayMetaGroupOperation() {
        // prepare shard and shard group
        // 2 groups, each has 3 shards
        Map<Long, Shard> shards = new HashMap<>();
        List<Long> groupIds1 = new ArrayList<>();
        long groupId1 = 1001;
        long groupId2 = 2001;
        groupIds1.add(groupId1);
        shards.put(10001L, new Shard(serviceId, groupIds1, 10001));
        shards.put(10002L, new Shard(serviceId, groupIds1, 10002));
        shards.put(10003L, new Shard(serviceId, groupIds1, 10003));

        List<Long> groupIds2 = new ArrayList<>();
        groupIds2.add(groupId2);
        shards.put(20001L, new Shard(serviceId, groupIds2, 20001));
        shards.put(20002L, new Shard(serviceId, groupIds2, 20002));
        shards.put(20003L, new Shard(serviceId, groupIds2, 20003));

        List<Long> groupIds = new ArrayList<>();
        groupIds.add(groupId1);
        groupIds.add(groupId2);

        ShardManager shardManager = starManager.getShardManager(serviceId);
        shardManager.overrideShards(shards);

        long metaGroupId = 12345;
        PlacementPolicy placementPolicy = PlacementPolicy.PACK;
        List<Long> anonymousGroupIds = new ArrayList<>();
        anonymousGroupIds.add(3001L);
        anonymousGroupIds.add(3002L);
        anonymousGroupIds.add(3003L);
        MetaGroup metaGroup = new MetaGroup(serviceId, metaGroupId,
                anonymousGroupIds, placementPolicy);

        // test create
        {
            CreateMetaGroupInfo createInfo = CreateMetaGroupInfo.newBuilder()
                    .setMetaGroupId(metaGroupId)
                    .addAllShardGroupIds(groupIds)
                    .setPlacementPolicy(placementPolicy)
                    .build();
            MetaGroupJournalInfo journalInfo = MetaGroupJournalInfo.newBuilder()
                    .setMetaGroupInfo(metaGroup.toProtobuf())
                    .setCreateInfo(createInfo)
                    .build();

            // check everything before replay
            Assert.assertEquals(shardManager.getShardCount(), 6);
            Assert.assertEquals(shardManager.getShardGroupCount(), 2);
            Assert.assertEquals(shardManager.getMetaGroupCount(), 0);

            // write journal and replay
            Journal j = null;
            try {
                j = StarMgrJournal.logCreateMetaGroup(serviceId, journalInfo);
            } catch (Exception e) {
                Assert.fail(e.getMessage());
            }
            journalReplayer.replay(j);

            // check everything after replay
            Assert.assertEquals(shardManager.getShardCount(), 6);
            Assert.assertEquals(shardManager.getShardGroupCount(), 5);
            Assert.assertEquals(shardManager.getMetaGroupCount(), 1);
            MetaGroupInfo metaGroupInfo = shardManager.getMetaGroupInfo(metaGroupId);
            Assert.assertEquals(metaGroupInfo.getServiceId(), metaGroup.getServiceId());
            Assert.assertEquals(metaGroupInfo.getMetaGroupId(), metaGroup.getMetaGroupId());
            Assert.assertEquals(metaGroupInfo.getShardGroupIdsList(), metaGroup.getShardGroupIds());
            Assert.assertEquals(metaGroupInfo.getPlacementPolicy(), metaGroup.getPlacementPolicy());
            ShardManagerTest.verifyMetaGroupAfterAdd(shardManager, metaGroupInfo, groupIds);
        }

        // test delete
        {
            DeleteMetaGroupInfo deleteInfo = DeleteMetaGroupInfo.newBuilder()
                    .setMetaGroupId(metaGroupId)
                    .build();
            MetaGroupJournalInfo journalInfo = MetaGroupJournalInfo.newBuilder()
                    .setDeleteInfo(deleteInfo)
                    .build();
            // write journal and replay
            Journal j = null;
            try {
                j = StarMgrJournal.logDeleteMetaGroup(serviceId, journalInfo);
            } catch (Exception e) {
                Assert.fail(e.getMessage());
            }
            journalReplayer.replay(j);

            // check everything after replay
            Assert.assertEquals(shardManager.getShardCount(), 6);
            Assert.assertEquals(shardManager.getShardGroupCount(), 2);
            Assert.assertEquals(shardManager.getMetaGroupCount(), 0);
            ShardManagerTest.verifyMetaGroupAfterDelete(shardManager, anonymousGroupIds, groupIds);
        }

        // test update
        {
            // create an empty meta group
            CreateMetaGroupInfo createInfo = CreateMetaGroupInfo.newBuilder()
                    .setMetaGroupId(metaGroupId)
                    .setPlacementPolicy(placementPolicy)
                    .build();
            MetaGroupInfo metaGroupInfo = shardManager.createMetaGroup(createInfo);
            MetaGroupInfo.Builder infoBuilder = MetaGroupInfo.newBuilder().mergeFrom(metaGroupInfo);
            infoBuilder.addAllShardGroupIds(anonymousGroupIds);

            JoinMetaGroupInfo joinInfo = JoinMetaGroupInfo.newBuilder()
                    .setMetaGroupId(metaGroupId)
                    .build();
            UpdateMetaGroupInfo updateInfo = UpdateMetaGroupInfo.newBuilder()
                    .setJoinInfo(joinInfo)
                    .addAllShardGroupIds(groupIds)
                    .build();
            MetaGroupJournalInfo journalInfo = MetaGroupJournalInfo.newBuilder()
                    .setMetaGroupInfo(infoBuilder.build())
                    .setUpdateInfo(updateInfo)
                    .build();
            // write journal and replay
            Journal j = null;
            try {
                j = StarMgrJournal.logUpdateMetaGroup(serviceId, journalInfo);
            } catch (Exception e) {
                Assert.fail(e.getMessage());
            }
            journalReplayer.replay(j);

            // check everything after replay
            ShardManagerTest.verifyMetaGroupAfterAdd(shardManager, infoBuilder.build(), groupIds);
        }
    }

    @Test
    public void testReplayWorkerOperation() {
        starManager.getWorkerManager().bootstrapService(serviceId);
        long groupId = TestUtils.createWorkerGroupForTest(starManager.getWorkerManager(), serviceId, 1);
        long workerId = 321;
        String ipPort = "127.0.0.1:9876";

        { // test replay add worker
            Assert.assertEquals(0L, starManager.getWorkerManager().getWorkerCount());

            Worker worker = WorkerTest.getTestWorker(serviceId, groupId, workerId, ipPort);
            Journal journal = StarMgrJournal.logAddWorker(worker);
            journalReplayer.replay(journal);
            Assert.assertEquals(1, starManager.getWorkerManager().getWorkerCount());
        }
        { // test replay update worker
            // Create a new worker, so the updateInfo will not change the worker inside workerManager directly.
            Worker worker = WorkerTest.getTestWorker(serviceId, groupId, workerId, ipPort);
            long sTime = 1357;
            worker.updateInfo(sTime, null, 2);

            Journal journal = StarMgrJournal.logUpdateWorker(serviceId, Collections.singletonList(worker));
            journalReplayer.replay(journal);

            WorkerInfo workerInfo = starManager.getWorkerInfo(serviceId, workerId);
            Assert.assertEquals(sTime, workerInfo.getStartTime());
            Assert.assertNotEquals(worker, starManager.getWorkerManager().getWorker(workerId));
            // field numOfShards not persisted (intended)
            Assert.assertEquals(0L, starManager.getWorkerManager().getWorker(workerId).getNumOfShards());
        }
        { // test replay remove worker
            Journal journal = StarMgrJournal.logRemoveWorker(serviceId, groupId, workerId);

            journalReplayer.replay(journal);
            Assert.assertEquals(0, starManager.getWorkerManager().getWorkerCount());
            // repeat replay remove, doesn't hurt anything
            journalReplayer.replay(journal);
            Assert.assertEquals(0, starManager.getWorkerManager().getWorkerCount());
            Assert.assertEquals(0, starManager.getWorkerManager().getWorkerGroup(serviceId, groupId).getWorkerCount());
        }
    }

    @Test
    public void testReplayWorkerGroupOperation() {
        WorkerManager workerManager = starManager.getWorkerManager();
        workerManager.bootstrapService(serviceId);
        long workerGroupId = 20221228;
        WorkerGroup group = new WorkerGroup(serviceId, workerGroupId);
        group.setWarmupLevel(WarmupLevel.WARMUP_NOTHING);

        // not exist yet
        Assert.assertNull(workerManager.getWorkerGroupNoException(serviceId, workerGroupId));
        { // Test ReplayCreateWorkerGroup
            Journal journal = StarMgrJournal.logCreateWorkerGroup(group);
            journalReplayer.replay(journal);
        }
        // now the workerGroupId exists
        Assert.assertNotNull(workerManager.getWorkerGroupNoException(serviceId, workerGroupId));

        WorkerGroupSpec spec = WorkerGroupSpec.newBuilder().setSize("XXXL").build();
        Map<String, String> labels = ImmutableMap.of("X1", "Y1", "X2", "Y2");
        Map<String, String> props = ImmutableMap.of("p1", "v1", "p2", "v2");
        int replicaNumber = 3;

        { // Test replayUpdateWorkerGroup
            UpdateWorkerGroupInfo updateInfo = UpdateWorkerGroupInfo.newBuilder()
                    .setGroupId(workerGroupId)
                    .setSpec(spec)
                    .putAllLabels(labels)
                    .putAllProperties(props)
                    .setReplicaNumber(replicaNumber)
                    .setReplicationType(ReplicationType.ASYNC)
                    .setWarmupLevel(WarmupLevel.WARMUP_ALL)
                    .build();
            Journal journal = StarMgrJournal.logUpdateWorkerGroup(serviceId, updateInfo);
            journalReplayer.replay(journal);

            WorkerGroup updatedGroup = workerManager.getWorkerGroupNoException(serviceId, workerGroupId);
            Assert.assertEquals(spec, updatedGroup.getSpec());
            Assert.assertEquals(labels, updatedGroup.getLabels());
            Assert.assertEquals(props, updatedGroup.getProperties());
            Assert.assertEquals(replicaNumber, updatedGroup.getReplicaNumber());
            Assert.assertEquals(ReplicationType.ASYNC, updatedGroup.getReplicationType());
            Assert.assertEquals(WarmupLevel.WARMUP_ALL, updatedGroup.getWarmupLevel());
        }

        { // Test deleteWorkerGroup
            // Add two workers to the group
            long workerId1 = starManager.addWorker(serviceId, workerGroupId, TestHelper.generateMockWorkerIpAddress());
            long workerId2 = starManager.addWorker(serviceId, workerGroupId, TestHelper.generateMockWorkerIpAddress());
            WorkerGroup toBeDeleted = workerManager.getWorkerGroupNoException(serviceId, workerGroupId);
            Assert.assertEquals(2L, toBeDeleted.getWorkerCount());
            Assert.assertNotNull(workerManager.getWorker(workerId1));
            Assert.assertNotNull(workerManager.getWorker(workerId2));

            Journal journal = StarMgrJournal.logDeleteWorkerGroup(serviceId, workerGroupId);
            journalReplayer.replay(journal);

            // WorkerGroup Deleted, corresponding workers deleted too.
            Assert.assertNull(workerManager.getWorkerGroupNoException(serviceId, workerGroupId));
            Assert.assertNull(workerManager.getWorker(workerId1));
            Assert.assertNull(workerManager.getWorker(workerId2));
        }
    }

    @Test
    public void testReplayServiceTemplateOperation() {
        String serviceTemplateName = "JournalReplayerTest2";

        // test replay register service
        ServiceTemplate serviceTemplate = new ServiceTemplate(serviceTemplateName, null);
        Journal j = null;
        try {
            j = StarMgrJournal.logRegisterService(serviceTemplate);
        } catch (Exception e) {
            Assert.assertNull(e);
        }
        journalReplayer.replay(j);
        Assert.assertEquals(2, starManager.getServiceManager().getServiceTemplateCount());

        // test replay deregister service
        try {
            j = StarMgrJournal.logDeregisterService(serviceTemplateName);
        } catch (Exception e) {
            Assert.assertNull(e);
        }
        journalReplayer.replay(j);
        Assert.assertEquals(1, starManager.getServiceManager().getServiceTemplateCount());

        journalReplayer.replay(j);
        Assert.assertEquals(1, starManager.getServiceManager().getServiceTemplateCount());
    }

    @Test
    public void testReplayServiceOperation() {
        String serviceTemplateName = "JournalReplayerTest3";
        String serviceName = "JournalReplayerTest3-1";

        ServiceTemplate serviceTemplate = new ServiceTemplate(serviceTemplateName, null);
        Journal j = null;
        try {
            j = StarMgrJournal.logRegisterService(serviceTemplate);
        } catch (Exception e) {
            Assert.assertNull(e);
        }
        journalReplayer.replay(j);

        // test replay bootstrap service
        String sid = "321";
        Service service = new Service(serviceTemplateName, serviceName, sid);
        try {
            j = StarMgrJournal.logBootstrapService(service);
        } catch (Exception e) {
            Assert.assertNull(e);
        }
        journalReplayer.replay(j);
        Assert.assertEquals(2, starManager.getServiceManager().getServiceCount());

        // test default worker group works fine after bootstrap
        String ipPort = "127.0.0.1:1234";
        long groupId = Constant.DEFAULT_ID;
        if (!Config.ENABLE_ZERO_WORKER_GROUP_COMPATIBILITY) {
            groupId = TestUtils.createWorkerGroupForTest(starManager.getWorkerManager(), sid, 1);
        }
        starManager.addWorker(sid, groupId, ipPort);

        // test default shard group works fine after bootstrap
        List<CreateShardInfo> createShardInfos = new ArrayList<>();
        createShardInfos.add(CreateShardInfo.newBuilder().build());
        starManager.createShard(sid, createShardInfos);

        // test replay shutdown service
        try {
            j = StarMgrJournal.logShutdownService(service);
        } catch (Exception e) {
            Assert.assertNull(e);
        }
        journalReplayer.replay(j);
        Assert.assertEquals(2, starManager.getServiceManager().getServiceCount());
    }

    @Test
    public void testReplaySetId() {
        long id = 51315;
        Assert.assertNotEquals(id, starManager.getIdGenerator().getNextId());

        Journal j = null;
        try {
            j = StarMgrJournal.logSetId(id);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        journalReplayer.replay(j);

        Assert.assertEquals(id, starManager.getIdGenerator().getNextId());
    }

    @Test
    public void testReplayFileStoreOperation() {
        // test replay create file store
        AwsCredentialInfo awsCredentialInfo = AwsCredentialInfo.newBuilder()
                .setDefaultCredential(AwsDefaultCredentialInfo.newBuilder().build()).build();
        S3FileStoreInfo s3fsInfo = S3FileStoreInfo.newBuilder().setCredential(awsCredentialInfo)
                .setBucket("test-bucket").setRegion("test-region").setEndpoint("test-endpoint").build();
        FileStoreInfo fileStoreInfo = FileStoreInfo.newBuilder().setFsKey("test-fs-key")
                .setS3FsInfo(s3fsInfo)
                .setFsType(FileStoreType.S3)
                .setFsName("test-fs-name").build();
        Journal j = null;
        try {
            j = StarMgrJournal.logAddFileStore(serviceId, fileStoreInfo);
        } catch (Exception e) {
            Assert.assertNull(e);
        }
        journalReplayer.replay(j);
        Assert.assertNotNull(starManager.getFileStoreMgr(serviceId).getFileStore("test-fs-key"));
        Assert.assertEquals("test-fs-key", starManager.getFileStoreMgr(serviceId)
                .getFileStore("test-fs-key").key());
        Assert.assertEquals(0,
                ((S3FileStore) starManager.getFileStoreMgr(serviceId)
                        .getFileStore("test-fs-key")).getVersion());

        // test replay update file store
        AwsCredential credential = new AwsDefaultCredential();
        S3FileStore s3fs1 = new S3FileStore("test-fs-key", "test-fs-name",
                "test-bucket", "test-region1", "test-endpoint", credential, "");
        FileStoreInfo fileStoreInfo1 = s3fs1.toProtobuf();
        try {
            j = StarMgrJournal.logUpdateFileStore(serviceId, fileStoreInfo1);
        } catch (Exception e) {
            Assert.assertNull(e);
        }
        journalReplayer.replay(j);
        Assert.assertNotNull(starManager.getFileStoreMgr(serviceId).getFileStore("test-fs-key"));
        Assert.assertEquals(FileStoreType.S3, starManager.getFileStoreMgr(serviceId)
                .getFileStore("test-fs-key").type());
        Assert.assertEquals(s3fs1.getRegion(),
                ((S3FileStore) starManager.getFileStoreMgr(serviceId)
                        .getFileStore("test-fs-key")).getRegion());
        Assert.assertEquals(1,
                ((S3FileStore) starManager.getFileStoreMgr(serviceId)
                        .getFileStore("test-fs-key")).getVersion());

        // test replay remove file store
        try {
            j = StarMgrJournal.logRemoveFileStore(serviceId, "test-fs-key");
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        journalReplayer.replay(j);
        Assert.assertNull(starManager.getFileStoreMgr(serviceId).getFileStore("test-fs-key"));
    }

    @Test
    public void testReplayLeaderInfoOperation() {
        LeaderInfo leader = LeaderInfo.newBuilder()
                .setHost("test-host-address")
                .setPort(1122)
                .build();
        Journal journal = null;
        try {
            journal = StarMgrJournal.logLeaderInfo(leader);
        } catch (Exception e) {
            Assert.fail("Unexpected exception: " + e.getMessage());
        }

        journalReplayer.replay(journal);
        try {
            LeaderInfo privateLeaderInfo = (LeaderInfo) FieldUtils.readField(starManager, "leaderInfo", true);
            Assert.assertEquals(leader.toString(), privateLeaderInfo.toString());
        } catch (IllegalAccessException exception) {
            Assert.fail("Unexpected exception: " + exception.getMessage());
        }
    }
}
