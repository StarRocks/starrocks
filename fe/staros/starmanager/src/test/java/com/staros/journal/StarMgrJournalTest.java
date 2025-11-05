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
import com.staros.proto.CreateMetaGroupInfo;
import com.staros.proto.CreateShardJournalInfo;
import com.staros.proto.DeleteMetaGroupInfo;
import com.staros.proto.DeleteShardGroupInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;
import com.staros.proto.JournalEntry;
import com.staros.proto.JournalHeader;
import com.staros.proto.LeaderInfo;
import com.staros.proto.MetaGroupJournalInfo;
import com.staros.proto.OperationType;
import com.staros.proto.PlacementPolicy;
import com.staros.proto.ReplicationType;
import com.staros.proto.S3FileStoreInfo;
import com.staros.proto.ShardInfo;
import com.staros.proto.TransferMetaGroupInfo;
import com.staros.proto.UpdateMetaGroupInfo;
import com.staros.proto.UpdateWorkerGroupInfo;
import com.staros.proto.WarmupLevel;
import com.staros.proto.WorkerGroupSpec;
import com.staros.proto.WorkerGroupState;
import com.staros.service.Service;
import com.staros.service.ServiceTemplate;
import com.staros.shard.Shard;
import com.staros.shard.ShardGroup;
import com.staros.shard.ShardTest;
import com.staros.starlet.StarletAgentFactory;
import com.staros.util.ByteWriter;
import com.staros.util.Constant;
import com.staros.worker.Worker;
import com.staros.worker.WorkerGroup;
import com.staros.worker.WorkerTest;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class StarMgrJournalTest {
    private String serviceId = "JournalTest";

    @BeforeClass
    public static void prepare() {
        StarletAgentFactory.AGENT_TYPE = StarletAgentFactory.AgentType.MOCK_STARLET_AGENT;
    }

    private static void assertShardEqual(Shard l, Shard r) {
        Assert.assertEquals(l.getServiceId(), r.getServiceId());
        Assert.assertEquals(l.getGroupIds(), r.getGroupIds());
        Assert.assertEquals(l.getShardId(), r.getShardId());
    }

    private static void assertShardGroupEqual(ShardGroup l, ShardGroup r) {
        Assert.assertEquals(l.getServiceId(), r.getServiceId());
        Assert.assertEquals(l.getGroupId(), r.getGroupId());
    }

    private void verifyHeader(JournalEntry entry, String serviceId, OperationType expectedType) {
        JournalHeader header = entry.getHeader();
        Assert.assertEquals(header.getServiceId(), serviceId);
        Assert.assertEquals(header.getOperationType(), expectedType);
    }

    private Journal parseJournal(ByteWriter writer) {
        byte[] bytes = writer.getData();
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
        try {
            return Journal.read(in);
        } catch (IOException e) {
            Assert.assertNull(e);
        }
        return null;
    }

    @Test
    public void testJournalCreateShard() {
        long groupId = 2;
        long shardId1 = 100;
        long shardId2 = 101;

        Shard shard1 = ShardTest.getTestShard(serviceId, groupId, shardId1);
        Shard shard2 = ShardTest.getTestShard(serviceId, groupId, shardId2);
        List<Shard> shards = new ArrayList<>();
        shards.add(shard1);
        shards.add(shard2);
        CreateShardJournalInfo.Builder builder = CreateShardJournalInfo.newBuilder();
        for (Shard shard : shards) {
            builder.addShardInfos(shard.toProtobuf());
        }

        // write
        Journal j = null;
        ByteWriter writer = new ByteWriter();
        try {
            j = StarMgrJournal.logCreateShard(serviceId, builder.build());
            j.write(writer);
        } catch (IOException e) {
            Assert.assertNull(e);
        }

        // read
        Journal j2 = parseJournal(writer);

        // verify header
        verifyHeader(j2.getEntry(), serviceId, OperationType.OP_CREATE_SHARD);

        // verify body
        try {
            CreateShardJournalInfo journalInfo = StarMgrJournal.parseLogCreateShard(j2);
            List<Shard> shards2 = new ArrayList<>();
            for (ShardInfo shardInfo : journalInfo.getShardInfosList()) {
                shards2.add(Shard.fromProtobuf(shardInfo));
            }
            Assert.assertEquals(shards.size(), shards2.size());
            assertShardEqual(shards.get(0), shards2.get(0));
            assertShardEqual(shards.get(1), shards2.get(1));
        } catch (IOException e) {
            Assert.assertNull(e);
        }
    }

    @Test
    public void testJournalUpdateShard() {
        long groupId = 2;
        long shardId1 = 100;
        long shardId2 = 101;

        Shard shard1 = ShardTest.getTestShard(serviceId, groupId, shardId1);
        Shard shard2 = ShardTest.getTestShard(serviceId, groupId, shardId2);
        List<Shard> shards = new ArrayList<>();
        shards.add(shard1);
        shards.add(shard2);

        // write
        Journal j = null;
        ByteWriter writer = new ByteWriter();
        try {
            j = StarMgrJournal.logUpdateShard(serviceId, shards);
            j.write(writer);
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }

        // read
        Journal j2 = parseJournal(writer);

        // verify header
        verifyHeader(j2.getEntry(), serviceId, OperationType.OP_UPDATE_SHARD);

        // verify body
        try {
            List<Shard> shards2 = StarMgrJournal.parseLogUpdateShard(j2);
            Assert.assertEquals(shards.size(), shards2.size());
            assertShardEqual(shards.get(0), shards2.get(0));
            assertShardEqual(shards.get(1), shards2.get(1));
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testJournalDeleteShard() {
        long groupId = 2;
        long shardId1 = 123;
        long shardId2 = 654;

        List<Long> shardIds = new ArrayList<>();
        shardIds.add(shardId1);
        shardIds.add(shardId2);

        // write
        Journal j = null;
        ByteWriter writer = new ByteWriter();
        try {
            j = StarMgrJournal.logDeleteShard(serviceId, shardIds);
            j.write(writer);
        } catch (IOException e) {
            Assert.assertNull(e);
        }

        // read
        Journal j2 = parseJournal(writer);

        // verify header
        verifyHeader(j2.getEntry(), serviceId, OperationType.OP_DELETE_SHARD);

        // verify body
        try {
            List<Long> shardIds2 = StarMgrJournal.parseLogDeleteShard(j2);
            Assert.assertEquals(shardIds.size(), shardIds2.size());
            Assert.assertEquals(shardIds.get(0), shardIds2.get(0));
            Assert.assertEquals(shardIds.get(1), shardIds2.get(1));
        } catch (IOException e) {
            Assert.assertNull(e);
        }
    }

    @Test
    public void testJournalCreateShardGroup() {
        long groupId1 = 101;
        long groupId2 = 102;

        ShardGroup shardGroup1 = new ShardGroup(serviceId, groupId1);
        ShardGroup shardGroup2 = new ShardGroup(serviceId, groupId2);
        List<ShardGroup> shardGroups = new ArrayList<>();
        shardGroups.add(shardGroup1);
        shardGroups.add(shardGroup2);

        // write
        Journal j = null;
        ByteWriter writer = new ByteWriter();
        try {
            j = StarMgrJournal.logCreateShardGroup(serviceId, shardGroups);
            j.write(writer);
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }

        // read
        Journal j2 = parseJournal(writer);

        // verify header
        verifyHeader(j2.getEntry(), serviceId, OperationType.OP_CREATE_SHARD_GROUP);

        // verify body
        try {
            List<ShardGroup> shardGroups2 = StarMgrJournal.parseLogCreateShardGroup(j2);
            Assert.assertEquals(shardGroups.size(), shardGroups2.size());
            assertShardGroupEqual(shardGroups.get(0), shardGroups2.get(0));
            assertShardGroupEqual(shardGroups.get(1), shardGroups2.get(1));
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testJournalDeleteShardGroup() {
        long groupId1 = 12345;
        long groupId2 = 54321;

        List<Long> groupIds = new ArrayList<>();
        groupIds.add(groupId1);
        groupIds.add(groupId2);
        DeleteShardGroupInfo deleteGroups = DeleteShardGroupInfo.newBuilder()
                .addAllGroupIds(groupIds)
                .setCascadeDeleteShard(true)
                .build();

        // write
        Journal j = null;
        ByteWriter writer = new ByteWriter();
        try {
            j = StarMgrJournal.logDeleteShardGroup(serviceId, deleteGroups);
            j.write(writer);
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }

        // read
        Journal j2 = parseJournal(writer);

        // verify header
        verifyHeader(j2.getEntry(), serviceId, OperationType.OP_DELETE_SHARD_GROUP);

        // verify body
        try {
            DeleteShardGroupInfo deleteInfo = StarMgrJournal.parseLogDeleteShardGroup(j2);
            List<Long> groupIds2 = deleteInfo.getGroupIdsList();
            Assert.assertTrue(deleteInfo.getCascadeDeleteShard());
            Assert.assertEquals(groupIds.size(), groupIds2.size());
            Assert.assertEquals(groupIds.get(0), groupIds2.get(0));
            Assert.assertEquals(groupIds.get(1), groupIds2.get(1));
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void testJournalUpdateShardGroup() {
        long groupId1 = 101;
        long groupId2 = 102;

        ShardGroup shardGroup1 = new ShardGroup(serviceId, groupId1);
        ShardGroup shardGroup2 = new ShardGroup(serviceId, groupId2);
        List<ShardGroup> shardGroups = new ArrayList<>();
        shardGroups.add(shardGroup1);
        shardGroups.add(shardGroup2);

        // write
        Journal j = null;
        ByteWriter writer = new ByteWriter();
        try {
            j = StarMgrJournal.logUpdateShardGroup(serviceId, shardGroups);
            j.write(writer);
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }

        // read
        Journal j2 = parseJournal(writer);

        // verify header
        verifyHeader(j2.getEntry(), serviceId, OperationType.OP_UPDATE_SHARD_GROUP);

        // verify body
        try {
            List<ShardGroup> shardGroups2 = StarMgrJournal.parseLogUpdateShardGroup(j2);
            Assert.assertEquals(shardGroups.size(), shardGroups2.size());
            assertShardGroupEqual(shardGroups.get(0), shardGroups2.get(0));
            assertShardGroupEqual(shardGroups.get(1), shardGroups2.get(1));
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testJournalCreateMetaGroup() {
        long metaGroupId = 12345;
        long groupId1 = 9876;
        long groupId2 = 6789;
        List<Long> groupIds = new ArrayList<>();
        groupIds.add(groupId1);
        groupIds.add(groupId2);
        CreateMetaGroupInfo createInfo = CreateMetaGroupInfo.newBuilder()
                .setMetaGroupId(metaGroupId)
                .addAllShardGroupIds(groupIds)
                .setPlacementPolicy(PlacementPolicy.PACK)
                .build();
        MetaGroupJournalInfo journalInfo = MetaGroupJournalInfo.newBuilder()
                .setCreateInfo(createInfo)
                .build();

        // write
        Journal j = null;
        ByteWriter writer = new ByteWriter();
        try {
            j = StarMgrJournal.logCreateMetaGroup(serviceId, journalInfo);
            j.write(writer);
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }

        // read
        Journal j2 = parseJournal(writer);

        // verify header
        verifyHeader(j2.getEntry(), serviceId, OperationType.OP_CREATE_META_GROUP);

        // verify body
        try {
            MetaGroupJournalInfo journalInfo2 = StarMgrJournal.parseLogCreateMetaGroup(j2);
            List<Long> groupIds2 = journalInfo2.getCreateInfo().getShardGroupIdsList();
            Assert.assertEquals(journalInfo2.getCreateInfo().getMetaGroupId(), metaGroupId);
            Assert.assertEquals(journalInfo2.getCreateInfo().getPlacementPolicy(),
                    journalInfo.getCreateInfo().getPlacementPolicy());
            Assert.assertEquals(groupIds.size(), groupIds2.size());
            Assert.assertEquals(groupIds.get(0), groupIds2.get(0));
            Assert.assertEquals(groupIds.get(1), groupIds2.get(1));
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testJournalDeleteMetaGroup() {
        long metaGroupId = 12345;
        DeleteMetaGroupInfo deleteInfo = DeleteMetaGroupInfo.newBuilder()
                .setMetaGroupId(metaGroupId)
                .build();
        MetaGroupJournalInfo journalInfo = MetaGroupJournalInfo.newBuilder()
                .setDeleteInfo(deleteInfo)
                .build();

        // write
        Journal j = null;
        ByteWriter writer = new ByteWriter();
        try {
            j = StarMgrJournal.logDeleteMetaGroup(serviceId, journalInfo);
            j.write(writer);
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }

        // read
        Journal j2 = parseJournal(writer);

        // verify header
        verifyHeader(j2.getEntry(), serviceId, OperationType.OP_DELETE_META_GROUP);

        // verify body
        try {
            MetaGroupJournalInfo journalInfo2 = StarMgrJournal.parseLogDeleteMetaGroup(j2);
            Assert.assertEquals(journalInfo2.getDeleteInfo().getMetaGroupId(), journalInfo.getDeleteInfo().getMetaGroupId());
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testJournalUpdateMetaGroup() {
        long srcMetaGroupId = 12345;
        long dstMetaGroupId = 98765;
        long groupId1 = 98765;
        long groupId2 = 56789;
        List<Long> groupIds = new ArrayList<>();
        groupIds.add(groupId1);
        groupIds.add(groupId2);
        TransferMetaGroupInfo transferInfo = TransferMetaGroupInfo.newBuilder()
                .setSrcMetaGroupId(srcMetaGroupId)
                .setDstMetaGroupId(dstMetaGroupId)
                .build();
        UpdateMetaGroupInfo updateInfo = UpdateMetaGroupInfo.newBuilder()
                .setTransferInfo(transferInfo)
                .addAllShardGroupIds(groupIds)
                .build();

        MetaGroupJournalInfo journalInfo = MetaGroupJournalInfo.newBuilder()
                .setUpdateInfo(updateInfo)
                .build();

        // write
        Journal j = null;
        ByteWriter writer = new ByteWriter();
        try {
            j = StarMgrJournal.logUpdateMetaGroup(serviceId, journalInfo);
            j.write(writer);
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }

        // read
        Journal j2 = parseJournal(writer);

        // verify header
        verifyHeader(j2.getEntry(), serviceId, OperationType.OP_UPDATE_META_GROUP);

        // verify body
        try {
            MetaGroupJournalInfo journalInfo2 = StarMgrJournal.parseLogUpdateMetaGroup(j2);
            List<Long> groupIds2 = journalInfo2.getUpdateInfo().getShardGroupIdsList();
            Assert.assertEquals(journalInfo2.getUpdateInfo().getTransferInfo().getSrcMetaGroupId(),
                    journalInfo.getUpdateInfo().getTransferInfo().getSrcMetaGroupId());
            Assert.assertEquals(journalInfo2.getUpdateInfo().getTransferInfo().getDstMetaGroupId(),
                    journalInfo.getUpdateInfo().getTransferInfo().getDstMetaGroupId());
            Assert.assertEquals(groupIds.size(), groupIds2.size());
            Assert.assertEquals(groupIds.get(0), groupIds2.get(0));
            Assert.assertEquals(groupIds.get(1), groupIds2.get(1));
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testJournalAddWorker() {
        long groupId = 2;
        long workerId = 321;
        String ipPort = "127.0.0.1:9876";

        Worker worker1 = WorkerTest.getTestWorker(serviceId, groupId, workerId, ipPort);

        // write
        Journal j = null;
        ByteWriter writer = new ByteWriter();
        try {
            j = StarMgrJournal.logAddWorker(worker1);
            j.write(writer);
        } catch (IOException e) {
            Assert.assertNull(e);
        }

        // read
        Journal j2 = parseJournal(writer);

        // verify header
        verifyHeader(j2.getEntry(), serviceId, OperationType.OP_ADD_WORKER);

        // verify body
        try {
            Worker worker2 = StarMgrJournal.parseLogAddWorker(j2);

            Assert.assertEquals(worker1.getServiceId(), worker2.getServiceId());
            Assert.assertEquals(worker1.getGroupId(), worker2.getGroupId());
            Assert.assertEquals(worker1.getWorkerId(), worker2.getWorkerId());
            Assert.assertEquals(worker1.getIpPort(), worker2.getIpPort());
        } catch (IOException e) {
            Assert.assertNull(e);
        }
    }

    @Test
    public void testJournalUpdateWorker() {
        long groupId = 2;
        long workerId = 321;
        String ipPort = "127.0.0.1:9876";

        Worker worker1 = WorkerTest.getTestWorker(serviceId, groupId, workerId, ipPort);

        // write
        Journal j = null;
        ByteWriter writer = new ByteWriter();
        try {
            List<Worker> workers1 = new ArrayList<>(1);
            workers1.add(worker1);
            j = StarMgrJournal.logUpdateWorker(serviceId, workers1);
            j.write(writer);
        } catch (IOException e) {
            Assert.assertNull(e);
        }

        // read
        Journal j2 = parseJournal(writer);

        // verify header
        verifyHeader(j2.getEntry(), serviceId, OperationType.OP_UPDATE_WORKER);

        // verify body
        try {
            List<Worker> workers = StarMgrJournal.parseLogUpdateWorker(j2);
            Assert.assertEquals(workers.size(), 1L);

            Worker worker2 = workers.get(0);
            Assert.assertEquals(worker1.getServiceId(), worker2.getServiceId());
            Assert.assertEquals(worker1.getGroupId(), worker2.getGroupId());
            Assert.assertEquals(worker1.getWorkerId(), worker2.getWorkerId());
            Assert.assertEquals(worker1.getIpPort(), worker2.getIpPort());
        } catch (IOException e) {
            Assert.assertNull(e);
        }
    }

    @Test
    public void testJournalRemoveWorker() {
        long groupId = 2;
        long workerId = 345;

        // write
        Journal j = null;
        ByteWriter writer = new ByteWriter();
        try {
            j = StarMgrJournal.logRemoveWorker(serviceId, groupId, workerId);
            j.write(writer);
        } catch (IOException e) {
            Assert.assertNull(e);
        }

        // read
        Journal j2 = parseJournal(writer);

        // verify header
        verifyHeader(j2.getEntry(), serviceId, OperationType.OP_REMOVE_WORKER);

        // verify body
        try {
            Pair<Long, Long> pair = StarMgrJournal.parseLogRemoveWorker(j2);

            Assert.assertEquals(groupId, (long) pair.getKey());
            Assert.assertEquals(workerId, (long) pair.getValue());
        } catch (IOException e) {
            Assert.assertNull(e);
        }
    }

    @Test
    public void testJournalRegisterService() {
        String serviceTemplateName = "JournalTest";
        List<String> serviceComponents = new ArrayList<>();
        serviceComponents.add("ccc");
        serviceComponents.add("ddd");
        ServiceTemplate serviceTemplate1 = new ServiceTemplate(serviceTemplateName, serviceComponents);

        // write
        Journal j = null;
        ByteWriter writer = new ByteWriter();
        try {
            j = StarMgrJournal.logRegisterService(serviceTemplate1);
            j.write(writer);
        } catch (IOException e) {
            Assert.assertNull(e);
        }

        // read
        Journal j2 = parseJournal(writer);

        // verify header
        verifyHeader(j2.getEntry(), Constant.EMPTY_SERVICE_ID, OperationType.OP_REGISTER_SERVICE);

        // verify body
        try {
            ServiceTemplate serviceTemplate2 = StarMgrJournal.parseLogRegisterService(j2);

            Assert.assertEquals(serviceTemplate1.getServiceTemplateName(), serviceTemplate2.getServiceTemplateName());
            Assert.assertEquals(serviceTemplate1.getServiceComponents().size(), serviceTemplate2.getServiceComponents().size());
        } catch (IOException e) {
            Assert.assertNull(e);
        }
    }

    @Test
    public void testJournalDeregisterService() {
        String serviceTemplateName = "JournalTest";

        // write
        Journal j = null;
        ByteWriter writer = new ByteWriter();
        try {
            j = StarMgrJournal.logDeregisterService(serviceTemplateName);
            j.write(writer);
        } catch (IOException e) {
            Assert.assertNull(e);
        }

        // read
        Journal j2 = parseJournal(writer);

        // verify header
        verifyHeader(j2.getEntry(), Constant.EMPTY_SERVICE_ID, OperationType.OP_DEREGISTER_SERVICE);

        // verify body
        try {
            String serviceTemplateName2 = StarMgrJournal.parseLogDeregisterService(j2);

            Assert.assertEquals(serviceTemplateName, serviceTemplateName2);
        } catch (IOException e) {
            Assert.assertNull(e);
        }
    }

    @Test
    public void testJournalBootstrapService() {
        String serviceTemplateName = "JournalTest";
        String serviceName = "JournalTest-0";
        Service service1 = new Service(serviceTemplateName, serviceName, serviceId);

        // write
        Journal j = null;
        ByteWriter writer = new ByteWriter();
        try {
            j = StarMgrJournal.logBootstrapService(service1);
            j.write(writer);
        } catch (IOException e) {
            Assert.assertNull(e);
        }

        // read
        Journal j2 = parseJournal(writer);

        // verify header
        verifyHeader(j2.getEntry(), serviceId, OperationType.OP_BOOTSTRAP_SERVICE);

        // verify body
        try {
            Service service2 = StarMgrJournal.parseLogBootstrapService(j2);

            Assert.assertEquals(service1.getServiceTemplateName(), service2.getServiceTemplateName());
            Assert.assertEquals(service1.getServiceName(), service2.getServiceName());
            Assert.assertEquals(service1.getServiceId(), service2.getServiceId());
        } catch (IOException e) {
            Assert.assertNull(e);
        }
    }

    @Test
    public void testJournalShutdownService() {
        String serviceTemplateName = "JournalTest";
        String serviceName = "JournalTest-0";
        Service service1 = new Service(serviceTemplateName, serviceName, serviceId);

        // write
        Journal j = null;
        ByteWriter writer = new ByteWriter();
        try {
            j = StarMgrJournal.logShutdownService(service1);
            j.write(writer);
        } catch (IOException e) {
            Assert.assertNull(e);
        }

        // read
        Journal j2 = parseJournal(writer);

        // verify header
        verifyHeader(j2.getEntry(), serviceId, OperationType.OP_SHUTDOWN_SERVICE);

        // verify body
        try {
            Service service2 = StarMgrJournal.parseLogShutdownService(j2);

            Assert.assertEquals(service1.getServiceTemplateName(), service2.getServiceTemplateName());
            Assert.assertEquals(service1.getServiceName(), service2.getServiceName());
            Assert.assertEquals(service1.getServiceId(), service2.getServiceId());
        } catch (IOException e) {
            Assert.assertNull(e);
        }
    }

    @Test
    public void testJournalCreateWorkerGroup() {
        String serviceId = "JournalTest-workerGroup";
        long workerGroupId = 123456;
        String owner = "TestOwner";
        WorkerGroupSpec spec = WorkerGroupSpec.newBuilder().setSize("XL").build();
        Map<String, String> labels = ImmutableMap.of("X1", "Y1", "X2", "Y2");
        Map<String, String> props = ImmutableMap.of("p1", "v1", "p2", "v2");
        WorkerGroupState state = WorkerGroupState.READY;
        int replicaNumber = 3;

        WorkerGroup groupA = new WorkerGroup(serviceId, workerGroupId, owner, spec, labels, props, replicaNumber,
                ReplicationType.ASYNC, WarmupLevel.WARMUP_NOT_SET);
        groupA.updateState(state);

        ByteWriter writer = new ByteWriter();
        try {
            StarMgrJournal.logCreateWorkerGroup(groupA).write(writer);
        } catch (Exception ex) {
            Assert.fail("Unexpected exception throw: " + ex.getMessage());
        }

        Journal j = parseJournal(writer);
        Assert.assertNotNull(j);
        verifyHeader(j.getEntry(), serviceId, OperationType.OP_CREATE_WORKER_GROUP);

        WorkerGroup groupB = StarMgrJournal.parseLogCreateWorkerGroup(j);

        Assert.assertEquals(owner, groupB.getOwner());
        Assert.assertEquals(spec, groupB.getSpec());
        Assert.assertEquals(workerGroupId, groupB.getGroupId());
        Assert.assertEquals(serviceId, groupB.getServiceId());
        Assert.assertEquals(state, groupB.getState());
        Assert.assertEquals(labels, groupB.getLabels());
        Assert.assertEquals(props, groupB.getProperties());
        Assert.assertEquals(replicaNumber, groupB.getReplicaNumber());
        Assert.assertEquals(ReplicationType.ASYNC, groupB.getReplicationType());
        // WARMUP_NOT_SET defaults to WARMUP_NOTHING
        Assert.assertEquals(WarmupLevel.WARMUP_NOTHING, groupB.getWarmupLevel());
    }

    @Test
    public void testJournalUpdateWorkerGroup() {
        String serviceId = "JournalTest-updateWorkerGroup";
        long workerGroupId = 123456;
        WorkerGroupSpec spec = WorkerGroupSpec.newBuilder().setSize("XL").build();
        Map<String, String> labels = ImmutableMap.of("X1", "Y1", "X2", "Y2");
        Map<String, String> props = ImmutableMap.of("p1", "v1", "p2", "v2");
        int replicaNumber = 3;
        ReplicationType replicationType = ReplicationType.ASYNC;
        WarmupLevel warmupLevel = WarmupLevel.WARMUP_META;

        ByteWriter writer = new ByteWriter();
        UpdateWorkerGroupInfo updateInfo = UpdateWorkerGroupInfo.newBuilder()
                .setGroupId(workerGroupId)
                .setSpec(spec)
                .putAllLabels(labels)
                .putAllProperties(props)
                .setReplicaNumber(replicaNumber)
                .setReplicationType(replicationType)
                .setWarmupLevel(warmupLevel)
                .build();

        try {
            StarMgrJournal.logUpdateWorkerGroup(serviceId, updateInfo).write(writer);
        } catch (Exception ex) {
            Assert.fail("Unexpected exception throw: " + ex.getMessage());
        }

        Journal j = parseJournal(writer);
        Assert.assertNotNull(j);
        verifyHeader(j.getEntry(), serviceId, OperationType.OP_UPDATE_WORKER_GROUP);

        UpdateWorkerGroupInfo replayInfo = StarMgrJournal.parseLogUpdateWorkerGroup(j);
        Assert.assertEquals(updateInfo.getGroupId(), replayInfo.getGroupId());
        Assert.assertEquals(updateInfo.getSpec(), replayInfo.getSpec());
        Assert.assertEquals(updateInfo.getLabelsMap(), replayInfo.getLabelsMap());
        Assert.assertEquals(updateInfo.getPropertiesMap(), replayInfo.getPropertiesMap());
        Assert.assertEquals(updateInfo.getReplicaNumber(), replayInfo.getReplicaNumber());
        Assert.assertEquals(updateInfo.getReplicationType(), replayInfo.getReplicationType());
        Assert.assertEquals(updateInfo.getWarmupLevel(), replayInfo.getWarmupLevel());
        Assert.assertEquals(updateInfo.toString(), replayInfo.toString());
    }

    @Test
    public void testJournalDeleteWorkerGroup() {
        long workerGroupId = 9527;
        ByteWriter writer = new ByteWriter();
        try {
            StarMgrJournal.logDeleteWorkerGroup(serviceId, workerGroupId).write(writer);
        } catch (Exception ex) {
            Assert.fail("Unexpected exception throw: " + ex.getMessage());
        }

        Journal j = parseJournal(writer);
        Assert.assertNotNull(j);
        verifyHeader(j.getEntry(), serviceId, OperationType.OP_DELETE_WORKER_GROUP);
        long replayGroupId = StarMgrJournal.parseLogDeleteWorkerGroup(j);
        Assert.assertEquals(workerGroupId, replayGroupId);
    }

    @Test
    public void testJournalSetId() {
        long id = 51315;

        // write
        Journal j = null;
        ByteWriter writer = new ByteWriter();
        try {
            j = StarMgrJournal.logSetId(id);
            j.write(writer);
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }

        // read
        Journal j2 = parseJournal(writer);

        // verify header
        verifyHeader(j2.getEntry(), Constant.EMPTY_SERVICE_ID, OperationType.OP_SET_ID);

        // verify body
        try {
            long id2 = StarMgrJournal.parseLogSetId(j2);

            Assert.assertEquals(id2, id);
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testJournalAddFileStore() {
        S3FileStoreInfo s3fsInfo = S3FileStoreInfo.newBuilder().setBucket("test-bucket").
                setRegion("test-region").setEndpoint("test-endpoint").build();
        FileStoreInfo fileStoreInfo = FileStoreInfo.newBuilder().setFsKey(Constant.S3_FSKEY_FOR_CONFIG).setS3FsInfo(s3fsInfo)
                .setFsName(Constant.S3_FSNAME_FOR_CONFIG)
                .setFsType(FileStoreType.S3)
                .build();
        ByteWriter writer = new ByteWriter();
        try {
            StarMgrJournal.logAddFileStore(serviceId, fileStoreInfo).write(writer);
        } catch (Exception ex) {
            Assert.fail("Unexpected exception throw: " + ex.getMessage());
        }

        Journal j = parseJournal(writer);
        Assert.assertNotNull(j);
        verifyHeader(j.getEntry(), serviceId, OperationType.OP_ADD_FILESTORE);

        try {
            FileStoreInfo replayFileStoreInfo = StarMgrJournal.parseLogAddFileStore(j);
            Assert.assertEquals(fileStoreInfo.getFsKey(), replayFileStoreInfo.getFsKey());
            Assert.assertEquals(fileStoreInfo.getFsName(), replayFileStoreInfo.getFsName());
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testJournalUpdateFileStore() {
        S3FileStoreInfo s3fsInfo = S3FileStoreInfo.newBuilder().setBucket("test-bucket").
                setRegion("test-region").setEndpoint("test-endpoint").build();
        FileStoreInfo fileStoreInfo = FileStoreInfo.newBuilder().setFsKey(Constant.S3_FSKEY_FOR_CONFIG).setS3FsInfo(s3fsInfo)
                .setFsName(Constant.S3_FSNAME_FOR_CONFIG)
                .setFsType(FileStoreType.S3)
                .build();
        ByteWriter writer = new ByteWriter();
        try {
            StarMgrJournal.logUpdateFileStore(serviceId, fileStoreInfo).write(writer);
        } catch (Exception ex) {
            Assert.fail("Unexpected exception throw: " + ex.getMessage());
        }

        Journal j = parseJournal(writer);
        Assert.assertNotNull(j);
        verifyHeader(j.getEntry(), serviceId, OperationType.OP_UPDATE_FILESTORE);

        try {
            FileStoreInfo replayFileStoreInfo = StarMgrJournal.parseLogUpdateFileStore(j);
            Assert.assertEquals(fileStoreInfo.getFsKey(), replayFileStoreInfo.getFsKey());
            Assert.assertEquals(fileStoreInfo.getFsName(), replayFileStoreInfo.getFsName());
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testJournalReplaceFileStore() {
        S3FileStoreInfo s3fsInfo = S3FileStoreInfo.newBuilder().setBucket("test-bucket").setRegion("test-region")
                .setEndpoint("test-endpoint").build();
        FileStoreInfo fileStoreInfo = FileStoreInfo.newBuilder().setFsKey(Constant.S3_FSKEY_FOR_CONFIG)
                .setS3FsInfo(s3fsInfo)
                .setFsName(Constant.S3_FSNAME_FOR_CONFIG)
                .setFsType(FileStoreType.S3)
                .build();
        ByteWriter writer = new ByteWriter();
        try {
            StarMgrJournal.logReplaceFileStore(serviceId, fileStoreInfo).write(writer);
        } catch (Exception ex) {
            Assert.fail("Unexpected exception throw: " + ex.getMessage());
        }

        Journal j = parseJournal(writer);
        Assert.assertNotNull(j);
        verifyHeader(j.getEntry(), serviceId, OperationType.OP_REPLACE_FILESTORE);

        try {
            FileStoreInfo replaceFileStoreInfo = StarMgrJournal.parseLogReplaceFileStore(j);
            Assert.assertEquals(fileStoreInfo.getFsKey(), replaceFileStoreInfo.getFsKey());
            Assert.assertEquals(fileStoreInfo.getFsName(), replaceFileStoreInfo.getFsName());
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testJournalRemoveFileStore() {
        ByteWriter writer = new ByteWriter();
        try {
            StarMgrJournal.logRemoveFileStore(serviceId, Constant.S3_FSKEY_FOR_CONFIG).write(writer);
        } catch (Exception ex) {
            Assert.fail("Unexpected exception throw: " + ex.getMessage());
        }

        Journal j = parseJournal(writer);
        Assert.assertNotNull(j);
        verifyHeader(j.getEntry(), serviceId, OperationType.OP_REMOVE_FILESTORE);

        try {
            String fsKey = StarMgrJournal.parseLogRemoveFileStore(j);
            Assert.assertEquals(Constant.S3_FSKEY_FOR_CONFIG, fsKey);
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testJournalLeaderInfo() {
        LeaderInfo info = LeaderInfo.newBuilder().setHost("test-host").setPort(12345).build();
        ByteWriter writer = new ByteWriter();
        try {
            StarMgrJournal.logLeaderInfo(info).write(writer);
        } catch (Exception ex) {
            Assert.fail("Unexpected exception:" + ex.getMessage());
        }

        Journal journal = parseJournal(writer);
        Assert.assertNotNull(journal);
        verifyHeader(journal.getEntry(), "", OperationType.OP_LEADER_CHANGE);

        try {
            LeaderInfo info2 = StarMgrJournal.parseLogLeaderInfo(journal);
            Assert.assertEquals(info.toByteString(), info2.toByteString());
        } catch (IOException ex) {
            Assert.fail("Unexpected exception:" + ex.getMessage());
        }
    }
}
