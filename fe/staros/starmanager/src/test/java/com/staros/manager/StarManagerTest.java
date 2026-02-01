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

package com.staros.manager;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.staros.common.HijackConfig;
import com.staros.common.MemoryJournalSystem;
import com.staros.common.TestHelper;
import com.staros.common.TestUtils;
import com.staros.credential.AwsCredential;
import com.staros.credential.AwsDefaultCredential;
import com.staros.exception.ExceptionCode;
import com.staros.exception.FailedPreconditionStarException;
import com.staros.exception.InvalidArgumentStarException;
import com.staros.exception.NotExistStarException;
import com.staros.exception.NotLeaderStarException;
import com.staros.exception.StarException;
import com.staros.filestore.HDFSFileStore;
import com.staros.filestore.S3FileStore;
import com.staros.journal.Journal;
import com.staros.journal.StarMgrJournal;
import com.staros.proto.AddShardInfo;
import com.staros.proto.CacheEnableState;
import com.staros.proto.CreateMetaGroupInfo;
import com.staros.proto.CreateShardGroupInfo;
import com.staros.proto.CreateShardInfo;
import com.staros.proto.FilePathInfo;
import com.staros.proto.FileStoreType;
import com.staros.proto.JoinMetaGroupInfo;
import com.staros.proto.LeaderInfo;
import com.staros.proto.MetaGroupInfo;
import com.staros.proto.PlacementPolicy;
import com.staros.proto.ReplicaInfo;
import com.staros.proto.ReplicaRole;
import com.staros.proto.ReplicaState;
import com.staros.proto.ReplicaUpdateInfo;
import com.staros.proto.ReplicationType;
import com.staros.proto.ServiceInfo;
import com.staros.proto.ServiceState;
import com.staros.proto.ShardGroupInfo;
import com.staros.proto.ShardInfo;
import com.staros.proto.ShardInfoList;
import com.staros.proto.ShardReportInfo;
import com.staros.proto.UpdateMetaGroupInfo;
import com.staros.proto.UpdateShardGroupInfo;
import com.staros.proto.UpdateShardInfo;
import com.staros.proto.WarmupLevel;
import com.staros.proto.WorkerGroupDetailInfo;
import com.staros.proto.WorkerGroupSpec;
import com.staros.proto.WorkerInfo;
import com.staros.proto.WorkerState;
import com.staros.replica.Replica;
import com.staros.schedule.Scheduler;
import com.staros.shard.Shard;
import com.staros.shard.ShardManager;
import com.staros.starlet.MockStarletAgent;
import com.staros.starlet.StarletAgent;
import com.staros.starlet.StarletAgentFactory;
import com.staros.util.Config;
import com.staros.util.Constant;
import com.staros.util.Utils;
import com.staros.worker.Worker;
import com.staros.worker.WorkerGroup;
import com.staros.worker.WorkerManager;
import mockit.Expectations;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import mockit.Verifications;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class StarManagerTest {
    private static final Logger LOG = LogManager.getLogger(StarManagerTest.class);
    private final String serviceTemplateName = "StarManagerTest";
    private StarManager starManager;
    private HijackConfig disableShardChecker;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @BeforeClass
    public static void prepare() {
        Config.S3_BUCKET = "test-bucket";
        Config.S3_REGION = "test-region";
        Config.S3_ENDPOINT = "test-endpoint";
        Config.SIMPLE_CREDENTIAL_ACCESS_KEY_ID = "test-ak";
        Config.SIMPLE_CREDENTIAL_ACCESS_KEY_SECRET = "test-sk";
        Config.HDFS_URL = "url";
        StarletAgentFactory.AGENT_TYPE = StarletAgentFactory.AgentType.MOCK_STARLET_AGENT;
    }

    @Before
    public void startStarManager() {
        disableShardChecker = new HijackConfig("DISABLE_BACKGROUND_SHARD_SCHEDULE_CHECK", "true");
        starManager = new StarManager();
        starManager.becomeLeader();
    }

    @After
    public void stopStarManager() {
        starManager.becomeFollower();
        disableShardChecker.reset();
    }

    @Test
    public void testReplayLeaderInfo() {
        StarManager mgr = new StarManager();
        mgr.becomeFollower();

        // choose an API that requires LEADER role to accomplish the task
        ThrowingRunnable runnable = () -> mgr.registerService("test-template", null);
        {
            StarException exception = Assert.assertThrows(StarException.class, runnable);
            Assert.assertNotNull(exception);
            Assert.assertTrue(exception instanceof NotLeaderStarException);
            // no leader info yet, leaderInfo is empty.
            LeaderInfo emptyLeaderInfo = LeaderInfo.newBuilder().build();
            Assert.assertEquals(emptyLeaderInfo, ((NotLeaderStarException) exception).getLeaderInfo());
            LOG.info("NotLeaderStarException errorMsg: {}", exception.getMessage());
            Assert.assertTrue(exception.getMessage(), exception.getMessage().contains("unknown leader"));
        }
        LeaderInfo info = LeaderInfo.newBuilder()
                .setHost("MyFakeHost")
                .setPort(54321)
                .build();
        // simulate replay LEADER_CHANGE edit log
        mgr.replayLeaderChange(info);

        { // test the runnable again
            StarException exception = Assert.assertThrows(StarException.class, runnable);
            Assert.assertNotNull(exception);
            Assert.assertTrue(exception instanceof NotLeaderStarException);
            // the exception carries the leader info
            LOG.info("NotLeaderStarException errorMsg: {}", exception.getMessage());
            Assert.assertTrue(exception.getMessage(), exception.getMessage().contains(info.toString()));
            Assert.assertEquals(info.toString(), ((NotLeaderStarException) exception).getLeaderInfo().toString());
        }
    }

    private void testBecomeLeaderWriterJournalInternal(LeaderInfo info) {
        MemoryJournalSystem journalSys = new MemoryJournalSystem();
        { // leader not set its address info
            journalSys.clearJournals();
            Assert.assertTrue(journalSys.getJournals().isEmpty());
            StarManager mgr = new StarManager(journalSys);
            if (!info.getHost().isEmpty()) {
                mgr.setListenAddressInfo(info.getHost(), info.getPort());
            }
            mgr.becomeLeader();
            // should write one journal
            Assert.assertEquals(1L, journalSys.getJournals().size());
            Journal journal = journalSys.getJournals().get(0);
            try {
                LeaderInfo leader = StarMgrJournal.parseLogLeaderInfo(journal);
                Assert.assertEquals(info.toString(), leader.toString());
            } catch (IOException exception) {
                Assert.fail("Unexpected exception: " + exception.getMessage());
            }
            mgr.becomeFollower();
        }
    }

    @Test
    public void testBecomeLeaderWriteJournal() {
        // leader with empty info
        testBecomeLeaderWriterJournalInternal(LeaderInfo.newBuilder().build());
        // leader sets its address info
        testBecomeLeaderWriterJournalInternal(LeaderInfo.newBuilder().setHost("LeaderHost").setPort(11111).build());
    }

    public static long createWorkerGroup(StarManager starManager, String serviceId) {
        return starManager.getWorkerManager().createWorkerGroup(serviceId, "testOwner",
                WorkerGroupSpec.newBuilder().setSize("X").build(), null, null, 1 /* replicaNumber */,
                ReplicationType.SYNC, WarmupLevel.WARMUP_NOT_SET);
    }

    @Test
    public void testStarManagerRegisterService() {
        starManager.registerService(serviceTemplateName, null);

        try {
            starManager.registerService(serviceTemplateName, null);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.ALREADY_EXIST);
        }
    }

    @Test
    public void testStarManagerDeregisterService() {
        starManager.registerService(serviceTemplateName, null);

        starManager.deregisterService(serviceTemplateName);

        try {
            starManager.deregisterService(serviceTemplateName);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.NOT_EXIST);
        }
    }

    @Test
    public void testStarManagerBootstrapService() {
        String serviceName = "StarManagerTest-1";

        starManager.registerService(serviceTemplateName, null);

        starManager.bootstrapService(serviceTemplateName, serviceName);

        try {
            starManager.bootstrapService(serviceTemplateName, serviceName);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.ALREADY_EXIST);
        }
    }

    @Test
    public void testStarManagerShutdownService() {
        String serviceName = "StarManagerTest-1";

        starManager.registerService(serviceTemplateName, null);

        String serviceId = starManager.bootstrapService(serviceTemplateName, serviceName);
        starManager.shutdownService(serviceId);

        try {
            starManager.shutdownService(serviceId + "1");
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.NOT_EXIST);
        }
    }

    @Test
    public void testStarManagerGetServiceInfo() {
        String serviceName1 = "StarManagerTest-1";
        String serviceName2 = "StarManagerTest-2";

        starManager.registerService(serviceTemplateName, null);

        String serviceId1 = starManager.bootstrapService(serviceTemplateName, serviceName1);
        String serviceId2 = starManager.bootstrapService(serviceTemplateName, serviceName2);

        ServiceInfo serviceInfo1 = starManager.getServiceInfoById(serviceId1);
        Assert.assertEquals(serviceInfo1.getServiceTemplateName(), serviceTemplateName);
        Assert.assertEquals(serviceInfo1.getServiceName(), serviceName1);
        Assert.assertEquals(serviceInfo1.getServiceId(), serviceId1);
        Assert.assertEquals(serviceInfo1.getServiceState(), ServiceState.RUNNING);
        Assert.assertEquals(serviceId1, starManager.getServiceIdByIdOrName(serviceName1));

        ServiceInfo serviceInfo2 = starManager.getServiceInfoByName(serviceName2);
        Assert.assertEquals(serviceInfo2.getServiceTemplateName(), serviceTemplateName);
        Assert.assertEquals(serviceInfo2.getServiceName(), serviceName2);
        Assert.assertEquals(serviceInfo2.getServiceId(), serviceId2);
        Assert.assertEquals(serviceInfo2.getServiceState(), ServiceState.RUNNING);
        Assert.assertEquals(serviceId2, starManager.getServiceIdByIdOrName(serviceId2));

        try {
            starManager.getServiceInfoById(serviceId2 + "1");
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.NOT_EXIST);
        }

        try {
            starManager.getServiceInfoByName(serviceName2 + "aaa");
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.NOT_EXIST);
        }
    }

    @Test
    public void testStarManagerAddWorker() {
        String serviceName = "StarManagerTest-1";
        String ipPort = "127.0.0.1:1234";

        starManager.registerService(serviceTemplateName, null);

        String serviceId = starManager.bootstrapService(serviceTemplateName, serviceName);
        long groupId = createWorkerGroup(starManager, serviceId);

        starManager.addWorker(serviceId, groupId, ipPort);

        try {
            starManager.addWorker(serviceId, groupId, ipPort);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.ALREADY_EXIST);
        }
    }

    @Test
    public void testStarManagerRemoveWorker() {
        String serviceName = "StarManagerTest-1";
        String ipPort = "127.0.0.1:1234";

        starManager.registerService(serviceTemplateName, null);

        String serviceId = starManager.bootstrapService(serviceTemplateName, serviceName);
        long groupId = createWorkerGroup(starManager, serviceId);

        long workerId = starManager.addWorker(serviceId, groupId, ipPort);
        starManager.removeWorker(serviceId, groupId, workerId);

        try {
            starManager.removeWorker(serviceId, groupId, workerId);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.NOT_EXIST);
        }
    }

    @Test
    public void testStarManagerGetWorkerInfo() {
        String serviceName = "StarManagerTest-1";
        String ipPort = "127.0.0.1:1234";

        starManager.registerService(serviceTemplateName, null);

        String serviceId = starManager.bootstrapService(serviceTemplateName, serviceName);
        long groupId = createWorkerGroup(starManager, serviceId);

        // test get worker by id
        long workerId = starManager.addWorker(serviceId, groupId, ipPort);
        {
            Worker worker = starManager.getWorkerManager().getWorker(workerId);
            Awaitility.await().atMost(1, TimeUnit.SECONDS).until(worker::isAlive);
            WorkerInfo workerInfo = starManager.getWorkerInfo(serviceId, workerId);
            Assert.assertEquals(workerInfo.getServiceId(), serviceId);
            Assert.assertEquals(workerInfo.getGroupId(), groupId);
            Assert.assertEquals(workerInfo.getWorkerId(), workerId);
            Assert.assertEquals(workerInfo.getIpPort(), ipPort);
            Assert.assertEquals(workerInfo.getWorkerState(), WorkerState.ON);

            try {
                starManager.getWorkerInfo(serviceId + "1", workerId);
            } catch (StarException e) {
                Assert.assertEquals(e.getExceptionCode(), ExceptionCode.NOT_EXIST);
            }

            try {
                starManager.getWorkerInfo(serviceId, workerId + 1);
            } catch (StarException e) {
                Assert.assertEquals(e.getExceptionCode(), ExceptionCode.NOT_EXIST);
            }
        }

        // test get worker by ip port
        {
            WorkerInfo workerInfo = starManager.getWorkerInfo(serviceId, ipPort);
            Assert.assertEquals(workerInfo.getServiceId(), serviceId);
            Assert.assertEquals(workerInfo.getGroupId(), groupId);
            Assert.assertEquals(workerInfo.getWorkerId(), workerId);
            Assert.assertEquals(workerInfo.getIpPort(), ipPort);
            Assert.assertEquals(workerInfo.getWorkerState(), WorkerState.ON);

            try {
                starManager.getWorkerInfo(serviceId, ipPort + "aaa");
            } catch (StarException e) {
                Assert.assertEquals(e.getExceptionCode(), ExceptionCode.NOT_EXIST);
            }
        }
    }

    @Test
    public void testStarManagerCreateShard() {
        String serviceName = "StarManagerTest-1";
        String ipPort1 = "127.0.0.1:1234";
        String ipPort2 = "127.0.0.1:1235";

        starManager.registerService(serviceTemplateName, null);
        String serviceId = starManager.bootstrapService(serviceTemplateName, serviceName);
        long workerGroupId = createWorkerGroup(starManager, serviceId);
        starManager.addWorker(serviceId, workerGroupId, ipPort1);
        starManager.addWorker(serviceId, workerGroupId, ipPort2);

        List<CreateShardInfo> createShardInfos = new ArrayList<>();
        createShardInfos.add(CreateShardInfo.newBuilder().build());
        createShardInfos.add(CreateShardInfo.newBuilder().build());
        List<ShardInfo> shardInfos = starManager.createShard(serviceId, createShardInfos);
        Assert.assertEquals(shardInfos.size(), 2);
        Assert.assertEquals(shardInfos.size(), starManager.getShardManager(serviceId).getShardCount());
        
        try {
            starManager.createShard(serviceId + "1", createShardInfos);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.NOT_EXIST);
        }

        try {
            List<Long> groupIds = new ArrayList<>();
            groupIds.add(workerGroupId + 1);
            List<CreateShardInfo> createShardInfos2 = new ArrayList<>();
            createShardInfos2.add(CreateShardInfo.newBuilder().addAllGroupIds(groupIds).build());
            createShardInfos2.add(CreateShardInfo.newBuilder().addAllGroupIds(groupIds).build());
            starManager.createShard(serviceId, createShardInfos2);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.NOT_EXIST);
        }
    }

    @Test
    public void testStarManagerCreateShardAndScheduleToWorkerGroup() {
        String serviceName = "StarManagerTest-1";
        starManager.registerService(serviceTemplateName, null);
        String serviceId = starManager.bootstrapService(serviceTemplateName, serviceName);
        long workerGroupId1 = createWorkerGroup(starManager, serviceId);
        long workerGroupId2 = createWorkerGroup(starManager, serviceId);

        List<Long> workerIds = new ArrayList<>();
        // workerGroupId1 has two workers, workerGroupId2 has 1 worker
        workerIds.add(starManager.addWorker(serviceId, workerGroupId1, TestHelper.generateMockWorkerIpAddress()));
        workerIds.add(starManager.addWorker(serviceId, workerGroupId1, TestHelper.generateMockWorkerIpAddress()));
        long workerId3 = starManager.addWorker(serviceId, workerGroupId2, TestHelper.generateMockWorkerIpAddress());

        CreateShardInfo createRequest = CreateShardInfo.newBuilder()
                .setReplicaCount(1)
                .setScheduleToWorkerGroup(workerGroupId1)
                .build();
        List<ShardInfo> shardInfos = starManager.createShard(serviceId, Collections.singletonList(createRequest));
        Assert.assertEquals(1L, shardInfos.size());
        ShardInfo info = shardInfos.get(0);
        long shardId = info.getShardId();
        Assert.assertEquals(1L, info.getReplicaInfoCount());
        Assert.assertTrue(workerIds.contains(info.getReplicaInfo(0).getWorkerInfo().getWorkerId()));

        { // directly pull all shard replica info from shard manager without filtering/scheduling
            List<ShardInfo> shardInfo2 = starManager.getShardManager(serviceId).getShardInfo(Collections.singletonList(shardId));
            Assert.assertEquals(1L, shardInfo2.size());
            // still only have 1 replicas
            Assert.assertEquals(1L, shardInfo2.get(0).getReplicaInfoCount());
            Assert.assertTrue(workerIds.contains(info.getReplicaInfo(0).getWorkerInfo().getWorkerId()));
        }
        // get shard info from workerGroupId2, triggers a schedule in workerGroupId2
        List<ShardInfo> shardInfos2 = starManager.getShardInfo(serviceId, Collections.singletonList(shardId), workerGroupId2);
        Assert.assertEquals(1L, shardInfos2.size());
        {
            ShardInfo info2 = shardInfos2.get(0);
            Assert.assertEquals(1L, info2.getReplicaInfoCount());
            Assert.assertEquals(workerId3, info2.getReplicaInfo(0).getWorkerInfo().getWorkerId());
        }
        { // Verify info from shardManager
            List<ShardInfo> shardInfo2 = starManager.getShardManager(serviceId).getShardInfo(Collections.singletonList(shardId));
            Assert.assertEquals(1L, shardInfo2.size());

            List<Long> replicaWorkerIds = shardInfo2.get(0).getReplicaInfoList().stream()
                    .map(x -> x.getWorkerInfo().getWorkerId())
                    .collect(Collectors.toList());
            // have 2 replicas
            Assert.assertEquals(2L, replicaWorkerIds.size());
            Assert.assertTrue(replicaWorkerIds.contains(workerId3));
            replicaWorkerIds.remove(workerId3);
            Assert.assertTrue(workerIds.contains(replicaWorkerIds.get(0)));
        }
    }

    @Test
    public void testStarManagerDeleteShard() {
        String serviceName = "StarManagerTest-1";
        String ipPort1 = "127.0.0.1:1234";
        String ipPort2 = "127.0.0.1:1235";

        starManager.registerService(serviceTemplateName, null);
        String serviceId = starManager.bootstrapService(serviceTemplateName, serviceName);
        long workerGroupId = createWorkerGroup(starManager, serviceId);
        starManager.addWorker(serviceId, workerGroupId, ipPort1);
        starManager.addWorker(serviceId, workerGroupId, ipPort2);

        List<CreateShardInfo> createShardInfos = new ArrayList<>();
        createShardInfos.add(CreateShardInfo.newBuilder().build());
        createShardInfos.add(CreateShardInfo.newBuilder().build());
        List<ShardInfo> shardInfos = starManager.createShard(serviceId, createShardInfos);

        List<Long> shardIds = shardInfos.stream().map(ShardInfo::getShardId).collect(Collectors.toList());

        starManager.deleteShard(serviceId, shardIds);
        Assert.assertEquals(0, starManager.getShardManager(serviceId).getShardCount());

        // test delete non-exist shard
        try {
            starManager.deleteShard(serviceId, shardIds);
        } catch (StarException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testStarManagerUpdateShard() {
        String serviceName = "StarManagerTest-1";
        String ipPort1 = "127.0.0.1:1234";
        String ipPort2 = "127.0.0.1:1235";

        starManager.registerService(serviceTemplateName, null);
        String serviceId = starManager.bootstrapService(serviceTemplateName, serviceName);
        long workerGroupId = createWorkerGroup(starManager, serviceId);
        starManager.addWorker(serviceId, workerGroupId, ipPort1);
        starManager.addWorker(serviceId, workerGroupId, ipPort2);

        List<CreateShardInfo> createShardInfos = new ArrayList<>();
        createShardInfos.add(CreateShardInfo.newBuilder().build());
        createShardInfos.add(CreateShardInfo.newBuilder().build());
        List<ShardInfo> shardInfos = starManager.createShard(serviceId, createShardInfos);

        List<Long> shardIds = shardInfos.stream().map(ShardInfo::getShardId).collect(Collectors.toList());

        List<UpdateShardInfo> updateShardInfos = new ArrayList<>();
        updateShardInfos.add(UpdateShardInfo.newBuilder().setShardId(shardIds.get(0))
                .setEnableCache(CacheEnableState.ENABLED).build());
        updateShardInfos.add(UpdateShardInfo.newBuilder().setShardId(shardIds.get(1))
                .setEnableCache(CacheEnableState.DISABLED).build());
        starManager.updateShard(serviceId, updateShardInfos);
        Assert.assertEquals(
                starManager.getShardManager(serviceId).getShard(shardIds.get(0)).getFileCache().getFileCacheEnable(),
                true);
        Assert.assertEquals(
                starManager.getShardManager(serviceId).getShard(shardIds.get(1)).getFileCache().getFileCacheEnable(),
                false);

        // test non-exist serviceId
        try {
            starManager.updateShard("abcd", updateShardInfos);
        } catch (StarException e) {
        }

        starManager.deleteShard(serviceId, shardIds);
        Assert.assertEquals(0, starManager.getShardManager(serviceId).getShardCount());

        // test update non-exist shard
        try {
            starManager.updateShard(serviceId, updateShardInfos);
        } catch (StarException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testStarManagerGetShardInfo() {
        String serviceName = "StarManagerTest-1";
        String ipPort1 = "127.0.0.1:1234";
        String ipPort2 = "127.0.0.1:1235";

        starManager.registerService(serviceTemplateName, null);
        String serviceId = starManager.bootstrapService(serviceTemplateName, serviceName);
        long workerGroupId = Constant.DEFAULT_ID;
        if (!Config.ENABLE_ZERO_WORKER_GROUP_COMPATIBILITY) {
            workerGroupId = createWorkerGroup(starManager, serviceId);
        }
        starManager.addWorker(serviceId, workerGroupId, ipPort1);
        starManager.addWorker(serviceId, workerGroupId, ipPort2);

        List<Long> groupIds = Collections.singletonList(Constant.DEFAULT_ID);
        List<CreateShardInfo> createShardInfos = new ArrayList<>();
        createShardInfos.add(CreateShardInfo.newBuilder().addGroupIds(Constant.DEFAULT_ID).build());
        createShardInfos.add(CreateShardInfo.newBuilder().addGroupIds(Constant.DEFAULT_ID).build());
        List<ShardInfo> shardInfos = starManager.createShard(serviceId, createShardInfos);
        Assert.assertEquals(shardInfos.size(), 2);

        List<Long> shardIds = new ArrayList<>();
        for (ShardInfo info : shardInfos) {
            shardIds.add(info.getShardId());
        }
        shardInfos = starManager.getShardInfo(serviceId, shardIds, workerGroupId);
        Assert.assertEquals(shardInfos.size(), 2);

        ShardInfo shard1 = shardInfos.get(0);
        Assert.assertEquals(shard1.getServiceId(), serviceId);
        Assert.assertEquals(shard1.getGroupIdsList(), groupIds);
        Assert.assertEquals(shard1.getShardId(), (long) shardIds.get(0));
        Assert.assertEquals(1, shard1.getReplicaInfoList().size());
        Assert.assertFalse(shard1.getReplicaInfoList().get(0).getWorkerInfo().getIpPort().isEmpty());
        Assert.assertEquals(shard1.getReplicaInfoList().get(0).getReplicaRole(), ReplicaRole.PRIMARY);
        long workerToRemove = shard1.getReplicaInfoList().get(0).getWorkerInfo().getWorkerId();

        ShardInfo shard2 = shardInfos.get(1);
        Assert.assertEquals(shard2.getServiceId(), serviceId);
        Assert.assertEquals(shard2.getGroupIdsList(), groupIds);
        Assert.assertEquals(shard2.getShardId(), (long) shardIds.get(1));
        Assert.assertEquals(shard2.getReplicaInfoList().size(), 1);

        // remove worker
        starManager.removeWorker(serviceId, workerGroupId, workerToRemove);
        // get shard info after worker removed
        // shard will be re-scheduled when calling starManager.getShardInfo() interface.
        shardInfos = starManager.getShardInfo(serviceId, shardIds, workerGroupId);
        ShardInfo shard = shardInfos.get(0);
        Assert.assertEquals(1, shard.getReplicaInfoList().size());
        // Not the same as original one
        Assert.assertNotEquals(workerToRemove, shard.getReplicaInfoList().get(0).getWorkerInfo().getWorkerId());

        try {
            starManager.getShardInfo(serviceId + "1", shardIds, workerGroupId);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.NOT_EXIST);
        }

        try {
            List<Long> shardIds2 = Collections.singletonList(Constant.DEFAULT_ID);
            starManager.getShardInfo(serviceId, shardIds2, workerGroupId);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.NOT_EXIST);
        }
    }

    @Test
    public void testStarManagerProcessWorkerHeartbeat() {
        String serviceName = "StarManagerTest-ProcessWorkerHeartBeat";
        starManager.registerService(serviceTemplateName, null);
        String serviceId = starManager.bootstrapService(serviceTemplateName, serviceName);
        starManager.getWorkerManager().bootstrapService(serviceId);

        // create shards to get shardId
        List<CreateShardInfo> requests = Collections.nCopies(3, CreateShardInfo.newBuilder().build());
        List<ShardInfo> response = starManager.createShard(serviceId, requests);
        Assert.assertEquals(requests.size(), response.size());
        List<Long> shardIds = response.stream().map(ShardInfo::getShardId).collect(Collectors.toList());

        // no replica yet
        shardIds.forEach(
                x -> Assert.assertEquals(0, starManager.getShardManager(serviceId).getShard(x).getReplicaSize()));

        // add workers
        long workerGroupId = createWorkerGroup(starManager, serviceId);
        long workerId = starManager.addWorker(serviceId, workerGroupId, "127.0.0.1:12345");

        StarletAgent agent = null;
        Worker worker = starManager.getWorkerManager().getWorker(workerId);
        try {
            agent = (StarletAgent) FieldUtils.readField(worker, "starletAgent", true);
        } catch (IllegalAccessException exception) {
            Assert.fail("Fail to get private field of Worker::starletAgent. Error:" + exception.getMessage());
        }

        Assert.assertTrue(agent instanceof MockStarletAgent);
        Map<Long, AddShardInfo> agentShards = ((MockStarletAgent) agent).getAllShards();
        // Agent has no shards yet
        Assert.assertTrue(agentShards.isEmpty());

        Shard shard = starManager.getShardManager(serviceId).getShard(shardIds.get(0));
        // shard[0] has a replica -> workerId, others doesn't have the replica
        shard.addReplica(workerId);
        WorkerInfo before = worker.toProtobuf();
        Assert.assertEquals(0, worker.getNumOfShards());
        Assert.assertEquals(0L, before.getStartTime());
        long mockStartTime = System.currentTimeMillis() - 1;
        worker.updateInfo(mockStartTime, null, 0);
        before = worker.toProtobuf();
        // give a non-zero time, so next update with a different startTime, will be treated as a restart event.
        Assert.assertEquals(mockStartTime, before.getStartTime());

        long startTime = System.currentTimeMillis();
        List<Long> junkIds = Arrays.asList(3789903L, 3789904L);
        // simulate worker restart, report empty shard ids
        starManager.processWorkerHeartbeat(serviceId, workerId, startTime, null, junkIds,
                new ArrayList<ShardReportInfo>());
        // The processWorkerHeartbeat is running async. Wait for its running completion.
        Awaitility.await().atMost(2, TimeUnit.SECONDS)
                .until(() -> TestUtils.workerManagerExecutorsIdle(starManager.getWorkerManager()));
        try {
            // use reflection to avoid adding nasty FOR-TEST-ONLY interfaces
            Scheduler scheduler = (Scheduler) FieldUtils.readField(starManager, "shardScheduler", true);
            // wait scheduler to complete its async tasks
            Awaitility.await().atMost(5, TimeUnit.SECONDS).until(scheduler::isIdle);
        } catch (Exception exception) {
            Assert.fail("Fail to get scheduler from starManager through reflection");
        }
        // junkIds will be removed from worker, and the real shardId will be added back to the worker.
        WorkerInfo after = starManager.getWorkerInfo(serviceId, workerId);
        Assert.assertEquals(startTime, after.getStartTime());
        Assert.assertTrue(shard.hasReplica(workerId));
        // validate agentShards info which is the result of worker.addShards() + worker.removeShards()
        Assert.assertEquals(1, agentShards.size());
        Assert.assertTrue(agentShards.containsKey(shard.getShardId()));
    }

    @Test
    public void testStarManagerListShardInfo() {
        String serviceName = "StarManagerTest-ListShardInfo";
        starManager.registerService(serviceTemplateName, null);
        String serviceId = starManager.bootstrapService(serviceTemplateName, serviceName);

        long workerGroupId1 = createWorkerGroup(starManager, serviceId);
        long workerGroupId2 = createWorkerGroup(starManager, serviceId);

        CreateShardGroupInfo shardGroupRequest = CreateShardGroupInfo.newBuilder()
                .setPolicy(PlacementPolicy.SPREAD)
                .build();
        List<ShardGroupInfo> shardGroupResponse =
                starManager.createShardGroup(serviceId, Collections.singletonList(shardGroupRequest));
        Assert.assertEquals(1L, shardGroupResponse.size());
        long shardGroupId = shardGroupResponse.get(0).getGroupId();
        List<Long> groupIds = Collections.singletonList(shardGroupId);

        CreateShardInfo createShardRequest = CreateShardInfo.newBuilder()
                .addGroupIds(shardGroupId)
                .build();
        List<ShardInfo> shardInfos = starManager.createShard(serviceId, Collections.singletonList(createShardRequest));
        Assert.assertEquals(1L, shardInfos.size());
        List<Long> shardIds = shardInfos.stream().map(ShardInfo::getShardId).collect(Collectors.toList());

        { // get shardInfo from shardManager directly
            List<List<ShardInfo>> shardInfoList =
                    starManager.getShardManager(serviceId).listShardInfo(groupIds, false /* withoutReplicaInfo */);
            Assert.assertEquals(1L, shardInfoList.size());
            Assert.assertEquals(1L, shardInfoList.get(0).size());
            ShardInfo info = shardInfoList.get(0).get(0);
            Assert.assertEquals((long) shardIds.get(0), info.getShardId());
            // no replica yet
            Assert.assertEquals(0L, info.getReplicaInfoCount());
        }
        // add a worker into worker group 1
        long workerId1 = starManager.addWorker(serviceId, workerGroupId1, TestHelper.generateMockWorkerIpAddress());
        // ensure the new added worker is alive
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(
                () -> starManager.getWorkerManager().getWorker(workerId1).isAlive());
        {
            // WorkerGroup1
            List<ShardInfoList> shardInfoList1 = starManager.listShardInfo(serviceId, groupIds, workerGroupId1,
                    false /* withoutReplicaInfo */);
            Assert.assertEquals(1L, shardInfoList1.size());
            Assert.assertEquals(1L, shardInfoList1.get(0).getShardInfosCount());
            ShardInfo info1 = shardInfoList1.get(0).getShardInfos(0);
            Assert.assertEquals((long) shardIds.get(0), info1.getShardId());
            Assert.assertEquals(1L, info1.getReplicaInfoCount());
            Assert.assertEquals(workerId1, info1.getReplicaInfo(0).getWorkerInfo().getWorkerId());

            // workerGroup2
            List<ShardInfoList> shardInfoList2 = starManager.listShardInfo(serviceId, groupIds, workerGroupId2,
                    false /* withoutReplicaInfo */);
            Assert.assertEquals(1L, shardInfoList2.size());
            Assert.assertEquals(1L, shardInfoList2.get(0).getShardInfosCount());
            ShardInfo info2 = shardInfoList2.get(0).getShardInfos(0);
            Assert.assertEquals((long) shardIds.get(0), info2.getShardId());
            // no replica in workerGroup2, because of no worker
            Assert.assertEquals(0L, info2.getReplicaInfoCount());

            Shard shard = starManager.getShardManager(serviceId).getShard(shardIds.get(0));
            Assert.assertEquals(1L, shard.getReplicaSize());
        }

        // test list shard info without replica info
        {
            // WorkerGroup1
            List<ShardInfoList> shardInfoList1 = starManager.listShardInfo(serviceId, groupIds, workerGroupId1,
                    true /* withoutReplicaInfo */);
            Assert.assertEquals(1L, shardInfoList1.size());
            Assert.assertEquals(1L, shardInfoList1.get(0).getShardInfosCount());
            ShardInfo info1 = shardInfoList1.get(0).getShardInfos(0);
            Assert.assertEquals((long) shardIds.get(0), info1.getShardId());
            Assert.assertEquals(0L, info1.getReplicaInfoCount());
        }

        try {
            starManager.getShardInfo(serviceId + "1", groupIds, workerGroupId1);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.NOT_EXIST);
        }
    }

    @Test
    public void testStarManagerShardGroup() {
        String serviceName = "StarManagerTest-ShardGroup";

        starManager.registerService(serviceTemplateName, null);
        String serviceId = starManager.bootstrapService(serviceTemplateName, serviceName);

        List<CreateShardGroupInfo> createShardGroupInfos = new ArrayList<>();
        createShardGroupInfos.add(CreateShardGroupInfo.newBuilder().build());
        createShardGroupInfos.add(CreateShardGroupInfo.newBuilder().build());

        // test service not exist
        try {
            starManager.createShardGroup(serviceId + "aaa", createShardGroupInfos);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.NOT_EXIST);
        }

        List<ShardGroupInfo> shardGroupInfos = starManager.createShardGroup(serviceId, createShardGroupInfos);
        Assert.assertEquals(starManager.getShardManager(serviceId).getShardGroupCount(),
                shardGroupInfos.size() + 1 /* plus default group */);
        List<Long> groupIds = shardGroupInfos.stream().map(ShardGroupInfo::getGroupId).collect(Collectors.toList());

        // add shards to group
        List<Long> groupIds1 = new ArrayList<>();
        groupIds1.add(groupIds.get(0));
        List<Long> groupIds2 = new ArrayList<>();
        groupIds2.add(groupIds.get(1));
        List<CreateShardInfo> createShardInfos = new ArrayList<>();
        createShardInfos.add(CreateShardInfo.newBuilder().addAllGroupIds(groupIds1).build());
        createShardInfos.add(CreateShardInfo.newBuilder().addAllGroupIds(groupIds2).build());
        List<ShardInfo> shardInfos = starManager.createShard(serviceId, createShardInfos);
        Assert.assertEquals(createShardInfos.size(), starManager.getShardManager(serviceId).getShardCount());

        // test service not exist
        try {
            starManager.deleteShardGroup(serviceId + "aaa", groupIds, true);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.NOT_EXIST);
        }

        starManager.deleteShardGroup(serviceId, groupIds, true);
        Assert.assertEquals(starManager.getShardManager(serviceId).getShardGroupCount(), 1 /* plus default group */);
        Assert.assertEquals(0, starManager.getShardManager(serviceId).getShardCount());

        // TODO: test delete shardgroup, keep shard
    }

    @Test
    public void testStarManagerUpdateShardGroup() {
        String serviceName = "StarManagerTest-UpdateShardGroup";

        starManager.registerService(serviceTemplateName, null);
        String serviceId = starManager.bootstrapService(serviceTemplateName, serviceName);

        List<CreateShardGroupInfo> createShardGroupInfos = new ArrayList<>();
        createShardGroupInfos.add(CreateShardGroupInfo.newBuilder().build());
        createShardGroupInfos.add(CreateShardGroupInfo.newBuilder().build());

        List<ShardGroupInfo> shardGroupInfos = starManager.createShardGroup(serviceId, createShardGroupInfos);
        List<Long> groupIds = shardGroupInfos.stream().map(ShardGroupInfo::getGroupId).collect(Collectors.toList());

        List<UpdateShardGroupInfo> updateShardGroupInfos = new ArrayList<>();
        updateShardGroupInfos.add(UpdateShardGroupInfo.newBuilder().setGroupId(groupIds.get(0))
                .setEnableCache(CacheEnableState.ENABLED).build());
        updateShardGroupInfos.add(UpdateShardGroupInfo.newBuilder().setGroupId(groupIds.get(1))
                .setEnableCache(CacheEnableState.DISABLED).build());
        starManager.updateShardGroup(serviceId, updateShardGroupInfos);

        // test non-exist serviceId
        try {
            starManager.updateShardGroup("abcd", updateShardGroupInfos);
        } catch (StarException e) {
        }

        starManager.deleteShardGroup(serviceId, groupIds, true);
        Assert.assertEquals(starManager.getShardManager(serviceId).getShardGroupCount(), 1 /* plus default group */);
        Assert.assertEquals(0, starManager.getShardManager(serviceId).getShardCount());

        // test update non-exist shard group
        try {
            starManager.updateShardGroup(serviceId, updateShardGroupInfos);
        } catch (StarException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testStarManagerListShardGroup() {
        String serviceName = "StarManagerTest-ListShardGroup";

        starManager.registerService(serviceTemplateName, null);
        String serviceId = starManager.bootstrapService(serviceTemplateName, serviceName);

        List<CreateShardGroupInfo> createShardGroupInfos = new ArrayList<>();
        createShardGroupInfos.add(CreateShardGroupInfo.newBuilder().build());
        createShardGroupInfos.add(CreateShardGroupInfo.newBuilder().build());

        List<ShardGroupInfo> shardGroupInfos = starManager.createShardGroup(serviceId, createShardGroupInfos);
        List<Long> groupIds = shardGroupInfos.stream().map(ShardGroupInfo::getGroupId).collect(Collectors.toList());

        Pair<List<ShardGroupInfo>, Long> pair = starManager.listShardGroupInfo(serviceId, false /* includeAnonymousGroup */, 0);
        List<ShardGroupInfo> shardGroupInfos2 = pair.getKey();
        List<Long> groupIds2 = shardGroupInfos2.stream().map(ShardGroupInfo::getGroupId).collect(Collectors.toList());
        Assert.assertEquals(shardGroupInfos.size() + 1 /* plus default group */, shardGroupInfos2.size());
        for (Long gid : groupIds) {
            Assert.assertTrue(groupIds2.contains(gid));
        }

        // test service not exist
        try {
            starManager.listShardGroupInfo(serviceId + "aaa", false /* includeAnonymousGroup */, 0);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.NOT_EXIST);
        }
    }

    @Test
    public void testStarManagerGetShardGroup() {
        String serviceName = "StarManagerTest-GetShardGroup";

        starManager.registerService(serviceTemplateName, null);
        String serviceId = starManager.bootstrapService(serviceTemplateName, serviceName);

        List<CreateShardGroupInfo> createShardGroupInfos = new ArrayList<>();
        createShardGroupInfos.add(CreateShardGroupInfo.newBuilder().build());
        createShardGroupInfos.add(CreateShardGroupInfo.newBuilder().build());

        List<ShardGroupInfo> shardGroupInfos = starManager.createShardGroup(serviceId, createShardGroupInfos);
        List<Long> groupIds = shardGroupInfos.stream().map(ShardGroupInfo::getGroupId).collect(Collectors.toList());

        List<ShardGroupInfo> shardGroupInfos2 = starManager.getShardGroupInfo(serviceId, groupIds);
        for (int i = 0; i < 2; ++i) {
            Assert.assertEquals(shardGroupInfos.get(i).getGroupId(), shardGroupInfos2.get(i).getGroupId());
        }

        // test service not exist
        try {
            starManager.getShardGroupInfo(serviceId + "aaa", groupIds);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.NOT_EXIST);
        }
    }

    @Test
    public void testMetaGroup() {
        String serviceName = "StarManagerTest-MetaGroup";

        starManager.registerService(serviceTemplateName, null);
        String serviceId = starManager.bootstrapService(serviceTemplateName, serviceName);

        // test list meta group before creation
        Assert.assertEquals(starManager.listMetaGroupInfo(serviceId).size(), 0);

        // create shard group
        List<CreateShardGroupInfo> createShardGroupInfos = new ArrayList<>();
        createShardGroupInfos.add(CreateShardGroupInfo.newBuilder().build());
        List<ShardGroupInfo> shardGroupInfos = starManager.createShardGroup(serviceId, createShardGroupInfos);

        // create a shard in this shard group
        List<Long> groupIds = new ArrayList<>();
        groupIds.add(shardGroupInfos.get(0).getGroupId());
        List<CreateShardInfo> createShardInfos = new ArrayList<>();
        createShardInfos.add(CreateShardInfo.newBuilder().addAllGroupIds(groupIds).build());
        List<ShardInfo> shardInfos = starManager.createShard(serviceId, createShardInfos);

        // test create meta group
        CreateMetaGroupInfo createInfo = CreateMetaGroupInfo.newBuilder()
                .setPlacementPolicy(PlacementPolicy.PACK)
                .build();
        MetaGroupInfo metaGroupInfo = starManager.createMetaGroup(serviceId, createInfo);
        Assert.assertEquals(metaGroupInfo.getServiceId(), serviceId);
        Assert.assertNotEquals(metaGroupInfo.getMetaGroupId(), 0);
        Assert.assertEquals(metaGroupInfo.getShardGroupIdsList().size(), 0);
        Assert.assertEquals(metaGroupInfo.getPlacementPolicy(), PlacementPolicy.PACK);
        // test service not exist
        try {
            starManager.createMetaGroup(serviceId + "aaa", createInfo);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.NOT_EXIST);
        }

        // test list meta group after creation
        Assert.assertEquals(starManager.listMetaGroupInfo(serviceId).size(), 1);

        // test update meta group
        JoinMetaGroupInfo joinInfo = JoinMetaGroupInfo.newBuilder()
                .setMetaGroupId(metaGroupInfo.getMetaGroupId())
                .build();
        UpdateMetaGroupInfo updateInfo = UpdateMetaGroupInfo.newBuilder()
                .setJoinInfo(joinInfo)
                .addAllShardGroupIds(groupIds)
                .build();
        starManager.updateMetaGroup(serviceId, updateInfo);
        // test service not exist
        try {
            starManager.updateMetaGroup(serviceId + "aaa", updateInfo);
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.NOT_EXIST);
        }

        // test delete meta group
        starManager.deleteMetaGroup(serviceId, metaGroupInfo.getMetaGroupId());
        // test service not exist
        try {
            starManager.deleteMetaGroup(serviceId + "aaa", metaGroupInfo.getMetaGroupId());
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.NOT_EXIST);
        }

        // test get meta group
        try {
            starManager.getMetaGroupInfo(serviceId, metaGroupInfo.getMetaGroupId());
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.NOT_EXIST);
        }
        // test service not exist
        try {
            starManager.getMetaGroupInfo(serviceId + "aaa", metaGroupInfo.getMetaGroupId());
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.NOT_EXIST);
        }
        // test list meta group
        Assert.assertEquals(starManager.listMetaGroupInfo(serviceId).size(), 0);
        // test service not exist
        try {
            starManager.listMetaGroupInfo(serviceId + "aaa");
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.NOT_EXIST);
        }
    }

    @Test
    public void testStarManagerAllocateStorage() {
        String serviceName = "StarManagerTest-1";

        starManager.registerService(serviceTemplateName, null);
        String serviceId = starManager.bootstrapService(serviceTemplateName, serviceName);

        FilePathInfo filePathInfo = starManager.allocateFilePath(serviceId, FileStoreType.S3, "123",
                Constant.S3_FSKEY_FOR_CONFIG, "");
        Assert.assertEquals(String.format("s3://%s/%s/123", Config.S3_BUCKET, serviceId),
                filePathInfo.getFullPath());

        try {
            starManager.allocateFilePath(serviceId + "1", FileStoreType.S3, "123", Constant.S3_FSKEY_FOR_CONFIG, "");
        } catch (StarException e) {
            Assert.assertEquals(e.getExceptionCode(), ExceptionCode.NOT_EXIST);
        }

        // test custom root dir
        String customRootDir = "test_root_dir";
        filePathInfo = starManager.allocateFilePath(serviceId, FileStoreType.S3, "123",
                Constant.S3_FSKEY_FOR_CONFIG, customRootDir);
        Assert.assertEquals(String.format("s3://%s/%s/123", Config.S3_BUCKET, customRootDir),
                filePathInfo.getFullPath());
    }

    @Test
    public void testStarManagerFileMeta() {
        String serviceName = "StarManagerTest-1";
        String ipPort = "192.168.1.1:2345";
        String ipPort2 = "192.168.1.1:3456";

        LeaderInfo info = LeaderInfo.newBuilder()
                .setHost("FakeLeaderHost")
                .setPort(1345)
                .build();

        StarManager starManager1 = new StarManager();

        starManager1.setListenAddressInfo(info.getHost(), info.getPort());
        starManager1.becomeLeader();

        // prepare service
        starManager1.registerService(serviceTemplateName, null);
        String serviceId = starManager1.bootstrapService(serviceTemplateName, serviceName);
        long workerGroupId = createWorkerGroup(starManager1, serviceId);
        // prepare worker
        long workerId = starManager1.addWorker(serviceId, workerGroupId, ipPort);
        // prepare shard
        List<CreateShardInfo> createShardInfos = new ArrayList<>();
        createShardInfos.add(CreateShardInfo.newBuilder().build());
        createShardInfos.add(CreateShardInfo.newBuilder().build());
        List<ShardInfo> shardInfos = starManager1.createShard(serviceId, createShardInfos);
        // prepare shard group
        List<CreateShardGroupInfo> createShardGroupInfos = new ArrayList<>();
        createShardGroupInfos.add(CreateShardGroupInfo.newBuilder().build());
        createShardGroupInfos.add(CreateShardGroupInfo.newBuilder().build());
        starManager1.createShardGroup(serviceId, createShardGroupInfos);
        // prepare file store
        AwsCredential credential = new AwsDefaultCredential();
        S3FileStore s3fs = new S3FileStore("", "test_name", "bucket", "region",
                "endpoint", credential, "/");
        HDFSFileStore hdfs = new HDFSFileStore("", "test_name", "url");
        String s3FsKey = starManager1.addFileStore(serviceId, s3fs.toProtobuf());
        String hdfsFsKey = starManager1.addFileStore(serviceId, hdfs.toProtobuf());

        CreateMetaGroupInfo createInfo = CreateMetaGroupInfo.newBuilder()
                .setPlacementPolicy(PlacementPolicy.PACK)
                .build();
        starManager1.createMetaGroup(serviceId, createInfo);

        // this is the base Image
        ByteArrayOutputStream baseImageOut = new ByteArrayOutputStream();
        try {
            starManager1.dumpMeta(baseImageOut);
            baseImageOut.flush();
            baseImageOut.close();
        } catch (Exception e) {
            Assert.fail();
        }
        starManager1.becomeFollower();

        // simulate the checkpoint behavior in production: a new StarManager instance is created and its state
        // is restored from the image + EditLog and then dump to a new image
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        {
            StarManager mgr = new StarManager();
            InputStream in = new ByteArrayInputStream(baseImageOut.toByteArray());
            try {
                mgr.loadMeta(in);
            } catch (Exception ex) {
                Assert.fail("Unexpected exception: " + ex.getMessage());
            }
            mgr.becomeFollower();
            out = new ByteArrayOutputStream();
            try {
                mgr.dumpMeta(out);
                out.flush();
                out.close();
            } catch (Exception e) {
                Assert.fail();
            }
        }

        { // test mgr restore meta info, including the leader's info
            StarManager mgr = new StarManager();
            InputStream in = new ByteArrayInputStream(out.toByteArray());
            try {
                mgr.loadMeta(in);
            } catch (Exception ex) {
                Assert.fail("Unexpected exception: " + ex.getMessage());
            }
            mgr.becomeFollower();
            StarException exception = Assert.assertThrows(StarException.class, () -> mgr.registerService("abc", null));
            Assert.assertNotNull(exception);
            Assert.assertTrue(exception instanceof NotLeaderStarException);
            // respond the leader is the starmanager1's address
            Assert.assertEquals(info.toString(), ((NotLeaderStarException) exception).getLeaderInfo().toString());
        }

        // load meta
        StarManager starManager2 = new StarManager();
        starManager2.becomeLeader();
        try {
            InputStream in = new ByteArrayInputStream(out.toByteArray());
            starManager2.loadMeta(in);
        } catch (Exception e) {
            Assert.fail();
        }
        // verify meta
        // verify service meta
        Assert.assertEquals(starManager1.getServiceManager().getServiceCount(),
                starManager2.getServiceManager().getServiceCount());
        Assert.assertEquals(starManager1.getServiceManager().getServiceTemplateCount(),
                starManager2.getServiceManager().getServiceTemplateCount());
        ServiceInfo serviceInfo1 = starManager1.getServiceInfoById(serviceId);
        ServiceInfo serviceInfo2 = starManager2.getServiceInfoById(serviceId);
        Assert.assertEquals(serviceInfo1.getServiceTemplateName(), serviceInfo2.getServiceTemplateName());
        Assert.assertEquals(serviceInfo1.getServiceName(), serviceInfo2.getServiceName());
        Assert.assertEquals(serviceInfo1.getServiceId(), serviceInfo2.getServiceId());

        // verify worker meta
        Assert.assertEquals(starManager1.getWorkerManager().getWorkerCount(),
                starManager2.getWorkerManager().getWorkerCount());
        WorkerInfo workerInfo1 = starManager1.getWorkerInfo(serviceId, workerId);
        WorkerInfo workerInfo2 = starManager2.getWorkerInfo(serviceId, workerId);
        Assert.assertEquals(workerInfo1.getServiceId(), workerInfo2.getServiceId());
        Assert.assertEquals(workerInfo1.getGroupId(), workerInfo2.getGroupId());
        Assert.assertEquals(workerInfo1.getWorkerId(), workerInfo2.getWorkerId());
        Assert.assertEquals(workerInfo1.getIpPort(), workerInfo2.getIpPort());

        // verify shard meta
        Assert.assertEquals(starManager1.getShardManager(serviceId).getShardCount(),
                starManager2.getShardManager(serviceId).getShardCount());
        List<Long> shardIds = shardInfos.stream().map(ShardInfo::getShardId).collect(Collectors.toList());
        List<ShardInfo> shardInfos1 = starManager1.getShardManager(serviceId).getShardInfo(shardIds);
        List<ShardInfo> shardInfos2 = starManager2.getShardManager(serviceId).getShardInfo(shardIds);

        for (int i = 0; i < shardInfos.size(); ++i) {
            ShardInfo shard1 = shardInfos1.get(i);
            ShardInfo shard2 = shardInfos2.get(i);
            Assert.assertEquals(shard1.getServiceId(), shard2.getServiceId());
            Assert.assertEquals(shard1.getGroupIdsList(), shard2.getGroupIdsList());
            Assert.assertEquals(shard1.getShardId(), shard2.getShardId());
        }

        // verify shard group meta
        Assert.assertEquals(starManager1.getShardManager(serviceId).getShardGroupCount(),
                starManager2.getShardManager(serviceId).getShardGroupCount());

        // verify meta group meta
        Assert.assertEquals(starManager1.getShardManager(serviceId).getMetaGroupCount(),
                starManager2.getShardManager(serviceId).getMetaGroupCount());

        // verify file store meta
        Assert.assertNotNull(starManager2.getFileStoreMgr(serviceId).getFileStore(Constant.S3_FSKEY_FOR_CONFIG));
        Assert.assertNotNull(starManager2.getFileStoreMgr(serviceId).getFileStore(Constant.HDFS_FSKEY_FOR_CONFIG));
        Assert.assertNotNull(starManager2.getFileStoreMgr(serviceId).getFileStore(s3FsKey));
        Assert.assertEquals(starManager1.getFileStoreMgr(serviceId).getFileStore(s3FsKey).key(),
                starManager2.getFileStoreMgr(serviceId).getFileStore(s3FsKey).key());
        Assert.assertNotNull(starManager2.getFileStoreMgr(serviceId).getFileStore(hdfsFsKey));
        Assert.assertEquals(starManager1.getFileStoreMgr(serviceId).getFileStore(hdfsFsKey).key(),
                starManager2.getFileStoreMgr(serviceId).getFileStore(hdfsFsKey).key());

        // test default group works fine
        starManager2.createShard(serviceId, createShardInfos);
        starManager2.addWorker(serviceId, workerGroupId, ipPort2);
        starManager2.becomeFollower();

        // test meta file corrupted - protobuf corruption
        try {
            byte[] corruptedData = out.toByteArray();
            for (int i = corruptedData.length / 2; i < corruptedData.length; ++i) {
                corruptedData[i] = Byte.MAX_VALUE;
            }
            InputStream in = new ByteArrayInputStream(corruptedData);
            StarManager starManager3 = new StarManager();
            Assert.assertThrows(IOException.class, () -> starManager3.loadMeta(in));
        } catch (Exception e) {
            Assert.fail();
        }

        // test meta file corrupted - mismatch length
        try {
            byte[] data = out.toByteArray();
            InputStream in = new ByteArrayInputStream(data, 0, data.length - 1);
            StarManager starManager3 = new StarManager();
            Assert.assertThrows(IOException.class, () -> starManager3.loadMeta(in));
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testStarManagerMetaChecksumValidation() {
        String serviceName = "StarManagerTest-1";
        StarManager starManager = new StarManager();
        starManager.becomeLeader();

        // add service
        starManager.registerService(serviceTemplateName, null);
        String serviceId = starManager.bootstrapService(serviceTemplateName, serviceName);

        // add workerGroup
        long workerGroupId = createWorkerGroup(starManager, serviceId);
        starManager.addWorker(serviceId, workerGroupId, TestHelper.generateMockWorkerIpAddress());
        starManager.addWorker(serviceId, workerGroupId, TestHelper.generateMockWorkerIpAddress());

        // create shard
        CreateShardInfo createShardRequest = CreateShardInfo.newBuilder().build();
        starManager.createShard(serviceId, Collections.nCopies(3, createShardRequest));

        // create shard group
        CreateShardGroupInfo createShardGroupRequest = CreateShardGroupInfo.newBuilder().build();
        starManager.createShardGroup(serviceId, Collections.nCopies(3, createShardGroupRequest));

        CreateMetaGroupInfo createInfo = CreateMetaGroupInfo.newBuilder()
                .setPlacementPolicy(PlacementPolicy.PACK)
                .build();
        starManager.createMetaGroup(serviceId, createInfo);
        starManager.becomeFollower();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            starManager.dumpMeta(out);
        } catch (Exception e) {
            Assert.fail();
        }

        int length = out.toByteArray().length;
        for (int i = 0; i < length - 1; ++i) {
            byte[] inData = out.toByteArray();
            // corrupt one byte
            inData[i] = (byte) (0xFF & ~inData[i]);
            ByteArrayInputStream in = new ByteArrayInputStream(inData);
            StarManager starManager2 = new StarManager();
            // expect an exception if any byte is changed, no matter what exception it is.
            Assert.assertThrows(
                    String.format("Expect exception when corrupt the %dth byte of total %d bytes", i, length),
                    Throwable.class, () -> starManager2.loadMeta(in));
        }
    }

    @Test
    public void testStarManagerFileMetaEmpty() {
        InputStream in = new ByteArrayInputStream(new byte[0]);
        try {
            starManager.loadMeta(in);
        } catch (IOException exception) {
            Assert.fail();
        }
    }

    @Test
    public void testStarManagerFileMetaWrongRawHeader() {
        int len = StarManager.IMAGE_META_MAGIC_BYTES.length;
        byte[] magic = Arrays.copyOf(StarManager.IMAGE_META_MAGIC_BYTES, len);
        magic[len - 1] = (byte) (0xFF & ~magic[len - 1]);
        ByteArrayInputStream in = new ByteArrayInputStream(magic);
        IOException thrown = Assert.assertThrows(IOException.class, () -> starManager.loadMeta(in));
        Assert.assertEquals(thrown.getMessage(), "verify star manager meta file raw header failed, meta is not valid.");
    }

    @Test
    public void testStarManagerNotLeader() {
        String serviceName = "StarManagerTest-1";
        String serviceId = "987";
        String ipPort = "127.0.0.1:1234";

        starManager = new StarManager();

        {
            StarException thrown = Assert.assertThrows(StarException.class, () -> {
                starManager.registerService(serviceTemplateName, null);
            });
            Assert.assertEquals(ExceptionCode.NOT_LEADER, thrown.getExceptionCode());
        }
        {
            StarException thrown = Assert.assertThrows(StarException.class, () -> {
                starManager.deregisterService(serviceTemplateName);
            });
            Assert.assertEquals(ExceptionCode.NOT_LEADER, thrown.getExceptionCode());
        }
        {
            StarException thrown = Assert.assertThrows(StarException.class, () -> {
                starManager.bootstrapService(serviceTemplateName, serviceName);
            });
            Assert.assertEquals(ExceptionCode.NOT_LEADER, thrown.getExceptionCode());
        }
        {
            StarException thrown = Assert.assertThrows(StarException.class, () -> {
                starManager.shutdownService(serviceId);
            });
            Assert.assertEquals(ExceptionCode.NOT_LEADER, thrown.getExceptionCode());
        }
        {
            StarException thrown = Assert.assertThrows(StarException.class, () -> {
                starManager.addWorker(serviceId, Constant.DEFAULT_ID, ipPort);
            });
            Assert.assertEquals(ExceptionCode.NOT_LEADER, thrown.getExceptionCode());
        }
        {
            StarException thrown = Assert.assertThrows(StarException.class, () -> {
                starManager.removeWorker(serviceId, Constant.DEFAULT_ID, 1);
            });
            Assert.assertEquals(ExceptionCode.NOT_LEADER, thrown.getExceptionCode());
        }
        {
            StarException thrown = Assert.assertThrows(StarException.class, () -> {
                List<CreateShardInfo> createShardInfos = new ArrayList<>();
                createShardInfos.add(CreateShardInfo.newBuilder().build());
                starManager.createShard(serviceId, createShardInfos);
            });
            Assert.assertEquals(ExceptionCode.NOT_LEADER, thrown.getExceptionCode());
        }
        {
            StarException thrown = Assert.assertThrows(StarException.class, () -> {
                List<Long> shardIds = new ArrayList<>();
                shardIds.add(1L);
                starManager.deleteShard(serviceId, shardIds);
            });
            Assert.assertEquals(ExceptionCode.NOT_LEADER, thrown.getExceptionCode());
        }
        {
            StarException thrown = Assert.assertThrows(StarException.class, () -> {
                List<CreateShardGroupInfo> createShardGroupInfos = new ArrayList<>();
                starManager.createShardGroup(serviceId, createShardGroupInfos);
            });
            Assert.assertEquals(ExceptionCode.NOT_LEADER, thrown.getExceptionCode());
        }
        {
            StarException thrown = Assert.assertThrows(StarException.class, () -> {
                List<Long> groupIds = new ArrayList<>();
                starManager.deleteShardGroup(serviceId, groupIds, true);
            });
            Assert.assertEquals(ExceptionCode.NOT_LEADER, thrown.getExceptionCode());
        }
        {
            StarException thrown = Assert.assertThrows(StarException.class, () -> {
                CreateMetaGroupInfo info = CreateMetaGroupInfo.newBuilder().build();
                starManager.createMetaGroup(serviceId, info);
            });
            Assert.assertEquals(ExceptionCode.NOT_LEADER, thrown.getExceptionCode());
        }
        {
            StarException thrown = Assert.assertThrows(StarException.class, () -> {
                starManager.deleteMetaGroup(serviceId, 0);
            });
            Assert.assertEquals(ExceptionCode.NOT_LEADER, thrown.getExceptionCode());
        }
        {
            StarException thrown = Assert.assertThrows(StarException.class, () -> {
                UpdateMetaGroupInfo info = UpdateMetaGroupInfo.newBuilder().build();
                starManager.updateMetaGroup(serviceId, info);
            });
            Assert.assertEquals(ExceptionCode.NOT_LEADER, thrown.getExceptionCode());
        }
        {
            StarException thrown = Assert.assertThrows(StarException.class, () -> {
                FilePathInfo info = starManager.allocateFilePath(serviceId, FileStoreType.S3, "",
                        Constant.S3_FSKEY_FOR_CONFIG, "");
            });
            Assert.assertEquals(ExceptionCode.NOT_LEADER, thrown.getExceptionCode());
        }
    }

    @Test
    public void testWorkerGroupOperations() {
        String serviceName = "StarManagerTest-WorkerGroupOptions";
        starManager.registerService(serviceTemplateName, null);
        String serviceId = starManager.bootstrapService(serviceTemplateName, serviceName);

        String owner = "StarManagerTestOwner";
        WorkerGroupSpec spec = WorkerGroupSpec.newBuilder().setSize("XL").build();
        Map<String, String> labels = ImmutableMap.of("X1", "Y1", "X2", "Y2");
        Map<String, String> props = ImmutableMap.of("p1", "v1", "p2", "v2");

        // create workerGroup
        long workerGroupId = starManager.createWorkerGroup(serviceId, owner, spec, labels, props, 2 /* replicaNumber */,
                ReplicationType.ASYNC, WarmupLevel.WARMUP_NOT_SET);
        {
            WorkerGroup group = starManager.getWorkerManager().getWorkerGroup(serviceId, workerGroupId);
            Assert.assertEquals(serviceId, group.getServiceId());
            Assert.assertEquals(owner, group.getOwner());
            Assert.assertEquals(labels, group.getLabels());
            Assert.assertEquals(props, group.getProperties());
            Assert.assertEquals(spec, group.getSpec());
            Assert.assertEquals(2, group.getReplicaNumber());
            Assert.assertEquals(ReplicationType.ASYNC, group.getReplicationType());
        }

        WorkerGroupSpec updateSpec = WorkerGroupSpec.newBuilder().setSize("M").build();
        Map<String, String> updateLabels = ImmutableMap.of("XX1", "YY1");
        Map<String, String> updateProps = ImmutableMap.of("pp1", "vv1");
        int updateReplicaNumber = 3;


        { // Update workerGroup
            starManager.updateWorkerGroup(serviceId, workerGroupId, updateSpec, updateLabels, updateProps,
                    updateReplicaNumber, ReplicationType.SYNC, WarmupLevel.WARMUP_NOT_SET);
            WorkerGroup group = starManager.getWorkerManager().getWorkerGroup(serviceId, workerGroupId);
            Assert.assertEquals(serviceId, group.getServiceId());
            Assert.assertEquals(owner, group.getOwner());
            Assert.assertEquals(updateLabels, group.getLabels());
            Assert.assertEquals(updateProps, group.getProperties());
            Assert.assertEquals(updateSpec, group.getSpec());
            Assert.assertEquals(updateReplicaNumber, group.getReplicaNumber());
            Assert.assertEquals(ReplicationType.SYNC, group.getReplicationType());
        }

        { // List workerGroup by ids
            List<WorkerGroupDetailInfo> lists = starManager.listWorkerGroups(
                    serviceId, Collections.singletonList(workerGroupId), Collections.emptyMap(), false);
            Assert.assertEquals(1L, lists.size());
            WorkerGroupDetailInfo info = lists.get(0);

            Assert.assertEquals(serviceId, info.getServiceId());
            Assert.assertEquals(owner, info.getOwner());
            Assert.assertEquals(updateLabels, info.getLabelsMap());
            Assert.assertEquals(updateProps, info.getPropertiesMap());
            Assert.assertEquals(updateSpec, info.getSpec());
        }
        { // List workerGroup by labels
            List<WorkerGroupDetailInfo> lists = starManager.listWorkerGroups(serviceId, Collections.emptyList(),
                    ImmutableMap.of("XX1", "YY1"), false);
            Assert.assertEquals(1L, lists.size());
            WorkerGroupDetailInfo info = lists.get(0);

            Assert.assertEquals(serviceId, info.getServiceId());
            Assert.assertEquals(owner, info.getOwner());
            Assert.assertEquals(updateLabels, info.getLabelsMap());
            Assert.assertEquals(updateProps, info.getPropertiesMap());
            Assert.assertEquals(updateSpec, info.getSpec());
        }
        { // labels value not match
            Map<String, String> filterLabels = ImmutableMap.of("XX1", "YYY1");
            Assert.assertThrows(NotExistStarException.class, () -> starManager.listWorkerGroups(
                    serviceId, Collections.emptyList(), filterLabels, false));
        }
        { // list with both group id and labels
            Assert.assertThrows(InvalidArgumentStarException.class, () -> starManager.listWorkerGroups(serviceId,
                    Collections.singletonList(workerGroupId), ImmutableMap.of("XX1", "YY1"), false));
        }

        { // delete worker group
            long workerId = starManager.addWorker(serviceId, workerGroupId, TestHelper.generateMockWorkerIpAddress());
            Assert.assertNotNull(starManager.getWorkerInfo(serviceId, workerId));

            starManager.deleteWorkerGroup(serviceId, workerGroupId);
            // the worker group is deleted
            Assert.assertNull(starManager.getWorkerManager().getWorkerGroupNoException(serviceId, workerGroupId));
            // the worker inside the worker group is deleted as well.
            StarException exception = Assert.assertThrows(StarException.class,
                    () -> starManager.getWorkerInfo(serviceId, workerId));
            Assert.assertEquals(ExceptionCode.NOT_EXIST, exception.getExceptionCode());
        }
    }

    @Test
    public void testAddFileStore() {
        String serviceName = "StarManagerTest-AddFileStore";
        starManager.registerService(serviceTemplateName, null);
        String serviceId = starManager.bootstrapService(serviceTemplateName, serviceName);

        AwsCredential credential = new AwsDefaultCredential();
        String s3FsKey = "bucket";
        S3FileStore s3fs = new S3FileStore(s3FsKey, "test_name", "bucket", "region",
                "endpoint", credential, "/");
        Assert.assertEquals(s3FsKey, starManager.addFileStore(serviceId, s3fs.toProtobuf()));
        Assert.assertEquals(s3FsKey, starManager.getFileStore(serviceId, "", s3FsKey).getFsKey());

        s3fs = new S3FileStore("", "test_name", "bucket", "region", "endpoint", credential, "/");
        s3FsKey = starManager.addFileStore(serviceId, s3fs.toProtobuf());
        Assert.assertEquals(s3FsKey, starManager.getFileStore(serviceId, "", s3FsKey).getFsKey());

        String hdfsKey = "url";
        HDFSFileStore hdfs = new HDFSFileStore(hdfsKey, "test_name", "url");
        Assert.assertEquals(hdfsKey, starManager.addFileStore(serviceId, hdfs.toProtobuf()));
        Assert.assertEquals(hdfsKey, starManager.getFileStore(serviceId, "", hdfsKey).getFsKey());

        hdfs = new HDFSFileStore("", "test_name", "url");
        hdfsKey = starManager.addFileStore(serviceId, hdfs.toProtobuf());
        Assert.assertEquals(hdfsKey, starManager.getFileStore(serviceId, "", hdfsKey).getFsKey());
    }

    @Test
    public void testStarManagerRemoveShardGroupReplicas() {
        String serviceName = "StarManagerTest-1";
        starManager.registerService(serviceTemplateName, null);
        String serviceId = starManager.bootstrapService(serviceTemplateName, serviceName);

        List<CreateShardGroupInfo> createShardGroupInfos = new ArrayList<>();
        createShardGroupInfos.add(CreateShardGroupInfo.newBuilder().build());
        List<ShardGroupInfo> shardGroupInfos = starManager.createShardGroup(serviceId, createShardGroupInfos);
        Assert.assertEquals(starManager.getShardManager(serviceId).getShardGroupCount(),
                shardGroupInfos.size() + 1 /* plus default group */);
        long shardGroupId = shardGroupInfos.get(0).getGroupId();

        // test non-exist seviceId
        StarException jsonSerializeError = Assert.assertThrows(
                StarException.class,
                () -> starManager.removeShardGroupReplicas("aaa", shardGroupId));
        Assert.assertEquals(jsonSerializeError.getMessage(), "service aaa not exist");

        // add shards to group
        List<CreateShardInfo> createShardInfos = new ArrayList<>();
        createShardInfos.add(CreateShardInfo.newBuilder().addAllGroupIds(Collections.singletonList(shardGroupId)).build());
        List<ShardInfo> shardInfos = starManager.createShard(serviceId, createShardInfos);
        Assert.assertEquals(createShardInfos.size(), starManager.getShardManager(serviceId).getShardCount());

        // prepare shard replicas
        Shard shard = starManager.getShardManager(serviceId).getShard(shardInfos.get(0).getShardId());
        Replica replica = new Replica(123);
        List<Replica> replicas = new ArrayList<>();
        replicas.add(replica);
        { // remove replicas normally
            shard.setReplicas(replicas);
            Assert.assertNotEquals(0, shard.getReplica().size());
            starManager.removeShardGroupReplicas(serviceId, shardGroupId);
            Assert.assertEquals(0, shard.getReplica().size());
        }
        { // test if replicas can be set back when an exception is thrown
            new MockUp<StarMgrJournal>() {
                @Mock
                public Journal logUpdateShard(String serviceId, List<Shard> shards) throws StarException {
                    throw new StarException(ExceptionCode.IO, "Mocked exception");
                }
            };
            shard.setReplicas(replicas);
            int replicaSize = shard.getReplica().size();
            Assert.assertNotEquals(0, replicaSize);
            Assert.assertThrows(StarException.class, () -> starManager.removeShardGroupReplicas(serviceId, shardGroupId));
            Assert.assertEquals(replicaSize, shard.getReplica().size());
        }
    }

    @Test
    public void testCheckAndUpdateShard() {
        TestHelper helper = new TestHelper(serviceTemplateName, starManager);
        helper.createDefaultServiceAndWorkerGroup(this.getClass().getName() + "-serviceName-0");
        helper.setDefaultWorkerGroupReplicaNumber(3);
        helper.setDefaultWorkerGroupReplication(ReplicationType.NO_REPLICATION);

        // create a shard
        long shardId1 = helper.createTestShard(0);
        Shard shard1 = helper.getDefaultShardManager().getShard(shardId1);
        // create a worker
        long workerId = helper.createTestWorkerToWorkerGroup(helper.getDefaultWorkerGroupId());

        int hashCode = shard1.hashCode();
        // reported shardInfo.hashCode match, don't need to trigger shard updating
        List<Long> shardIds = Collections.singletonList(shardId1);
        List<ShardReportInfo> shardReportInfos = new ArrayList<>();
        for (Long shardId : shardIds) {
            shardReportInfos.add(ShardReportInfo.newBuilder().setShardId(shardId)
                    .setHashCode(hashCode).build());
        }

        Scheduler scheduler = starManager.getScheduler();
        new Expectations(scheduler) {
            {
                // don't expect invoke scheduleAsyncAddToWorker()
                scheduler.scheduleAsyncAddToWorker(anyString, (List<Long>) any, anyLong);
                minTimes = 0;
                maxTimes = 0;
            }
        };
        starManager.checkAndUpdateShard(starManager.getServiceManager(), helper.getDefaultServiceId(), workerId,
                shardReportInfos);

        // reported shardInfo.enable_cache mismatch, need to trigger shard updating
        shardReportInfos.clear();
        for (Long shardId : shardIds) {
            shardReportInfos.add(ShardReportInfo.newBuilder().setShardId(shardId)
                    .setHashCode(1).build());
        }

        new Expectations(scheduler) {
            {
                scheduler.scheduleAsyncAddToWorker(anyString, (List<Long>) any, anyLong);
                minTimes = 1;
                maxTimes = 1;
            }
        };
        starManager.checkAndUpdateShard(starManager.getServiceManager(), helper.getDefaultServiceId(), workerId,
                shardReportInfos);

        new Verifications() {
            {
                List<List<Long>> shardIdResults = new ArrayList<>();
                List<Long> workerIdResults = new ArrayList<>();
                scheduler.scheduleAsyncAddToWorker(anyString, withCapture(shardIdResults), withCapture(workerIdResults));
                Assert.assertEquals(1, shardIdResults.size());
                Assert.assertEquals(1, shardIdResults.get(0).size());
                Assert.assertEquals(shardId1, (long) shardIdResults.get(0).get(0));
                Assert.assertEquals(1, workerIdResults.size());
                Assert.assertEquals(workerId, (long) workerIdResults.get(0));
            }
        };
    }

    @Test
    public void testCheckAndUpdateShardWithReplicaState() {
        TestHelper helper = new TestHelper(serviceTemplateName, starManager);
        helper.createDefaultServiceAndWorkerGroup(this.getClass().getName() + "-serviceName-0");
        helper.setDefaultWorkerGroupReplicaNumber(3);
        helper.setDefaultWorkerGroupWarmupLevel(WarmupLevel.WARMUP_META);
        helper.setDefaultWorkerGroupReplication(ReplicationType.SYNC);
        Scheduler scheduler = starManager.getScheduler();

        // create a shard
        long shardId1 = helper.createTestShard(0);
        Shard shard1 = helper.getDefaultShardManager().getShard(shardId1);

        // create a worker
        long workerId = helper.createTestWorkerToWorkerGroup(helper.getDefaultWorkerGroupId());
        StarletAgent agent = helper.getMockWorkerStarletAgent(workerId);
        Assert.assertTrue(agent instanceof MockStarletAgent);

        // create a replica in the workergroup
        scheduler.scheduleAddToGroup(shard1.getServiceId(), shardId1, helper.getDefaultWorkerGroupId());
        Assert.assertEquals(1L, shard1.getReplicaSize());

        int hashCode = shard1.hashCode();
        // reported shardInfo.hashCode match, don't need to trigger shard updating
        List<Long> shardIds = Collections.singletonList(shardId1);
        List<ShardReportInfo> shardReportInfos = new ArrayList<>();
        for (Long shardId : shardIds) {
            shardReportInfos.add(ShardReportInfo.newBuilder().setShardId(shardId)
                    .setHashCode(hashCode).build());
        }
        starManager.checkAndUpdateShard(starManager.getServiceManager(), helper.getDefaultServiceId(), workerId,
                shardReportInfos);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(scheduler::isIdle);
        AddShardInfo info = ((MockStarletAgent) agent).getAllShards().get(shardId1);
        // the replica info is sent to the starlet along in the first AddShardRequest
        Assert.assertEquals(1L, info.getReplicaInfoCount());

        // Now create a new worker and do the schedule, a new replica is created for the shard
        helper.createTestWorkerToWorkerGroup(helper.getDefaultWorkerGroupId());
        scheduler.scheduleAddToGroup(shard1.getServiceId(), shardId1, helper.getDefaultWorkerGroupId());
        Assert.assertEquals(2L, shard1.getReplicaSize());

        // use the old info to updateShard
        starManager.checkAndUpdateShard(starManager.getServiceManager(), helper.getDefaultServiceId(), workerId,
                shardReportInfos);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(scheduler::isIdle);

        // Verify the MockStarletAgent, it should contain two replica in the AddShardInfo
        info = ((MockStarletAgent) agent).getAllShards().get(shardId1);
        Assert.assertEquals(2L, info.getReplicaInfoCount());
    }

    @Test
    public void testGetShardInfoWithDifferentReplicaState() {
        // Get different replica list according to the replicas' State.
        // setup environment
        TestHelper helper = new TestHelper(serviceTemplateName, starManager);
        helper.createDefaultServiceAndWorkerGroup(this.getClass().getName() + "-serviceName-0");
        long workerGroupId = helper.getDefaultWorkerGroupId();
        helper.setDefaultWorkerGroupReplicaNumber(3);

        // create 5 workers to the default workerGroup
        List<Long> workerIds = helper.createTestWorkersToWorkerGroup(5, workerGroupId);

        { // shard has 3 REPLICA_OK. all the replicas can be retrieved from getShardInfo interface
            // create a shard
            long shardId = helper.createTestShard(0);
            Shard shard = helper.getDefaultShardManager().getShard(shardId);
            Assert.assertTrue(shard.addReplica(workerIds.get(0))); // REPLICA_OK
            Assert.assertTrue(shard.addReplica(workerIds.get(1))); // REPLICA_OK
            Assert.assertTrue(shard.addReplica(workerIds.get(2))); // REPLICA_OK

            List<ShardInfo> shardListResult =
                    starManager.getShardInfo(helper.getDefaultServiceId(), Collections.singletonList(shardId),
                            workerGroupId);
            Assert.assertEquals(1, shardListResult.size());
            ShardInfo shardInfo = shardListResult.get(0);
            Assert.assertEquals(3, shardInfo.getReplicaInfoCount());
            for (ReplicaInfo replicaInfo : shardInfo.getReplicaInfoList()) {
                Assert.assertTrue(workerIds.subList(0, 3).contains(replicaInfo.getWorkerInfo().getWorkerId()));
                Assert.assertEquals(ReplicaState.REPLICA_OK, replicaInfo.getReplicaState());
            }
        }

        {
            // shard has 3 REPLICA_OK, 1 SCALE_IN, 1 SCALE_OUT. REPLICA_OK and SCALE_IN will be visible to external.
            // shard only needs 3 replica in the workerGroup, shardChecker will remove the additional SCALE_OUT + SCALE_IN.

            // create a shard
            long shardId = helper.createTestShard(0);
            Shard shard = helper.getDefaultShardManager().getShard(shardId);
            Assert.assertTrue(shard.addReplica(workerIds.get(0))); // REPLICA_OK
            Assert.assertTrue(shard.scaleInReplica(workerIds.get(0))); // REPLICA_SCALE_IN
            Assert.assertTrue(shard.scaleOutReplica(workerIds.get(1))); // REPLICA_SCALE_OUT
            Assert.assertTrue(shard.addReplica(workerIds.get(2))); // REPLICA_OK
            Assert.assertTrue(shard.addReplica(workerIds.get(3))); // REPLICA_OK
            Assert.assertTrue(shard.addReplica(workerIds.get(4))); // REPLICA_OK

            // replica of REPLICA_SCALE_OUT will not be in the result
            List<Long> expectedWorkerIds = new ArrayList<>(workerIds);
            expectedWorkerIds.remove(workerIds.get(1));

            List<ShardInfo> shardListResult =
                    starManager.getShardInfo(helper.getDefaultServiceId(), Collections.singletonList(shardId),
                            workerGroupId);
            Assert.assertEquals(1, shardListResult.size());
            ShardInfo shardInfo = shardListResult.get(0);
            Assert.assertEquals(4, shardInfo.getReplicaInfoCount());
            for (ReplicaInfo replicaInfo : shardInfo.getReplicaInfoList()) {
                long workerId = replicaInfo.getWorkerInfo().getWorkerId();
                Assert.assertTrue(expectedWorkerIds.contains(workerId));
                if (workerId == workerIds.get(0)) {
                    Assert.assertEquals(ReplicaState.REPLICA_SCALE_IN, replicaInfo.getReplicaState());
                } else {
                    Assert.assertEquals(ReplicaState.REPLICA_OK, replicaInfo.getReplicaState());
                }
            }
        }

        { // SCALE_OUT replica will not be in the list even the remain number of replicas less than the expected number.
            // shard has 1 REPLICA_OK, 1 SCALE_IN, 1 SCALE_OUT. REPLICA_OK and SCALE_IN will be visible to external.
            // shard only needs 3 replica in the workerGroup.
            // create a shard
            long shardId = helper.createTestShard(0);
            Shard shard = helper.getDefaultShardManager().getShard(shardId);
            Assert.assertTrue(shard.addReplica(workerIds.get(0))); // REPLICA_OK
            Assert.assertTrue(shard.scaleInReplica(workerIds.get(0))); // REPLICA_SCALE_IN
            Assert.assertTrue(shard.scaleOutReplica(workerIds.get(1))); // REPLICA_SCALE_OUT
            Assert.assertTrue(shard.addReplica(workerIds.get(2))); // REPLICA_OK

            // replica of REPLICA_SCALE_OUT will not be in the result
            List<Long> expectedWorkerIds = new ArrayList<>(workerIds);
            expectedWorkerIds.remove(workerIds.get(1));

            List<ShardInfo> shardListResult =
                    starManager.getShardInfo(helper.getDefaultServiceId(), Collections.singletonList(shardId),
                            workerGroupId);

            Assert.assertEquals(1, shardListResult.size());
            ShardInfo shardInfo = shardListResult.get(0);
            Assert.assertEquals(2, shardInfo.getReplicaInfoCount());
            for (ReplicaInfo replicaInfo : shardInfo.getReplicaInfoList()) {
                long workerId = replicaInfo.getWorkerInfo().getWorkerId();
                Assert.assertTrue(expectedWorkerIds.contains(workerId));
                if (workerId == workerIds.get(0)) {
                    Assert.assertEquals(ReplicaState.REPLICA_SCALE_IN, replicaInfo.getReplicaState());
                } else {
                    Assert.assertEquals(ReplicaState.REPLICA_OK, replicaInfo.getReplicaState());
                }
            }
        }

        { // If only SCALE_OUT replicas available, return just one of them
            // shard has 2 SCALE_OUT.
            // create a shard
            long shardId = helper.createTestShard(0);
            Shard shard = helper.getDefaultShardManager().getShard(shardId);
            Assert.assertTrue(shard.scaleOutReplica(workerIds.get(0))); // REPLICA_SCALE_OUT
            Assert.assertTrue(shard.scaleOutReplica(workerIds.get(1))); // REPLICA_SCALE_OUT

            List<Long> expectedWorkerIds = workerIds.subList(0, 2);

            List<ShardInfo> shardListResult =
                    starManager.getShardInfo(helper.getDefaultServiceId(), Collections.singletonList(shardId),
                            workerGroupId);

            Assert.assertEquals(1, shardListResult.size());
            ShardInfo shardInfo = shardListResult.get(0);
            // Only has one replica
            Assert.assertEquals(1, shardInfo.getReplicaInfoCount());
            for (ReplicaInfo replicaInfo : shardInfo.getReplicaInfoList()) {
                long workerId = replicaInfo.getWorkerInfo().getWorkerId();
                Assert.assertTrue(expectedWorkerIds.contains(workerId));
                // The replica is in scale-out
                Assert.assertEquals(ReplicaState.REPLICA_SCALE_OUT, replicaInfo.getReplicaState());
            }
        }
    }

    @Test
    public void testUpdateShardReplicaInfo() {
        TestHelper helper = new TestHelper(serviceTemplateName, starManager);
        helper.createDefaultServiceAndWorkerGroup(this.getClass().getName() + "-serviceName-0");
        long workerGroupId = helper.getDefaultWorkerGroupId();
        helper.setDefaultWorkerGroupReplicaNumber(1);
        helper.setDefaultWorkerGroupWarmupLevel(WarmupLevel.WARMUP_META);
        long workerId = helper.createTestWorkerToWorkerGroup(workerGroupId);

        List<Long> shardIds = helper.createTestShards(3, 0);
        ShardManager shardManager = helper.getDefaultShardManager();
        Shard shard1 = shardManager.getShard(shardIds.get(0));
        Shard shard2 = shardManager.getShard(shardIds.get(1));
        Shard shard3 = shardManager.getShard(shardIds.get(2));

        shardManager.scaleOutShardReplicas(shardIds, workerId, false);
        for (long id : shardIds) {
            Shard shard = shardManager.getShard(id);
            Assert.assertEquals(1L, shard.getReplicaSize());
            Assert.assertEquals(ReplicaState.REPLICA_SCALE_OUT, shard.getReplica().get(0).getState());
        }

        List<ReplicaUpdateInfo> updateInfos = new ArrayList<>();
        // update shard1 & shard2
        updateInfos.add(ReplicaUpdateInfo.newBuilder().setShardId(shard1.getShardId())
                .setReplicaState(ReplicaState.REPLICA_OK).build());
        updateInfos.add(ReplicaUpdateInfo.newBuilder().setShardId(shard2.getShardId())
                .setReplicaState(ReplicaState.REPLICA_OK).build());

        for (long id : shardIds) {
            Shard shard = shardManager.getShard(id);
            Assert.assertEquals(1L, shard.getReplicaSize());
            Assert.assertEquals(ReplicaState.REPLICA_SCALE_OUT, shard.getReplica().get(0).getState());
        }

        starManager.updateShardReplicaInfo(helper.getDefaultServiceId(), workerId, updateInfos);
        // shard1 & shard2 updated
        Assert.assertEquals(1L, shard1.getReplicaSize());
        Assert.assertEquals(ReplicaState.REPLICA_OK, shard1.getReplica().get(0).getState());
        Assert.assertEquals(1L, shard2.getReplicaSize());
        Assert.assertEquals(ReplicaState.REPLICA_OK, shard2.getReplica().get(0).getState());
        Assert.assertEquals(1L, shard3.getReplicaSize());
        Assert.assertEquals(ReplicaState.REPLICA_SCALE_OUT, shard3.getReplica().get(0).getState());
    }

    @Test
    public void testProcessWorkerHeartbeatAsyncException() {
        TestHelper helper = new TestHelper(serviceTemplateName, starManager);
        helper.createDefaultServiceAndWorkerGroup(this.getClass().getName() + "-serviceName-0");
        long workerGroupId = helper.getDefaultWorkerGroupId();
        long workerId = helper.createTestWorkerToWorkerGroup(workerGroupId);
        WorkerManager workerManager = helper.getWorkerManager();
        AtomicInteger expectCount = new AtomicInteger();

        new MockUp<WorkerManager>() {
            @Mock
            public boolean processWorkerHeartbeat(String serviceId, long workerId, long startTime, long numOfShards,
                                                  Map<String, String> workerProperties, long lastSeenTime) {
                expectCount.incrementAndGet();
                throw new OutOfMemoryError("Injected exception");
            }
        };

        Assert.assertEquals(0, expectCount.get());
        long startTime = System.currentTimeMillis();
        List<Long> shardIds = Lists.newArrayList(1L, 2L);
        starManager.processWorkerHeartbeat(helper.getDefaultServiceId(), workerId, startTime, null, shardIds,
                Collections.emptyList());
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(() -> TestUtils.workerManagerExecutorsIdle(workerManager));
        Assert.assertEquals(1, expectCount.get());
    }

    private static class RunTaskContext {
        public String serviceId;
        public long workerId;
        public long startTime;
        public List<Long> shardIds;
        public long lastSeenTime;
    }

    // Mock WorkerManager with the counting of invokes and rejects
    static final class MockWorkerManager extends MockUp<WorkerManager> {
        public AtomicInteger invokeCount = new AtomicInteger();
        public AtomicInteger rejectedCount = new AtomicInteger();
        public CountDownLatch runLatch = new CountDownLatch(1);

        @Mock
        public boolean processWorkerHeartbeat(Invocation inv, String serviceId, long workerId, long startTime,
                                              long numOfShards,
                                              Map<String, String> workerProperties, long lastSeenTime)
                throws StarException, InterruptedException {
            runLatch.await();
            invokeCount.incrementAndGet();
            try {
                return inv.proceed(serviceId, workerId, startTime, numOfShards, workerProperties, lastSeenTime);
            } catch (FailedPreconditionStarException ex) {
                rejectedCount.incrementAndGet();
                throw ex;
            }
        }
    }

    void submitHeartbeatRequests(StarManager manager, RunTaskContext ctx, CountDownLatch latch) {
        manager.getWorkerManager().submitAsyncTask(
                () -> manager.processWorkerHeartbeatAsync(ctx.serviceId, ctx.workerId, ctx.startTime, null,
                        ctx.shardIds, Collections.emptyList(), ctx.lastSeenTime));
        latch.countDown();
    }

    @Test
    public void testProcessWorkerHeartbeatOutOfOrder() throws InterruptedException {
        TestHelper helper = new TestHelper(serviceTemplateName, starManager);
        helper.createDefaultServiceAndWorkerGroup(this.getClass().getName() + "-serviceName-0");
        long workerGroupId = helper.getDefaultWorkerGroupId();
        long workerId = helper.createTestWorkerToWorkerGroup(workerGroupId);
        Worker worker = starManager.getWorkerManager().getWorker(workerId);
        Assert.assertEquals(0, worker.getNumOfShards());

        long shardGroup = helper.createTestShardGroup(PlacementPolicy.SPREAD);
        int nTasks = 1000;
        List<Long> shardIds = helper.createTestShards(nTasks, shardGroup);
        // assign all shards to the worker
        helper.getDefaultShardManager().addShardReplicas(shardIds, workerId, false);
        List<RunTaskContext> ctxs = new ArrayList<>();
        List<Runnable> tasks = new ArrayList<>();
        CountDownLatch doneSubmitLatch = new CountDownLatch(nTasks);
        long now = System.currentTimeMillis();
        Assert.assertTrue(worker.getLastSeenTime() < now);

        // start the MockUp
        MockWorkerManager mockWorkerManager = new MockWorkerManager();

        // With the following well-designed ctx, the lastSeenTime is ordered as tasks[0] > tasks[1] > ... > tasks[1000]
        // No matter which execution order these tasks, it is always the ctx[0] win after all executed
        for (int i = 0; i < nTasks; ++i) {
            RunTaskContext ctx = new RunTaskContext();
            ctx.serviceId = helper.getDefaultServiceId();
            ctx.workerId = workerId;
            ctx.startTime = now;
            ctx.shardIds = shardIds.subList(0, i);
            ctx.lastSeenTime = now - i + nTasks * 10;
            ctxs.add(ctx);
            Runnable run = () -> submitHeartbeatRequests(starManager, ctx, doneSubmitLatch);
            tasks.add(run);
        }
        RunTaskContext winCtx = ctxs.get(0);

        // random shuffle all the tasks
        Collections.shuffle(tasks);
        // submit all tasks
        ThreadPoolExecutor executor =
                new ThreadPoolExecutor(20, 20, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        for (Runnable run : tasks) {
            executor.submit(run);
        }
        // wait for all tasks submitted
        doneSubmitLatch.await();
        // the SubmitTasks ThreadPool is idle, but none of the task get executed
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> executor.getActiveCount() == 0 && executor.getQueue().isEmpty());
        Assert.assertEquals(0, mockWorkerManager.invokeCount.get());
        // allow running all tasks now.
        mockWorkerManager.runLatch.countDown();

        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> TestUtils.workerManagerExecutorsIdle(starManager.getWorkerManager()));
        Assert.assertEquals(nTasks, mockWorkerManager.invokeCount.get());
        LOG.warn("InvokeCount: {}, RejectedCount: {}", mockWorkerManager.invokeCount.get(),
                mockWorkerManager.rejectedCount.get());
        // 0 <= rejectCount < nTasks
        // happens to be running in the exact order of the lastSeenTime
        Assert.assertTrue(mockWorkerManager.rejectedCount.get() >= 0);
        // the latest one happens to be the first one and all the remaining are rejected
        Assert.assertTrue(mockWorkerManager.rejectedCount.get() < nTasks);

        // validate Worker info
        Assert.assertEquals(winCtx.lastSeenTime, worker.getLastSeenTime());
        Assert.assertEquals(winCtx.shardIds.size(), worker.getNumOfShards());
        Utils.shutdownExecutorService(executor);
    }
}
