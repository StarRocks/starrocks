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

import com.staros.common.DelayedDummyJournalSystem;
import com.staros.common.HijackConfig;
import com.staros.common.TestHelper;
import com.staros.journal.StarMgrJournal;
import com.staros.proto.PlacementPolicy;
import com.staros.proto.WorkerState;
import com.staros.starlet.StarletAgentFactory;
import com.staros.util.Config;
import com.staros.util.Constant;
import com.staros.util.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class StarManagerPerfTest {
    private static final Logger LOG = LogManager.getLogger(StarManagerPerfTest.class);
    private final String serviceTemplateName = "StarManagerTest";
    private StarManager starManager;
    private HijackConfig disableShardChecker;
    private HijackConfig enableZeroGroupCompatible;
    private DelayedDummyJournalSystem journalSystem;
    private TestHelper testHelper;

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
        enableZeroGroupCompatible = new HijackConfig("ENABLE_ZERO_WORKER_GROUP_COMPATIBILITY", "true");
        journalSystem = new DelayedDummyJournalSystem();
        starManager = new StarManager(journalSystem);
        testHelper = new TestHelper(serviceTemplateName, starManager);
        starManager.becomeLeader();
        // don't do heartbeat check.
        starManager.getHeartbeatManager().stop();
    }

    @After
    public void stopStarManager() {
        starManager.becomeFollower();
        disableShardChecker.reset();
        enableZeroGroupCompatible.reset();
    }

    private void createShardsInNewShardGroup(int numShards) {
        long shardGroupId = testHelper.createTestShardGroup(PlacementPolicy.SPREAD);
        testHelper.createTestShards(numShards, shardGroupId);
    }

    private long testGetAllShardInfo(String serviceId, ExecutorService executor) throws InterruptedException {
        List<Long> allShardGroupIds = starManager.getShardManager(serviceId).getAllShardGroupIds();
        List<Callable<Boolean>> tasks = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(allShardGroupIds.size());
        for (Long id : allShardGroupIds) {
            tasks.add(() -> {
                starManager.listShardInfo(serviceId, Collections.singletonList(id), 0, false);
                latch.countDown();
                return true;
            });
        }
        long startTime = System.currentTimeMillis();
        executor.invokeAll(tasks);
        latch.countDown();
        long endTime = System.currentTimeMillis();
        return endTime - startTime;
    }

    @Test
    public void testShardManagerRescheduleReplicaCapability_4_8192_16_200() throws InterruptedException {
        testShardManagerRescheduleReplicaUpdateAbility(4, 8192, 16, 200);
    }

    @Test
    public void testShardManagerRescheduleReplicaCapability_8_4096_16_500() throws InterruptedException {
        testShardManagerRescheduleReplicaUpdateAbility(8, 4096, 32, 500);
    }

    private void testShardManagerRescheduleReplicaUpdateAbility(int numWorkers, int numShardGroups,
                                                                int numShardPerGroup, int journalDelay)
            throws InterruptedException {
        String serviceName = "StarManagerTest-1";
        String serviceId = testHelper.bootstrapService(serviceName);
        testHelper.setDefaultServiceId(serviceId);

        long lastWorkerId = 0;
        for (long i = 0; i < numWorkers; ++i) {
            lastWorkerId =
                    starManager.addWorker(serviceId, Constant.DEFAULT_ID, TestHelper.generateMockWorkerIpAddress());
        }

        // Wait all workers online
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> {
            List<Long> workerIds =
                    starManager.getWorkerManager().getDefaultWorkerGroup(serviceId).getAllWorkerIds(true);
            return numWorkers == workerIds.size();
        });

        List<Long> workerIds = starManager.getWorkerManager().getDefaultWorkerGroup(serviceId).getAllWorkerIds(true);
        Assert.assertEquals(numWorkers, workerIds.size());

        for (int i = 0; i < numShardGroups; ++i) {
            createShardsInNewShardGroup(numShardPerGroup);
        }

        ExecutorService executor = Executors.newFixedThreadPool(32);

        int numShards1 =
                (int) StarMgrJournal.METRIC_LOG_UPDATE_SHARDS_NUM_COUNTER.collect().getDataPoints().get(0).getValue();
        int numOps1 =
                (int) StarMgrJournal.METRIC_LOG_UPDATE_SHARDS_OPS_COUNTER.collect().getDataPoints().get(0).getValue();
        LOG.warn("Metrics before 1st retrieve: numShards: {}, numOps: {}", numShards1, numOps1);

        // generate the getShardInfo request for every shard and executor in thread pool, evaluate the total time
        long before = testGetAllShardInfo(serviceId, executor);

        long downWorkerNumShards = starManager.getWorkerManager().getWorker(lastWorkerId).getNumOfShards();
        Assert.assertTrue(downWorkerNumShards >= (long) numShardGroups * numShardPerGroup / numWorkers);

        journalSystem.setWriteDelayMicroSeconds(journalDelay);
        // set the WORKER to DOWN state
        starManager.getWorkerManager().getWorker(lastWorkerId).setState(WorkerState.DOWN);
        // all the dead replicas are expired
        HijackConfig var = new HijackConfig("SHARD_DEAD_REPLICA_EXPIRE_SECS", "-1");

        int numShards2 =
                (int) StarMgrJournal.METRIC_LOG_UPDATE_SHARDS_NUM_COUNTER.collect().getDataPoints().get(0).getValue();
        int numOps2 =
                (int) StarMgrJournal.METRIC_LOG_UPDATE_SHARDS_OPS_COUNTER.collect().getDataPoints().get(0).getValue();
        LOG.warn("Metrics before 2nd retrieve: numShards: {}, numOps: {}", numShards2, numOps2);
        Assert.assertEquals(numOps2, numOps1);
        Assert.assertEquals(numShards2, numShards1);

        // now do the getShardInfo in batch and see how long it takes to complete
        long after = testGetAllShardInfo(serviceId, executor);
        LOG.warn("Perf test of {} shards in {} shardgroups, before(no-delay): {}ms v.s. after(delay-{}us): {}ms",
                numShardGroups * numShardPerGroup, numShardGroups, before, journalDelay, after);

        int numShards3 =
                (int) StarMgrJournal.METRIC_LOG_UPDATE_SHARDS_NUM_COUNTER.collect().getDataPoints().get(0).getValue();
        int numOps3 =
                (int) StarMgrJournal.METRIC_LOG_UPDATE_SHARDS_OPS_COUNTER.collect().getDataPoints().get(0).getValue();
        LOG.warn("Metrics after 2nd retrieve: numShards: {}, numOps: {}, increases: {}", numShards3, numOps3,
                numOps3 - numOps2);
        Assert.assertEquals(numShards3 - numShards2, downWorkerNumShards);

        Utils.shutdownExecutorService(executor);
        var.reset();
    }
}
