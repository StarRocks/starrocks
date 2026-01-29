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

package com.staros.shard;

import com.staros.common.HijackConfig;
import com.staros.common.TestHelper;
import com.staros.exception.CheckInterruptedException;
import com.staros.exception.ExceptionCode;
import com.staros.exception.StarException;
import com.staros.proto.PlacementPolicy;
import com.staros.proto.ReplicaState;
import com.staros.proto.WarmupLevel;
import com.staros.proto.WorkerState;
import com.staros.schedule.Scheduler;
import com.staros.schedule.ShardSchedulerV2;
import com.staros.service.ServiceManager;
import com.staros.starlet.StarletAgentFactory;
import com.staros.util.Config;
import com.staros.util.Constant;
import com.staros.worker.Worker;
import com.staros.worker.WorkerManager;
import mockit.Expectations;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ShardCheckerTest {
    private static final Logger LOG = LogManager.getLogger(ShardCheckerTest.class);

    private HijackConfig hijackTriggerScheduling;

    private String defaultServiceId;
    private long defaultWorkerGroupId;
    private TestHelper helper;
    private ShardSchedulerV2 realScheduler;

    private static class MockShardChecker extends ShardChecker {
        public MockShardChecker(ServiceManager serviceManager, WorkerManager workerManager, Scheduler scheduler) {
            super(serviceManager, workerManager, scheduler);
        }

        public void runServiceHealthCheck(String serviceId) {
            super.shardHealthCheckForService(serviceId);
        }

        public void runServiceBalanceCheck(String serviceId) {
            super.shardGroupBalanceCheckForService(serviceId);
        }
    }

    @BeforeClass
    public static void setupForTestSuite() {
        // Turn On MockStarletAgent
        StarletAgentFactory.AGENT_TYPE = StarletAgentFactory.AgentType.MOCK_STARLET_AGENT;
        Config.S3_BUCKET = "test-bucket";
        Config.S3_REGION = "test-region";
        Config.S3_ENDPOINT = "test-endpoint";
        Config.SIMPLE_CREDENTIAL_ACCESS_KEY_ID = "test-ak";
        Config.SIMPLE_CREDENTIAL_ACCESS_KEY_SECRET = "test-sk";
    }

    @Before
    public void setUp() {
        // disable auto schedule
        hijackTriggerScheduling = new HijackConfig("SCHEDULER_TRIGGER_SCHEDULE_WHEN_CREATE_SHARD", "false");
        helper = new TestHelper(this.getClass().getName() + "-serviceTemplateName-0");
        helper.createDefaultServiceAndWorkerGroup(this.getClass().getName() + "-serviceName-0");
        defaultServiceId = helper.getDefaultServiceId();
        defaultWorkerGroupId = helper.getDefaultWorkerGroupId();
        realScheduler = new ShardSchedulerV2(helper.getServiceManager(), helper.getWorkerManager());
        realScheduler.start();
    }

    @After
    public void tearDown() {
        hijackTriggerScheduling.reset();
        realScheduler.stop();
    }

    @Test
    public void testShardAllFine() {
        // set expected replica to 3 in defaultWorkerGroup
        helper.setDefaultWorkerGroupReplicaNumber(3);

        long shardId = helper.createTestShard(0);
        List<Long> workerIds = helper.createTestWorkersToWorkerGroup(3, defaultWorkerGroupId);
        // create a shard with 3 replicas, each worker has a replica
        Shard shard = helper.getDefaultShardManager().getShard(shardId);
        workerIds.forEach(shard::addReplica);

        // give a null scheduler, still OK, so the scheduler method is never invoked.
        MockShardChecker checker = new MockShardChecker(helper.getServiceManager(), helper.getWorkerManager(), null);
        // everything is fine, no scheduler calls are made.
        checker.runServiceHealthCheck(defaultServiceId);

        // shard2 has no replicas yet, even though it expects 3 replicas
        long shardId2 = helper.createTestShard(0);
        Shard shard2 = helper.getDefaultShardManager().getShard(shardId2);
        Assert.assertEquals(0, shard2.getReplicaSize());
        // still no scheduler calls, if shard has no replica, suppose it is in a lazy scheduling status,
        // don't do scheduling at all until necessary.
        checker.runServiceHealthCheck(defaultServiceId);
    }

    @Test
    public void testRemoveRedundantUnhealthyReplicas() {
        helper.setDefaultWorkerGroupReplicaNumber(3);
        long shardId = helper.createTestShard(0);
        List<Long> workerIds = helper.createTestWorkersToWorkerGroup(5, defaultWorkerGroupId);
        // create a shard with 3 replicas, a worker group with 5 workers
        Shard shard = helper.getDefaultShardManager().getShard(shardId);
        // add shard replica, so shard will have 5 replicas
        workerIds.forEach(shard::addReplica);

        MockShardChecker checker = new MockShardChecker(helper.getServiceManager(), helper.getWorkerManager(), realScheduler);
        // make worker[0], worker[1], worker[2] DOWN
        Worker worker0 = helper.getWorkerManager().getWorker(workerIds.get(0));
        Worker worker1 = helper.getWorkerManager().getWorker(workerIds.get(1));
        Worker worker2 = helper.getWorkerManager().getWorker(workerIds.get(2));
        long current = System.currentTimeMillis();
        // set worker[0], worker[1], worker[2] DOWN
        worker0.setState(WorkerState.DOWN);
        worker1.setState(WorkerState.DOWN);
        worker2.setState(WorkerState.DOWN);

        // set worker1 downtime expiration
        worker1.setLastDownTime(System.currentTimeMillis() - Config.SHARD_DEAD_REPLICA_EXPIRE_SECS * 1000L - 1000);
        Assert.assertFalse(worker0.replicaExpired());
        Assert.assertTrue(worker1.replicaExpired());
        Assert.assertFalse(worker2.replicaExpired());

        Assert.assertEquals(5L, shard.getReplicaSize());
        checker.runServiceHealthCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
        // worker[1] is removed
        Assert.assertEquals(4L, shard.getReplicaSize());
        Assert.assertFalse(shard.hasReplica(worker1.getWorkerId()));

        // set worker0 downtime expiration
        worker0.setLastDownTime(System.currentTimeMillis() - Config.SHARD_DEAD_REPLICA_EXPIRE_SECS * 1000L - 1000);
        Assert.assertTrue(worker0.replicaExpired());
        Assert.assertTrue(worker1.replicaExpired());
        Assert.assertFalse(worker2.replicaExpired());

        // DO IT AGAIN
        checker.runServiceHealthCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
        // worker[0] is removed
        Assert.assertEquals(3L, shard.getReplicaSize());
        Assert.assertFalse(shard.hasReplica(worker0.getWorkerId()));

        // set worker2 downtime expiration
        worker2.setLastDownTime(System.currentTimeMillis() - Config.SHARD_DEAD_REPLICA_EXPIRE_SECS * 1000L - 1000);
        Assert.assertTrue(worker0.replicaExpired());
        Assert.assertTrue(worker1.replicaExpired());
        Assert.assertTrue(worker2.replicaExpired());
        // DO IT AGAIN, all expired replicas are moved
        checker.runServiceHealthCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
        Assert.assertEquals(2L, shard.getReplicaSize());

        // Every round, checker is trying to submit scheduler a new request to add replicas, but there is no worker
        // available to add additional replica for the shard.
    }

    @Test
    public void testShutdownReplicaNotRemoved() {
        helper.setDefaultWorkerGroupReplicaNumber(3);
        long shardId = helper.createTestShard(0);
        // create a shard with 3 replicas, a worker group with 5 workers
        List<Long> workerIds = helper.createTestWorkersToWorkerGroup(5, defaultWorkerGroupId);
        Shard shard = helper.getDefaultShardManager().getShard(shardId);
        // add shard replica to [0, 1, 2, 3, 4], so shard will have 5 replicas
        workerIds.forEach(shard::addReplica);

        MockShardChecker checker = new MockShardChecker(helper.getServiceManager(), helper.getWorkerManager(), realScheduler);
        Worker worker0 = helper.getWorkerManager().getWorker(workerIds.get(0));
        // make worker[0] DOWN
        worker0.setState(WorkerState.SHUTTING_DOWN);

        // shard have 5 replicas, where 4 are healthy and 1 is SHUTTING_DOWN
        Assert.assertEquals(5L, shard.getReplicaSize());

        checker.runServiceHealthCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
        // worker[0] is NOT removed, one of the healthy replica will be removed
        Assert.assertEquals(4L, shard.getReplicaSize());
        Assert.assertTrue(shard.hasReplica(worker0.getWorkerId()));

        // DO IT AGAIN, NO more replica removed, 3 healthy replica with 1 shutdown replica
        checker.runServiceHealthCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
        // worker[0] is NOT removed
        Assert.assertEquals(4L, shard.getReplicaSize());
        Assert.assertTrue(shard.hasReplica(worker0.getWorkerId()));

        try (HijackConfig var = new HijackConfig("SHARD_DEAD_REPLICA_EXPIRE_SECS", "-1")) {
            // DO IT AGAIN with immediate expire settings, the replica on worker[0] won't be removed.
            checker.runServiceHealthCheck(defaultServiceId);
            Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
            // worker[0] is NOT removed
            Assert.assertEquals(4L, shard.getReplicaSize());
            Assert.assertTrue(shard.hasReplica(worker0.getWorkerId()));
        }
    }

    @Test
    public void testShutdownNewReplicaCreatedOldReplicaNotRemoved() {
        helper.setDefaultWorkerGroupReplicaNumber(3);
        helper.setDefaultWorkerGroupWarmupLevel(WarmupLevel.WARMUP_ALL);
        long shardId = helper.createTestShard(0);
        // create a shard with 3 replicas, a worker group with 5 workers
        List<Long> workerIds = helper.createTestWorkersToWorkerGroup(4, defaultWorkerGroupId);
        Shard shard = helper.getDefaultShardManager().getShard(shardId);
        // add shard replica to [0, 1, 2], so shard will have 3 replicas
        workerIds.subList(0, 3).forEach(shard::addReplica);

        MockShardChecker checker = new MockShardChecker(helper.getServiceManager(), helper.getWorkerManager(), realScheduler);
        Worker worker0 = helper.getWorkerManager().getWorker(workerIds.get(0));
        // make worker[0] DOWN
        worker0.setState(WorkerState.SHUTTING_DOWN);

        // shard have 3 replicas, where 2 are healthy and 1 is SHUTTING_DOWN
        Assert.assertEquals(3L, shard.getReplicaSize());

        checker.runServiceHealthCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
        // worker[0] is NOT removed, no new replica created
        Assert.assertEquals(3L, shard.getReplicaSize());
        Assert.assertTrue(shard.hasReplica(worker0.getWorkerId()));

        // make worker[0] DOWN
        worker0.setState(WorkerState.DOWN);
        checker.runServiceHealthCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
        // worker[0] is not removed
        Assert.assertEquals(3L, shard.getReplicaSize());
        Assert.assertTrue(shard.hasReplica(worker0.getWorkerId()));

        // set the REPLICA_SCALE_OUT replica to REPLICA_OK
        shard.getReplica().forEach(x -> shard.scaleOutReplicaDone(x.getWorkerId()));

        // run the check again, nothing changed
        Assert.assertFalse(worker0.replicaExpired());
        checker.runServiceHealthCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
        // worker[0] is not removed
        Assert.assertEquals(3L, shard.getReplicaSize());
        Assert.assertTrue(shard.hasReplica(worker0.getWorkerId()));

        worker0.setLastDownTime(System.currentTimeMillis() - Config.SHARD_DEAD_REPLICA_EXPIRE_SECS * 1000L - 1000);
        // run the check again
        Assert.assertTrue(worker0.replicaExpired());
        checker.runServiceHealthCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
        // worker[0] is removed now
        Assert.assertEquals(3L, shard.getReplicaSize());
        Assert.assertFalse(shard.hasReplica(worker0.getWorkerId()));
    }

    @Test
    public void testNoHealthReplicas() {
        helper.setDefaultWorkerGroupReplicaNumber(2);
        long shardId = helper.createTestShard(0);
        List<Long> workerIds = helper.createTestWorkersToWorkerGroup(3, defaultWorkerGroupId);
        // create a shard with 2 replicas.
        Shard shard = helper.getDefaultShardManager().getShard(shardId);
        // set one replica to worker[0] and set worker[0] dead.
        long nonExistWorkerId = 13579;
        shard.addReplica(nonExistWorkerId);
        shard.addReplica(workerIds.get(0));

        Worker worker = helper.getWorkerManager().getWorker(workerIds.get(0));
        worker.setState(WorkerState.DOWN);
        Assert.assertNull(helper.getWorkerManager().getWorker(nonExistWorkerId));

        Assert.assertTrue(realScheduler.isIdle());
        Assert.assertEquals(2L, shard.getReplicaSize());
        Assert.assertTrue(shard.hasReplica(nonExistWorkerId));
        Assert.assertTrue(shard.hasReplica(worker.getWorkerId()));

        MockShardChecker checker = new MockShardChecker(helper.getServiceManager(), helper.getWorkerManager(), realScheduler);
        checker.runServiceHealthCheck(defaultServiceId);
        // Expectation:
        // * one replica will be scheduled to one of the (worker[1], worker[2])
        // * nonExist workerId will be removed immediately
        // * worker[0] (the dead replica) will be kept because of still not expired
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
        Assert.assertEquals(2L, shard.getReplicaSize());
        Assert.assertFalse(shard.hasReplica(nonExistWorkerId));

        Assert.assertTrue(shard.hasReplica(worker.getWorkerId()));
        Assert.assertTrue(shard.hasReplica(workerIds.get(1)) || shard.hasReplica(workerIds.get(2)));
        shard.getReplicaWorkerIds().forEach(shard::scaleOutReplica);

        // run again, worker[0] will not be removed, because of not expired
        checker.runServiceHealthCheck(defaultServiceId);
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(realScheduler::isIdle);
        Assert.assertEquals(2L, shard.getReplicaSize());
        Assert.assertTrue(shard.hasReplica(worker.getWorkerId()));
        Assert.assertTrue(shard.hasReplica(workerIds.get(1)) || shard.hasReplica(workerIds.get(2)));

        try (HijackConfig var = new HijackConfig("SHARD_DEAD_REPLICA_EXPIRE_SECS", "-1")) {
            // run again this time, the expiration is set to -1 second, the dead replica must be expired
            checker.runServiceHealthCheck(defaultServiceId);
            Awaitility.await().atMost(2, TimeUnit.SECONDS).until(realScheduler::isIdle);
            Assert.assertEquals(2L, shard.getReplicaSize());
            Assert.assertFalse(shard.hasReplica(workerIds.get(0)));
            Assert.assertTrue(shard.hasReplica(workerIds.get(1)));
            Assert.assertTrue(shard.hasReplica(workerIds.get(2)));
        }
    }

    @Test
    public void testNeedMoreReplicas() {
        int expectedReplica = 3;
        helper.setDefaultWorkerGroupReplicaNumber(expectedReplica);
        long shardId = helper.createTestShard(0);
        List<Long> workerIds1 = helper.createTestWorkersToWorkerGroup(3, defaultWorkerGroupId);
        long workGroupId2 = helper.createWorkerGroup(expectedReplica);
        List<Long> workerIds2 = helper.createTestWorkersToWorkerGroup(2, workGroupId2);
        long workGroupId3 = helper.createWorkerGroup(expectedReplica);
        helper.createTestWorkersToWorkerGroup(1, workGroupId3);

        Shard shard = helper.getDefaultShardManager().getShard(shardId);
        // Shard needs 3 replicas, already have 1 replica in defaultWorkerGroup and 1 replica in workGroupId2
        shard.addReplica(workerIds1.get(0));
        shard.addReplica(workerIds2.get(0));

        MockShardChecker checker = new MockShardChecker(helper.getServiceManager(), helper.getWorkerManager(), realScheduler);
        checker.runServiceHealthCheck(defaultServiceId);
        // Expectation:
        //   in defaultWorkerGroup: shard will have 3 replicas to every worker
        //   in workGroupId2: can add one more replica, but still less than 3 replicas
        //   no replica in workGroupId3, because of no exist replica in this worker group.
        List<Long> allWorkerIds = new ArrayList<>(workerIds1);
        allWorkerIds.addAll(workerIds2);
        Awaitility.await().atMost(3, TimeUnit.SECONDS).until(realScheduler::isIdle);
        Assert.assertEquals(allWorkerIds.size(), shard.getReplicaSize());
        allWorkerIds.forEach(x -> Assert.assertTrue(shard.hasReplica(x)));
    }

    @Test
    public void testRemoveHealthyReplicas() {
        helper.setDefaultWorkerGroupReplicaNumber(3);
        long shardId = helper.createTestShard(0);
        Shard shard = helper.getDefaultShardManager().getShard(shardId);
        long workerGroupId = helper.createWorkerGroup();
        List<Long> workerIds = helper.createTestWorkersToWorkerGroup(5, workerGroupId);
        workerIds.forEach(shard::addReplica);

        Assert.assertEquals(workerIds.size(), shard.getReplicaSize());

        MockShardChecker checker = new MockShardChecker(helper.getServiceManager(), helper.getWorkerManager(), realScheduler);
        checker.runServiceHealthCheck(defaultServiceId);
        // a replica is removed
        Awaitility.await().atMost(3, TimeUnit.SECONDS).until(realScheduler::isIdle);
        Assert.assertEquals(workerIds.size() - 1, shard.getReplicaSize());

        checker.runServiceHealthCheck(defaultServiceId);
        // a second replica is removed
        Awaitility.await().atMost(3, TimeUnit.SECONDS).until(realScheduler::isIdle);
        Assert.assertEquals(workerIds.size() - 2, shard.getReplicaSize());
    }

    @Test
    public void testServiceIdNotExist() {
        MockShardChecker checker = new MockShardChecker(helper.getServiceManager(), helper.getWorkerManager(), null);
        checker.runServiceHealthCheck("some-non-exist-service-id");
        // nothing happens, checker will swallow the exception.
    }

    @Test
    public void testShardNotExist() {
        helper.setDefaultWorkerGroupReplicaNumber(3);
        long shardId = helper.createTestShard(0);
        ShardManager manager = helper.getDefaultShardManager();
        MockShardChecker checker = new MockShardChecker(helper.getServiceManager(), helper.getWorkerManager(), null);

        // Partially mock ShardManager
        new Expectations(manager) {
            {
                manager.getShard(shardId);
                result = null;
            }
        };
        checker.runServiceHealthCheck(defaultServiceId);
        // nothing happens, checker will swallow the exception.
    }

    @Test
    public void testHealthyCheckSchedulerException() {
        helper.setDefaultWorkerGroupReplicaNumber(2);
        long shardId = helper.createTestShard(0);
        List<Long> workerIds = helper.createTestWorkersToWorkerGroup(5, defaultWorkerGroupId);
        // create a shard with 2 replicas.
        Shard shard = helper.getDefaultShardManager().getShard(shardId);
        // 4 replicas, replica[0] worker id not exist, replica[1] worker dead. replica[2], replica[3] GOOD
        long nonExistWorkerId = 13579;
        shard.addReplica(nonExistWorkerId);
        shard.addReplica(workerIds.get(0));
        shard.addReplica(workerIds.get(1));
        shard.addReplica(workerIds.get(2));

        Worker worker = helper.getWorkerManager().getWorker(workerIds.get(0));
        worker.setState(WorkerState.DOWN);
        Assert.assertNull(helper.getWorkerManager().getWorker(nonExistWorkerId));

        Assert.assertTrue(realScheduler.isIdle());
        Assert.assertEquals(4L, shard.getReplicaSize());
        Assert.assertTrue(shard.hasReplica(nonExistWorkerId));
        Assert.assertTrue(shard.hasReplica(worker.getWorkerId()));

        new Expectations(realScheduler) { // partial mock realScheduler
            {
                realScheduler.scheduleRemoveFromWorker(defaultServiceId, (List<Long>) any, anyLong);
                minTimes = 1;
                result = new StarException(ExceptionCode.SCHEDULE, "injected exception for scheduleAsyncRemoveFromWorker");

                realScheduler.scheduleAsyncRemoveFromGroup(defaultServiceId, shardId, defaultWorkerGroupId);
                maxTimes = 0;
            }
        };

        MockShardChecker checker = new MockShardChecker(helper.getServiceManager(), helper.getWorkerManager(), realScheduler);
        checker.runServiceHealthCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
        // every thing is good. Nothing changed, because scheduler excepted
        Assert.assertEquals(4L, shard.getReplicaSize());
        Assert.assertTrue(shard.hasReplica(nonExistWorkerId));
        Assert.assertTrue(shard.hasReplica(worker.getWorkerId()));
    }

    @Test
    public void testShardCheckExcepted() {
        helper.setDefaultWorkerGroupReplicaNumber(2);
        long shardId = helper.createTestShard(0);
        List<Long> workerIds = helper.createTestWorkersToWorkerGroup(5, defaultWorkerGroupId);
        // create a shard with 2 replicas.
        Shard shard = helper.getDefaultShardManager().getShard(shardId);

        long nonExistWorkerId = 13579;
        shard.addReplica(nonExistWorkerId);
        shard.addReplica(workerIds.get(0));
        shard.addReplica(workerIds.get(1));
        shard.addReplica(workerIds.get(2));

        Worker worker = helper.getWorkerManager().getWorker(workerIds.get(0));

        Assert.assertTrue(realScheduler.isIdle());
        Assert.assertEquals(4L, shard.getReplicaSize());
        Assert.assertTrue(shard.hasReplica(nonExistWorkerId));
        Assert.assertTrue(shard.hasReplica(worker.getWorkerId()));

        {
            MockShardChecker checker =
                    new MockShardChecker(helper.getServiceManager(), helper.getWorkerManager(), realScheduler);
            new Expectations(checker) { // partial mock realScheduler
                {
                    checker.shardHealthCheck(withAny(shard));
                    result = new StarException(ExceptionCode.SCHEDULE,
                            "injected exception for ShardChecker::shardHealthCheck");
                    maxTimes = 1;
                    minTimes = 1;
                }
            };
            // StarException is captured
            checker.runServiceHealthCheck(defaultServiceId);

            // Nothing changed
            Assert.assertTrue(realScheduler.isIdle());
            Assert.assertEquals(4L, shard.getReplicaSize());
            Assert.assertTrue(shard.hasReplica(nonExistWorkerId));
            Assert.assertTrue(shard.hasReplica(worker.getWorkerId()));
        }

        {
            MockShardChecker checker =
                    new MockShardChecker(helper.getServiceManager(), helper.getWorkerManager(), realScheduler);
            new Expectations(checker) { // partial mock realScheduler
                {
                    checker.shardHealthCheck(withAny(shard));
                    result = new ConcurrentModificationException(
                            "Injected ConcurrentModificationException for ShardChecker::shardHealthCheck");
                    minTimes = 1;
                    maxTimes = 1;
                }
            };
            // ConcurrentModificationException is captured
            checker.runServiceHealthCheck(defaultServiceId);

            // Nothing changed
            Assert.assertTrue(realScheduler.isIdle());
            Assert.assertEquals(4L, shard.getReplicaSize());
            Assert.assertTrue(shard.hasReplica(nonExistWorkerId));
            Assert.assertTrue(shard.hasReplica(worker.getWorkerId()));
        }
    }

    @Test
    public void testSpreadShardGroupBalanceBasic() {
        HijackConfig maxSkew = new HijackConfig("SCHEDULER_BALANCE_MAX_SKEW", "2");
        // entry level case / sunny case
        long shardGroupId = helper.createTestShardGroup(PlacementPolicy.SPREAD);
        List<Long> shardIds = helper.createTestShards(4, shardGroupId);
        long workerGroupId = helper.createWorkerGroup();
        List<Long> workerIds = helper.createTestWorkersToWorkerGroup(2, workerGroupId);
        helper.setWorkerGroupWarmupLevel(workerGroupId, WarmupLevel.WARMUP_ALL);

        // 4 shards (x1 replica) to 2 workers, expect each worker has two replicas
        realScheduler.scheduleAddToGroup(defaultServiceId, shardIds, workerGroupId);
        Map<Long, Integer> stats = helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);
        Assert.assertEquals(workerIds.size(), stats.size());
        // each worker has two shards (replicas)
        List<Integer> distribution = Arrays.asList(2, 2);
        Assert.assertEquals(distribution, new ArrayList<>(stats.values()));

        MockShardChecker checker = new MockShardChecker(helper.getServiceManager(), helper.getWorkerManager(), realScheduler);
        checker.runServiceBalanceCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
        { // Nothing changed
            Map<Long, Integer> statsCurrent = helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);
            Assert.assertEquals(stats, statsCurrent);
        }

        // Add two more workers into workerGroup
        workerIds.addAll(helper.createTestWorkersToWorkerGroup(2, workerGroupId));
        // run the check again
        checker.runServiceBalanceCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
        { // Nothing changed, due to maxSkew == 2
            Map<Long, Integer> statsCurrent = helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);
            Assert.assertEquals(stats, statsCurrent);
        }

        // change to "1"
        maxSkew.updateValue("1");
        checker.runServiceBalanceCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
        // now each worker has a SCALE_IN replica and a SCALE_OUT replica, with (2, 2, 1, 1) distribution
        Map<Long, Integer> statsCurrent = helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);
        Assert.assertEquals(workerIds.size(), statsCurrent.size());
        Assert.assertEquals(Arrays.asList(2, 2, 1, 1), new ArrayList<>(statsCurrent.values()));

        // update the replica to REPLICA_OK
        for (long shardId : shardIds) {
            Shard shard = helper.getDefaultShardManager().getShard(shardId);
            shard.getReplica().forEach(x -> shard.scaleOutReplicaDone(x.getWorkerId()));
        }

        // run healthCheck, those SCALE_IN replicas will be removed.
        checker.runServiceHealthCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
        // now each worker has a REPLICA_OK replica, with (1, 1, 1, 1) distribution
        statsCurrent = helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);
        Assert.assertEquals(workerIds.size(), statsCurrent.size());
        Assert.assertEquals(Arrays.asList(1, 1, 1, 1), new ArrayList<>(statsCurrent.values()));

        maxSkew.reset();
    }

    @Test
    public void testSpreadShardGroupReachesBalanceInOneRound() {
        HijackConfig maxSkew = new HijackConfig("SCHEDULER_BALANCE_MAX_SKEW", "1");
        // 1. Spread shard group contains 12 shard in 3 workers, with a 4-4-4 distribution
        // 2. A new worker is added
        // 3. SpreadGroup balance check runs once, the distribution is changed to 4-4-4-3 where REPLICA_SCALE_OUT
        //    replicas are on the new workers
        // 4. set REPLICA_SCALE_OUT to REPLICA_OK
        // 5. Run shardHealthyCheck, REPLICA_SCALE_IN replicas are removed, the distribution reaches final 3-3-3-3
        long shardGroupId = helper.createTestShardGroup(PlacementPolicy.SPREAD);
        List<Long> shardIds = helper.createTestShards(12, shardGroupId);
        long workerGroupId = helper.createWorkerGroup();
        List<Long> workerIds = helper.createTestWorkersToWorkerGroup(3, workerGroupId);
        helper.setWorkerGroupWarmupLevel(workerGroupId, WarmupLevel.WARMUP_ALL);

        // 12 shards (x1 replica) to 3 workers, expect 4-4-4 distribution
        realScheduler.scheduleAddToGroup(defaultServiceId, shardIds, workerGroupId);
        Map<Long, Integer> stats = helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);
        Assert.assertEquals(workerIds.size(), stats.size());
        List<Integer> distribution = Arrays.asList(4, 4, 4);
        Assert.assertEquals(distribution, new ArrayList<>(stats.values()));
        // set all replica to REPLICA_OK state
        shardIds.stream().map(x -> helper.getDefaultShardManager().getShard(x))
                .forEach(x -> x.getReplica().forEach(y -> x.scaleOutReplicaDone(y.getWorkerId())));

        MockShardChecker checker = new MockShardChecker(helper.getServiceManager(), helper.getWorkerManager(), realScheduler);
        checker.runServiceBalanceCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
        { // Nothing changed, already reaches balance state
            Map<Long, Integer> statsCurrent = helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);
            Assert.assertEquals(stats, statsCurrent);
        }

        // Add one more worker into workerGroup
        workerIds.addAll(helper.createTestWorkersToWorkerGroup(1, workerGroupId));
        // run the check again
        checker.runServiceBalanceCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
        // expect a 4-4-4-3 distribution
        Map<Long, Integer> statsCurrent = helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);
        Assert.assertEquals(workerIds.size(), statsCurrent.size());
        Assert.assertEquals(Arrays.asList(4, 4, 4, 3), new ArrayList<>(statsCurrent.values()));

        // validate shard replica state
        int balancedShardCount = 0;
        for (long shardId : shardIds) {
            Shard shard = helper.getDefaultShardManager().getShard(shardId);
            if (shard.getReplicaSize() == 1) { // the shard is not selected for balance
                Assert.assertEquals(1L,
                        shard.getReplica().stream().filter(x -> x.getState() == ReplicaState.REPLICA_OK).count());
            } else { // the shard is selected for balance
                ++balancedShardCount;
                Assert.assertEquals(1L,
                        shard.getReplica().stream().filter(x -> x.getState() == ReplicaState.REPLICA_SCALE_IN).count());
                Assert.assertEquals(1L,
                        shard.getReplica().stream().filter(x -> x.getState() == ReplicaState.REPLICA_SCALE_OUT)
                                .count());
                shard.getReplica().stream().filter(x -> x.getState() == ReplicaState.REPLICA_SCALE_OUT)
                        .forEach(x -> x.setReplicaState(ReplicaState.REPLICA_OK));
            }
        }
        Assert.assertEquals(3L, balancedShardCount);

        // run the check again
        checker.runServiceHealthCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
        // expect a 3-3-3-3 distribution
        statsCurrent = helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);
        Assert.assertEquals(workerIds.size(), statsCurrent.size());
        Assert.assertEquals(Arrays.asList(3, 3, 3, 3), new ArrayList<>(statsCurrent.values()));
        maxSkew.reset();
    }

    double calculateDiff(Map<Long, Integer> stats) {
        if (stats.isEmpty()) {
            return 0;
        }
        List<Integer> values = new ArrayList<>(stats.values());
        Collections.sort(values);
        int min = values.get(0);
        int max = values.get(values.size() - 1);
        int total = 0;
        for (int v : values) {
            total += v;
        }
        return (max - min) * values.size() / (double) total;
    }

    @Test
    public void testSpreadShardGroupBalanceWorkersShardNum() {
        HijackConfig maxSkew = new HijackConfig("SCHEDULER_BALANCE_MAX_SKEW", "1");
        HijackConfig balanceWorkerShards = new HijackConfig("ENABLE_BALANCE_SHARD_NUM_BETWEEN_WORKERS", "false");
        // 1. create 50 spread groups with each have 5 shards (single replica)
        // 2. create 5 workers for a worker group
        // 3. expect each worker has 50 replicas
        // 4. Add one more worker into the workerGroup
        // 5. (WorkerNum Balanced disabled), Expect no balance no matter how many rounds checked
        // 6. (WorkerNum Balanced enabled). Expect balance happens, the (max - min) / avg should be around threshold
        List<Long> allShardIds = new ArrayList<>();
        int numShardGroups = 50;
        int numShards = 5;
        for (int i = 0; i < numShardGroups; ++i) {
            long shardGroupId = helper.createTestShardGroup(PlacementPolicy.SPREAD);
            List<Long> shardIds = helper.createTestShards(numShards, shardGroupId);
            allShardIds.addAll(shardIds);
        }
        long workerGroupId = helper.createWorkerGroup();
        helper.setWorkerGroupWarmupLevel(workerGroupId, WarmupLevel.WARMUP_NOTHING);
        List<Long> workerIds = helper.createTestWorkersToWorkerGroup(5, workerGroupId);
        realScheduler.scheduleAddToGroup(defaultServiceId, allShardIds, workerGroupId);
        Map<Long, Integer> stats = helper.collectShardReplicaDistributionStats(allShardIds, workerGroupId);
        Assert.assertEquals(workerIds.size(), stats.size());
        List<Integer> distribution = Arrays.asList(50, 50, 50, 50, 50);
        Assert.assertEquals(distribution, new ArrayList<>(stats.values()));

        // Add one more worker into the group
        long newWorkerId = helper.createTestWorkerToWorkerGroup(workerGroupId);
        MockShardChecker checker = new MockShardChecker(helper.getServiceManager(), helper.getWorkerManager(), realScheduler);
        int repeatSchedules = 40;

        // Disable the balance between workers due to shard num imbalance
        Config.ENABLE_BALANCE_SHARD_NUM_BETWEEN_WORKERS = false;
        for (int i = 0; i < repeatSchedules; ++i) {
            checker.runServiceBalanceCheck(defaultServiceId);
            Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
        }
        { // Nothing changed, already reaches balance state, no shard will be migrated to the new worker
            Map<Long, Integer> statsCurrent = helper.collectShardReplicaDistributionStats(allShardIds, workerGroupId);
            Assert.assertEquals(stats, statsCurrent);
        }

        // Turn on the balance between workers due to number of shards imbalance.
        // However, the balance is a probability based, it will be eventually reaches the threshold limit,
        // but the time to take is not predictable.
        Config.ENABLE_BALANCE_SHARD_NUM_BETWEEN_WORKERS = true;
        LOG.info("The shard distribution before balance: {}", stats);
        for (int i = 0; i < repeatSchedules; ++i) {
            checker.runServiceBalanceCheck(defaultServiceId);
            Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
            Map<Long, Integer> statsCurrent = helper.collectShardReplicaDistributionStats(allShardIds, workerGroupId);
            LOG.debug("The shard distribution after single balance check: {}", statsCurrent);
        }
        {
            Map<Long, Integer> statsCurrent = helper.collectShardReplicaDistributionStats(allShardIds, workerGroupId);
            Assert.assertEquals(workerIds.size() + 1, statsCurrent.size());
            Assert.assertTrue(statsCurrent.containsKey(newWorkerId));
            Assert.assertTrue(statsCurrent.get(newWorkerId) > 0);
            LOG.info("The final shard distribution: {}", statsCurrent);
            double diff = calculateDiff(statsCurrent);
            // If this assertion fails, increase the number of `repeatSchedules`
            Assert.assertTrue(String.format("diff: %.2f", diff),
                    diff <= Config.BALANCE_WORKER_SHARDS_THRESHOLD_IN_PERCENT);
        }

        // Add 2 more workers into the group
        long newWorkerId2 = helper.createTestWorkerToWorkerGroup(workerGroupId);
        long newWorkerId3 = helper.createTestWorkerToWorkerGroup(workerGroupId);
        for (int i = 0; i < repeatSchedules; ++i) {
            checker.runServiceBalanceCheck(defaultServiceId);
            Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
            Map<Long, Integer> statsCurrent = helper.collectShardReplicaDistributionStats(allShardIds, workerGroupId);
            LOG.debug("The shard distribution after single balance check: {}", statsCurrent);
        }
        {
            Map<Long, Integer> statsCurrent = helper.collectShardReplicaDistributionStats(allShardIds, workerGroupId);
            Assert.assertEquals(workerIds.size() + 3, statsCurrent.size());
            Assert.assertTrue(statsCurrent.containsKey(newWorkerId2));
            Assert.assertTrue(statsCurrent.get(newWorkerId2) > 0);
            Assert.assertTrue(statsCurrent.containsKey(newWorkerId3));
            Assert.assertTrue(statsCurrent.get(newWorkerId3) > 0);
            LOG.info("The final shard distribution: {}", statsCurrent);
            double diff = calculateDiff(statsCurrent);
            // If this assertion fails, increase the number of `repeatSchedules`
            Assert.assertTrue(String.format("diff: %.2f", diff),
                    diff <= Config.BALANCE_WORKER_SHARDS_THRESHOLD_IN_PERCENT);
        }

        maxSkew.reset();
        balanceWorkerShards.reset();
    }

    @Test
    public void testSpreadShardGroupBlackListFailedWorkerId() {
        HijackConfig maxSkew = new HijackConfig("SCHEDULER_BALANCE_MAX_SKEW", "1");
        // 1. Spread shard group contains 12 shard in 3 workers, with a 4-4-4 distribution
        // 2. two new workers are added
        // 3. scheduleAddToWorker() call to one of the new workers fails and blacklisted
        // 3. SpreadGroup balance check runs once, the distribution is changed to 3-3-3-3-0
        long shardGroupId = helper.createTestShardGroup(PlacementPolicy.SPREAD);
        List<Long> shardIds = helper.createTestShards(12, shardGroupId);
        long workerGroupId = helper.createWorkerGroup();
        helper.setWorkerGroupWarmupLevel(workerGroupId, WarmupLevel.WARMUP_META);
        List<Long> workerIds = helper.createTestWorkersToWorkerGroup(3, workerGroupId);

        // 12 shards (x1 replica) to 3 workers, expect 4-4-4 distribution
        realScheduler.scheduleAddToGroup(defaultServiceId, shardIds, workerGroupId);
        Map<Long, Integer> stats = helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);
        Assert.assertEquals(workerIds.size(), stats.size());
        List<Integer> distribution = Arrays.asList(4, 4, 4);
        Assert.assertEquals(distribution, new ArrayList<>(stats.values()));
        // set all replica to REPLICA_OK state
        shardIds.stream().map(x -> helper.getDefaultShardManager().getShard(x))
                .forEach(x -> x.getReplica().forEach(y -> x.scaleOutReplicaDone(y.getWorkerId())));

        MockShardChecker checker = new MockShardChecker(helper.getServiceManager(), helper.getWorkerManager(), realScheduler);
        checker.runServiceBalanceCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
        { // Nothing changed, already reaches balance state
            Map<Long, Integer> statsCurrent = helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);
            Assert.assertEquals(stats, statsCurrent);
        }

        // Add one more worker into workerGroup
        workerIds.add(helper.createTestWorkerToWorkerGroup(workerGroupId));
        long workerId = helper.createTestWorkerToWorkerGroup(workerGroupId);

        List<Long> aliveIds = helper.getWorkerManager().getWorkerGroup(defaultServiceId, workerGroupId).getAllWorkerIds(true);
        Assert.assertTrue(aliveIds.contains(workerId));
        Assert.assertFalse(workerIds.contains(workerId));

        new Expectations(realScheduler) { // partially mock
            {
                // inject exception when call scheduleAddToWorker with given workerId
                realScheduler.scheduleAddToWorker(defaultServiceId, (List<Long>) any, workerId);
                minTimes = 1;
                maxTimes = 1;
                result = new StarException(ExceptionCode.SCHEDULE, "Injected Exception");
            }
        };
        // run the check again
        checker.runServiceBalanceCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
        // expect a 4-4-4-3 distribution, Worker:workerId doesn't have any shard balanced to.
        Map<Long, Integer> statsCurrent = helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);
        Assert.assertEquals(workerIds.size(), statsCurrent.size());
        Assert.assertEquals(Arrays.asList(4, 4, 4, 3), new ArrayList<>(statsCurrent.values()));

        // validate shard replica state
        int balancedShardCount = 0;
        for (long shardId : shardIds) {
            Shard shard = helper.getDefaultShardManager().getShard(shardId);
            if (shard.getReplicaSize() == 1) { // the shard is not selected for balance
                Assert.assertEquals(1L,
                        shard.getReplica().stream().filter(x -> x.getState() == ReplicaState.REPLICA_OK).count());
            } else { // the shard is selected for balance
                ++balancedShardCount;
                Assert.assertEquals(1L,
                        shard.getReplica().stream().filter(x -> x.getState() == ReplicaState.REPLICA_SCALE_IN).count());
                Assert.assertEquals(1L,
                        shard.getReplica().stream().filter(x -> x.getState() == ReplicaState.REPLICA_SCALE_OUT)
                                .count());
                shard.getReplica().stream().filter(x -> x.getState() == ReplicaState.REPLICA_SCALE_OUT)
                        .forEach(x -> x.setReplicaState(ReplicaState.REPLICA_OK));
            }
        }
        Assert.assertEquals(3L, balancedShardCount);

        // run the health check to remove the REPLICA_SCALE_IN replica
        checker.runServiceHealthCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
        // expect a 3-3-3-3 distribution, Worker:workerId doesn't have any shard balanced to.
        statsCurrent = helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);
        Assert.assertEquals(workerIds.size(), statsCurrent.size());
        Assert.assertEquals(Arrays.asList(3, 3, 3, 3), new ArrayList<>(statsCurrent.values()));

        maxSkew.reset();
    }

    @Test
    public void testNonExistServiceBalanceCheckNoException() {
        MockShardChecker checker = new MockShardChecker(helper.getServiceManager(), helper.getWorkerManager(), null);
        // no exception.
        checker.runServiceBalanceCheck("non-exist-service-id");
    }

    @Test
    public void testShardInPackGroupNotSelectedForBalance() {
        HijackConfig maxSkew = new HijackConfig("SCHEDULER_BALANCE_MAX_SKEW", "1");
        // 3 shards (A, B, C) in the SPREAD group
        // A in a PACK group (with D which is not scheduled),
        // B in a EXCLUDE group (with E which is not scheduled).
        // C will be chosen to move around.

        long spreadGroupId = helper.createTestShardGroup(PlacementPolicy.SPREAD);
        long packGroupId = helper.createTestShardGroup(PlacementPolicy.PACK);
        long excludeGroupId = helper.createTestShardGroup(PlacementPolicy.EXCLUDE);

        long shardAId = helper.createTestShard(Arrays.asList(spreadGroupId, packGroupId));
        long shardBId = helper.createTestShard(Arrays.asList(spreadGroupId, excludeGroupId));
        long shardCId = helper.createTestShard(spreadGroupId);

        // create D & E. Add one shard to packGroup & excludeGroup, so the shard group will actually take effect even though the
        // shards are not scheduled.
        helper.createTestShard(packGroupId);
        helper.createTestShard(excludeGroupId);
        List<Long> shardIds = Arrays.asList(shardAId, shardBId, shardCId);
        long workerGroupId = helper.createWorkerGroup();
        helper.setWorkerGroupWarmupLevel(workerGroupId, WarmupLevel.WARMUP_META);
        long workerId1 = helper.createTestWorkerToWorkerGroup(workerGroupId);
        // schedule 3 shards on to the same worker
        realScheduler.scheduleAddToGroup(defaultServiceId, shardIds, workerGroupId);
        Map<Long, Integer> stats = helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);
        Assert.assertEquals(1L, stats.size());
        Assert.assertEquals((Integer) 3, new ArrayList<>(stats.values()).get(0));
        // set all replica to REPLICA_OK state
        shardIds.stream().map(x -> helper.getDefaultShardManager().getShard(x))
                .forEach(x -> x.getReplica().forEach(y -> x.scaleOutReplicaDone(y.getWorkerId())));

        { // null scheduler, nothing happens because only 1 worker in the workerGroup
            MockShardChecker checker = new MockShardChecker(helper.getServiceManager(), helper.getWorkerManager(), null);
            checker.runServiceBalanceCheck(defaultServiceId);
            Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
            Map<Long, Integer> statsCurrent = helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);
            Assert.assertEquals(stats, statsCurrent);
        }

        // add one more worker
        long workerId2 = helper.createTestWorkerToWorkerGroup(workerGroupId);

        List<Long> aReplicas = new ArrayList<>(helper.getDefaultShardManager().getShard(shardAId).getReplicaWorkerIds());
        List<Long> bReplicas = new ArrayList<>(helper.getDefaultShardManager().getShard(shardBId).getReplicaWorkerIds());
        List<Long> cReplicas = new ArrayList<>(helper.getDefaultShardManager().getShard(shardCId).getReplicaWorkerIds());
        Assert.assertEquals((Long) workerId1, cReplicas.get(0));

        MockShardChecker checker = new MockShardChecker(helper.getServiceManager(), helper.getWorkerManager(), realScheduler);
        checker.runServiceBalanceCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
        // A & B not changed.
        Assert.assertEquals(aReplicas, new ArrayList<>(helper.getDefaultShardManager().getShard(shardAId).getReplicaWorkerIds()));
        Assert.assertEquals(bReplicas, new ArrayList<>(helper.getDefaultShardManager().getShard(shardBId).getReplicaWorkerIds()));
        List<Long> cReplicasAfter = new ArrayList<>(helper.getDefaultShardManager().getShard(shardCId).getReplicaWorkerIds());
        // A scale-in, and a scale-out replica
        Assert.assertEquals(2, cReplicasAfter.size());
        Assert.assertNotEquals(cReplicas, cReplicasAfter);

        Shard shardC = helper.getDefaultShardManager().getShard(shardCId);
        shardC.getReplica().forEach(x -> shardC.scaleOutReplicaDone(x.getWorkerId()));

        // run the health check, the SCALE_IN replica get removed
        checker.runServiceHealthCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
        // A & B not changed.
        Assert.assertEquals(aReplicas,
                new ArrayList<>(helper.getDefaultShardManager().getShard(shardAId).getReplicaWorkerIds()));
        Assert.assertEquals(bReplicas,
                new ArrayList<>(helper.getDefaultShardManager().getShard(shardBId).getReplicaWorkerIds()));
        List<Long> cReplicasAfter2 =
                new ArrayList<>(helper.getDefaultShardManager().getShard(shardCId).getReplicaWorkerIds());
        Assert.assertNotEquals(cReplicasAfter, cReplicasAfter2);
        Assert.assertEquals(1, cReplicasAfter2.size());
        Assert.assertEquals((Long) workerId2, cReplicasAfter2.get(0));

        maxSkew.reset();
    }

    @Test
    public void testSpreadShardBalanceFailToAddNewReplicaOldNotRemoved() {
        try (HijackConfig ignore = new HijackConfig("SCHEDULER_BALANCE_MAX_SKEW", "1")) {
            // create 3 shards x 1 replica in a SPREAD shard group
            long spreadGroupId = helper.createTestShardGroup(PlacementPolicy.SPREAD);
            List<Long> shardIds = helper.createTestShards(3, spreadGroupId);

            long workerGroupId = helper.createWorkerGroup();
            long workerId1 = helper.createTestWorkerToWorkerGroup(workerGroupId);
            realScheduler.scheduleAddToGroup(defaultServiceId, shardIds, workerGroupId);

            Map<Long, Integer> stats = helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);
            Assert.assertEquals(1L, stats.size());
            Assert.assertEquals((Integer) 3, stats.get(workerId1));

            // Add a new worker, trigger the balance
            helper.createTestWorkerToWorkerGroup(workerGroupId);
            new Expectations(realScheduler) { // partially mock
                {
                    realScheduler.scheduleAddToWorker(defaultServiceId, (List<Long>) any, anyLong);
                    minTimes = 1;
                    maxTimes = 1;
                    result = new StarException(ExceptionCode.SCHEDULE, "Injected Exception");
                    // should not call this at all.
                    realScheduler.scheduleAsyncRemoveFromWorker(anyString, (List<Long>) any, anyLong);
                    minTimes = 0;
                    maxTimes = 0;
                }
            };

            MockShardChecker checker = new MockShardChecker(helper.getServiceManager(), helper.getWorkerManager(), realScheduler);
            checker.runServiceBalanceCheck(defaultServiceId);
            Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
            { // Nothing changed, because scheduleAddToGroup excepted
                Map<Long, Integer> statsCurrent = helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);
                Assert.assertEquals(stats, statsCurrent);
            }
        }
    }

    @Test
    public void testPackGroupBalanceWorkerNotEnough() {
        // set default to 3 replicas
        int replicaNum = 3;
        long shardGroupId = helper.createTestShardGroup(PlacementPolicy.PACK);
        // create 3 shards
        List<Long> shardIds = helper.createTestShards(3, shardGroupId);
        long workerGroupId = helper.createWorkerGroup(replicaNum);
        long workerId1 = helper.createTestWorkerToWorkerGroup(workerGroupId);
        // do the schedule, each shard can only schedule 1 replica
        realScheduler.scheduleAddToGroup(defaultServiceId, shardIds, workerGroupId);
        Map<Long, Integer> stats = helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);
        Assert.assertEquals(1L, stats.size());
        Assert.assertTrue(stats.containsKey(workerId1));
        Assert.assertEquals((Integer) shardIds.size(), stats.get(workerId1));

        MockShardChecker checker = new MockShardChecker(helper.getServiceManager(), helper.getWorkerManager(), realScheduler);
        // nothing changed
        checker.runServiceBalanceCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
        {
            Map<Long, Integer> stats2 = helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);
            Assert.assertEquals(stats, stats2);
        }
        // Add the 2nd worker
        helper.createTestWorkerToWorkerGroup(workerGroupId);
        Assert.assertEquals(2, helper.getWorkerManager().getWorkerGroup(defaultServiceId, workerGroupId).getWorkerCount());
        // Still nothing changed, due to numOfWorkers < numOfReplicas
        checker.runServiceBalanceCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
        {
            Map<Long, Integer> stats2 = helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);
            Assert.assertEquals(stats, stats2);
        }
        // Add the 3rd worker
        helper.createTestWorkerToWorkerGroup(workerGroupId);
        Assert.assertEquals(3, helper.getWorkerManager().getWorkerGroup(defaultServiceId, workerGroupId).getWorkerCount());
        checker.runServiceBalanceCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
        {
            Map<Long, Integer> stats2 = helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);
            Assert.assertEquals(3, stats2.size());
            List<Integer> distribution = Arrays.asList(3, 3, 3);
            Assert.assertEquals(distribution, new ArrayList<>(stats2.values()));
        }
    }

    @Test
    public void testSinglePackGroupBalanceMissingReplica() {
        int expectedReplica = 3;
        // Pack Group Balance Checker will not specifically add missing replicas back.
        long shardGroupId = helper.createTestShardGroup(PlacementPolicy.PACK);
        List<Long> shardIds = helper.createTestShards(3, shardGroupId);
        long workerGroupId = helper.createWorkerGroup(expectedReplica);
        List<Long> workerIds = helper.createTestWorkersToWorkerGroup(4, workerGroupId);
        // set workers[3] DOWN, so the PACK group will be scheduled to workers[0~2]
        helper.setWorkerStatus(workerIds.get(3), WorkerState.DOWN);

        realScheduler.scheduleAddToGroup(defaultServiceId, shardIds, workerGroupId);
        helper.setWorkerStatus(workerIds.get(3), WorkerState.ON);
        Map<Long, Integer> stats = helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);
        Assert.assertEquals(3, stats.size());
        List<Integer> distribution = Arrays.asList(3, 3, 3);
        Assert.assertEquals(distribution, new ArrayList<>(stats.values()));

        Shard shard0 = helper.getDefaultShardManager().getShard(shardIds.get(0));
        Assert.assertEquals(3, shard0.getReplicaSize());
        // manual remove the first
        helper.getDefaultShardManager().removeShardReplicas(Arrays.asList(shard0.getShardId()), workerIds.get(0));
        Assert.assertEquals(2, shard0.getReplicaSize());

        Map<Long, Integer> statsBefore = helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);
        // give a NULL scheduler, expect triggering no scheduling, otherwise NPE exception will be thrown
        MockShardChecker checker = new MockShardChecker(helper.getServiceManager(), helper.getWorkerManager(), null);
        checker.runServiceBalanceCheck(defaultServiceId);
        Map<Long, Integer> statsAfter = helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);
        // balance checker will not be particularly check missing replicas and adding back.
        Assert.assertEquals(statsAfter, statsBefore);
    }

    @Test
    public void testTwoPackGroupsBalancedTogether() {
        // The workergroup needs 2 replica for every shard
        int replicaNum = 2;
        // PACK X (A, B, C) && PACK Y (A, D, E) => PACK (A, B, C, D, E)
        long packX = helper.createTestShardGroup(PlacementPolicy.PACK);
        long packY = helper.createTestShardGroup(PlacementPolicy.PACK);
        // A, B, C in PACK-X
        List<Long> shardIdX = helper.createTestShards(3, packX);
        // D, E in PACK-Y
        List<Long> shardIdY = helper.createTestShards(2, packY);
        List<Long> allShardIds = new ArrayList<>(shardIdX);
        allShardIds.addAll(shardIdY);

        long workerGroupId = helper.createWorkerGroup(replicaNum);
        // Create 4 workers in the worker group
        helper.createTestWorkersToWorkerGroup(4, workerGroupId);

        // shardIdX should be scheduled to 2 out of 4 workers
        realScheduler.scheduleAddToGroup(defaultServiceId, shardIdX, workerGroupId);
        // shardIdY should be scheduled to 2 out of 4 workers
        realScheduler.scheduleAddToGroup(defaultServiceId, shardIdY, workerGroupId);
        Map<Long, Integer> statsX = helper.collectShardReplicaDistributionStats(shardIdX, workerGroupId);
        Assert.assertEquals(2, statsX.size());
        Map<Long, Integer> statsY = helper.collectShardReplicaDistributionStats(shardIdY, workerGroupId);
        Assert.assertEquals(2, statsY.size());
        Set<Long> workerForX = statsX.keySet();
        Set<Long> workerForY = statsY.keySet();
        workerForX.forEach(x -> Assert.assertFalse(workerForY.contains(x)));
        workerForY.forEach(y -> Assert.assertFalse(workerForX.contains(y)));

        // Now, let shardX[0] join PACK Y, so the two PACK group are connected
        helper.getDefaultShardManager().getShard(shardIdX.get(0)).joinGroup(packY);
        helper.getDefaultShardManager().getShardGroup(packY).addShardId(shardIdX.get(0));

        MockShardChecker checker = new MockShardChecker(helper.getServiceManager(), helper.getWorkerManager(), realScheduler);
        checker.runServiceBalanceCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
        // After the check, all the shards are packed together
        Map<Long, Integer> stats = helper.collectShardReplicaDistributionStats(allShardIds, workerGroupId);
        Assert.assertEquals(2, stats.size());
        Assert.assertEquals(Collections.nCopies(2, allShardIds.size()), new ArrayList<>(stats.values()));
        Set<Long> workerForAll = stats.keySet();
        // all moved to workerForX, because workerForY has fewer replicas than workerForX
        Assert.assertEquals(workerForX, workerForAll);
    }

    @Test
    public void testSinglePackGroupBalancedInTwoWorkerGroups() {
        int expectedReplica = 2;
        // a PACK group with 3 shards (A, B, C) configured with 2 replicas, scheduled two worker groups
        // workerGroup1 (W1, W2, W3): W1(A1, B1, C1), W2 (A1, B1), W3(C1)
        // workerGroup2 (Z1, Z2, Z3): Z1(A1), Z2(B1, C1), Z3(A1, B1, C1)
        long shardGroupId = helper.createTestShardGroup(PlacementPolicy.PACK);
        List<Long> shardIds = helper.createTestShards(3, shardGroupId);
        long workerGroup1 = helper.createWorkerGroup(expectedReplica);
        List<Long> workerIds1 = helper.createTestWorkersToWorkerGroup(2, workerGroup1);
        ShardManager shardManager = helper.getDefaultShardManager();

        // scheduled to WG1 (W1, W2)
        realScheduler.scheduleAddToGroup(defaultServiceId, shardIds, workerGroup1);
        Map<Long, Integer> statsW1 = helper.collectShardReplicaDistributionStats(shardIds, workerGroup1);

        // add W3 into WG1
        workerIds1.add(helper.createTestWorkerToWorkerGroup(workerGroup1));
        // manual move one replica of A from W1 to W3
        Shard shard0 = shardManager.getShard(shardIds.get(0));
        // Add shardA replica -> WG1-W3
        shardManager.addShardReplicas(Collections.nCopies(1, shard0.getShardId()), workerIds1.get(2), false);
        shardManager.removeShardReplicas(Collections.nCopies(1, shard0.getShardId()), workerIds1.get(0));
        Map<Long, Integer> tmp1 = helper.collectShardReplicaDistributionStats(shardIds, workerGroup1);
        // After manual adjustment, the distribution changed
        Assert.assertNotEquals(statsW1, tmp1);

        long workerGroup2 = helper.createWorkerGroup(expectedReplica);
        List<Long> workerIds2 = helper.createTestWorkersToWorkerGroup(3, workerGroup2);
        // set WG2 -> Z1 DOWN, so the shards will be scheduled to Z2, Z3
        helper.setWorkerStatus(workerIds2.get(0), WorkerState.DOWN);
        realScheduler.scheduleAddToGroup(defaultServiceId, shardIds, workerGroup2);
        Map<Long, Integer> statsW2 = helper.collectShardReplicaDistributionStats(shardIds, workerGroup2);

        // Get WG2-Z1 back
        helper.setWorkerStatus(workerIds2.get(0), WorkerState.ON);
        // Manual move A's replica from Z2 -> Z1
        shardManager.addShardReplicas(Collections.nCopies(1, shard0.getShardId()), workerIds2.get(0), false);
        shardManager.removeShardReplicas(Collections.nCopies(1, shard0.getShardId()), workerIds1.get(1));
        Map<Long, Integer> tmp2 = helper.collectShardReplicaDistributionStats(shardIds, workerGroup1);
        // After manual adjustment, the distribution changed
        Assert.assertNotEquals(statsW2, tmp2);

        // Do a balance check
        MockShardChecker checker = new MockShardChecker(helper.getServiceManager(), helper.getWorkerManager(), realScheduler);
        checker.runServiceBalanceCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);

        // Shard distribution in both worker group reverted.
        Map<Long, Integer> tmp11 = helper.collectShardReplicaDistributionStats(shardIds, workerGroup1);
        Assert.assertEquals(statsW1, tmp11);

        Map<Long, Integer> tmp22 = helper.collectShardReplicaDistributionStats(shardIds, workerGroup2);
        Assert.assertEquals(statsW2, tmp22);
    }

    @Test
    public void testPackGroupBalanceScheduleExcepted() {
        // Pack Group A, B, C x 1 replica, on W1(A, B), W2(C)
        // if the operation of moving C from W2 -> W1 is excepted, then no change will be made.
        long shardGroupId = helper.createTestShardGroup(PlacementPolicy.PACK);
        List<Long> shardIds = helper.createTestShards(3, shardGroupId);
        long workerGroupId = helper.createWorkerGroup();
        // create worker0 to the worker group
        List<Long> workerIds = helper.createTestWorkersToWorkerGroup(1, workerGroupId);
        // All shards scheduled
        realScheduler.scheduleAddToGroup(defaultServiceId, shardIds, workerGroupId);

        // create worker1 to the worker group
        workerIds.add(helper.createTestWorkerToWorkerGroup(workerGroupId));

        ShardManager shardManager = helper.getDefaultShardManager();
        Shard shardC = shardManager.getShard(shardIds.get(2));
        shardManager.removeShardReplicas(Collections.nCopies(1, shardC.getShardId()), workerIds.get(0));
        shardManager.addShardReplicas(Collections.nCopies(1, shardC.getShardId()), workerIds.get(1), false);
        Assert.assertEquals(1, shardC.getReplicaSize());
        Map<Long, Integer> stats = helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);

        new Expectations(realScheduler) { // partial mock realSchedule
            {
                realScheduler.scheduleAddToWorker(defaultServiceId, (List<Long>) any, workerIds.get(0));
                minTimes = 1;
                maxTimes = 1;
                result = new StarException(ExceptionCode.INTERNAL, "Injected exception from mock");
                // following method should not be invoked at all
                realScheduler.scheduleAsyncRemoveFromWorker(anyString, (List<Long>) any, anyLong);
                minTimes = 0;
                maxTimes = 0;
            }
        };

        MockShardChecker checker = new MockShardChecker(helper.getServiceManager(), helper.getWorkerManager(), realScheduler);
        // scheduleAddToWorker excepted, no replica changed will be made at all.
        checker.runServiceBalanceCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
        Map<Long, Integer> statsAfter = helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);
        Assert.assertEquals(stats, statsAfter);
    }

    @Test
    public void testExcludeGroupBalance() {
        // Have 3 shards with no group, and scheduled to worker-1
        // then manual put them into a EXCLUDE group, and add worker one by one to see how the replica balances
        List<Long> shardIds = helper.createTestShards(3, 0);
        long workerGroupId = helper.createWorkerGroup();
        long workerId1 = helper.createTestWorkerToWorkerGroup(workerGroupId);
        // all scheduled to the only worker, because right now, these shards are not in the same group yet.
        realScheduler.scheduleAddToGroup(defaultServiceId, shardIds, workerGroupId);
        Map<Long, Integer> stats = helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);
        Assert.assertEquals(1, stats.size());
        Assert.assertTrue(stats.containsKey(workerId1));
        Assert.assertEquals((Integer) 3, stats.get(workerId1));

        // Now manual put 3 shards into the same shard group
        long excludeGroupId = helper.createTestShardGroup(PlacementPolicy.EXCLUDE);
        ShardManager shardManager = helper.getDefaultShardManager();
        ShardGroup excludeGroup = shardManager.getShardGroup(excludeGroupId);
        shardIds.forEach(excludeGroup::addShardId);
        shardIds.forEach(x -> {
            Shard shard = shardManager.getShard(x);
            shard.joinGroup(excludeGroupId);
        });

        MockShardChecker checker = new MockShardChecker(helper.getServiceManager(), helper.getWorkerManager(), realScheduler);
        checker.runServiceBalanceCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
        { // nothing changed.
            Map<Long, Integer> statsTested = helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);
            Assert.assertEquals(stats, statsTested);
        }

        // Add worker2
        long workerId2 = helper.createTestWorkerToWorkerGroup(workerGroupId);
        checker.runServiceBalanceCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
        { // one replica moved to worker2
            Map<Long, Integer> statsTested = helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);
            Assert.assertEquals(2, statsTested.size());
            Assert.assertEquals((Integer) 2, statsTested.get(workerId1));
            Assert.assertEquals((Integer) 1, statsTested.get(workerId2));
        }

        // Add worker3
        long workerId3 = helper.createTestWorkerToWorkerGroup(workerGroupId);
        checker.runServiceBalanceCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
        { // one replica moved to worker3
            Map<Long, Integer> statsTested = helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);
            Assert.assertEquals(3, statsTested.size());
            Assert.assertEquals((Integer) 1, statsTested.get(workerId1));
            Assert.assertEquals((Integer) 1, statsTested.get(workerId2));
            Assert.assertEquals((Integer) 1, statsTested.get(workerId3));
        }

        // Add worker4, balance will not move any replica
        helper.createTestWorkerToWorkerGroup(workerGroupId);
        checker.runServiceBalanceCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
        {
            Map<Long, Integer> statsTested = helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);
            Assert.assertEquals(3, statsTested.size());
            Assert.assertEquals(Arrays.asList(1, 1, 1), new ArrayList<>(statsTested.values()));
        }
    }

    @Test
    public void testExcludeShardGroupBalanceExcepted() {
        // EXCLUDE GROUP has (A, B, C), 3 shards x 1 replica. workerGroup has 3 workers.
        // Manual move shard A's replica to another worker, so the distribution will be (2, 1, 0)
        long shardGroupId = helper.createTestShardGroup(PlacementPolicy.EXCLUDE);
        List<Long> shardIds = helper.createTestShards(3, shardGroupId);
        long workerGroupId = helper.createWorkerGroup();
        List<Long> workerIds = helper.createTestWorkersToWorkerGroup(3, workerGroupId);

        realScheduler.scheduleAddToGroup(defaultServiceId, shardIds, workerGroupId);
        {
            Map<Long, Integer> stats = helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);
            Assert.assertEquals(3, stats.size());
            Assert.assertEquals(Arrays.asList(1, 1, 1), new ArrayList<>(stats.values()));
        }

        Shard shard0 = helper.getDefaultShardManager().getShard(shardIds.get(0));
        List<Long> existingReplicas = shard0.getReplicaWorkerIds();
        Assert.assertEquals(1, existingReplicas.size());
        List<Long> candidates = workerIds.stream().filter(x -> !existingReplicas.contains(x)).collect(Collectors.toList());
        Assert.assertEquals(2, candidates.size());
        shard0.removeReplica(existingReplicas.get(0));
        shard0.addReplica(candidates.get(0));

        Map<Long, Integer> statsBefore = helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);

        // checker will try to move replica from candidates[0] to existingReplicas[0], but failed
        new Expectations(realScheduler) { // partial mock realSchedule
            {
                realScheduler.scheduleAddToWorker(defaultServiceId, (List<Long>) any, existingReplicas.get(0));
                minTimes = 1;
                maxTimes = 1;
                result = new StarException(ExceptionCode.INTERNAL, "Injected exception from mock");
                // following method should not be invoked at all
                realScheduler.scheduleAsyncRemoveFromWorker(anyString, (List<Long>) any, anyLong);
                minTimes = 0;
                maxTimes = 0;
            }
        };

        MockShardChecker checker = new MockShardChecker(helper.getServiceManager(), helper.getWorkerManager(), realScheduler);
        checker.runServiceBalanceCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
        Map<Long, Integer> statsAfter = helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);

        Assert.assertEquals(statsBefore, statsAfter);
    }

    @Test
    public void testMetaGroupSchedule() {
        // 1. create 2 SPREAD shard group with each group has 3 shards, scheduled to a workergroup desiring for 3 replicas.
        // 2. schedule them separately into 18 workers (each worker has 1 replicas).
        // 3. create a meta group, bundle 2 SPREAD shard group together into a meta group.
        // 4. expect shard checker redistributes shard replicas, enforce anonymous pack group rule, all replicas will be moved
        //  to 9 workers to enforce the PACK restriction
        long spreadGroup1 = helper.createTestShardGroup(PlacementPolicy.SPREAD);
        long spreadGroup2 = helper.createTestShardGroup(PlacementPolicy.SPREAD);
        int numWorkers = 18;
        List<Long> shardIds1 = helper.createTestShards(3, spreadGroup1);
        List<Long> shardIds2 = helper.createTestShards(3, spreadGroup2);
        long workerGroupId = helper.createWorkerGroup(3);
        helper.createTestWorkersToWorkerGroup(numWorkers, workerGroupId);

        realScheduler.scheduleAddToGroup(defaultServiceId, shardIds1, workerGroupId);
        realScheduler.scheduleAddToGroup(defaultServiceId, shardIds2, workerGroupId);

        List<Long> allShardIds = new ArrayList<>(shardIds1);
        allShardIds.addAll(shardIds2);

        Map<Long, Integer> stats = helper.collectShardReplicaDistributionStats(allShardIds, workerGroupId);
        Assert.assertEquals(numWorkers, stats.size());
        // each worker has only 1 replica
        Assert.assertEquals(Collections.nCopies(numWorkers, 1), new ArrayList<>(stats.values()));

        // create a metaGroup
        long metaGroupId = helper.createMetaGroup(PlacementPolicy.PACK, Arrays.asList(spreadGroup1, spreadGroup2));
        ShardManager shardManager = helper.getDefaultShardManager();
        // the new created metaGroup is unstable
        Assert.assertFalse(shardManager.isMetaGroupStable(
                metaGroupId,
                helper.getWorkerManager().getWorkerGroup(defaultServiceId, workerGroupId).getAllWorkerIds(false)));

        // trigger a shard balance check
        MockShardChecker checker = new MockShardChecker(helper.getServiceManager(), helper.getWorkerManager(), realScheduler);
        checker.runServiceBalanceCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
        // Overall validation
        Map<Long, Integer> statsAfter = helper.collectShardReplicaDistributionStats(allShardIds, workerGroupId);
        // Replicas moved to 9 workers, with each worker has two replicas.
        Assert.assertEquals(9, statsAfter.size());
        Assert.assertEquals(Collections.nCopies(9, 2), new ArrayList<>(statsAfter.values()));

        // two spread shard groups has identical replica distribution
        Map<Long, Integer> statsGroup1 = helper.collectShardReplicaDistributionStats(shardIds1, workerGroupId);
        Map<Long, Integer> statsGroup2 = helper.collectShardReplicaDistributionStats(shardIds2, workerGroupId);
        Assert.assertEquals(statsGroup1, statsGroup2);

        // Check every shard's replica stats
        Assert.assertEquals(shardIds1.size(), shardIds2.size());
        for (int i = 0; i < shardIds1.size(); ++i) {
            List<Long> xReplica = shardManager.getShard(shardIds1.get(i)).getReplicaWorkerIds();
            List<Long> yReplica = shardManager.getShard(shardIds2.get(i)).getReplicaWorkerIds();
            Assert.assertEquals(xReplica, yReplica);
        }
        // after balance check, the metaGroup is stable.
        Assert.assertTrue(shardManager.isMetaGroupStable(
                metaGroupId,
                helper.getWorkerManager().getWorkerGroup(defaultServiceId, workerGroupId).getAllWorkerIds(false)));
    }

    @Test
    public void testMetaGroupBalanceCase1() {
        // 1. create 2 SPREAD shard group with each have 3 shards x 1 replica
        // 2. create a meta group, bundle 2 SPREAD shard group together into a meta group.
        // 3. give one worker in the worker group
        // 4. after reaches stable status, (that is: all shard replicas are on the same node) - simulating a cluster scaling-in
        // to one node
        // 5. add the 2nd worker, trigger one round balance and check the final distribution
        // 6. add the 3rd worker, trigger one round balance and check the final distribution
        long spreadGroup1 = helper.createTestShardGroup(PlacementPolicy.SPREAD);
        long spreadGroup2 = helper.createTestShardGroup(PlacementPolicy.SPREAD);
        List<Long> shardIds1 = helper.createTestShards(3, spreadGroup1);
        List<Long> shardIds2 = helper.createTestShards(3, spreadGroup2);
        long workerGroupId = helper.createWorkerGroup();
        helper.createTestWorkersToWorkerGroup(1, workerGroupId);

        realScheduler.scheduleAddToGroup(defaultServiceId, shardIds1, workerGroupId);
        realScheduler.scheduleAddToGroup(defaultServiceId, shardIds2, workerGroupId);

        List<Long> allShardIds = new ArrayList<>(shardIds1);
        allShardIds.addAll(shardIds2);

        Map<Long, Integer> stats = helper.collectShardReplicaDistributionStats(allShardIds, workerGroupId);
        Assert.assertEquals(1, stats.size());
        // all replicas are on the same worker
        Assert.assertEquals(Collections.nCopies(1, 6), new ArrayList<>(stats.values()));

        // create a metaGroup
        long metaGroupId = helper.createMetaGroup(PlacementPolicy.PACK, Arrays.asList(spreadGroup1, spreadGroup2));
        ShardManager shardManager = helper.getDefaultShardManager();
        // the new created metaGroup is stable, because all are on the same worker
        Assert.assertTrue(shardManager.isMetaGroupStable(
                metaGroupId,
                helper.getWorkerManager().getWorkerGroup(defaultServiceId, workerGroupId).getAllWorkerIds(false)));

        MockShardChecker checker = new MockShardChecker(helper.getServiceManager(), helper.getWorkerManager(), realScheduler);

        // Add the 2nd worker into the worker group
        helper.createTestWorkerToWorkerGroup(workerGroupId);
        // trigger a shard balance check
        checker.runServiceBalanceCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
        {
            // Overall validation
            Map<Long, Integer> statsAfter = helper.collectShardReplicaDistributionStats(allShardIds, workerGroupId);
            // 3 packGroup on two workers, with packGroup distribution: (1, 2), with replica distribution: (2, 4)
            Assert.assertEquals(2, statsAfter.size());
            Assert.assertEquals(Arrays.asList(2, 4),
                    new ArrayList<>(statsAfter.values().stream().sorted().collect(Collectors.toList())));
            // two spread shard groups has identical replica distribution
            Map<Long, Integer> statsGroup1 = helper.collectShardReplicaDistributionStats(shardIds1, workerGroupId);
            Map<Long, Integer> statsGroup2 = helper.collectShardReplicaDistributionStats(shardIds2, workerGroupId);
            Assert.assertEquals(statsGroup1, statsGroup2);
        }

        // Add the 3nd worker into the worker group
        helper.createTestWorkerToWorkerGroup(workerGroupId);
        // trigger a shard balance check
        checker.runServiceBalanceCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
        {
            // Overall validation
            Map<Long, Integer> statsAfter = helper.collectShardReplicaDistributionStats(allShardIds, workerGroupId);
            // 3 packGroups on three workers, with packGroup distribution: (1, 1, 1), with replica distribution: (2, 2, 2)
            Assert.assertEquals(3, statsAfter.size());
            Assert.assertEquals(Arrays.asList(2, 2, 2), new ArrayList<>(statsAfter.values()));
            // two spread shard groups has identical replica distribution
            Map<Long, Integer> statsGroup1 = helper.collectShardReplicaDistributionStats(shardIds1, workerGroupId);
            Map<Long, Integer> statsGroup2 = helper.collectShardReplicaDistributionStats(shardIds2, workerGroupId);
            Assert.assertEquals(statsGroup1, statsGroup2);
        }

        // Add the 4th worker into the worker group, expect no balance happens at all
        helper.createTestWorkerToWorkerGroup(workerGroupId);
        // expect no balance happens at all, no `scheduleAddToWorker` invocation.
        new Expectations(realScheduler) { // partial mock realSchedule
            {
                realScheduler.scheduleAddToWorker(defaultServiceId, (List<Long>) any, anyLong);
                minTimes = 0;
                maxTimes = 0;
                result = new StarException(ExceptionCode.INTERNAL, "Injected exception from mock");
                // following method should not be invoked at all
                realScheduler.scheduleAddToWorker(defaultServiceId, anyLong, anyLong);
                minTimes = 0;
                maxTimes = 0;
                result = new StarException(ExceptionCode.INTERNAL, "Injected exception from mock");
            }
        };
        // trigger a shard balance check
        checker.runServiceBalanceCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
        {
            // Overall validation
            Map<Long, Integer> statsAfter = helper.collectShardReplicaDistributionStats(allShardIds, workerGroupId);
            // 3 packGroups on three workers, with packGroup distribution: (1, 1, 1), with replica distribution: (2, 2, 2)
            Assert.assertEquals(3, statsAfter.size());
            Assert.assertEquals(Arrays.asList(2, 2, 2), new ArrayList<>(statsAfter.values()));
            // two spread shard groups has identical replica distribution
            Map<Long, Integer> statsGroup1 = helper.collectShardReplicaDistributionStats(shardIds1, workerGroupId);
            Map<Long, Integer> statsGroup2 = helper.collectShardReplicaDistributionStats(shardIds2, workerGroupId);
            Assert.assertEquals(statsGroup1, statsGroup2);
        }
    }

    @Test
    public void testMetaGroupBalanceCase2() {
        // Partitions: 3, Bucket: 5
        // 1. create 3 SPREAD shard group with each have 5 shards x 1 replica
        // 2. create a meta group, bundle 3 SPREAD shard groups together into a meta group.
        // 3. start with 3 workers, trigger one round of balance and check the distribution.
        // 4. scale out to 5 workers, trigger one round of balance and check the distribution.
        long spreadGroup1 = helper.createTestShardGroup(PlacementPolicy.SPREAD);
        long spreadGroup2 = helper.createTestShardGroup(PlacementPolicy.SPREAD);
        long spreadGroup3 = helper.createTestShardGroup(PlacementPolicy.SPREAD);
        List<Long> shardIds1 = helper.createTestShards(5, spreadGroup1);
        List<Long> shardIds2 = helper.createTestShards(5, spreadGroup2);
        List<Long> shardIds3 = helper.createTestShards(5, spreadGroup3);
        // create a metaGroup
        long metaGroupId = helper.createMetaGroup(PlacementPolicy.PACK, Arrays.asList(spreadGroup1, spreadGroup2, spreadGroup3));

        long workerGroupId = helper.createWorkerGroup();
        helper.createTestWorkersToWorkerGroup(3, workerGroupId);

        List<Long> allShardIds = new ArrayList<>(shardIds1);
        allShardIds.addAll(shardIds2);
        allShardIds.addAll(shardIds3);

        // trigger the schedule.
        realScheduler.scheduleAddToGroup(defaultServiceId, allShardIds, workerGroupId);

        MockShardChecker checker = new MockShardChecker(helper.getServiceManager(), helper.getWorkerManager(), realScheduler);
        // trigger a shard balance check
        checker.runServiceBalanceCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
        { // check distribution
            Map<Long, Integer> stats = helper.collectShardReplicaDistributionStats(allShardIds, workerGroupId);
            // 5 packGroup on 3 workers, with packGroup distribution: (1,2,2), with replica distribution: (3,6,6)
            Assert.assertEquals(3, stats.size());
            Assert.assertEquals(Arrays.asList(3, 6, 6),
                    new ArrayList<>(stats.values().stream().sorted().collect(Collectors.toList())));
            // 3 spread shard groups has identical replica distribution, due to pack meta group
            Map<Long, Integer> statsGroup1 = helper.collectShardReplicaDistributionStats(shardIds1, workerGroupId);
            Map<Long, Integer> statsGroup2 = helper.collectShardReplicaDistributionStats(shardIds2, workerGroupId);
            Map<Long, Integer> statsGroup3 = helper.collectShardReplicaDistributionStats(shardIds3, workerGroupId);
            Assert.assertEquals(statsGroup1, statsGroup2);
            Assert.assertEquals(statsGroup1, statsGroup3);
        }

        // Add two additional workers into the worker group
        helper.createTestWorkersToWorkerGroup(2, workerGroupId);
        // trigger a shard balance check
        checker.runServiceBalanceCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
        { // check distribution
            Map<Long, Integer> stats = helper.collectShardReplicaDistributionStats(allShardIds, workerGroupId);
            // 5 packGroup on 5 workers, with packGroup distribution: (1,1,1,1,1), with replica distribution: (3,3,3,3,3)
            Assert.assertEquals(5, stats.size());
            Assert.assertEquals(Arrays.asList(3, 3, 3, 3, 3), new ArrayList<>(stats.values()));
            // 3 spread shard groups has identical replica distribution, due to pack meta group
            Map<Long, Integer> statsGroup1 = helper.collectShardReplicaDistributionStats(shardIds1, workerGroupId);
            Map<Long, Integer> statsGroup2 = helper.collectShardReplicaDistributionStats(shardIds2, workerGroupId);
            Map<Long, Integer> statsGroup3 = helper.collectShardReplicaDistributionStats(shardIds3, workerGroupId);
            Assert.assertEquals(statsGroup1, statsGroup2);
            Assert.assertEquals(statsGroup1, statsGroup3);
        }
    }

    @Test
    public void testSpreadShardGroupBalanceRandomSelectTablets() {
        HijackConfig maxSkew = new HijackConfig("SCHEDULER_BALANCE_MAX_SKEW", "1");
        // entry level case / sunny case
        long shardGroupId = helper.createTestShardGroup(PlacementPolicy.SPREAD);
        long workerGroupId = helper.createWorkerGroup();
        // disable the warmup feature to make the test result easier to verify
        helper.setWorkerGroupWarmupLevel(workerGroupId, WarmupLevel.WARMUP_NOTHING);

        // initial cluster size: 3
        List<Long> workerIds = helper.createTestWorkersToWorkerGroup(3, workerGroupId);
        // create 36 shards with 1 replica
        List<Long> shardIds = new ArrayList<>();
        for (int i = 0; i < 36 / 3; ++i) {
            // simulate incremental expansion of the shard group
            List<Long> newShardIds = helper.createTestShards(3, shardGroupId);
            realScheduler.scheduleAddToGroup(defaultServiceId, newShardIds, workerGroupId);
            shardIds.addAll(newShardIds);
        }
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);

        Map<Long, Integer> stats = helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);
        Assert.assertEquals(workerIds.size(), stats.size());
        // each worker has 12 shards (replicas)
        List<Integer> distribution = Arrays.asList(12, 12, 12);
        Assert.assertEquals(distribution, new ArrayList<>(stats.values()));

        // expand the workergroup from 3 to 6
        LOG.info("Shard ids: {}", shardIds);
        workerIds.addAll(helper.createTestWorkersToWorkerGroup(3, workerGroupId));
        MockShardChecker checker = new MockShardChecker(helper.getServiceManager(), helper.getWorkerManager(), realScheduler);
        checker.runServiceBalanceCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);

        stats = helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);
        Assert.assertEquals(workerIds.size(), stats.size());
        // each worker has 6 shards (replicas)
        distribution = Arrays.asList(6, 6, 6, 6, 6, 6);
        Assert.assertEquals(distribution, new ArrayList<>(stats.values()));

        // need manual validating the balance, how's the tablet moved to the new workers.
        maxSkew.reset();
    }

    @Test
    public void testScaleOutReplicaChangeToOkDueToConfigChange() {
        long shardGroup = helper.createTestShardGroup(PlacementPolicy.SPREAD);
        long shardId = helper.createTestShard(shardGroup);
        Shard shard = helper.getDefaultShardManager().getShard(shardId);
        long workerGroupId = helper.createWorkerGroup(3);
        helper.setWorkerGroupWarmupLevel(workerGroupId, WarmupLevel.WARMUP_ALL);
        helper.createTestWorkersToWorkerGroup(5, workerGroupId);
        realScheduler.scheduleAddToGroup(defaultServiceId, shardId, workerGroupId);

        // all the 3 replicas are in SCALE_OUT state
        Assert.assertEquals(3L, shard.getReplicaSize());
        Assert.assertEquals(3L,
                shard.getReplica().stream().filter(x -> x.getState() == ReplicaState.REPLICA_SCALE_OUT).count());

        // turn off the warmup feature
        helper.setWorkerGroupWarmupLevel(workerGroupId, WarmupLevel.WARMUP_NOTHING);
        MockShardChecker checker = new MockShardChecker(helper.getServiceManager(), helper.getWorkerManager(), realScheduler);
        checker.runServiceHealthCheck(defaultServiceId);

        // all the 3 replicas are set to REPLICA_OK
        Assert.assertEquals(3L, shard.getReplicaSize());
        Assert.assertEquals(3L,
                shard.getReplica().stream().filter(x -> x.getState() == ReplicaState.REPLICA_OK).count());
    }

    @Test
    public void testScaleOutReplicaChangeToOkDueToTimeout() {
        HijackConfig stateTimeOut = new HijackConfig("SHARD_REPLICA_SCALE_OUT_TIMEOUT_SECS", "300");

        long shardGroup = helper.createTestShardGroup(PlacementPolicy.SPREAD);
        long shardId = helper.createTestShard(shardGroup);
        Shard shard = helper.getDefaultShardManager().getShard(shardId);
        long workerGroupId = helper.createWorkerGroup(3);
        helper.setWorkerGroupWarmupLevel(workerGroupId, WarmupLevel.WARMUP_META);
        helper.createTestWorkersToWorkerGroup(5, workerGroupId);
        realScheduler.scheduleAddToGroup(defaultServiceId, shardId, workerGroupId);

        // all the 3 replicas are in SCALE_OUT state
        Assert.assertEquals(3L, shard.getReplicaSize());
        Assert.assertEquals(3L,
                shard.getReplica().stream().filter(x -> x.getState() == ReplicaState.REPLICA_SCALE_OUT).count());
        MockShardChecker checker = new MockShardChecker(helper.getServiceManager(), helper.getWorkerManager(), realScheduler);
        long replicaTs = System.currentTimeMillis();

        checker.runServiceHealthCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);

        // Nothing changed
        Assert.assertEquals(3L, shard.getReplicaSize());
        Assert.assertEquals(3L,
                shard.getReplica().stream().filter(x -> x.getState() == ReplicaState.REPLICA_SCALE_OUT).count());

        stateTimeOut.updateValue("0");
        // ensure the replica state is expired
        Awaitility.await().atMost(200L, TimeUnit.MILLISECONDS).until(() -> System.currentTimeMillis() > replicaTs);
        checker.runServiceHealthCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);

        // All changed to REPLICA_OK
        Assert.assertEquals(3L, shard.getReplicaSize());
        Assert.assertEquals(3L,
                shard.getReplica().stream().filter(x -> x.getState() == ReplicaState.REPLICA_OK).count());

        stateTimeOut.reset();
    }

    @Test
    public void testScaleOutBalanceStable() {
        // 1. create a shardgroup with 336 shards
        // 2. create a workergroup with 3 nodes
        // 3. distribute the shardgroup to the workergroup (112 shards per node)
        // 4. set all the replica to OK status
        // 5. scale out the workergroup to 4 nodes (84 shards per node)
        // 6. balance the shardgroup inside the workergroup (expect to create 84 REPLICA_SCALE_OUT and 84 REPLICA_SCALE_IN)
        // 7. set all REPLICA_SCALE_OUT to REPLICA_OK
        // 8. redo the balance, SHOULD BE NO CHANGE
        HijackConfig stateTimeOut = new HijackConfig("SHARD_REPLICA_SCALE_OUT_TIMEOUT_SECS", "300");
        int nShards = 336;
        long shardGroup = helper.createTestShardGroup(PlacementPolicy.SPREAD);
        List<Long> shardIds = helper.createTestShards(nShards, shardGroup);
        long workerGroupId = helper.createWorkerGroup(1);
        helper.setWorkerGroupWarmupLevel(workerGroupId, WarmupLevel.WARMUP_META);
        helper.createTestWorkersToWorkerGroup(3, workerGroupId);
        realScheduler.scheduleAddToGroup(defaultServiceId, shardIds, workerGroupId);

        {
            Map<Long, Integer> stats = helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);
            Assert.assertEquals(3, stats.size());
            // [112,112,112] distribution
            List<Integer> distribution = Arrays.asList(112, 112, 112);
            Assert.assertEquals(distribution, new ArrayList<>(stats.values()));
            long totalCount = 0;
            for (long id : shardIds) {
                totalCount += helper.getDefaultShardManager().getShard(id).getReplica().stream()
                                .filter(x -> x.getState() == ReplicaState.REPLICA_SCALE_OUT).count();
            }
            Assert.assertEquals(nShards, totalCount);
            for (long id : shardIds) {
                helper.getDefaultShardManager().getShard(id).getReplica()
                        .forEach(x -> x.setReplicaState(ReplicaState.REPLICA_OK));
            }
        }

        // add one more worker into the workerGroup
        helper.createTestWorkerToWorkerGroup(workerGroupId);

        MockShardChecker checker = new MockShardChecker(helper.getServiceManager(), helper.getWorkerManager(), realScheduler);
        checker.runServiceBalanceCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);

        {
            Map<Long, Integer> stats = helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);
            Assert.assertEquals(4, stats.size());
            // [112,112,112, 84] distribution
            List<Integer> distribution = Arrays.asList(112, 112, 112, 84);
            Assert.assertEquals(distribution, new ArrayList<>(stats.values()));
            long totalScaleOutCount = 0;
            long totalScaleInCount = 0;
            for (long id : shardIds) {
                totalScaleOutCount += helper.getDefaultShardManager().getShard(id).getReplica().stream()
                        .filter(x -> x.getState() == ReplicaState.REPLICA_SCALE_OUT).count();
                totalScaleInCount += helper.getDefaultShardManager().getShard(id).getReplica().stream()
                        .filter(x -> x.getState() == ReplicaState.REPLICA_SCALE_IN).count();
            }
            Assert.assertEquals(nShards / 4, totalScaleOutCount);
            Assert.assertEquals(nShards / 4, totalScaleInCount);

            // reset all REPLICA_SCALE_OUT replica to REPLICA_OK
            for (long id : shardIds) {
                Shard shard = helper.getDefaultShardManager().getShard(id);
                shard.getReplica().forEach(x -> {
                    if (x.getState() == ReplicaState.REPLICA_SCALE_OUT) {
                        shard.scaleOutReplicaDone(x.getWorkerId());
                    }
                });
            }
        }

        checker.runServiceBalanceCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);

        {
            // expect no change
            Map<Long, Integer> stats = helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);
            Assert.assertEquals(4, stats.size());
            // [112,112,112, 84] distribution
            List<Integer> distribution = Arrays.asList(112, 112, 112, 84);
            Assert.assertEquals(distribution, new ArrayList<>(stats.values()));

            long totalScaleOutCount = 0;
            long totalScaleInCount = 0;
            for (long id : shardIds) {
                totalScaleOutCount += helper.getDefaultShardManager().getShard(id).getReplica().stream()
                        .filter(x -> x.getState() == ReplicaState.REPLICA_SCALE_OUT).count();
                totalScaleInCount += helper.getDefaultShardManager().getShard(id).getReplica().stream()
                        .filter(x -> x.getState() == ReplicaState.REPLICA_SCALE_IN).count();
            }
            Assert.assertEquals(0, totalScaleOutCount);
            Assert.assertEquals(nShards / 4, totalScaleInCount);
        }

        stateTimeOut.reset();
    }

    @Test
    public void testWorkerRestartTabletDistributionTheSame() {
        // 5 * 4 * 4 = 80 shards
        testShardRedistributionInWorkerRestart(5, 4, false);
        // 6 * 5 * 3 = 90 shards
        testShardRedistributionInWorkerRestart(6, 3, false);
    }

    @Test
    public void testWorkerRestartTabletDistributionMayDifferent() {
        // 5 * 4 * 4 = 80 shards
        testShardRedistributionInWorkerRestart(5, 4, true);
        // 6 * 5 * 3 = 90 shards
        testShardRedistributionInWorkerRestart(6, 3, true);
    }

    // nWorker = 4, factor = 3, shards = 4 * (4-1) * 3 = 36
    // nWorker = 5, factor = 4, shards = 5 * (5-1) * 4 = 80
    public void testShardRedistributionInWorkerRestart(int nWorkers, int factor, boolean expireReplica) {
        int nShards = nWorkers * (nWorkers - 1) * factor;
        long shardGroup = helper.createTestShardGroup(PlacementPolicy.SPREAD);
        List<Long> shardIds = helper.createTestShards(nShards, shardGroup);
        long workerGroupId = helper.createWorkerGroup(1);
        Worker worker0;
        helper.createTestWorkersToWorkerGroup(nWorkers, workerGroupId);
        realScheduler.scheduleAddToGroup(defaultServiceId, shardIds, workerGroupId);
        {
            Map<Long, Integer> stats = helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);
            Assert.assertEquals(nWorkers, stats.size());
            List<Integer> distribution = Collections.nCopies(nWorkers, nShards / nWorkers);
            Assert.assertEquals(distribution, new ArrayList<>(stats.values()));
            // get the first worker
            worker0 = helper.getWorkerManager().getWorker(stats.keySet().iterator().next());
        }
        Map<Long, Long> replicaDistribution = new HashMap<>();
        for (long shardId : shardIds) {
            helper.getDefaultShardManager().getShard(shardId).getReplica()
                    .forEach(x -> replicaDistribution.put(shardId, x.getWorkerId()));
        }

        // set the first worker DOWN
        worker0.setState(WorkerState.DOWN);

        MockShardChecker checker =
                new MockShardChecker(helper.getServiceManager(), helper.getWorkerManager(), realScheduler);
        // check healthy and then check balance
        checker.runServiceHealthCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
        checker.runServiceBalanceCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
        {
            Map<Long, Integer> stats = helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);
            Assert.assertEquals(nWorkers, stats.size());
            // the first one is keep the same, remain workers serving all the shards
            List<Integer> distribution = new ArrayList<>();
            // distribution should be [nShards/nWorkers, nShards/(nWorkers-1), ..., nShards/(nWorkers-1)]
            distribution.add(nShards / nWorkers);
            distribution.addAll(Collections.nCopies(nWorkers - 1, nShards / (nWorkers - 1)));
            Assert.assertEquals(distribution, new ArrayList<>(stats.values()));
        }

        if (expireReplica) {
            worker0.setLastDownTime(System.currentTimeMillis() - Config.SHARD_DEAD_REPLICA_EXPIRE_SECS * 1000L - 1000);
        }
        // check healthy and then check balance
        checker.runServiceHealthCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
        checker.runServiceBalanceCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
        {
            Map<Long, Integer> stats = helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);
            List<Integer> distribution = new ArrayList<>();
            if (!expireReplica) {
                // dead worker still counts
                distribution.add(nShards / nWorkers);
            }
            distribution.addAll(Collections.nCopies(nWorkers - 1, nShards / (nWorkers - 1)));
            Assert.assertEquals(distribution, new ArrayList<>(stats.values()));
        }

        // get the worker back online, balance will move tablets around
        worker0.setState(WorkerState.ON);

        // check healthy and then check balance
        checker.runServiceHealthCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
        checker.runServiceBalanceCheck(defaultServiceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(realScheduler::isIdle);
        {
            Map<Long, Integer> stats = helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);
            // No matter the dead replica is expired or not, after the worker is back online.
            // the distribution should be back to even
            Assert.assertEquals(nWorkers, stats.size());
            List<Integer> distribution = Collections.nCopies(nWorkers, nShards / nWorkers);
            Assert.assertEquals(distribution, new ArrayList<>(stats.values()));
        }

        Map<Long, Long> replicaDistributionAfterRestart = new HashMap<>();
        for (long shardId : shardIds) {
            helper.getDefaultShardManager().getShard(shardId).getReplica()
                    .forEach(x -> replicaDistributionAfterRestart.put(shardId, x.getWorkerId()));
        }
        if (expireReplica) {
            Assert.assertNotEquals(replicaDistribution, replicaDistributionAfterRestart);
        } else {
            // dead replica is not expired, the final result should be exactly the same as before restart
            Assert.assertEquals(replicaDistribution, replicaDistributionAfterRestart);
        }
    }

    @Test
    public void testWorkerGroup0ReplicaAlwaysChecked() {
        testWorkerGroup0ReplicaHealthCheckBody(true);
        testWorkerGroup0ReplicaHealthCheckBody(false);
    }

    void testWorkerGroup0ReplicaHealthCheckBody(boolean enableZeroGroupCompatibility) {
        HijackConfig hijackConfig = new HijackConfig("ENABLE_ZERO_WORKER_GROUP_COMPATIBILITY",
                String.valueOf(enableZeroGroupCompatibility));

        // generate a new tmpHelper with fresh new WorkerManager that will take
        // the new value of the ConfigVar into consideration.
        String name = String.format("testWorkerGroup0ReplicaHealthCheckBody-%d", ThreadLocalRandom.current().nextInt());
        TestHelper tmpHelper = new TestHelper(name);
        tmpHelper.createDefaultServiceAndWorkerGroup(name);
        String serviceId = tmpHelper.getDefaultServiceId();

        ShardSchedulerV2 scheduler = new ShardSchedulerV2(tmpHelper.getServiceManager(), tmpHelper.getWorkerManager());
        scheduler.start();

        long shardGroup = tmpHelper.createTestShardGroup(PlacementPolicy.SPREAD);
        long shardId = tmpHelper.createTestShard(shardGroup);
        Shard shard = tmpHelper.getDefaultShardManager().getShard(shardId);

        long builtinWorkerGroupId = Constant.DEFAULT_ID;
        List<Long> defaultWorkerIds;
        if (enableZeroGroupCompatibility) {
            defaultWorkerIds = tmpHelper.createTestWorkersToWorkerGroup(2, builtinWorkerGroupId);
        } else {
            defaultWorkerIds = new ArrayList<>();
            // there is no workerGroup with groupId = 0 at all
            Assert.assertNull(tmpHelper.getWorkerManager().getWorkerGroupNoException(serviceId, builtinWorkerGroupId));
        }

        long nonDefaultWorkerGroupId = tmpHelper.createWorkerGroup();
        List<Long> nonDefaultWorkerIds = tmpHelper.createTestWorkersToWorkerGroup(2, nonDefaultWorkerGroupId);

        Assert.assertEquals(0, shard.getReplicaSize());

        MockShardChecker checker =
                new MockShardChecker(tmpHelper.getServiceManager(), tmpHelper.getWorkerManager(), scheduler);
        // check healthy and then check balance
        checker.runServiceHealthCheck(serviceId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(scheduler::isIdle);

        {
            // no replica in nonDefaultWorkerGroupId regardless of ENABLE_ZERO_WORKER_GROUP_COMPATIBILITY
            long nReplicas =
                    shard.getReplica().stream().filter(x -> nonDefaultWorkerIds.contains(x.getWorkerId())).count();
            Assert.assertEquals(0, nReplicas);
        }
        {
            // Depends on the ConfigVar value, the new replica will be created
            long nReplicas =
                    shard.getReplica().stream().filter(x -> defaultWorkerIds.contains(x.getWorkerId())).count();
            if (enableZeroGroupCompatibility) {
                Assert.assertEquals(1, nReplicas);
            } else {
                Assert.assertEquals(0, nReplicas);
            }
        }

        scheduler.stop();
        tmpHelper.stop();
        hijackConfig.reset();
    }

    @Test
    public void shardCheckInterruptedInTheMiddle() {
        long shardGroup = helper.createTestShardGroup(PlacementPolicy.SPREAD);
        long shardId = helper.createTestShard(shardGroup);
        long workerGroupId = helper.createWorkerGroup(3);
        helper.setWorkerGroupWarmupLevel(workerGroupId, WarmupLevel.WARMUP_ALL);
        helper.createTestWorkersToWorkerGroup(5, workerGroupId);
        realScheduler.scheduleAddToGroup(defaultServiceId, shardId, workerGroupId);

        HijackConfig hijackConfig = new HijackConfig("DISABLE_BACKGROUND_SHARD_SCHEDULE_CHECK", "true");
        MockShardChecker checker =
                new MockShardChecker(helper.getServiceManager(), helper.getWorkerManager(), realScheduler);

        Assert.assertThrows(CheckInterruptedException.class, () -> checker.runServiceHealthCheck(defaultServiceId));
        Assert.assertThrows(CheckInterruptedException.class, () -> checker.runServiceBalanceCheck(defaultServiceId));
        hijackConfig.reset();
    }
}
