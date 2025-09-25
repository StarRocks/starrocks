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

package com.staros.schedule;

import com.google.common.collect.Lists;
import com.staros.common.HijackConfig;
import com.staros.common.TestHelper;
import com.staros.exception.ExceptionCode;
import com.staros.exception.NoAliveWorkersException;
import com.staros.exception.StarException;
import com.staros.exception.WorkerNotHealthyStarException;
import com.staros.proto.AddShardInfo;
import com.staros.proto.PlacementPolicy;
import com.staros.proto.ReplicaInfoLite;
import com.staros.proto.ReplicaState;
import com.staros.proto.ReplicationType;
import com.staros.proto.WarmupLevel;
import com.staros.proto.WorkerState;
import com.staros.replica.Replica;
import com.staros.service.ServiceManager;
import com.staros.shard.Shard;
import com.staros.shard.ShardManager;
import com.staros.starlet.MockStarletAgent;
import com.staros.starlet.StarletAgent;
import com.staros.starlet.StarletAgentFactory;
import com.staros.util.Config;
import com.staros.worker.Worker;
import com.staros.worker.WorkerGroup;
import com.staros.worker.WorkerManager;
import mockit.Expectations;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * StarManager replace builtin scheduler, only manage worker/shard/shardGroup/workerGroup
 */
public class SchedulerV2Test {

    private WorkerManager workerManager;
    private String serviceId;

    private ShardSchedulerV2 scheduler;
    private HijackConfig hijackTriggerScheduling;
    private TestHelper helper;

    @BeforeClass
    public static void setupForClass() {
        // Turn On MockStarletAgent
        StarletAgentFactory.AGENT_TYPE = StarletAgentFactory.AgentType.MOCK_STARLET_AGENT;
    }

    @Before
    public void setup() {
        Config.S3_BUCKET = "test-bucket";
        // disable auto schedule
        hijackTriggerScheduling = new HijackConfig("SCHEDULER_TRIGGER_SCHEDULE_WHEN_CREATE_SHARD", "false");
        helper = new TestHelper(this.getClass().getName() + "-serviceTemplateName-0");
        workerManager = helper.getWorkerManager();

        // Setup SchedulerV2
        ServiceManager serviceManager = helper.getServiceManager();
        scheduler = new ShardSchedulerV2(serviceManager, workerManager);
        serviceManager.setShardScheduler(scheduler);
        scheduler.start();

        helper.createDefaultServiceAndWorkerGroup(this.getClass().getName() + "-serviceName-0");
        serviceId = helper.getDefaultServiceId();
    }

    @After
    public void tearDown() {
        scheduler.stop();
        hijackTriggerScheduling.reset();
        Config.S3_BUCKET = "";
    }

    @Test
    public void testSingleShardScheduleAddToGroupNotExist() {
        long shardId = helper.createTestShard(0);
        long workerGroupId = helper.getDefaultWorkerGroupId();

        { // serviceId not exist
            try {
                scheduler.scheduleAddToGroup("not exist service id", shardId, workerGroupId);
                Assert.fail("Scheduler requests should fail");
            } catch (StarException e) {
                Assert.assertEquals(e.getExceptionCode(), ExceptionCode.NOT_EXIST);
            }
        }
        { // shard id not exist
            try {
                scheduler.scheduleAddToGroup(serviceId, -1, workerGroupId);
                Assert.fail("Scheduler requests should fail");
            } catch (StarException e) {
                Assert.assertEquals(e.getExceptionCode(), ExceptionCode.NOT_EXIST);
            }
        }
        { // workerGroup id not exist
            try {
                scheduler.scheduleAddToGroup(serviceId, shardId, -1);
                Assert.fail("Scheduler requests should fail");
            } catch (StarException e) {
                Assert.assertEquals(e.getExceptionCode(), ExceptionCode.NOT_EXIST);
            }
        }
    }

    @Test
    public void testScheduleNotRunning() {
        scheduler.stop();
        {
            try {
                scheduler.scheduleAddToGroup(serviceId, 0, 0);
                Assert.fail("Scheduler requests should fail");
            } catch (StarException e) {
                Assert.assertEquals(e.getExceptionCode(), ExceptionCode.SCHEDULE);
            }
        }
        {
            try {
                scheduler.scheduleAsyncAddToGroup(serviceId, 0, 0);
                Assert.fail("Scheduler requests should fail");
            } catch (StarException e) {
                Assert.assertEquals(e.getExceptionCode(), ExceptionCode.SCHEDULE);
            }
        }
        {
            try {
                List<Long> shards = Arrays.asList(0L, 1L);
                scheduler.scheduleAddToGroup(serviceId, shards, 0);
                Assert.fail("Scheduler requests should fail");
            } catch (StarException e) {
                Assert.assertEquals(e.getExceptionCode(), ExceptionCode.SCHEDULE);
            }
        }
    }

    @Test
    public void testScheduleSingleShardMultipleReplicas1() {
        scheduleSingleShardMultipleReplicasInternal(WarmupLevel.WARMUP_ALL);
    }

    @Test
    public void testScheduleSingleShardMultipleReplicas2() {
        scheduleSingleShardMultipleReplicasInternal(WarmupLevel.WARMUP_NOTHING);
    }

    void scheduleSingleShardMultipleReplicasInternal(WarmupLevel warmupLevel) {
        helper.setDefaultWorkerGroupWarmupLevel(warmupLevel);
        List<Long> workerIds = new ArrayList<>();
        long workerGroupId = helper.getDefaultWorkerGroupId();
        boolean enabledWarmup = workerManager.getWorkerGroup(serviceId, workerGroupId).warmupEnabled();
        { // shard: 1 replica, workerGroup: 0 worker
            long emptyWorkerGroupId = helper.createWorkerGroup();
            // empty worker group
            Assert.assertEquals(0, workerManager.getWorkerGroup(serviceId, emptyWorkerGroupId).getWorkerCount());
            long shardId = helper.createTestShard(0);
            try {
                // no available worker in given workerGroup
                scheduler.scheduleAddToGroup(serviceId, shardId, emptyWorkerGroupId);
                Assert.fail("Expect throw exception before reach here");
            } catch (StarException exception) {
                Assert.assertEquals(exception.getExceptionCode(), ExceptionCode.SCHEDULE);
            }
        }
        { // shard: 1 replica, workerGroup: 1 worker
            long shardId = helper.createTestShard(0);

            workerIds.add(helper.createTestWorkerToWorkerGroup(workerGroupId));
            Assert.assertEquals(1, workerIds.size());

            List<Long> workerIdsBefore = helper.getWorkerIdsOfShardReplicaFromWorkerGroup(shardId, workerGroupId);
            Assert.assertTrue(workerIdsBefore.isEmpty());

            scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);

            List<Long> workerIdsAfter = helper.getWorkerIdsOfShardReplicaFromWorkerGroup(shardId, workerGroupId);
            Assert.assertEquals(1, workerIdsAfter.size());
            Assert.assertEquals(workerIds.get(0), workerIdsAfter.get(0));

            for (Replica replica : helper.getDefaultShardManager().getShard(shardId).getReplica()) {
                if (enabledWarmup) {
                    Assert.assertEquals(ReplicaState.REPLICA_SCALE_OUT, replica.getState());
                } else {
                    Assert.assertEquals(ReplicaState.REPLICA_OK, replica.getState());
                }
            }
        }
        { // shard: 3 replicas
            helper.setDefaultWorkerGroupReplicaNumber(3);
            long shardId = helper.createTestShard(0);

            List<Long> ids1 = helper.getWorkerIdsOfShardReplicaFromWorkerGroup(shardId, workerGroupId);
            Assert.assertTrue(ids1.isEmpty());

            // workerGroup: 1 worker
            scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);
            List<Long> ids2 = helper.getWorkerIdsOfShardReplicaFromWorkerGroup(shardId, workerGroupId);
            Assert.assertEquals(1, ids2.size());
            Assert.assertEquals(ids2.get(0), workerIds.get(0));

            // workerGroup: 2 workers
            workerIds.add(helper.createTestWorkerToWorkerGroup(workerGroupId));
            Assert.assertEquals(2, workerIds.size());

            // schedule again, the second replica will be scheduled
            scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);
            List<Long> ids3 = helper.getWorkerIdsOfShardReplicaFromWorkerGroup(shardId, workerGroupId);
            Assert.assertEquals(2, ids3.size());
            ids3.forEach(x -> Assert.assertTrue(workerIds.contains(x)));

            // workerGroup: 3 workers
            workerIds.add(helper.createTestWorkerToWorkerGroup(workerGroupId));
            Assert.assertEquals(3, workerIds.size());

            // schedule again, the third replica will be scheduled
            scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);
            List<Long> ids4 = helper.getWorkerIdsOfShardReplicaFromWorkerGroup(shardId, workerGroupId);
            Assert.assertEquals(3, ids4.size());
            ids4.forEach(x -> Assert.assertTrue(workerIds.contains(x)));

            // default state for new replicas are all REPLICA_SCALE_OUT
            for (Replica replica : helper.getDefaultShardManager().getShard(shardId).getReplica()) {
                if (enabledWarmup) {
                    Assert.assertEquals(ReplicaState.REPLICA_SCALE_OUT, replica.getState());
                } else {
                    Assert.assertEquals(ReplicaState.REPLICA_OK, replica.getState());
                }
            }

            // workerGroup: 4 workers
            workerIds.add(helper.createTestWorkerToWorkerGroup(workerGroupId));
            Assert.assertEquals(4, workerIds.size());

            // schedule again, nothing changed
            scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);
            List<Long> ids5 = helper.getWorkerIdsOfShardReplicaFromWorkerGroup(shardId, workerGroupId);
            Assert.assertEquals(ids5, ids4);
        }
    }

    @Test
    public void scheduleSingleReplicaStateChangeInternal() {
        // create a test shard
        long shardId = helper.createTestShard(0);
        Shard shard = helper.getDefaultShardManager().getShard(shardId);

        // create a worker group that expects 3 replicas
        long workerGroupId = helper.createWorkerGroup(3);
        helper.setWorkerGroupWarmupLevel(workerGroupId, WarmupLevel.WARMUP_META);
        List<Long> workerIds = new ArrayList<>();

        { // shard: 3 replica, workerGroup: 0 worker
            Assert.assertEquals(0, workerManager.getWorkerGroup(serviceId, workerGroupId).getWorkerCount());
            StarException exception = Assert.assertThrows(StarException.class,
                    () -> scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId));
            Assert.assertEquals(exception.getExceptionCode(), ExceptionCode.SCHEDULE);
        }
        { // shard: 3 replica, workerGroup: 1 worker
            workerIds.add(helper.createTestWorkerToWorkerGroup(workerGroupId));
            Assert.assertEquals(1, workerIds.size());

            List<Long> workerIdsBefore = helper.getWorkerIdsOfShardReplicaFromWorkerGroup(shardId, workerGroupId);
            Assert.assertTrue(workerIdsBefore.isEmpty());

            scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);

            List<Long> workerIdsAfter = helper.getWorkerIdsOfShardReplicaFromWorkerGroup(shardId, workerGroupId);
            Assert.assertEquals(1, workerIdsAfter.size());
            Assert.assertEquals(workerIds.get(0), workerIdsAfter.get(0));

            // default state for new replicas are all REPLICA_SCALE_OUT
            for (Replica replica : helper.getDefaultShardManager().getShard(shardId).getReplica()) {
                Assert.assertEquals(ReplicaState.REPLICA_SCALE_OUT, replica.getState());
            }
        }
        { // shard: 3 replicas, workerGroup: 3 workers
            // workerGroup: add 2 more workers
            workerIds.addAll(helper.createTestWorkersToWorkerGroup(2, workerGroupId));
            Assert.assertEquals(3, workerIds.size());

            // schedule again, the other 2 replicas will be scheduled
            scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);
            List<Long> ids3 = helper.getWorkerIdsOfShardReplicaFromWorkerGroup(shardId, workerGroupId);
            Assert.assertEquals(3, ids3.size());
            ids3.forEach(x -> Assert.assertTrue(workerIds.contains(x)));

            for (Replica replica : helper.getDefaultShardManager().getShard(shardId).getReplica()) {
                Assert.assertEquals(ReplicaState.REPLICA_SCALE_OUT, replica.getState());
            }
        }
        { // shard: 3 replicas (2x SCALE_OUT + 1x SCALE_IN), workerGroup: 3 workers, can't add a new SCALE_OUT
            shard.getReplica().get(0).setReplicaState(ReplicaState.REPLICA_SCALE_IN);
            long firstReplicaWorkerId = shard.getReplica().get(0).getWorkerId();
            scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);

            // SCALE_IN replica reset to REPLICA_OK status
            List<Long> ids3 = helper.getWorkerIdsOfShardReplicaFromWorkerGroup(shardId, workerGroupId);
            Assert.assertEquals(3, ids3.size());
            ids3.forEach(x -> Assert.assertTrue(workerIds.contains(x)));
            for (Replica replica : helper.getDefaultShardManager().getShard(shardId).getReplica()) {
                if (replica.getWorkerId() == firstReplicaWorkerId) {
                    Assert.assertEquals(ReplicaState.REPLICA_OK, replica.getState());
                } else {
                    Assert.assertEquals(ReplicaState.REPLICA_SCALE_OUT, replica.getState());
                }
            }
        }
        { // shard: 3 replicas (2x SCALE_OUT + 1x SCALE_IN), workerGroup: 4 workers, a new SCALE_OUT replica will be created
            workerIds.add(helper.createTestWorkerToWorkerGroup(workerGroupId));
            Assert.assertEquals(4, workerIds.size());
            shard.getReplica().get(0).setReplicaState(ReplicaState.REPLICA_SCALE_IN);

            // a new replica is created as SCALE_OUT, it will be 3x SCALE_OUT, 1x SCALE_IN
            scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);
            List<Long> ids4 = helper.getWorkerIdsOfShardReplicaFromWorkerGroup(shardId, workerGroupId);
            Assert.assertEquals(4, ids4.size());
            ids4.forEach(x -> Assert.assertTrue(workerIds.contains(x)));
            for (Replica replica : helper.getDefaultShardManager().getShard(shardId).getReplica()) {
                if (replica.getWorkerId() == workerIds.get(0)) {
                    Assert.assertEquals(ReplicaState.REPLICA_SCALE_IN, replica.getState());
                } else {
                    Assert.assertEquals(ReplicaState.REPLICA_SCALE_OUT, replica.getState());
                }
            }

            // add two more workers
            workerIds.addAll(helper.createTestWorkersToWorkerGroup(2, workerGroupId));
            Assert.assertEquals(6, workerIds.size());

            // schedule again, no change
            scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);
            List<Long> ids5 = helper.getWorkerIdsOfShardReplicaFromWorkerGroup(shardId, workerGroupId);
            Assert.assertEquals(4, ids5.size());
            // should be the same distribution as ids4
            Assert.assertEquals(ids4, ids5);
        }
    }

    @Test
    public void testSingleShardScheduleIgnoreDeadWorkers() {
        // works the same way no matter the config var on or off
        singleShardScheduleIgnoreDeadWorkersInternal(true);
        singleShardScheduleIgnoreDeadWorkersInternal(false);
    }

    void singleShardScheduleIgnoreDeadWorkersInternal(boolean enableWarmup) {
        helper.setDefaultWorkerGroupReplicaNumber(3);
        long shardId = helper.createTestShard(0);
        long workerGroupId = helper.getDefaultWorkerGroupId();
        helper.setDefaultWorkerGroupWarmupLevel(enableWarmup ? WarmupLevel.WARMUP_INDEX : WarmupLevel.WARMUP_NOTHING);

        String serviceId = helper.getDefaultServiceId();
        helper.createTestWorkersToWorkerGroup(4, workerGroupId);
        Shard shard = helper.getDefaultShardManager().getShard(shardId);
        // no replicas yet
        Assert.assertEquals(0, shard.getReplicaSize());
        scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);
        // have 3 replicas
        Assert.assertEquals(3L, shard.getReplicaSize());

        // mock a worker DOWN
        long deadWorkerId = shard.getReplicaWorkerIds().get(0);
        helper.setWorkerStatus(deadWorkerId, WorkerState.DOWN);

        // schedule again. dead replica still counts. no new replica will be created.
        scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);
        Assert.assertEquals(3L, shard.getReplicaSize());
        Assert.assertTrue(shard.hasReplica(deadWorkerId));

        helper.getWorkerManager().getWorker(deadWorkerId)
                .setLastDownTime(System.currentTimeMillis() - Config.SHARD_DEAD_REPLICA_EXPIRE_SECS * 1000L - 1000);
        // schedule again. dead replica will be expired (but not removed) and a new replica will be created.
        scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);
        Assert.assertEquals(4L, shard.getReplicaSize());

        // each worker has a replica of the shard
        Map<Long, Integer> stats = helper.collectShardReplicaDistributionStats(shardId, workerGroupId);
        List<Integer> distribution = Arrays.asList(1, 1, 1, 1);
        Assert.assertEquals(new ArrayList<>(stats.values()), distribution);
    }

    @Test
    public void testSingleShardAsyncSchedule() {
        long shardGroupId = helper.createTestShardGroup(PlacementPolicy.RANDOM);
        long shardId = helper.createTestShard(shardGroupId);
        long workerGroupId = helper.getDefaultWorkerGroupId();
        helper.createTestWorkersToWorkerGroup(3, workerGroupId);
        // schedule no wait
        scheduler.scheduleAsyncAddToGroup(serviceId, shardId, workerGroupId);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(scheduler::isIdle);
        Map<Long, Integer> stats = helper.collectShardReplicaDistributionStats(shardId, workerGroupId);
        Assert.assertFalse(stats.isEmpty());
    }

    @Test
    public void testSpreadShardGroupSchedule() {
        long spreadGroupId = helper.createTestShardGroup(PlacementPolicy.SPREAD);
        // create 3 shards and add into a SPREAD shardGroup
        List<Long> spreadShardIds = helper.createTestShards(3, spreadGroupId);
        { // create a workerGroup with 1 worker
            long wgId = helper.createWorkerGroup();
            // workerGroup only contains one worker
            long workerId = helper.createTestWorkerToWorkerGroup(wgId);
            scheduler.scheduleAddToGroup(serviceId, spreadShardIds, wgId);
            // check result, all shards scheduled to one worker
            Map<Long, Integer> stats = helper.collectShardReplicaDistributionStats(spreadShardIds, wgId);
            Assert.assertEquals(1, stats.size());
            Assert.assertTrue(stats.containsKey(workerId));
            Assert.assertEquals(stats.get(workerId), (Integer) spreadShardIds.size());
        }
        { // create a workerGroup with 2 worker
            long wgId = helper.createWorkerGroup();
            // workerGroup has 2 workers
            List<Long> workerIds = helper.createTestWorkersToWorkerGroup(2, wgId);
            scheduler.scheduleAddToGroup(serviceId, spreadShardIds, wgId);
            // 3 shards to 2 workers, with 1, 2 distribution
            Map<Long, Integer> stats = helper.collectShardReplicaDistributionStats(spreadShardIds, wgId);
            Assert.assertEquals(2, stats.size());
            for (Long id : workerIds) {
                Assert.assertTrue(stats.containsKey(id));
            }
            List<Integer> distribution = Arrays.asList(1, 2);
            Assert.assertEquals(stats.values().stream().sorted().collect(Collectors.toList()), distribution);
        }

        { // create a workerGroup with 3 worker
            long wgId = helper.createWorkerGroup();
            // workerGroup has 3 workers
            List<Long> workerIds = helper.createTestWorkersToWorkerGroup(3, wgId);
            scheduler.scheduleAddToGroup(serviceId, spreadShardIds, wgId);
            // 3 shards to 3 workers, with 1, 1, 1 distribution
            Map<Long, Integer> stats = helper.collectShardReplicaDistributionStats(spreadShardIds, wgId);
            Assert.assertEquals(3, stats.size());
            for (Long id : workerIds) {
                Assert.assertTrue(stats.containsKey(id));
            }
            List<Integer> distribution = Arrays.asList(1, 1, 1);
            Assert.assertEquals(new ArrayList<>(stats.values()), distribution);
        }
    }

    @Test
    public void testPackShardGroupSchedule() {
        long packGroupId = helper.createTestShardGroup(PlacementPolicy.PACK);
        // create 3 shards and add into a PACK shardGroup
        List<Long> packShardIds = helper.createTestShards(3, packGroupId);
        { // create a workerGroup with 1 worker
            long wgId = helper.createWorkerGroup();
            // workerGroup only contains one worker
            List<Long> workerIds = helper.createTestWorkersToWorkerGroup(1, wgId);
            scheduler.scheduleAddToGroup(serviceId, packShardIds, wgId);
            // check result, all shards scheduled to one worker
            Map<Long, Integer> stats = helper.collectShardReplicaDistributionStats(packShardIds, wgId);
            Assert.assertEquals(1, stats.size());
            Assert.assertTrue(stats.containsKey(workerIds.get(0)));
            Assert.assertEquals(stats.get(workerIds.get(0)), (Integer) packShardIds.size());
        }
        { // create a workerGroup with 2 worker, still schedule to one worker
            long wgId = helper.createWorkerGroup();
            // workerGroup has 2 workers
            List<Long> workerIds = helper.createTestWorkersToWorkerGroup(2, wgId);
            scheduler.scheduleAddToGroup(serviceId, packShardIds, wgId);
            // check result, all shards scheduled to one worker, could be either worker but all shards to the same worker.
            Map<Long, Integer> stats = helper.collectShardReplicaDistributionStats(packShardIds, wgId);
            Assert.assertEquals(1, stats.size());
            Assert.assertTrue(workerIds.contains(stats.keySet().iterator().next()));
            Assert.assertEquals((Integer) packShardIds.size(), new ArrayList<>(stats.values()).get(0));
        }

        { // create a workerGroup with 10 worker
            long wgId = helper.createWorkerGroup();
            // workerGroup has 10 workers
            List<Long> workerIds = helper.createTestWorkersToWorkerGroup(10, wgId);
            scheduler.scheduleAddToGroup(serviceId, packShardIds, wgId);
            // 3 shards to 1 worker no matter how many free workers available
            Map<Long, Integer> stats = helper.collectShardReplicaDistributionStats(packShardIds, wgId);
            Assert.assertEquals(1, stats.size());
            Assert.assertTrue(workerIds.contains(stats.keySet().iterator().next()));
            Assert.assertEquals((Integer) packShardIds.size(), new ArrayList<>(stats.values()).get(0));
        }
    }

    @Test
    public void testExcludeShardGroupSchedule() {
        long excludeGroupId = helper.createTestShardGroup(PlacementPolicy.EXCLUDE);
        // create 3 shards and add into a EXCLUDE shardGroup
        List<Long> excludeShardIds = helper.createTestShards(3, excludeGroupId);
        { // create a workerGroup with 1 worker
            long wgId = helper.createWorkerGroup();
            // workerGroup only contains one worker
            long workerId = helper.createTestWorkerToWorkerGroup(wgId);
            scheduler.scheduleAddToGroup(serviceId, excludeShardIds.get(0), wgId);
            // check result, all shards scheduled to one worker
            Map<Long, Integer> stats = helper.collectShardReplicaDistributionStats(excludeShardIds, wgId);
            Assert.assertEquals(1, stats.size());
            Assert.assertTrue(stats.containsKey(workerId));
            Assert.assertEquals((Integer) 1, stats.get(workerId));

            try {
                // only has one worker, can't schedule the 2nd shard in the exclusive shardGroup
                scheduler.scheduleAddToGroup(serviceId, excludeShardIds.get(1), wgId);
                Assert.fail("Should fail with exception");
            } catch (StarException exception) {
                Assert.assertEquals(exception.getExceptionCode(), ExceptionCode.SCHEDULE);
            }
        }
        { // create a workerGroup with 2 worker
            long wgId = helper.createWorkerGroup();
            // workerGroup has 2 workers
            List<Long> workerIds = helper.createTestWorkersToWorkerGroup(2, wgId);
            // schedule the first 2 shard, is OK
            scheduler.scheduleAddToGroup(serviceId, excludeShardIds.subList(0, 2), wgId);
            // 2 shards to 2 workers, with 1, 1 distribution
            Map<Long, Integer> stats = helper.collectShardReplicaDistributionStats(excludeShardIds, wgId);
            Assert.assertEquals(2, stats.size());
            for (Long id : workerIds) {
                Assert.assertTrue(stats.containsKey(id));
            }
            // each worker has one shard assigned
            List<Integer> distribution = Arrays.asList(1, 1);
            Assert.assertEquals(stats.values().stream().sorted().collect(Collectors.toList()), distribution);

            try {
                // the 3rd shard can't be assigned, the workerGroup only has 2 workers
                scheduler.scheduleAddToGroup(serviceId, excludeShardIds, wgId);
                Assert.fail("Should fail with exception");
            } catch (StarException exception) {
                Assert.assertEquals(exception.getExceptionCode(), ExceptionCode.SCHEDULE);
            }
        }

        { // create a workerGroup with 5 worker
            long wgId = helper.createWorkerGroup();
            // workerGroup has 5 workers
            List<Long> workerIds = helper.createTestWorkersToWorkerGroup(5, wgId);
            scheduler.scheduleAddToGroup(serviceId, excludeShardIds, wgId);
            // 3 shards to 3 workers, with 1, 1, 1 distribution
            Map<Long, Integer> stats = helper.collectShardReplicaDistributionStats(excludeShardIds, wgId);
            Assert.assertEquals(3, stats.size());
            for (Long id : stats.keySet()) {
                Assert.assertTrue(workerIds.contains(id));
            }
            List<Integer> distribution = Arrays.asList(1, 1, 1);
            Assert.assertEquals(new ArrayList<>(stats.values()), distribution);
        }
    }

    @Test
    public void testSpreadPackShardGroupSchedule() {
        // simulate co-located partition
        long spreadShardGroup1 = helper.createTestShardGroup(PlacementPolicy.SPREAD);
        List<Long> shardIds1 = helper.createTestShards(3, spreadShardGroup1);
        long spreadShardGroup2 = helper.createTestShardGroup(PlacementPolicy.SPREAD);
        List<Long> shardIds2 = helper.createTestShards(3, spreadShardGroup2);
        // create 3 PACK shardGroup
        List<Long> packGroups = new ArrayList<>();
        packGroups.add(helper.createTestShardGroup(PlacementPolicy.PACK));
        packGroups.add(helper.createTestShardGroup(PlacementPolicy.PACK));
        packGroups.add(helper.createTestShardGroup(PlacementPolicy.PACK));

        ShardManager manager = helper.getDefaultShardManager();
        for (int i = 0; i < shardIds1.size(); ++i) {
            // shardIds1[i] -> packGroups[i]
            // shardIds2[i] -> packGroups[i]
            manager.getShard(shardIds1.get(i)).joinGroup(packGroups.get(i));
            manager.getShardGroup(packGroups.get(i)).addShardId(shardIds1.get(i));
            manager.getShard(shardIds2.get(i)).joinGroup(packGroups.get(i));
            manager.getShardGroup(packGroups.get(i)).addShardId(shardIds2.get(i));
        }

        long workerGroup = helper.createWorkerGroup();
        // workerGroup has 5 workers
        helper.createTestWorkersToWorkerGroup(5, workerGroup);
        scheduler.scheduleAddToGroup(serviceId, shardIds1, workerGroup);
        Map<Long, Integer> stats1 = helper.collectShardReplicaDistributionStats(shardIds1, workerGroup);
        scheduler.scheduleAddToGroup(serviceId, shardIds2, workerGroup);
        Map<Long, Integer> stats2 = helper.collectShardReplicaDistributionStats(shardIds2, workerGroup);
        // shardIds1 and shardIds2 have the same distribution
        Assert.assertEquals(stats1, stats2);
        // All are (1, 1, 1) distribution
        List<Integer> distribution = Arrays.asList(1, 1, 1);
        Assert.assertEquals(new ArrayList<>(stats1.values()), distribution);
    }

    @Test
    public void testShardScheduleShardGroupConflictCompromised() {
        // PACK1 (A, B),  PACK2 (A, C), EXCLUDE3 (B, C)
        // Schedule A: OK
        // Schedule B: OK
        // Schedule C: OK, but PACK2 shard group policy is compromised.
        long packGroup1 = helper.createTestShardGroup(PlacementPolicy.PACK);
        long packGroup2 = helper.createTestShardGroup(PlacementPolicy.PACK);
        long excludeGroup3 = helper.createTestShardGroup(PlacementPolicy.EXCLUDE);

        long shardA = helper.createTestShard(Arrays.asList(packGroup1, packGroup2));
        long shardB = helper.createTestShard(Arrays.asList(packGroup1, excludeGroup3));
        long shardC = helper.createTestShard(Arrays.asList(packGroup2, excludeGroup3));

        long workerGroup = helper.createWorkerGroup();
        // create 5 workers
        helper.createTestWorkersToWorkerGroup(5, workerGroup);

        // schedule shardA: OK
        scheduler.scheduleAddToGroup(serviceId, shardA, workerGroup);
        Map<Long, Integer> statsA = helper.collectShardReplicaDistributionStats(shardA, workerGroup);
        Assert.assertEquals(1, statsA.size());

        // schedule shardB: OK
        scheduler.scheduleAddToGroup(serviceId, shardB, workerGroup);
        Map<Long, Integer> statsB = helper.collectShardReplicaDistributionStats(shardB, workerGroup);
        Assert.assertEquals(1, statsB.size());
        // should be on the same workers as A
        Assert.assertEquals(statsA, statsB);

        // PACK group is adjusted to the best deliver mode, in consideration of availability > restriction
        // Can schedule shardC, but PACK2 shard group restriction is not satisfied.
        scheduler.scheduleAddToGroup(serviceId, shardC, workerGroup);
        Shard shard1 = helper.getDefaultShardManager().getShard(shardA);
        Shard shard2 = helper.getDefaultShardManager().getShard(shardB);
        Shard shard3 = helper.getDefaultShardManager().getShard(shardC);

        Assert.assertEquals(1L, shard3.getReplicaSize());
        long replicaWorkerId = shard3.getReplicaWorkerIds().get(0);
        // shard3 replica is assigned to a new worker different to A and B
        Assert.assertFalse(shard1.hasReplica(replicaWorkerId));
        Assert.assertFalse(shard2.hasReplica(replicaWorkerId));
    }

    @Test
    public void testShardScheduleMultiThreadCapability() {
        // be able to do multi-thread scheduling
        new Expectations(scheduler) {
            {
                ShardSchedulerV2.estimateNumOfCoreThreads(anyLong);
                result = 8;
                minTimes = 0;
            }
        };

        long group1 = helper.createTestShardGroup(PlacementPolicy.SPREAD);
        long group2 = helper.createTestShardGroup(PlacementPolicy.SPREAD);

        // 64 * 3, 192 replicas
        List<Long> shardIds1 = helper.createTestShards(64, group1);
        List<Long> shardIds2 = helper.createTestShards(64, group2);
        long workerGroupId = helper.createWorkerGroup(3);
        helper.createTestWorkersToWorkerGroup(192, workerGroupId);
        List<Long> allShards = new ArrayList<>(shardIds1);
        allShards.addAll(shardIds2);
        // schedule all shards in a batch
        scheduler.scheduleAddToGroup(serviceId, allShards, workerGroupId);
        // collect shard distribution.
        // Expect shard spread across all workers with each worker a copy of replica
        Map<Long, Integer> stats1 = helper.collectShardReplicaDistributionStats(shardIds1, workerGroupId);
        Map<Long, Integer> stats2 = helper.collectShardReplicaDistributionStats(shardIds2, workerGroupId);
        Assert.assertEquals(192, stats1.size());
        Assert.assertEquals(Collections.nCopies(192, 1), new ArrayList<>(stats1.values()));
        Assert.assertEquals(stats2, stats1);
    }

    @Test
    public void testScheduleAddToWorker() {
        long shardId = helper.createTestShard(0);
        { // Error Case
            // resources in service(serviceId)
            long workerGroupId1 = helper.createWorkerGroup();
            List<Long> workerIds1 = helper.createTestWorkersToWorkerGroup(2, workerGroupId1);

            // resources in service(serviceId2)
            String serviceId2 = helper.bootstrapService("second-service-name");
            long workerGroupId2 = helper.createWorkerGroup(serviceId2);
            List<Long> workerIds2 = helper.createTestWorkersToWorkerGroup(serviceId2, 2, workerGroupId2);

            // serviceId not exist
            Assert.assertThrows("service not exist", StarException.class,
                    () -> scheduler.scheduleAddToWorker("not exist service id", shardId, workerIds1.get(0)));

            // shardId not exist
            Assert.assertThrows("shard not exist", StarException.class,
                    () -> scheduler.scheduleAddToWorker(serviceId, 1726394, workerIds1.get(0)));

            // workerId not exist
            Assert.assertThrows("worker not exist", StarException.class,
                    () -> scheduler.scheduleAddToWorker(serviceId, shardId, 987654));

            // workerIds2[0] NOT belongs to the serviceId
            Assert.assertThrows("worker not belongs to the service", StarException.class,
                    () -> scheduler.scheduleAddToWorker(serviceId, shardId, workerIds2.get(0)));
        }
        { // single shard add to a single worker
            // create a new workerGroup for test
            long workerGroupId = helper.createWorkerGroup();
            List<Long> workerIds = helper.createTestWorkersToWorkerGroup(2, workerGroupId);

            long workerId = workerIds.get(0);

            Map<Long, Integer> stats =
                    helper.collectShardReplicaDistributionStats(shardId, workerGroupId);
            Assert.assertTrue(stats.isEmpty());

            // shard[0] -> worker[0]
            scheduler.scheduleAddToWorker(serviceId, shardId, workerId);
            stats = helper.collectShardReplicaDistributionStats(shardId, workerGroupId);
            Assert.assertEquals(1, stats.size());
            Assert.assertTrue(stats.containsKey(workerId));
            Assert.assertEquals((Integer) 1, stats.get(workerId));
        }
        { // batch shards add to a single worker
            // create a new workerGroup for test
            long workerGroupId = helper.createWorkerGroup();
            List<Long> workerIds = helper.createTestWorkersToWorkerGroup(2, workerGroupId);
            List<Long> shardIds = helper.createTestShards(3, 0);

            // use the 2nd worker
            long workerId = workerIds.get(1);
            Map<Long, Integer> stats =
                    helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);
            Assert.assertTrue(stats.isEmpty());

            // shardIds -> worker[1]
            scheduler.scheduleAddToWorker(serviceId, shardIds, workerId);
            stats = helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);
            Assert.assertEquals(1, stats.size());
            Assert.assertTrue(stats.containsKey(workerId));
            Assert.assertEquals((Integer) shardIds.size(), stats.get(workerId));
        }
    }

    @Test
    public void testScheduleAddToWorkerWhileEncounterRpcException() {
        StarletAgentFactory.AgentType initialAgentType = StarletAgentFactory.AGENT_TYPE;
        StarletAgentFactory.AGENT_TYPE = StarletAgentFactory.AgentType.MOCK_STARLET_AGENT_WITH_RPC;
        long shardId = helper.createTestShard(0);
        { // single shard add to a single worker
            // create a new workerGroup for test
            long workerGroupId = helper.createWorkerGroup();
            List<Long> workerIds = helper.createTestWorkersToWorkerGroup(2, workerGroupId);

            long workerId = workerIds.get(0);

            Map<Long, Integer> stats =
                    helper.collectShardReplicaDistributionStats(shardId, workerGroupId);
            Assert.assertTrue(stats.isEmpty());

            // shard[0] -> worker[0]
            Assert.assertThrows(WorkerNotHealthyStarException.class,
                    () -> scheduler.scheduleAddToWorker(serviceId, shardId, workerId));
        }
        { // batch shards add to a single worker
            // create a new workerGroup for test
            long workerGroupId = helper.createWorkerGroup();
            List<Long> workerIds = helper.createTestWorkersToWorkerGroup(2, workerGroupId);
            List<Long> shardIds = helper.createTestShards(3, 0);

            // use the 2nd worker
            long workerId = workerIds.get(1);
            Map<Long, Integer> stats =
                    helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);
            Assert.assertTrue(stats.isEmpty());

            // shardIds -> worker[1]
            Assert.assertThrows(WorkerNotHealthyStarException.class,
                    () -> scheduler.scheduleAddToWorker(serviceId, shardId, workerId));
        }
        // reset
        StarletAgentFactory.AGENT_TYPE = initialAgentType;
    }

    @Test
    public void testScheduleAsyncAddToWorker() {
        List<Long> shardIds = helper.createTestShards(3, 0);
        // create a new workerGroup for test
        long workerGroupId = helper.createWorkerGroup();
        List<Long> workerIds = helper.createTestWorkersToWorkerGroup(2, workerGroupId);

        // use the 2nd worker
        long workerId = workerIds.get(1);
        Map<Long, Integer> stats =
                helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);
        Assert.assertTrue(stats.isEmpty());

        // schedule no wait
        scheduler.scheduleAsyncAddToWorker(serviceId, shardIds, workerId);
        Awaitility.await().atMost(3, TimeUnit.SECONDS).until(scheduler::isIdle);
        Map<Long, Integer> resultStats = helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);
        Assert.assertEquals(1, resultStats.size());
        Assert.assertTrue(resultStats.containsKey(workerId));
        Assert.assertEquals((Integer) shardIds.size(), resultStats.get(workerId));
    }

    @Test
    public void testScheduleRemoveFromWorker() {
        ShardManager shardManager = helper.getDefaultShardManager();
        long shardGroupId = helper.createTestShardGroup(PlacementPolicy.SPREAD);
        List<Long> shardIds = helper.createTestShards(8, shardGroupId);
        { // Error Case
            // resources in service(serviceId)
            long workerGroupId1 = helper.createWorkerGroup();
            List<Long> workerIds1 = helper.createTestWorkersToWorkerGroup(2, workerGroupId1);

            // resources in service(serviceId2)
            String serviceId2 = helper.bootstrapService("second-service-name");
            long workerGroupId2 = helper.createWorkerGroup(serviceId2);
            List<Long> workerIds2 = helper.createTestWorkersToWorkerGroup(serviceId2, 2, workerGroupId2);

            long shardId = shardIds.get(0);

            // serviceId not exist
            Assert.assertThrows("service not exist", StarException.class,
                    () -> scheduler.scheduleRemoveFromWorker("not exist service id", shardId, workerIds1.get(0)));

            // shardId not exist, blindly execution, shard replica removed,
            // because it will be ignored by shardManager when doing shardInfo update.
            scheduler.scheduleRemoveFromWorker(serviceId, 1726394, workerIds1.get(0));

            long invalidWorkerId = 987654;
            Assert.assertFalse(workerIds1.contains(invalidWorkerId));

            Shard shard = helper.getDefaultShardManager().getShard(shardId);
            // Inject a replica with invalid worker id, e.g. the worker is deleted but the replica has not cleaned yet.
            shard.addReplica(invalidWorkerId);
            Assert.assertTrue(shard.hasReplica(invalidWorkerId));

            // workerId not exist, blindly execution, shard replica removed because of an invalid replica
            scheduler.scheduleRemoveFromWorker(serviceId, shardId, invalidWorkerId);
            Assert.assertFalse(shard.hasReplica(invalidWorkerId));

            long workerId2 = workerIds2.get(0);
            // Inject a junk replica
            shard.addReplica(workerId2);
            Assert.assertTrue(shard.hasReplica(workerId2));
            // workerId -> serviceId mismatch, blindly execution, shard replica removed because of an invalid replica
            scheduler.scheduleRemoveFromWorker(serviceId, shardId, workerId2);
            Assert.assertFalse(shard.hasReplica(workerId2));
        }
        { // single shard remove to a single worker
            // create a new workerGroup for test
            long workerGroupId = helper.createWorkerGroup();
            List<Long> workerIds = helper.createTestWorkersToWorkerGroup(2, workerGroupId);

            // create shard distributions
            scheduler.scheduleAddToGroup(serviceId, shardIds, workerGroupId);

            Map<Long, Integer> stats =
                    helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);
            Assert.assertEquals(workerIds.size(), stats.size());
            long workerId = workerIds.get(0);
            Assert.assertTrue(stats.containsKey(workerId));

            List<Long> removeIds = shardIds.stream()
                    .filter(x -> shardManager.getShard(x).hasReplica(workerId))
                    .collect(Collectors.toList());
            Assert.assertFalse(removeIds.isEmpty());
            long shardId = removeIds.get(0);
            Assert.assertTrue(shardManager.getShard(shardId).hasReplica(workerId));
            scheduler.scheduleRemoveFromWorker(serviceId, shardId, workerId);
            // Replica removed
            Assert.assertFalse(shardManager.getShard(shardId).hasReplica(workerId));
        }
        { // batch shards remove to a single worker
            // create a new workerGroup for test
            long workerGroupId = helper.createWorkerGroup();
            List<Long> workerIds = helper.createTestWorkersToWorkerGroup(2, workerGroupId);
            // create shard distributions
            scheduler.scheduleAddToGroup(serviceId, shardIds, workerGroupId);

            long workerId = workerIds.get(0);
            List<Long> removeIds = shardIds.stream()
                    .filter(x -> shardManager.getShard(x).hasReplica(workerId))
                    .collect(Collectors.toList());
            Assert.assertFalse(removeIds.isEmpty());
            removeIds.forEach(x -> Assert.assertTrue(shardManager.getShard(x).hasReplica(workerId)));
            scheduler.scheduleRemoveFromWorker(serviceId, removeIds, workerId);
            // verify all replicas are cleaned
            removeIds.forEach(x -> Assert.assertFalse(shardManager.getShard(x).hasReplica(workerId)));
        }
    }

    @Test
    public void testScheduleRemoveFromWorkerWhileEncounterRpcException() {
        StarletAgentFactory.AgentType initialAgentType = StarletAgentFactory.AGENT_TYPE;
        StarletAgentFactory.AGENT_TYPE = StarletAgentFactory.AgentType.MOCK_STARLET_AGENT_WITH_RPC;
        { // single shard remove to a single worker
            long workerGroupId = helper.createWorkerGroup();
            List<Long> workerIds = helper.createTestWorkersToWorkerGroup(2, workerGroupId);
            long workerId = workerIds.get(0);
            long shardId = 123L;
            Assert.assertThrows(WorkerNotHealthyStarException.class,
                    () -> scheduler.scheduleRemoveFromWorker(serviceId, shardId, workerId));
        }
        // reset
        StarletAgentFactory.AGENT_TYPE = initialAgentType;
    }

    @Test
    public void testScheduleAsyncRemoveFromWorker() {
        ShardManager shardManager = helper.getDefaultShardManager();
        long shardGroupId = helper.createTestShardGroup(PlacementPolicy.SPREAD);
        List<Long> shardIds = helper.createTestShards(8, shardGroupId);
        // create a new workerGroup for test
        long workerGroupId = helper.createWorkerGroup();
        List<Long> workerIds = helper.createTestWorkersToWorkerGroup(2, workerGroupId);
        // create shard distributions
        scheduler.scheduleAddToGroup(serviceId, shardIds, workerGroupId);

        long workerId = workerIds.get(0);
        // get shardIds that are scheduled to workerId (worker[0])
        List<Long> removeIds = shardIds.stream()
                .filter(x -> shardManager.getShard(x).hasReplica(workerId))
                .collect(Collectors.toList());
        Assert.assertFalse(removeIds.isEmpty());
        removeIds.forEach(x -> Assert.assertTrue(shardManager.getShard(x).hasReplica(workerId)));

        // remove all shards of `removeIds` from workerId
        scheduler.scheduleAsyncRemoveFromWorker(serviceId, removeIds, workerId);
        Awaitility.await().atMost(3, TimeUnit.SECONDS).until(scheduler::isIdle);
        // verify all replicas are cleaned
        removeIds.forEach(x -> Assert.assertFalse(shardManager.getShard(x).hasReplica(workerId)));
    }

    @Test
    public void testScheduleAsyncRemoveFromGroupServiceOrShardOrWorkerGroupNotExist() {
        helper.setDefaultWorkerGroupReplicaNumber(3);
        long shardId = helper.createTestShard(0);
        List<Long> workerIds = helper.createTestWorkersToWorkerGroup(5, helper.getDefaultWorkerGroupId());
        Shard shard = helper.getDefaultShardManager().getShard(shardId);
        // manual add 5 replicas
        workerIds.forEach(shard::addReplica);

        List<Long> expectedReplicas = new ArrayList<>(shard.getReplicaWorkerIds());
        scheduler.scheduleAsyncRemoveFromGroup("not-exist-service-id", shardId, helper.getDefaultWorkerGroupId());
        Awaitility.await().atMost(3, TimeUnit.SECONDS).until(scheduler::isIdle);
        // nothing changed.
        Assert.assertEquals(expectedReplicas, shard.getReplicaWorkerIds());

        scheduler.scheduleAsyncRemoveFromGroup(serviceId, 123456789L, helper.getDefaultWorkerGroupId());
        // nothing changed.
        Awaitility.await().atMost(3, TimeUnit.SECONDS).until(scheduler::isIdle);

        scheduler.scheduleAsyncRemoveFromGroup(serviceId, shardId, 1234567890L);
        // nothing changed.
        Awaitility.await().atMost(3, TimeUnit.SECONDS).until(scheduler::isIdle);
        Assert.assertEquals(expectedReplicas, shard.getReplicaWorkerIds());
    }

    @Test
    public void testScheduleAsyncRemoveFromGroupUnhealthyReplicas() {
        helper.setDefaultWorkerGroupReplicaNumber(3);
        helper.setDefaultWorkerGroupWarmupLevel(WarmupLevel.WARMUP_INDEX);
        // Shard has 3 replicas at the beginning, then two replicas down, the scheduler is asked
        // to add 2 new replicas and to remove the 2 dead replicas one by one.
        long shardId = helper.createTestShard(0);
        List<Long> workerIds = helper.createTestWorkersToWorkerGroup(3, helper.getDefaultWorkerGroupId());
        scheduler.scheduleAddToGroup(serviceId, shardId, helper.getDefaultWorkerGroupId());
        Shard shard = helper.getDefaultShardManager().getShard(shardId);
        workerIds.forEach(x -> Assert.assertTrue(shard.hasReplica(x)));
        // set all replicas as REPLICA_OK
        workerIds.forEach(x -> Assert.assertTrue(shard.scaleOutReplicaDone(x)));

        List<Long> expectedReplicas = new ArrayList<>(shard.getReplicaWorkerIds());
        // NOW set 2 workers DOWN
        helper.setWorkerStatus(workerIds.get(0), WorkerState.DOWN);
        helper.setWorkerStatus(workerIds.get(1), WorkerState.DOWN);
        // nothing changed, because still one replica available.
        scheduler.scheduleAddToGroup(serviceId, shardId, helper.getDefaultWorkerGroupId());
        Assert.assertEquals(expectedReplicas, shard.getReplicaWorkerIds());
        // nothing changed, because no additional healthy replicas
        scheduler.scheduleAsyncRemoveFromGroup(serviceId, shardId, helper.getDefaultWorkerGroupId());
        Awaitility.await().atMost(3, TimeUnit.SECONDS).until(scheduler::isIdle);
        Assert.assertEquals(expectedReplicas, shard.getReplicaWorkerIds());

        // Expire worker[0]
        Worker w0 = helper.getWorkerManager().getWorker(workerIds.get(0));
        w0.setLastDownTime(System.currentTimeMillis() - Config.SHARD_DEAD_REPLICA_EXPIRE_SECS * 1000L - 1000);
        Assert.assertTrue(w0.replicaExpired());

        // scheduleAddToGroup will ignore expired replicas but not remove them
        Assert.assertThrows(StarException.class,
                () -> scheduler.scheduleAddToGroup(serviceId, shardId, helper.getDefaultWorkerGroupId()));
        Assert.assertEquals(expectedReplicas, shard.getReplicaWorkerIds());

        // expired replicas will be removed in `scheduleAsyncRemoveFromGroup`
        scheduler.scheduleAsyncRemoveFromGroup(serviceId, shardId, helper.getDefaultWorkerGroupId());
        Awaitility.await().atMost(3, TimeUnit.SECONDS).until(scheduler::isIdle);
        // w0 should be removed from the shard replica list because of expiration
        expectedReplicas.remove(w0.getWorkerId());
        Assert.assertEquals(expectedReplicas, shard.getReplicaWorkerIds());
        Assert.assertFalse(shard.hasReplica(workerIds.get(0)));

        // Add two more workers into workerGroup
        workerIds.addAll(helper.createTestWorkersToWorkerGroup(2, helper.getDefaultWorkerGroupId()));
        // nothing changed, because unhealthy replicas will not be removed until expired
        scheduler.scheduleAsyncRemoveFromGroup(serviceId, shardId, helper.getDefaultWorkerGroupId());
        Awaitility.await().atMost(3, TimeUnit.SECONDS).until(scheduler::isIdle);
        Assert.assertEquals(expectedReplicas, shard.getReplicaWorkerIds());

        // schedule again, only one replica will be created, because
        // 1x dead + 1x OK < expected(3)
        scheduler.scheduleAddToGroup(serviceId, shardId, helper.getDefaultWorkerGroupId());
        Assert.assertEquals(3L, shard.getReplicaSize());

        // set all replicas as REPLICA_OK
        workerIds.forEach(shard::scaleOutReplicaDone);

        // nothing changed, dead replicas are not removed because of not-expired yet
        scheduler.scheduleAsyncRemoveFromGroup(serviceId, shardId, helper.getDefaultWorkerGroupId());
        Awaitility.await().atMost(3, TimeUnit.SECONDS).until(scheduler::isIdle);
        Assert.assertEquals(3L, shard.getReplicaSize());

        try (HijackConfig var = new HijackConfig("SHARD_DEAD_REPLICA_EXPIRE_SECS", "-1")) {
            scheduler.scheduleAsyncRemoveFromGroup(serviceId, shardId, helper.getDefaultWorkerGroupId());
            Awaitility.await().atMost(3, TimeUnit.SECONDS).until(scheduler::isIdle);
            // all expired replicas are removed
            Assert.assertEquals(2L, shard.getReplicaSize());
            Assert.assertFalse(shard.hasReplica(workerIds.get(1)));

            expectedReplicas = new ArrayList<>(shard.getReplicaWorkerIds());
            scheduler.scheduleAsyncRemoveFromGroup(serviceId, shardId, helper.getDefaultWorkerGroupId());
            Awaitility.await().atMost(3, TimeUnit.SECONDS).until(scheduler::isIdle);
            // Nothing changed
            Assert.assertEquals(2L, shard.getReplicaSize());
            Assert.assertEquals(expectedReplicas, shard.getReplicaWorkerIds());

            // 2x OK, 0x dead, expected: 3
            // schedule again, A replica will be created
            scheduler.scheduleAddToGroup(serviceId, shardId, helper.getDefaultWorkerGroupId());
            Assert.assertEquals(3L, shard.getReplicaSize());
            // All are normal replicas
            shard.getReplica().forEach(x -> Assert.assertFalse(x.getTempFlag()));
        }
    }

    @Test
    public void testScheduleUnknownReplicaRemovedImmediately() {
        helper.setDefaultWorkerGroupReplicaNumber(3);
        long shardId = helper.createTestShard(0);
        Shard shard = helper.getDefaultShardManager().getShard(shardId);
        shard.addReplica(13579L);
        shard.addReplica(24680L);

        // Two replicas with unknown workers
        Assert.assertEquals(2L, shard.getReplicaSize());
        scheduler.scheduleAsyncRemoveFromGroup(serviceId, shardId, helper.getDefaultWorkerGroupId());
        Awaitility.await().atMost(3, TimeUnit.SECONDS).until(scheduler::isIdle);
        // replicas with unknown workers are removed immediately
        Assert.assertEquals(0, shard.getReplicaSize());
    }

    @Test
    public void testScheduleRemoveHealthyReplicas() {
        int expectedReplicaNum = 3;
        helper.setDefaultWorkerGroupReplicaNumber(expectedReplicaNum);
        { // shard with no group, easy to remove
            long shardId = helper.createTestShard(0);
            Shard shard = helper.getDefaultShardManager().getShard(shardId);
            List<Long> workerIds = helper.createTestWorkersToWorkerGroup(5, helper.getDefaultWorkerGroupId());
            // set 5 replicas
            workerIds.forEach(shard::addReplica);
            Assert.assertEquals(5L, shard.getReplicaSize());

            scheduler.scheduleAsyncRemoveFromGroup(serviceId, shardId, helper.getDefaultWorkerGroupId());
            Awaitility.await().atMost(3, TimeUnit.SECONDS).until(scheduler::isIdle);
            // removed one replica
            Assert.assertEquals(4L, shard.getReplicaSize());

            scheduler.scheduleAsyncRemoveFromGroup(serviceId, shardId, helper.getDefaultWorkerGroupId());
            Awaitility.await().atMost(3, TimeUnit.SECONDS).until(scheduler::isIdle);
            // removed another replica
            Assert.assertEquals(3L, shard.getReplicaSize());

            scheduler.scheduleAsyncRemoveFromGroup(serviceId, shardId, helper.getDefaultWorkerGroupId());
            Awaitility.await().atMost(3, TimeUnit.SECONDS).until(scheduler::isIdle);
            // Nothing changed
            Assert.assertEquals(3L, shard.getReplicaSize());
        }
        { // shard in RANDOM/NONE group, the worker who has the least shards is selected to remove the replica
            long shardId = helper.createTestShard(0);
            Shard shard = helper.getDefaultShardManager().getShard(shardId);
            long workerGroupId = helper.createWorkerGroup(expectedReplicaNum);
            List<Long> workerIds = helper.createTestWorkersToWorkerGroup(4, workerGroupId);
            workerIds.forEach(shard::addReplica);
            Assert.assertEquals(workerIds.size(), shard.getReplicaSize());

            for (long id : workerIds) {
                Worker worker = helper.getWorkerManager().getWorker(id);
                worker.updateInfo(System.currentTimeMillis(), null, 1);
            }
            // set a different numOfShards to worker[1]
            long testWorker = workerIds.get(1);
            helper.getWorkerManager().getWorker(testWorker).updateInfo(System.currentTimeMillis(), null, 5);
            // Make sure testWorker is in shard replica list
            Assert.assertTrue(shard.hasReplica(testWorker));

            scheduler.scheduleAsyncRemoveFromGroup(serviceId, shardId, workerGroupId);
            Awaitility.await().atMost(3, TimeUnit.SECONDS).until(scheduler::isIdle);
            // A replica is removed
            Assert.assertEquals(expectedReplicaNum, shard.getReplicaSize());
            // testWorker replica is removed
            Assert.assertFalse(shard.hasReplica(testWorker));
        }
        { // shard in SPREAD group, the worker who has higher number of replicas are removed.
            long spreadGroupId = helper.createTestShardGroup(PlacementPolicy.SPREAD);
            List<Long> shardIds = helper.createTestShards(3, spreadGroupId);
            long workerGroupId = helper.createWorkerGroup(expectedReplicaNum);
            List<Long> workerIds = helper.createTestWorkersToWorkerGroup(9, workerGroupId);
            // 3 shard with 3 replicas scheduled to 9 workers, each worker as exactly one replica
            scheduler.scheduleAddToGroup(serviceId, shardIds, workerGroupId);
            Map<Long, Integer> stats =
                    helper.collectShardReplicaDistributionStats(shardIds, workerGroupId);
            // each worker has at least one replica
            Assert.assertEquals(workerIds.size(), stats.size());
            // each worker has exactly one replica
            Assert.assertEquals(new ArrayList<>(Collections.nCopies(9, 1)), new ArrayList<>(stats.values()));

            long shardId = shardIds.get(0);
            Shard shard = helper.getDefaultShardManager().getShard(shardId);
            Optional<Long> workerId = workerIds.stream().filter(x -> !shard.hasReplica(x)).findAny();
            Assert.assertTrue(workerId.isPresent());
            Assert.assertFalse(shard.hasReplica(workerId.get()));

            // set all replica status to REPLICA_OK
            shard.getReplica().forEach(x -> x.setReplicaState(ReplicaState.REPLICA_OK));

            List<Long> expectedReplicas = new ArrayList<>(shard.getReplicaWorkerIds());
            shard.addReplica(workerId.get());
            Assert.assertTrue(shard.hasReplica(workerId.get()));
            Assert.assertNotEquals(expectedReplicas, shard.getReplicaWorkerIds());
            // Try to ask scheduler to remove redundant replica
            scheduler.scheduleAsyncRemoveFromGroup(serviceId, shardId, workerGroupId);
            Awaitility.await().atMost(3, TimeUnit.SECONDS).until(scheduler::isIdle);
            // workerId replica is removed
            Assert.assertFalse(shard.hasReplica(workerId.get()));
            Assert.assertEquals(expectedReplicas, shard.getReplicaWorkerIds());
        }
        { // shard in PACK group, the worker with the least PACK replicas get selected.
            long packGroupId = helper.createTestShardGroup(PlacementPolicy.PACK);
            // 3 shards with 3 replicas for each
            List<Long> shardIds = helper.createTestShards(3, packGroupId);
            long workerGroupId = helper.createWorkerGroup(expectedReplicaNum);
            helper.createTestWorkersToWorkerGroup(3, workerGroupId);
            // each worker has a copy of each shard
            scheduler.scheduleAddToGroup(serviceId, shardIds, workerGroupId);
            for (long id : shardIds) {
                Shard shard = helper.getDefaultShardManager().getShard(id);
                // turn every replica to OK status
                shard.getReplica().forEach(x -> x.setReplicaState(ReplicaState.REPLICA_OK));
            }
            // Add a new worker
            long testWorkerId = helper.createTestWorkerToWorkerGroup(workerGroupId);
            // shard[0].addReplica(testWorkerId)
            Shard shard0 = helper.getDefaultShardManager().getShard(shardIds.get(0));
            shard0.addReplica(testWorkerId);
            // shard[1].addReplica(testWorkerId)
            Shard shard1 = helper.getDefaultShardManager().getShard(shardIds.get(1));
            shard1.addReplica(testWorkerId);
            // update testWorkerId with correct number of Shards
            helper.getWorkerManager().getWorker(testWorkerId).updateInfo(System.currentTimeMillis(), null, 2);

            // All other testWorkers has 3 replicas, testWorkerId has 2 replicas.
            // try to remove shard[0] redundant replicas
            scheduler.scheduleAsyncRemoveFromGroup(serviceId, shard0.getShardId(), workerGroupId);
            Awaitility.await().atMost(3, TimeUnit.SECONDS).until(scheduler::isIdle);
            Assert.assertFalse(shard0.hasReplica(testWorkerId));

            // try to remove shard[1] redundant replicas
            scheduler.scheduleAsyncRemoveFromGroup(serviceId, shard1.getShardId(), workerGroupId);
            Awaitility.await().atMost(3, TimeUnit.SECONDS).until(scheduler::isIdle);
            Assert.assertFalse(shard1.hasReplica(testWorkerId));
        }
    }

    @Test
    public void testScheduleRemoveReplicaInConsiderDifferentState() {
        // A shard has REPLICA_OK, REPLICA_SCALE_IN, REPLICA_SCALE_OUT, unknown worker, dead worker
        // test the replica remove order.
        int expectedReplicaNum = 3;
        long spreadGroupId = helper.createTestShardGroup(PlacementPolicy.SPREAD);
        long shardId = helper.createTestShard(spreadGroupId);
        Shard shard = helper.getDefaultShardManager().getShard(shardId);
        long workerGroupId = helper.createWorkerGroup(serviceId, expectedReplicaNum);
        List<Long> workerIds = helper.createTestWorkersToWorkerGroup(16, workerGroupId);
        helper.getWorkerManager().getWorkerGroup(serviceId, workerGroupId).setWarmupLevel(WarmupLevel.WARMUP_META);

        // 4 - REPLICA_OK
        int index = 0;
        List<Long> okWorkerIds = workerIds.subList(index, index + 4);
        shard.addReplica(workerIds.get(index++));
        shard.addReplica(workerIds.get(index++));
        shard.addReplica(workerIds.get(index++));
        shard.addReplica(workerIds.get(index++));
        Assert.assertEquals(index, shard.getReplicaSize());

        // 2 - REPLICA_SCALE_IN
        List<Long> scaleInWorkerIds = workerIds.subList(index, index + 2);
        shard.addReplica(workerIds.get(index++), ReplicaState.REPLICA_SCALE_IN);
        shard.addReplica(workerIds.get(index++), ReplicaState.REPLICA_SCALE_IN);
        Assert.assertEquals(index, shard.getReplicaSize());

        // 1 - REPLICA_SCALE_OUT
        long scaleOutWorkerId = workerIds.get(index);
        shard.addReplica(workerIds.get(index++), ReplicaState.REPLICA_SCALE_OUT);
        Assert.assertEquals(index, shard.getReplicaSize());

        // 3 dead workers, with REPLICA_SCALE_IN, REPLICA_OK, REPLICA_SCALE_OUT
        List<Long> deadWorkerIds = workerIds.subList(index, index + 3);
        workerManager.getWorker(workerIds.get(index)).setState(WorkerState.DOWN);
        shard.addReplica(workerIds.get(index++), ReplicaState.REPLICA_OK);
        workerManager.getWorker(workerIds.get(index)).setState(WorkerState.DOWN);
        shard.addReplica(workerIds.get(index++), ReplicaState.REPLICA_SCALE_IN);
        workerManager.getWorker(workerIds.get(index)).setState(WorkerState.DOWN);
        shard.addReplica(workerIds.get(index++), ReplicaState.REPLICA_SCALE_OUT);
        Assert.assertEquals(index, shard.getReplicaSize());

        Assert.assertEquals(3L, deadWorkerIds.size());
        Assert.assertEquals(3L,
                shard.getReplica().stream().filter(x -> deadWorkerIds.contains(x.getWorkerId())).count());

        int expectedReplicas = index;
        // NOTE: dead replicas will not be removed until the replicas are expired
        // 1st round of remove replicas, remove one of the scale_in replica
        Assert.assertEquals(2L, scaleInWorkerIds.size());
        Assert.assertEquals(2L,
                shard.getReplica().stream().filter(x -> scaleInWorkerIds.contains(x.getWorkerId())).count());
        scheduler.scheduleAsyncRemoveFromGroup(serviceId, shardId, workerGroupId);
        Awaitility.await().atMost(3, TimeUnit.SECONDS).until(scheduler::isIdle);
        Assert.assertEquals(--expectedReplicas, shard.getReplicaSize());
        Assert.assertEquals(1L,
                shard.getReplica().stream().filter(x -> scaleInWorkerIds.contains(x.getWorkerId())).count());

        // 2nd round of remove replicas, remove one of the scale_in replica
        scheduler.scheduleAsyncRemoveFromGroup(serviceId, shardId, workerGroupId);
        Awaitility.await().atMost(3, TimeUnit.SECONDS).until(scheduler::isIdle);
        Assert.assertEquals(--expectedReplicas, shard.getReplicaSize());
        Assert.assertEquals(0L,
                shard.getReplica().stream().filter(x -> scaleInWorkerIds.contains(x.getWorkerId())).count());

        // 3rd round of remove replicas, remove one of the OK replica
        Assert.assertEquals(4L, okWorkerIds.size());
        Assert.assertEquals(4L,
                shard.getReplica().stream().filter(x -> okWorkerIds.contains(x.getWorkerId())).count());
        scheduler.scheduleAsyncRemoveFromGroup(serviceId, shardId, workerGroupId);
        Awaitility.await().atMost(3, TimeUnit.SECONDS).until(scheduler::isIdle);
        Assert.assertEquals(--expectedReplicas, shard.getReplicaSize());
        Assert.assertEquals(3L,
                shard.getReplica().stream().filter(x -> okWorkerIds.contains(x.getWorkerId())).count());

        // 3x REPLICA_OK + 1x SCALE_OUT + 3x Dead Replica
        Assert.assertEquals(7L, shard.getReplicaSize());
        scheduler.scheduleAsyncRemoveFromGroup(serviceId, shardId, workerGroupId);
        Awaitility.await().atMost(3, TimeUnit.SECONDS).until(scheduler::isIdle);
        // nothing changed
        Assert.assertEquals(7L, shard.getReplicaSize());

        // set SCALE_OUT to SCALE_OK
        shard.scaleOutReplicaDone(scaleOutWorkerId);
        scheduler.scheduleAsyncRemoveFromGroup(serviceId, shardId, workerGroupId);
        Awaitility.await().atMost(3, TimeUnit.SECONDS).until(scheduler::isIdle);
        // One of the REPLICA_OK replica removed again. reaches final 6 replica state (3x OK + 3x Dead)
        Assert.assertEquals(6L, shard.getReplicaSize());

        // run again, nothing changed, no expired replica
        scheduler.scheduleAsyncRemoveFromGroup(serviceId, shardId, workerGroupId);
        Awaitility.await().atMost(3, TimeUnit.SECONDS).until(scheduler::isIdle);
        Assert.assertEquals(6L, shard.getReplicaSize());
        Assert.assertEquals(3L,
                shard.getReplica().stream().filter(x -> deadWorkerIds.contains(x.getWorkerId())).count());

        workerManager.getWorker(deadWorkerIds.get(0))
                .setLastDownTime(System.currentTimeMillis() - Config.SHARD_DEAD_REPLICA_EXPIRE_SECS * 1000L - 1);
        Assert.assertTrue(workerManager.getWorker(deadWorkerIds.get(0)).replicaExpired());

        // worker0 replica expired
        scheduler.scheduleAsyncRemoveFromGroup(serviceId, shardId, workerGroupId);
        Awaitility.await().atMost(3, TimeUnit.SECONDS).until(scheduler::isIdle);
        Assert.assertEquals(5L, shard.getReplicaSize());
        Assert.assertEquals(2L,
                shard.getReplica().stream().filter(x -> deadWorkerIds.contains(x.getWorkerId())).count());

        try (HijackConfig var = new HijackConfig("SHARD_DEAD_REPLICA_EXPIRE_SECS", "-1")) {
            // Change the ConfigVar, all dead replicas should be expired
            scheduler.scheduleAsyncRemoveFromGroup(serviceId, shardId, workerGroupId);
            Awaitility.await().atMost(3, TimeUnit.SECONDS).until(scheduler::isIdle);
            Assert.assertEquals(3L, shard.getReplicaSize());
            Assert.assertEquals(0L,
                    shard.getReplica().stream().filter(x -> deadWorkerIds.contains(x.getWorkerId())).count());
        }
    }

    @Test
    public void testScheduleAddAndRemoveReplicaConflict() {
        int expectedReplicaNum = 3;
        long spreadGroupId = helper.createTestShardGroup(PlacementPolicy.SPREAD);
        List<Long> shardIds = helper.createTestShards(32, spreadGroupId);
        long workerGroupId = helper.createWorkerGroup(serviceId, expectedReplicaNum);
        List<Long> workerIds = helper.createTestWorkersToWorkerGroup(32, workerGroupId);

        Shard shard0 = helper.getDefaultShardManager().getShard(shardIds.get(0));
        // schedule shard[0]
        scheduler.scheduleAddToGroup(serviceId, shard0.getShardId(), workerGroupId);
        Assert.assertEquals(expectedReplicaNum, shard0.getReplicaSize());
        // Add two more shards to shard[0]
        for (long id : workerIds) {
            if (shard0.hasReplica(id)) {
                continue;
            }
            if (shard0.getReplicaSize() >= 5) {
                break;
            }
            shard0.addReplica(id);
        }
        // set all replicas to OK status.
        shard0.getReplica().forEach(x -> x.setReplicaState(ReplicaState.REPLICA_OK));
        Assert.assertEquals(5L, shard0.getReplicaSize());
        // now schedule the shardGroup together with the shard removal
        shardIds.forEach(x -> scheduler.scheduleAsyncAddToGroup(serviceId, x, workerGroupId));
        // submit twice, so two replicas will be removed
        scheduler.scheduleAsyncRemoveFromGroup(serviceId, shard0.getShardId(), workerGroupId);
        scheduler.scheduleAsyncRemoveFromGroup(serviceId, shard0.getShardId(), workerGroupId);
        Awaitility.await().atMost(3, TimeUnit.SECONDS).until(scheduler::isIdle);
        Assert.assertEquals(expectedReplicaNum, shard0.getReplicaSize());
    }

    @Test
    public void testSchedulingNoWorkerException() {
        long shardGroupId = helper.createTestShardGroup(PlacementPolicy.SPREAD);
        List<Long> shardIds = helper.createTestShards(2, shardGroupId);
        // create a new workerGroup but with no workers
        long workerGroupId = helper.createWorkerGroup();
        Assert.assertThrows(NoAliveWorkersException.class, () ->
                scheduler.scheduleAddToGroup(serviceId, shardIds, workerGroupId));
    }

    @Test
    public void testSchedulingAndShutdown() {
        long workerGroupId = helper.createWorkerGroup();
        // plenty of workers
        Thread doStopThread = new Thread(() -> {
            // sleep 1s and stop the schedule
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // ignore the exception
            }
            scheduler.stop();
        });

        helper.createTestWorkersToWorkerGroup(64, workerGroupId);
        // shard with no shard groups
        List<Long> shardIds = helper.createTestShards(512, 0);

        doStopThread.start();
        boolean stopped = false;
        while (!stopped) {
            for (Long id : shardIds) {
                try {
                    scheduler.scheduleAsyncAddToGroup(serviceId, id, workerGroupId);
                } catch (StarException exception) {
                    // stop when shutdown
                    Assert.assertFalse(scheduler.isRunning());
                    stopped = true;
                    break;
                }
            }
        }
        try {
            doStopThread.join();
        } catch (InterruptedException e) {
            // ignore the exception
        }
    }

    @Test
    public void testScheduleAutoAdjustThreadPoolSize() throws IllegalAccessException {
        scheduler.start();

        ThreadPoolExecutor calc = (ThreadPoolExecutor) FieldUtils.readField(scheduler, "calculateExecutors", true);
        Assert.assertNotNull(calc);
        Assert.assertEquals(calc.getCorePoolSize(), calc.getMaximumPoolSize());

        ThreadPoolExecutor dispatch = (ThreadPoolExecutor) FieldUtils.readField(scheduler, "dispatchExecutors", true);
        Assert.assertNotNull(dispatch);
        Assert.assertEquals(dispatch.getCorePoolSize(), dispatch.getMaximumPoolSize());

        int expectedThreads1 = 16;
        int expectedThreads2 = 8;
        Assert.assertNotEquals(expectedThreads1, calc.getCorePoolSize());
        Assert.assertNotEquals(expectedThreads1 * 2, dispatch.getCorePoolSize());

        new Expectations(scheduler) { // partial mock realScheduler
            {
                ShardSchedulerV2.estimateNumOfCoreThreads(anyLong);
                result = expectedThreads1;
                result = expectedThreads2;
            }
        };

        // scale out
        scheduler.adjustScheduleThreadPoolSize();

        Assert.assertEquals(expectedThreads1, calc.getCorePoolSize());
        Assert.assertEquals(expectedThreads1, calc.getMaximumPoolSize());
        Assert.assertEquals(expectedThreads1 * 2, dispatch.getCorePoolSize());
        Assert.assertEquals(expectedThreads1 * 2, dispatch.getMaximumPoolSize());

        // scale in
        scheduler.adjustScheduleThreadPoolSize();

        Assert.assertEquals(expectedThreads2, calc.getCorePoolSize());
        Assert.assertEquals(expectedThreads2, calc.getMaximumPoolSize());
        Assert.assertEquals(expectedThreads2 * 2, dispatch.getCorePoolSize());
        Assert.assertEquals(expectedThreads2 * 2, dispatch.getMaximumPoolSize());

        scheduler.stop();
    }

    @Test
    public void testSchedulerCalculateThreadNums() {
        // < 5k
        Assert.assertEquals(1, ShardSchedulerV2.estimateNumOfCoreThreads(-1));
        Assert.assertEquals(1, ShardSchedulerV2.estimateNumOfCoreThreads(0));
        Assert.assertEquals(1, ShardSchedulerV2.estimateNumOfCoreThreads(1));
        Assert.assertEquals(1, ShardSchedulerV2.estimateNumOfCoreThreads(999));
        Assert.assertEquals(1, ShardSchedulerV2.estimateNumOfCoreThreads(4999));

        // [5k, 10k)
        Assert.assertEquals(2, ShardSchedulerV2.estimateNumOfCoreThreads(5000));
        Assert.assertEquals(2, ShardSchedulerV2.estimateNumOfCoreThreads(5001));
        Assert.assertEquals(2, ShardSchedulerV2.estimateNumOfCoreThreads(9999));
        // [10k, 100k)
        Assert.assertEquals(4, ShardSchedulerV2.estimateNumOfCoreThreads(10000));
        Assert.assertEquals(4, ShardSchedulerV2.estimateNumOfCoreThreads(23456));
        Assert.assertEquals(4, ShardSchedulerV2.estimateNumOfCoreThreads(99999));

        // [100k, 1M)
        Assert.assertEquals(8, ShardSchedulerV2.estimateNumOfCoreThreads(100000));
        Assert.assertEquals(8, ShardSchedulerV2.estimateNumOfCoreThreads(358679));
        Assert.assertEquals(8, ShardSchedulerV2.estimateNumOfCoreThreads(999999));
        // > 1M
        Assert.assertEquals(16, ShardSchedulerV2.estimateNumOfCoreThreads(10000000));
        Assert.assertEquals(16, ShardSchedulerV2.estimateNumOfCoreThreads(30000000));
        Assert.assertEquals(16, ShardSchedulerV2.estimateNumOfCoreThreads(99999999));
        Assert.assertEquals(16, ShardSchedulerV2.estimateNumOfCoreThreads(100000000));
        Assert.assertEquals(16, ShardSchedulerV2.estimateNumOfCoreThreads(1223456789));
    }

    @Test
    public void testSameShardGroupScheduleToDifferentWorkerGroupWithRequiredReplicas() {
        long shardGroupId = helper.createTestShardGroup(PlacementPolicy.SPREAD);
        List<Long> shardIds = helper.createTestShards(8, shardGroupId);

        // workerGroup1 requires 1 replicas for the shards
        long workerGroupId1 = helper.createWorkerGroup(1);
        // workerGroup2 requires 3 replicas for the shards
        long workerGroupId2 = helper.createWorkerGroup(3);

        // workerGroup1 has 4 workers, expects each node hosting 2 replicas
        helper.createTestWorkersToWorkerGroup(4, workerGroupId1);
        // workerGroup2 has 12 workers, expects each node hosting 2 replicas
        helper.createTestWorkersToWorkerGroup(12, workerGroupId2);

        // create shard distributions to workerGroup1
        scheduler.scheduleAddToGroup(serviceId, shardIds, workerGroupId1);
        for (long shardId : shardIds) {
            Assert.assertEquals(1, helper.getDefaultShardManager().getShard(shardId).getReplicaSize());
        }
        // create shard distributions to workerGroup2, each shard will have 4 replicas: 1 in group1, 3 in group2
        scheduler.scheduleAddToGroup(serviceId, shardIds, workerGroupId2);
        for (long shardId : shardIds) {
            Assert.assertEquals(4, helper.getDefaultShardManager().getShard(shardId).getReplicaSize());
        }
    }

    @Test
    public void testAddShardWithReplicaInfoToWorker() {
        long shardGroupId = helper.createTestShardGroup(PlacementPolicy.SPREAD);
        long shardId = helper.createTestShard(shardGroupId);
        Shard shard = helper.getDefaultShardManager().getShard(shardId);
        long workerGroupId = helper.createWorkerGroup(3);
        // enable replication sync in the worker group
        helper.setWorkerGroupReplicationType(workerGroupId, ReplicationType.SYNC);
        helper.setWorkerGroupWarmupLevel(workerGroupId, WarmupLevel.WARMUP_ALL);

        long workerId1 = helper.createTestWorkerToWorkerGroup(workerGroupId);
        // should have only one replica
        scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);
        Assert.assertEquals(1L, shard.getReplicaSize());
        Assert.assertEquals(workerId1, shard.getReplica().get(0).getWorkerId());

        { // verify the info sent to starlet side
            StarletAgent agent = helper.getMockWorkerStarletAgent(workerId1);
            Assert.assertNotNull(agent);
            Assert.assertTrue(agent instanceof MockStarletAgent);
            AddShardInfo info = ((MockStarletAgent) agent).getAllShards().get(shardId);
            Assert.assertEquals(1L, info.getReplicaInfoCount());
            ReplicaInfoLite infoLite0 = info.getReplicaInfo(0);
            Assert.assertEquals(workerId1, infoLite0.getWorkerId());
            Assert.assertEquals(ReplicaState.REPLICA_SCALE_OUT, infoLite0.getReplicaState());
        }

        long workerId2 = helper.createTestWorkerToWorkerGroup(workerGroupId);
        scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);
        Assert.assertEquals(2L, shard.getReplicaSize());
        Assert.assertEquals(workerId2, shard.getReplica().get(1).getWorkerId());

        { // verify the info sent to starlet side
            StarletAgent agent = helper.getMockWorkerStarletAgent(workerId2);
            Assert.assertNotNull(agent);
            Assert.assertTrue(agent instanceof MockStarletAgent);
            AddShardInfo info = ((MockStarletAgent) agent).getAllShards().get(shardId);
            Assert.assertEquals(2L, info.getReplicaInfoCount());
            ReplicaInfoLite infoLite0 = info.getReplicaInfo(0);
            Assert.assertEquals(workerId1, infoLite0.getWorkerId());
            Assert.assertEquals(ReplicaState.REPLICA_SCALE_OUT, infoLite0.getReplicaState());

            ReplicaInfoLite infoLite1 = info.getReplicaInfo(1);
            Assert.assertEquals(workerId2, infoLite1.getWorkerId());
            Assert.assertEquals(ReplicaState.REPLICA_SCALE_OUT, infoLite1.getReplicaState());
        }

        long workerId3 = helper.createTestWorkerToWorkerGroup(workerGroupId);
        scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);
        Assert.assertEquals(3L, shard.getReplicaSize());
        Assert.assertEquals(workerId3, shard.getReplica().get(2).getWorkerId());

        { // verify the info sent to starlet side
            StarletAgent agent = helper.getMockWorkerStarletAgent(workerId3);
            Assert.assertNotNull(agent);
            Assert.assertTrue(agent instanceof MockStarletAgent);
            AddShardInfo info = ((MockStarletAgent) agent).getAllShards().get(shardId);
            Assert.assertEquals(3L, info.getReplicaInfoCount());
            ReplicaInfoLite infoLite0 = info.getReplicaInfo(0);
            Assert.assertEquals(workerId1, infoLite0.getWorkerId());
            Assert.assertEquals(ReplicaState.REPLICA_SCALE_OUT, infoLite0.getReplicaState());

            ReplicaInfoLite infoLite1 = info.getReplicaInfo(1);
            Assert.assertEquals(workerId2, infoLite1.getWorkerId());
            Assert.assertEquals(ReplicaState.REPLICA_SCALE_OUT, infoLite1.getReplicaState());

            ReplicaInfoLite infoLite2 = info.getReplicaInfo(2);
            Assert.assertEquals(workerId3, infoLite2.getWorkerId());
            Assert.assertEquals(ReplicaState.REPLICA_SCALE_OUT, infoLite2.getReplicaState());
        }
        // TODO: the first worker still has one replica info. the updateShardInfo action is not triggered.
    }

    @Test
    public void testNewReplicaCreatedIfNoLiveReplicas() {
        ShardManager shardManager = helper.getDefaultShardManager();
        WorkerManager workerManager = helper.getWorkerManager();
        long shardGroupId = helper.createTestShardGroup(PlacementPolicy.SPREAD);
        long shardId = helper.createTestShard(shardGroupId);
        Shard shard = shardManager.getShard(shardId);
        // the workerGroup requires 3 replicas
        long workerGroupId = helper.createWorkerGroup(3);
        // there are 5 workers in the workerGroup
        helper.createTestWorkersToWorkerGroup(5, workerGroupId);

        scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);
        Assert.assertEquals(3L, shard.getReplicaSize());
        // All are alive
        Assert.assertEquals(3L,
                shard.getReplicaWorkerIds().stream().filter(x -> workerManager.getWorker(x).isAlive()).count());

        // set a worker down
        workerManager.getWorker(shard.getReplicaWorkerIds().get(0)).setState(WorkerState.DOWN);
        scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);
        // no new replica
        Assert.assertEquals(3L, shard.getReplicaSize());
        Assert.assertEquals(2L,
                shard.getReplicaWorkerIds().stream().filter(x -> workerManager.getWorker(x).isAlive()).count());

        // set the second worker down
        workerManager.getWorker(shard.getReplicaWorkerIds().get(1)).setState(WorkerState.DOWN);
        scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);
        // no new replica
        Assert.assertEquals(3L, shard.getReplicaSize());
        // only one alive
        Assert.assertEquals(1L,
                shard.getReplicaWorkerIds().stream().filter(x -> workerManager.getWorker(x).isAlive()).count());

        // set the third worker down
        workerManager.getWorker(shard.getReplicaWorkerIds().get(2)).setState(WorkerState.DOWN);
        scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);
        // one new replica added
        Assert.assertEquals(4L, shard.getReplicaSize());
        // only one alive
        Assert.assertEquals(1L,
                shard.getReplicaWorkerIds().stream().filter(x -> workerManager.getWorker(x).isAlive()).count());

        // set the forth worker down
        workerManager.getWorker(shard.getReplicaWorkerIds().get(3)).setState(WorkerState.DOWN);
        scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);
        // one new replica added
        Assert.assertEquals(5L, shard.getReplicaSize());
        // only one alive
        Assert.assertEquals(1L,
                shard.getReplicaWorkerIds().stream().filter(x -> workerManager.getWorker(x).isAlive()).count());
    }

    @Test
    public void testScaleOutSingleTempReplicaSingleReplicaAndRemoved() {
        ShardManager shardManager = helper.getDefaultShardManager();
        WorkerManager workerManager = helper.getWorkerManager();
        long shardGroupId = helper.createTestShardGroup(PlacementPolicy.SPREAD);
        long shardId = helper.createTestShard(shardGroupId);
        Shard shard = shardManager.getShard(shardId);
        // the workerGroup requires single replica
        long workerGroupId = helper.createWorkerGroup(1);
        // there are enough workers in the workerGroup
        helper.createTestWorkersToWorkerGroup(5, workerGroupId);

        scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);
        Assert.assertEquals(1L, shard.getReplicaSize());

        // set worker0 to DOWN
        Worker worker0 = workerManager.getWorker(shard.getReplicaWorkerIds().get(0));
        worker0.setState(WorkerState.DOWN);

        // schedule again, a new replica will be created with temp flag
        scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);
        scheduler.scheduleAsyncRemoveFromGroup(serviceId, shardId, workerGroupId);
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(scheduler::isIdle);

        // schedule again, a new replica will be created with temp flag, no replica will be removed
        Assert.assertEquals(2L, shard.getReplicaSize());
        {
            Replica replica = shard.getReplica().get(1);
            // it's a temp flag
            Assert.assertTrue(replica.getTempFlag());
        }

        // set worker1 DOWN
        Worker worker1 = workerManager.getWorker(shard.getReplicaWorkerIds().get(1));
        worker1.setState(WorkerState.DOWN);

        // schedule again, a new replica will be created with temp flag
        scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);
        scheduler.scheduleAsyncRemoveFromGroup(serviceId, shardId, workerGroupId);
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(scheduler::isIdle);

        // schedule again, a new replica will be created with temp flag, no replica will be removed
        Assert.assertEquals(3L, shard.getReplicaSize());
        {
            Replica replica = shard.getReplica().get(2);
            // it's a temp flag
            Assert.assertTrue(replica.getTempFlag());
        }

        // set both worker0, worker1 back to normal
        worker0.setState(WorkerState.ON);
        worker1.setState(WorkerState.ON);

        // schedule again, the original workerId0 will be kept, the temp replicas will be removed one by one
        scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);
        scheduler.scheduleAsyncRemoveFromGroup(serviceId, shardId, workerGroupId);
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(scheduler::isIdle);
        // one temp replica will be removed
        Assert.assertEquals(2L, shard.getReplicaSize());
        Assert.assertTrue(shard.hasReplica(worker0.getWorkerId()));

        // schedule again, the original workerId0 will be kept, the temp replicas will be removed
        scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);
        scheduler.scheduleAsyncRemoveFromGroup(serviceId, shardId, workerGroupId);
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(scheduler::isIdle);
        // one temp replica will be removed
        Assert.assertEquals(1L, shard.getReplicaSize());
        Assert.assertTrue(shard.hasReplica(worker0.getWorkerId()));
    }

    @Test
    public void testScaleOutSingleTempReplicaMultipleReplicasAndRemoved() {
        ShardManager shardManager = helper.getDefaultShardManager();
        WorkerManager workerManager = helper.getWorkerManager();
        long shardGroupId = helper.createTestShardGroup(PlacementPolicy.SPREAD);
        long shardId = helper.createTestShard(shardGroupId);
        Shard shard = shardManager.getShard(shardId);
        // the workerGroup requires 3 replicas
        long workerGroupId = helper.createWorkerGroup(3);
        // there are enough workers in the workerGroup
        helper.createTestWorkersToWorkerGroup(5, workerGroupId);

        scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);
        Assert.assertEquals(3L, shard.getReplicaSize());
        List<Long> expectedWorkerIds = new ArrayList<>(shard.getReplicaWorkerIds());

        // set worker0 to DOWN
        long workerId0 = shard.getReplicaWorkerIds().get(0);
        workerManager.getWorker(workerId0).setState(WorkerState.DOWN);

        // schedule add and remove, nothing changed.
        scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);
        scheduler.scheduleAsyncRemoveFromGroup(serviceId, shardId, workerGroupId);
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(scheduler::isIdle);

        Assert.assertEquals(3L, shard.getReplicaSize());
        Assert.assertEquals(expectedWorkerIds, shard.getReplicaWorkerIds());

        // set worker1 DOWN
        long workerId1 = shard.getReplicaWorkerIds().get(1);
        workerManager.getWorker(workerId1).setState(WorkerState.DOWN);

        // schedule add and remove, nothing changed.
        scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);
        scheduler.scheduleAsyncRemoveFromGroup(serviceId, shardId, workerGroupId);
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(scheduler::isIdle);

        Assert.assertEquals(3L, shard.getReplicaSize());
        Assert.assertEquals(expectedWorkerIds, shard.getReplicaWorkerIds());

        // set worker2 DOWN
        long workerId2 = shard.getReplicaWorkerIds().get(2);
        workerManager.getWorker(workerId2).setState(WorkerState.DOWN);

        // schedule add and remove, Now a new replica is created with the temp replica
        scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);
        scheduler.scheduleAsyncRemoveFromGroup(serviceId, shardId, workerGroupId);
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(scheduler::isIdle);

        Assert.assertEquals(4L, shard.getReplicaSize());
        Replica replica = shard.getReplica().get(3);
        Assert.assertTrue(replica.getTempFlag());
        long workerId4 = replica.getWorkerId();

        // now the workers are back one by one
        expectedWorkerIds = new ArrayList<>(shard.getReplicaWorkerIds());

        workerManager.getWorker(workerId2).setState(WorkerState.ON);
        // schedule add and remove, nothing changed
        scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);
        scheduler.scheduleAsyncRemoveFromGroup(serviceId, shardId, workerGroupId);
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(scheduler::isIdle);
        Assert.assertEquals(4L, shard.getReplicaSize());
        Assert.assertEquals(expectedWorkerIds, shard.getReplicaWorkerIds());
        Assert.assertTrue(shard.hasReplica(workerId4));

        workerManager.getWorker(workerId1).setState(WorkerState.ON);
        // schedule add and remove, nothing changed
        scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);
        scheduler.scheduleAsyncRemoveFromGroup(serviceId, shardId, workerGroupId);
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(scheduler::isIdle);
        Assert.assertEquals(4L, shard.getReplicaSize());
        Assert.assertEquals(expectedWorkerIds, shard.getReplicaWorkerIds());
        Assert.assertTrue(shard.hasReplica(workerId4));

        workerManager.getWorker(workerId0).setState(WorkerState.ON);
        // all workers are back, the temp replica is removed
        scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);
        scheduler.scheduleAsyncRemoveFromGroup(serviceId, shardId, workerGroupId);
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(scheduler::isIdle);
        Assert.assertEquals(3L, shard.getReplicaSize());
        Assert.assertFalse(shard.hasReplica(workerId4));
    }

    @Test
    public void testSingleReplicaTempReplicaConvertedToNormal() {
        // * Replica0 Alive
        // * Replica0 DOWN
        // * Replica1 (TEMP) created
        // * Replica0 expired
        // * Replica1 converted to normal
        ShardManager shardManager = helper.getDefaultShardManager();
        WorkerManager workerManager = helper.getWorkerManager();
        long shardGroupId = helper.createTestShardGroup(PlacementPolicy.SPREAD);
        long shardId = helper.createTestShard(shardGroupId);
        Shard shard = shardManager.getShard(shardId);
        // the workerGroup requires single replica
        long workerGroupId = helper.createWorkerGroup(1);
        // there are enough workers in the workerGroup
        helper.createTestWorkersToWorkerGroup(5, workerGroupId);

        scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);
        Assert.assertEquals(1L, shard.getReplicaSize());

        // set worker0 to DOWN
        Worker worker0 = workerManager.getWorker(shard.getReplicaWorkerIds().get(0));
        worker0.setState(WorkerState.DOWN);

        // schedule again, a new replica will be created with temp flag
        scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);
        scheduler.scheduleAsyncRemoveFromGroup(serviceId, shardId, workerGroupId);
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(scheduler::isIdle);
        Assert.assertEquals(2L, shard.getReplicaSize());
        long workerId;
        {
            Replica replica = shard.getReplica().get(1);
            // it's a temp flag
            Assert.assertTrue(replica.getTempFlag());
            workerId = replica.getWorkerId();
        }

        // expire the dead replica
        worker0.setLastDownTime(System.currentTimeMillis() - Config.SHARD_DEAD_REPLICA_EXPIRE_SECS * 1000L - 1000);
        // schedule again, the dead replica is removed and the temp replica is converted to normal replica
        scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);
        scheduler.scheduleAsyncRemoveFromGroup(serviceId, shardId, workerGroupId);
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(scheduler::isIdle);

        Assert.assertEquals(1L, shard.getReplicaSize());
        // original one removed
        Assert.assertFalse(shard.hasReplica(worker0.getWorkerId()));
        // new one created
        Assert.assertTrue(shard.hasReplica(workerId));
        {
            Replica replica = shard.getReplica().get(0);
            Assert.assertFalse(replica.getTempFlag());
        }

        // set worker0 back to normal, nothing changed.
        worker0.setState(WorkerState.ON);
        scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);
        scheduler.scheduleAsyncRemoveFromGroup(serviceId, shardId, workerGroupId);
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(scheduler::isIdle);

        Assert.assertEquals(1L, shard.getReplicaSize());
        Assert.assertFalse(shard.hasReplica(worker0.getWorkerId()));
        Assert.assertTrue(shard.hasReplica(workerId));
    }

    @Test
    public void testMultipleReplicaTempReplicaConvertedToNormal() {
        // * Replica0,1,2 Alive
        // * Replica0,1,2 DOWN
        // * Replica3 (TEMP) created
        // * Replica0 expired
        // * Replica3 converted to normal
        ShardManager shardManager = helper.getDefaultShardManager();
        WorkerManager workerManager = helper.getWorkerManager();
        long shardGroupId = helper.createTestShardGroup(PlacementPolicy.SPREAD);
        long shardId = helper.createTestShard(shardGroupId);
        Shard shard = shardManager.getShard(shardId);
        // the workerGroup requires single replica
        long workerGroupId = helper.createWorkerGroup(3);
        // there are enough workers in the workerGroup
        helper.createTestWorkersToWorkerGroup(5, workerGroupId);

        scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);
        Assert.assertEquals(3L, shard.getReplicaSize());
        List<Long> replicaWorkerIds = new ArrayList<>(shard.getReplicaWorkerIds());

        // set worker0 to DOWN
        Worker worker0 = workerManager.getWorker(shard.getReplicaWorkerIds().get(0));
        worker0.setState(WorkerState.DOWN);

        // schedule again, no change, 2 alive, 1 dead
        scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);
        scheduler.scheduleAsyncRemoveFromGroup(serviceId, shardId, workerGroupId);
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(scheduler::isIdle);
        Assert.assertEquals(3L, shard.getReplicaSize());
        Assert.assertEquals(replicaWorkerIds, shard.getReplicaWorkerIds());

        // set worker1 to DOWN
        Worker worker1 = workerManager.getWorker(shard.getReplicaWorkerIds().get(1));
        worker1.setState(WorkerState.DOWN);

        // schedule again, no change, 1 alive, 2 dead
        scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);
        scheduler.scheduleAsyncRemoveFromGroup(serviceId, shardId, workerGroupId);
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(scheduler::isIdle);
        Assert.assertEquals(3L, shard.getReplicaSize());
        Assert.assertEquals(replicaWorkerIds, shard.getReplicaWorkerIds());

        // set worker2 to DOWN
        Worker worker2 = workerManager.getWorker(shard.getReplicaWorkerIds().get(2));
        worker2.setState(WorkerState.DOWN);

        // schedule again, new temp replica created. 1 alive temp, 3 dead
        scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);
        scheduler.scheduleAsyncRemoveFromGroup(serviceId, shardId, workerGroupId);
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(scheduler::isIdle);
        Assert.assertEquals(4L, shard.getReplicaSize());
        Assert.assertEquals(replicaWorkerIds, shard.getReplicaWorkerIds().subList(0, 3));
        long tempReplicaWorkerId;
        {
            Replica replica = shard.getReplica().get(3);
            Assert.assertTrue(replica.getTempFlag());
            tempReplicaWorkerId = replica.getWorkerId();
        }

        // expire the dead replica on worker0
        worker0.setLastDownTime(System.currentTimeMillis() - Config.SHARD_DEAD_REPLICA_EXPIRE_SECS * 1000L - 1000);

        // schedule again, the dead replica is removed and the temp replica is converted to normal replica
        scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);
        scheduler.scheduleAsyncRemoveFromGroup(serviceId, shardId, workerGroupId);
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(scheduler::isIdle);

        Assert.assertEquals(3L, shard.getReplicaSize());
        Assert.assertFalse(shard.hasReplica(worker0.getWorkerId()));
        Assert.assertTrue(shard.hasReplica(tempReplicaWorkerId));
        {
            Replica replica = shard.getReplica().get(2);
            Assert.assertFalse(replica.getTempFlag());
            Assert.assertEquals(tempReplicaWorkerId, replica.getWorkerId());
        }
    }

    @Test
    public void testMultipleTempReplicaRemovedAndConvertedToNormal() {
        // * Replica0,1,2 Alive
        // * Replica0,1,2 DOWN
        // * Replica3 (TEMP) created
        // * Replica3 DOWN
        // * Replica4 (TEMP) created
        // * Replica3 UP
        // * Replica2 expired, nothing happen
        // * Replica1 expired, replica3,4 converted to normal
        // * Replica0 expired, replica5 created (normal replica)
        ShardManager shardManager = helper.getDefaultShardManager();
        WorkerManager workerManager = helper.getWorkerManager();
        long shardGroupId = helper.createTestShardGroup(PlacementPolicy.SPREAD);
        long shardId = helper.createTestShard(shardGroupId);
        Shard shard = shardManager.getShard(shardId);
        // the workerGroup requires single replica
        long workerGroupId = helper.createWorkerGroup(3);
        // there are enough workers in the workerGroup
        helper.createTestWorkersToWorkerGroup(10, workerGroupId);

        scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);
        Assert.assertEquals(3L, shard.getReplicaSize());
        List<Long> replicaWorkerIds = new ArrayList<>(shard.getReplicaWorkerIds());

        // set worker0 to DOWN
        Worker worker0 = workerManager.getWorker(shard.getReplicaWorkerIds().get(0));
        worker0.setState(WorkerState.DOWN);

        // schedule again, no change, 2 alive, 1 dead
        scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);
        scheduler.scheduleAsyncRemoveFromGroup(serviceId, shardId, workerGroupId);
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(scheduler::isIdle);
        Assert.assertEquals(3L, shard.getReplicaSize());
        Assert.assertEquals(replicaWorkerIds, shard.getReplicaWorkerIds());

        // set worker1 to DOWN
        Worker worker1 = workerManager.getWorker(shard.getReplicaWorkerIds().get(1));
        worker1.setState(WorkerState.DOWN);

        // schedule again, no change, 1 alive, 2 dead
        scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);
        scheduler.scheduleAsyncRemoveFromGroup(serviceId, shardId, workerGroupId);
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(scheduler::isIdle);
        Assert.assertEquals(3L, shard.getReplicaSize());
        Assert.assertEquals(replicaWorkerIds, shard.getReplicaWorkerIds());

        // set worker2 to DOWN
        Worker worker2 = workerManager.getWorker(shard.getReplicaWorkerIds().get(2));
        worker2.setState(WorkerState.DOWN);

        // schedule again, new temp replica created. 1 alive temp, 3 dead
        scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);
        scheduler.scheduleAsyncRemoveFromGroup(serviceId, shardId, workerGroupId);
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(scheduler::isIdle);
        Assert.assertEquals(4L, shard.getReplicaSize());
        Assert.assertEquals(replicaWorkerIds, shard.getReplicaWorkerIds().subList(0, 3));
        {
            Replica replica = shard.getReplica().get(3);
            Assert.assertTrue(replica.getTempFlag());
        }

        // set worker3 to DOWN
        Worker worker3 = workerManager.getWorker(shard.getReplicaWorkerIds().get(3));
        worker3.setState(WorkerState.DOWN);

        // schedule again, new temp replica created. 1 alive temp, 4 dead (3 ok, 1 temp)
        scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);
        scheduler.scheduleAsyncRemoveFromGroup(serviceId, shardId, workerGroupId);
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(scheduler::isIdle);
        Assert.assertEquals(5L, shard.getReplicaSize());
        Assert.assertEquals(replicaWorkerIds, shard.getReplicaWorkerIds().subList(0, 3));
        {
            Replica replica = shard.getReplica().get(4);
            Assert.assertTrue(replica.getTempFlag());
        }

        List<Long> beforeWorker3Up = new ArrayList<>(shard.getReplicaWorkerIds());
        // take worker3 back online
        worker3.setState(WorkerState.ON);
        // schedule again, nothing changed. 2 alive temp, 3 dead
        scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);
        scheduler.scheduleAsyncRemoveFromGroup(serviceId, shardId, workerGroupId);
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(scheduler::isIdle);
        Assert.assertEquals(5L, shard.getReplicaSize());
        Assert.assertEquals(beforeWorker3Up, shard.getReplicaWorkerIds());
        {
            // [dead, dead, dead, temp-alive, temp-alive]
            Assert.assertTrue(shard.getReplica().get(4).getTempFlag());
            Assert.assertTrue(shard.getReplica().get(3).getTempFlag());
        }

        // expire the dead replica on worker2
        worker2.setLastDownTime(System.currentTimeMillis() - Config.SHARD_DEAD_REPLICA_EXPIRE_SECS * 1000L - 1000);

        // schedule again, the dead replica is removed , 2 alive temp, 2 dead, nothing removed
        scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);
        scheduler.scheduleAsyncRemoveFromGroup(serviceId, shardId, workerGroupId);
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(scheduler::isIdle);

        Assert.assertEquals(4L, shard.getReplicaSize());
        Assert.assertFalse(shard.hasReplica(worker2.getWorkerId()));
        {
            // [dead, dead, temp-alive, temp-alive]
            Assert.assertTrue(shard.getReplica().get(3).getTempFlag());
            Assert.assertTrue(shard.getReplica().get(2).getTempFlag());
        }

        // expire the dead replica on worker1
        worker1.setLastDownTime(System.currentTimeMillis() - Config.SHARD_DEAD_REPLICA_EXPIRE_SECS * 1000L - 1000);

        // schedule again, the dead replica is removed, the temp is converted to normal: [dead, alive, alive]
        scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);
        scheduler.scheduleAsyncRemoveFromGroup(serviceId, shardId, workerGroupId);
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(scheduler::isIdle);

        Assert.assertEquals(3L, shard.getReplicaSize());
        Assert.assertFalse(shard.hasReplica(worker1.getWorkerId()));
        {
            // [dead, alive, alive]
            Assert.assertFalse(shard.getReplica().get(2).getTempFlag());
            Assert.assertFalse(shard.getReplica().get(1).getTempFlag());
        }

        // expire the dead replica on worker0
        worker0.setLastDownTime(System.currentTimeMillis() - Config.SHARD_DEAD_REPLICA_EXPIRE_SECS * 1000L - 1000);

        // schedule again, the dead replica is removed, a new normal replica is created:  3x alive
        scheduler.scheduleAddToGroup(serviceId, shardId, workerGroupId);
        scheduler.scheduleAsyncRemoveFromGroup(serviceId, shardId, workerGroupId);
        Awaitility.await().atMost(2, TimeUnit.SECONDS).until(scheduler::isIdle);

        Assert.assertEquals(3L, shard.getReplicaSize());
        Assert.assertFalse(shard.hasReplica(worker1.getWorkerId()));
        {
            // [alive, alive, alive]
            Assert.assertFalse(shard.getReplica().get(0).getTempFlag());
            Assert.assertFalse(shard.getReplica().get(1).getTempFlag());
            Assert.assertFalse(shard.getReplica().get(2).getTempFlag());
        }
    }

    @Test
    public void testAddShardNewReplicaToWorkerWithReplicaInfo() {
        // Create one shard
        ShardManager shardManager = helper.getDefaultShardManager();
        long shardGroupId = helper.createTestShardGroup(PlacementPolicy.SPREAD);
        long shardId = helper.createTestShard(shardGroupId);
        Shard shard = shardManager.getShard(shardId);

        List<ReplicationType> replicationTypes =
                Lists.newArrayList(ReplicationType.NO_REPLICATION, ReplicationType.SYNC);
        List<WarmupLevel> warmupLevels = Lists.newArrayList(WarmupLevel.WARMUP_NOTHING, WarmupLevel.WARMUP_ALL);
        Map<Long, Long> workerGroupId2WorkerId = new HashMap<>();
        // Create 4 workerGroups
        for (WarmupLevel level : warmupLevels) {
            for (ReplicationType replicationType : replicationTypes) {
                long workerGroupId = helper.createWorkerGroup(1);
                helper.setWorkerGroupReplicationType(workerGroupId, replicationType);
                helper.setWorkerGroupWarmupLevel(workerGroupId, level);
                // Add a worker into the workerGroup
                long workerId = helper.createTestWorkerToWorkerGroup(workerGroupId);
                workerGroupId2WorkerId.put(workerGroupId, workerId);
            }
        }

        // Schedule the shard into the 4 workerGroups with different configurations and validate the result.
        for (long workerId : workerGroupId2WorkerId.values()) {
            scheduler.scheduleAddToWorker(helper.getDefaultServiceId(), shardId, workerId);
        }

        // Verify result
        for (Map.Entry<Long, Long> entry : workerGroupId2WorkerId.entrySet()) {
            long workerGroupId = entry.getKey();
            long workerId = entry.getValue();
            Assert.assertTrue(shard.hasReplica(workerId));
            AddShardInfo info = getAddShardInfoFromStarletAgent(workerId, shard.getShardId());
            WorkerGroup wg =
                    helper.getWorkerManager().getWorkerGroupNoException(helper.getDefaultServiceId(), workerGroupId);
            ReplicationType replicationType = wg.getReplicationType();
            WarmupLevel warmupLevel = wg.getWarmupLevel();
            String msg = String.format("ReplicationType=%s, WarmupLevel=%s", replicationType, warmupLevel);

            Assert.assertEquals(msg, 1L, info.getReplicaInfoCount());
            ReplicaInfoLite infoLite = info.getReplicaInfo(0);
            Assert.assertEquals(msg, workerId, infoLite.getWorkerId());
            if (warmupLevel != WarmupLevel.WARMUP_NOTHING) {
                Assert.assertEquals(msg, ReplicaState.REPLICA_SCALE_OUT, infoLite.getReplicaState());
            } else {
                Assert.assertEquals(msg, ReplicaState.REPLICA_OK, infoLite.getReplicaState());
            }
        }
    }

    AddShardInfo getAddShardInfoFromStarletAgent(long workerId, long shardId) {
        StarletAgent agent = helper.getMockWorkerStarletAgent(workerId);
        Assert.assertNotNull(agent);
        Assert.assertTrue(agent instanceof MockStarletAgent);
        return ((MockStarletAgent) agent).getAllShards().get(shardId);
    }

    @Test
    public void testAddShardExistingReplicaToWorkerWithReplicaInfo() {
        // Create one shard
        ShardManager shardManager = helper.getDefaultShardManager();
        long shardGroupId = helper.createTestShardGroup(PlacementPolicy.SPREAD);
        long shardId = helper.createTestShard(shardGroupId);
        Shard shard = shardManager.getShard(shardId);


        List<ReplicationType> replicationTypes =
                Lists.newArrayList(ReplicationType.NO_REPLICATION, ReplicationType.SYNC);
        List<WarmupLevel> warmupLevels = Lists.newArrayList(WarmupLevel.WARMUP_NOTHING, WarmupLevel.WARMUP_ALL);
        Map<Long, Long> workerGroupId2WorkerId = new HashMap<>();
        // Create 4 workerGroups
        for (WarmupLevel level : warmupLevels) {
            for (ReplicationType replicationType : replicationTypes) {
                long workerGroupId = helper.createWorkerGroup(1);
                helper.setWorkerGroupReplicationType(workerGroupId, replicationType);
                helper.setWorkerGroupWarmupLevel(workerGroupId, level);
                // Add a worker into the workerGroup
                long workerId = helper.createTestWorkerToWorkerGroup(workerGroupId);
                workerGroupId2WorkerId.put(workerGroupId, workerId);
                // add an OK replica to the shard on the worker
                shard.addReplica(workerId);
            }
        }

        // Schedule the shard into the 4 workerGroups with different configurations and validate the result.
        for (long workerId : workerGroupId2WorkerId.values()) {
            scheduler.scheduleAddToWorker(helper.getDefaultServiceId(), shardId, workerId);
        }

        // Verify result
        for (Map.Entry<Long, Long> entry : workerGroupId2WorkerId.entrySet()) {
            long workerGroupId = entry.getKey();
            long workerId = entry.getValue();
            Assert.assertTrue(shard.hasReplica(workerId));
            AddShardInfo info = getAddShardInfoFromStarletAgent(workerId, shard.getShardId());
            WorkerGroup wg =
                    helper.getWorkerManager().getWorkerGroupNoException(helper.getDefaultServiceId(), workerGroupId);
            ReplicationType replicationType = wg.getReplicationType();
            WarmupLevel warmupLevel = wg.getWarmupLevel();
            String msg = String.format("ReplicationType=%s, WarmupLevel=%s", replicationType, warmupLevel);

            Assert.assertEquals(msg, 1L, info.getReplicaInfoCount());
            // ReplicaInfo to starlet side is always the same as the one in Shard's replica
            ReplicaInfoLite infoLite = info.getReplicaInfo(0);
            Assert.assertEquals(workerId, infoLite.getWorkerId());
            Assert.assertEquals(ReplicaState.REPLICA_OK, infoLite.getReplicaState());
        }
    }
}
