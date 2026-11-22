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

package com.starrocks.load.routineload;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.InternalErrorCode;
import com.starrocks.common.LoadException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.KafkaUtil;
import com.starrocks.common.util.PulsarUtil;
import com.starrocks.epack.warehouse.LocalWarehouse;
import com.starrocks.epack.warehouse.WarehouseManagerEPack;
import com.starrocks.epack.warehouse.cngroup.CNGroupResource;
import com.starrocks.epack.warehouse.cngroup.CNGroupResourceProvider;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.Warehouse;
import com.starrocks.warehouse.cngroup.CRAcquireContext;
import com.starrocks.warehouse.cngroup.ComputeResource;
import com.starrocks.warehouse.cngroup.WarehouseComputeResourceProvider;
import mockit.Injectable;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A RUNNING Kafka routine load job is rescheduled onto another CN group of its warehouse when no compute node of its
 * own CN group is alive. The CN groups, their liveness and the Kafka brokers are faked; update(), prepare() and the
 * scheduler rounds of the jobs are real.
 */
public class RoutineLoadJobCnGroupSwitchTest {
    private static final long TABLE_ID = 22L;
    private static final long JOB_GROUP = 20L;
    private static final long OTHER_GROUP = 21L;
    private static final long THIRD_GROUP = 23L;

    // worker group ids of the CN groups that have an alive compute node
    private final Set<Long> aliveGroups = new HashSet<>();
    // worker group ids of the CN groups that DISABLE CNGROUP disabled
    private final Set<Long> disabledGroups = new HashSet<>();
    // worker group ids of the CN groups whose compute nodes cannot reach the Kafka brokers
    private final Set<Long> brokerUnreachableGroups = new HashSet<>();
    // the partitions that the brokers report for a topic
    private final List<Integer> topicPartitions = new ArrayList<>();
    // the number of partition list requests sent towards the brokers
    private int partitionFetches;
    // the previous compute resource passed to every acquisition
    private final List<ComputeResource> acquisitions = new ArrayList<>();
    // the number of times the liveness of a CN group was looked up
    private int availabilityLookups;
    private boolean sharedData;
    private boolean acquisitionKeepsPrevious;

    @BeforeEach
    public void setUp() {
        UtFrameUtils.setUpForPersistTest();
        aliveGroups.clear();
        disabledGroups.clear();
        brokerUnreachableGroups.clear();
        topicPartitions.clear();
        topicPartitions.addAll(List.of(0, 1, 2));
        partitionFetches = 0;
        acquisitions.clear();
        availabilityLookups = 0;
        sharedData = true;
        acquisitionKeepsPrevious = false;

        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return sharedData ? RunMode.SHARED_DATA : RunMode.SHARED_NOTHING;
            }
        };
        new MockUp<WarehouseManager>() {
            @Mock
            public boolean isResourceAvailable(ComputeResource computeResource) {
                availabilityLookups++;
                return aliveGroups.contains(computeResource.getWorkerGroupId());
            }

            // like the CN group provider: keep the previous CN group while it has an alive compute node, else take
            // another one that has, enabled or not, else fail
            @Mock
            public ComputeResource acquireComputeResource(CRAcquireContext acquireContext) {
                ComputeResource previous = acquireContext.getPrevComputeResource();
                acquisitions.add(previous);
                if (previous != null
                        && (acquisitionKeepsPrevious || aliveGroups.contains(previous.getWorkerGroupId()))) {
                    return CNGroupResource.of(acquireContext.getWarehouseId(), previous.getWorkerGroupId());
                }
                for (long group : new long[] {JOB_GROUP, OTHER_GROUP, THIRD_GROUP}) {
                    if (aliveGroups.contains(group)) {
                        return CNGroupResource.of(acquireContext.getWarehouseId(), group);
                    }
                }
                throw ErrorReportException.report(ErrorCode.ERR_WAREHOUSE_UNAVAILABLE,
                        WarehouseManager.DEFAULT_WAREHOUSE_NAME);
            }

            @Mock
            public boolean isComputeResourceDisabled(ComputeResource computeResource) {
                return disabledGroups.contains(computeResource.getWorkerGroupId());
            }

            @Mock
            public Warehouse getWarehouse(long warehouseId) {
                return new LocalWarehouse(warehouseId, WarehouseManager.DEFAULT_WAREHOUSE_NAME, null, "");
            }
        };
        new MockUp<WarehouseManagerEPack>() {
            @Mock
            public boolean isComputeResourceDisabled(ComputeResource computeResource) {
                return disabledGroups.contains(computeResource.getWorkerGroupId());
            }
        };
        new MockUp<CNGroupResourceProvider>() {
            @Mock
            public List<ComputeResource> getComputeResources(Warehouse warehouse) {
                return aliveComputeResources();
            }
        };
        new MockUp<WarehouseComputeResourceProvider>() {
            @Mock
            public List<ComputeResource> getComputeResources(Warehouse warehouse) {
                return aliveComputeResources();
            }
        };
        // the topics keep the partitions of the jobs unless a test adds one, so the partition refresh does not
        // reschedule a job by itself
        new MockUp<KafkaUtil>() {
            @Mock
            public List<Integer> getAllKafkaPartitions(String brokerList, String topic,
                                                       ImmutableMap<String, String> properties,
                                                       ComputeResource computeResource) throws StarRocksException {
                partitionFetches++;
                if (brokerUnreachableGroups.contains(computeResource.getWorkerGroupId())) {
                    throw new LoadException("failed to get the partitions of topic " + topic);
                }
                return new ArrayList<>(topicPartitions);
            }
        };
        new MockUp<PulsarUtil>() {
            @Mock
            public List<String> getAllPulsarPartitions(String serviceUrl, String topic, String subscription,
                                                      ImmutableMap<String, String> properties,
                                                      ComputeResource computeResource) {
                return Lists.newArrayList("p1", "p2");
            }
        };
        // a scheduler round divides every Kafka job into one task, whatever the compute nodes of its CN group
        new MockUp<KafkaRoutineLoadJob>() {
            @Mock
            public int calculateCurrentConcurrentTaskNum() {
                return 1;
            }
        };
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    private List<ComputeResource> aliveComputeResources() {
        List<ComputeResource> groups = new ArrayList<>();
        for (long group : aliveGroups) {
            groups.add(CNGroupResource.of(WarehouseManager.DEFAULT_WAREHOUSE_ID, group));
        }
        return groups;
    }

    private static void createTable(long dbId) {
        Database database = new Database(dbId, "testDb" + dbId);
        database.registerTableUnlocked(new OlapTable(TABLE_ID, "test", null, null, null, null));
        GlobalStateMgr.getCurrentState().getLocalMetastore().replayCreateDb(database);
    }

    private static void setRunningOnJobGroup(RoutineLoadJob job, RoutineLoadProgress progress) {
        Deencapsulation.setField(job, "state", RoutineLoadJob.JobState.RUNNING);
        Deencapsulation.setField(job, "computeResource",
                CNGroupResource.of(WarehouseManager.DEFAULT_WAREHOUSE_ID, JOB_GROUP));
        Deencapsulation.setField(job, "progress", progress);
    }

    private static KafkaRoutineLoadJob runningKafkaJob(long dbId, KafkaProgress progress) {
        createTable(dbId);
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(dbId, "job" + dbId, dbId, TABLE_ID, "brokerList", "topic");
        setRunningOnJobGroup(job, progress);
        Deencapsulation.setField(job, "currentKafkaPartitions", Lists.newArrayList(0, 1, 2));
        return job;
    }

    private static RoutineLoadMgr managerOf(RoutineLoadJob... jobs) {
        RoutineLoadMgr routineLoadMgr = new RoutineLoadMgr();
        Map<Long, RoutineLoadJob> idToJob = Deencapsulation.getField(routineLoadMgr, "idToRoutineLoadJob");
        for (RoutineLoadJob job : jobs) {
            idToJob.put(job.getId(), job);
        }
        return routineLoadMgr;
    }

    // One scheduler round: update() of every job, then prepare() and the division into tasks of the jobs that
    // need to be scheduled
    private static void runSchedulerRound(RoutineLoadScheduler scheduler) {
        Deencapsulation.invoke(scheduler, "process");
    }

    // as if the delay after the job's last failure to be scheduled had passed
    private static void passRetryDelay(RoutineLoadJob job) {
        Deencapsulation.setField(job, "rescheduleRetryAtMs", 0L);
    }

    private static List<RoutineLoadTaskInfo> tasksOf(RoutineLoadJob job) {
        return Deencapsulation.getField(job, "routineLoadTaskInfoList");
    }

    @Test
    public void testRescheduleWhenCnGroupHasNoAliveComputeNode(@Injectable KafkaProgress progress)
            throws StarRocksException {
        aliveGroups.add(OTHER_GROUP);
        KafkaRoutineLoadJob job = runningKafkaJob(61L, progress);

        job.update();

        // back in NEED_SCHEDULE, with the reason in OtherMsg; the compute resource is still prepare()'s to pick
        Assertions.assertEquals(RoutineLoadJob.JobState.NEED_SCHEDULE, job.getState());
        Assertions.assertEquals(JOB_GROUP, job.getComputeResource().getWorkerGroupId());
        Assertions.assertTrue(job.getOtherMsg().contains("worker group id " + JOB_GROUP), job.getOtherMsg());

        job.prepare();
        Assertions.assertEquals(OTHER_GROUP, job.getComputeResource().getWorkerGroupId());

        // divided into tasks again; the job stays on the new CN group when the old one recovers
        Deencapsulation.setField(job, "state", RoutineLoadJob.JobState.RUNNING);
        aliveGroups.add(JOB_GROUP);
        job.update();
        Assertions.assertEquals(RoutineLoadJob.JobState.RUNNING, job.getState());
        Assertions.assertEquals(OTHER_GROUP, job.getComputeResource().getWorkerGroupId());
    }

    @Test
    public void testNoRescheduleWhileCnGroupHasAliveComputeNode(@Injectable KafkaProgress progress)
            throws StarRocksException {
        aliveGroups.add(JOB_GROUP);
        aliveGroups.add(OTHER_GROUP);
        KafkaRoutineLoadJob job = runningKafkaJob(62L, progress);

        job.update();

        Assertions.assertEquals(RoutineLoadJob.JobState.RUNNING, job.getState());
        Assertions.assertEquals(JOB_GROUP, job.getComputeResource().getWorkerGroupId());
        Assertions.assertTrue(acquisitions.isEmpty());
    }

    @Test
    public void testRescheduleOnceAnotherCnGroupComesBack(@Injectable KafkaProgress progress)
            throws StarRocksException {
        KafkaRoutineLoadJob job = runningKafkaJob(63L, progress);

        // no CN group of the warehouse has an alive compute node: the job stays as it is
        job.update();
        Assertions.assertEquals(RoutineLoadJob.JobState.RUNNING, job.getState());
        Assertions.assertEquals(JOB_GROUP, job.getComputeResource().getWorkerGroupId());

        // another CN group comes back before the job's own one: the next tick moves the job
        aliveGroups.add(OTHER_GROUP);
        job.update();
        Assertions.assertEquals(RoutineLoadJob.JobState.NEED_SCHEDULE, job.getState());
        Assertions.assertTrue(job.getOtherMsg().contains("worker group id " + JOB_GROUP), job.getOtherMsg());
        job.prepare();
        Assertions.assertEquals(OTHER_GROUP, job.getComputeResource().getWorkerGroupId());
    }

    @Test
    public void testNoRescheduleWhenAcquisitionKeepsTheCnGroup(@Injectable KafkaProgress progress)
            throws StarRocksException {
        // the acquisition sees the job's CN group as usable again, e.g. a compute node came back in between
        acquisitionKeepsPrevious = true;
        KafkaRoutineLoadJob job = runningKafkaJob(64L, progress);

        job.rescheduleIfComputeResourceUnavailable(new RoutineLoadJob.CnGroupView());

        Assertions.assertEquals(RoutineLoadJob.JobState.RUNNING, job.getState());
        Assertions.assertEquals(1, acquisitions.size());
    }

    @Test
    public void testOnlyRunningJobsAreRescheduled(@Injectable KafkaProgress progress) throws StarRocksException {
        aliveGroups.add(OTHER_GROUP);
        KafkaRoutineLoadJob job = runningKafkaJob(65L, progress);
        for (RoutineLoadJob.JobState state : new RoutineLoadJob.JobState[] {RoutineLoadJob.JobState.NEED_SCHEDULE,
                RoutineLoadJob.JobState.PAUSED, RoutineLoadJob.JobState.STOPPED, RoutineLoadJob.JobState.CANCELLED}) {
            Deencapsulation.setField(job, "state", state);
            job.rescheduleIfComputeResourceUnavailable(new RoutineLoadJob.CnGroupView());
            Assertions.assertEquals(state, job.getState());
        }
        Assertions.assertTrue(acquisitions.isEmpty());
    }

    @Test
    public void testNoRescheduleInSharedNothingMode(@Injectable KafkaProgress progress) throws StarRocksException {
        sharedData = false;
        aliveGroups.add(OTHER_GROUP);
        KafkaRoutineLoadJob job = runningKafkaJob(66L, progress);

        job.rescheduleIfComputeResourceUnavailable(new RoutineLoadJob.CnGroupView());

        Assertions.assertEquals(RoutineLoadJob.JobState.RUNNING, job.getState());
        Assertions.assertTrue(acquisitions.isEmpty());
    }

    @Test
    public void testPulsarJobKeepsItsCnGroup(@Injectable PulsarProgress progress) throws StarRocksException {
        // the tasks of a Pulsar job on a CN group with fewer compute nodes could get more partitions than a BE takes
        aliveGroups.add(OTHER_GROUP);
        createTable(67L);
        PulsarRoutineLoadJob job = new PulsarRoutineLoadJob(67L, "job67", 67L, TABLE_ID,
                "http://pulsar-service", "topic1", "sub1");
        setRunningOnJobGroup(job, progress);
        Deencapsulation.setField(job, "currentPulsarPartitions", Lists.newArrayList("p1", "p2"));

        job.update();

        Assertions.assertEquals(RoutineLoadJob.JobState.RUNNING, job.getState());
        Assertions.assertEquals(JOB_GROUP, job.getComputeResource().getWorkerGroupId());
    }

    @Test
    public void testRescheduledJobWaitsForAnotherCnGroup(@Injectable KafkaProgress progress)
            throws StarRocksException {
        aliveGroups.add(OTHER_GROUP);
        KafkaRoutineLoadJob job = runningKafkaJob(68L, progress);
        RoutineLoadScheduler scheduler = new RoutineLoadScheduler(managerOf(job));
        job.update();
        Assertions.assertEquals(RoutineLoadJob.JobState.NEED_SCHEDULE, job.getState());

        // the other CN group fails as well before the job is scheduled: neither the partition refresh nor prepare()
        // of the next round gets a CN group, and the job waits instead of pausing
        aliveGroups.clear();
        runSchedulerRound(scheduler);
        Assertions.assertEquals(RoutineLoadJob.JobState.NEED_SCHEDULE, job.getState());
        Assertions.assertEquals(JOB_GROUP, job.getComputeResource().getWorkerGroupId());
        Assertions.assertTrue(job.getOtherMsg().contains("retry in 10 s"), job.getOtherMsg());

        // the other CN group comes back: once the delay has passed, a round divides the job into tasks on it
        aliveGroups.add(OTHER_GROUP);
        passRetryDelay(job);
        runSchedulerRound(scheduler);
        Assertions.assertEquals(RoutineLoadJob.JobState.RUNNING, job.getState());
        Assertions.assertEquals(OTHER_GROUP, job.getComputeResource().getWorkerGroupId());
        Assertions.assertEquals(1, tasksOf(job).size());
        Assertions.assertEquals(OTHER_GROUP, tasksOf(job).get(0).getComputeResource().getWorkerGroupId());
        Assertions.assertFalse(job.isWaitingForAnotherCnGroup());
    }

    @Test
    public void testRescheduledJobRetriesUntilItGetsTasks(@Injectable KafkaProgress progress)
            throws StarRocksException {
        aliveGroups.add(OTHER_GROUP);
        brokerUnreachableGroups.add(JOB_GROUP);
        brokerUnreachableGroups.add(OTHER_GROUP);
        KafkaRoutineLoadJob job = runningKafkaJob(69L, progress);
        // prepare() checks the partitions given at creation against the brokers through the job's CN group
        Deencapsulation.setField(job, "customKafkaPartitions", Lists.newArrayList(0, 1, 2));
        RoutineLoadScheduler scheduler = new RoutineLoadScheduler(managerOf(job));

        // the round moves the job, and its partition check through the other CN group fails: the job waits, with
        // its compute resource back on its old CN group
        runSchedulerRound(scheduler);
        Assertions.assertEquals(RoutineLoadJob.JobState.NEED_SCHEDULE, job.getState());
        Assertions.assertEquals(JOB_GROUP, job.getComputeResource().getWorkerGroupId());
        Assertions.assertTrue(job.getOtherMsg().contains("retry in 10 s"), job.getOtherMsg());
        Assertions.assertTrue(job.getOtherMsg().contains("failed to get the partitions"), job.getOtherMsg());

        // within the delay a round does not prepare the job
        int numAcquisitions = acquisitions.size();
        runSchedulerRound(scheduler);
        Assertions.assertEquals(numAcquisitions, acquisitions.size());
        Assertions.assertEquals(RoutineLoadJob.JobState.NEED_SCHEDULE, job.getState());

        // the old CN group comes back while the brokers are still unreachable: the job keeps waiting, with a longer
        // delay, where a RUNNING job left on that group would have kept retrying its offset requests
        aliveGroups.add(JOB_GROUP);
        passRetryDelay(job);
        runSchedulerRound(scheduler);
        Assertions.assertEquals(RoutineLoadJob.JobState.NEED_SCHEDULE, job.getState());
        Assertions.assertTrue(job.getOtherMsg().contains("retry in 20 s"), job.getOtherMsg());

        // the brokers answer again: the job is divided into tasks on its old CN group
        brokerUnreachableGroups.clear();
        passRetryDelay(job);
        runSchedulerRound(scheduler);
        Assertions.assertEquals(RoutineLoadJob.JobState.RUNNING, job.getState());
        Assertions.assertEquals(JOB_GROUP, job.getComputeResource().getWorkerGroupId());
        Assertions.assertFalse(job.isWaitingForAnotherCnGroup());
    }

    @Test
    public void testMovedJobWaitsWhileItsNewCnGroupCannotReachTheBrokers(@Injectable KafkaProgress progress)
            throws StarRocksException {
        // the compute nodes of the other CN group are alive but cannot reach the brokers
        aliveGroups.add(OTHER_GROUP);
        brokerUnreachableGroups.add(OTHER_GROUP);
        KafkaRoutineLoadJob job = runningKafkaJob(70L, progress);
        RoutineLoadScheduler scheduler = new RoutineLoadScheduler(managerOf(job));

        // the job moves, but the brokers do not answer through the other CN group: it waits instead of running there
        runSchedulerRound(scheduler);
        Assertions.assertEquals(RoutineLoadJob.JobState.NEED_SCHEDULE, job.getState());
        Assertions.assertEquals(JOB_GROUP, job.getComputeResource().getWorkerGroupId());
        Assertions.assertTrue(tasksOf(job).isEmpty());

        // its own CN group comes back: the job is divided into tasks there
        aliveGroups.add(JOB_GROUP);
        passRetryDelay(job);
        runSchedulerRound(scheduler);
        Assertions.assertEquals(RoutineLoadJob.JobState.RUNNING, job.getState());
        Assertions.assertEquals(JOB_GROUP, job.getComputeResource().getWorkerGroupId());
        Assertions.assertEquals(1, tasksOf(job).size());
    }

    @Test
    public void testMovedJobSkipsADisabledCnGroup(@Injectable KafkaProgress progress) throws StarRocksException {
        // the acquisition gives the other CN group, which is disabled while its compute nodes stay alive
        aliveGroups.add(OTHER_GROUP);
        aliveGroups.add(THIRD_GROUP);
        disabledGroups.add(OTHER_GROUP);
        KafkaRoutineLoadJob job = runningKafkaJob(71L, progress);

        job.update();
        Assertions.assertEquals(RoutineLoadJob.JobState.NEED_SCHEDULE, job.getState());
        job.prepare();
        Assertions.assertEquals(THIRD_GROUP, job.getComputeResource().getWorkerGroupId());

        // when the disabled CN group is the only one with an alive compute node, the job does not move
        aliveGroups.remove(THIRD_GROUP);
        KafkaRoutineLoadJob another = runningKafkaJob(72L, progress);
        another.update();
        Assertions.assertEquals(RoutineLoadJob.JobState.RUNNING, another.getState());
        Assertions.assertEquals(JOB_GROUP, another.getComputeResource().getWorkerGroupId());
    }

    @Test
    public void testPartitionChangeInTheRoundOfTheMoveKeepsTheWait(@Injectable KafkaProgress progress)
            throws StarRocksException {
        // the acquisition gives the other CN group, which is disabled; the third one cannot reach the brokers yet
        aliveGroups.add(OTHER_GROUP);
        aliveGroups.add(THIRD_GROUP);
        disabledGroups.add(OTHER_GROUP);
        brokerUnreachableGroups.add(THIRD_GROUP);
        KafkaRoutineLoadJob job = runningKafkaJob(76L, progress);
        RoutineLoadScheduler scheduler = new RoutineLoadScheduler(managerOf(job));

        // the round that moves the job also finds a new partition, which reschedules the job as well: the job still
        // waits for another CN group, so prepare() skips the disabled CN group, and the failed check through the
        // third CN group does not pause the job
        topicPartitions.add(3);
        runSchedulerRound(scheduler);
        Assertions.assertEquals(RoutineLoadJob.JobState.NEED_SCHEDULE, job.getState());
        Assertions.assertTrue(job.isWaitingForAnotherCnGroup());
        Assertions.assertEquals(JOB_GROUP, job.getComputeResource().getWorkerGroupId());
        Assertions.assertTrue(job.getOtherMsg().contains("retry in 10 s"), job.getOtherMsg());
        Assertions.assertEquals(List.of(0, 1, 2, 3), Deencapsulation.getField(job, "currentKafkaPartitions"));

        // the third CN group reaches the brokers: the job is divided into tasks there, not on the disabled one
        brokerUnreachableGroups.clear();
        passRetryDelay(job);
        runSchedulerRound(scheduler);
        Assertions.assertEquals(RoutineLoadJob.JobState.RUNNING, job.getState());
        Assertions.assertEquals(THIRD_GROUP, job.getComputeResource().getWorkerGroupId());
    }

    @Test
    public void testNoBrokerRequestWithinTheRetryDelay(@Injectable KafkaProgress progress) throws StarRocksException {
        // the job finds its partitions itself, and the other CN group cannot reach the brokers
        aliveGroups.add(OTHER_GROUP);
        brokerUnreachableGroups.add(OTHER_GROUP);
        KafkaRoutineLoadJob job = runningKafkaJob(77L, progress);
        RoutineLoadScheduler scheduler = new RoutineLoadScheduler(managerOf(job));
        runSchedulerRound(scheduler);
        Assertions.assertTrue(job.isRescheduleRetryDelayed(System.currentTimeMillis()));

        // within the delay, neither the partition refresh nor prepare() sends a request for the job
        partitionFetches = 0;
        runSchedulerRound(scheduler);
        runSchedulerRound(scheduler);
        Assertions.assertEquals(0, partitionFetches);
        Assertions.assertEquals(RoutineLoadJob.JobState.NEED_SCHEDULE, job.getState());

        // once the delay has passed, the refresh and prepare() of a round try the brokers again, and the next delay
        // is twice as long
        passRetryDelay(job);
        runSchedulerRound(scheduler);
        Assertions.assertEquals(2, partitionFetches);
        Assertions.assertTrue(job.getOtherMsg().contains("retry in 20 s"), job.getOtherMsg());
    }

    @Test
    public void testDelayEndingWhileJobsAreUpdatedWaitsForTheNextRound(@Injectable KafkaProgress progress)
            throws StarRocksException {
        aliveGroups.add(OTHER_GROUP);
        brokerUnreachableGroups.add(OTHER_GROUP);
        KafkaRoutineLoadJob job = runningKafkaJob(78L, progress);
        RoutineLoadScheduler scheduler = new RoutineLoadScheduler(managerOf(job));
        runSchedulerRound(scheduler);
        Assertions.assertTrue(job.isRescheduleRetryDelayed(System.currentTimeMillis()));

        // while the job waits, the brokers answer again and the topic gets a partition; the delay of the job ends
        // after update() skipped the partition refresh of the round, before the scheduler looks at the job
        brokerUnreachableGroups.clear();
        topicPartitions.add(3);
        AtomicBoolean delayEndsAfterUpdate = new AtomicBoolean(true);
        new MockUp<RoutineLoadMgr>() {
            @Mock
            public void updateRoutineLoadJob(Invocation invocation) throws InterruptedException {
                invocation.proceed();
                if (delayEndsAfterUpdate.getAndSet(false)) {
                    Thread.sleep(2);
                    Deencapsulation.setField(job, "rescheduleRetryAtMs", System.currentTimeMillis());
                }
            }
        };

        // the round does not divide the job into tasks with the partitions from before its wait
        runSchedulerRound(scheduler);
        Assertions.assertEquals(RoutineLoadJob.JobState.NEED_SCHEDULE, job.getState());
        Assertions.assertEquals(List.of(0, 1, 2), Deencapsulation.getField(job, "currentKafkaPartitions"));

        // the next round refreshes the partitions first and divides the job into tasks with the new one
        runSchedulerRound(scheduler);
        Assertions.assertEquals(RoutineLoadJob.JobState.RUNNING, job.getState());
        Assertions.assertEquals(List.of(0, 1, 2, 3), Deencapsulation.getField(job, "currentKafkaPartitions"));
    }

    @Test
    public void testPauseEndsTheWaitForAnotherCnGroup(@Injectable KafkaProgress progress) throws StarRocksException {
        aliveGroups.add(OTHER_GROUP);
        KafkaRoutineLoadJob job = runningKafkaJob(73L, progress);
        job.update();
        Assertions.assertEquals(RoutineLoadJob.JobState.NEED_SCHEDULE, job.getState());
        Assertions.assertTrue(job.isWaitingForAnotherCnGroup());

        // paused and resumed while it waits, the job is scheduled as any other: a failure pauses it
        job.updateState(RoutineLoadJob.JobState.PAUSED, new ErrorReason(InternalErrorCode.MANUAL_PAUSE_ERR, "pause"));
        job.updateState(RoutineLoadJob.JobState.NEED_SCHEDULE, null);
        Assertions.assertFalse(job.isWaitingForAnotherCnGroup());
        Assertions.assertFalse(job.retryRescheduleLater("failure"));
        Assertions.assertEquals(RoutineLoadJob.JobState.NEED_SCHEDULE, job.getState());
    }

    @Test
    public void testCnGroupIsLookedUpOncePerRound(@Injectable KafkaProgress progress) throws StarRocksException {
        aliveGroups.add(JOB_GROUP);
        aliveGroups.add(OTHER_GROUP);
        KafkaRoutineLoadJob job1 = runningKafkaJob(74L, progress);
        KafkaRoutineLoadJob job2 = runningKafkaJob(75L, progress);
        for (KafkaRoutineLoadJob job : List.of(job1, job2)) {
            // with partitions given at creation the partition refresh makes no lookup of its own
            Deencapsulation.setField(job, "customKafkaPartitions", Lists.newArrayList(0, 1, 2));
        }
        RoutineLoadMgr routineLoadMgr = managerOf(job1, job2);

        routineLoadMgr.updateRoutineLoadJob();
        Assertions.assertEquals(1, availabilityLookups);
        Assertions.assertEquals(RoutineLoadJob.JobState.RUNNING, job1.getState());
        Assertions.assertEquals(RoutineLoadJob.JobState.RUNNING, job2.getState());

        // the CN group loses its last alive compute node: both jobs leave it after one lookup of the group and one
        // acquisition of the CN group to move to
        aliveGroups.remove(JOB_GROUP);
        availabilityLookups = 0;
        acquisitions.clear();
        routineLoadMgr.updateRoutineLoadJob();
        Assertions.assertEquals(1, availabilityLookups);
        Assertions.assertEquals(1, acquisitions.size());
        Assertions.assertEquals(RoutineLoadJob.JobState.NEED_SCHEDULE, job1.getState());
        Assertions.assertEquals(RoutineLoadJob.JobState.NEED_SCHEDULE, job2.getState());
    }
}
