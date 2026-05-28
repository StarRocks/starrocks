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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/load/routineload/RoutineLoadTaskSchedulerTest.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.load.routineload;

import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.starrocks.common.Config;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TRoutineLoadTask;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class RoutineLoadTaskSchedulerTest {

    @Mocked
    private RoutineLoadMgr routineLoadManager;

    @BeforeEach
    public void setUp() {
        UtFrameUtils.setUpForPersistTest();
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testRunOneCycle(@Injectable KafkaRoutineLoadJob kafkaRoutineLoadJob1,
                                @Injectable KafkaRoutineLoadJob routineLoadJob) {
        long beId = 100L;

        Map<Integer, Long> partitionIdToOffset = Maps.newHashMap();
        partitionIdToOffset.put(1, 100L);
        partitionIdToOffset.put(2, 200L);
        KafkaProgress kafkaProgress = new KafkaProgress();
        Deencapsulation.setField(kafkaProgress, "partitionIdToOffset", partitionIdToOffset);

        Queue<RoutineLoadTaskInfo> routineLoadTaskInfoQueue = Queues.newLinkedBlockingQueue();
        KafkaTaskInfo routineLoadTaskInfo1 = new KafkaTaskInfo(new UUID(1, 1), kafkaRoutineLoadJob1, 20000,
                System.currentTimeMillis(), partitionIdToOffset, Config.routine_load_task_timeout_second * 1000);
        routineLoadTaskInfoQueue.add(routineLoadTaskInfo1);

        Map<Long, RoutineLoadTaskInfo> idToRoutineLoadTask = Maps.newHashMap();
        idToRoutineLoadTask.put(1L, routineLoadTaskInfo1);

        Map<String, RoutineLoadJob> idToRoutineLoadJob = Maps.newConcurrentMap();
        idToRoutineLoadJob.put("1", routineLoadJob);

        Deencapsulation.setField(routineLoadManager, "idToRoutineLoadJob", idToRoutineLoadJob);

        GlobalStateMgr.getCurrentState().setRoutineLoadMgr(routineLoadManager);

        new Expectations() {
            {
                routineLoadManager.getClusterIdleSlotNum();
                minTimes = 0;
                result = 1;
                routineLoadManager.checkTaskInJob(anyLong, (UUID) any);
                minTimes = 0;
                result = true;

                kafkaRoutineLoadJob1.getDbId();
                minTimes = 0;
                result = 1L;
                kafkaRoutineLoadJob1.getTableId();
                minTimes = 0;
                result = 1L;
                kafkaRoutineLoadJob1.getName();
                minTimes = 0;
                result = "";
                routineLoadManager.getJob(anyLong);
                minTimes = 0;
                result = kafkaRoutineLoadJob1;
            }
        };

        RoutineLoadTaskScheduler routineLoadTaskScheduler = new RoutineLoadTaskScheduler();
        Deencapsulation.setField(routineLoadTaskScheduler, "needScheduleTasksQueue", routineLoadTaskInfoQueue);
        routineLoadTaskScheduler.runAfterLeaseValid();

        // The task was submitted to the thread pool waiting for execution, it could be some delay the task didn't get
        // executed at all when main thread thought the testing is done.
        // Fix it by waiting for a few seconds and shutdown the thread pool to make sure the task get executed.
        ExecutorService executor = Deencapsulation.getField(routineLoadTaskScheduler, "threadPool");
        executor.shutdown();
        Awaitility.await().timeout(5, TimeUnit.SECONDS).until(executor::isTerminated);
    }

    @Test
    public void testSchedulerOneTaskTxnNotFound() {

        KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob(1L, "job1", 1L, 1L, null, "topic1");

        Map<Integer, Long> partitionIdToOffset = Maps.newHashMap();
        partitionIdToOffset.put(1, 100L);
        partitionIdToOffset.put(2, 200L);
        KafkaProgress kafkaProgress = new KafkaProgress();
        Deencapsulation.setField(kafkaProgress, "partitionIdToOffset", partitionIdToOffset);

        Queue<RoutineLoadTaskInfo> routineLoadTaskInfoQueue = Queues.newLinkedBlockingQueue();
        KafkaTaskInfo routineLoadTaskInfo1 = new KafkaTaskInfo(new UUID(1, 1), routineLoadJob, 20000,
                System.currentTimeMillis(), partitionIdToOffset, Config.routine_load_task_timeout_second * 1000);
        routineLoadTaskInfoQueue.add(routineLoadTaskInfo1);

        Map<Long, RoutineLoadTaskInfo> idToRoutineLoadTask = Maps.newHashMap();
        idToRoutineLoadTask.put(1L, routineLoadTaskInfo1);

        Map<String, RoutineLoadJob> idToRoutineLoadJob = Maps.newConcurrentMap();
        idToRoutineLoadJob.put("1", routineLoadJob);

        Deencapsulation.setField(routineLoadManager, "idToRoutineLoadJob", idToRoutineLoadJob);

        GlobalStateMgr.getCurrentState().setRoutineLoadMgr(routineLoadManager);

        new Expectations() {
            {
                routineLoadManager.getClusterIdleSlotNum();
                minTimes = 0;
                result = 1;
                routineLoadManager.checkTaskInJob(anyLong, (UUID) any);
                minTimes = 0;
                result = true;

                routineLoadManager.getJob(anyLong);
                minTimes = 0;
                result = routineLoadJob;
            }
        };

        new MockUp<KafkaTaskInfo>() {
            @Mock
            public boolean readyToExecute() throws StarRocksException {
                return true;
            }

            @Mock
            public TRoutineLoadTask createRoutineLoadTask() throws StarRocksException {
                throw new StarRocksException("txn does not exist: 1");
            }
        };

        new MockUp<RoutineLoadTaskInfo>() {
            @Mock
            public void beginTxn() throws Exception {
                return;
            }

            @Mock
            public TRoutineLoadTask createRoutineLoadTask() throws StarRocksException {
                throw new StarRocksException("txn does not exist: 1");
            }
        };

        RoutineLoadTaskScheduler routineLoadTaskScheduler = new RoutineLoadTaskScheduler();

        Deencapsulation.setField(routineLoadTaskScheduler, "needScheduleTasksQueue", routineLoadTaskInfoQueue);
        try {
            routineLoadTaskScheduler.scheduleOneTask(routineLoadTaskInfo1);
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof StarRocksException);
            Assertions.assertEquals("txn does not exist: 1", e.getMessage());
            Assertions.assertEquals(RoutineLoadJob.JobState.PAUSED, routineLoadJob.state);
        }
    }

    @Test
    public void testSchedulerOneTaskDbNotFound() {

        KafkaRoutineLoadJob routineLoadJob = new KafkaRoutineLoadJob(1L, "job1", 1L, 1L, null, "topic1");

        Map<Integer, Long> partitionIdToOffset = Maps.newHashMap();
        partitionIdToOffset.put(1, 100L);
        partitionIdToOffset.put(2, 200L);
        KafkaProgress kafkaProgress = new KafkaProgress();
        Deencapsulation.setField(kafkaProgress, "partitionIdToOffset", partitionIdToOffset);

        Queue<RoutineLoadTaskInfo> routineLoadTaskInfoQueue = Queues.newLinkedBlockingQueue();
        KafkaTaskInfo routineLoadTaskInfo1 = new KafkaTaskInfo(new UUID(1, 1), routineLoadJob, 20000,
                System.currentTimeMillis(), partitionIdToOffset, Config.routine_load_task_timeout_second * 1000);
        routineLoadTaskInfoQueue.add(routineLoadTaskInfo1);

        Map<Long, RoutineLoadTaskInfo> idToRoutineLoadTask = Maps.newHashMap();
        idToRoutineLoadTask.put(1L, routineLoadTaskInfo1);

        Map<String, RoutineLoadJob> idToRoutineLoadJob = Maps.newConcurrentMap();
        idToRoutineLoadJob.put("1", routineLoadJob);

        Deencapsulation.setField(routineLoadManager, "idToRoutineLoadJob", idToRoutineLoadJob);

        GlobalStateMgr.getCurrentState().setRoutineLoadMgr(routineLoadManager);

        new Expectations() {
            {
                routineLoadManager.getClusterIdleSlotNum();
                minTimes = 0;
                result = 1;
                routineLoadManager.checkTaskInJob(anyLong, (UUID) any);
                minTimes = 0;
                result = true;

                routineLoadManager.getJob(anyLong);
                minTimes = 0;
                result = routineLoadJob;
            }
        };

        new MockUp<KafkaTaskInfo>() {
            @Mock
            public boolean readyToExecute() throws StarRocksException {
                return true;
            }

            @Mock
            public TRoutineLoadTask createRoutineLoadTask() throws StarRocksException {
                throw new MetaNotFoundException("database 1 does not exist");
            }
        };

        new MockUp<RoutineLoadTaskInfo>() {
            @Mock
            public void beginTxn() throws Exception {
                return;
            }

            @Mock
            public TRoutineLoadTask createRoutineLoadTask() throws StarRocksException {
                throw new MetaNotFoundException("database 1 does not exist");
            }
        };

        RoutineLoadTaskScheduler routineLoadTaskScheduler = new RoutineLoadTaskScheduler();

        Deencapsulation.setField(routineLoadTaskScheduler, "needScheduleTasksQueue", routineLoadTaskInfoQueue);
        try {
            routineLoadTaskScheduler.scheduleOneTask(routineLoadTaskInfo1);
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof MetaNotFoundException);
            Assertions.assertEquals("database 1 does not exist", e.getMessage());
            Assertions.assertEquals(RoutineLoadJob.JobState.CANCELLED, routineLoadJob.state);
        }
    }

    @Test
    public void testStartRefusesToRestartBeforeScheduledExecutorTerminates() {
        RoutineLoadTaskScheduler routineLoadTaskScheduler = new RoutineLoadTaskScheduler();
        ScheduledExecutorService blockedScheduler = Executors.newSingleThreadScheduledExecutor();
        blockedScheduler.execute(() -> {
            try {
                Thread.sleep(30_000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        blockedScheduler.shutdown();
        Deencapsulation.setField(routineLoadTaskScheduler, "scheduledExecutorService", blockedScheduler);

        try {
            Assertions.assertThrows(IllegalStateException.class, routineLoadTaskScheduler::start);
        } finally {
            blockedScheduler.shutdownNow();
        }
    }

    @Test
    public void testStartRefusesToRestartBeforeThreadPoolTerminates() {
        RoutineLoadTaskScheduler routineLoadTaskScheduler = new RoutineLoadTaskScheduler();
        ThreadPoolExecutor blockedPool = new ThreadPoolExecutor(
                1, 1, 0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1));
        blockedPool.execute(() -> {
            try {
                Thread.sleep(30_000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        blockedPool.shutdown();
        Deencapsulation.setField(routineLoadTaskScheduler, "threadPool", blockedPool);

        try {
            Assertions.assertThrows(IllegalStateException.class, routineLoadTaskScheduler::start);
        } finally {
            blockedPool.shutdownNow();
        }
    }

    @Test
    public void testOnStoppedShutsDownPoolsAndStartRebuildsThem() {
        // onStopped() must shutdownNow() both executors so their worker threads exit promptly
        // during the drain. A subsequent start() must rebuild both pools so the scheduler is
        // reusable when the FE is re-elected.
        RoutineLoadTaskScheduler scheduler = new RoutineLoadTaskScheduler();
        ScheduledExecutorService originalScheduler =
                Deencapsulation.getField(scheduler, "scheduledExecutorService");
        ExecutorService originalThreadPool = Deencapsulation.getField(scheduler, "threadPool");

        Deencapsulation.invoke(scheduler, "onStopped");

        Assertions.assertTrue(originalScheduler.isShutdown(),
                "scheduledExecutorService must be shut down on demotion");
        Assertions.assertTrue(originalThreadPool.isShutdown(),
                "threadPool must be shut down on demotion");
        Awaitility.await().timeout(5, TimeUnit.SECONDS).until(originalScheduler::isTerminated);
        Awaitility.await().timeout(5, TimeUnit.SECONDS).until(originalThreadPool::isTerminated);

        // The original pools are terminated, so start() should hit the rebuild branches.
        scheduler.start();
        ScheduledExecutorService rebuiltScheduler =
                Deencapsulation.getField(scheduler, "scheduledExecutorService");
        ExecutorService rebuiltThreadPool = Deencapsulation.getField(scheduler, "threadPool");
        Assertions.assertNotSame(originalScheduler, rebuiltScheduler,
                "scheduledExecutorService must be rebuilt on re-election");
        Assertions.assertNotSame(originalThreadPool, rebuiltThreadPool,
                "threadPool must be rebuilt on re-election");
        Assertions.assertFalse(rebuiltScheduler.isShutdown());
        Assertions.assertFalse(rebuiltThreadPool.isShutdown());

        scheduler.setStop();
    }

    @Test
    public void testOnStoppedClearsQueueAfterPoolsTerminate() {
        // Race fix: a delay-runnable scheduled by the previous leader can fire mid-onStopped()
        // and call needScheduleTasksQueue.put() before the interrupt from shutdownNow() lands.
        // If clear() runs before the pools drain, that stale put survives demotion and the
        // next leader polls it. Pin the invariant by routing the production clear() through a
        // queue subclass that records whether both pools were already terminated at the
        // moment clear() ran.
        RoutineLoadTaskScheduler scheduler = new RoutineLoadTaskScheduler();
        ScheduledExecutorService sched = Deencapsulation.getField(scheduler, "scheduledExecutorService");
        ExecutorService pool = Deencapsulation.getField(scheduler, "threadPool");
        boolean[] schedTerminatedAtClear = {false};
        boolean[] poolTerminatedAtClear = {false};
        LinkedBlockingQueue<RoutineLoadTaskInfo> trackingQueue = new LinkedBlockingQueue<RoutineLoadTaskInfo>() {
            @Override
            public void clear() {
                schedTerminatedAtClear[0] = sched.isTerminated();
                poolTerminatedAtClear[0] = pool.isTerminated();
                super.clear();
            }
        };
        Deencapsulation.setField(scheduler, "needScheduleTasksQueue", trackingQueue);

        Deencapsulation.invoke(scheduler, "onStopped");

        Assertions.assertTrue(schedTerminatedAtClear[0],
                "scheduledExecutorService must be terminated before clear() runs");
        Assertions.assertTrue(poolTerminatedAtClear[0],
                "threadPool must be terminated before clear() runs");
        Long watermark = Deencapsulation.getField(scheduler, "lastBackendSlotUpdateTime");
        Assertions.assertEquals(-1L, watermark.longValue(),
                "slot watermark must be reset for the next leader");
    }

    @Test
    public void testDelayPutToQueueSkipsWhenStopped() throws Exception {
        // After onStopped() shuts down scheduledExecutorService, delayPutToQueue must skip
        // cleanly instead of throwing - the next leader will re-divide the routine load.
        RoutineLoadTaskScheduler scheduler = new RoutineLoadTaskScheduler();
        ScheduledExecutorService originalScheduler =
                Deencapsulation.getField(scheduler, "scheduledExecutorService");
        Deencapsulation.invoke(scheduler, "onStopped");
        Awaitility.await().timeout(5, TimeUnit.SECONDS).until(originalScheduler::isTerminated);

        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(1L, "job1", 1L, 1L, null, "topic1");
        KafkaTaskInfo taskInfo = new KafkaTaskInfo(new UUID(1, 1), job, 20000,
                System.currentTimeMillis(), Maps.newHashMap(), Config.routine_load_task_timeout_second * 1000);

        // delayPutToQueue is private; invoke through Deencapsulation. Must not throw.
        Deencapsulation.invoke(scheduler, "delayPutToQueue", taskInfo, "test-msg");
    }

    @Test
    public void testSubmitToScheduleSkipsWhenStopped() throws Exception {
        // Mirror of the previous test: submitToSchedule's stopped-guard short-circuits when
        // the threadPool was shut down, preventing RejectedExecutionException leakage.
        RoutineLoadTaskScheduler scheduler = new RoutineLoadTaskScheduler();
        ExecutorService originalThreadPool = Deencapsulation.getField(scheduler, "threadPool");
        Deencapsulation.invoke(scheduler, "onStopped");
        Awaitility.await().timeout(5, TimeUnit.SECONDS).until(originalThreadPool::isTerminated);

        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(1L, "job1", 1L, 1L, null, "topic1");
        KafkaTaskInfo taskInfo = new KafkaTaskInfo(new UUID(1, 1), job, 20000,
                System.currentTimeMillis(), Maps.newHashMap(), Config.routine_load_task_timeout_second * 1000);

        Deencapsulation.invoke(scheduler, "submitToSchedule", taskInfo);
    }
}
