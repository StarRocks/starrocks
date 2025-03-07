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
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class RoutineLoadTaskSchedulerTest {

    @Mocked
    private RoutineLoadMgr routineLoadManager;
    @Mocked
    private GlobalStateMgr globalStateMgr;

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

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;
                globalStateMgr.getRoutineLoadMgr();
                minTimes = 0;
                result = routineLoadManager;

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
        routineLoadTaskScheduler.runAfterCatalogReady();

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

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;
                globalStateMgr.getRoutineLoadMgr();
                minTimes = 0;
                result = routineLoadManager;

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
            Assert.assertTrue(e instanceof StarRocksException);
            Assert.assertEquals("txn does not exist: 1", e.getMessage());
            Assert.assertEquals(RoutineLoadJob.JobState.PAUSED, routineLoadJob.state);
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

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;
                globalStateMgr.getRoutineLoadMgr();
                minTimes = 0;
                result = routineLoadManager;

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
            Assert.assertTrue(e instanceof MetaNotFoundException);
            Assert.assertEquals("database 1 does not exist", e.getMessage());
            Assert.assertEquals(RoutineLoadJob.JobState.CANCELLED, routineLoadJob.state);
        }
    }
}
