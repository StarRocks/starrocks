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
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.server.GlobalStateMgr;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Test;

import java.util.Map;
import java.util.Queue;
import java.util.UUID;

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
        KafkaTaskInfo routineLoadTaskInfo1 = new KafkaTaskInfo(new UUID(1, 1), 1L, 20000,
                System.currentTimeMillis(), partitionIdToOffset);
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
                routineLoadManager.checkTaskInJob((UUID) any);
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
    }
}
