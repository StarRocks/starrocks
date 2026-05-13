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

package com.starrocks.load;

import com.starrocks.common.StarRocksException;
import com.starrocks.load.ExportJob.JobState;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

public class ExportCheckerTest {
    
    @Test
    public void testCheckBeStatus() throws NoSuchFieldException, SecurityException, 
        IllegalArgumentException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {

        new MockUp<ExportJob>() {
            @Mock
            public synchronized void cancel(ExportFailMsg.CancelType type, String msg) throws StarRocksException {
            }
        };

        Backend be = new Backend();
        
        new MockUp<SystemInfoService>() {
            @Mock
            public ComputeNode getBackendOrComputeNode(long backendId) {
                return be;
            }
        };

        ExportChecker.init(1000L);
        Field field = ExportChecker.class.getDeclaredField("checkers");
        field.setAccessible(true);
        Object obj = field.get(ExportChecker.class);
        
        Map<JobState, ExportChecker> map = (Map<JobState, ExportChecker>) obj;
        ExportChecker checker = map.get(JobState.EXPORTING);
        Method method = ExportChecker.class.getDeclaredMethod("checkJobNeedCancel", ExportJob.class);
        method.setAccessible(true);

        ExportJob job = new ExportJob();
        job.setBeStartTime(1, 1000L);
        boolean cancelled = (boolean) method.invoke(checker, job);
        Assertions.assertTrue(cancelled);

        be.setAlive(true);
        be.setDecommissioned(true);

        be.setLastStartTime(1001L);

        cancelled = (boolean) method.invoke(checker, job);
        Assertions.assertTrue(cancelled);

        be.setLastStartTime(999L);

        cancelled = (boolean) method.invoke(checker, job);
        Assertions.assertTrue(!cancelled);

        new MockUp<SystemInfoService>() {
            @Mock
            public ComputeNode getBackendOrComputeNode(long backendId) {
                return null;
            }
        };

        cancelled = (boolean) method.invoke(checker, job);
        Assertions.assertTrue(cancelled);
    }

    @Test
    public void testStopAllClosesExecutorsAndInitRebuilds() throws Exception {
        // stopAll must shut down all three LeaderTaskExecutor instances so their worker
        // threads exit on demotion; a subsequent init() must replace the static maps with
        // fresh instances for the next leader.
        ExportChecker.init(1000L);
        com.starrocks.task.LeaderTaskExecutor pendingBefore = readExecutor(JobState.PENDING);
        com.starrocks.task.LeaderTaskExecutor exportingBefore = readExecutor(JobState.EXPORTING);
        com.starrocks.task.LeaderTaskExecutor subTaskBefore = readSubTaskExecutor();
        Assertions.assertNotNull(pendingBefore);
        Assertions.assertNotNull(exportingBefore);
        Assertions.assertNotNull(subTaskBefore);

        ExportChecker.stopAll(1000L);
        Assertions.assertTrue(readPoolFromExecutor(pendingBefore).isShutdown(),
                "pending executor pool must be shut down");
        Assertions.assertTrue(readPoolFromExecutor(exportingBefore).isShutdown(),
                "exporting executor pool must be shut down");
        Assertions.assertTrue(readPoolFromExecutor(subTaskBefore).isShutdown(),
                "sub-task executor pool must be shut down");
        // stopAll must block until the pools actually terminate so demotion does not race
        // with old-leader export work. With idle pools (no in-flight tasks) every executor
        // should terminate before stopAll returns.
        Assertions.assertTrue(readPoolFromExecutor(pendingBefore).isTerminated(),
                "pending executor pool must be terminated before stopAll returns");
        Assertions.assertTrue(readPoolFromExecutor(exportingBefore).isTerminated(),
                "exporting executor pool must be terminated before stopAll returns");
        Assertions.assertTrue(readPoolFromExecutor(subTaskBefore).isTerminated(),
                "sub-task executor pool must be terminated before stopAll returns");

        // init() again must produce fresh instances.
        ExportChecker.init(1000L);
        Assertions.assertNotSame(pendingBefore, readExecutor(JobState.PENDING));
        Assertions.assertNotSame(exportingBefore, readExecutor(JobState.EXPORTING));
        Assertions.assertNotSame(subTaskBefore, readSubTaskExecutor());
        // Tear down what init() just created so the test does not leak threads.
        ExportChecker.stopAll(1000L);
    }

    private com.starrocks.task.LeaderTaskExecutor readExecutor(JobState state) throws Exception {
        Field field = ExportChecker.class.getDeclaredField("executors");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<JobState, com.starrocks.task.LeaderTaskExecutor> executors =
                (Map<JobState, com.starrocks.task.LeaderTaskExecutor>) field.get(null);
        return executors.get(state);
    }

    private com.starrocks.task.LeaderTaskExecutor readSubTaskExecutor() throws Exception {
        Field field = ExportChecker.class.getDeclaredField("exportingSubTaskExecutor");
        field.setAccessible(true);
        return (com.starrocks.task.LeaderTaskExecutor) field.get(null);
    }

    private java.util.concurrent.ThreadPoolExecutor readPoolFromExecutor(com.starrocks.task.LeaderTaskExecutor lte)
            throws Exception {
        Field field = com.starrocks.task.LeaderTaskExecutor.class.getDeclaredField("executor");
        field.setAccessible(true);
        return (java.util.concurrent.ThreadPoolExecutor) field.get(lte);
    }
}
