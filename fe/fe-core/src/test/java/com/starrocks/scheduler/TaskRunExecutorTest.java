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

package com.starrocks.scheduler;

import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.WALApplier;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.server.GlobalStateMgr;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

public class TaskRunExecutorTest {
    private TaskRun pendingTask() {
        TaskRun task = Mockito.mock(TaskRun.class);
        TaskRunStatus status = new TaskRunStatus();
        status.setState(Constants.TaskRunState.PENDING);
        Mockito.when(task.getStatus()).thenReturn(status);
        Mockito.when(task.getFuture()).thenReturn(new CompletableFuture<>());
        return task;
    }

    @Test
    public void testQueuedRunSettlesExceptionallyWithoutExecutingOrRewritingDurableStatus() throws Exception {
        TaskRunExecutor executor = new TaskRunExecutor();
        executor.shutdown();
        ExecutorService pool = Executors.newSingleThreadExecutor();
        Deencapsulation.setField(executor, "taskRunPool", pool);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        pool.submit(() -> {
            entered.countDown();
            release.await();
            return null;
        });
        TaskRun task = pendingTask();
        GlobalStateMgr state = Mockito.mock(GlobalStateMgr.class);
        EditLog log = Mockito.mock(EditLog.class);
        Mockito.when(state.getEditLog()).thenReturn(log);
        Mockito.doAnswer(invocation -> {
            WALApplier applier = invocation.getArgument(1);
            applier.apply(invocation.getArgument(0));
            return null;
        }).when(log).logUpdateTaskRun(Mockito.any(), Mockito.any());
        try (MockedStatic<GlobalStateMgr> global = Mockito.mockStatic(GlobalStateMgr.class)) {
            global.when(GlobalStateMgr::getCurrentState).thenReturn(state);
            Assertions.assertTrue(entered.await(3, TimeUnit.SECONDS));
            Assertions.assertTrue(executor.executeTaskRun(task));
            executor.shutdown();
            Assertions.assertFalse(pool.isTerminated());
            Assertions.assertFalse(task.getFuture().isDone());
            release.countDown();
            Assertions.assertTrue(pool.awaitTermination(3, TimeUnit.SECONDS));
            Assertions.assertTrue(task.getFuture().isCompletedExceptionally());
            Assertions.assertEquals(Constants.TaskRunState.RUNNING, task.getStatus().getState());
            Mockito.verify(task, Mockito.never()).executeTaskRun();
        } finally {
            release.countDown();
            pool.shutdown();
            pool.awaitTermination(3, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSubmissionCannotMoveToRebuiltPoolAfterWalApply() throws Exception {
        TaskRunExecutor executor = new TaskRunExecutor();
        executor.shutdown();
        ExecutorService oldPool = Executors.newSingleThreadExecutor();
        ExecutorService newPool = Executors.newSingleThreadExecutor();
        Deencapsulation.setField(executor, "taskRunPool", oldPool);
        TaskRun task = pendingTask();
        GlobalStateMgr state = Mockito.mock(GlobalStateMgr.class);
        EditLog log = Mockito.mock(EditLog.class);
        Mockito.when(state.getEditLog()).thenReturn(log);
        Mockito.doAnswer(invocation -> {
            WALApplier applier = invocation.getArgument(1);
            applier.apply(invocation.getArgument(0));
            // A submitting connection thread can pause after admission while the pools change term.
            oldPool.shutdown();
            Deencapsulation.setField(executor, "taskRunPool", newPool);
            return null;
        }).when(log).logUpdateTaskRun(Mockito.any(), Mockito.any());
        try (MockedStatic<GlobalStateMgr> global = Mockito.mockStatic(GlobalStateMgr.class)) {
            global.when(GlobalStateMgr::getCurrentState).thenReturn(state);
            Assertions.assertThrows(RejectedExecutionException.class, () -> executor.executeTaskRun(task));
            Mockito.verify(task, Mockito.never()).executeTaskRun();
        } finally {
            oldPool.shutdown();
            newPool.shutdown();
            oldPool.awaitTermination(3, TimeUnit.SECONDS);
            newPool.awaitTermination(3, TimeUnit.SECONDS);
        }
    }
}
