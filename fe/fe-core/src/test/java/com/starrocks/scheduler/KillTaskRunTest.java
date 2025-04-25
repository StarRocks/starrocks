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

import com.starrocks.qe.ConnectContext;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KillTaskRunTest {
    private TaskRunScheduler taskRunScheduler;
    private TaskRunManager taskRunManager;

    @Before
    public void setUp() {
        taskRunScheduler = mock(TaskRunScheduler.class);
        taskRunManager = new TaskRunManager(taskRunScheduler);
    }

    @Test
    public void testKillTaskRun_Success() {
        long taskId = 123L;
        TaskRun mockTaskRun = mock(TaskRun.class);
        CompletableFuture<Constants.TaskRunState> future = new CompletableFuture<>();
        ConnectContext ctx = mock(ConnectContext.class);

        when(taskRunScheduler.getRunningTaskRun(taskId)).thenReturn(mockTaskRun);
        when(mockTaskRun.getFuture()).thenReturn(future);
        when(mockTaskRun.getRunCtx()).thenReturn(ctx);

        boolean result = taskRunManager.killTaskRun(taskId, false);
        assertTrue(result);

        verify(mockTaskRun).kill();
        assertTrue(future.isCompletedExceptionally());
        verify(ctx).kill(eq(false), anyString());
        verify(taskRunScheduler, never()).removeRunningTask(anyLong());
    }

    @Test
    public void testKillTaskRun_FutureAlreadyDone() {
        long taskId = 456L;
        TaskRun mockTaskRun = mock(TaskRun.class);
        CompletableFuture<Constants.TaskRunState> future = new CompletableFuture<>();
        future.complete(Constants.TaskRunState.SUCCESS);

        when(taskRunScheduler.getRunningTaskRun(taskId)).thenReturn(mockTaskRun);
        when(mockTaskRun.getFuture()).thenReturn(future);
        when(mockTaskRun.getRunCtx()).thenReturn(mock(ConnectContext.class));

        boolean result = taskRunManager.killTaskRun(taskId, false);
        assertTrue(result);

        verify(mockTaskRun).kill();
    }

    @Test
    public void testKillTaskRun_NullCtx() {
        long taskId = 789L;
        TaskRun mockTaskRun = mock(TaskRun.class);
        CompletableFuture<Constants.TaskRunState> future = new CompletableFuture<>();

        when(taskRunScheduler.getRunningTaskRun(taskId)).thenReturn(mockTaskRun);
        when(mockTaskRun.getFuture()).thenReturn(future);
        when(mockTaskRun.getRunCtx()).thenReturn(null);

        boolean result = taskRunManager.killTaskRun(taskId, false);
        assertFalse(result);
    }

    @Test
    public void testKillTaskRun_NullTask() {
        boolean result = taskRunManager.killTaskRun(999L, false);
        assertFalse(result);
    }

    @Test
    public void testKillTaskRun_ForceRemoveAlways() {
        long taskId = 888L;
        TaskRun mockTaskRun = mock(TaskRun.class);
        CompletableFuture<Constants.TaskRunState> future = new CompletableFuture<>();

        when(taskRunScheduler.getRunningTaskRun(taskId)).thenReturn(mockTaskRun);
        when(mockTaskRun.getTaskId()).thenReturn(taskId);
        when(mockTaskRun.getFuture()).thenReturn(future);
        when(mockTaskRun.getRunCtx()).thenReturn(null);

        boolean result = taskRunManager.killTaskRun(taskId, true);
        assertFalse(result);

        verify(taskRunScheduler).removeRunningTask(taskId);
    }
}
