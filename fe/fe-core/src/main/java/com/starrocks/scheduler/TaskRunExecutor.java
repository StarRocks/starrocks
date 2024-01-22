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

import com.starrocks.common.Config;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.persist.TaskRunStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

public class TaskRunExecutor {
    private static final Logger LOG = LogManager.getLogger(TaskRunExecutor.class);
    private final ExecutorService taskRunPool = ThreadPoolManager
            .newDaemonCacheThreadPool(Config.max_task_runs_threads_num, "starrocks-taskrun-pool", true);

    /**
     * Async execute a task-run, use the return value to indicate submit success or not
     */
    public boolean executeTaskRun(TaskRun taskRun) {
        if (taskRun == null) {
            return false;
        }
        TaskRunStatus status = taskRun.getStatus();
        if (status == null) {
            return false;
        }
        if (status.getState() != Constants.TaskRunState.PENDING) {
            LOG.warn("TaskRun {}/{} is in {} state, avoid execute it again", status.getTaskName(),
                    status.getQueryId(), status.getState());
            return false;
        }

        CompletableFuture<Constants.TaskRunState> future = CompletableFuture.supplyAsync(() -> {
            status.setState(Constants.TaskRunState.RUNNING);
            // set process start time
            status.setProcessStartTime(System.currentTimeMillis());
            try {
                boolean isSuccess = taskRun.executeTaskRun();
                if (isSuccess) {
                    status.setState(Constants.TaskRunState.SUCCESS);
                } else {
                    status.setState(Constants.TaskRunState.FAILED);
                }
            } catch (Exception ex) {
                LOG.warn("failed to execute TaskRun.", ex);
                status.setState(Constants.TaskRunState.FAILED);
                status.setErrorCode(-1);
                status.setErrorMessage(ex.getMessage());
            } finally {
                // NOTE: Ensure this thread local is removed after this method to avoid memory leak in JVM.
                ConnectContext.remove();
                status.setFinishTime(System.currentTimeMillis());
            }
            return status.getState();
        }, taskRunPool);
        future.whenComplete((r, e) -> {
            if (e == null) {
                taskRun.getFuture().complete(r);
            } else {
                taskRun.getFuture().completeExceptionally(e);
            }
        });
        return true;
    }

}
