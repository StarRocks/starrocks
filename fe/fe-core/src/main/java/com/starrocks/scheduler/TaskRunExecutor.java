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
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.persist.TaskRunStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class TaskRunExecutor {
    private static final Logger LOG = LogManager.getLogger(TaskRunExecutor.class);
    private final ExecutorService taskRunPool = ThreadPoolManager
            .newDaemonCacheThreadPool(Config.max_task_runs_threads_num, "starrocks-taskrun-pool", true);
    /**
     * Since TaskRun supports `task_retry_attempts`, we need to retry the task-run if it fails.
     * `RetriableActionAttempt` means a retry atempt for a task-run.
     */
    class RetriableActionAttempt {
        private final TaskRun taskRun;
        private final AtomicInteger attempt;
        private final AtomicInteger totalAttempt;

        public RetriableActionAttempt(TaskRun taskRun, int attempt, int totalAttempt) {
            this.taskRun = taskRun;
            this.attempt = new AtomicInteger(attempt);
            this.totalAttempt = new AtomicInteger(totalAttempt);
        }

        public TaskRun getTaskRun() {
            return taskRun;
        }

        public int getAttempt() {
            return attempt.get();
        }

        public int getTotalAttempt() {
            return totalAttempt.get();
        }

        public RetriableActionAttempt next() {
            attempt.incrementAndGet();
            if (attempt.get() >= totalAttempt.get()) {
                return null;
            }
            if (this.attempt.get() > 0) {
                TaskRunStatus status = taskRun.getStatus();
                // refresh queryId and createTime each time retry
                String queryId = UUIDUtil.genUUID().toString();
                status.setQueryId(queryId);
                long created = System.currentTimeMillis();
                status.setCreateTime(created);
            }
            return new RetriableActionAttempt(taskRun, attempt.get(), totalAttempt.get());
        }

        public boolean executeTaskRun() throws Exception {
            return taskRun.executeTaskRun();
        }
    }

    /**
     * Async execute a task-run, use the return value to indicate submit success or not
     */
    public boolean executeTaskRun(TaskRun taskRun) {
        if (taskRun == null) {
            return false;
        }
        TaskRunStatus status = taskRun.getStatus();
        if (status == null) {
            LOG.warn("TaskRun {}/{} has no state, avoid execute it again", status.getTaskName(),
                    status.getQueryId());
            return false;
        }
        if (status.getState() != Constants.TaskRunState.PENDING) {
            LOG.warn("TaskRun {}/{} is in {} state, avoid execute it again", status.getTaskName(),
                    status.getQueryId(), status.getState());
            return false;
        }

        // Synchronously update the status, to make sure they can be persisted
        status.setState(Constants.TaskRunState.RUNNING);
        status.setProcessStartTime(System.currentTimeMillis());

        CompletableFuture<Constants.TaskRunState> future = CompletableFuture.supplyAsync(() -> {
            Task task = taskRun.getTask();
            try {
                boolean isSuccess = executeRetryingTaskRun(taskRun);
                if (isSuccess) {
                    status.setState(Constants.TaskRunState.SUCCESS);
                } else {
                    status.setState(Constants.TaskRunState.FAILED);
                }
            } catch (Exception ex) {
                status.setState(Constants.TaskRunState.FAILED);
                status.setErrorCode(-1);
                status.setErrorMessage(DebugUtil.getStackTrace(ex));
            } finally {
                status.setFinishTime(System.currentTimeMillis());
                task.setLastLastFinishTime(status.getFinishTime());
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

    /**
     * Execute a task-run with retrying mechanism.
     * @param taskRun the task-run to execute
     * @return true if the task-run is executed successfully, false otherwise
     */
    public boolean executeRetryingTaskRun(TaskRun taskRun) {
        RetriableActionAttempt taskRunAttempt = new RetriableActionAttempt(taskRun, 0, taskRun.getTaskRetryAttempts());
        Exception lastEx = null;
        TaskRunStatus status = taskRun.getTaskRunStatus();
        while (taskRunAttempt != null && taskRunAttempt.getAttempt() < PropertyAnalyzer.MAX_RETRY_ATTEMPT_TIMES) {
            // update task run attempt
            status.setTaskRunAttempt(taskRunAttempt.getAttempt());
            try {
                boolean isSuccess = taskRunAttempt.executeTaskRun();
                if (isSuccess) {
                    LOG.info("TaskRun {}/{} is executed successfully in attempt{}/{}, task run status:{}",
                            status.getTaskName(), status.getQueryId(), taskRunAttempt.getAttempt(),
                            taskRunAttempt.getTotalAttempt(), status);
                    return true;
                }
            } catch (Exception ex) {
                // TODO: Determine whether to retry the task-run according to the exception type later since some failures may
                // be non-recoverable.
                LOG.warn("failed to execute TaskRun in attempt{}/{}, task run status:{}", taskRunAttempt.getAttempt(),
                        taskRunAttempt.getTotalAttempt(), status, ex);
                lastEx = ex;
            } finally {
                // NOTE: Ensure this thread local is removed after this method to avoid memory leak in JVM.
                ConnectContext.remove();
            }
            taskRunAttempt = taskRunAttempt.next();
        }
        status.setErrorCode(-1);
        if (lastEx != null) {
            status.setErrorMessage(DebugUtil.getStackTrace(lastEx));
        }
        return false;
    }
}