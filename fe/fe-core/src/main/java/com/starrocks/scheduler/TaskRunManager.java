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

import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.starrocks.common.Config;
import com.starrocks.common.util.QueryableReentrantLock;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.common.util.Util;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.scheduler.persist.TaskRunStatusChange;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Future;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TaskRunManager {

    private static final Logger LOG = LogManager.getLogger(TaskRunManager.class);

    // taskId -> pending TaskRun Queue, for each Task only support 1 running taskRun currently,
    // so the map value is priority queue need to be sorted by priority from large to small
    private final Map<Long, PriorityBlockingQueue<TaskRun>> pendingTaskRunMap = Maps.newConcurrentMap();

    // taskId -> running TaskRun, for each Task only support 1 running taskRun currently,
    // so the map value is not queue
    private final Map<Long, TaskRun> runningTaskRunMap = Maps.newConcurrentMap();

    // include SUCCESS/FAILED/CANCEL taskRun
    private final TaskRunHistory taskRunHistory = new TaskRunHistory();

    // Use to execute actual TaskRun
    private final TaskRunExecutor taskRunExecutor = new TaskRunExecutor();

    private final QueryableReentrantLock taskRunLock = new QueryableReentrantLock(true);

    public SubmitResult submitTaskRun(TaskRun taskRun, ExecuteOption option) {
        // duplicate submit
        if (taskRun.getStatus() != null) {
            return new SubmitResult(taskRun.getStatus().getQueryId(), SubmitResult.SubmitStatus.FAILED);
        }

        int validPendingCount = 0;
        for (Long taskId : pendingTaskRunMap.keySet()) {
            PriorityBlockingQueue<TaskRun> taskRuns = pendingTaskRunMap.get(taskId);
            if (taskRuns != null && !taskRuns.isEmpty()) {
                validPendingCount++;
            }
        }

        if (validPendingCount >= Config.task_runs_queue_length) {
            LOG.warn("pending TaskRun exceeds task_runs_queue_length:{}, reject the submit.",
                    Config.task_runs_queue_length);
            return new SubmitResult(null, SubmitResult.SubmitStatus.REJECTED);
        }

        String queryId = UUIDUtil.genUUID().toString();
        TaskRunStatus status = taskRun.initStatus(queryId, System.currentTimeMillis());
        status.setPriority(option.getPriority());
        status.setMergeRedundant(option.isMergeRedundant());
        GlobalStateMgr.getCurrentState().getEditLog().logTaskRunCreateStatus(status);
        arrangeTaskRun(taskRun, option.isMergeRedundant());
        return new SubmitResult(queryId, SubmitResult.SubmitStatus.SUBMITTED, taskRun.getFuture());
    }

    public boolean killTaskRun(Long taskId) {
        TaskRun taskRun = runningTaskRunMap.get(taskId);
        if (taskRun == null) {
            return false;
        }
        ConnectContext runCtx = taskRun.getRunCtx();
        if (runCtx != null) {
            runCtx.kill(false);
            return true;
        }
        return false;
    }

    // At present, only the manual and automatic tasks of the materialized view have different priorities.
    // The manual priority is higher. For manual tasks, we do not merge operations.
    // For automatic tasks, we will compare the definition, and if they are the same,
    // we will perform the merge operation.
    public void arrangeTaskRun(TaskRun taskRun, boolean mergeRedundant) {
        if (!tryTaskRunLock()) {
            return;
        }
        try {
            long taskId = taskRun.getTaskId();
            PriorityBlockingQueue<TaskRun> taskRuns = pendingTaskRunMap.computeIfAbsent(taskId,
                    u -> Queues.newPriorityBlockingQueue());
            if (mergeRedundant) {
                TaskRun oldTaskRun = getTaskRun(taskRuns, taskRun);
                if (oldTaskRun != null) {
                    // The remove here is actually remove the old TaskRun.
                    // Note that the old TaskRun and new TaskRun may have the same definition,
                    // but other attributes may be different, such as priority, creation time.
                    // higher priority and create time will be result after merge is complete
                    // and queryId will be change.
                    boolean isRemove = taskRuns.remove(taskRun);
                    if (!isRemove) {
                        LOG.warn("failed to remove TaskRun definition is [{}]",
                                taskRun.getStatus().getDefinition());
                    }
                    if (oldTaskRun.getStatus().getPriority() > taskRun.getStatus().getPriority()) {
                        taskRun.getStatus().setPriority(oldTaskRun.getStatus().getPriority());
                    }
                    if (oldTaskRun.getStatus().getCreateTime() > taskRun.getStatus().getCreateTime()) {
                        taskRun.getStatus().setCreateTime(oldTaskRun.getStatus().getCreateTime());
                    }
                }
            }
            if (!taskRuns.offer(taskRun)) {
                LOG.warn("failed to offer task");
            }
        } finally {
            taskRunUnlock();
        }
    }

    // Because java PriorityQueue does not provide an interface for searching by element,
    // so find it by code O(n), which can be optimized later
    @Nullable
    private TaskRun getTaskRun(PriorityBlockingQueue<TaskRun> taskRuns, TaskRun taskRun) {
        TaskRun oldTaskRun = null;
        for (TaskRun run : taskRuns) {
            if (run.equals(taskRun)) {
                oldTaskRun = run;
                break;
            }
        }
        return oldTaskRun;
    }

    // check if a running TaskRun is complete and remove it from running TaskRun map
    public void checkRunningTaskRun() {
        Iterator<Long> runningIterator = runningTaskRunMap.keySet().iterator();
        while (runningIterator.hasNext()) {
            Long taskId = runningIterator.next();
            TaskRun taskRun = runningTaskRunMap.get(taskId);
            if (taskRun == null) {
                LOG.warn("failed to get running TaskRun by taskId:{}", taskId);
                runningIterator.remove();
                return;
            }
            Future<?> future = taskRun.getFuture();
            if (future.isDone()) {
                runningIterator.remove();
                taskRunHistory.addHistory(taskRun.getStatus());
                TaskRunStatusChange statusChange = new TaskRunStatusChange(taskRun.getTaskId(), taskRun.getStatus(),
                        Constants.TaskRunState.RUNNING, taskRun.getStatus().getState());
                GlobalStateMgr.getCurrentState().getEditLog().logUpdateTaskRun(statusChange);
            }
        }
    }

    // schedule the pending TaskRun that can be run into running TaskRun map
    public void scheduledPendingTaskRun() {
        int currentRunning = runningTaskRunMap.size();

        Iterator<Long> pendingIterator = pendingTaskRunMap.keySet().iterator();
        while (pendingIterator.hasNext()) {
            Long taskId = pendingIterator.next();
            TaskRun runningTaskRun = runningTaskRunMap.get(taskId);
            if (runningTaskRun == null) {
                Queue<TaskRun> taskRunQueue = pendingTaskRunMap.get(taskId);
                if (taskRunQueue.size() == 0) {
                    pendingIterator.remove();
                } else {
                    if (currentRunning >= Config.task_runs_concurrency) {
                        break;
                    }
                    TaskRun pendingTaskRun = taskRunQueue.poll();
                    taskRunExecutor.executeTaskRun(pendingTaskRun);
                    runningTaskRunMap.put(taskId, pendingTaskRun);
                    // RUNNING state persistence is for FE FOLLOWER update state
                    TaskRunStatusChange statusChange = new TaskRunStatusChange(taskId, pendingTaskRun.getStatus(),
                            Constants.TaskRunState.PENDING, Constants.TaskRunState.RUNNING);
                    GlobalStateMgr.getCurrentState().getEditLog().logUpdateTaskRun(statusChange);
                    currentRunning++;
                }
            }
        }
    }

    public boolean tryTaskRunLock() {
        try {
            if (!taskRunLock.tryLock(5, TimeUnit.SECONDS)) {
                Thread owner = taskRunLock.getOwner();
                if (owner != null) {
                    LOG.warn("task run lock is held by: {}", () -> Util.dumpThread(owner, 50));
                } else {
                    LOG.warn("task run lock owner is null");
                }
                return false;
            }
            return true;
        } catch (InterruptedException e) {
            LOG.warn("got exception while getting task run lock", e);
            Thread.currentThread().interrupt();
        }
        return false;
    }

    public void taskRunUnlock() {
        this.taskRunLock.unlock();
    }

    public Map<Long, PriorityBlockingQueue<TaskRun>> getPendingTaskRunMap() {
        return pendingTaskRunMap;
    }

    public Map<Long, TaskRun> getRunningTaskRunMap() {
        return runningTaskRunMap;
    }

    public TaskRunHistory getTaskRunHistory() {
        return taskRunHistory;
    }
}
