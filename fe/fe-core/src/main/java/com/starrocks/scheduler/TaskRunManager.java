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

import com.google.common.collect.ImmutableMap;
import com.starrocks.common.Config;
import com.starrocks.common.util.LogUtil;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.common.util.concurrent.QueryableReentrantLock;
import com.starrocks.memory.MemoryTrackable;
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

public class TaskRunManager implements MemoryTrackable {

    private static final Logger LOG = LogManager.getLogger(TaskRunManager.class);

    private final TaskRunScheduler taskRunScheduler = new TaskRunScheduler();

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

        long validPendingCount = taskRunScheduler.getPendingQueueCount();
        if (validPendingCount >= Config.task_runs_queue_length) {
            LOG.warn("pending TaskRun exceeds task_runs_queue_length:{}, reject the submit: {}",
                    Config.task_runs_queue_length, taskRun);
            return new SubmitResult(null, SubmitResult.SubmitStatus.REJECTED);
        }

        String queryId = UUIDUtil.genUUID().toString();
        TaskRunStatus status = taskRun.initStatus(queryId, System.currentTimeMillis());
        status.setPriority(option.getPriority());
        status.setMergeRedundant(option.isMergeRedundant());
        status.setProperties(option.getTaskRunProperties());
        if (!arrangeTaskRun(taskRun)) {
            LOG.warn("Submit task run to pending queue failed, reject the submit:{}", taskRun);
            return new SubmitResult(null, SubmitResult.SubmitStatus.REJECTED);
        }
        // Only log create task run status when it's not rejected and created.
        GlobalStateMgr.getCurrentState().getEditLog().logTaskRunCreateStatus(status);
        return new SubmitResult(queryId, SubmitResult.SubmitStatus.SUBMITTED, taskRun.getFuture());
    }

    public boolean killTaskRun(Long taskId) {
        TaskRun taskRun = taskRunScheduler.getRunningTaskRun(taskId);
        if (taskRun == null) {
            return false;
        }
        taskRun.kill();
        ConnectContext runCtx = taskRun.getRunCtx();
        if (runCtx != null) {
            runCtx.kill(false, "kill TaskRun");
            return true;
        }
        return false;
    }

    // At present, only the manual and automatic tasks of the materialized view have different priorities.
    // The manual priority is higher. For manual tasks, we do not merge operations.
    // For automatic tasks, we will compare the definition, and if they are the same,
    // we will perform the merge operation.
    public boolean arrangeTaskRun(TaskRun taskRun) {
        if (!tryTaskRunLock()) {
            return false;
        }
        try {
            long taskId = taskRun.getTaskId();
            Queue<TaskRun> taskRuns = taskRunScheduler.getPendingTaskRunsByTaskId(taskId);
            // If the task run is sync-mode, it will hang forever if the task run is merged because
            // user's using `future.get()` to wait and the future will not be set forever.
            ExecuteOption executeOption = taskRun.getExecuteOption();
            if (taskRuns != null && executeOption.isMergeRedundant()) {
                Iterator<TaskRun> iter = taskRuns.iterator();
                while (iter.hasNext()) {
                    TaskRun oldTaskRun = iter.next();
                    if (oldTaskRun == null) {
                        continue;
                    }
                    // If old task run is a sync-mode task, skip to merge it to avoid sync-mode task
                    // hanging after removing it.
                    if (!oldTaskRun.getExecuteOption().isMergeRedundant()) {
                        continue;
                    }
                    // skip if old task run is not equal to the task run
                    // The remove here is actually remove the old TaskRun.
                    // Note that the old TaskRun and new TaskRun may have the same definition,
                    // but other attributes may be different, such as priority, creation time.
                    // higher priority and create time will be result after merge is complete
                    // and queryId will be changed.
                    if (!oldTaskRun.equals(taskRun)) {
                        LOG.warn("failed to remove TaskRun definition is [{}]",
                                taskRun);
                        continue;
                    }

                    // prefer higher priority to be better scheduler
                    if (oldTaskRun.getStatus().getPriority() > taskRun.getStatus().getPriority()) {
                        taskRun.getStatus().setPriority(oldTaskRun.getStatus().getPriority());
                    }

                    // prefer older create time to be better scheduler
                    if (oldTaskRun.getStatus().getCreateTime() < taskRun.getStatus().getCreateTime()) {
                        taskRun.getStatus().setCreateTime(oldTaskRun.getStatus().getCreateTime());
                    }
                    LOG.info("Merge redundant task run, oldTaskRun: {}, taskRun: {}",
                            oldTaskRun, taskRun);

                    taskRunScheduler.removePendingTaskRunFromQueue(oldTaskRun);
                    iter.remove();

                    // Update follower's state to SUCCESS, otherwise the merged task run will always be PENDING.
                    // TODO: 1. add a MERGED state later. 2. support batch update to reduce the number of edit logs.
                    oldTaskRun.getStatus().setFinishTime(System.currentTimeMillis());
                    TaskRunStatusChange statusChange = new TaskRunStatusChange(oldTaskRun.getTaskId(), oldTaskRun.getStatus(),
                            oldTaskRun.getStatus().getState(), Constants.TaskRunState.SUCCESS);
                    GlobalStateMgr.getCurrentState().getEditLog().logUpdateTaskRun(statusChange);
                }
            }
            if (!taskRunScheduler.addPendingTaskRun(taskRun)) {
                LOG.warn("failed to offer task: {}", taskRun);
                return false;
            }
        } finally {
            taskRunUnlock();
        }
        return true;
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
        Map<Long, TaskRun> runningTaskRunMap = taskRunScheduler.getRunningTaskRunMap();
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
                LOG.info("Task run is done from state RUNNING to {}, {}", taskRun.getStatus().getState(), taskRun);
                taskRunHistory.addHistory(taskRun.getStatus());
                TaskRunStatusChange statusChange = new TaskRunStatusChange(taskRun.getTaskId(), taskRun.getStatus(),
                        Constants.TaskRunState.RUNNING, taskRun.getStatus().getState());
                GlobalStateMgr.getCurrentState().getEditLog().logUpdateTaskRun(statusChange);
            }
        }
    }

    // schedule the pending TaskRun that can be run into running TaskRun map
    public void scheduledPendingTaskRun() {
        taskRunScheduler.scheduledPendingTaskRun(pendingTaskRun -> {
            if (taskRunExecutor.executeTaskRun(pendingTaskRun)) {
                LOG.info("start to schedule pending task run to execute: {}", pendingTaskRun);
                long taskId = pendingTaskRun.getTaskId();
                // RUNNING state persistence is for FE FOLLOWER update state
                TaskRunStatusChange statusChange = new TaskRunStatusChange(taskId, pendingTaskRun.getStatus(),
                        Constants.TaskRunState.PENDING, Constants.TaskRunState.RUNNING);
                GlobalStateMgr.getCurrentState().getEditLog().logUpdateTaskRun(statusChange);
            } else {
                LOG.warn("failed to scheduled task-run {}", pendingTaskRun);
            }
        });
    }

    public boolean tryTaskRunLock() {
        try {
            if (!taskRunLock.tryLock(5, TimeUnit.SECONDS)) {
                Thread owner = taskRunLock.getOwner();
                if (owner != null) {
                    LOG.warn("task run lock is held by: {}", () -> LogUtil.dumpThread(owner, 50));
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

    public TaskRun getRunnableTaskRun(long taskId) {
        return taskRunScheduler.getRunnableTaskRun(taskId);
    }

    public Queue<TaskRun> getPendingTaskRuns() {
        return taskRunScheduler.getPendingTaskRuns();
    }
    public Map<Long, Queue<TaskRun>> getPendingTaskRunMap() {
        return taskRunScheduler.getPendingTaskRunMap();
    }

    public Map<Long, TaskRun> getRunningTaskRunMap() {
        return taskRunScheduler.getRunningTaskRunMap();
    }

    public TaskRunHistory getTaskRunHistory() {
        return taskRunHistory;
    }

    public boolean containsTaskInRunningTaskRunMap(long taskId) {
        return taskRunScheduler.getRunningTaskRunMap().containsKey(taskId);
    }

    public long getPendingTaskRunCount(long taskId) {
        taskRunLock.lock();
        try {
            Queue<TaskRun> pendingTaskRuns = taskRunScheduler.getPendingTaskRunsByTaskId(taskId);
            return  pendingTaskRuns == null ? 0L : pendingTaskRuns.size();
        } catch (Exception e) {
            return 0L;
        } finally {
            taskRunLock.unlock();
        }
    }

    @Override
    public Map<String, Long> estimateCount() {
        long validPendingCount = taskRunScheduler.getPendingQueueCount();
        return ImmutableMap.of("PendingTaskRun", validPendingCount,
                "RunningTaskRun", (long) taskRunScheduler.getRunningTaskRunMap().size(),
                "HistoryTaskRun", taskRunHistory.getTaskRunCount());
    }
  
    /**
     * For diagnosis purpose
     *
     * @return JSON-representation of the whole status
     */
    public String inspect() {
        return taskRunScheduler.toString();
    }
}
