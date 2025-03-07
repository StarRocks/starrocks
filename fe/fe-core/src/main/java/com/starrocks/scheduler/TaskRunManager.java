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
import com.google.common.collect.Lists;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.util.LogUtil;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.common.util.concurrent.QueryableReentrantLock;
import com.starrocks.memory.MemoryTrackable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.history.TaskRunHistory;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.scheduler.persist.TaskRunStatusChange;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
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
        LOG.info("submit task run:{}", taskRun);

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
        if (!arrangeTaskRun(taskRun, false)) {
            LOG.warn("Submit task run to pending queue failed, reject the submit:{}", taskRun);
            return new SubmitResult(null, SubmitResult.SubmitStatus.REJECTED);
        }
        // Only log create task run status when it's not rejected and created.
        GlobalStateMgr.getCurrentState().getEditLog().logTaskRunCreateStatus(status);
        return new SubmitResult(queryId, SubmitResult.SubmitStatus.SUBMITTED, taskRun.getFuture());
    }

    public boolean killTaskRun(Long taskId, boolean force) {
        TaskRun taskRun = taskRunScheduler.getRunningTaskRun(taskId);
        if (taskRun == null) {
            return false;
        }
        try {
            taskRun.kill();
            ConnectContext runCtx = taskRun.getRunCtx();
            if (runCtx != null) {
                runCtx.kill(false, "kill TaskRun");
                return true;
            }
        } finally {
            // if it's force, remove it from running TaskRun map no matter it's killed or not
            if (force) {
                taskRunScheduler.removeRunningTask(taskRun.getTaskId());
            }
        }
        return false;
    }

    // At present, only the manual and automatic tasks of the materialized view have different priorities.
    // The manual priority is higher. For manual tasks, we do not merge operations.
    // For automatic tasks, we will compare the definition, and if they are the same,
    // we will perform the merge operation.
    public boolean arrangeTaskRun(TaskRun taskRun, boolean isReplay) {
        if (!tryTaskRunLock()) {
            return false;
        }
        List<TaskRun> mergedTaskRuns = new ArrayList<>();
        try {
            long taskId = taskRun.getTaskId();
            Set<TaskRun> taskRuns = taskRunScheduler.getPendingTaskRunsByTaskId(taskId);
            // If the task run is sync-mode, it will hang forever if the task run is merged because
            // user's using `future.get()` to wait and the future will not be set forever.
            ExecuteOption executeOption = taskRun.getExecuteOption();
            boolean isTaskRunContainsToMergeProperties = executeOption.containsToMergeProperties();
            if (taskRuns != null && executeOption.isMergeRedundant()) {
                for (TaskRun oldTaskRun : taskRuns) {
                    if (oldTaskRun == null) {
                        continue;
                    }
                    // If old task run is a sync-mode task, skip to merge it to avoid sync-mode task
                    // hanging after removing it.
                    ExecuteOption oldExecuteOption = oldTaskRun.getExecuteOption();
                    if (!oldExecuteOption.isMergeRedundant()) {
                        continue;
                    }
                    // skip if old task run is not equal to the task run
                    // The remove here is actually remove the old TaskRun.
                    // Note that the old TaskRun and new TaskRun may have the same definition,
                    // but other attributes may be different, such as priority, creation time.
                    // higher priority and create time will be result after merge is complete
                    // and queryId will be changed.
                    if (!oldTaskRun.isEqualTask(taskRun)) {
                        LOG.warn("failed to remove TaskRun definition is [{}]",
                                taskRun);
                        continue;
                    }

                    // TODO: Here we always merge the older task run to the newer task run which it can
                    // record the history of the task run. But we can also reject the newer task run directly to
                    // avoid the merge operation later.
                    boolean isOldTaskRunContainsToMergeProperties = oldExecuteOption.containsToMergeProperties();
                    // this should not happen since one task only can one task run in the running queue
                    if (isTaskRunContainsToMergeProperties && isOldTaskRunContainsToMergeProperties) {
                        LOG.warn("failed to merge TaskRun, both TaskRun contains toMergeProperties, " +
                                        "oldTaskRun: {}, taskRun: {}", oldTaskRun, taskRun);
                        continue;
                    }
                    // merge the old execution option into the new task run
                    if (isOldTaskRunContainsToMergeProperties && !isTaskRunContainsToMergeProperties) {
                        executeOption.mergeProperties(oldExecuteOption);
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
                    mergedTaskRuns.add(oldTaskRun);
                }
            }

            // recheck it again to avoid pending task run is too much
            long validPendingCount = taskRunScheduler.getPendingQueueCount();
            if (validPendingCount >= Config.task_runs_queue_length || !taskRunScheduler.addPendingTaskRun(taskRun)) {
                LOG.warn("failed to offer task: {}", taskRun);
                return false;
            }

            // if it 's not replay, update the status of the old TaskRun to MERGED in FOLLOWER/LEADER.
            // if it is replay, no need to update the status of the old TaskRun because follower FE cannot
            // update edit log.
            if (!isReplay) {
                // TODO: support batch update to reduce the number of edit logs.
                for (TaskRun oldTaskRun : mergedTaskRuns) {
                    oldTaskRun.getStatus().setFinishTime(System.currentTimeMillis());
                    // update the state of the old TaskRun to MERGED in FOLLOWER
                    TaskRunStatusChange statusChange = new TaskRunStatusChange(oldTaskRun.getTaskId(), oldTaskRun.getStatus(),
                            oldTaskRun.getStatus().getState(), Constants.TaskRunState.MERGED);
                    GlobalStateMgr.getCurrentState().getEditLog().logUpdateTaskRun(statusChange);
                    // update the state of the old TaskRun to MERGED in LEADER
                    oldTaskRun.getStatus().setState(Constants.TaskRunState.MERGED);
                    taskRunScheduler.removePendingTaskRun(oldTaskRun, Constants.TaskRunState.MERGED);
                    taskRunHistory.addHistory(oldTaskRun.getStatus());
                }
            } else {
                for (TaskRun oldTaskRun : mergedTaskRuns) {
                    // update the state of the old TaskRun to MERGED in LEADER
                    oldTaskRun.getStatus().setFinishTime(System.currentTimeMillis());
                    oldTaskRun.getStatus().setState(Constants.TaskRunState.MERGED);
                    taskRunScheduler.removePendingTaskRun(oldTaskRun, Constants.TaskRunState.MERGED);
                    taskRunHistory.addHistory(oldTaskRun.getStatus());
                }
            }
        } finally {
            taskRunUnlock();
        }
        return true;
    }

    // check if a running TaskRun is complete and remove it from running TaskRun map
    public void checkRunningTaskRun() {
        Set<Long> runningTaskIds = taskRunScheduler.getCopiedRunningTaskIds();
        for (Long taskId : runningTaskIds) {
            TaskRun taskRun = taskRunScheduler.getRunningTaskRun(taskId);
            if (taskRun == null) {
                LOG.warn("failed to get running TaskRun by taskId:{}", taskId);
                taskRunScheduler.removeRunningTask(taskId);
                return;
            }

            Future<?> future = taskRun.getFuture();
            if (future.isDone()) {
                taskRunScheduler.removeRunningTask(taskId);
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

    public TaskRunScheduler getTaskRunScheduler() {
        return taskRunScheduler;
    }

    public TaskRunHistory getTaskRunHistory() {
        return taskRunHistory;
    }

    @Override
    public Map<String, Long> estimateCount() {
        long validPendingCount = taskRunScheduler.getPendingQueueCount();
        return ImmutableMap.of("PendingTaskRun", validPendingCount,
                "RunningTaskRun", (long) taskRunScheduler.getRunningTaskCount(),
                "HistoryTaskRun", taskRunHistory.getTaskRunCount());
    }

    @Override
    public List<Pair<List<Object>, Long>> getSamples() {
        List<Object> taskRunSamples = taskRunHistory.getSamplesForMemoryTracker();
        long size = taskRunScheduler.getPendingQueueCount()
                + taskRunScheduler.getRunningTaskCount()
                + taskRunHistory.getTaskRunCount();
        return Lists.newArrayList(Pair.create(taskRunSamples, size));
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
